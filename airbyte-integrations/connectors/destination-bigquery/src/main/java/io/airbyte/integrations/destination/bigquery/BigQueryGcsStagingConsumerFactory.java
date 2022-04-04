package io.airbyte.integrations.destination.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Preconditions;
import io.airbyte.commons.functional.CheckedBiConsumer;
import io.airbyte.commons.functional.CheckedBiFunction;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.AirbyteStreamNameNamespacePair;
import io.airbyte.integrations.destination.StandardNameTransformer;
import io.airbyte.integrations.destination.bigquery.formatter.BigQueryRecordFormatter;
import io.airbyte.integrations.destination.bigquery.formatter.DefaultBigQueryRecordFormatter;
import io.airbyte.integrations.destination.bigquery.formatter.GcsAvroBigQueryRecordFormatter;
import io.airbyte.integrations.destination.bigquery.formatter.GcsCsvBigQueryRecordFormatter;
import io.airbyte.integrations.destination.bigquery.uploader.AbstractBigQueryUploader;
import io.airbyte.integrations.destination.bigquery.uploader.GcsAvroBigQueryUploader;
import io.airbyte.integrations.destination.bigquery.uploader.UploaderType;
import io.airbyte.integrations.destination.bigquery.uploader.config.UploaderConfig;
import io.airbyte.integrations.destination.buffered_stream_consumer.BufferedStreamConsumer;
import io.airbyte.integrations.destination.buffered_stream_consumer.OnCloseFunction;
import io.airbyte.integrations.destination.buffered_stream_consumer.OnStartFunction;
import io.airbyte.integrations.destination.gcs.GcsDestinationConfig;
import io.airbyte.integrations.destination.record_buffer.SerializableBuffer;
import io.airbyte.integrations.destination.record_buffer.SerializedBufferingStrategy;
import io.airbyte.integrations.destination.s3.BlobStorageOperations;
import io.airbyte.integrations.destination.s3.S3ConsumerFactory;
import io.airbyte.integrations.destination.s3.S3DestinationConstants;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryGcsStagingConsumerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3ConsumerFactory.class);
  private static final DateTime SYNC_DATETIME = DateTime.now(DateTimeZone.UTC);

  public AirbyteMessageConsumer create(final Consumer<AirbyteMessage> outputRecordCollector,
                                       final BigQuery bigQuery,
                                       final BlobStorageOperations storageOperations,
                                       final StandardNameTransformer namingResolver,
                                       final CheckedBiFunction<AirbyteStreamNameNamespacePair, ConfiguredAirbyteCatalog, SerializableBuffer, Exception> onCreateBuffer,
                                       final JsonNode config,
                                       final GcsDestinationConfig gcsConfig,
                                       final ConfiguredAirbyteCatalog catalog) {
    final Map<AirbyteStreamNameNamespacePair, UploaderConfig> writeConfigs = catalog.getStreams()
        .stream()
        .map(toWriteConfig(storageOperations, namingResolver, config))
        .collect(Collectors.toUnmodifiableMap(
            c -> new AirbyteStreamNameNamespacePair(c.getStreamName(), c.getNamespace()),
            c -> c));
    final Map<AirbyteStreamNameNamespacePair, AbstractBigQueryUploader<?>> uploaders = writeConfigs.entrySet().stream()
        .collect(Collectors.toUnmodifiableMap(
            Entry::getKey,
            e -> {
              final UploaderConfig writeConfig = e.getValue();
              final TableId targetTable = TableId.of(writeConfig.getOutputSchema(), writeConfig.getTargetTableName());
              final TableId tmpTable = TableId.of(writeConfig.getOutputSchema(), writeConfig.getTmpTableName());
              final JobInfo.WriteDisposition syncMode = BigQueryUtils.getWriteDisposition(writeConfig.getConfigStream().getDestinationSyncMode());
              final boolean keepStagingFiles = BigQueryUtils.isKeepFilesInGcs(config);
              return new GcsAvroBigQueryUploader(
                  targetTable,
                  tmpTable,
                  null,
                  syncMode,
                  gcsConfig,
                  bigQuery,
                  keepStagingFiles,
                  writeConfig.getFormatterMap().get(UploaderType.AVRO));
            }
        ));

    return new BufferedStreamConsumer(
        outputRecordCollector,
        onStartFunction(bigQuery, storageOperations, namingResolver, writeConfigs),
        new SerializedBufferingStrategy(
            onCreateBuffer,
            catalog,
            flushBufferFunction(storageOperations, writeConfigs, catalog)),
        onCloseFunction(storageOperations, writeConfigs, uploaders),
        catalog,
        storageOperations::isValidData);
  }

  private OnStartFunction onStartFunction(final BigQuery bigQuery,
                                          final BlobStorageOperations storageOptions,
                                          final StandardNameTransformer namingResolver,
                                          final Map<AirbyteStreamNameNamespacePair, UploaderConfig> writeConfigs) {
    final Set<String> existingSchemas = new HashSet<>();

    return () -> {
      LOGGER.info("Preparing tmp tables in destination started for {} streams", writeConfigs.size());

      for (final UploaderConfig writeConfig : writeConfigs.values()) {
        final ConfiguredAirbyteStream stream = writeConfig.getConfigStream();

        final BigQueryRecordFormatter recordFormatter = new GcsAvroBigQueryRecordFormatter(stream.getStream().getJsonSchema(), namingResolver);
        final Schema bigQuerySchema = recordFormatter.getBigQuerySchema();

        storageOptions.createBucketObjectIfNotExists(writeConfig.getOutputBucketPath());
        BigQueryUtils.createSchemaAndTableIfNeeded(
            bigQuery,
            existingSchemas,
            writeConfig.getOutputSchema(),
            writeConfig.getTmpTableName(),
            writeConfig.getOutputDatasetLocation(),
            bigQuerySchema);
      }

      LOGGER.info("Preparing tmp tables in destination completed");
    };
  }

  private CheckedBiConsumer<AirbyteStreamNameNamespacePair, SerializableBuffer, Exception> flushBufferFunction(final BlobStorageOperations storageOperations,
                                                                                                               final Map<AirbyteStreamNameNamespacePair, UploaderConfig> writeConfigs,
                                                                                                               final ConfiguredAirbyteCatalog catalog) {
    return (stream, buffer) -> {
      LOGGER.info("Flushing buffer for stream {} ({}) to storage", stream.getName(), FileUtils.byteCountToDisplaySize(buffer.getByteCount()));
      if (!writeConfigs.containsKey(stream)) {
        throw new IllegalArgumentException(
            String.format("Message contained record from a stream %s that was not in the catalog. \ncatalog: %s", stream, Jsons.serialize(catalog)));
      }

      final UploaderConfig writeConfig = writeConfigs.get(stream);
      try (buffer) {
        buffer.flush();
        writeConfig.addStoredFile(storageOperations.uploadRecordsToBucket(
            buffer,
            writeConfig.getOutputSchema(),
            writeConfig.getStreamName(),
            writeConfig.getOutputBucketPath()));
      } catch (final Exception e) {
        LOGGER.error("Failed to flush and upload buffer to storage", e);
        throw new RuntimeException("Failed to upload buffer to storage", e);
      }
    };
  }

  private OnCloseFunction onCloseFunction(final BlobStorageOperations storageOperations,
                                          final Map<AirbyteStreamNameNamespacePair, UploaderConfig> writeConfigs,
                                          final Map<AirbyteStreamNameNamespacePair, AbstractBigQueryUploader<?>> uploaders) {
    return (hasFailed) -> {
      if (hasFailed) {
        return;
      }

      LOGGER.info("Cleaning up destination started for {} streams", uploaders.size());

      for (final Map.Entry<AirbyteStreamNameNamespacePair, AbstractBigQueryUploader<?>> entry : uploaders.entrySet()) {
        LOGGER.info("Finalizing stream {}", entry.getKey());
        final AbstractBigQueryUploader<?> uploader = entry.getValue();
        uploader.close(hasFailed, null, null);
      }

      for (final Map.Entry<AirbyteStreamNameNamespacePair, UploaderConfig> entry : writeConfigs.entrySet()) {
        LOGGER.info("Cleaning up staging area for stream {}", entry.getKey());
        final UploaderConfig writeConfig = entry.getValue();
        storageOperations.cleanUpBucketObject(writeConfig.getOutputBucketPath(), writeConfig.getStoredFiles());
      }

      LOGGER.info("Cleaning up destination completed.");
    };
  }

  private Function<ConfiguredAirbyteStream, UploaderConfig> toWriteConfig(final BlobStorageOperations storageOperations,
                                                                          final StandardNameTransformer namingResolver,
                                                                          final JsonNode config) {
    return stream -> {
      Preconditions.checkNotNull(stream.getDestinationSyncMode(), "Undefined destination sync mode");

      final AirbyteStream abStream = stream.getStream();
      final String namespace = abStream.getNamespace();
      final String streamName = abStream.getName();
      final String outputDatasetLocation = BigQueryUtils.getDatasetLocation(config);
      final String outputSchema = BigQueryUtils.getSchema(config, stream);
      final String outputBucketPath = storageOperations.getBucketObjectPath(outputSchema, streamName, SYNC_DATETIME, S3DestinationConstants.DEFAULT_PATH_FORMAT);
      final DestinationSyncMode syncMode = stream.getDestinationSyncMode();
      final UploaderConfig writeConfig = UploaderConfig
          .builder()
          .configStream(stream)
          .config(config)
          .tmpTableName(namingResolver.getTmpTableName(streamName))
          .targetTableName(namingResolver.getRawTableName(streamName))
          .formatterMap(getFormatterMap(abStream.getJsonSchema(), namingResolver))
          .namespace(namespace)
          .streamName(streamName)
          .outputDatasetLocation(outputDatasetLocation)
          .outputSchema(outputSchema)
          .outputBucketPath(outputBucketPath)
          .syncMode(syncMode)
          .build();

      LOGGER.info("Write config: {}", writeConfig);
      return writeConfig;
    };
  }

  protected Map<UploaderType, BigQueryRecordFormatter> getFormatterMap(final JsonNode jsonSchema, final StandardNameTransformer nameTransformer) {
    return Map.of(UploaderType.STANDARD, new DefaultBigQueryRecordFormatter(jsonSchema, nameTransformer),
        UploaderType.CSV, new GcsCsvBigQueryRecordFormatter(jsonSchema, nameTransformer),
        UploaderType.AVRO, new GcsAvroBigQueryRecordFormatter(jsonSchema, nameTransformer));
  }

}
