package io.airbyte.integrations.destination.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.airbyte.commons.functional.CheckedBiConsumer;
import io.airbyte.commons.functional.CheckedBiFunction;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.AirbyteStreamNameNamespacePair;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.integrations.destination.buffered_stream_consumer.BufferedStreamConsumer;
import io.airbyte.integrations.destination.buffered_stream_consumer.OnCloseFunction;
import io.airbyte.integrations.destination.buffered_stream_consumer.OnStartFunction;
import io.airbyte.integrations.destination.record_buffer.SerializableBuffer;
import io.airbyte.integrations.destination.record_buffer.SerializedBufferingStrategy;
import io.airbyte.integrations.destination.s3.BlobStorageOperations;
import io.airbyte.integrations.destination.s3.S3ConsumerFactory;
import io.airbyte.integrations.destination.s3.S3DestinationConstants;
import io.airbyte.integrations.destination.s3.WriteConfig;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryGcsConsumerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3ConsumerFactory.class);
  private static final DateTime SYNC_DATETIME = DateTime.now(DateTimeZone.UTC);
  private static final String PATH_FORMAT_FIELD = "s3_path_format";
  private static final String BUCKET_PATH_FIELD = "s3_bucket_path";

  public AirbyteMessageConsumer create(final Consumer<AirbyteMessage> outputRecordCollector,
                                       final BlobStorageOperations storageOperations,
                                       final NamingConventionTransformer namingResolver,
                                       final CheckedBiFunction<AirbyteStreamNameNamespacePair, ConfiguredAirbyteCatalog, SerializableBuffer, Exception> onCreateBuffer,
                                       final JsonNode config,
                                       final ConfiguredAirbyteCatalog catalog) {
    final List<WriteConfig> writeConfigs = createWriteConfigs(storageOperations, namingResolver, config, catalog);
    return new BufferedStreamConsumer(
        outputRecordCollector,
        onStartFunction(storageOperations, writeConfigs),
        new SerializedBufferingStrategy(
            onCreateBuffer,
            catalog,
            flushBufferFunction(storageOperations, writeConfigs, catalog)),
        onCloseFunction(storageOperations, writeConfigs),
        catalog,
        storageOperations::isValidData);
  }

  private static List<WriteConfig> createWriteConfigs(final BlobStorageOperations storageOperations,
                                                      final NamingConventionTransformer namingResolver,
                                                      final JsonNode config,
                                                      final ConfiguredAirbyteCatalog catalog) {
    return catalog.getStreams()
        .stream()
        .map(toWriteConfig(storageOperations, namingResolver, config))
        .collect(Collectors.toList());
  }

  private static Function<ConfiguredAirbyteStream, WriteConfig> toWriteConfig(
      final BlobStorageOperations storageOperations,
      final NamingConventionTransformer namingResolver,
      final JsonNode config) {
    return stream -> {
      Preconditions.checkNotNull(stream.getDestinationSyncMode(), "Undefined destination sync mode");
      final AirbyteStream abStream = stream.getStream();
      final String namespace = abStream.getNamespace();
      final String streamName = abStream.getName();
      final String outputNamespace = getOutputNamespace(abStream, config.get(BUCKET_PATH_FIELD).asText(), namingResolver);
      final String customOutputFormat =
          config.has(PATH_FORMAT_FIELD) && !config.get(PATH_FORMAT_FIELD).asText().isBlank() ? config.get(PATH_FORMAT_FIELD).asText()
              : S3DestinationConstants.DEFAULT_PATH_FORMAT;
      final String outputBucketPath = storageOperations.getBucketObjectPath(outputNamespace, streamName, SYNC_DATETIME, customOutputFormat);
      final DestinationSyncMode syncMode = stream.getDestinationSyncMode();
      final WriteConfig writeConfig = new WriteConfig(namespace, streamName, outputNamespace, outputBucketPath, syncMode);
      LOGGER.info("Write config: {}", writeConfig);
      return writeConfig;
    };
  }

  private static String getOutputNamespace(final AirbyteStream stream,
                                           final String defaultDestNamespace,
                                           final NamingConventionTransformer namingResolver) {
    return stream.getNamespace() != null
        ? namingResolver.getNamespace(stream.getNamespace())
        : namingResolver.getNamespace(defaultDestNamespace);
  }

  private OnStartFunction onStartFunction(final BlobStorageOperations storageOperations, final List<WriteConfig> writeConfigs) {
    return () -> {

    };
  }

  private static AirbyteStreamNameNamespacePair toNameNamespacePair(final WriteConfig config) {
    return new AirbyteStreamNameNamespacePair(config.getStreamName(), config.getNamespace());
  }

  private CheckedBiConsumer<AirbyteStreamNameNamespacePair, SerializableBuffer, Exception> flushBufferFunction(final BlobStorageOperations storageOperations,
                                                                                                               final List<WriteConfig> writeConfigs,
                                                                                                               final ConfiguredAirbyteCatalog catalog) {
    final Map<AirbyteStreamNameNamespacePair, WriteConfig> pairToWriteConfig =
        writeConfigs.stream()
            .collect(Collectors.toUnmodifiableMap(
                BigQueryGcsConsumerFactory::toNameNamespacePair, Function.identity()));

    return (pair, writer) -> {

    };
  }

  private OnCloseFunction onCloseFunction(final BlobStorageOperations storageOperations,
                                          final List<WriteConfig> writeConfigs) {
    return (hasFailed) -> {

    };
  }

}
