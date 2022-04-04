/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.gcs.GcsDestinationConfig;
import io.airbyte.integrations.destination.gcs.credential.GcsCredentialConfigs;
import io.airbyte.integrations.destination.record_buffer.FileBuffer;
import io.airbyte.integrations.destination.s3.S3FormatConfigs;
import io.airbyte.integrations.destination.s3.S3StorageOperations;
import io.airbyte.integrations.destination.s3.SerializedBufferFactory;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDestinationV2 extends BaseConnector implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryDestinationV2.class);
  private static final List<String> REQUIRED_PERMISSIONS = List.of(
      "storage.multipartUploads.abort",
      "storage.multipartUploads.create",
      "storage.objects.create",
      "storage.objects.delete",
      "storage.objects.get",
      "storage.objects.list");
  private static final JsonNode FORMAT_CONFIG = Jsons.deserialize("{ \"format\": { \"format_type\": \"AVRO\" } }");

  private final BigQuerySQLNameTransformer namingResolver;

  public BigQueryDestinationV2() {
    namingResolver = new BigQuerySQLNameTransformer();
  }

  public static void main(final String[] args) throws Exception {
    final Destination destination = new BigQueryDestinationV2();
    new IntegrationRunner(destination).run(args);
  }

  @Override
  public AirbyteConnectionStatus check(final JsonNode config) {
    try {
      final String datasetId = BigQueryUtils.getDatasetId(config);
      final String datasetLocation = BigQueryUtils.getDatasetLocation(config);
      final BigQuery bigquery = getBigQuery(config);

      BigQueryUtils.createSchemaTable(bigquery, datasetId, datasetLocation);
      final QueryJobConfiguration queryConfig = QueryJobConfiguration
          .newBuilder(String.format("SELECT * FROM `%s.INFORMATION_SCHEMA.TABLES` LIMIT 1;", datasetId))
          .setUseLegacySql(false)
          .build();

      final AirbyteConnectionStatus airbyteConnectionStatus = checkStorageIamPermissions(config);
      if (Status.FAILED == airbyteConnectionStatus.getStatus()) {
        return new AirbyteConnectionStatus().withStatus(Status.FAILED).withMessage(airbyteConnectionStatus.getMessage());
      }

      final ImmutablePair<Job, String> result = BigQueryUtils.executeQuery(bigquery, queryConfig);
      if (result.getLeft() != null) {
        return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
      } else {
        return new AirbyteConnectionStatus().withStatus(Status.FAILED).withMessage(result.getRight());
      }
    } catch (final Exception e) {
      LOGGER.info("Check failed.", e);
      return new AirbyteConnectionStatus().withStatus(Status.FAILED).withMessage(e.getMessage() != null ? e.getMessage() : e.toString());
    }
  }

  public AirbyteConnectionStatus checkStorageIamPermissions(final JsonNode config) {
    final JsonNode loadingMethod = config.get(BigQueryConsts.LOADING_METHOD);
    final String bucketName = loadingMethod.get(BigQueryConsts.GCS_BUCKET_NAME).asText();

    try {
      final GoogleCredentials credentials = getGoogleCredentials(config);

      final Storage storage = StorageOptions.newBuilder()
          .setProjectId(config.get(BigQueryConsts.CONFIG_PROJECT_ID).asText())
          .setCredentials(credentials)
          .build().getService();
      final List<Boolean> permissionsCheckStatusList = storage.testIamPermissions(bucketName, REQUIRED_PERMISSIONS);

      final List<String> missingPermissions = StreamUtils
          .zipWithIndex(permissionsCheckStatusList.stream())
          .filter(i -> !i.getValue())
          .map(i -> REQUIRED_PERMISSIONS.get(Math.toIntExact(i.getIndex())))
          .toList();

      if (!missingPermissions.isEmpty()) {
        LOGGER.error("Please make sure you account has all of these permissions:{}", REQUIRED_PERMISSIONS);

        return new AirbyteConnectionStatus()
            .withStatus(Status.FAILED)
            .withMessage("Could not connect to the Gcs bucket with the provided configuration. "
                + "Missing permissions: " + missingPermissions);
      }
      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);

    } catch (final Exception e) {
      LOGGER.error("Exception attempting to access the Gcs bucket: {}", e.getMessage());

      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage("Could not connect to the Gcs bucket with the provided configuration. \n" + e
              .getMessage());
    }
  }

  protected BigQuery getBigQuery(final JsonNode config) {
    final String projectId = config.get(BigQueryConsts.CONFIG_PROJECT_ID).asText();

    try {
      final BigQueryOptions.Builder bigQueryBuilder = BigQueryOptions.newBuilder();
      final GoogleCredentials credentials = getGoogleCredentials(config);
      return bigQueryBuilder
          .setProjectId(projectId)
          .setCredentials(credentials != null ? credentials : ServiceAccountCredentials.getApplicationDefault())
          .build()
          .getService();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Handle the credentials json being passed as a json object or a json object already serialized as a string.
   */
  private GoogleCredentials getGoogleCredentials(final JsonNode config) throws IOException {
    final JsonNode configCreds = config.get(BigQueryConsts.CONFIG_CREDS);
    if (configCreds == null || configCreds.asText().isEmpty()) {
      return ServiceAccountCredentials.getApplicationDefault();
    }

    final String credentialsString = config.get(BigQueryConsts.CONFIG_CREDS).isObject()
        ? Jsons.serialize(config.get(BigQueryConsts.CONFIG_CREDS))
        : config.get(BigQueryConsts.CONFIG_CREDS).asText();
    return ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialsString.getBytes(Charsets.UTF_8)));
  }

  /**
   * Strategy:
   * <p>
   * 1. Create a temporary table for each stream
   * </p>
   * <p>
   * 2. Write records to each stream directly (the bigquery client handles managing when to push the records over the network)
   * </p>
   * <p>
   * 4. Once all records have been written close the writers, so that any remaining records are flushed.
   * </p>
   * <p>
   * 5. Copy the temp tables to the final table name (overwriting if necessary).
   * </p>
   *
   * @param config  - integration-specific configuration object as json. e.g. { "username": "airbyte", "password": "super secure" }
   * @param catalog - schema of the incoming messages.
   * @return consumer that writes singer messages to the database.
   */
  @Override
  public AirbyteMessageConsumer getConsumer(final JsonNode config,
                                            final ConfiguredAirbyteCatalog catalog,
                                            final Consumer<AirbyteMessage> outputRecordCollector) {
    LOGGER.info("Config: {}", config.toString());
    final GcsDestinationConfig gcsConfig = GcsDestinationConfig.getGcsDestinationConfig(
        BigQueryUtils.getGcsAvroJsonNodeConfig(config));

    return new BigQueryGcsStagingConsumerFactory().create(
        outputRecordCollector,
        getBigQuery(config),
        new S3StorageOperations(namingResolver, gcsConfig.getS3Client(), gcsConfig),
        namingResolver,
        SerializedBufferFactory.getCreateFunction(FORMAT_CONFIG, FileBuffer::new),
        config,
        gcsConfig,
        catalog);
  }

}
