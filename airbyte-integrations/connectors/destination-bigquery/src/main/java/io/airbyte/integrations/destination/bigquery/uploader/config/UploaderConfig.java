/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery.uploader.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.BigQuery;
import io.airbyte.integrations.destination.bigquery.formatter.BigQueryRecordFormatter;
import io.airbyte.integrations.destination.bigquery.uploader.UploaderType;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class UploaderConfig {

  private JsonNode config;
  private ConfiguredAirbyteStream configStream;
  private String targetTableName;
  private String tmpTableName;
  private BigQuery bigQuery;
  private Map<UploaderType, BigQueryRecordFormatter> formatterMap;
  private boolean isDefaultAirbyteTmpSchema;

  private final String namespace;
  private final String streamName;
  private final String outputDatasetLocation;
  private final String outputSchema;
  private final String outputBucketPath;
  private final DestinationSyncMode syncMode;
  private final List<String> storedFiles = new ArrayList<>();

  public void addStoredFile(final String file) {
    storedFiles.add(file);
  }

}
