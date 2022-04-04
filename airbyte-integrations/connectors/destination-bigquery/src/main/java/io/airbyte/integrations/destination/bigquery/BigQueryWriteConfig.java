package io.airbyte.integrations.destination.bigquery;

import io.airbyte.integrations.destination.s3.WriteConfig;
import io.airbyte.protocol.models.DestinationSyncMode;
import java.util.List;

/**
 * Write configuration POJO for big query destinations
 */
public class BigQueryWriteConfig extends WriteConfig {

  public BigQueryWriteConfig(final String namespace,
                             final String streamName,
                             final String outputNamespace,
                             final String outputBucketPath,
                             final DestinationSyncMode syncMode) {
    super(namespace, streamName, outputNamespace, outputBucketPath, syncMode);
  }

}
