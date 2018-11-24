package io.gearpump.streaming.kafka;

import io.gearpump.streaming.kafka.lib.source.consumer.FetchThread;
import io.gearpump.streaming.kafka.lib.util.KafkaClient;
import io.gearpump.streaming.kafka.util.KafkaConfig;
import kafka.common.TopicAndPartition;
import io.gearpump.streaming.kafka.lib.source.AbstractKafkaSource;
import org.apache.gearpump.streaming.transaction.api.CheckpointStore;
import org.apache.gearpump.streaming.transaction.api.TimeReplayableSource;

import java.util.Properties;

/**
 * USER API for kafka source connector.
 * Please refer to {@link AbstractKafkaSource} for detailed descriptions and implementations.
 */
public class KafkaSource extends AbstractKafkaSource implements TimeReplayableSource {

  public KafkaSource(String topic, Properties properties) {
    super(topic, properties);
  }

  // constructor for tests
  KafkaSource(String topic, Properties properties,
      KafkaConfig.KafkaConfigFactory configFactory,
      KafkaClient.KafkaClientFactory clientFactory,
      FetchThread.FetchThreadFactory threadFactory) {
    super(topic, properties, configFactory, clientFactory, threadFactory);
  }

  /**
   * for tests only
   */
  protected void addPartitionAndStore(TopicAndPartition tp, CheckpointStore store) {
    addCheckpointStore(tp, store);
  }
}
