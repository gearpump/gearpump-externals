package io.gearpump.streaming.kafka;

import io.gearpump.streaming.kafka.lib.store.AbstractKafkaStoreFactory;
import io.gearpump.streaming.kafka.util.KafkaConfig;
import org.apache.gearpump.streaming.transaction.api.CheckpointStoreFactory;

import java.util.Properties;

/**
 * USER API for kafka store factory.
 * Please refer to {@link AbstractKafkaStoreFactory} for detailed descriptions and implementations.
 */
public class KafkaStoreFactory extends AbstractKafkaStoreFactory implements CheckpointStoreFactory {

  public KafkaStoreFactory(Properties props) {
    super(props);
  }

  /** constructor for tests */
  KafkaStoreFactory(Properties props, KafkaConfig.KafkaConfigFactory factory) {
    super(props, factory);
  }
}
