package io.gearpump.streaming.kafka;

import io.gearpump.streaming.kafka.lib.sink.AbstractKafkaSink;
import io.gearpump.streaming.kafka.util.KafkaConfig;
import org.apache.gearpump.streaming.sink.DataSink;

import java.util.Properties;

/**
 * USER API for kafka sink connector.
 * Please refer to {@link AbstractKafkaSink} for detailed descriptions and implementations.
 */
public class KafkaSink extends AbstractKafkaSink implements DataSink {

  public KafkaSink(String topic, Properties props) {
    super(topic, props);
  }

  KafkaSink(String topic, Properties props,
      KafkaConfig.KafkaConfigFactory kafkaConfigFactory,
      KafkaProducerFactory factory) {
    super(topic, props, kafkaConfigFactory, factory);
  }
}
