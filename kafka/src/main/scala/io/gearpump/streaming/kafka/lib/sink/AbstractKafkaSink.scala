package io.gearpump.streaming.kafka.lib.sink

import java.util.Properties

import org.apache.gearpump.Message
import AbstractKafkaSink.KafkaProducerFactory
import io.gearpump.streaming.kafka.util.KafkaConfig
import io.gearpump.streaming.kafka.util.KafkaConfig.KafkaConfigFactory
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

object AbstractKafkaSink {
  private val LOG = LogUtil.getLogger(classOf[AbstractKafkaSink])

  val producerFactory = new KafkaProducerFactory {
    override def getKafkaProducer(config: KafkaConfig): KafkaProducer[Array[Byte], Array[Byte]] = {
      new KafkaProducer[Array[Byte], Array[Byte]](config.getProducerConfig,
        new ByteArraySerializer, new ByteArraySerializer)
    }
  }

  trait KafkaProducerFactory extends java.io.Serializable {
    def getKafkaProducer(config: KafkaConfig): KafkaProducer[Array[Byte], Array[Byte]]
  }
}
/**
 * kafka sink connectors that invokes {{org.apache.kafka.clients.producer.KafkaProducer}} to send
 * messages to kafka queue
 */
abstract class AbstractKafkaSink private[kafka](
    topic: String,
    props: Properties,
    kafkaConfigFactory: KafkaConfigFactory,
    factory: KafkaProducerFactory) extends DataSink {
  import AbstractKafkaSink._

  def this(topic: String, props: Properties) = {
    this(topic, props, new KafkaConfigFactory, AbstractKafkaSink.producerFactory)
  }

  private lazy val config = kafkaConfigFactory.getKafkaConfig(props)
  // Lazily construct producer since KafkaProducer is not serializable
  private lazy val producer = factory.getKafkaProducer(config)

  override def open(context: TaskContext): Unit = {
    LOG.info("KafkaSink opened")
  }

  override def write(message: Message): Unit = {
    message.value match {
      case (k: Array[Byte], v: Array[Byte]) =>
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, k, v)
        producer.send(record)
        LOG.debug("KafkaSink sent record {} to Kafka", record)
      case v: Array[Byte] =>
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, v)
        producer.send(record)
        LOG.debug("KafkaSink sent record {} to Kafka", record)
      case m =>
        val errorMsg = s"unexpected message type ${m.getClass}; " +
          s"Array[Byte] or (Array[Byte], Array[Byte]) required"
        LOG.error(errorMsg)
    }
  }

  override def close(): Unit = {
    producer.close()
    LOG.info("KafkaSink closed")
  }
}

