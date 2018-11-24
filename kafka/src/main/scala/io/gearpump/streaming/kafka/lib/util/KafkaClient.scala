package io.gearpump.streaming.kafka.lib.util

import io.gearpump.streaming.kafka.lib.source.consumer.KafkaConsumer
import io.gearpump.streaming.kafka.util.KafkaConfig
import kafka.admin.AdminUtils
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.gearpump.util.LogUtil
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.Serializer

object KafkaClient {
  private val LOG = LogUtil.getLogger(classOf[KafkaClient])

  val factory = new KafkaClientFactory

  class KafkaClientFactory extends java.io.Serializable {
    def getKafkaClient(config: KafkaConfig): KafkaClient = {
      val consumerConfig = config.getConsumerConfig
      val zkClient = new ZkClient(consumerConfig.zkConnect, consumerConfig.zkSessionTimeoutMs,
        consumerConfig.zkConnectionTimeoutMs, ZKStringSerializer)
      new KafkaClient(config, zkClient)
    }
  }
}

class KafkaClient(config: KafkaConfig, zkClient: ZkClient) {
  import KafkaClient._

  private val consumerConfig = config.getConsumerConfig

  def getTopicAndPartitions(consumerTopics: List[String]): Array[TopicAndPartition] = {
    try {
      ZkUtils.getPartitionsForTopics(zkClient, consumerTopics).flatMap {
        case (topic, partitions) => partitions.map(TopicAndPartition(topic, _))
      }.toArray
    } catch {
      case e: Exception =>
        LOG.error(e.getMessage)
        throw e
    }
  }

  def getBroker(topic: String, partition: Int): Broker = {
    try {
      val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
        .getOrElse(throw new RuntimeException(
          s"leader not available for TopicAndPartition($topic, $partition)"))
      ZkUtils.getBrokerInfo(zkClient, leader)
        .getOrElse(throw new RuntimeException(s"broker info not found for leader $leader"))
    } catch {
      case e: Exception =>
        LOG.error(e.getMessage)
        throw e
    }
  }

  def createConsumer(topic: String, partition: Int, startOffsetTime: Long): KafkaConsumer = {
    val broker = getBroker(topic, partition)
    val soTimeout = consumerConfig.socketTimeoutMs
    val soBufferSize = consumerConfig.socketReceiveBufferBytes
    val clientId = consumerConfig.clientId
    val fetchSize = consumerConfig.fetchMessageMaxBytes
    val consumer = new SimpleConsumer(broker.host, broker.port, soTimeout, soBufferSize, clientId)
    KafkaConsumer(topic, partition, startOffsetTime, fetchSize, consumer)
  }

  def createProducer[K, V](keySerializer: Serializer[K],
      valueSerializer: Serializer[V]): KafkaProducer[K, V] = {
    new KafkaProducer[K, V](config.getProducerConfig, keySerializer, valueSerializer)
  }

  /**
   * create a new kafka topic
   * return true if topic already exists, and false otherwise
   */
  def createTopic(topic: String, partitions: Int, replicas: Int): Boolean = {
    try {
      if (AdminUtils.topicExists(zkClient, topic)) {
        LOG.info(s"topic $topic exists")
        true
      } else {
        AdminUtils.createTopic(zkClient, topic, partitions, replicas)
        LOG.info(s"created topic $topic")
        false
      }
    } catch {
      case e: Exception =>
        LOG.error(e.getMessage)
        throw e
    }
  }

  def close(): Unit = {
    zkClient.close()
  }
}

