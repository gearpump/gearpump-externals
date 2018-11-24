package io.gearpump.streaming.kafka.lib.source.consumer

import kafka.common.TopicAndPartition

/**
 * wrapper over messages from kafka
 * @param topicAndPartition where message comes from
 * @param offset message offset on kafka queue
 * @param key message key, could be None
 * @param msg message payload
 */
case class KafkaMessage(topicAndPartition: TopicAndPartition, offset: Long,
    key: Option[Array[Byte]], msg: Array[Byte]) {

  def this(topic: String, partition: Int, offset: Long,
      key: Option[Array[Byte]], msg: Array[Byte]) = {
    this(TopicAndPartition(topic, partition), offset, key, msg)
  }
}

