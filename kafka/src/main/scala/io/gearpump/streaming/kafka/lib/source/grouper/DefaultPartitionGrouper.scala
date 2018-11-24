package io.gearpump.streaming.kafka.lib.source.grouper

import kafka.common.TopicAndPartition

/**
 * default grouper groups TopicAndPartitions among StreamProducers by partitions
 *
 * e.g. given 2 topics (topicA with 2 partitions and topicB with 3 partitions) and
 * 2 streamProducers (streamProducer0 and streamProducer1)
 *
 * streamProducer0 gets (topicA, partition1), (topicB, partition1) and (topicA, partition3)
 * streamProducer1 gets (topicA, partition2), (topicB, partition2)
 */
class DefaultPartitionGrouper extends PartitionGrouper {
  def group(taskNum: Int, taskIndex: Int, topicAndPartitions: Array[TopicAndPartition])
    : Array[TopicAndPartition] = {
    topicAndPartitions.indices.filter(_ % taskNum == taskIndex)
      .map(i => topicAndPartitions(i)).toArray
  }
}
