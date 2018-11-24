package io.gearpump.streaming.kafka.lib.source.grouper

import kafka.common.TopicAndPartition

/**
 * this class dispatches kafka kafka.common.TopicAndPartition to gearpump tasks
 */
trait PartitionGrouper {
  def group(taskNum: Int, taskIndex: Int, topicAndPartitions: Array[TopicAndPartition])
    : Array[TopicAndPartition]
}

