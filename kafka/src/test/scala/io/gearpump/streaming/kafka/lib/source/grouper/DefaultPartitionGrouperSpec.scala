package io.gearpump.streaming.kafka.lib.source.grouper

import kafka.common.TopicAndPartition
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class DefaultPartitionGrouperSpec extends PropSpec with PropertyChecks with Matchers {
  property("KafkaDefaultGrouper should group TopicAndPartitions in a round-robin way") {
    forAll(Gen.posNum[Int], Gen.posNum[Int], Gen.posNum[Int]) {
      (topicNum: Int, partitionNum: Int, taskNum: Int) => {
        val topicAndPartitions = for {
          t <- 0.until(topicNum)
          p <- 0.until(partitionNum)
        } yield TopicAndPartition("topic" + t, p)
        0.until(taskNum).foreach { taskIndex =>
          val grouper = new DefaultPartitionGrouper
          grouper.group(taskNum, taskIndex, topicAndPartitions.toArray).forall(
            tp => topicAndPartitions.indexOf(tp) % taskNum == taskIndex)
        }
      }
    }
  }
}
