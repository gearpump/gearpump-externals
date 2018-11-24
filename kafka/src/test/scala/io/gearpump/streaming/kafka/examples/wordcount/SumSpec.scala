package io.gearpump.streaming.kafka.examples.wordcount

import java.time.Instant

import io.gearpump.streaming.kafka.util.MockUtil

import scala.collection.mutable
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.{FlatSpec, Matchers}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig

class SumSpec extends FlatSpec with Matchers {

  it should "sum should calculate the frequency of the word correctly" in {

    val stringGenerator = Gen.alphaStr
    val expectedWordCountMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

    val taskContext = MockUtil.mockTaskContext

    val sum = new Sum(taskContext, UserConfig.empty)
    sum.onStart(Instant.EPOCH)
    val str = "once two two three three three"

    var totalWordCount = 0
    stringGenerator.map { word =>
      totalWordCount += 1
      expectedWordCountMap.put(word, expectedWordCountMap.getOrElse(word, 0L) + 1)
      sum.onNext(Message(word))
    }
    verify(taskContext, times(totalWordCount)).output(anyObject[Message])

    expectedWordCountMap.foreach { wordCount =>
      val (word, count) = wordCount
      assert(count == sum.wordcount(word))
    }
  }
}
