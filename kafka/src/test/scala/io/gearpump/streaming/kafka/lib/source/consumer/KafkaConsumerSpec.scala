package io.gearpump.streaming.kafka.lib.source.consumer

import com.twitter.bijection.Injection
import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.{Message, MessageAndOffset}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class KafkaConsumerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {
  val messageGen = Gen.alphaStr map (msg => new Message(Injection[String, Array[Byte]](msg)))
  val messageNumGen = Gen.choose[Int](0, 1000)
  val topicAndPartitionGen = for {
    topic <- Gen.alphaStr
    partition <- Gen.choose[Int](0, Int.MaxValue)
  } yield (topic, partition)

  property("KafkaConsumer should iterate MessageAndOffset calling hasNext and next") {
    forAll(messageGen, messageNumGen, topicAndPartitionGen) {
      (message: Message, num: Int, topicAndPartition: (String, Int)) =>
        val (topic, partition) = topicAndPartition
        val consumer = mock[SimpleConsumer]
        when(consumer.earliestOrLatestOffset(TopicAndPartition(topic, partition),
          OffsetRequest.EarliestTime, -1)).thenReturn(0)
        val iterator = 0.until(num).map(index => MessageAndOffset(message, index.toLong)).iterator
        val getIterator = (offset: Long) => iterator
        val kafkaConsumer = new KafkaConsumer(consumer, topic, partition, getIterator)
        0.until(num).foreach { i =>
          kafkaConsumer.hasNext shouldBe true
          val kafkaMessage = kafkaConsumer.next
          kafkaMessage.offset shouldBe i.toLong
          kafkaMessage.key shouldBe None
        }
        kafkaConsumer.hasNext shouldBe false
    }
  }

  val startOffsetGen = Gen.choose[Long](1L, 1000L)
  property("KafkaConsumer setStartOffset should reset internal iterator") {
    forAll(topicAndPartitionGen, startOffsetGen) {
      (topicAndPartition: (String, Int), startOffset: Long) =>
        val (topic, partition) = topicAndPartition
        val consumer = mock[SimpleConsumer]
        val getIterator = mock[Long => Iterator[MessageAndOffset]]
        when(consumer.earliestOrLatestOffset(TopicAndPartition(topic, partition),
          OffsetRequest.EarliestTime, -1)).thenReturn(0)
        val kafkaConsumer = new KafkaConsumer(consumer, topic, partition, getIterator)
        kafkaConsumer.setStartOffset(startOffset)
        verify(getIterator).apply(startOffset)
    }
  }

  property("KafkaConsumer close should close SimpleConsumer") {
    forAll(topicAndPartitionGen) {
      (topicAndPartition: (String, Int)) =>
        val (topic, partition) = topicAndPartition
        val consumer = mock[SimpleConsumer]
        when(consumer.earliestOrLatestOffset(TopicAndPartition(topic, partition),
          OffsetRequest.EarliestTime, -1)).thenReturn(0)
        val getIterator = mock[Long => Iterator[MessageAndOffset]]
        val kafkaConsumer = new KafkaConsumer(consumer, topic, partition, getIterator)
        kafkaConsumer.close()
        verify(consumer).close()
    }
  }
}
