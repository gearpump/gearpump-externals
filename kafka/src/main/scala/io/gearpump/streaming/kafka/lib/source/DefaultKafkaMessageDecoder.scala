package io.gearpump.streaming.kafka.lib.source

import java.time.Instant

import io.gearpump.streaming.kafka.lib.{KafkaMessageDecoder, MessageAndWatermark}
import org.apache.gearpump.Message

class DefaultKafkaMessageDecoder extends KafkaMessageDecoder {

  override def fromBytes(key: Array[Byte], value: Array[Byte]): MessageAndWatermark = {
    val time = Instant.now()
    MessageAndWatermark(Message(value, time), time)
  }

}

