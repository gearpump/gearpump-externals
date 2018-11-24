package io.gearpump.streaming.kafka.lib.source

import com.twitter.bijection.Injection
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class DefaultKafkaMessageDecoderSpec extends PropSpec with PropertyChecks with Matchers {
  property("DefaultMessageDecoder should keep the original bytes data in Message") {
    val decoder = new DefaultKafkaMessageDecoder()
    forAll(Gen.chooseNum[Int](0, 100), Gen.alphaStr) { (k: Int, v: String) =>
      val kbytes = Injection[Int, Array[Byte]](k)
      val vbytes = Injection[String, Array[Byte]](v)
      val msgAndWmk = decoder.fromBytes(kbytes, vbytes)
      val message = msgAndWmk.message
      val watermark = msgAndWmk.watermark
      message.value shouldBe vbytes
      // processing time as message timestamp and watermark
      message.timestamp shouldBe watermark
    }
  }
}

