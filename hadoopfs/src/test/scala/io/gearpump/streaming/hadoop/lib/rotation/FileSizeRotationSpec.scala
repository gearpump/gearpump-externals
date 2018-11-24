package io.gearpump.streaming.hadoop.lib.rotation

import java.time.Instant

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import org.apache.gearpump.Time.MilliSeconds

class FileSizeRotationSpec extends PropSpec with PropertyChecks with Matchers {

  val timestampGen = Gen.chooseNum[Long](0L, 1000L)
  val fileSizeGen = Gen.chooseNum[Long](1, Long.MaxValue)

  property("FileSize rotation rotates on file size") {
    forAll(timestampGen, fileSizeGen) { (timestamp: MilliSeconds, fileSize: Long) =>
      val rotation = new FileSizeRotation(fileSize)
      rotation.shouldRotate shouldBe false
      rotation.mark(Instant.ofEpochMilli(timestamp), rotation.maxBytes / 2)
      rotation.shouldRotate shouldBe false
      rotation.mark(Instant.ofEpochMilli(timestamp), rotation.maxBytes)
      rotation.shouldRotate shouldBe true
      rotation.rotate
      rotation.shouldRotate shouldBe false
    }
  }
}
