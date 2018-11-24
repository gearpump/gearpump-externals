package io.gearpump.streaming.kafka.examples.wordcount

import scala.concurrent.Future
import scala.util.Success

import com.typesafe.config.Config
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

import org.apache.gearpump.cluster.ClientToMaster.SubmitApplication
import org.apache.gearpump.cluster.MasterToClient.SubmitApplicationResult
import org.apache.gearpump.cluster.{MasterHarness, TestUtil}

class KafkaWordCountSpec
  extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter with MasterHarness {

  before {
    startActorSystem()
  }

  after {
    shutdownActorSystem()
  }

  override def config: Config = TestUtil.DEFAULT_CONFIG

  property("KafkaWordCount should succeed to submit application with required arguments") {
    val requiredArgs = Array.empty[String]
    val optionalArgs = Array(
      "-source", "1",
      "-split", "1",
      "-sum", "1",
      "-sink", "1")

    val args = {
      Table(
        ("requiredArgs", "optionalArgs"),
        (requiredArgs, optionalArgs)
      )
    }
    val masterReceiver = createMockMaster()
    forAll(args) { (requiredArgs: Array[String], optionalArgs: Array[String]) =>
      val args = requiredArgs ++ optionalArgs

      Future {
        KafkaWordCount.main(masterConfig, args)
      }

      masterReceiver.expectMsgType[SubmitApplication](PROCESS_BOOT_TIME)
      masterReceiver.reply(SubmitApplicationResult(Success(0)))
    }
  }
}
