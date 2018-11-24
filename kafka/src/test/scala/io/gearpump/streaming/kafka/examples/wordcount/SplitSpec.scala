package io.gearpump.streaming.kafka.examples.wordcount

import com.twitter.bijection.Injection
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.TaskContext

class SplitSpec extends FlatSpec with Matchers with MockitoSugar {

  it should "split should split the text and deliver to next task" in {
    val taskContext = mock[TaskContext]
    val split = new Split(taskContext, UserConfig.empty)

    val msg = "this is a test message"
    split.onNext(Message(Injection[String, Array[Byte]](msg)))
    verify(taskContext, times(msg.split(" ").length)).output(anyObject[Message])
  }
}
