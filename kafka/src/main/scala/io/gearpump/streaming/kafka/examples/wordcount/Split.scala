package io.gearpump.streaming.kafka.examples.wordcount

import java.time.Instant

import com.twitter.bijection.Injection
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}

class Split(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  import taskContext.output

  override def onStart(startTime: Instant): Unit = {
  }

  override def onNext(msg: Message): Unit = {
    Injection.invert[String, Array[Byte]](msg.value.asInstanceOf[Array[Byte]])
      .foreach(_.split("\\s+").foreach(
        word => output(Message(word, msg.timestamp))))
  }
}
