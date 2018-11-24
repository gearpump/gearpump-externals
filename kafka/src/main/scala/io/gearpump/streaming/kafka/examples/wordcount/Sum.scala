package io.gearpump.streaming.kafka.examples.wordcount

import java.time.Instant

import com.twitter.bijection.Injection
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}

class Sum(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  import taskContext.output

  private[wordcount] var wordcount = Map.empty[String, Long]

  override def onStart(startTime: Instant): Unit = {}

  override def onNext(message: Message): Unit = {
    val word = message.value.asInstanceOf[String]
    val count = wordcount.getOrElse(word, 0L) + 1
    wordcount += word -> count
    output(Message(
      Injection[String, Array[Byte]](word) ->
        Injection[Long, Array[Byte]](count),
      message.timestamp))
  }
}