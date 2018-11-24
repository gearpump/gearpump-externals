package io.gearpump.streaming.kudu.examples

import java.time.Instant

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.task.TaskContext

class Split extends DataSource {

  private var x: Long = 0

  override def open(context: TaskContext, startTime: Instant): Unit = {}

  override def read(): Message = {

    val tuple = ("column1" -> s"value$x", "column2" -> s"value2$x")
    x+=1

    Message(tuple)
  }

  override def close(): Unit = {}

  override def getWatermark: Instant = Instant.now()

}
