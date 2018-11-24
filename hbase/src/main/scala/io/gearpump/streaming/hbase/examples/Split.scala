package io.gearpump.streaming.hbase.examples

import java.time.Instant

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.hadoop.hbase.util.Bytes

class Split extends DataSource {

  private var x = 0

  override def open(context: TaskContext, startTime: Instant): Unit = {}

  override def read(): Message = {

    val tuple = (Bytes.toBytes(s"$x"), Bytes.toBytes("group"),
      Bytes.toBytes("group:name"), Bytes.toBytes("99"))
    x+=1

    Message(tuple)
  }

  override def close(): Unit = {}

  override def getWatermark: Instant = Instant.now()

}
