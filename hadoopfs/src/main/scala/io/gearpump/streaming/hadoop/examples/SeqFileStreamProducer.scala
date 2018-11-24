package io.gearpump.streaming.hadoop.examples

import java.time.Instant

import org.apache.gearpump.streaming.source.Watermark
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile._
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}

class SeqFileStreamProducer(taskContext: TaskContext, config: UserConfig)
  extends Task(taskContext, config) {

  import HadoopConfig._
  import SeqFileStreamProducer._
  import taskContext.output

  val value = new Text()
  val key = new Text()
  var reader: SequenceFile.Reader = _
  val hadoopConf = config.hadoopConf
  val fs = FileSystem.get(hadoopConf)
  val inputPath = new Path(config.getString(INPUT_PATH).get)

  override def onStart(startTime: Instant): Unit = {
    reader = new SequenceFile.Reader(hadoopConf, Reader.file(inputPath))
    self ! Start
    LOG.info("sequence file spout initiated")
  }

  override def onNext(msg: Message): Unit = {
    if (reader.next(key, value)) {
      output(Message(key + "++" + value))
    } else {
      reader.close()
      reader = new SequenceFile.Reader(hadoopConf, Reader.file(inputPath))
    }
    self ! Continue
  }

  override def onStop(): Unit = {
    reader.close()
  }
}

object SeqFileStreamProducer {
  def INPUT_PATH: String = "inputpath"

  val Start = Watermark(Instant.now)
  val Continue = Watermark(Instant.now)
}
