package io.gearpump.streaming.hadoop.examples

import java.io.File
import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import akka.actor.Cancellable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile._
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}

class SeqFileStreamProcessor(taskContext: TaskContext, config: UserConfig)
  extends Task(taskContext, config) {

  import HadoopConfig._
  import SeqFileStreamProcessor._
  import taskContext.taskId

  val outputPath = new Path(config.getString(OUTPUT_PATH).get + File.separator + taskId)
  var writer: SequenceFile.Writer = null
  val textClass = new Text().getClass
  val key = new Text()
  val value = new Text()
  val hadoopConf = config.hadoopConf

  private var msgCount: Long = 0
  private var snapShotKVCount: Long = 0
  private var snapShotTime: Long = 0
  private var scheduler: Cancellable = null

  override def onStart(startTime: Instant): Unit = {

    val fs = FileSystem.get(hadoopConf)
    fs.deleteOnExit(outputPath)
    writer = SequenceFile.createWriter(hadoopConf, Writer.file(outputPath),
      Writer.keyClass(textClass), Writer.valueClass(textClass))

    scheduler = taskContext.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportStatus())
    snapShotTime = System.currentTimeMillis()
    LOG.info("sequence file bolt initiated")
  }

  override def onNext(msg: Message): Unit = {
    val kv = msg.value.asInstanceOf[String].split("\\+\\+")
    if (kv.length >= 2) {
      key.set(kv(0))
      value.set(kv(1))
      writer.append(key, value)
    }
    msgCount += 1
  }

  override def onStop(): Unit = {
    if (scheduler != null) {
      scheduler.cancel()
    }
    writer.close()
    LOG.info("sequence file bolt stopped")
  }

  private def reportStatus() = {
    val current: Long = System.currentTimeMillis()
    LOG.info(s"Task $taskId Throughput: ${
      (msgCount - snapShotKVCount,
        (current - snapShotTime) / 1000)
    } (KVPairs, second)")
    snapShotKVCount = msgCount
    snapShotTime = current
  }
}

object SeqFileStreamProcessor {
  val OUTPUT_PATH = "outputpath"
}