package io.gearpump.streaming.hadoop.lib

import java.io.EOFException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.gearpump.Time.MilliSeconds

class HadoopCheckpointStoreReader(
    path: Path,
    hadoopConfig: Configuration)
  extends Iterator[(MilliSeconds, Array[Byte])] {

  private val stream = HadoopUtil.getInputStream(path, hadoopConfig)
  private var nextTimeStamp: Option[MilliSeconds] = None
  private var nextData: Option[Array[Byte]] = None

  override def hasNext: Boolean = {
    if (nextTimeStamp.isDefined) {
      true
    } else {
      try {
        nextTimeStamp = Some(stream.readLong())
        val length = stream.readInt()
        val buffer = new Array[Byte](length)
        stream.readFully(buffer)
        nextData = Some(buffer)
        true
      } catch {
        case e: EOFException =>
          close()
          false
        case e: Exception =>
          close()
          throw e
      }
    }
  }

  override def next(): (MilliSeconds, Array[Byte]) = {
    val timeAndData = for {
      time <- nextTimeStamp
      data <- nextData
    } yield (time, data)
    nextTimeStamp = None
    nextData = None
    timeAndData.get
  }

  def close(): Unit = {
    stream.close()
  }
}
