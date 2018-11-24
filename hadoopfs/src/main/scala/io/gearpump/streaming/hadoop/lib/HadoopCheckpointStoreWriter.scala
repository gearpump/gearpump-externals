package io.gearpump.streaming.hadoop.lib

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.gearpump.Time.MilliSeconds

class HadoopCheckpointStoreWriter(path: Path, hadoopConfig: Configuration) {
  private lazy val stream = HadoopUtil.getOutputStream(path, hadoopConfig)

  def write(timestamp: MilliSeconds, data: Array[Byte]): Long = {
    stream.writeLong(timestamp)
    stream.writeInt(data.length)
    stream.write(data)
    stream.hflush()
    stream.getPos()
  }

  def close(): Unit = {
    stream.close()
  }
}
