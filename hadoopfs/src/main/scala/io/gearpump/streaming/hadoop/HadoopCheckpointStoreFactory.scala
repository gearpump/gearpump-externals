package io.gearpump.streaming.hadoop

import java.io.{ObjectInputStream, ObjectOutputStream}

import io.gearpump.streaming.hadoop.lib.HadoopUtil
import io.gearpump.streaming.hadoop.lib.rotation.{FileSizeRotation, Rotation}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.gearpump.streaming.transaction.api.{CheckpointStore, CheckpointStoreFactory}

object HadoopCheckpointStoreFactory {
  val VERSION = 1
}

class HadoopCheckpointStoreFactory(
    dir: String,
    @transient private var hadoopConfig: Configuration,
    rotation: Rotation = new FileSizeRotation(128 * Math.pow(2, 20).toLong))
  extends CheckpointStoreFactory {
  import HadoopCheckpointStoreFactory._

  /**
   * Overrides Java's default serialization
   * Please do not remove this
   */
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    hadoopConfig.write(out)
  }

  /**
   * Overrides Java's default deserialization
   * Please do not remove this
   */
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    hadoopConfig = new Configuration(false)
    hadoopConfig.readFields(in)
  }

  override def getCheckpointStore(name: String): CheckpointStore = {
    val dirPath = new Path(dir + Path.SEPARATOR + s"v$VERSION", name)
    val fs = HadoopUtil.getFileSystemForPath(dirPath, hadoopConfig)
    new HadoopCheckpointStore(dirPath, fs, hadoopConfig, rotation)
  }
}