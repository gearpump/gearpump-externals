package io.gearpump.streaming.hadoop.lib.rotation

import java.time.Instant

case class FileSizeRotation(maxBytes: Long) extends Rotation {

  private var bytesWritten = 0L

  override def mark(timestamp: Instant, offset: Long): Unit = {
    bytesWritten = offset
  }

  override def shouldRotate: Boolean = bytesWritten >= maxBytes

  override def rotate(): Unit = {
    bytesWritten = 0L
  }
}

