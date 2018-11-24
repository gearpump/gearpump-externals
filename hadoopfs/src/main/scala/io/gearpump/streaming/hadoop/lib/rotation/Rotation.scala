package io.gearpump.streaming.hadoop.lib.rotation

import java.time.Instant

trait Rotation extends Serializable {
  def mark(timestamp: Instant, offset: Long): Unit
  def shouldRotate: Boolean
  def rotate(): Unit
}
