package io.gearpump.streaming.hadoop.lib.format

import org.apache.hadoop.io.Writable
import org.apache.gearpump.Message

trait OutputFormatter extends Serializable {
  def getKeyClass: Class[_ <: Writable]

  def getValueClass: Class[_ <: Writable]

  def getKey(message: Message): Writable

  def getValue(message: Message): Writable
}
