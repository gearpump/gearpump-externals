package io.gearpump.streaming.hadoop.lib.format

import org.apache.hadoop.io.{LongWritable, Text, Writable}
import org.apache.gearpump.Message

class DefaultSequenceFormatter extends OutputFormatter {
  override def getKey(message: Message): Writable = new LongWritable(message.timestamp.toEpochMilli)

  override def getValue(message: Message): Writable = new Text(message.value.asInstanceOf[String])

  override def getKeyClass: Class[_ <: Writable] = classOf[LongWritable]

  override def getValueClass: Class[_ <: Writable] = classOf[Text]
}
