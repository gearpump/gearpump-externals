package io.gearpump.streaming.hbase.dsl

import io.gearpump.streaming.hbase.HBaseSink

import scala.language.implicitConversions
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.scalaapi.Stream

/** Create a HBase DSL Sink */
class HBaseDSLSink[T](stream: Stream[T]) {

  def writeToHbase(userConfig: UserConfig, table: String,
      parallelism: Int, description: String): Stream[T] = {
    stream.sink(HBaseSink[T](userConfig, table), parallelism, userConfig, description)
  }

}

object HBaseDSLSink {
  implicit def streamToHBaseDSLSink[T](stream: Stream[T]): HBaseDSLSink[T] = {
    new HBaseDSLSink[T](stream)
  }
}