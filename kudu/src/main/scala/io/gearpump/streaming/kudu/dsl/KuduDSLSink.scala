package io.gearpump.streaming.kudu.dsl

import io.gearpump.streaming.kudu.KuduSink

import scala.language.implicitConversions
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.scalaapi.Stream

/** Create a Kudu DSL Sink */
object KuduDSLSink {
  implicit def streamToHBaseDSLSink[T](stream: Stream[T]): KuduDSLSink[T] = {
    new KuduDSLSink[T](stream)
  }
}

class KuduDSLSink[T](stream: Stream[T]) {

  def writeToKudu(userConfig: UserConfig, table: String, parallelism: Int, description: String)
  : Stream[T] = {
    stream.sink(KuduSink[T](userConfig, table), parallelism, userConfig, description)
  }

}

