package io.gearpump.streaming.serializer

import com.twitter.chill.KryoInjection
import org.apache.gearpump.streaming.state.api.Serializer

import scala.util.Try

class ChillSerializer[T] extends Serializer[T] {
  override def serialize(t: T): Array[Byte] =
    KryoInjection(t)

  override def deserialize(bytes: Array[Byte]): Try[T] =
    KryoInjection.invert(bytes).map(_.asInstanceOf[T])
}
