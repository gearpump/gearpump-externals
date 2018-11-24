package io.gearpump.streaming.monoid

import com.twitter.algebird.{Group => ABGroup, Monoid => ABMonoid}
import org.apache.gearpump.streaming.state.api.{Group, Monoid}

class AlgebirdMonoid[T](monoid: ABMonoid[T]) extends Monoid[T] {
  override def plus(l: T, r: T): T = monoid.plus(l, r)

  override def zero: T = monoid.zero
}

class AlgebirdGroup[T](group: ABGroup[T]) extends Group[T] {
  override def zero: T = group.zero

  override def plus(l: T, r: T): T = group.plus(l, r)

  override def minus(l: T, r: T): T = group.minus(l, r)
}
