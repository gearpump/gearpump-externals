package io.gearpump.streaming.twitter

import java.time.Instant

import org.apache.gearpump.streaming.task.TaskContext
import TwitterSource.{Factory, MessageListener}
import org.mockito.Mockito._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks
import twitter4j.{FilterQuery, TwitterStream}

class TwitterSourceSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  implicit val arbQuery: Arbitrary[Option[FilterQuery]] = Arbitrary {
    Gen.oneOf(None, Some(new FilterQuery()))
  }

  property("TwitterSource should properly setup, poll message and teardown") {
    forAll {
      (query: Option[FilterQuery], startTime: Long) =>
        val factory = mock[Factory]
        val stream = mock[TwitterStream]
        val listener = mock[MessageListener]

        when(factory.getTwitterStream).thenReturn(stream)
        val twitterSource = new TwitterSource(factory, query, listener)

        twitterSource.open(mock[TaskContext], Instant.ofEpochMilli(startTime))

        verify(stream).addListener(listener)
        query match {
          case Some(q) =>
            verify(stream).filter(q)
          case None =>
            verify(stream).sample()
        }

        twitterSource.read()
        verify(listener).poll()

        twitterSource.close()
        verify(stream).shutdown()
    }
  }
}
