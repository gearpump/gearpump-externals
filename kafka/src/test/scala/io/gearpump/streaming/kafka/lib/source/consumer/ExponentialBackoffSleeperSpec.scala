package io.gearpump.streaming.kafka.lib.source.consumer

import org.scalatest.{Matchers, WordSpec}

class ExponentialBackoffSleeperSpec extends WordSpec with Matchers {

  "ExponentialBackOffSleeper" should {
    "sleep for increasing duration" in {
      val sleeper = new ExponentialBackoffSleeper(
        backOffMultiplier = 2.0,
        initialDurationMs = 100,
        maximumDurationMs = 10000
      )
      sleeper.getSleepDuration shouldBe 100
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 200
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 400
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 800
    }

    "sleep for no more than maximum duration" in {
      val sleeper = new ExponentialBackoffSleeper(
        backOffMultiplier = 2.0,
        initialDurationMs = 6400,
        maximumDurationMs = 10000
      )
      sleeper.getSleepDuration shouldBe 6400
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 10000
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 10000
    }

    "sleep for initial duration after reset" in {
      val sleeper = new ExponentialBackoffSleeper(
        backOffMultiplier = 2.0,
        initialDurationMs = 100,
        maximumDurationMs = 10000
      )
      sleeper.getSleepDuration shouldBe 100
      sleeper.setNextSleepDuration()
      sleeper.getSleepDuration shouldBe 200
      sleeper.reset()
      sleeper.getSleepDuration shouldBe 100
    }
  }
}

