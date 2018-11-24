package io.gearpump.streaming.kafka.lib.source.consumer

/**
 * someone sleeps for exponentially increasing duration each time
 * until the cap
 *
 * @param backOffMultiplier The factor by which the duration increases.
 * @param initialDurationMs Time in milliseconds for initial sleep.
 * @param maximumDurationMs Cap up to which we will increase the duration.
 */
private[consumer] class ExponentialBackoffSleeper(
    backOffMultiplier: Double = 2.0,
    initialDurationMs: Long = 100,
    maximumDurationMs: Long = 10000) {

  require(backOffMultiplier > 1.0, "backOffMultiplier must be greater than 1")
  require(initialDurationMs > 0, "initialDurationMs must be positive")
  require(maximumDurationMs >= initialDurationMs, "maximumDurationMs must be >= initialDurationMs")

  private var sleepDuration = initialDurationMs

  def reset(): Unit = {
    sleepDuration = initialDurationMs
  }

  def sleep(): Unit = {
    sleep(sleepDuration)
    setNextSleepDuration()
  }

  def sleep(duration: Long): Unit = {
    Thread.sleep(duration)
  }

  def getSleepDuration: Long = sleepDuration

  def setNextSleepDuration(): Unit = {
    val next = (sleepDuration * backOffMultiplier).asInstanceOf[Long]
    sleepDuration = math.min(math.max(initialDurationMs, next), maximumDurationMs)
  }
}
