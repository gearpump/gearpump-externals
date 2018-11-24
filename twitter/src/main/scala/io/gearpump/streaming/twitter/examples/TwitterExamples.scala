package io.gearpump.streaming.twitter.examples

import java.time.Duration

import io.gearpump.streaming.twitter.TwitterSource
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.streaming.dsl.scalaapi.{LoggerSink, StreamApp}
import org.apache.gearpump.streaming.dsl.window.api.{EventTimeTrigger, FixedWindows}
import org.apache.gearpump.util.AkkaApp
import twitter4j.conf.ConfigurationBuilder

object TwitterExamples extends AkkaApp with ArgumentsParser {

  val CONSUMER_KEY = "consumer-key"
  val CONSUMER_SECRET = "consumer-secret"
  val TOKEN = "token"
  val TOKEN_SECRET = "token-secret"

  override val options: Array[(String, CLIOption[Any])] = Array(
    CONSUMER_KEY -> CLIOption[String]("consumer key", required = true),
    CONSUMER_SECRET -> CLIOption[String]("consumer secret", required = true),
    TOKEN -> CLIOption[String]("token", required = true),
    TOKEN_SECRET -> CLIOption[String]("token secret", required = true)
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)

    val twitterConf = new ConfigurationBuilder()
      .setOAuthConsumerKey(config.getString(CONSUMER_KEY))
      .setOAuthConsumerSecret(config.getString(CONSUMER_SECRET))
      .setOAuthAccessToken(config.getString(TOKEN))
      .setOAuthAccessTokenSecret(config.getString(TOKEN_SECRET))
      .build()

    val twitterSource = TwitterSource(twitterConf)

    val context: ClientContext = ClientContext(akkaConf)
    val app = StreamApp("TwitterExample", context)

    app.source[String](twitterSource)
      .flatMap(tweet => tweet.split("[\\s]+"))
      .filter(_.startsWith("#"))
      .map((_, 1))
      .window(FixedWindows.apply(Duration.ofMinutes(1)).triggering(EventTimeTrigger))
      .groupBy(_._1)
      .sum
      .sink(new LoggerSink)

    context.submit(app).waitUntilFinish()
    context.close()
  }

}
