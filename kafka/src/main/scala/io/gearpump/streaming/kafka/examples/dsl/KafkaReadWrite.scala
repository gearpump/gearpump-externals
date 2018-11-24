package io.gearpump.streaming.kafka.examples.dsl

import java.util.Properties

import io.gearpump.streaming.kafka.KafkaStoreFactory
import io.gearpump.streaming.kafka.dsl.KafkaDSL
import io.gearpump.streaming.kafka.dsl.KafkaDSL._
import io.gearpump.streaming.kafka.util.KafkaConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.streaming.dsl.scalaapi.StreamApp
import org.apache.gearpump.util.AkkaApp

object KafkaReadWrite extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "source" -> CLIOption[Int]("<hom many kafka producer tasks>", required = false,
      defaultValue = Some(1)),
    "sink" -> CLIOption[Int]("<hom many kafka processor tasks>", required = false,
      defaultValue = Some(1)),
    "zookeeperConnect" -> CLIOption[String]("<zookeeper connect string>", required = false,
      defaultValue = Some("localhost:2181")),
    "brokerList" -> CLIOption[String]("<broker server list string>", required = false,
      defaultValue = Some("localhost:9092")),
    "sourceTopic" -> CLIOption[String]("<kafka source topic>", required = false,
      defaultValue = Some("topic1")),
    "sinkTopic" -> CLIOption[String]("<kafka sink topic>", required = false,
      defaultValue = Some("topic2")),
    "atLeastOnce" -> CLIOption[Boolean]("<turn on at least once source>", required = false,
      defaultValue = Some(true))
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val sourceNum = config.getInt("source")
    val sinkNum = config.getInt("sink")
    val zookeeperConnect = config.getString("zookeeperConnect")
    val brokerList = config.getString("brokerList")
    val sourceTopic = config.getString("sourceTopic")
    val sinkTopic = config.getString("sinkTopic")
    val atLeastOnce = config.getBoolean("atLeastOnce")
    val props = new Properties
    val appName = "KafkaDSL"
    props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperConnect)
    props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(KafkaConfig.CHECKPOINT_STORE_NAME_PREFIX_CONFIG, appName)

    val context = ClientContext(akkaConf)
    val app = StreamApp(appName, context)

    if (atLeastOnce) {
      val checkpointStoreFactory = new KafkaStoreFactory(props)
      KafkaDSL.createAtLeastOnceStream(app, sourceTopic, checkpointStoreFactory, props, sourceNum)
        .writeToKafka(sinkTopic, props, sinkNum)
    } else {
      KafkaDSL.createAtMostOnceStream(app, sourceTopic, props, sourceNum)
        .writeToKafka(sinkTopic, props, sinkNum)
    }

    context.submit(app)
    context.close()
  }
}
