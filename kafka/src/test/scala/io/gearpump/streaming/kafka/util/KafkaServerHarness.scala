package io.gearpump.streaming.kafka.util

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.common.KafkaException
import kafka.server.{KafkaConfig => KafkaServerConfig, KafkaServer}
import kafka.utils.{TestUtils, Utils}

trait KafkaServerHarness extends ZookeeperHarness {
  val configs: List[KafkaServerConfig]
  private var servers: List[KafkaServer] = null
  private var brokerList: String = null

  def getServers: List[KafkaServer] = servers
  def getBrokerList: String = brokerList

  override def setUp() {
    super.setUp()
    if (configs.size <= 0) {
      throw new KafkaException("Must supply at least one server config.")
    }
    brokerList = TestUtils.getBrokerListStrFromConfigs(configs)
    servers = configs.map(TestUtils.createServer(_))
  }

  override def tearDown() {
    servers.foreach(_.shutdown())
    servers.foreach(_.config.logDirs.foreach(Utils.rm))
    super.tearDown()
  }

  def createTopicUntilLeaderIsElected(topic: String, partitions: Int,
      replicas: Int, timeout: Long = 10000): Map[Int, Option[Int]] = {
    val zkClient = getZkClient
    try {
      // Creates topic
      AdminUtils.createTopic(zkClient, topic, partitions, replicas, new Properties)
      // Waits until the update metadata request for new topic reaches all servers
      (0 until partitions).map { case i =>
        TestUtils.waitUntilMetadataIsPropagated(servers, topic, i, timeout)
        i -> TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, i, timeout)
      }.toMap
    } catch {
      case e: Exception => throw e
    }
  }
}
