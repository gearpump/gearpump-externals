package io.gearpump.streaming.kafka.util

import kafka.utils.{TestZKUtils, Utils, ZKStringSerializer}
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient

trait ZookeeperHarness {
  val zkConnect: String = TestZKUtils.zookeeperConnect
  val zkConnectionTimeout = 60000
  val zkSessionTimeout = 60000
  private var zookeeper: EmbeddedZookeeper = null
  private var zkClient: ZkClient = null

  def getZookeeper: EmbeddedZookeeper = zookeeper

  def getZkClient: ZkClient = zkClient

  def setUp() {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    zkClient = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
  }

  def tearDown() {
    zkClient.close()
    Utils.swallow(zookeeper.shutdown())
  }
}
