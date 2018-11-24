package io.gearpump.streaming.hadoop.examples

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.util.Constants._

class HadoopConfig(config: UserConfig) {

  def withHadoopConf(conf: Configuration): UserConfig = {
    config.withBytes(HADOOP_CONF, serializeHadoopConf(conf))
  }

  def hadoopConf: Configuration = deserializeHadoopConf(config.getBytes(HADOOP_CONF).get)

  private def serializeHadoopConf(conf: Configuration): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(out)
    conf.write(dataOut)
    dataOut.close()
    out.toByteArray
  }

  private def deserializeHadoopConf(bytes: Array[Byte]): Configuration = {
    val in = new ByteArrayInputStream(bytes)
    val dataIn = new DataInputStream(in)
    val result = new Configuration()
    result.readFields(dataIn)
    dataIn.close()
    result
  }
}

object HadoopConfig {
  def empty: HadoopConfig = new HadoopConfig(UserConfig.empty)
  def apply(config: UserConfig): HadoopConfig = new HadoopConfig(config)

  implicit def userConfigToHadoopConfig(userConf: UserConfig): HadoopConfig = {
    HadoopConfig(userConf)
  }
}
