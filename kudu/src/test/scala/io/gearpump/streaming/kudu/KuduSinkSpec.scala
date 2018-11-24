package io.gearpump.streaming.kudu

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import KuduSink.{KuduWriter, KuduWriterFactory}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.kudu.client._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class KuduSinkSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {


  property("KuduSink should invoke KuduWriter for writing message to Kudu") {

    implicit val system = ActorSystem
    val kuduWriter = mock[KuduWriter]
    val kuduWriterFactory = mock[KuduWriterFactory]

    val userConfig = UserConfig.empty
    val tableName = "kudu"

    when(kuduWriterFactory.getKuduWriter(userConfig, tableName))
      .thenReturn(kuduWriter)

    val kuduSink = new KuduSink(userConfig, tableName, kuduWriterFactory)

    kuduSink.open(mock[TaskContext])

    val value = ("key", "value")
    val message = Message(value)
    kuduSink.write(message)
    verify(kuduWriter, atLeastOnce()).put(message.value)

    kuduSink.close()
    verify(kuduWriter).close()
  }

  property("KuduWriter should insert a row successfully") {

    val table = mock[KuduTable]
    val kuduClient = mock[KuduClient]
    val tableName = "kudu"

    when(kuduClient.openTable(tableName)).thenReturn(table)
  }
}