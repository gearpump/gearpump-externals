package io.gearpump.streaming.hbase

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import HBaseSink.{HBaseWriter, HBaseWriterFactory}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.mockito.{ArgumentMatcher, Matchers => MockitoMatchers}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class HBaseSinkSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {


  property("HBaseSink should invoke HBaseWriter for writing message to HBase") {

    val hbaseWriter = mock[HBaseWriter]
    val hbaseWriterFactory = mock[HBaseWriterFactory]

    implicit val system: ActorSystem = ActorSystem()

    val userConfig = UserConfig.empty
    val tableName = "hbase"

    when(hbaseWriterFactory.getHBaseWriter(userConfig, tableName))
      .thenReturn(hbaseWriter)

    val hbaseSink = new HBaseSink(userConfig, tableName, hbaseWriterFactory)

    hbaseSink.open(mock[TaskContext])

    forAll(Gen.alphaStr) { (value: String) =>
      val message = Message(value)
      hbaseSink.write(message)
      verify(hbaseWriter, atLeastOnce()).put(value)
    }

    hbaseSink.close()
    verify(hbaseWriter).close()
  }

  property("HBaseWriter should insert a row successfully") {

    val table = mock[Table]
    val config = mock[Configuration]
    val connection = mock[Connection]
    val taskContext = mock[TaskContext]

    val map = Map[String, String]("HBASESINK" -> "hbasesink", "TABLE_NAME" -> "hbase.table.name",
      "COLUMN_FAMILY" -> "hbase.table.column.family", "COLUMN_NAME" -> "hbase.table.column.name",
      "HBASE_USER" -> "hbase.user", "GEARPUMP_KERBEROS_PRINCIPAL" -> "gearpump.kerberos.principal",
      "GEARPUMP_KEYTAB_FILE" -> "gearpump.keytab.file"
    )
    val userConfig = new UserConfig(map)
    val tableName = "hbase"
    val row = "row"
    val group = "group"
    val name = "name"
    val value = "3.0"

    when(connection.getTable(TableName.valueOf(tableName))).thenReturn(table)

    val put = new Put(Bytes.toBytes(row))
    put.addColumn(Bytes.toBytes(group), Bytes.toBytes(name), Bytes.toBytes(value))

    val hbaseWriter = new HBaseWriter(connection, tableName)
    hbaseWriter.insert(Bytes.toBytes(row), Bytes.toBytes(group), Bytes.toBytes(name),
      Bytes.toBytes(value))

    verify(table).put(argMatch[Put](_.getRow sameElements Bytes.toBytes(row)))

    def argMatch[T](func: T => Boolean): T = {
      MockitoMatchers.argThat(new ArgumentMatcher[T] {
        override def matches(param: Any): Boolean = {
          val message = param.asInstanceOf[T]
          func(message)
        }
      })
    }
  }
}
