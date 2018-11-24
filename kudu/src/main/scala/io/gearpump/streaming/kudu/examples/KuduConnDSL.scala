package io.gearpump.streaming.kudu.examples

import io.gearpump.streaming.kudu.KuduSink
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.ArgumentsParser
import org.apache.gearpump.streaming.dsl.scalaapi.StreamApp
import org.apache.gearpump.util.AkkaApp

object KuduConnDSL extends AkkaApp with ArgumentsParser {
  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val map = Map[String, String]("KUDUSINK" -> "kudusink", "kudu.masters" -> "localhost",
      "KUDU_USER" -> "kudu.user", "GEARPUMP_KERBEROS_PRINCIPAL" -> "gearpump.kerberos.principal",
      "GEARPUMP_KEYTAB_FILE" -> "gearpump.keytab.file", "TABLE_NAME" -> "kudu.table.name"
    )

    val userConfig = new UserConfig(map)
    val appName = "KuduDSL"
    val context = ClientContext(akkaConf)
    val app = StreamApp(appName, context)

    app.source(new Split).sink(new KuduSink(userConfig, "impala::default.kudu_1"), 1,
      userConfig, "KuduSink" )

    context.submit(app)
    context.close()
  }
}
