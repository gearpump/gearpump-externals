package io.gearpump.streaming.hadoop.examples

import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.streaming.partitioner.ShufflePartitioner
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{AkkaApp, Graph, LogUtil}

object SequenceFileIO extends AkkaApp with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  import HadoopConfig._

  override val options: Array[(String, CLIOption[Any])] = Array(
    "source" -> CLIOption[Int]("<sequence file reader number>", required = false,
      defaultValue = Some(1)),
    "sink" -> CLIOption[Int]("<sequence file writer number>", required = false,
      defaultValue = Some(1)),
    "input" -> CLIOption[String]("<input file path>", required = true),
    "output" -> CLIOption[String]("<output file directory>", required = true)
  )

  def application(config: ParseResult): StreamApplication = {
    val spoutNum = config.getInt("source")
    val boltNum = config.getInt("sink")
    val input = config.getString("input")
    val output = config.getString("output")
    val appConfig = UserConfig.empty.withString(SeqFileStreamProducer.INPUT_PATH, input)
      .withString(SeqFileStreamProcessor.OUTPUT_PATH, output)
    val hadoopConfig = appConfig.withHadoopConf(new Configuration())
    val partitioner = new ShufflePartitioner()
    val streamProducer = Processor[SeqFileStreamProducer](spoutNum)
    val streamProcessor = Processor[SeqFileStreamProcessor](boltNum)

    val app = StreamApplication("SequenceFileIO",
      Graph(streamProducer ~ partitioner ~> streamProcessor), hadoopConfig)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    val appId = context.submit(application(config))
    context.close()
  }
}
