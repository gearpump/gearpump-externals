import mill.scalalib.{DepSyntax, PublishModule, SbtModule}
import mill.scalalib.publish.{Developer, License, PomSettings, VersionControl}


trait External extends SbtModule with PublishModule {
  def scalaVersion = "2.11.8"

  def publishVersion = "0.8.5-SNAPSHOT"

  def pomSettings = PomSettings(
    description = artifactName(),
    organization = "io.gearpump",
    url = "https://github.com/gearpump/gearpump-externals",
    licenses = Seq(License.`Apache-2.0`),
    versionControl = VersionControl.github("gearpump", "gearpump-externals"),
    developers = Seq(
      Developer("gearpump", "Gearpump", "https://github.com/gearpump")
    )
  )

  val gearpumpVersion = "0.8.5-SNAPSHOT"
  val gearpumpDeps = Agg(
    ivy"org.apache.gearpump::gearpump-core:$gearpumpVersion",
    ivy"org.apache.gearpump::gearpump-streaming:$gearpumpVersion"
  )

  def compileIvyDeps = gearpumpDeps

  def scalaLibraryIvyDeps = T { Agg.empty }

  trait test extends Tests {
    def ivyDeps = gearpumpDeps ++ Agg(
      ivy"com.typesafe.akka::akka-testkit:2.5.18",
      ivy"org.scalatest::scalatest:2.2.0",
      ivy"org.scalacheck::scalacheck:1.11.3",
      ivy"org.mockito:mockito-core:1.10.17"
    )

    def testFrameworks = Seq("org.scalatest.tools.Framework")
  }
}


object hadoopfs extends External {

  val hadoopVersion = "2.6.0"

  override def artifactName = "gearpump-externals-hadoopfs"

  def compileIvyDeps = super.compileIvyDeps() ++ Agg(
    ivy"org.apache.hadoop:hadoop-common:$hadoopVersion",
    ivy"org.apache.hadoop:hadoop-hdfs:$hadoopVersion"
  )

  object test extends super.test {

    def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"org.apache.hadoop:hadoop-common:$hadoopVersion"
    )
  }
}

object hbase extends External {

  override def artifactName = "gearpump-externals-hbase"

  val hadoopVersion = "2.6.0"
  val hbaseVersion = "1.0.0"
  val jacksonVersion = "1.9.13"

  def compileIvyDeps = super.compileIvyDeps() ++ Agg(
    ivy"org.apache.hadoop:hadoop-common:$hadoopVersion",
    ivy"org.apache.hadoop:hadoop-hdfs:$hadoopVersion",
    ivy"org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion",
    ivy"org.codehaus.jackson:jackson-core-asl:$jacksonVersion",
    ivy"org.codehaus.jackson:jackson-mapper-asl:$jacksonVersion",
  )

  def ivyDeps = Agg(
    ivy"org.apache.hbase:hbase-client:$hbaseVersion",
    ivy"org.apache.hbase:hbase-common:$hbaseVersion"
  )

  object test extends super.test
}

object kafka extends External {

  override def artifactName = "gearpump-externals-kafka"

  val kafkaVersion = "0.8.2.1"

  def ivyDeps = Agg(
    ivy"org.apache.kafka::kafka:$kafkaVersion",
    ivy"com.twitter::bijection-core:0.8.0",
  )

  object test extends super.test {

    def ivyDeps = super.ivyDeps() ++ Agg(
      ivy"org.apache.kafka::kafka:$kafkaVersion;classifier=test",
      ivy"org.apache.gearpump::gearpump-core:$gearpumpVersion;classifier=tests"
    )
  }
}

object kudu extends External {

  override def artifactName = "gearpump-externals-kudu"

  def ivyDeps = Agg(
    ivy"org.apache.kudu:kudu-client:1.7.0"
  )

  object test extends super.test
}

object monoid extends External {

  override def artifactName = "gearpump-externals-monoid"

  def ivyDeps = Agg(
    ivy"com.twitter::algebird-core:0.13.5"
  )
}


object serializer extends External {

  override def artifactName = "gearpump-externals-serializer"

  def ivyDeps = Agg(
    ivy"com.twitter::chill-bijection:0.6.0"
      .excludeOrg("com.esotericsoftware.kryo", "com.esotericsoftware.minlog")
  )
}


object twitter extends External {

  override def artifactName = "gearpump-externals-twitter"

  def ivyDeps = Agg(
    ivy"org.twitter4j:twitter4j-stream:4.0.4"
  )

  object test extends super.test
}