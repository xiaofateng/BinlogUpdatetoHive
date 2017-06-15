import _root_.sbt.Keys._
import _root_.sbt._

lazy val root = (project in file(".")).
  settings(
    name := "MysqltoHiveIncrement",
    version := "1.0",
    resolvers +=
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    scalaVersion := "2.10.5",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.10" % "1.6.2",
      "org.apache.spark" % "spark-sql_2.10" % "1.6.2",
      "org.apache.spark" % "spark-hive_2.10" % "1.6.2"  exclude("org.spark-project.hive","hive-metastore") exclude("org.spark-project.hive","hive-exec"),
      "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2",
      "org.apache.spark" % "spark-streaming_2.10" % "1.6.2",
      "org.apache.hbase" % "hbase-common" % "1.2.2",
      "org.apache.hbase" % "hbase-client" % "1.2.2",
      "org.apache.hbase" % "hbase-protocol" % "1.2.2",
      "com.alibaba.otter" % "canal.client" % "1.0.14",
      "redis.clients" % "jedis" % "2.7.2",
      "com.alibaba" % "fastjson" % "1.2.0",
      "com.alibaba.otter" % "canal.client" % "1.0.14",
      "org.slf4j" % "slf4j-api" % "1.7.10",
      "com.github.sgroschupf" % "zkclient" % "0.1",
      "org.apache.hive.hcatalog" % "hive-hcatalog-streaming" % "2.1.1",
      "org.apache.hive.hcatalog" % "hive-hcatalog-core" % "2.1.1",
      "org.apache.hive" % "hive-common" % "2.1.1",
      "org.apache.hadoop" % "hadoop-common" % "2.6.0"



    )
  )

