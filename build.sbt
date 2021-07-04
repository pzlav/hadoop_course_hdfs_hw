name := "hdfs_hw1"

version := "0.1"

scalaVersion := "2.12.8"


libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % "3.2.1",
      "ch.qos.logback" % "logback-classic" % "1.2.3" % Runtime
)

