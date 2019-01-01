name := "streamy-db"

version := "0.1"

scalaVersion := "2.12.8"

val scioVersion = "0.7.0-beta2"
val beamVersion = "2.8.0"
val flinkVersion = "1.7.0"
val akkaVersion = "2.5.19"


libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,

  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.slf4j" % "slf4j-simple" % "1.7.25",

  "com.lihaoyi" %% "upickle" % "0.7.1",

  "org.apache.flink" % "flink-core" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka-0.11" % flinkVersion,

  "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
  "org.apache.beam" % "beam-sdks-java-io-kafka" % beamVersion,
  "com.spotify" %% "scio-core" % scioVersion,
//  "com.spotify" %% "scio-extra" % scioVersion,
//  "org.apache.beam" % "beam-sdks-java-extensions-kryo" % beamVersion,
//  "org.apache.beam" % "beam-sdks-java-extensions-euphoria" % beamVersion,
)
