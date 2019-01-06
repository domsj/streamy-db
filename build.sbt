import sbt.Keys.libraryDependencies

name := "streamy-db"

val scioVersion = "0.7.0-beta2"
val beamVersion = "2.8.0"
val flinkVersion = "1.7.0"
val akkaVersion = "2.5.19"
val kafkaVersion = "1.1.1"

ThisBuild / organization := "domsj.streamy.db"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.8"

lazy val core = (project in file("modules/core"))
  .settings(
    name := "streamy-db-core",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.spotify" %% "scio-core" % scioVersion,
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "org.slf4j" % "slf4j-simple" % "1.7.25",
      "com.lihaoyi" %% "upickle" % "0.7.1"
    )
  )

lazy val beamRunner = (project in file("modules/runners/beam"))
  .dependsOn(core)
  .settings(
    name := "streamy-db-beam-runner",
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-kafka" % beamVersion
    )
  )

lazy val flinkRunner = (project in file("modules/runners/flink"))
  .dependsOn(core)
  .settings(
    name := "streamy-db-flink-runner",
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-core" % flinkVersion,
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
      "org.apache.flink" %% "flink-connector-kafka-0.11" % flinkVersion
    )
  )

