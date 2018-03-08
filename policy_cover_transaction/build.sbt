name := "policy_cover_transaction"

organization := "com.jack"

version := "1.0.0"

scalaVersion := "2.11.8"

val jacksonVersion = "2.9.1"
libraryDependencies ++= Seq(
  "org.apache.kafka"  % "kafka-clients"         % "1.0.0",
  "org.apache.kafka"  % "kafka-streams"         % "1.0.0",
  "com.lightbend"     %% "kafka-streams-scala"  % "0.1.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonVersion
)