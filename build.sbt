
organization := "com.iservport"

name := "tce-comb-etl"

version := "0.1.DEV"

scalaVersion := "2.11.8"

mainClass in Compile := Some("com.iservport.tce.Application")

lazy val root = (project in file("."))
  .enablePlugins(JavaServerAppPackaging)
  .enablePlugins(DockerPlugin)
  .settings(
    packageName in Docker := "iservport/tce-comb-etl",
    dockerBaseImage := "anapsix/alpine-java:latest",
    dockerUpdateLatest := true,
    dockerExposedPorts := Seq(8085),
    dockerExposedVolumes := Seq("/opt/data")
  )

libraryDependencies ++= Seq(
  "com.lightbend.akka"          %% "akka-stream-alpakka-file"       % "0.9",
  "com.lightbend.akka"          %% "akka-stream-alpakka-xml"        % "0.9",
  "org.reactivemongo"            % "reactivemongo_2.11"             % "0.12.5",
  "org.apache.spark"             % "spark-core_2.11"                % "2.2.0",
  "org.apache.spark"             % "spark-sql_2.11"                 % "2.2.0",
  "org.mongodb.spark"            % "mongo-spark-connector_2.11"     % "2.2.0",
  "com.typesafe"                 % "config"                         % "1.3.1",
  "org.slf4j"                    % "slf4j-simple"                   % "1.7.14"

)
