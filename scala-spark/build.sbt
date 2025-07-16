import sbt._

val sparkScala3Compat: String = "0.2.6"
val sparkVersion: String = "3.5.0"
val deltaVersion: String = "3.2.1"
val hadoopVersion: String = "3.3.4"

lazy val scala_spark = (project in file("."))
  .settings(
    name := "spark-iceberg-nessie",
    scalaVersion := "3.3.6",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      ("io.github.vincenzobaz" %% "spark-scala3-encoders" % sparkScala3Compat),
      ("io.github.vincenzobaz" %% "spark-scala3-udf" % sparkScala3Compat),
      // Spark Core and SQL
      ("org.apache.spark" %% "spark-core" % sparkVersion % Provided)
        .cross(CrossVersion.for3Use2_13),
      ("org.apache.spark" %% "spark-sql" % sparkVersion % Provided)
        .cross(CrossVersion.for3Use2_13),

      // Iceberg
      ("org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.4.3" % Provided)
        .cross(CrossVersion.for3Use2_13),
      "org.apache.iceberg" % "iceberg-nessie" % "1.4.3",

      // AWS S3 Support
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % Provided,
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.565" % Provided,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Provided,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,

      // Logging
      "org.slf4j" % "slf4j-api" % "1.7.36",
      "ch.qos.logback" % "logback-classic" % "1.2.12",
      "com.lihaoyi" %% "mainargs" % "0.7.6",
      "com.typesafe" % "config" % "1.4.4",
      "org.scalameta" %% "munit" % "1.0.4" % Test
    )
  )
fork := true

// Configure Scala 3 compatibility with Spark 2.13 dependencies
scalacOptions ++= Seq(
  "-Ykind-projector:underscores",
  "-Wunused:all",
  "-Wconf:cat=unused:ws",
  "-Xsource:3"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*)        => MergeStrategy.discard
  case x: String                            => MergeStrategy.first
}

Test / javaOptions ++= Seq("--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")
