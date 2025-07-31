package com.example

import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

package object iceberg:
  val logger: Logger = org.slf4j.LoggerFactory.getLogger("IcebergDemo")

  private val commonConfig: Map[String, String] = Map(
    "spark.sql.iceberg.handle-timestamp-without-timezone" -> "true",
    "spark.sql.extensions"                                -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.adaptive.enabled"                          -> "false",
    "spark.hadoop.fs.s3a.endpoint"                        -> "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key"                      -> "admin",
    "spark.hadoop.fs.s3a.secret.key"                      -> "password",
    "spark.hadoop.fs.s3a.path.style.access"               -> "true",
    "spark.hadoop.fs.s3a.impl"                            -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider"        -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.serializer"                                    -> "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.parquet.enableVectorizedReader"            -> "false",
    "spark.sql.parquet.writeLegacyFormat"                 -> "true"
  )

  private val lakeKeeperConfig: Map[String, String] = Map(
    "spark.sql.catalog.lakekeeper"           -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakekeeper.type"      -> "rest",
    "spark.sql.catalog.lakekeeper.uri"       -> "http://lakekeeper:8181/catalog",
    "spark.sql.catalog.lakekeeper.scope"     -> "lakekeeper",
    "spark.sql.catalog.lakekeeper.warehouse" -> "warehouse"
  ) ++ commonConfig

  lazy val lakeKeeperSpark: SparkSession = {
    val builder = SparkSession.builder().appName("IcebergLakekeeperDemo")
    lakeKeeperConfig.foreach { case (k, v) => builder.config(k, v) }
    builder.getOrCreate()
  }

  private val nessieConfig: Map[String, String] = Map(
    "spark.sql.catalog.nessie"              -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.catalog-impl" -> "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.uri"          -> "http://nessie:19120/api/v1",
    "spark.sql.catalog.nessie.ref"          -> "main",
    "spark.sql.catalog.nessie.warehouse"    -> "s3a://warehouse/"
  ) ++ commonConfig

  lazy val nessieSpark: SparkSession = {
    val builder = SparkSession.builder().appName("IcebergNessieScalaDemo")
    nessieConfig.foreach { case (k, v) => builder.config(k, v) }
    builder.getOrCreate()
  }

  implicit val spark: SparkSession = nessieSpark

  opaque type InputFilePath = String
  opaque type TableName     = String
  opaque type MatchCond     = String

  object InputFilePath:
    def apply(path: String): InputFilePath =
      if path.startsWith("s3a://") then path
      else throw new IllegalArgumentException("Path must start with s3a prefix")

  object TableName:
    def apply(name: String): TableName =
      if name.contains(".") then name
      else throw new IllegalArgumentException("Path must start with nessie prefix")

  object MatchCond:
    def apply(condition: String): MatchCond =
      if condition.nonEmpty && condition.contains("=") then condition
      else throw new IllegalArgumentException("Match condition cannot be empty")

  extension (input: InputFilePath) def path: String = input
  extension (table: TableName) def name: String     = table
  extension (cond: MatchCond) def condition: String = cond
