package com.example

import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

package object iceberg:
  val logger: Logger               = org.slf4j.LoggerFactory.getLogger("IcebergNessieDemo")
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("IcebergNessieScalaDemo")
    .config(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.adaptive.enabled", "false")
    .config(
      "spark.sql.catalog.nessie",
      "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
      "spark.sql.catalog.nessie.catalog-impl",
      "org.apache.iceberg.nessie.NessieCatalog"
    )
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1")
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/")
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
      "spark.hadoop.fs.s3a.impl",
      "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

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
