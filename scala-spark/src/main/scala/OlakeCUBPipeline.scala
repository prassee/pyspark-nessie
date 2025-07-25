package com.example.iceberg
import org.apache.spark.sql.functions._

object OlakeCUBPipeline:

  def writeUnnest(tableName: String, datePath: String): Unit =
    val tableDf = spark.read.parquet(s"s3a://cdc/public/${tableName}/${datePath}/*.parquet")
    tableDf
      .withColumn("obs_year", year(col("created_at")))
      .withColumn("obs_month", month(col("created_at")))
      .withColumn("obs_day", dayofmonth(col("created_at")))
      .write
      .partitionBy("obs_year", "obs_month", "obs_day")
      .format("iceberg")
      .mode("overwrite")
      .option("write.spark.fanout.enabled", "true")
      .option("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .option("mergeSchema", "true")
      .saveAsTable(s"nessie.unnest_oms.${tableName}")
