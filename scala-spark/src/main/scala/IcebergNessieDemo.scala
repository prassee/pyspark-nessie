package com.example.iceberg

import mainargs.{main, arg, ParserForMethods, Flag}
import com.example.iceberg.TableOperations.showTableMetadata

object IcebergNessieDemo:
  // backFillTable(InputFilePath("s3a://sdc/matches.csv"), TableName("nessie.master.matches"))
  // loadIncData(spark.table("nessie.master.maps"), TableName("nessie.master.maps"), MatchCond("target.mapid = source.mapid"))

  @main
  def inspect(@arg(name = "name", short = 'n') tableName: String): Unit =
    try {
      logger.info("🚀 Starting Spark Iceberg Demo with Nessie...")
      import com.example.iceberg.TableOperations.*
      showTableMetadata(TableName(tableName))
      logger.info("✅ Demo completed successfully!")
    } catch {
      case e: Exception =>
        logger.info(s"❌ Error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }

  @main
  def backFill(@arg(name = "path", short = 'p') path: String, @arg(name = "table", short = 't') tableName: String): Unit =
    try {
      logger.info("🚀 Starting Spark Iceberg Backfill Demo with Nessie...")
      import com.example.iceberg.TableOperations.*
      backFillTable(InputFilePath(path), TableName(tableName))
      logger.info("✅ Backfill completed successfully!")
    } catch {
      case e: Exception =>
        logger.info(s"❌ Error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }

  @main
  def incFill(@arg(name = "path", short = 'p') path: String, @arg(name = "table", short = 't') tableName: String): Unit =
    try {
      logger.info("🚀 Starting Spark Iceberg Incremental Fill Demo with Nessie...")
      import com.example.iceberg.TableOperations.*
      val incDataDf = spark.read.option("inferSchema", "true").option("header", "true").csv(path)
      loadIncData(incDataDf, TableName(tableName), MatchCond("target.mapid = source.mapid"))
      logger.info("✅ Incremental fill completed successfully!")
    } catch {
      case e: Exception =>
        logger.info(s"❌ Error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrExit(args)
