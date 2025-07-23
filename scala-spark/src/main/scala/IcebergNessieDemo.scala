package com.example.iceberg

import mainargs.{main, arg, ParserForMethods, Flag}
import com.example.iceberg.TableOperations.showTableMetadata

object IcebergNessieDemo:

  @main
  def olakeCdc(): Unit =
    try {
      spark.read.parquet("s3a://sdc/public/customers/*.parquet").show(10, truncate = false)
      // spark.read.parquet("s3a://sdc/public/orders/*.parquet").show(10, truncate = false)
      // spark.read.parquet("s3a://sdc/public/order_items/*.parquet").show(10, truncate = false)
    } catch {
      case e: Exception =>
        logger.info(s"❌ Error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }

  @main
  def createNs(
      @arg(name = "catalog", short = 'c') catalog: String,
      @arg(name = "name", short = 'n') name: String,
      @arg(name = "path", short = 'p') path: String
  ): Unit =
    try {
      logger.info("🚀 Starting Spark Iceberg Demo with Nessie...")
      import com.example.iceberg.TableOperations.createNamespace
      createNamespace(s"${catalog}.${name}", path)
      logger.info("✅ Namespace created successfully!")
    } catch {
      case e: Exception =>
        logger.info(s"❌ Error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }

  @main
  def showNs(): Unit =
    try {
      logger.info("🚀 Starting Spark Iceberg Demo with Lakekeeper...")
      import com.example.iceberg.TableOperations.showNameSpaces
      showNameSpaces()
      logger.info("✅ Demo completed successfully!")
    } catch {
      case e: Exception =>
        logger.info(s"❌ Error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }

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
