package com.example.iceberg

import com.example.iceberg.TableOperations.showTableMetadata
import mainargs.Flag
import mainargs.ParserForMethods
import mainargs.arg
import mainargs.main
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object IcebergNessieDemo:

  @main
  def olakeCdc(
      @arg(name = "table", short = 't') tableName: String,
      @arg(name = "date", short = 'd') datePath: String,
      @arg(name = "firstLoad", short = 'f') firstLoad: Flag
  ): Unit =
    try OlakeCUBPipeline.writeCDCParqToUnnest(tableName, datePath, firstLoad.value)
    catch
      case e: Exception =>
        logger.info(s"Error occurred: ${e.getMessage}")
    finally spark.stop()

  @main
  def olakeU2b(
      @arg(name = "table", short = 't') tableName: String,
      @arg(name = "date", short = 'd', doc = "Date in YYYY/MM/DD format") date: String,
      @arg(name = "cols", short = 'c') cols: String
  ): Unit =
    try
      val unnestSchemaName: String = "iceberg_db"
      val clspCols: List[String]   = cols.split(",").toList
      val unnestTable: TableName   = TableName(s"nessie.${unnestSchemaName}.${tableName}")
      val tableDf: Dataset[Row]    = spark.read
        .option("mergeSchema", "true")
        .parquet(
          s"s3a://warehouse/iceberg_db/orders_5593506a-3b3f-4fa4-9e4e-57470e94a3f1/data/updated_at_day=2022-06-04/*.parquet"
        )
      tableDf.show(5, truncate = false)
      val snapshotDf: Dataset[Row] = spark.sql(s"select * from ${unnestTable.name} where updated_at = DATE '${date}'")
      snapshotDf.show(5, truncate = false)
      logger.info(s"Starting Olake Unnesting... for table ${tableName} with cols ${clspCols}")
      logger.info(s"Unnest table: ${unnestTable.name}")
      // OlakeCUBPipeline.writeUnnestToBase(
      //   unnestTable,
      //   date,
      //   TableName(s"nessie.oms.${tableName}"),
      //   List("_cdc_timestamp", "_olake_id", "_op_type", "_olake_timestamp"),
      //   clspCols
      // )
      // logger.info("Olake Unnesting completed successfully!")
    catch
      case e: Exception =>
        logger.info(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
    finally spark.stop()

  @main
  def createNs(
      @arg(name = "catalog", short = 'c') catalog: String,
      @arg(name = "name", short = 'n') name: String,
      @arg(name = "path", short = 'p') path: String
  ): Unit =
    try
      logger.info("Starting Spark Iceberg Demo with Nessie...")
      import com.example.iceberg.TableOperations.createNamespace
      createNamespace(s"${catalog}.${name}", path)
      logger.info("Namespace created successfully!")
    catch
      case e: Exception =>
        logger.info(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
    finally spark.stop()

  @main
  def dropNs(
      @arg(name = "catalog", short = 'c') catalog: String,
      @arg(name = "name", short = 'n') name: String
  ): Unit =
    try
      logger.info("Starting Spark Iceberg Demo with Nessie...")
      import com.example.iceberg.TableOperations.dropNamespace
      dropNamespace(catalog, name)
      logger.info("Namespace dropped successfully!")
    catch
      case e: Exception =>
        logger.info(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
    finally spark.stop()

  @main
  def showNs(): Unit =
    try
      logger.info("Starting Spark Iceberg Demo with Lakekeeper...")
      import com.example.iceberg.TableOperations.showNameSpaces
      showNameSpaces()
      logger.info("Demo completed successfully!")
    catch
      case e: Exception =>
        logger.info(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
    finally spark.stop()

  @main
  def inspect(@arg(name = "name", short = 'n') tableName: String): Unit =
    try
      logger.info("Starting Spark Iceberg Demo with Nessie...")
      import com.example.iceberg.TableOperations.*
      showTableMetadata(TableName(tableName))
      logger.info("Demo completed successfully!")
    catch
      case e: Exception =>
        logger.info(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
    finally spark.stop()

  @main
  def backFill(@arg(name = "path", short = 'p') path: String, @arg(name = "table", short = 't') tableName: String): Unit =
    try
      logger.info("Starting Spark Iceberg Backfill Demo with Nessie...")
      import com.example.iceberg.TableOperations.*
      backFillTable(InputFilePath(path), TableName(tableName))
      logger.info("Backfill completed successfully!")
    catch
      case e: Exception =>
        logger.info(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
    finally spark.stop()

  @main
  def incFill(@arg(name = "path", short = 'p') path: String, @arg(name = "table", short = 't') tableName: String): Unit =
    try
      logger.info("Starting Spark Iceberg Incremental Fill Demo with Nessie...")
      import com.example.iceberg.TableOperations.*
      val incDataDf =
        spark.read
          .option("inferSchema", "true")
          .option("header", "true")
          .csv(path)
      loadIncData(incDataDf, TableName(tableName), MatchCond("target.mapid = source.mapid"))
      logger.info("Incremental fill completed successfully!")
    catch
      case e: Exception =>
        logger.info(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
    finally spark.stop()

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrExit(args)
