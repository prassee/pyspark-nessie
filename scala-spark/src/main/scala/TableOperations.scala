package com.example.iceberg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import mainargs.arg

/** Iceberg Demo with Nessie Catalog using Spark Scala Equivalent to the PySpark demo showing:
  *   - Iceberg table creation and operations
  *   - Nessie catalog integration
  *   - Schema evolution
  *   - Time travel queries
  *   - Data versioning
  */
object TableOperations:
  def backFillTable(path: InputFilePath, tableName: TableName)(implicit spark: SparkSession): Unit =
    val maps: DataFrame = spark.read.option("inferSchema", "true").option("header", "true").csv(path.path)
    maps.write.format("iceberg").mode("overwrite").saveAsTable(tableName.name)

  def mutateUnnestTable(fromTable: TableName, incDataDf: DataFrame): Unit =
    val sourceTable: String  = fromTable.name
    val tempViewName: String = sourceTable.replaceAll("\\.", "_") + "_temp"
    incDataDf.createOrReplaceTempView(tempViewName)
    val existingSchema: Set[String] = spark.sql(s"DESCRIBE ${sourceTable}").collect().map(_.getString(0)).toSet
    val incSchema: Set[String]      = spark.sql(s"DESCRIBE ${tempViewName}").collect().map(_.getString(0)).toSet
    val newColumns: Set[String]     = incSchema -- existingSchema
    logger.info(s"New columns to add: ${newColumns.mkString(", ")}")
    newColumns.foreach { column =>
      val columnType = spark.sql(s"DESCRIBE ${tempViewName}").filter(col("col_name") === column).collect()(0).getString(1)
      spark.sql(s"ALTER TABLE ${sourceTable} ADD COLUMN $column $columnType")
      logger.info(s"Added new column: $column ($columnType)")
    }
    // spark.sql("drop temporary view if exists " + tempViewName)

  def loadIncData(incDataDf: DataFrame, fromTable: TableName, matchCond: MatchCond)(implicit spark: SparkSession): Unit =
    val sourceTable: String  = fromTable.name
    val tableExists: Boolean = spark.catalog.tableExists(sourceTable)
    if (!tableExists) then
      logger.error(s"Checked for existence of Table ${sourceTable} and does not exist")
      incDataDf
        .drop("obs_year", "obs_month", "obs_day")
        .withColumn("obs_year", year(col("created_at")))
        .withColumn("obs_month", month(col("created_at")))
        .withColumn("obs_day", dayofmonth(col("created_at")))
        .write
        .partitionBy("obs_year", "obs_month", "obs_day")
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(sourceTable)
    else
      val tempViewName: String = sourceTable.replaceAll("\\.", "_") + "_temp"
      incDataDf.createOrReplaceTempView(tempViewName)
      // First, evolve schema to add new columns if they don't exist
      val existingSchema: Set[String] = spark.sql(s"DESCRIBE ${sourceTable}").collect().map(_.getString(0)).toSet
      val incSchema: Set[String]      = spark.sql(s"DESCRIBE ${tempViewName}").collect().map(_.getString(0)).toSet
      val newColumns: Set[String]     = incSchema -- existingSchema
      logger.info(s"New columns to add: ${newColumns.mkString(", ")}")
      newColumns.foreach { column =>
        val columnType = spark.sql(s"DESCRIBE ${tempViewName}").filter(col("col_name") === column).collect()(0).getString(1)
        spark.sql(s"ALTER TABLE ${sourceTable} ADD COLUMN $column $columnType")
        logger.info(s"Added new column: $column ($columnType)")
      }
      // Merge incremental data with existing table
      val query = s"""
        MERGE INTO ${sourceTable} AS target
        USING ${tempViewName} AS source
        ON ${matchCond.condition}
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
      """
      logger.info(s"Executing query: $query")
      spark.sql(query)
      logger.info("Incremental data merged from unnest to base table with schema evolution!")

  def createNamespace(name: String, path: String)(implicit spark: SparkSession): Unit =
    logger.info(s"📁 Creating namespace '$name' on location $path")
    // Use the provided path as the namespace location, even if it differs from the default warehouse location
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $name LOCATION '$path'")
    showNameSpaces()

  def showNameSpaces(catalogName: String = "nessie")(implicit spark: SparkSession): Unit =
    logger.info("📋 Available namespaces:")
    spark.sql(s"use ${catalogName}").show(truncate = false)
    spark.sql(s"SHOW SCHEMAS IN ${catalogName}").collect().map(_.getString(0)).foreach { schema =>
      logger.info(s"Tables in schema '$schema':")
      spark.sql(s"SHOW TABLES IN $catalogName.$schema").show(truncate = false)
    }

  def dropNamespace(catalogName: String, namespace: String)(implicit spark: SparkSession): Unit =
    logger.info(s"🗑️ Dropping namespace '$namespace'")
    // First drop all tables in the namespace
    val tables = spark.sql(s"SHOW TABLES IN $catalogName.$namespace").collect()
    tables.foreach { row =>
      val tableName = row.getString(1) // table name is in the second column
      logger.info(s"Dropping table: $catalogName.$namespace.$tableName")
      spark.sql(s"DROP TABLE IF EXISTS $catalogName.$namespace.$tableName")
    }
    // Then drop the namespace
    spark.sql(s"DROP NAMESPACE IF EXISTS ${catalogName}.$namespace")
    showNameSpaces()

  def showTableMetadata(tableName: TableName)(implicit spark: SparkSession): Unit =
    val table = tableName.name
    logger.info("🔍 Table properties:")
    spark.sql(s"SHOW TBLPROPERTIES ${table}").show(truncate = false)
    logger.info("\n📚 Table history:")
    spark.sql(s"SELECT * FROM ${table}.history").show(truncate = false)
    logger.info("\n📂 Table snapshots:")
    spark
      .sql(s"SELECT operation,summary,committed_at,manifest_list,parent_id FROM ${table}.snapshots")
      .show(truncate = false)
    logger.info("\n📁 Table files:")
    val filesDF = spark.sql(s"SELECT * FROM ${table}.files")
    logger.info(s"Number of data files: ${filesDF.count()}")
    filesDF
      .select("file_path", "file_format", "record_count", "file_size_in_bytes")
      .show(truncate = false)

  def demonstrateTimeTravel()(implicit spark: SparkSession): Unit =
    logger.info("⏰ Demonstrating time travel...")

    val snapshots = spark
      .sql("SELECT snapshot_id FROM employees.snapshots ORDER BY committed_at")
      .collect()

    if snapshots.nonEmpty then
      val firstSnapshotId = snapshots.head.getLong(0)
      logger.info(s"🕐 Querying first snapshot: $firstSnapshotId")

      val timeTravelDF = spark.sql(s"SELECT * FROM employees VERSION AS OF $firstSnapshotId")
      logger.info(s"Records in first snapshot: ${timeTravelDF.count()}")
      timeTravelDF.show()

  def performUnnest(path: InputFilePath)(implicit spark: SparkSession): Unit =
    logger.info("🔄 Performing unnest operation...")
