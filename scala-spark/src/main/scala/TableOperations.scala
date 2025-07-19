package com.example.iceberg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Iceberg Demo with Nessie Catalog using Spark Scala Equivalent to the PySpark demo showing:
  *   - Iceberg table creation and operations
  *   - Nessie catalog integration
  *   - Schema evolution
  *   - Time travel queries
  *   - Data versioning
  */
object TableOperations:
  def backFillTable(path: InputFilePath, tableName: TableName)(implicit spark: SparkSession): Unit = {
    val maps: DataFrame = spark.read.option("inferSchema", "true").option("header", "true").csv(path.path)
    maps.write.format("iceberg").mode("overwrite").saveAsTable(tableName.name)
  }

  def loadIncData(
      incDataDf: DataFrame,
      fromTable: TableName = TableName("nessie.master.maps"),
      matchCond: MatchCond = MatchCond("target.mapid = source.mapid")
  )(implicit spark: SparkSession): Unit =
    val sourceTable: String  = fromTable.name
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
    // Merge incremental data with existing maps table
    spark.sql(s"""
        MERGE INTO ${sourceTable} AS target
        USING ${tempViewName} AS source
        ON ${matchCond.condition}
        WHEN MATCHED THEN
        UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *
      """)
    logger.info("Incremental data merged into maps table with schema evolution!")

  def createNamespace(name: String)(implicit spark: SparkSession): Unit =
    logger.info(s"📁 Creating namespace '${name}'...")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS nessie.${name}")
    logger.info("📋 Available namespaces:")
    spark.sql("SHOW NAMESPACES").show()

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
