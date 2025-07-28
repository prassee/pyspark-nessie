package com.example.iceberg
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec

object OlakeCUBPipeline:

  def writeCDCParqToUnnest(tableName: String, datePath: String, firstLoad: Boolean = false): Unit =
    val tableDf: Dataset[Row] = spark.read
      .option("mergeSchema", "true")
      .parquet(s"s3a://cdc/olake/writes/public/${tableName}/${datePath}/*.parquet")
    if (!firstLoad)
      TableOperations.mutateUnnestTable(TableName(s"nessie.unnest_oms.${tableName}"), tableDf)
    tableDf
      .withColumn("obs_year", year(col("updated_at")))
      .withColumn("obs_month", month(col("updated_at")))
      .withColumn("obs_day", dayofmonth(col("updated_at")))
      .write
      .partitionBy("obs_year", "obs_month", "obs_day")
      .format("iceberg")
      .mode("append")
      .option("write.spark.fanout.enabled", "true")
      .option("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .option("mergeSchema", "true")
      .saveAsTable(s"nessie.unnest_oms.${tableName}")

  /** Writes data from unnest table to base table, merging based on specified columns.
    *
    * TODO
    * ====
    * - unnest table based on date and reduce full table scan
    * - move unnestTable, baseTable, colsToIgnore and clpsCols params to config
    * - this method should only take config key and the date to unnest as the parameters
    *
    * @param unnestTable
    * @param baseTable
    * @param colsToIgnore
    * @param clpsCols
    */
  def writeUnnestToBase(unnestTable: TableName, baseTable: TableName, colsToIgnore: List[String], clpsCols: List[String]): Unit =
    logger.info(s"Data from ${unnestTable} merged into ${baseTable} successfully!")
    val unnestDf: DataFrame    = spark.table(unnestTable.name)
    val windowSpec: WindowSpec = Window.partitionBy(clpsCols.map(col): _*).orderBy(col("updated_at").desc)
    val normUnnest: DataFrame  = unnestDf
      .withColumn("row_number", row_number().over(windowSpec))
      .filter(col("row_number") === 1)
      .drop(colsToIgnore: _*)
      .drop("row_number")
    TableOperations.loadIncData(
      incDataDf = normUnnest,
      fromTable = baseTable,
      matchCond = MatchCond(clpsCols.map(col => s"target.$col = source.$col").mkString(" AND "))
    )
