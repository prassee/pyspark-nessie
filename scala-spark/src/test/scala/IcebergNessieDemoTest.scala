package com.example.iceberg

import org.apache.spark.sql.SparkSession

class IcebergNessieDemoTest:

  private val spark: SparkSession = SparkSession
    .builder()
    .appName("IcebergNessieDemoTest")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse")
    .config(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.iceberg.spark.SparkSessionCatalog"
    )
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config(
      "spark.sql.catalog.spark_catalog.warehouse",
      "target/iceberg-warehouse"
    )
    .getOrCreate()

  def test(): Unit =
    import spark.implicits.*
    import scala3encoders.given

    val employeesData = Seq(
      (1, "John Doe", 30, 75000.0, "Engineering"),
      (2, "Jane Smith", 25, 65000.0, "Marketing")
    )

    val df = employeesData.toDF("id", "name", "age", "salary", "department")

    assert(df.count() == 2)
    assert(df.columns.length == 5)
    assert(df.filter($"department" === "Engineering").count() == 1)
