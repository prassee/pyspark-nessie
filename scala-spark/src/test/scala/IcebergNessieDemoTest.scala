// package com.example.iceberg

// import org.apache.spark.sql.SparkSession
// import org.scalatest.BeforeAndAfterAll
// import org.scalatest.funsuite.AnyFunSuite

// class IcebergNessieDemoTest extends AnyFunSuite with BeforeAndAfterAll {

//   private var spark: SparkSession = _

//   override def beforeAll(): Unit = {
//     super.beforeAll()
//     spark = SparkSession
//       .builder()
//       .appName("IcebergNessieDemoTest")
//       .master("local[*]")
//       .config("spark.sql.warehouse.dir", "target/spark-warehouse")
//       .config(
//         "spark.sql.extensions",
//         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
//       )
//       .config(
//         "spark.sql.catalog.spark_catalog",
//         "org.apache.iceberg.spark.SparkSessionCatalog"
//       )
//       .config("spark.sql.catalog.spark_catalog.type", "hadoop")
//       .config(
//         "spark.sql.catalog.spark_catalog.warehouse",
//         "target/iceberg-warehouse"
//       )
//       .getOrCreate()

//     spark.sparkContext.setLogLevel("WARN")
//   }

//   override def afterAll(): Unit = {
//     if (spark != null) {
//       spark.stop()
//     }
//     super.afterAll()
//   }

//   test("Should create sample data successfully") {
//     import spark.implicits._

//     val employeesData = Seq(
//       (1, "John Doe", 30, 75000.0, "Engineering"),
//       (2, "Jane Smith", 25, 65000.0, "Marketing")
//     )

//     val df = employeesData.toDF("id", "name", "age", "salary", "department")

//     assert(df.count() == 2)
//     assert(df.columns.length == 5)
//     assert(df.filter($"department" === "Engineering").count() == 1)
//   }

//   test("Should create Iceberg table in local mode") {
//     import spark.implicits._

//     val testData = Seq(
//       (1, "Test User", 25, 50000.0, "IT")
//     ).toDF("id", "name", "age", "salary", "department")

//     // Create table in local catalog
//     testData.write
//       .format("iceberg")
//       .mode("overwrite")
//       .saveAsTable("test_employees")

//     val resultDF = spark.sql("SELECT * FROM test_employees")
//     assert(resultDF.count() == 1)

//     val firstRow = resultDF.collect().head
//     assert(firstRow.getString(1) == "Test User")
//     assert(firstRow.getString(4) == "IT")
//   }

//   test("Should perform aggregations correctly") {
//     import spark.implicits._

//     val testData = Seq(
//       (1, "John", 30, 75000.0, "Engineering"),
//       (2, "Jane", 25, 65000.0, "Engineering"),
//       (3, "Bob", 35, 50000.0, "Marketing")
//     ).toDF("id", "name", "age", "salary", "department")

//     val avgSalaryByDept = testData
//       .groupBy("department")
//       .agg(spark.sql.functions.avg("salary").alias("avg_salary"))
//       .collect()

//     val engineeringAvg = avgSalaryByDept
//       .find(_.getString(0) == "Engineering")
//       .get
//       .getDouble(1)

//     assert(engineeringAvg == 70000.0)
//   }
// }
