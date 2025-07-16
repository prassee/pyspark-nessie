package com.example.iceberg

import org.apache.spark.sql.SparkSession

/** Iceberg Demo with Nessie Catalog using Spark Scala Equivalent to the PySpark
  * demo showing:
  *   - Iceberg table creation and operations
  *   - Nessie catalog integration
  *   - Schema evolution
  *   - Time travel queries
  *   - Data versioning
  */
object IcebergNessieDemo {

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    println(s"🚀 Starting Spark Iceberg Demo with Nessie... ${spark.version}")
    // try {
    //   println("🚀 Starting Spark Iceberg Demo with Nessie...")

    //   // Create namespace
    //   createNamespace(spark)

    //   // Create and populate table
    //   val employeesDF = createSampleData(spark)
    //   createIcebergTable(spark, employeesDF)

    //   // Demonstrate basic operations
    //   performBasicOperations(spark)

    //   // Add more data
    //   appendData(spark)

    //   // Schema evolution
    //   evolveSchema(spark)

    //   // Show table history and metadata
    //   showTableMetadata(spark)

    //   // Demonstrate time travel
    //   demonstrateTimeTravel(spark)

    //   // Advanced analytics
    //   performAdvancedAnalytics(spark)

    //   println("✅ Demo completed successfully!")

    // } catch {
    //   case e: Exception =>
    //     println(s"❌ Error occurred: ${e.getMessage}")
    //     e.printStackTrace()
    //     System.exit(1)
    // } finally {
    //   spark.stop()
    // }
  }

  private def createSparkSession(): SparkSession = {
    println("🔧 Creating SparkSession with Iceberg and Nessie configuration...")

    SparkSession
      .builder()
      .appName("IcebergNessieScalaDemo")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
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
  }

  private def createNamespace(spark: SparkSession): Unit = {
    println("📁 Creating namespace 'demo'...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.demo")

    // Set default catalog and database
    spark.sql("USE nessie")
    spark.sql("USE demo")

    println("📋 Available namespaces:")
    spark.sql("SHOW NAMESPACES").show()
  }

}
