"""
Sample PySpark job to create and write data to Iceberg tables using Nessie catalog
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def create_spark_session():
    """Create Spark session with Iceberg and Nessie configuration"""
    return (
        SparkSession.builder.appName("IcebergWithNessie")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def create_sample_data(spark):
    """Create sample data for testing"""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("department", StringType(), True),
            StructField("created_at", TimestampType(), True),
        ]
    )

    data = [
        (1, "John Doe", 30, 75000.0, "Engineering", None),
        (2, "Jane Smith", 25, 65000.0, "Marketing", None),
        (3, "Bob Johnson", 35, 85000.0, "Engineering", None),
        (4, "Alice Brown", 28, 70000.0, "Sales", None),
        (5, "Charlie Wilson", 32, 80000.0, "Engineering", None),
    ]

    df = spark.createDataFrame(data, schema)
    df = df.withColumn("created_at", current_timestamp())

    return df


def main():
    print("Starting PySpark Iceberg job...")

    # Create Spark session
    spark = create_spark_session()

    # # Create namespace (database)
    # print("Creating namespace 'demo'...")
    # spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.demo")

    # # Create sample data
    # print("Creating sample data...")
    # df = create_sample_data(spark)

    # # Show sample data
    # print("Sample data:")
    # df.show()

    # # Create Iceberg table
    # print("Creating Iceberg table 'employees'...")
    # df.write.format("iceberg").mode("overwrite").saveAsTable("nessie.demo.employees")

    # print("Table created successfully!")

    # # Read back the data to verify
    # print("Reading data back from Iceberg table...")
    # result_df = spark.sql("SELECT * FROM nessie.demo.employees")
    # result_df.show()

    # Show table metadata
    # print("Table metadata:")
    # spark.sql("DESCRIBE EXTENDED nessie.demo.employees").show(truncate=False)

    # # Show table location (will show the UUID path)
    # print("Table location in S3:")
    # location_df = spark.sql("SHOW TBLPROPERTIES nessie.demo.employees")
    # location_df.filter(location_df.key == "location").show(truncate=False)

    # # Append more data
    # print("Appending more data...")
    # new_data = [
    #     (6, "David Lee", 29, 72000.0, "Marketing", None),
    #     (7, "Emma Davis", 31, 78000.0, "Sales", None),
    # ]

    # new_df = spark.createDataFrame(new_data, df.schema)
    # new_df = new_df.withColumn("created_at", current_timestamp())

    # new_df.write.format("iceberg").mode("append").saveAsTable("nessie.demo.employees")

    # print("Data appended successfully!")

    # Show updated data
    # print("Updated data:")
    # spark.sql("SELECT * FROM nessie.demo.employees ORDER BY id").show()

    # Show table history
    print("Table history:")
    spark.sql("SELECT * FROM nessie.demo.employees.history").show(truncate=False)
    print("Table Files:")
    spark.sql("SELECT * FROM nessie.demo.employees.files").show(truncate=False)

    print("Job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
