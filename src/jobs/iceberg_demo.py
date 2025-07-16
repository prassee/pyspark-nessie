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


def load_matches_data(spark: SparkSession):
    import pyspark.sql.functions as F

    matches = spark.read.csv("s3a://sdc/matches.csv", header=True, inferSchema=True)
    matches = matches.withColumn("obs_year", F.year("completion_date")).withColumn(
        "obs_month", F.month("completion_date")
    )
    (
        matches.write.format("iceberg")
        # .partitionBy("obs_year", "obs_month")
        .mode("overwrite").saveAsTable("nessie.unnest_master.matches")
    )


def partition_matches_data(spark: SparkSession):
    (
        spark.sql("""select * from nessie.unnest_master.matches""")
        .write.partitionBy("obs_year", "obs_month")
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("nessie.unnest_master.matches")
    )


def load_matches_data_partitioned(spark: SparkSession):
    matches = spark.sql("""select * from nessie.unnest_master.matches""")
    print(f"total matches count {matches.count()}")  # Trigger action to load data
    matches.show(5, False)
    matches.printSchema()


def load_incremental_data(spark: SparkSession):
    import pyspark.sql.functions as F

    matches_inc = (
        spark.read.csv(
            "s3a://sdc/matches/matches-dev_year.csv", header=True, inferSchema=True
        )
        .withColumn("obs_year", F.year("completion_date"))
        .withColumn("obs_month", F.month("completion_date"))
    )

    matches = spark.sql("""select * from nessie.unnest_master.matches""")
    matches.printSchema()

    # Check schema differences and allow schema evolution
    print("Merging incremental data with schema evolution...")

    # Enable schema evolution
    spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")

    # Write incremental data with schema evolution enabled
    # Perform MERGE INTO operation
    # Create a temporary view for the incremental data
    matches_inc.createOrReplaceTempView("temp_matches_inc")

    # Get schema differences
    target_columns = set(
        [
            field.name
            for field in spark.table("nessie.unnest_master.matches").schema.fields
        ]
    )
    source_columns = set([field.name for field in matches_inc.schema.fields])
    new_columns = source_columns - target_columns

    if new_columns:
        print(f"Found new columns in source: {new_columns}")
        # Add new columns to target table with ALTER TABLE
        for col in new_columns:
            col_type = [
                field.dataType
                for field in matches_inc.schema.fields
                if field.name == col
            ][0]
            spark.sql(
                f"ALTER TABLE nessie.unnest_master.matches ADD COLUMN {col} {col_type.simpleString()}"
            )

    spark.sql(
        """
        MERGE INTO nessie.unnest_master.matches AS target
        USING temp_matches_inc AS source
        ON target.match_id = source.match_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    # Inspect schema - post merge
    updated_matches = spark.sql("DESCRIBE TABLE nessie.unnest_master.matches")
    updated_matches.show(truncate=False)
    print(
        f"Incremental data merged. New total count: {spark.sql('select * from nessie.unnest_master.matches').count()}"
    )


def main():
    print("Starting PySpark Iceberg job...")

    # Create Spark session
    spark = create_spark_session()
    load_incremental_data(spark)

    print("Job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
