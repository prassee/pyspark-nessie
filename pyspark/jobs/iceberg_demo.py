"""
Sample PySpark job to create and write data to Iceberg tables using Nessie catalog
"""

from pyspark.sql import SparkSession


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
        .config("spark.driver.memory", "2g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def load_matches_data(spark: SparkSession):
    import pyspark.sql.functions as F

    matches = spark.read.csv("s3a://stage/matches.csv", header=True, inferSchema=True)
    matches = matches.withColumn("obs_year", F.year("completion_date")).withColumn(
        "obs_month", F.month("completion_date")
    )
    (
        matches.write.format("iceberg")
        # .partitionBy("obs_year", "obs_month")
        .mode("overwrite").saveAsTable("nessie.unnest_master.matches")
    )


def load_persons_data(spark: SparkSession):
    import pyspark.sql.functions as F

    users = spark.read.csv("s3a://stage/export.csv", header=True, inferSchema=True)
    users = users.withColumn("obs_year", F.year("birthDate"))
    (
        users.write.format("iceberg")
        .partitionBy("obs_year")
        .mode("overwrite")
        .saveAsTable("nessie.unnest_oms.users")
    )


def load_mf_data(spark: SparkSession, years: list[int]):
    # schemes = spark.read.csv(
    #     "s3a://stage/schemes_202508230836.csv", header=True, inferSchema=True
    # )
    # schemes.write.format("iceberg").mode("overwrite").saveAsTable(
    #     "nessie.unnest_mf.schemes"
    # )

    # securities = spark.read.csv(
    #     "s3a://stage/securities_202508230840.csv", header=True, inferSchema=True
    # )
    # securities.write.format("iceberg").mode("overwrite").saveAsTable(
    #     "nessie.unnest_mf.securities"
    # )
    import pyspark.sql.functions as F
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StructField,
        StructType,
    )

    # Define schema for the NAV CSV (adjust field names/types to match actual file)
    nav_schema = StructType(
        [
            StructField("scheme_code", IntegerType(), True),
            StructField("date", DateType(), True),
            StructField("nav", DoubleType(), True),
        ]
    )
    for year in years:
        navs = (
            spark.read.schema(nav_schema)
            .option("header", "true")
            .option("timestampFormat", "yyyy-MM-dd")
            .option("dateFormat", "yyyy-MM-dd")
            .csv(f"s3a://stage/mf/{year}.csv")
        )
        navs = navs.withColumn("obs_year", F.year("date")).withColumn(
            "obs_month", F.month("date")
        )
        navs.writeTo("nessie.unnest_mf.navs").overwritePartitions()


def curate_navs(spark: SparkSession, year: int, is_create: bool = False):
    # Implement curation logic here
    import pyspark.sql.functions as F

    navs = spark.read.table("nessie.unnest_mf.navs").filter(f"obs_year = {year}")
    navs_writer = (
        navs.groupBy("scheme_code")
        .agg(F.collect_list("nav").alias("navs"), F.collect_list("date").alias("dates"))
        .withColumn("year", F.lit(year))
        .join(
            spark.read.table("nessie.unnest_mf.schemes"),
            "scheme_code",
            "left",
        )
        .writeTo("nessie.mf.curated_navs")
    )
    if is_create:
        navs_writer.using("iceberg").partitionedBy("year").createOrReplace()
    else:
        navs_writer.overwritePartitions()


def curate_daily_navs(spark: SparkSession, year: int, month: int, day: int):
    # Implement curation logic here
    import pyspark.sql.functions as F

    incr_navs = (
        spark.read.table("nessie.unnest_mf.navs")
        .filter(f"obs_year = {year} AND obs_month = {month} AND day(date) = {day}")
        .groupBy("scheme_code")
        .agg(F.collect_list("nav").alias("navs"), F.collect_list("date").alias("dates"))
        .withColumn("year", F.lit(year))
        .join(
            spark.read.table("nessie.unnest_mf.schemes"),
            "scheme_code",
            "left",
        )
    )
    incr_navs.createTempView("staging_navs")
    spark.sql(
        """
        MERGE INTO nessie.mf.curated_navs AS target
        USING staging_navs AS source
        ON target.scheme_code = source.scheme_code
        AND target.year = source.year
        AND target.scheme_code = source.scheme_code
        WHEN MATCHED THEN UPDATE SET
        target.navs = array_union(target.navs, source.navs),
        target.dates = array_union(target.dates, source.dates)
        WHEN NOT MATCHED THEN INSERT (scheme_code, navs, dates, year, scheme_name) VALUES
        (source.scheme_code, source.navs, source.dates, source.year, source.scheme_name)
        """
    )
    # navs = spark.read.table("nessie.mf.curated_navs")


def partition_matches_data(spark: SparkSession):
    (
        spark.sql("""select * from nessie.unnest_master.matches""")
        .write.partitionBy("obs_year", "obs_month")
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("nessie.unnest_master.matches")
    )


def load_matches_data_partitioned(spark: SparkSession):
    matches_query = """select * from nessie.oms.matches"""
    matches = spark.sql(matches_query)
    print(f"total matches count {matches.count()}")  # Trigger action to load data
    matches.show(5, False)
    matches.printSchema()


def load_incremental_data(
    spark: SparkSession,
    source_path="s3a://sdc/matches/matches-dev_year.csv",
    on_cond="target.match_id = source.match_id",
    table_name="nessie.unnest_master.matches",
):
    import pyspark.sql.functions as F

    spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")

    matches_inc = (
        spark.read.csv(source_path, header=True, inferSchema=True)
        .withColumn("obs_year", F.year("completion_date"))
        .withColumn("obs_month", F.month("completion_date"))
    )
    temp_view_name = table_name.replace(".", "_") + "_inc"

    # Check schema differences and allow schema evolution
    print("Merging incremental data with schema evolution...")

    matches_inc.createOrReplaceTempView(temp_view_name)

    # Get schema differences
    target_columns = {field.name for field in spark.table(table_name).schema.fields}
    source_columns = {field.name for field in matches_inc.schema.fields}
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
                f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type.simpleString()}"
            )

    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING {temp_view_name} AS source
        ON {on_cond}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    # Inspect schema - post merge
    updated_matches = spark.sql(f"DESCRIBE TABLE {table_name}")
    updated_matches.show(truncate=False)
    print(
        f"""
        Incremental data merged. New total count: 
        {spark.sql('select * from nessie.unnest_master.matches').count()}
        """
    )


def main():
    print("Starting PySpark Iceberg job...")

    # Create Spark session
    spark = create_spark_session()
    # curate_navs(spark, 2011)
    # curate_daily_navs(spark, 2014, 1, 1)
    curate_daily_navs(spark, 2014, 1, 2)
    # load_incremental_data(spark)
    # load_matches_data_partitioned(spark)
    # load_persons_data(spark)
    # load_mf_data(
    #     spark,
    #     years=[
    #         2011,
    #         2012,
    #         2013,
    #         2014,
    #         2015,
    #         2016,
    #         2017,
    #         2018,
    #         2019,
    #         2020,
    #         2021,
    #         2022,
    #         2023,
    #         2024,
    #     ],
    # )
    # print("Job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
