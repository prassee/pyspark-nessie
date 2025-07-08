import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from ..jobs.iceberg_demo import create_sample_data


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


class TestSparkSessionInstance:

    def test_simple_expr(self, spark):
        assert 1 + 1 == 2, "Simple expression test failed"

    def test_spark_session(self, spark):
        assert spark is not None, "Spark session should not be None"
        assert spark.version.startswith("4."), "Spark version should start with 3."

    def test_sample_data(self, spark):
        df: DataFrame = create_sample_data(spark)
        dfc = df
        assert df is not None, "DataFrame should not be None"
        assert len(df.columns) == 6, "DataFrame should have 6 columns"
        assert_df_equality(dfc, df)
