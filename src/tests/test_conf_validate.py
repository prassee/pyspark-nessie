import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


class TestSparkSessionInstance:

    def test_simple_expr(self, spark):
        assert 1 + 1 == 2, "Simple expression test failed"

    def test_spark_session(self, spark):
        assert spark is not None, "Spark session should not be None"
        assert spark.version.startswith("4."), "Spark version should start with 3."
