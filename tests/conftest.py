from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

import pytest


@pytest.fixture(scope='session')
def sql_context():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Local Testing") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    sql_context = SQLContext(spark)
    yield sql_context
    spark.stop()
