import datetime
import queries.run_query
from .Mock_data import create_mock_data
from pyspark_test import assert_pyspark_df_equal
import pandas as pd
import pytest
from pyspark import SparkContext
from uuid import uuid4
from pyspark.sql.types import StructType, StringType
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from queries.run_query import *
import pytest
from pyspark.sql import SparkSession

def testfromisodate(sql_context):
    expected_value = datetime.datetime(2020, 7, 27, 18, 18, 54, 123000, tzinfo=datetime.timezone.utc)
    print("Expect iso date to be", expected_value)
    assert fromisodate("2020-07-27T18:18:54.123Z") == expected_value

def test_spark_create_json_format(sql_context):

    # Create Data and expected values
    data = {"withColumn": [0, 0, 0], 'bigInt': [3, 4, 4], 'string': ["foo", "foo", "foo"]}
    expectedResult = 'DataFrame[Items: array<struct<withColumn:bigint,bigInt:bigint,string:string>>]'

    # Convert data to Data Frame
    df = pd.DataFrame(data)
    ddf = sql_context.createDataFrame(df)
    result = spark_create_json_format(ddf)
    assert str(result) == expectedResult


# def test_add_snapshot_metadata_columns():
#     # Start spark session
#     spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()
#     sparkContext = SparkContext.getOrCreate()
#     sqlContext = SQLContext(sparkContext)
#     entity_df = create_mock_data('Person', 2)
#     snapshot_metadata = {"guid": "", "snapshotDate": "", "tags": ["Fall Census", "June End"]}

# def test():
#     tenant_id = "bbce3932-0456-4513-9a9d-8d71f433b2af"
#     survey_type = "TWELVE_MONTH_ENROLLMENT_1"
#     STAGE = 'TST'
#     year = 2020
#     util.spark_refresh_entity_views_v2(tenant_id, survey_type, STAGE, 2020)
