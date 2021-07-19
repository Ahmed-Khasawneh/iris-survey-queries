import logging
import sys
import boto3
import json
from uuid import uuid4
from common import query_helpers
from queries.twelve_month_enrollment_query import run_twelve_month_enrollment_query
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from awsglue.utils import getResolvedOptions

spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()
sparkContext = SparkContext.getOrCreate()
sqlContext = SQLContext(sparkContext)
glueContext = GlueContext(sparkContext)

logger = logging.getLogger('run_query')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

def run_query():
  options = get_options()

  set_logger_level(options)

  logger.info(options)

  stage = options['stage']
  year = options['calendarYear']
  user_id = options['userId']
  tenant_id = options['tenantId']
  survey_type = options['surveyType']


  spark_refresh_entity_views_v2(tenant_id=tenant_id, survey_type=survey_type, stage=stage, year=year, user_id=user_id)

  if (survey_type == 'TWELVE_MONTH_ENROLLMENT_1' 
    or survey_type == 'TWELVE_MONTH_ENROLLMENT_2' 
    or survey_type == 'TWELVE_MONTH_ENROLLMENT_3' 
    or survey_type == 'TWELVE_MONTH_ENROLLMENT_4'):
    surveyOutput = run_twelve_month_enrollment_query(options, spark, sparkContext)
  else:
    raise Exception(f'Unsupported survey type: {survey_type}')
  
  surveyOutput.show()

  surveyOutput = spark_create_json_format(surveyOutput)

  s3_path = f"s3://{options['s3Bucket']}/{options['s3Key']}"

  surveyOutput.repartition(1).write.json(path=s3_path, mode='overwrite')

  print(f'Stored report JSON to {s3_path}')

#def convert_four_digit_year_to_two_year_range(four_digit_year):
#  year1 = str(four_digit_year[2:4])
#  year2 = str(int(year1) + 1)
#  return year1 + year2
  
def get_options():
    optionNames = [
      'debugLogging',
      'calendarYear',
      'surveyType',
      'stage',
      's3Bucket',
      's3Key',
      'tenantId',
      'userId',
    ]

    options = getResolvedOptions(sys.argv, optionNames)
#    options['calendarYear'] = convert_four_digit_year_to_two_year_range(options['calendarYear'])

    return options

def set_logger_level(options):
  if (options['debugLogging'].lower() == 'true'):
    logger.setLevel(logging.DEBUG)
    logger.debug('Debug logging enabled.')

def spark_refresh_entity_views_v2(tenant_id, survey_type, stage, year, user_id=None):
    lambda_client = boto3.client('lambda', 'us-east-1')
    invoke_response = lambda_client.invoke(
        FunctionName = "iris-connector-doris-{}-getReportPayload".format(stage),
        InvocationType = 'RequestResponse', 
        LogType = "None",
        Payload = json.dumps({ 'tenantId': tenant_id, 'surveyType': survey_type, 'stateMachineExecutionId': '', 'calendarYear': year, 'userId': user_id }).encode('utf-8')
    )
    view_metadata_without_s3_paths = json.loads(invoke_response['Payload'].read().decode("utf-8"))

    print(json.dumps(view_metadata_without_s3_paths, indent=2))

    view_metadata_without_s3_paths["tenantId"] = tenant_id
    invoke_response = lambda_client.invoke(
        FunctionName = "doris-data-access-apis-{}-GetEntitySnapshotPaths".format(stage),
        InvocationType = 'RequestResponse', 
        LogType = "None",
        Payload = json.dumps(view_metadata_without_s3_paths)
    )
    view_metadata = json.loads(invoke_response['Payload'].read().decode("utf-8"))
    snapshot_metadata = view_metadata.get('snapshotMetadata', {})

    print(json.dumps(view_metadata, indent=2))

    for view in view_metadata.get('views', []):
        s3_paths = view.get('s3Paths', [])
        view_name = view.get('viewName')
        if len(s3_paths) > 0:
            print("{}: ({})".format(view_name, ','.join(s3_paths)))
            df = spark_read_s3_source(s3_paths).toDF()
            df = add_snapshot_metadata_columns(df, snapshot_metadata)
            df.createOrReplaceTempView(view_name)
        else:
            print("No snapshots found for {}".format(view_name))

def spark_read_s3_source(s3_paths, format="parquet"):
    """Reads data from s3 on the basis of
    s3 path and format
    """
    s3_data = glueContext.getSource(format, paths=s3_paths)
    return s3_data.getFrame()

def add_snapshot_metadata_columns(entity_df, snapshot_metadata):
    snapshot_date_col = f.lit(None)
    snapshot_tags_col = f.lit(f.array([]))

    if not has_column(entity_df, 'snapshotGuid'):
        entity_df = entity_df.withColumn('snapshotGuid', f.lit(None))

    if snapshot_metadata is not None:
        iterator = 0
        for guid, metadata in snapshot_metadata.items():
            snapshot_date_value = fromisodate(metadata['snapshotDate']) if 'snapshotDate' in metadata else None
            snapshot_tags_values = f.array(list(map(lambda v: f.lit(v), metadata['tags'] if 'tags' in metadata else [])))
            if iterator == 0:
                iterator = 1
                snapshot_date_col = f.when(f.col('snapshotGuid') == guid, snapshot_date_value)
                snapshot_tags_col = f.when(f.col('snapshotGuid') == guid, snapshot_tags_values)
            else:
                snapshot_date_col = snapshot_date_col.when(f.col('snapshotGuid') == guid, snapshot_date_value)
                snapshot_tags_col = snapshot_tags_col.when(f.col('snapshotGuid') == guid, snapshot_tags_values)

    entity_df = entity_df.withColumn('snapshotDate', snapshot_date_col)
    entity_df = entity_df.withColumn('tags', snapshot_tags_col)

    return entity_df

def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

def fromisodate(iso_date_str):
    # example format: 2020-07-27T18:18:54.123Z
    try:
        date_str_with_timezone = str(iso_date_str).replace('Z', '+00:00')
        return datetime.strptime(date_str_with_timezone, "%Y-%m-%dT%H:%M:%S.%f%z")
    except:
        return datetime.strptime(iso_date_str, "%Y-%m-%dT%H:%M:%S.%fZ")

def spark_create_json_format(data_frame):
    column_name = str(uuid4())
    df = data_frame.withColumn(column_name, f.lit(0))
    result = df.groupBy(column_name).agg(f.collect_list(f.struct(data_frame.columns)).alias("Items"))
    result = result.drop(column_name)
    return result

if __name__ == '__main__':
  run_query()
