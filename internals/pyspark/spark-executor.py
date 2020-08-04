import json
import boto3
import uuid
import re
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as f
from pyspark import SparkContext
from pyspark.sql.utils import AnalysisException
from awsglue.context import GlueContext
import argparse
from datetime import datetime

OUTPUT_BUCKET = 'doris-survey-reports-dev'
S3_URI_REGEX = re.compile(r"s3://([^/]+)/?(.*)")

spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
glueContext = GlueContext(SparkContext.getOrCreate())


def main():
    args = parse_args()
    sql = get_file_from_s3(args.sql)
    spark_refresh_entity_views_v2(args.tenant_id, args.survey_type, args.stage)
    dataframe = spark.sql(sql)
    dataframe = spark_create_json_format(dataframe)
    s3_path = 's3://{}/tmp-for-testing/report-output/{}'.format(OUTPUT_BUCKET, str(uuid.uuid4()))
    write_dataframe_as_json_to_s3(
        dataframe.repartition(1), s3_path, 'overwrite', 'json')
    output_file_key = get_output_file_key(s3_path)

    print('##OUTPUT##: s3://{}/{}'.format(OUTPUT_BUCKET, output_file_key))


def spark_read_s3_source(s3_paths, format="parquet"):
    """Reads data from s3 on the basis of
    s3 path and format
    """
    s3_data = glueContext.getSource(format, paths=s3_paths)
    return s3_data.getFrame()

def spark_refresh_entity_views_v2(tenant_id, survey_type, stage):
    lambda_client = boto3.client('lambda', 'us-east-1')
    invoke_response = lambda_client.invoke(
        FunctionName = "iris-connector-doris-2019-{}-getReportPayload".format(stage),
        LogType = "None",
        Payload = json.dumps({ 'tenantId': tenant_id, 'surveyType': survey_type, 'stateMachineExecutionId': '' }).encode('utf-8')
    )
    view_metadata_without_s3_paths = json.loads(invoke_response['Payload'].read().decode("utf-8"))
    view_metadata_without_s3_paths["tenantId"] = tenant_id
    invoke_response = lambda_client.invoke(
        FunctionName = "doris-data-access-apis-{}-GetEntitySnapshotPaths".format(stage),
        LogType = "None",
        Payload = json.dumps(view_metadata_without_s3_paths)
    )
    view_metadata = json.loads(invoke_response['Payload'].read().decode("utf-8"))
    snapshot_metadata = view_metadata.get('snapshotMetadata', {})
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

def spark_refresh_entity_views(tenant_id, stage):
    lambda_client = boto3.client('lambda', 'us-east-1')
    s3 = boto3.client('s3', 'us-east-1')

    invoke_response = lambda_client.invoke(
        FunctionName="doris-data-access-apis-{}-GetEntitiesStatuses".format(
            stage),
        LogType="None",
        Payload=json.dumps({'tenant_id': tenant_id}).encode('utf-8')
    )

    entity_response = json.loads(
        invoke_response['Payload'].read().decode("utf-8"))

    entity_map = {}

    for entity in entity_response['entities']:
        bucket_name = "doris-data-raw-{}".format(stage.lower())

        key_prefix = "processed-data/{}/{}/".format(tenant_id, entity['id'])

        paginator = s3.get_paginator('list_objects_v2')

        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=key_prefix
        )

        entity_key_map = {}

        for page in page_iterator:
            if 'Contents' in page:
                for item in page['Contents']:
                    if not '$folder$' in item['Key']:
                        item_key = '/'.join(item['Key'].split('/')[0:-1])
                        entity_key_map[item_key] = True

        entity_keys = list(entity_key_map.keys())
        entity_keys.sort(reverse=True)

        if len(entity_keys) > 0:
            entity_map[entity['name']] = 's3://{}/{}'.format(bucket_name, entity_keys[0])

    for entity_name, s3_path in entity_map.items():
        print("{} = {}".format(entity_name, s3_path))
        spark_read_s3_source([s3_path]).toDF().createOrReplaceTempView(entity_name)


def spark_create_json_format(data_frame):
    column_name = str(uuid.uuid4())
    df = data_frame.withColumn(column_name, f.lit(0))
    result = df.groupBy(column_name).agg(f.collect_list(f.struct(data_frame.columns)).alias("Items"))
    result = result.drop(column_name)
    return result


def write_dataframe_as_json_to_s3(dataframe, s3_path, mode, file_format):
    """Writes spark dataframe to s3 
    path in given format. 
    """
    dataframe.write.mode(mode).format(file_format).json(s3_path)


def get_output_file_key(uri):
    s3 = boto3.client('s3', 'us-east-1')
    (bucket, key) = parse_s3_uri(uri)
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=key
    )

    if 'Contents' in response and len(response['Contents']) > 0:
        return response['Contents'][0]['Key']

    return None


def get_file_from_s3(uri):
    s3 = boto3.client('s3', 'us-east-1')
    (bucket, key) = parse_s3_uri(uri)
    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )
    return response['Body'].read().decode('utf-8')

def parse_s3_uri(uri):
    match = S3_URI_REGEX.match(uri)
    return (match.group(1), match.group(2))

def parse_args():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--sql', dest='sql', help='sql for the script to run')
    parser.add_argument('--tenant_id', dest='tenant_id',
                        help='tenant id to get data from')
    parser.add_argument('--stage', dest='stage', default='DEV',
                        help='stage to run data against')
    parser.add_argument('--survey_type', dest='survey_type',
                        help='survey type to prepare data for')

    return parser.parse_args()


if __name__ == '__main__':
    main()
