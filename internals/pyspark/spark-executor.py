import json
import boto3
import uuid
import re
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as f
from pyspark import SparkContext
from awsglue.context import GlueContext
import argparse

OUTPUT_BUCKET = 'doris-survey-reports-dev'
S3_URI_REGEX = re.compile(r"s3://([^/]+)/?(.*)")

spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
glueContext = GlueContext(SparkContext.getOrCreate())


def main():
    args = parse_args()
    sql = get_file_from_s3(args.sql)
    spark_refresh_entity_views(args.tenant_id, args.stage)
    dataframe = spark.sql(sql)
    dataframe = spark_create_json_format(dataframe)
    s3_path = 's3://{}/tmp-for-testing/report-output/{}'.format(OUTPUT_BUCKET, str(uuid.uuid4()))
    write_dataframe_as_json_to_s3(
        dataframe.repartition(1), s3_path, 'overwrite', 'json')
    output_file_key = get_output_file_key(s3_path)

    print('##OUTPUT##: s3://{}/{}'.format(OUTPUT_BUCKET, output_file_key))


def spark_read_s3_source(s3_path, format="parquet"):
    """Reads data from s3 on the basis of
    s3 path and format
    """
    s3_data = glueContext.getSource(format, paths=[s3_path])
    return s3_data.getFrame()


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
        spark_read_s3_source(s3_path).toDF().createOrReplaceTempView(entity_name)


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

    return parser.parse_args()


if __name__ == '__main__':
    main()
