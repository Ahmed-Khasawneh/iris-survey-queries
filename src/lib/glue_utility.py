"""A collection of functions that simplify working with S3, Glue, and Athena. 

This module defines functions used to
    * get metadata used to query a data from a JDBC or S3 data source
    * append boolean type columns to a DataFrame
    * decrypt database passwords
    * append and transform DataFrame columns
    * read/write data to/from S3

Most of these functions use the `AWS SDK for Python (Boto3)`_ to complete their
tasks.  Refer to this documentation to understand the data sent to and returned
from it.  

.. _AWS SDK for Python (Boto3):
   https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
"""
import sys
from awsglue.utils import getResolvedOptions
import utilities.date_utility as date_utility
import common.constants as constants
import json
import uuid
import boto3

def get_jdbc_raw_job_parameters():
    """Returns the AWS Glue job parameters that will be used to query data from
    a database via JDBC.

    Returns:
        dict: A dictionary containing the AWS Glue job parameters used to
            query data from a database via JDBC. 
    """
    try:
        args = getResolvedOptions(sys.argv,
                            
                            [constants.COLLECTION_QUERY,
                            constants.USERNAME,
                            constants.PASSWORD,
                            constants.HOST,
                            constants.DATABASE,
                            constants.PORT,
                            constants.DB_SERVER_NAME,
                            constants.S3_BUCKET_NAME,
                            constants.S3_RAW_KEY_NAME,
                            constants.TENANT_ID,
                            constants.TENANT_NAME,
                            constants.GROUP_ENTITY_EXECUTION_ID,
                            constants.STAGE,
                            constants.USER_ID,
                            constants.EMAIL,
                            constants.FIRST_NAME,
                            constants.LAST_NAME,
                            constants.INGESTION_TRIGGER,
                            constants.GROUP_JOB_ID,
                            constants.ENTITY_ID,
                            constants.ENTITY_NAME,
                            constants.APPLICATION_NAME,
                            constants.JOB_NAME,
                            constants.ENTITY_METADATA])
        query_path = json.loads(args[constants.COLLECTION_QUERY])
        query = read_data_from_s3(query_path['bucket'], query_path['key'])
        args[constants.COLLECTION_QUERY] = "(" + query + ") ptable"
        args[constants.PORT] = str(args[constants.PORT])

        return args
    except Exception as e:
        print('printing exception ... !!!!!')
        print(str(e))
        return None
        
def get_report_exection_parameters():
    """Returns the AWS Glue job parameters that will be used to query data from
    an S3 object.

    Returns:
        dict: A dictionary containing the AWS Glue job parameters used to
            query data from an S3 object. 
    """
    try:
        args = getResolvedOptions(sys.argv,    
                            [constants.TENANT_ID,
                            constants.ENTITY_LIST,
                            constants.SQL_SCRIPT_INPUT_BUCKET,
                            constants.SQL_SCRIPT_INPUT_KEY,
                            constants.SQL_SCRIPT_OUTPUT_BUCKET,
                            constants.SQL_SCRIPT_OUTPUT_KEY,
                            constants.JOB_NAME,
                            constants.ENTITY_LIST_EMPTY,
                            constants.INGESTION_TRIGGER,
                            constants.APPLICATION_NAME,
                            constants.STAGE,
                            constants.USER_ID,
                            constants.LAST_NAME,
                            constants.FIRST_NAME,
                            constants.EMAIL,
                            constants.GROUP_JOB_ID,
                            constants.ENTITY_ID,
                            constants.ENTITY_NAME,
                            constants.GROUP_ENTITY_EXECUTION_ID,
                            constants.TENANT_NAME,
                            constants.INPUT_ENTITY_LIST,
                            constants.SURVEY_ID,
                            constants.ATTEMPT,
                            constants.EXECUTION_ID,
                            constants.SURVEY_TYPE,
                            constants.SNAPSHOT_METADATA,
                            ])
        empty_entity_list_path = json.loads(args[constants.ENTITY_LIST_EMPTY])
        args['empty_entity_list_s3_path'] = empty_entity_list_path
        args[constants.ENTITY_LIST_EMPTY] = read_data_from_s3(empty_entity_list_path['bucket'], empty_entity_list_path['key'])
        snapshot_metadata_json_path = json.loads(args[constants.SNAPSHOT_METADATA])
        snapshot_metadata_json = read_data_from_s3(snapshot_metadata_json_path['bucket'], snapshot_metadata_json_path['key'])
        args[constants.SNAPSHOT_METADATA] = json.loads(snapshot_metadata_json)
        args['snapshot_metadata_json'] = snapshot_metadata_json
        try:
            entityViewsArgs = getResolvedOptions(sys.argv, [constants.ENTITY_VIEWS])
            entity_views_json_path = json.loads(entityViewsArgs[constants.ENTITY_VIEWS])
            entity_views_json = read_data_from_s3(entity_views_json_path['bucket'], entity_views_json_path['key'])
            args[constants.ENTITY_VIEWS] = json.loads(entity_views_json)
            args['entity_views_json'] = entity_views_json
        except:
            pass          
        return args
    except Exception as e:
        raise e

def apply_boolean_mapping(data_frame, boolean_columns):
    """Adds a list of boolean columns to a DataFrame.

    Args:
        data_frame (DataFrame): The DataFrame to which a list of boolean type
            columns is to be added.
        boolean_columns (list): The list of boolean type columns to add to the
            DataFrame.

    Returns:
        DataFrame: The DataFrame with the boolean type columns added to it.
    """
    import pyspark.sql.functions as f

    boolean_columns = json.loads(boolean_columns)
    print('printing boolean columns')
    print(boolean_columns)
    print('printed boolean columns')
    for col in boolean_columns:
        print("printing boolean colum",col)
        try:
            mapped = f.when(f.col(col) == True, True)
            mapped = mapped.when(f.col(col) == 1, True)
            mapped = mapped.when(f.lower(f.col(col)) == 'true', True)
            mapped = mapped.when(f.lower(f.col(col)) == 'yes', True)
            mapped = mapped.when(f.lower(f.col(col)) == 'y', True)
            mapped = mapped.otherwise(False)
            mapped = mapped.cast('boolean')
            data_frame = data_frame.withColumn(col, mapped)
        except Exception as e:
            print('exception in boolean mapping',e)
    return data_frame

def decrypt_password(password):
    """Decrypts a password.

    Args:
        password (string): The password to be decrypted.

    Returns:
        str: The decrypted password.
    """
    kms_client = boto3.client('kms',region_name='us-east-1')
    encrypted_password = json.loads(password)
    plaintext = kms_client.decrypt(
        CiphertextBlob=bytes(encrypted_password['data'])
    )
    return plaintext['Plaintext'].decode("utf-8")

def add_guid_column(data_frame):
    """Adds a UUID type column to a DataFrame.

    Args:
        data_frame (DataFrame): The DataFrame to which a UUID type column is
            to be added.

    Returns:
        DataFrame: The DataFrame with the UUID type column added to it.
    """
    import pyspark.sql.functions as f
    from pyspark.sql.types import StringType
    data_frame = data_frame.withColumn("rowGuid", f.expr("uuid()"))
    return data_frame

def add_unmapped_column(data_frame, unmapped_column):
    """Adds an unmapped column to a DataFrame.

    Args:
        data_frame (DataFrame): The DataFrame to which the unmapped column is to be added.
        unmapped_column (str): The name of the column to add to the DataFrame.

    Returns:
        DataFrame: The DataFrame with the unmapped column added to it.
    """
    import pyspark.sql.functions as f
    from pyspark.sql.types import StringType
    unmapped_column_list = json.loads(unmapped_column)
    for column in unmapped_column_list:
        data_frame = data_frame.withColumn(column, f.lit(None).cast(StringType()))
    return data_frame

def add_operational_columns(data_frame, tenant_id, group_execution_id, group_entity_execution_id, userId, dataPath):
    """Adds 'operational' columns to a DataFrame.

    Args:
        data_frame (DataFrame): The DataFrame to which the 'operational' columns
            are to be added.
        tenant_id (str): The tenant id to be assigned to rows of the added
            'tenantId' column.
        group_execution_id (str): Not used in this function.
        group_entity_execution_id (str): The group entity execution id to be
            assigned to the rows of the added 'groupEntityExecutionId' column.
        userId (str): The user id to be assigned to the rows of the added
            'userId' column.
        dataPath (str): The data path to be added to the rows of the added
            'dataPath' column.

    Returns:
        DataFrame: The DataFrame with the 'operational' rows added to it.
    """
    import pyspark.sql.functions as f
    data_frame = data_frame.withColumn('tenantId', f.lit(tenant_id))
    data_frame = data_frame.withColumn('groupEntityExecutionId', f.lit(int(group_entity_execution_id)))
    data_frame = data_frame.withColumn('userId', f.lit(userId))
    data_frame = data_frame.withColumn('recordInsertedTimestamp', f.current_timestamp())
    data_frame = data_frame.withColumn('dataPath', f.lit(dataPath))
    return data_frame

def add_snapshot_columns(data_frame, snapshot_guid, snapshot_date):
    """Adds 'snapshot' columns to a DataFrame.

    Args:
        data_frame (DataFrame): The DataFrame to which the 'snapshot' columns
            are to be added.
        snapshot_guid (str): The snapshot GUID to be added to rows of the added
            'snapshotGuid' column.
        snapshot_date (str): The snapshot date to be added to rows of the added
            'snapshotDate' column.

    Returns:
        DataFrame: The DataFrame with the 'snapshot' columns added to it.
    """
    import pyspark.sql.functions as f
    snapshot_date_value = f.to_date(f.lit(date_utility.fromisodate(snapshot_date)))
    data_frame = data_frame \
        .withColumn('snapshotGuid', f.lit(snapshot_guid)) \
        .withColumn('snapshotDate', snapshot_date_value)
    return data_frame

def apply_transformation(data_frame, column_mapping, enum_mapping, entity_metadata):
    """Transforms a DataFrame.  Details of this operation are TBD.

    Args:
        data_frame (DataFrame): The DataFrame to be transformed.
        column_mapping (str): A JSON string representing ???.
        enum_mapping (str): A JSON string representing ???.
        entity_metadata (dict): A dictionary representing ???.

    Raises:
        ex: A TypeError or JSONDecodeError if enum_mapping is malformed.

    Returns:
        DataFrame: The transformed DataFrame. 
    """
    import pyspark.sql.functions as f
    column_identifier =  str(uuid.uuid4()) +'_'
    try:
        if enum_mapping and len(enum_mapping) > 0:
            enum_mapping_dict = json.loads(enum_mapping)
        else:
            enum_mapping_dict = {}
    except Exception as ex:
        print(ex)
        try:
            # this is to handle some weird scenarios when the json is malformed....maybe?
            # don't 100% know why.  This is left over from NBS code
            enum_mapping_dict = json.loads(enum_mapping + '}}')
        except Exception as ex2:
            print(ex2)
            # we want to fail if the enum mapping json is invalid
            # will cause data to be corrupt otherwise
            raise ex
    enum_columns_list = []
    columns_list = []
    print(enum_mapping_dict)
    column_mapping_dict = json.loads(column_mapping)
    print(column_mapping_dict)
    for key in enum_mapping_dict:
        enum_columns_list.append(key)
    for key in column_mapping_dict:
        columns_list.append(key)

    column_metadata_dict = {}
    for column in entity_metadata.get('columns', []):
        column_name = column.get('name')
        if column_name:
            column_metadata_dict[column_name] = column

    print(enum_columns_list)
    data_frame = data_frame.select([c for c in data_frame.columns if c in columns_list])
    for key, value in column_mapping_dict.items():
        print('printing keys')
        print(key, value)
        print(type(key), type(value))
        column_metadata = column_metadata_dict.get(value, {})
        column_type = str(column_metadata.get('type', 'string')).lower()
        if (value in enum_columns_list):
            
            iterator = 0
            for enum_key, enum_value in enum_mapping_dict[value].items():
                if iterator == 0:
                    iterator = 1
                    mapped = f.when(f.col(key) == enum_key, enum_value)
                else:
                    mapped = mapped.when(f.col(key) == enum_key, enum_value)
                if enum_key == "null":
                    mapped = mapped.when(f.col(key) == None, enum_value)

            data_frame = data_frame.withColumn(key+"Raw", data_frame[key])            
            data_frame = data_frame.withColumn(key, mapped)
            data_frame = data_frame.withColumnRenamed(key,column_identifier+value)
            data_frame = data_frame.withColumnRenamed(key+"Raw",column_identifier+value+"Raw")
            print('printing column names')
            print(data_frame.schema.names)
        else:
            mapped_col = f.col(key)

            if column_type == 'string':
                mapped_col = mapped_col.cast(column_type)
            elif column_type == 'uuid':
                mapped_col = mapped_col.cast('string')
            elif column_type == 'date':
                mapped_col = mapped_col.cast(column_type)
            elif column_type == 'int':
                mapped_col = mapped_col.cast(column_type)
            elif column_type.startswith('decimal'):
                mapped_col = mapped_col.cast(column_type)

            data_frame = data_frame.withColumn(key, mapped_col)
            data_frame = data_frame.withColumnRenamed(key, column_identifier+value)

    for column in data_frame.columns:
        data_frame = data_frame.withColumnRenamed(column, column.replace(column_identifier, ''))

    return data_frame

def get_jdbc_processed_job_parameters(): # todo: rename this function; it is not used to assist reading from a JDBC data source
    """Returns the AWS Glue job parameters that will be used to query data from
    an S3 object.

    Returns:
        dict: A dictionary containing the AWS Glue job parameters used to
            query data from an S3 object.
    """
    try:
        args = getResolvedOptions(sys.argv,
                            
                            [constants.COLLECTION_QUERY,
                            constants.S3_BUCKET_NAME,
                            constants.S3_RAW_KEY_NAME,
                            constants.S3_PROCESSED_KEY_NAME,
                            constants.S3_PROCESSED_CSV_KEY_NAME,
                            constants.S3_PROCESSED_JSON_KEY_NAME,
                            constants.COLUMN_MAPPING,
                            constants.ENUM_MAPPING,
                            constants.BOOLEAN_COLUMNS,
                            constants.TENANT_ID,
                            constants.TENANT_NAME,
                            constants.GROUP_ENTITY_EXECUTION_ID,
                            constants.STAGE,
                            constants.USER_ID,
                            constants.EMAIL,
                            constants.FIRST_NAME,
                            constants.LAST_NAME,
                            constants.INGESTION_TRIGGER,
                            constants.GROUP_JOB_ID,
                            constants.JOB_NAME,
                            constants.ENTITY_ID,
                            constants.ENTITY_NAME,
                            constants.UNMAPPED_COLUMNS,
                            constants.APPLICATION_NAME,
                            constants.SNAPSHOT_GUID,
                            constants.SNAPSHOT_DATE,
                            constants.ENTITY_METADATA
                            ])
        entity_metadata_path = json.loads(args[constants.ENTITY_METADATA])
        entity_metadata = read_data_from_s3(entity_metadata_path['bucket'], entity_metadata_path['key'])
        args[constants.ENTITY_METADATA] = json.loads(entity_metadata)
        return args
    except Exception as e:
        print('printing exception ... !!!!!')
        print(str(e))
        return None

def get_jdbc_job_parameters():
    """Returns the AWS Glue job parameters that will be used to query data from
    a database via JDBC.

    Returns:
        dict: A dictionary containing the AWS Glue job parameters used to
            query data from a database via JDBC. 
    """
    try:
        args = getResolvedOptions(sys.argv,
                            
                            [constants.COLLECTION_QUERY,
                            constants.USERNAME,
                            constants.PASSWORD,
                            constants.HOST,
                            constants.DATABASE,
                            constants.PORT,
                            constants.DB_SERVER_NAME,
                            constants.S3_BUCKET_NAME,
                            constants.S3_RAW_KEY_NAME,
                            constants.S3_PROCESSED_KEY_NAME,
                            constants.S3_PROCESSED_CSV_KEY_NAME,
                            constants.S3_PROCESSED_JSON_KEY_NAME,
                            constants.COLUMN_MAPPING,
                            constants.ENUM_MAPPING,
                            constants.BOOLEAN_COLUMNS,
                            constants.TENANT_ID,
                            constants.TENANT_NAME,
                            constants.GROUP_ENTITY_EXECUTION_ID,
                            constants.STAGE,
                            constants.USER_ID,
                            constants.EMAIL,
                            constants.FIRST_NAME,
                            constants.LAST_NAME,
                            constants.INGESTION_TRIGGER,
                            constants.GROUP_JOB_ID,
                            constants.ENTITY_ID,
                            constants.ENTITY_NAME,
                            constants.UNMAPPED_COLUMNS,
                            constants.APPLICATION_NAME,
                            constants.JOB_NAME,
                            constants.SNAPSHOT_GUID,
                            constants.SNAPSHOT_DATE,
                            constants.ENTITY_METADATA])
        query_path = json.loads(args[constants.COLLECTION_QUERY])
        query = read_data_from_s3(query_path['bucket'], query_path['key'])
        args[constants.COLLECTION_QUERY] = "(" + query + ") ptable"
        args[constants.PORT] = str(args[constants.PORT])
        entity_metadata_path = json.loads(args[constants.ENTITY_METADATA])
        entity_metadata = read_data_from_s3(entity_metadata_path['bucket'], entity_metadata_path['key'])
        args[constants.ENTITY_METADATA] = json.loads(entity_metadata)

        return args
    except Exception as e:
        print('printing exception ... !!!!!')
        print(str(e))
        return None

def read_data_from_s3(bucket, key):
    """Returns data read from an Object stored in Amazon S3.

    Args:
        bucket (str): The Object's bucket_name identifier.
        key (str): The Object's key identifier.

    Returns:
        str: The data read from the Object stored in Amazon S3.
    """
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket, key)
    raw_content = content_object.get()['Body'].read().decode('utf-8')
    return raw_content

def write_data_to_s3(bucket, key, body):
    """Writes data to an Object stored in Amazon S3.

    Args:
        bucket (str): The Object's bucket_name identifier.
        key (str): The Object's key identifier.
        body (object): The object data to be written to S3.
    """
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket, key)
    content_object.put(Body=body)

def get_ingestion_query(column_mapping,enum_mapping): # this function does not appear to be used by anything
    """Returns query for ingestions.

    Args:
        column_mapping (str): A JSON string representing ???
        enum_mapping (str): A JSON string representing ???

    Returns:
        str: The ingestion query.
    """
    query = ''
    try:
        #enum_mapping_string = enum_mapping.replace("'", "\"")
        enum_mapping_dict = json.loads(enum_mapping)
        print(enum_mapping_dict)
        enum_mapping_list = []
        for key in enum_mapping_dict:
            enum_mapping_list.append({key: enum_mapping_dict[key]})
        enum_mapping_dict = enum_mapping_list

        #column_mapping_string = column_mapping.replace("'", "\"")
        column_mapping_dict = json.loads(column_mapping)
        print(column_mapping_dict)

        columnNames = []
        enum_columns = []
        enum_columns_dict = {}
        for enums in enum_mapping_dict:
            for enum in enums:
                enum_columns_dict[enum] = enums[enum]
                enum_columns.append(enum)
        spark_query = Table('entity_data')
        for key in column_mapping_dict:
            if column_mapping_dict[key] in enum_columns:
                case = Case()
                for enum_fields in enum_columns_dict[column_mapping_dict[key]]:
                    if (enum_fields == ''):
                        case = case.when(spark_query[key] ==None , enum_columns_dict[column_mapping_dict[key]][enum_fields])
                    case = case.when(spark_query[key] == enum_fields, enum_columns_dict[column_mapping_dict[key]][enum_fields])
                    
                case = case.else_(None)
                case = case.as_(column_mapping_dict[key])
                columnNames.append(case)
                columnNames.append(Field(key).as_(column_mapping_dict[key] +'Raw'))
            else:
                columnNames.append(Field(key).as_(column_mapping_dict[key]))
        query = Query.from_(spark_query).select( *columnNames )

        query = query.get_sql(quote_char=None)
        print(query)        
    except Exception as e:
        print('printing exception')
        print(e)
    query = query.replace('=NULL',' is Null' )
    return query

def create_json_format(data_frame):
    """???

    Args:
        data_frame (DataFrame): ???

    Returns:
        DataFrame: ???
    """
    import pyspark.sql.functions as f
    column_name = str(uuid.uuid4())
    df = data_frame.withColumn(column_name,f.lit(0))
    result = df.groupBy(column_name).agg(f.collect_list(f.struct(data_frame.columns)).alias("Items"))
    result = result.drop(column_name)
    return result

def delete_file_from_s3(entity_definitions):
    """Deletes an Object stored in Amazon S3.

    Args:
        entity_definitions (dict): A dictionary containing keys
            * bucket - the bucket name containing the object to delete from S3.
            * key - the key of the object to delete from S3.
    """
    client = boto3.client('s3')
    client.delete_object(Bucket=entity_definitions['bucket'], Key=entity_definitions['key'])

def add_calculated_column(data_frame, calculated_column, entity_name):
    """Adds a column denoted as 'calculated' to a DataFrame.

    Args:
        data_frame (DataFrame): The DataFrame to which the 'calculated' column
            is to be added.
        calculated_column (dict): The column to be added to the DataFrame.
        entity_name (str): The name of the entity view to replace with the
            DataFrame.

    Returns:
        DataFrame: The DataFrame with the 'calculated' column added to it.
    """
    from pyspark.sql import functions as f
    from connections import glue_connectivity as connection

    query = calculated_column.get('calculated', {}).get('query')
    calculated_column_name = calculated_column.get('name')

    data_frame.createOrReplaceTempView(entity_name)
    new_data_frame = connection.spark_sql(query).select(f.col('rowGuid'), f.lit(True).alias(calculated_column_name))

    data_frame = data_frame.alias('all_records')
    data_frame = data_frame.join(new_data_frame, 'rowGuid', "left")

    data_frame = data_frame \
        .select("all_records.*", f.when(f.col(calculated_column_name) == True, True) \
        .otherwise(False) \
        .alias(calculated_column_name))

    return data_frame

def add_calculated_columns(data_frame, entity_metadata, entity_name):
    """Adds columns denoted as 'calculated' to a DataFrame.

    Args:
        data_frame (DataFrame): The DataFrame to which the 'calculated' columns
            are to be added.
        entity_metadata (dict): A dictionary containing the key 'columns'
            from which 'calculated' columns will be searched for and added
            to the DataFrame.
        entity_name (str): The name of the entity view to replace with the
            DataFrame.

    Returns:
        DataFrame: The DataFrame with the 'calculated' columns added to it.
    """
    calculated_columns = []
    for column in entity_metadata.get('columns', []):
        if column.get('calculated'):
            calculated_columns.append(column)
            
    for calculated_column in calculated_columns:
        data_frame = add_calculated_column(data_frame, calculated_column, entity_name)
        
    return data_frame
