import re
import pyspark
import yaml
from pyspark.sql.functions import lit, array
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, BooleanType

# conf = pyspark.SparkConf().setMaster("local[2]").setAppName("testing")
# conf = pyspark.SparkConf().setMaster("local[2]").setAppName("testing")
sparkContext = pyspark.SparkContext.getOrCreate()
spark = pyspark.SQLContext(sparkContext)
# Press the green button in the gutter to run the script.

def create_mock_data(
        doris_entity_name,
        record_count=1,
        entity_data_override={},  # Accepts a dictionary of values to be manually included. (ex.{'DORIS_field': 'value')
        entity_data_override_type=None,
        # Valid values: 'A', append 'entity_data_override' values to mock records; 'R', replace with 'entity_data_override' values.
        entity_metadata_override={}  # Accepts a dictionary of values for entity metadata.
):
    # Enable for production use
    with open(f"./Data_Model/{doris_entity_name}.yml") as f:#relative path here
        doris_entity_yml = yaml.load(f, Loader=yaml.FullLoader)
    #        C:/Users/ahmed.khasawneh/PycharmProjects/pythonProject_unittest/venv/data_model_definition/{doris_entity_name}.yml

    # Enable for manual Zeppelin use
    #doris_entity_yml = eval(doris_entity_name + 'YML')

    # variable field values - Write a line to explain why the defaults
    person_id_default = '12345'
    term_code_default = '202110'
    date_default = '0909999'

    # variable metadata values (override assignement if dictionary of value(s) are provided)
    row_guid_default = '35ce827d-9f43-4c68-a25a-986980556745'  # if entity_metadata_override['row_guid'] is None else entity_metadata_override['row_guid']
    tenant_id_default = '11702b15-8db2-4a35-8087-b560bb233420'
    group_entity_execution_id_default = '23729'
    user_id_default = '130db900-05cd-4948-b055-d0bbba7c962a'
    record_inserted_timestamp_default = '0909999'
    data_path_default = 'processed-data/11702b15-8db2-4a35-8087-b560bb233420/5/2021-05-03_20_53_21'
    snapshot_guid_default = 'fb35fbf2-6c1f-4298-a59f-0f9975630cc2'
    snapshot_date_default = '0909999'
    tags_default = 'Fall Census'

    if 'rowGuid' in entity_metadata_override:
        row_guid_default = entity_metadata_override['rowGuid']

    if 'tenantId' in entity_metadata_override:
        tenant_id_default = entity_metadata_override['tenantId']

    if 'groupEntityExecutionId' in entity_metadata_override:
        group_entity_execution_id_default = entity_metadata_override['groupEntityExecutionId']

    if 'userId' in entity_metadata_override:
        user_id_default = entity_metadata_override['userId']

    if 'recordInsertedTimestamp' in entity_metadata_override:
        record_inserted_timestamp_default = entity_metadata_override['recordInsertedTimestamp']

    if 'dataPath' in entity_metadata_override:
        data_path_default = entity_metadata_override['dataPath']

    if 'snapshotGuid' in entity_metadata_override:
        snapshot_guid_default = entity_metadata_override['snapshotGuid']

    if 'snapshotDate' in entity_metadata_override:
        snapshot_date_default = entity_metadata_override['snapshotDate']

    if 'tags' in entity_metadata_override:
        tags_default = entity_metadata_override['tags']

    for entity in doris_entity_yml:

        # Keeps nested array used for this function
        doris_entity_fields_yml = doris_entity_yml['fields']

        # Dictionary for DORIS DM to spark data types
        data_type_map = {
            'string': 'StringType()',
            'date': 'StringType()',  # 'TimestampType()',
            'boolean': 'BooleanType()',
            'enum': 'StringType()',
            'number': 'IntegerType()',
            'uuid': 'StringType()'
        }

        field_list = []
        field_type_list = []
        field_enum_list = []
        schema_list = []
        field_df_type_list = []
        mock_data_row = []
        mock_data_set = []

        # (field_list) Returns list of entity fields
        # (field_df_type_list) Returns list of converted fields types (DataModel Type => spark type)
        for field in doris_entity_fields_yml:
            if 'depreciated' not in field and 'private' not in field:
                entity_field = (field['name'])
                field_list.append(entity_field)
                entity_field_type = (field['type'])
                field_type_list.append(entity_field_type)
                field_df_type_list.append(data_type_map[entity_field_type])

       # Returns a dictionary of field names (key): formatted data type (value)
        entity_schema = {field_list[i]: field_df_type_list[i] for i in range(len(field_list))}

        df_schema_formatting = []

        for key, value in entity_schema.items():
            df_schema_formatting.append(eval(f'StructField("{key}", {value})'))

        df_schema = StructType(df_schema_formatting)

        # Returns field data based on field type
        i = 1
        boolean_counter = 0
        enum_counter = 0
        mock_data_row = []
        mock_data_set = []
        enum_list = []
        enum_values = ''

        while i <= record_count:
            for field in doris_entity_fields_yml:
                if 'depreciated' not in field and 'private' not in field:
                    if field['name'] in entity_data_override and entity_data_override_type == 'R':
                        data = entity_data_override[field['name']]
                    elif bool(re.match('personId', field['name'])) == True:
                        data = person_id_default
                    elif bool(re.search('termCode', field['name'])) == True:
                        data = term_code_default
                    elif field['type'] == 'string':
                        data = ''
                    elif field['type'] == 'date':
                        data = date_default
                    elif field['type'] == 'boolean':
                        if boolean_counter > 1:
                            boolean_counter = 0
                        data = [True, False][boolean_counter]
                    elif field['type'] == 'enum':
                        enum_values = field['enumValues']
                        enum_length = len(enum_values) - 1
                        if enum_counter > enum_length:
                            data = enum_values[enum_length]
                        else:
                            data = enum_values[enum_counter]
                    elif field['type'] == 'number':
                        data = 0
                    mock_data_row.append(data)
            mock_data_set.append(mock_data_row)
            boolean_counter += 1
            enum_counter += 1
            mock_data_row = []
            i += 1

        if entity_data_override_type == 'A':
            for field in doris_entity_fields_yml:
                if 'depreciated' not in field and 'private' not in field:
                    if field['name'] in entity_data_override.keys():
                        data = entity_data_override[field['name']]
                    else:
                        data = None
                    mock_data_row.append(data)
            mock_data_set.append(mock_data_row)

        # Create and populate dataframe
        data = sparkContext.parallelize(mock_data_set)
        df = spark.createDataFrame(data, schema=df_schema)

        df = df.withColumn('rowGuid', lit(row_guid_default).cast(StringType())).withColumn(
            'tenantId', lit(tenant_id_default).cast(StringType())).withColumn(
            'groupEntityExecutionId', lit(group_entity_execution_id_default).cast(StringType())).withColumn(
            'userId', lit(user_id_default).cast(StringType())).withColumn(
            'recordInsertedTimestamp', lit(record_inserted_timestamp_default).cast(StringType())).withColumn(
            'dataPath', lit(data_path_default).cast(StringType())).withColumn(
            'snapshotGuid', lit(snapshot_guid_default).cast(StringType())).withColumn(
            'snapshotDate', lit(snapshot_date_default).cast(StringType())).withColumn(
            'tags', array(lit(tags_default)))

    return df

