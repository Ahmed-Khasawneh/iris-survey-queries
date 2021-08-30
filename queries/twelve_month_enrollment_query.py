import logging
import sys
import boto3
import json
from uuid import uuid4
from common import query_helpers
from common import survey_format
from pyspark.sql.window import Window
# from queries.twelve_month_enrollment_query import run_twelve_month_enrollment_query
from pyspark.sql.functions import sum as sum, expr, col, lit, upper, to_timestamp, max, min, row_number, date_trunc, \
    to_date, when, coalesce, count, rank, round
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from awsglue.utils import getResolvedOptions

spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).config(
    "spark.dynamicAllocation.enabled", 'true').getOrCreate()
sparkContext = SparkContext.getOrCreate()
sqlContext = SQLContext(sparkContext)
glueContext = GlueContext(sparkContext)

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
stage = options['stage']
user_id = options['userId']
tenant_id = options['tenantId']
survey_type = options['surveyType']    


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
            snapshot_tags_values = f.array(
                list(map(lambda v: f.lit(v), metadata['tags'] if 'tags' in metadata else [])))
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
    column_name = str(uuid.uuid4())
    df = data_frame.withColumn(column_name, f.lit(0))
    result = df.groupBy(column_name).agg(f.collect_list(f.struct(data_frame.columns)).alias("Items"))
    result = result.drop(column_name)
    return result


def spark_refresh_entity_views_v2(tenant_id='11702b15-8db2-4a35-8087-b560bb233420',
                                  survey_type='TWELVE_MONTH_ENROLLMENT_1', stage='DEV', year=2020, user_id=None):
    lambda_client = boto3.client('lambda', 'us-east-1')
    invoke_response = lambda_client.invoke(
        FunctionName="iris-connector-doris-{}-getReportPayload".format(stage),
        InvocationType='RequestResponse',
        LogType="None",
        Payload=json.dumps(
            {'tenantId': tenant_id, 'surveyType': survey_type, 'stateMachineExecutionId': '', 'calendarYear': year,
             'userId': user_id}).encode('utf-8')
    )
    view_metadata_without_s3_paths = json.loads(invoke_response['Payload'].read().decode("utf-8"))

    print(json.dumps(view_metadata_without_s3_paths, indent=2))

    view_metadata_without_s3_paths["tenantId"] = tenant_id
    invoke_response = lambda_client.invoke(
        FunctionName="doris-data-access-apis-{}-GetEntitySnapshotPaths".format(stage),
        InvocationType='RequestResponse',
        LogType="None",
        Payload=json.dumps(view_metadata_without_s3_paths)
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


spark_refresh_entity_views_v2()
# spark_refresh_entity_views_v2(tenant_id=args['tenant_id'], survey_type=args['survey_type'], stage=args['stage'], year=args['year'], user_id=args['user_id'])


def run_twelve_month_enrollment_query():
    
# ********** Survey Default Values
    survey_type = options['surveyType']
    #survey_type = 'TWELVE_MONTH_ENROLLMENT_1'
    year = options['calendarYear']
    #year = calendarYear = '2020'        #'2019' = 1920, '2020' = 2021, '2021' = 2122, '2022 = 2223
    year1 = str(year[2:4])
    year2 = str(int(year1) + 1)
    survey_year = year1 + year2

    if survey_type == 'TWELVE_MONTH_ENROLLMENT_1':
        survey_id = 'E1D'
    elif survey_type == 'TWELVE_MONTH_ENROLLMENT_2':
        survey_id = 'E12'
    elif survey_type == 'TWELVE_MONTH_ENROLLMENT_3':
        survey_id = 'E1E'
    else:  # V4
        survey_id = 'E1F'
    survey_type = '12ME'

    cohort_academic_fall_tag = 'Fall Census'
    cohort_academic_pre_fall_summer_tag = 'Pre-Fall Summer Census'
    cohort_academic_spring_tag = 'Spring Census'
    cohort_academic_post_spring_summer_tag = 'Post-Spring Summer Census'
    cohort_program_tag_1 = 'Academic Year End'
    cohort_status_tag_1 = 'June End'
    cohort_status_tag_2 = 'Academic Year End'
    survey_sections = ['COHORT']
    #Figure out how to build these timestamps using the year1, year2 input parameters
    #ipedsReportingStartDate = to_timestamp(lit('2019-07-01'))
    #ipedsReportingEndDate = to_timestamp(lit('2020-06-30')) 
    
# ********** Survey Reporting Period
    ipeds_client_config = query_helpers.ipeds_client_config_mcr(survey_year_in = survey_year).withColumn('survey_id', lit(survey_id))
    all_academic_terms = query_helpers.academic_term_mcr()
    reporting_period_terms = query_helpers.academic_term_reporting_refactor(academic_term_in = all_academic_terms, survey_year_in = survey_year, survey_id_in = survey_id, survey_sections_in = survey_sections, survey_type_in = survey_type)

# ********** Course Type Counts
    course_counts = query_helpers.course_type_counts(ipeds_client_config_in = ipeds_client_config, academic_term_in = all_academic_terms, academic_term_reporting_refactor_in = reporting_period_terms, survey_type_in = survey_type)

# ********** Cohort
    cohort = query_helpers.student_cohort(ipeds_client_config_in = ipeds_client_config, academic_term_in = all_academic_terms, academic_term_reporting_refactor_in = reporting_period_terms, course_type_counts_in = course_counts, survey_type_in = survey_type)

# ********** Survey Data Transformations    
    cohort_out = cohort.select(
        cohort["*"],
        (when(cohort.studentLevelUGGR == 'GR', lit('99'))
            .when((cohort.isNonDegreeSeeking_calc == True) & (cohort.timeStatus_calc == 'Full Time'), lit('7'))
            .when((cohort.isNonDegreeSeeking_calc == True) & (cohort.timeStatus_calc == 'Part Time'), lit('21'))
            .when((cohort.studentType_calc == 'First Time') & (cohort.timeStatus_calc == 'Full Time'), lit('1'))
            .when((cohort.studentType_calc == 'First Time') & (cohort.timeStatus_calc == 'Part Time'), lit('15'))
            .when((cohort.studentType_calc == 'Transfer') & (cohort.timeStatus_calc == 'Full Time'), lit('2'))
            .when((cohort.studentType_calc == 'Transfer') & (cohort.timeStatus_calc == 'Part Time'), lit('16'))
            .when((cohort.studentType_calc == 'Continuing') & (cohort.timeStatus_calc == 'Full Time'), lit('3'))
            .when((cohort.studentType_calc == 'Continuing') & (cohort.timeStatus_calc == 'Part Time'), lit('17'))
            .otherwise(lit('1'))).alias('ipedsPartAStudentLevel'), 
        (when(cohort.studentLevelUGGR == 'GR', lit('3'))
            .when(cohort.isNonDegreeSeeking_calc == True, lit('2'))
            .when(cohort.studentLevelUGGR == 'UG', lit('1'))
            .otherwise(lit('1'))).alias('ipedsPartCStudentLevel')).filter(cohort.ipedsInclude == 1)

    course_type_counts_out = course_counts.join(
        cohort_out,
        (cohort_out.regPersonId == course_counts.regPersonId), 'inner').select(
        course_counts["*"]).agg(
        sum("UGCreditHours").alias("UGCreditHours"),
        sum("UGClockHours").alias("UGClockHours"),
        sum("GRCreditHours").alias("GRCreditHours"),
        sum("DPPCreditHours").alias("DPPCreditHours"))        

# ********** Survey Formatting

# Part A
    a_data = survey_format.get_part_format_string(survey_type_in = survey_type, survey_id_in = survey_id, part_in = 'A', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config, cohort_flag_in = cohort_out.rdd.isEmpty())
    a_columns = survey_format.get_part_format_string(survey_type_in = survey_type, survey_id_in = survey_id, part_in = 'A', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config, cohort_flag_in = cohort_out.rdd.isEmpty())
    a_level_values = survey_format.get_part_format_string(survey_type_in = survey_type, survey_id_in = survey_id, part_in = 'A', part_type_in = 'levels', ipeds_client_config_in = ipeds_client_config, cohort_flag_in = cohort_out.rdd.isEmpty())
    
    FormatPartA = sparkContext.parallelize(a_data)
    FormatPartA = spark.createDataFrame(FormatPartA).toDF(*a_columns)

    if cohort_out.rdd.isEmpty() == False:
        partA_out = cohort_out.select("regPersonId", "ipedsPartAStudentLevel", "persIpedsEthnValue", "persIpedsGender").filter(
            (cohort_out.ipedsPartAStudentLevel).isin(a_level_values)).union(FormatPartA)

        partA_out = partA_out.select(
            partA_out.ipedsPartAStudentLevel.alias("field1"),
            when(((col('persIpedsEthnValue') == '1') & (col('persIpedsGender') == 'M')), lit(1)).otherwise(
                lit(0)).alias("field2"),  # FYRACE01 - Nonresident alien - Men (1), 0 to 999999
            when(((col('persIpedsEthnValue') == '1') & (col('persIpedsGender') == 'F')), lit(1)).otherwise(
                lit(0)).alias("field3"),  # FYRACE02 - Nonresident alien - Women (2), 0 to 999999
            when(((col('persIpedsEthnValue') == '2') & (col('persIpedsGender') == 'M')), lit(1)).otherwise(
                lit(0)).alias("field4"),  # FYRACE25 - Hispanic/Latino - Men (25), 0 to 999999
            when(((col('persIpedsEthnValue') == '2') & (col('persIpedsGender') == 'F')), lit(1)).otherwise(
                lit(0)).alias("field5"),  # FYRACE26 - Hispanic/Latino - Women (26), 0 to 999999
            when(((col('persIpedsEthnValue') == '3') & (col('persIpedsGender') == 'M')), lit(1)).otherwise(
                lit(0)).alias("field6"),  # FYRACE27 - American Indian or Alaska Native - Men (27), 0 to 999999
            when(((col('persIpedsEthnValue') == '3') & (col('persIpedsGender') == 'F')), lit(1)).otherwise(
                lit(0)).alias("field7"),  # FYRACE28 - American Indian or Alaska Native - Women (28), 0 to 999999
            when(((col('persIpedsEthnValue') == '4') & (col('persIpedsGender') == 'M')), lit(1)).otherwise(
                lit(0)).alias("field8"),  # FYRACE29 - Asian - Men (29), 0 to 999999
            when(((col('persIpedsEthnValue') == '4') & (col('persIpedsGender') == 'F')), lit(1)).otherwise(
                lit(0)).alias("field9"),  # FYRACE30 - Asian - Women (30), 0 to 999999
            when(((col('persIpedsEthnValue') == '5') & (col('persIpedsGender') == 'M')), lit(1)).otherwise(
                lit(0)).alias("field10"),  # FYRACE31 - Black or African American - Men (31), 0 to 999999
            when(((col('persIpedsEthnValue') == '5') & (col('persIpedsGender') == 'F')), lit(1)).otherwise(
                lit(0)).alias("field11"),  # FYRACE32 - Black or African American - Women (32), 0 to 999999
            when(((col('persIpedsEthnValue') == '6') & (col('persIpedsGender') == 'M')), lit(1)).otherwise(
                lit(0)).alias("field12"),# FYRACE33 - Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
            when(((col('persIpedsEthnValue') == '6') & (col('persIpedsGender') == 'F')), lit(1)).otherwise(
                lit(0)).alias("field13"),# FYRACE34 - Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
            when(((col('persIpedsEthnValue') == '7') & (col('persIpedsGender') == 'M')), lit(1)).otherwise(
                lit(0)).alias("field14"),  # FYRACE35 - White - Men (35), 0 to 999999
            when(((col('persIpedsEthnValue') == '7') & (col('persIpedsGender') == 'F')), lit(1)).otherwise(
                lit(0)).alias("field15"),  # FYRACE36 - White - Women (36), 0 to 999999
            when(((col('persIpedsEthnValue') == '8') & (col('persIpedsGender') == 'M')), lit(1)).otherwise(
                lit(0)).alias("field16"),  # FYRACE37 - Two or more races - Men (37), 0 to 999999
            when(((col('persIpedsEthnValue') == '8') & (col('persIpedsGender') == 'F')), lit(1)).otherwise(
                lit(0)).alias("field17"),  # FYRACE38 - Two or more races - Women (38), 0 to 999999
            when(((col('persIpedsEthnValue') == '9') & (col('persIpedsGender') == 'M')), lit(1)).otherwise(
                lit(0)).alias("field18"),  # FYRACE13 - Race and ethnicity unknown - Men (13), 0 to 999999
            when(((col('persIpedsEthnValue') == '9') & (col('persIpedsGender') == 'F')), lit(1)).otherwise(
                lit(0)).alias("field19"))  # FYRACE14 - Race and ethnicity unknown - Women (14), 0 to 999999

        partA_out = partA_out.withColumn('part', lit('A')).groupBy("part", "field1").agg(
            sum("field2").cast('int').alias("field2"),
            sum("field3").cast('int').alias("field3"),
            sum("field4").cast('int').alias("field4"),
            sum("field5").cast('int').alias("field5"),
            sum("field6").cast('int').alias("field6"),
            sum("field7").cast('int').alias("field7"),
            sum("field8").cast('int').alias("field8"),
            sum("field9").cast('int').alias("field9"),
            sum("field10").cast('int').alias("field10"),
            sum("field11").cast('int').alias("field11"),
            sum("field12").cast('int').alias("field12"),
            sum("field13").cast('int').alias("field13"),
            sum("field14").cast('int').alias("field14"),
            sum("field15").cast('int').alias("field15"),
            sum("field16").cast('int').alias("field16"),
            sum("field17").cast('int').alias("field17"),
            sum("field18").cast('int').alias("field18"),
            sum("field19").cast('int').alias("field19")
        )
    else:
        partA_out = FormatPartA
        
# Part C
    c_data = survey_format.get_part_format_string(survey_type_in = survey_type, survey_id_in = survey_id, part_in = 'C', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config, cohort_flag_in = cohort_out.rdd.isEmpty())
    c_columns = survey_format.get_part_format_string(survey_type_in = survey_type, survey_id_in = survey_id, part_in = 'C', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config, cohort_flag_in = cohort_out.rdd.isEmpty())
    c_level_values = survey_format.get_part_format_string(survey_type_in = survey_type, survey_id_in = survey_id, part_in = 'C', part_type_in = 'levels', ipeds_client_config_in = ipeds_client_config, cohort_flag_in = cohort_out.rdd.isEmpty())

    FormatPartC = sparkContext.parallelize(c_data)
    FormatPartC = spark.createDataFrame(FormatPartC).toDF(*c_columns)

    if cohort_out.rdd.isEmpty() == False:
        partC_out = cohort_out.select("regPersonId", "ipedsPartCStudentLevel", "distanceEdInd_calc").filter(
            (cohort_out.ipedsPartCStudentLevel).isin(c_level_values) & (cohort_out.distanceEdInd_calc != 'None')).union(FormatPartC)
            
        partC_out = partC_out.select(
            partC_out.ipedsPartCStudentLevel.alias("field1"),
            when((col('distanceEdInd_calc') == 'Exclusive DE'), lit(1)).otherwise(lit(0)).alias("field2"),
            when((col('distanceEdInd_calc') == 'Some DE'), lit(1)).otherwise(lit(0)).alias("field3")) 

        partC_out = partC_out.withColumn('part', lit('C')).groupBy("part", "field1").agg(
            sum("field2").cast('int').alias("field2"),
            sum("field3").cast('int').alias("field3"))
    else:
        partC_out = FormatPartC
        
# Part B
    if (course_type_counts_out.rdd.isEmpty() == False) & (ipeds_client_config.rdd.isEmpty() == False):
        partB_out = course_type_counts_out.crossJoin(ipeds_client_config).withColumn('part', lit('B')).select(
            'part',
            when((ipeds_client_config.icOfferUndergradAwardLevel == 'Y') & (ipeds_client_config.instructionalActivityType != 'CL'),
                 coalesce(course_type_counts_out.UGCreditHours, lit(0))).cast('int').alias('field2'),
            when((ipeds_client_config.icOfferUndergradAwardLevel == 'Y') & (ipeds_client_config.instructionalActivityType != 'CR'),
                 coalesce(course_type_counts_out.UGCreditHours, lit(0))).cast('int').alias('field3'),
            when((ipeds_client_config.icOfferGraduateAwardLevel == 'Y') & (ipeds_client_config.survey_id == 'E1D'),
                 coalesce(course_type_counts_out.GRCreditHours, lit(0))).cast('int').alias('field4'),
            round(when((ipeds_client_config.icOfferDoctorAwardLevel == 'Y') & (ipeds_client_config.survey_id == 'E1D'),
                 when(coalesce(course_type_counts_out.DPPCreditHours, lit(0)) > 0, course_type_counts_out.DPPCreditHours/ipeds_client_config.tmAnnualDPPCreditHoursFTE).otherwise(lit(0))), 0).cast('int').alias('field5'))
    else:
        b_data = survey_format.get_part_format_string(survey_type_in = survey_type, survey_id_in = survey_id, part_in = 'B', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config, course_flag_in = course_type_counts_out.rdd.isEmpty())
        b_columns = survey_format.get_part_format_string(survey_type_in = survey_type, survey_id_in = survey_id, part_in = 'B', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config, course_flag_in = course_type_counts_out.rdd.isEmpty())

        partB_out = sparkContext.parallelize(b_data)
        partB_out = spark.createDataFrame(partB_out).toDF(*b_columns)

# Output column formatting
    for column in [column for column in partB_out.columns if column not in partA_out.columns]:
        partA_out = partA_out.withColumn(column, lit(None))

    for column in [column for column in partA_out.columns if column not in partC_out.columns]:
        partC_out = partC_out.withColumn(column, lit(None))

    for column in [column for column in partA_out.columns if column not in partB_out.columns]:
        partB_out = partB_out.withColumn(column, lit(None))

    survey_output = partA_out.unionByName(partC_out).unionByName(partB_out)   
    
    return survey_output

# Testing

#test = run_twelve_month_enrollment_query()

#if test is None: # and isinstance(test,DataFrame): #exists(test): #test.isEmpty:
#    test = test
#else:
#    test.createOrReplaceTempView('test')
#test.show() #3m 31s
#test.print
#print(test)
