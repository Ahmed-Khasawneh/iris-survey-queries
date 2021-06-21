import sys
from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from pyspark.sql.functions import sum as _sum, expr, col, lit
from awsglue.utils import getResolvedOptions
from common import query_helpers
import pandas as pd # todo: replace pandas with pyspark
import json
import boto3
import uuid
import re
from pyspark.sql.utils import AnalysisException
from datetime import datetime

OUTPUT_BUCKET = 'doris-survey-reports-dev'
S3_URI_REGEX = re.compile(r"s3://([^/]+)/?(.*)")

spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()
sparkContext = SparkContext.getOrCreate()
sqlContext = SQLContext(sparkContext)
glueContext = GlueContext(sparkContext)

optionNames = [
    'survey_type',
    'year'
    'sql',
    'tenant_id',
    'stage',
    'user_id'
]

#args = getResolvedOptions(sys.argv, optionNames)

#Default survey values
var_surveyYear = '2021' #args['year']

"""
survey_id_map = {
    'TWELVE_MONTH_ENROLLMENT_1': 'E1D', 
    'TWELVE_MONTH_ENROLLMENT_2': 'E12',
    'TWELVE_MONTH_ENROLLMENT_3': 'E1E',
    'TWELVE_MONTH_ENROLLMENT_4': 'E1F'
}
"""
var_surveyId = 'E1D' #survey_id_map[args['survey_type']]
var_surveyType = '12ME'
var_repEndTag = 'June End'

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

def spark_refresh_entity_views_v2(tenant_id='11702b15-8db2-4a35-8087-b560bb233420', survey_type='TWELVE_MONTH_ENROLLMENT_1', stage='DEV', year=2020, user_id=None):
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


spark_refresh_entity_views_v2()
#spark_refresh_entity_views_v2(tenant_id=args['tenant_id'], survey_type=args['survey_type'], stage=args['stage'], year=args['year'], user_id=args['user_id'])

#IPEDSReportingPeriod   
global_ipedsReportingPeriod = spark.sql(query_helpers.func_ipedsReportingPeriod(surveyYear = var_surveyYear, surveyId = var_surveyId, repPeriodTag1 = var_repEndTag)).distinct()
#(surveyYear = "1920", surveyId = 'EF1', repPeriodTag1 = '', repPeriodTag2 = '', repPeriodTag3 = '', repPeriodTag4 = '', repPeriodTag5 = '', dmVersion = '')

reportEndSnapshot = pd.DataFrame(global_ipedsReportingPeriod.limit(1).select("snapshotDate").collect())
reportEndSnapshot = reportEndSnapshot.to_string(header=False, index=False, index_names=False)

#IPEDSClientConfig
global_ipedsClientConfig = spark.sql(query_helpers.func_ipedsClientConfig(surveyYear = var_surveyYear, snapshotDate = reportEndSnapshot)).limit(1).collect()
#(surveyYear = "1920", snapshotDate = "9999-09-09", dmVersion = '')

global_ipedsClientConfig = pd.DataFrame(global_ipedsClientConfig)
global_ipedsClientConfig = global_ipedsClientConfig.to_string(header=False, index=False, index_names=False).split()
list_ipedsClientConfig = ["', '".join(ele.split()) for ele in global_ipedsClientConfig]

config_acadOrProgReporter = list_ipedsClientConfig[0]
config_genderForNonBinary = list_ipedsClientConfig[30]
config_genderForUnknown = list_ipedsClientConfig[31]
config_icOfferDoctorAwardLevel = list_ipedsClientConfig[34]
config_icOfferGraduateAwardLevel = list_ipedsClientConfig[35]
config_icOfferUndergradAwardLevel = list_ipedsClientConfig[36]
config_instructionalActivityType = list_ipedsClientConfig[38]
config_tmAnnualDPPCreditHoursFTE = list_ipedsClientConfig[51]

#AcademicTerm
global_academicTerm = spark.sql(query_helpers.func_academicTerm())
#(repPeriodTag1 = '', repPeriodTag2 = '', repPeriodTag3 = '', repPeriodTag4 = '', repPeriodTag5 = '', dmVersion = '')

#IPEDSReportingPeriod || AcademicTerm
global_reportingPeriodOrder = global_ipedsReportingPeriod.join(
    global_academicTerm, 
    (global_ipedsReportingPeriod.termCode == global_academicTerm.termCode) &
    (global_ipedsReportingPeriod.partOfTermCode == global_academicTerm.partOfTermCode), 'inner').select(
    global_ipedsReportingPeriod["*"], 
    global_academicTerm.termOrder,
    global_academicTerm.termType,
    global_academicTerm.termClassification,
    global_academicTerm.financialAidYear,
    global_academicTerm.censusDate,
    global_academicTerm.maxCensus,
    global_academicTerm.startDate,
    global_academicTerm.endDate,
    global_academicTerm.requiredFTClockHoursUG,
    global_academicTerm.requiredFTCreditHoursGR, 
    global_academicTerm.requiredFTCreditHoursUG)

global_reportingPeriodOrder.createOrReplaceTempView("global_reportingPeriodOrder")

global_reportingPeriodRefactor = spark.sql(query_helpers.func_reportingPeriodRefactor()).distinct()
        
global_reportingPeriodRefactor.createOrReplaceTempView("global_reportingPeriodRefactor")

#Cohort
global_cohort = spark.sql(query_helpers.func_cohort(repPeriod = 'global_reportingPeriodRefactor', termOrder = 'global_reportingPeriodOrder', instructionalActivityType = config_instructionalActivityType, genderForNonBinary = config_genderForNonBinary, genderForUnknown = config_genderForUnknown, acadOrProgReporter = config_acadOrProgReporter, surveyType = var_surveyType))

#CourseLevelCounts
global_courseLevelCounts = spark.sql(query_helpers.func_courseLevelCounts(repPeriod = 'global_reportingPeriodRefactor', termOrder = 'global_reportingPeriodOrder', instructionalActivityType = config_instructionalActivityType))
global_courseLevelCounts = global_courseLevelCounts.join(
    global_cohort,
    (global_cohort.personId == global_courseLevelCounts.personId), 'inner').select(
        global_courseLevelCounts["*"],
        global_cohort.ipedsEnrolled).filter(global_cohort.ipedsEnrolled == 1)

CourseTypeCountsCRN = global_courseLevelCounts.agg(
    _sum("UGCreditHours").alias("UGCreditHours"),
    _sum("UGClockHours").alias("UGClockHours"),
    _sum("GRCreditHours").alias("GRCreditHours"),
    _sum("DPPCreditHours").alias("DPPCreditHours"))

cohort_out = global_cohort.select(
    global_cohort["*"],
    expr("""
    case when studentLevelUGGR = 'GR' then '99'
         when isNonDegreeSeeking = 1 and timeStatus = 'Full Time' then '7'
         when isNonDegreeSeeking = 1 and timeStatus = 'Part Time' then '21'
         when studentType = 'First Time' and timeStatus = 'Full Time' then '1'
         when studentType = 'Transfer' and timeStatus = 'Full Time' then '2'
         when studentType = 'Continuing' and timeStatus = 'Full Time' then '3'
         when studentType = 'First Time' and timeStatus = 'Part Time' then '15'
         when studentType = 'Transfer' and timeStatus = 'Part Time' then '16'
         when studentType = 'Continuing' and timeStatus = 'Part Time' then '17'
         else '1'
    end
    """).alias("ipedsPartAStudentLevel"),
    expr("""
    case when studentLevelUGGR = 'GR' then '3'
         when isNonDegreeSeeking = 1 then '2'
         when studentLevelUGGR = 'UG' then '1'
         else null
    end
    """).alias("ipedsPartCStudentLevel")
    ).filter(global_cohort.ipedsEnrolled == 1)
    
#Survey version output lists
if var_surveyId == 'E1D':
    A_UgGrBoth = ["1", "2", "3", "7", "15", "16", "17", "21", "99"]
    A_UgOnly = ["1", "2", "3", "7", "15", "16", "17", "21"]
    A_GrOnly = ["99"]
    C_UgGrBoth = ["1", "2", "3"]
    C_UgOnly = ["1", "2"]
    C_GrOnly = ["3"]
elif var_surveyId == 'E12':
    A_UgGrBoth = ["1", "2", "3", "7", "15", "16", "17", "21"]
    A_UgOnly = ["1", "2", "3", "7", "15", "16", "17", "21"]
    A_GrOnly = [""] 
    C_UgGrBoth = ["1", "2"]
    C_UgOnly = ["1", "2"]
    C_GrOnly = [""]
elif var_surveyId == 'E1E':
    A_UgGrBoth = ["1", "3", "7", "15", "17", "21"]
    A_UgOnly = ["1", "3", "7", "15", "17", "21"]
    A_GrOnly = [""] 
    C_UgGrBoth = ["1", "2"]
    C_UgOnly = ["1", "2"]
    C_GrOnly = [""]
else: #V4
    A_UgGrBoth = ["1", "3", "15", "17"]
    A_UgOnly = ["1", "3", "15", "17"]
    A_GrOnly = [""] 
    C_UgGrBoth = ["1", "2", "3"]
    C_UgOnly = ["1", "2"]
    C_GrOnly = ["3"]

#Part A
if cohort_out.rdd.isEmpty() == False:
    #FormatPartA
    a_columns = ["personId", "ipedsLevel", "ipedsEthnicity", "ipedsGender"]
    a_data = [("", "1", "", ""), ("", "2", "", ""), ("", "3", "", ""), ("", "7", "", ""), ("", "15", "", ""), ("", "16", "", ""), ("", "17", "", ""), ("", "21", "", ""), ("", "99", "", "")]
    FormatPartA = sparkContext.parallelize(a_data)
    FormatPartA = spark.createDataFrame(FormatPartA).toDF(*a_columns)

    partA_out = cohort_out.select("personId", "ipedsPartAStudentLevel", "ipedsEthnicity", "ipedsGender").filter((cohort_out.ipedsPartAStudentLevel.isNotNull()) & (cohort_out.ipedsPartAStudentLevel != '')).union(FormatPartA)
    partA_out = partA_out.select(
		partA_out.ipedsPartAStudentLevel.alias("field1"),
		expr("case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end").cast('int').alias("field2"),   # FYRACE01 - Nonresident alien - Men (1), 0 to 999999
		expr("case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end").cast('int').alias("field3"),   # FYRACE02 - Nonresident alien - Women (2), 0 to 999999
		expr("case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end").cast('int').alias("field4"),   # FYRACE25 - Hispanic/Latino - Men (25), 0 to 999999
		expr("case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end").cast('int').alias("field5"),   # FYRACE26 - Hispanic/Latino - Women (26), 0 to 999999
		expr("case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end").cast('int').alias("field6"),   # FYRACE27 - American Indian or Alaska Native - Men (27), 0 to 999999
		expr("case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end").cast('int').alias("field7"),   # FYRACE28 - American Indian or Alaska Native - Women (28), 0 to 999999
		expr("case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end").cast('int').alias("field8"),   # FYRACE29 - Asian - Men (29), 0 to 999999
		expr("case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end").cast('int').alias("field9"),   # FYRACE30 - Asian - Women (30), 0 to 999999
		expr("case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end").cast('int').alias("field10"),  # FYRACE31 - Black or African American - Men (31), 0 to 999999
		expr("case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end").cast('int').alias("field11"),  # FYRACE32 - Black or African American - Women (32), 0 to 999999
		expr("case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end").cast('int').alias("field12"),  # FYRACE33 - Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
		expr("case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end").cast('int').alias("field13"),  # FYRACE34 - Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
		expr("case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end").cast('int').alias("field14"),  # FYRACE35 - White - Men (35), 0 to 999999
		expr("case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end").cast('int').alias("field15"),  # FYRACE36 - White - Women (36), 0 to 999999
		expr("case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end").cast('int').alias("field16"),  # FYRACE37 - Two or more races - Men (37), 0 to 999999
		expr("case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end").cast('int').alias("field17"),  # FYRACE38 - Two or more races - Women (38), 0 to 999999
		expr("case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end").cast('int').alias("field18"),  # FYRACE13 - Race and ethnicity unknown - Men (13), 0 to 999999
		expr("case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end").cast('int').alias("field19"))  # FYRACE14 - Race and ethnicity unknown - Women (14), 0 to 999999

    partA_out = partA_out.withColumn('part', f.lit('A')).groupBy("part", "field1").agg(
	    _sum("field2").alias("field2"),
	    _sum("field3").alias("field3"),
	    _sum("field4").alias("field4"), 
	    _sum("field5").alias("field5"), 
	    _sum("field6").alias("field6"), 
	    _sum("field7").alias("field7"), 
	    _sum("field8").alias("field8"), 
	    _sum("field9").alias("field9"), 
	    _sum("field10").alias("field10"),
    	_sum("field11").alias("field11"), 
	    _sum("field12").alias("field12"), 
	    _sum("field13").alias("field13"), 
	    _sum("field14").alias("field14"), 
	    _sum("field15").alias("field15"), 
	    _sum("field16").alias("field16"), 
	    _sum("field17").alias("field17"), 
	    _sum("field18").alias("field18"), 
	    _sum("field19").alias("field19")
	    )

else:
    a_columns = ["part", "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9", "field10", "field11", "field12", "field13", "field14", "field15", "field16", "field17", "field18", "field19"]
    a_data = [("A", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"), 
        ("A", "2", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"), 
        ("A", "3", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"), 
        ("A", "7", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"), 
        ("A", "15", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"), 
        ("A", "16", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"), 
        ("A", "17", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"), 
        ("A", "21", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"), 
        ("A", "99", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")]

    partA_out = sparkContext.parallelize(a_data)
    partA_out = spark.createDataFrame(partA_out).toDF(*a_columns)
#Part A output filter
if config_icOfferUndergradAwardLevel == 'Y' and config_icOfferGraduateAwardLevel == 'Y':
    partA_out = partA_out.where(partA_out.field1.isin(A_UgGrBoth))
elif config_icOfferUndergradAwardLevel == 'Y' and config_icOfferGraduateAwardLevel == 'N':
    partA_out = partA_out.where(partA_out.field1.isin(A_UgOnly))
elif config_icOfferUndergradAwardLevel == 'N' and config_icOfferGraduateAwardLevel == 'Y':
    partA_out = partA_out.where(partA_out.field1.isin(A_GrOnly))
    
#Part C
if cohort_out.rdd.isEmpty() == False:
	#FormatPartC
	c_columns = ["personId", "ipedsLevel", "distanceEdInd"]
	c_data = [("", "1", ""), ("", "2", ""), ("", "3", "")]
	FormatPartC = sparkContext.parallelize(c_data)
	FormatPartC = spark.createDataFrame(FormatPartC).toDF(*c_columns)

	#Part C
	partC_out = cohort_out.select("personId", "ipedsPartCStudentLevel", "distanceEdInd").filter((cohort_out.ipedsPartCStudentLevel.isNotNull()) & (cohort_out.ipedsPartCStudentLevel != '') & (cohort_out.distanceEdInd != 'None')).union(FormatPartC)
	partC_out = partC_out.select(
		partC_out.ipedsPartCStudentLevel.alias("field1"),
		expr("case when distanceEdInd = 'Exclusive DE' then 1 else 0 end").cast('int').alias("field2"), # Enrolled exclusively in distance education courses
		expr("case when distanceEdInd = 'Some DE' then 1 else 0 end").cast('int').alias("field3"))        # Enrolled in at least one but not all distance education courses

	partC_out = partC_out.withColumn('part', f.lit('C')).groupBy("part", "field1").agg(
		_sum("field2").alias("field2"),
		_sum("field3").alias("field3"))

else:
    c_columns = ["part", "field1", "field2", "field3"]
    c_data = [("C", "1", "0", "0"), ("C", "2", "0", "0"), ("C", "3", "0", "0")]
    partC_out = sparkContext.parallelize(c_data)
    partC_out = spark.createDataFrame(partC_out).toDF(*c_columns)

#Part C output filter
if config_icOfferUndergradAwardLevel == 'Y' and config_icOfferGraduateAwardLevel == 'Y':
    partC_out = partC_out.where(partC_out.field1.isin(C_UgGrBoth))
elif config_icOfferUndergradAwardLevel == 'Y' and config_icOfferGraduateAwardLevel == 'N':
    partC_out = partC_out.where(partC_out.field1.isin(C_UgOnly))
elif config_icOfferUndergradAwardLevel == 'N' and config_icOfferGraduateAwardLevel == 'Y':
    partC_out = partC_out.where(partC_out.field1.isin(C_GrOnly))

#Part B
if CourseTypeCountsCRN.rdd.isEmpty() == False:
    partB_out = CourseTypeCountsCRN.withColumn('part', f.lit('B')).select(
        "part",
    #CREDHRSU - credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.    
	    expr(f"round((case when '{config_icOfferUndergradAwardLevel}' = 'Y' and '{config_instructionalActivityType}' != 'CL' then coalesce(UGCreditHours, 0) else null end))").cast('int').alias("field2"),
    # CONTHRS  - clock hour instructional activity at the undergraduate level, 0 to 9999999, blank = not applicable, if no undergraduate programs are measured in clock hours.
	    expr(f"round((case when '{config_icOfferUndergradAwardLevel}' = 'Y' and '{config_instructionalActivityType}' != 'CR' then coalesce(UGClockHours, 0) else null end))").cast('int').alias("field3"),
    # CREDHRSG - credit hour instructional activity at the graduate level, 0 to 99999999, blank = not applicable
	    expr(f"round((case when '{config_icOfferGraduateAwardLevel}' = 'Y' and '{var_surveyId}' = 'E1D' then coalesce(GRCreditHours, 0) else null end))").cast('int').alias("field4"),
    # RDOCFTE  - reported Doctor'92s degree-professional practice student FTE, 0 to 99999999, blank = not applicable
	    expr(f"""round((case when '{config_icOfferDoctorAwardLevel}' = 'Y' and '{var_surveyId}' = 'E1D' then 
						    (case when coalesce(DPPCreditHours, 0) > 0 then coalesce(cast(round(DPPCreditHours / {config_tmAnnualDPPCreditHoursFTE}, 0) as string), '0') 
					    else '0' end) else null end))""").cast('int').alias("field5")
	)
else:
    b_columns = ["part", "field2", "field3", "field4", "field5"]
    b_data = [("B", "0", "", "0", "0")]
    partB_out = sparkContext.parallelize(b_data)
    partB_out = spark.createDataFrame(partB_out).toDF(*b_columns)
    
#Survey out formatting
for column in [column for column in partB_out.columns if column not in partA_out.columns]:
    partA_out = partA_out.withColumn(column, lit(None))

for column in [column for column in partA_out.columns if column not in partC_out.columns]:
    partC_out = partC_out.withColumn(column, lit(None))

for column in [column for column in partA_out.columns if column not in partB_out.columns]:
    partB_out = partB_out.withColumn(column, lit(None))
    
surveyOutput = partA_out.unionByName(partC_out).unionByName(partB_out)

surveyOutput.show()
#surveyOutput = create_json_format(surveyOutput)
#write_dataframe_as_json_to_s3(surveyOutput.repartition(1), s3_path, constants.SPARK_OVERWRITE_MODE, 'json')

#global_courseLevelCounts.show()
#surveyOutput.show()

