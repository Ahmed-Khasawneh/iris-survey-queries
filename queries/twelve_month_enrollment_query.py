import logging
import sys
import boto3
import json
from uuid import uuid4
from common import query_helpers
from pyspark.sql.window import Window
# from queries.twelve_month_enrollment_query import run_twelve_month_enrollment_query
from pyspark.sql.functions import sum as sum, expr, col, lit, upper, to_timestamp, max, min, row_number, date_trunc, \
    to_date, when, coalesce, count, rank
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
    'survey_type',
    'year'
    'sql',
    'tenant_id',
    'stage',
    'user_id',
    'sql_script_s3_output_bucket',
]

# args = getResolvedOptions(sys.argv, optionNames)

# Default survey values
var_surveyYear = '2021'  # args['year']

"""
survey_id_map = {
    'TWELVE_MONTH_ENROLLMENT_1': 'E1D', 
    'TWELVE_MONTH_ENROLLMENT_2': 'E12',
    'TWELVE_MONTH_ENROLLMENT_3': 'E1E',
    'TWELVE_MONTH_ENROLLMENT_4': 'E1F'
}
"""
var_surveyId = 'E1D'  # survey_id_map[args['survey_type']]
var_surveyType = '12ME'
var_repPeriodTag1 = 'Academic Year End'
var_repPeriodTag2 = 'June End'
var_repPeriodTag3 = 'Fall Census'
var_repPeriodTag4 = 'Fall Census'
var_repPeriodTag5 = 'Fall Census'


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

ipeds_client_config_partition = "surveyCollectionYear"
ipeds_client_config_order = f"""
    ((case when array_contains(tags, '{var_repPeriodTag1}') then 1
         when array_contains(tags, '{var_repPeriodTag2}') then 2
         else 3 end) asc,
    snapshotDate desc,
    coalesce(recordActivityDate, CAST('9999-09-09' as DATE)) desc)
     """
ipeds_client_config_partition_filter = f"surveyCollectionYear = '{var_surveyYear}'"  # f"surveyId = '{var_surveyId}' and var_surveyYear = '{var_surveyYear}"

ipeds_reporting_period_partition = "surveyCollectionYear, surveyId, surveySection, termCode, partOfTermCode"
ipeds_reporting_period_order = f"""
    ((case when array_contains(tags, '{var_repPeriodTag1}') then 1
         when array_contains(tags, '{var_repPeriodTag2}') then 2
         else 3 end) asc,
    snapshotDate desc,
    coalesce(recordActivityDate, CAST('9999-09-09' as DATE)) desc)
     """
ipeds_reporting_period_partition_filter = f"surveyId = '{var_surveyId}'"

academic_term_partition = "termCode, partOfTermCode"
academic_term_order = "(snapshotDate desc, recordActivityDate desc)"
academic_term_partition_filter = "coalesce(isIpedsReportable, true) = true"

ipeds_client_config_in = ipeds_client_config_mcr(ipeds_client_config_partition, ipeds_client_config_order,
                            ipeds_client_config_partition_filter)

academic_term_in = academic_term_mcr(
    academic_term_partition,
    academic_term_order,
    academic_term_partition_filter).cache()

academic_term_reporting_refactor_in = academic_term_reporting_refactor(
    ipeds_reporting_period_partition,
    ipeds_reporting_period_order,
    ipeds_reporting_period_partition_filter,
    academic_term_partition,
    academic_term_order,
    academic_term_partition_filter).cache()

course_type_counts_in = ipeds_course_type_counts()

student_in = spark.sql("select * from student")
person_in = spark.sql("select * from person")
academic_track_in = spark.sql("select * from academicTrack")
degree_program_in = spark.sql("select * from degreeProgram")
degree_in = spark.sql("select * from degree")
field_of_study_in = spark.sql("select * from fieldOfStudy")

student = student_in.join(
    academic_term_reporting_refactor_in,
    ((upper(student_in.termCode) == academic_term_reporting_refactor_in.termCode)
     & (coalesce(student_in.isIPEDSReportable, lit(True)) == True)), 'inner').select(
    # academic_term_reporting_refactor_in['*'],
    academic_term_reporting_refactor_in.yearType.alias('repRefYearType'),
    academic_term_reporting_refactor_in.financialAidYear.alias('repRefFinancialAidYear'),
    academic_term_reporting_refactor_in.surveySection.alias('repRefSurveySection'),
    academic_term_reporting_refactor_in.termCodeOrder.alias('repRefTermCodeOrder'),
    academic_term_reporting_refactor_in.termTypeNew.alias('repRefTermTypeNew'),
    academic_term_reporting_refactor_in.startDate.alias('repRefStartDate'),
    academic_term_reporting_refactor_in.censusDate.alias('repRefCensusDate'),
    academic_term_reporting_refactor_in.requiredFTCreditHoursGR.alias('repRefRequiredFTCreditHoursGR'),
    academic_term_reporting_refactor_in.requiredFTCreditHoursUG.alias('repRefRequiredFTCreditHoursUG'),
    academic_term_reporting_refactor_in.requiredFTClockHoursUG.alias('repRefRequiredFTClockHoursUG'),
    academic_term_reporting_refactor_in.snapshotDate.alias('repRefSnapshotDate'),
    academic_term_reporting_refactor_in.fullTermOrder.alias('repRefFullTermOrder'),
    student_in.personId.alias('stuPersonId'),
    student_in.termCode.alias('stuTermCode'),
    (when((student_in.studentType).isin('High School', 'Visiting', 'Unknown'), lit(True)).otherwise(
        when((student_in.studentLevel).isin('Continuing Education', 'Other'), lit(True)).otherwise(
            when(student_in.studyAbroadStatus == 'Study Abroad - Host Institution', lit(True)).otherwise(
                coalesce(col('isNonDegreeSeeking'), lit(False)))))).alias('stuIsNonDegreeSeeking'),
    student_in.studentLevel.alias('stuStudentLevel'),
    student_in.studentType.alias('stuStudentType'),
    student_in.residency.alias('stuResidency'),
    upper(student_in.homeCampus).alias('stuHomeCampus'),
    student_in.fullTimePartTimeStatus.alias('stuFullTimePartTimeStatus'),
    student_in.studyAbroadStatus.alias('stuStudyAbroadStatus'),
    coalesce(student_in.recordActivityDate, to_timestamp(lit('9999-09-09'))).alias('stuRecordActivityDate'),
    to_timestamp(student_in.snapshotDate).alias('stuSnapshotDate')).filter(
    ((col('stuRecordActivityDate') != to_timestamp(lit('9999-09-09')))
     & (col('stuRecordActivityDate') <= col('repRefCensusDate')))
    | (col('stuRecordActivityDate') == to_timestamp(lit('9999-09-09')))
).withColumn(
    'studentRowNum',
    row_number().over(
        Window.partitionBy(
            col('repRefYearType'),
            col('repRefSurveySection'),
            col('stuPersonId'),
            col('stuTermCode')).orderBy(
            when(col('stuSnapshotDate') == col('repRefSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
            when(col('stuSnapshotDate') > col('repRefSnapshotDate'), col('stuSnapshotDate')).otherwise(
                to_timestamp(lit('1900-09-09'))).asc(),
            when(col('stuSnapshotDate') < col('repRefSnapshotDate'), col('stuSnapshotDate')).otherwise(
                to_timestamp(lit('1900-09-09'))).desc(),
            col('stuSnapshotDate').desc(),
            col('stuRecordActivityDate').desc()))
).filter(col('studentRowNum') == 1)

student_reg = student.join(
    course_type_counts_in,
    (student.stuPersonId == course_type_counts_in.regPersonId) &
    (student.stuTermCode == course_type_counts_in.regTermCode), 'left').filter(
    course_type_counts_in.regPersonId.isNotNull()
).withColumn(
    'NDSRn',
    row_number().over(
        Window.partitionBy(
            student.stuPersonId,
            student.repRefYearType).orderBy(
            student.stuIsNonDegreeSeeking,
            student.repRefFullTermOrder,
            student.repRefTermCodeOrder,
            student.repRefStartDate))
).withColumn(
    'FFTRn',
    row_number().over(
        Window.partitionBy(
            student.stuPersonId,
            student.repRefYearType).orderBy(
            student.repRefFullTermOrder,
            student.repRefTermCodeOrder,
            student.repRefStartDate))
).select(
    course_type_counts_in.regPersonId,
    course_type_counts_in.repRefYearType,
    course_type_counts_in.regTermCode,
    col('FFTRn'),
    col('NDSRn'),
    when(student.stuIsNonDegreeSeeking == True, lit(True)).otherwise(lit(False)).alias('stuRefIsNonDegreeSeeking'),
    expr("""
    (case when stuStudentLevel not in ('Masters', 'Doctorate', 'Professional Practice Doctorate') and stuStudentType is null then 'First Time'
        else
        (case when stuIsNonDegreeSeeking = false then
            (case when stuStudentLevel != 'Undergraduate' then stuStudentType
                when NDSRn = 1 and FFTRn = 1 then stuStudentType
                when NDSRn = 1 then 'Continuing'
            end)
            else stuStudentType
        end) 
    end)
    """).alias('stuRefStudentType'),
    #    when((student.stuIsNonDegreeSeeking == False) & (student.stuStudentLevel != 'Undergraduate'), None)
    #        .when((student.stuIsNonDegreeSeeking == False) & (col('NDSRn') == 1) & (col('FFTRn') == 1),
    #              student.stuStudentType)
    #        .when((student.stuIsNonDegreeSeeking == False) & (col('NDSRn') == 1), 'Continuing')
    #        .otherwise(None).alias('stuRefStudentType'),
    when((student.stuIsNonDegreeSeeking == False) & (student.stuStudentLevel != 'Undergraduate'), None)
        .when((student.stuIsNonDegreeSeeking == False) & (col('NDSRn') == 1), student.repRefTermTypeNew)
        .otherwise(None).alias('stuRefTypeTermType'),
    when(student.repRefTermTypeNew == 'Pre-Fall Summer', student.stuStudentType).otherwise(None).alias(
        'preFallStudType'),
    when(col('FFTRn') == 1, student.stuStudentLevel).otherwise(None).alias("stuRefStudentLevel"),
    when(col('FFTRn') == 1, student.stuTermCode).otherwise(None).alias("firstFullTerm"),
    when(col('FFTRn') == 1, student.stuHomeCampus).otherwise(None).alias("stuRefCampus"),
    when(col('FFTRn') == 1, student.stuFullTimePartTimeStatus).otherwise(None).alias(
        "stuRefFullTimePartTimeStatus"),
    when(col('FFTRn') == 1, student.stuStudyAbroadStatus).otherwise(None).alias("stuRefStudyAbroadStatus"),
    when(col('FFTRn') == 1, student.stuResidency).otherwise(None).alias("stuRefResidency"),
    when(col('FFTRn') == 1, student.repRefSurveySection).otherwise(None).alias("stuRefSurveySection"),
    when(col('FFTRn') == 1, student.stuSnapshotDate).otherwise(None).alias("stuRefSnapshotDate"),
    when(col('FFTRn') == 1, student.repRefTermCodeOrder).otherwise(None).alias("stuRefTermCodeOrder"),
    when(col('FFTRn') == 1, student.repRefTermTypeNew).otherwise(None).alias("stuRefTermTypeNew"),
    when(col('FFTRn') == 1, student.repRefCensusDate).otherwise(None).alias("stuRefCensusDate"),
    when(col('FFTRn') == 1, student.repRefFinancialAidYear).otherwise(None).alias("stuRefFinancialAidYear"),
    when(col('FFTRn') == 1, student.repRefRequiredFTCreditHoursGR).otherwise(None).alias(
        "stuRefRequiredFTCreditHoursGR"),
    when(col('FFTRn') == 1, student.repRefRequiredFTCreditHoursUG).otherwise(None).alias(
        "stuRefRequiredFTCreditHoursUG"),
    when(col('FFTRn') == 1, student.repRefRequiredFTClockHoursUG).otherwise(None).alias(
        "stuRefRequiredFTClockHoursUG"),
    when(col('FFTRn') == 1, course_type_counts_in.totalCourses).otherwise(None).alias("stuRefTotalCourses"),
    when(col('FFTRn') == 1, course_type_counts_in.totalCreditCourses).otherwise(None).alias(
        "stuRefTotalCreditCourses"),
    when(col('FFTRn') == 1, course_type_counts_in.totalCreditHrs).otherwise(None).alias("stuRefTotalCreditHrs"),
    when(col('FFTRn') == 1, course_type_counts_in.totalClockHrs).otherwise(None).alias("stuRefTotalClockHrs"),
    when(col('FFTRn') == 1, course_type_counts_in.totalCECourses).otherwise(None).alias("stuRefTotalCECourses"),
    when(col('FFTRn') == 1, course_type_counts_in.totalSAHomeCourses).otherwise(None).alias(
        "stuRefTotalSAHomeCourses"),
    when(col('FFTRn') == 1, course_type_counts_in.totalESLCourses).otherwise(None).alias("stuRefTotalESLCourses"),
    when(col('FFTRn') == 1, course_type_counts_in.totalRemCourses).otherwise(None).alias("stuRefTotalRemCourses"),
    when(col('FFTRn') == 1, course_type_counts_in.totalIntlCourses).otherwise(None).alias("stuRefTotalIntlCourses"),
    when(col('FFTRn') == 1, course_type_counts_in.totalAuditCourses).otherwise(None).alias("stuRefTotalAuditCourses"),
    when(col('FFTRn') == 1, course_type_counts_in.totalThesisCourses).otherwise(None).alias(
        "stuRefTotalThesisCourses"),
    when(col('FFTRn') == 1, course_type_counts_in.totalProfResidencyCourses).otherwise(None).alias(
        "stuRefTotalProfResidencyCourses"),
    when(col('FFTRn') == 1, course_type_counts_in.totalDECourses).otherwise(None).alias("stuRefTotalDECourses"),
    when(col('FFTRn') == 1, course_type_counts_in.UGCreditHours).otherwise(None).alias("stuRefUGCreditHours"),
    when(col('FFTRn') == 1, course_type_counts_in.UGClockHours).otherwise(None).alias("stuRefUGClockHours"),
    when(col('FFTRn') == 1, course_type_counts_in.GRCreditHours).otherwise(None).alias("stuRefGRCreditHours"),
    when(col('FFTRn') == 1, course_type_counts_in.DPPCreditHours).otherwise(None).alias("stuRefDPPCreditHours"))

student_fft = student_reg.groupBy(student_reg.regPersonId, student_reg.repRefYearType).agg(
    min(student_reg.stuRefIsNonDegreeSeeking).alias("stuRefIsNonDegreeSeeking"),
    max(student_reg.stuRefStudentType).alias("stuRefStudentType"),
    max(student_reg.stuRefTypeTermType).alias("stuRefTypeTermType"),
    max(student_reg.preFallStudType).alias("preFallStudType"),
    max(student_reg.stuRefStudentLevel).alias("stuRefStudentLevel"),
    when(max(student_reg.stuRefStudentLevel).isin("Masters", "Doctorate", "Professional Practice Doctorate"),
         "GR").otherwise("UG").alias("studentLevelUGGR"),
    max(student_reg.firstFullTerm).alias("regFirstFullTerm"),
    max(student_reg.stuRefCampus).alias("stuRefCampus"),
    max(student_reg.stuRefFullTimePartTimeStatus).alias("stuRefFullTimePartTimeStatus"),
    max(student_reg.stuRefStudyAbroadStatus).alias("stuRefStudyAbroadStatus"),
    max(student_reg.stuRefResidency).alias("stuRefResidency"),
    max(student_reg.stuRefSurveySection).alias("stuRefSurveySection"),
    max(student_reg.stuRefSnapshotDate).alias("stuRefSnapshotDate"),
    max(student_reg.stuRefTermCodeOrder).alias("stuRefTermCodeOrder"),
    max(student_reg.stuRefTermTypeNew).alias("stuRefTermTypeNew"),
    max(student_reg.stuRefCensusDate).alias("stuRefCensusDate"),
    max(student_reg.stuRefFinancialAidYear).alias("stuRefFinancialAidYear"),
    max(student_reg.stuRefRequiredFTCreditHoursGR).alias("stuRefRequiredFTCreditHoursGR"),
    max(student_reg.stuRefRequiredFTCreditHoursUG).alias("stuRefRequiredFTCreditHoursUG"),
    max(student_reg.stuRefRequiredFTClockHoursUG).alias("stuRefRequiredFTClockHoursUG"),
    sum(student_reg.stuRefTotalCourses).alias("stuRefTotalCourses"),
    sum(student_reg.stuRefTotalCreditCourses).alias("stuRefTotalCreditCourses"),
    sum(student_reg.stuRefTotalCreditHrs).alias("stuRefTotalCreditHrs"),
    sum(student_reg.stuRefTotalClockHrs).alias("stuRefTotalClockHrs"),
    sum(student_reg.stuRefTotalCECourses).alias("stuRefTotalCECourses"),
    sum(student_reg.stuRefTotalSAHomeCourses).alias("stuRefTotalSAHomeCourses"),
    sum(student_reg.stuRefTotalESLCourses).alias("stuRefTotalESLCourses"),
    sum(student_reg.stuRefTotalRemCourses).alias("stuRefTotalRemCourses"),
    sum(student_reg.stuRefTotalIntlCourses).alias("stuRefTotalIntlCourses"),
    sum(student_reg.stuRefTotalAuditCourses).alias("stuRefTotalAuditCourses"),
    sum(student_reg.stuRefTotalThesisCourses).alias("stuRefTotalThesisCourses"),
    sum(student_reg.stuRefTotalProfResidencyCourses).alias("stuRefTotalProfResidencyCourses"),
    sum(student_reg.stuRefTotalDECourses).alias("stuRefTotalDECourses"),
    sum(student_reg.stuRefUGCreditHours).alias("stuRefUGCreditHours"),
    sum(student_reg.stuRefUGClockHours).alias("stuRefUGClockHours"),
    sum(student_reg.stuRefGRCreditHours).alias("stuRefGRCreditHours"),
    sum(student_reg.stuRefDPPCreditHours).alias("stuRefDPPCreditHours"))

student_fft_out = student_fft.crossJoin(ipeds_client_config_in).select(
    student_fft['*'],
    ipeds_client_config_in.acadOrProgReporter.alias('configAcadOrProgReporter'),
    ipeds_client_config_in.admUseTestScores.alias('configAdmUseTestScores'),
    ipeds_client_config_in.compGradDateOrTerm.alias('configCompGradDateOrTerm'),
    ipeds_client_config_in.feIncludeOptSurveyData.alias('configFeIncludeOptSurveyData'),
    ipeds_client_config_in.fourYrOrLessInstitution.alias('configFourYrOrLessInstitution'),
    ipeds_client_config_in.genderForNonBinary.alias('configGenderForNonBinary'),
    ipeds_client_config_in.genderForUnknown.alias('configGenderForUnknown'),
    ipeds_client_config_in.grReportTransferOut.alias('configGrReportTransferOut'),
    ipeds_client_config_in.icOfferUndergradAwardLevel.alias('configIcOfferUndergradAwardLevel'),
    ipeds_client_config_in.icOfferGraduateAwardLevel.alias('configIcOfferGraduateAwardLevel'),
    ipeds_client_config_in.icOfferDoctorAwardLevel.alias('configIcOfferDoctorAwardLevel'),
    ipeds_client_config_in.includeNonDegreeAsUG.alias('configIncludeNonDegreeAsUG'),
    ipeds_client_config_in.instructionalActivityType.alias('configInstructionalActivityType'),
    ipeds_client_config_in.publicOrPrivateInstitution.alias('configPublicOrPrivateInstitution'),
    ipeds_client_config_in.recordActivityDate.alias('configRecordActivityDate'),
    ipeds_client_config_in.sfaGradStudentsOnly.alias('configSfaGradStudentsOnly'),
    ipeds_client_config_in.sfaLargestProgCIPC.alias('configSfaLargestProgCIPC'),
    ipeds_client_config_in.sfaReportPriorYear.alias('configSfaReportPriorYear'),
    ipeds_client_config_in.sfaReportSecondPriorYear.alias('configSfaReportSecondPriorYear'),
    ipeds_client_config_in.surveyCollectionYear.alias('configSurveyCollectionYear'),
    ipeds_client_config_in.tmAnnualDPPCreditHoursFTE.alias('configTmAnnualDPPCreditHoursFTE')
).withColumn(
    "isNonDegreeSeeking_calc",
    when(student_fft.stuRefStudyAbroadStatus != 'Study Abroad - Home Institution',
         student_fft.stuRefIsNonDegreeSeeking)
        .when((student_fft.stuRefTotalSAHomeCourses > 0) | (student_fft.stuRefTotalCreditHrs > 0) | (
            student_fft.stuRefTotalClockHrs > 0), False)
        .otherwise(student_fft.stuRefIsNonDegreeSeeking)
).withColumn(
    "studentType_calc",
    when((student_fft.studentLevelUGGR == 'UG') & (student_fft.stuRefTypeTermType == 'Fall') & (
            student_fft.stuRefStudentType == 'Continuing') & (student_fft.preFallStudType.isNotNull()),
         student_fft.preFallStudType)
        .otherwise(student_fft.stuRefStudentType)
).withColumn(
    "timeStatus_calc",
    expr("""
            (case when studentLevelUGGR = 'UG' and stuRefTotalCreditHrs is not null and stuRefTotalClockHrs is not null then
                    (case when configInstructionalActivityType in ('CR', 'B') then 
                            (case when stuRefTotalCreditHrs >= stuRefRequiredFTCreditHoursUG then 'Full Time' else 'Part Time' end)
                        when configInstructionalActivityType = 'CL' then 
                            (case when stuRefTotalClockHrs >= stuRefRequiredFTClockHoursUG then 'Full Time' else 'Part Time' end) 
                      else null end)
                when studentLevelUGGR = 'GR' and stuRefTotalCreditHrs is not null then
                    (case when stuRefTotalCreditHrs >= stuRefRequiredFTCreditHoursUG then 'Full Time' else 'Part Time' end)
            else null end)
    """)
    #    when((student_fft.studentLevelUGGR == 'UG') & (col('configInstructionalActivityType').isin('CR', 'B')) & (
    #            student_fft.stuRefTotalCreditHrs >= student_fft.stuRefRequiredFTCreditHoursUG), 'Full Time')
    #        .when((student_fft.studentLevelUGGR == 'UG') & (col('configInstructionalActivityType').isin('CR', 'B')) & (
    #            student_fft.stuRefTotalCreditHrs < student_fft.stuRefRequiredFTCreditHoursUG), 'Part Time')
    #        .when((student_fft.studentLevelUGGR == 'UG') & (col('configInstructionalActivityType') == 'CL') & (
    #            student_fft.stuRefTotalClockHrs >= student_fft.stuRefRequiredFTClockHoursUG), 'Full Time')
    #        .when((student_fft.studentLevelUGGR == 'UG') & (col('configInstructionalActivityType') == 'CL') & (
    #            student_fft.stuRefTotalClockHrs < student_fft.stuRefRequiredFTClockHoursUG), 'Part Time')
    #        .when((student_fft.studentLevelUGGR == 'GR') & (
    #                col('configInstructionalActivityType') >= student_fft.stuRefRequiredFTCreditHoursGR), 'Full Time')
    #        .when((student_fft.studentLevelUGGR == 'GR') & (
    #                col('configInstructionalActivityType') < student_fft.stuRefRequiredFTCreditHoursGR), 'Part Time')
    #        .otherwise(None)
).withColumn(
    "distanceEdInd_calc",
    expr("""
        (case when stuRefTotalDECourses = stuRefTotalCourses then 'Exclusive DE'
                      when stuRefTotalDECourses > 0 then 'Some DE'
        end)
    """))
#    when(student_fft.stuRefTotalDECourses == student_fft.stuRefTotalCourses, 'Exclusive DE')
#        .when(student_fft.stuRefTotalDECourses > 0, 'Some DE')
#        .otherwise('None'))

cohort_person = student_fft_out.join(
    person_in,
    (student_fft_out.regPersonId == person_in.personId) &
    (coalesce(person_in.isIPEDSReportable, lit(True)) == True) &
    ((coalesce(to_timestamp(person_in.recordActivityDate), to_timestamp(lit('9999-09-09'))) == to_timestamp(
        lit('9999-09-09')))
     | ((coalesce(to_timestamp(person_in.recordActivityDate), to_timestamp(lit('9999-09-09'))) != to_timestamp(
                lit('9999-09-09')))
        & (to_timestamp(person_in.recordActivityDate) <= to_timestamp(student_fft.stuRefCensusDate)))),
    'left').select(
    student_fft_out["*"],
    to_date(person_in.birthDate, 'YYYY-MM-DD').alias("persBirthDate"),
    upper(person_in.nation).alias("persNation"),
    upper(person_in.state).alias("persState"),
    (when(person_in.gender == 'Male', 'M')
     .when(person_in.gender == 'Female', 'F')
     .when(person_in.gender == 'Non-Binary', student_fft_out.configGenderForNonBinary)
     .otherwise(student_fft_out.configGenderForUnknown)).alias('persIpedsGender'),
    expr("""
        (case when isUSCitizen = 1 or ((coalesce(isInUSOnVisa, false) = 1 or stuRefCensusDate between visaStartDate and visaEndDate)
                            and visaType in ('Employee Resident', 'Other Resident')) then 
            (case when coalesce(isHispanic, false) = true then '2' 
                when coalesce(isMultipleRaces, false) = true then '8' 
                when ethnicity != 'Unknown' and ethnicity is not null then
                    (case when ethnicity = 'Hispanic or Latino' then '2'
                        when ethnicity = 'American Indian or Alaskan Native' then '3'
                        when ethnicity = 'Asian' then '4'
                        when ethnicity = 'Black or African American' then '5'
                        when ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
                        when ethnicity = 'Caucasian' then '7'
                        else '9' 
                    end) 
                else '9' end) 
            when ((coalesce(isInUSOnVisa, false) = 1 or stuRefCensusDate between person.visaStartDate and person.visaEndDate)
                and visaType in ('Student Non-resident', 'Employee Non-resident', 'Other Non-resident')) then '1'
            else '9'
        end) ipedsEthnicity
        """).alias('persIpedsEthnValue'),
    person_in.ethnicity.alias('ethnicity'),
    #    (when(coalesce(person_in.isUSCitizen, lit(True)) == True, 'Y')
    #     .when(((coalesce(person_in.isInUSOnVisa, lit(False)) == True) |
    #            ((student_fft.stuRefCensusDate >= to_date(person_in.visaStartDate)) & (
    #                    student_fft.stuRefCensusDate <= to_date(person_in.visaEndDate)) & (
    #                 person_in.visaType.isin('Employee Resident', 'Other Resident')))), 'Y')
    #     .when(((person_in.isInUSOnVisa == 1) | ((student_fft.stuRefCensusDate >= to_date(person_in.visaStartDate)) & (
    #            student_fft.stuRefCensusDate <= to_date(person_in.visaEndDate))))
    #           & (person_in.visaType.isin('Student Non-resident', 'Other Resident', 'Other Non-resident')),
    #           '1')  # non-resident alien
    #     .otherwise('9')).alias('persIpedsEthnInd'),
    #    (when(coalesce(person_in.isMultipleRaces, lit(False)) == True, '8')
    #     .when(((person_in.ethnicity == 'Hispanic or Latino') | (coalesce(person_in.isHispanic, lit(False))) == True), '2')
    #     .when(person_in.ethnicity == 'American Indian or Alaskan Native', '3')
    #     .when(person_in.ethnicity == 'Asian', '4')
    #     .when(person_in.ethnicity == 'Black or African American', '5')
    #     .when(person_in.ethnicity == 'Native Hawaiian or Other Pacific Islander', '6')
    #     .when(person_in.ethnicity == 'Caucasian', '7')
    #     .otherwise('9')).alias('persIpedsEthnValue'),
    to_timestamp(person_in.recordActivityDate).alias('persRecordActivityDate'),
    to_timestamp(person_in.snapshotDate).alias('persSnapshotDate')
).withColumn(
    'persRowNum',
    row_number().over(
        Window.partitionBy(
            col('repRefYearType'),
            col('repRefYearType'),
            col('stuRefFullTimePartTimeStatus'),
            col('regPersonId')).orderBy(
            when(col('persSnapshotDate') == col('stuRefSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
            when(col('persSnapshotDate') > col('stuRefSnapshotDate'), col('persSnapshotDate')).otherwise(
                to_timestamp(lit('9999-09-09'))).asc(),
            when(col('persSnapshotDate') < col('stuRefSnapshotDate'), col('persSnapshotDate')).otherwise(
                to_timestamp(lit('1900-09-09'))).desc(),
            col('persSnapshotDate').desc(),
            col('persRecordActivityDate').desc()))).filter(col('persRowNum') == 1)

academic_track = cohort_person.join(
    academic_track_in,
    (cohort_person.regPersonId == academic_track_in.personId) &
    (coalesce(academic_track_in.isIPEDSReportable, lit(True)) == True) &
    (academic_track_in.fieldOfStudyType == 'Major') &
    (((academic_track_in.fieldOfStudyActionDate != to_date(lit('9999-09-09'), 'YYYY-MM-DD')) & (
            academic_track_in.fieldOfStudyActionDate <= cohort_person.stuRefCensusDate))
     | ((academic_track_in.fieldOfStudyActionDate == to_date(lit('9999-09-09'), 'YYYY-MM-DD')) & (
                    academic_track_in.recordActivityDate != to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
        & (academic_track_in.recordActivityDate <= cohort_person.stuRefCensusDate))
     | ((academic_track_in.fieldOfStudyActionDate == to_date(lit('9999-09-09'), 'YYYY-MM-DD')) & (
                    academic_track_in.recordActivityDate == to_date(lit('9999-09-09'), 'YYYY-MM-DD'))))
    & (academic_track_in.snapshotDate <= cohort_person.stuRefCensusDate), 'left').join(
    academic_term_in,
    (academic_term_in.termCode == academic_track_in.termCodeEffective) &
    (academic_term_in.termCodeOrder <= cohort_person.stuRefTermCodeOrder), 'left').select(
    cohort_person["*"],
    upper(academic_track_in.degreeProgram).alias('acadTrkDegreeProgram'),
    academic_track_in.academicTrackStatus.alias('acadTrkAcademicTrackStatus'),
    coalesce(academic_track_in.fieldOfStudyPriority, lit(1)).alias('acadTrkFieldOfStudyPriority'),
    upper(academic_track_in.termCodeEffective).alias('acadTrkAcademicTrackTermCodeEffective'),
    academic_term_in.termCodeOrder.alias('acadTrkTermOrder'),
    to_timestamp(academic_track_in.fieldOfStudyActionDate).alias('acadTrkFieldOfStudyActionDate'),
    to_timestamp(academic_track_in.recordActivityDate).alias('acadTrkRecordActivityDate'),
    to_timestamp(academic_track_in.snapshotDate).alias('acadTrkSnapshotDate')
).withColumn(
    'acadTrkTrackRowNum',
    row_number().over(
        Window.partitionBy(
            col('repRefYearType'), col('regPersonId')).orderBy(
            when(col('acadTrkSnapshotDate') == col('stuRefSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
            when(col('acadTrkSnapshotDate') > col('stuRefSnapshotDate'), col('acadTrkSnapshotDate')).otherwise(
                to_timestamp(lit('9999-09-09'))).asc(),
            when(col('acadTrkSnapshotDate') < col('stuRefSnapshotDate'), col('acadTrkSnapshotDate')).otherwise(
                to_timestamp(lit('1900-09-09'))).desc(),
            col('acadTrkSnapshotDate').desc(),
            col('acadTrkFieldOfStudyPriority').asc(),
            col('acadTrkTermOrder').desc(),
            col('acadTrkRecordActivityDate').desc(),
            when(col('acadTrkAcademicTrackStatus') == lit('In Progress'), lit(1)).otherwise(lit(2)).asc()))).filter(
    col('acadTrkTrackRowNum') == 1)

degree_program = academic_track.join(
    degree_program_in,
    (academic_track.acadTrkDegreeProgram == degree_program_in.degreeProgram) &
    (coalesce(degree_program_in.isIPEDSReportable, lit(True)) == True) &
    ((coalesce(to_date(degree_program_in.recordActivityDate, 'YYYY-MM-DD'),
               to_date(lit('9999-09-09'), 'YYYY-MM-DD')) == to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
     | ((coalesce(to_date(degree_program_in.recordActivityDate, 'YYYY-MM-DD'),
                  to_date(lit('9999-09-09'), 'YYYY-MM-DD')) != to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
        & (to_date(degree_program_in.recordActivityDate, 'YYYY-MM-DD') <= to_date(academic_track.stuRefCensusDate,
                                                                               'YYYY-MM-DD'))))
    & (degree_program_in.snapshotDate <= academic_track.stuRefCensusDate), 'left').join(
    academic_term_in,
    (academic_term_in.termCode == degree_program_in.termCodeEffective) &
    (academic_term_in.termCodeOrder <= academic_track.stuRefTermCodeOrder), 'left').select(
    academic_track["*"],
    upper(degree_program_in.degreeProgram).alias('degProgDegreeProgram'),
    upper(degree_program_in.degree).alias('degProgDegree'),
    upper(degree_program_in.major).alias('degProgMajor'),
    degree_program_in.startDate.alias('degProgStartDate'),
    coalesce(degree_program_in.isESL, lit(False)).alias('degProgIsESL'),
    upper(degree_program_in.termCodeEffective).alias('degProgTermCodeEffective'),
    academic_term_in.termCodeOrder.alias('degProgTermOrder'),
    to_timestamp(degree_program_in.recordActivityDate).alias('degProgRecordActivityDate'),
    to_timestamp(degree_program_in.snapshotDate).alias('degProgSnapshotDate')
).withColumn(
    'degProgRowNum',
    row_number().over(
        Window.partitionBy(
            col('repRefYearType'),
            col('regPersonId'),
            col('acadTrkDegreeProgram'),
            col('degProgDegreeProgram')).orderBy(
            when(col('degProgSnapshotDate') == col('stuRefSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
            when(col('degProgSnapshotDate') > col('stuRefSnapshotDate'), col('degProgSnapshotDate')).otherwise(
                to_timestamp(lit('9999-09-09'))).asc(),
            when(col('degProgSnapshotDate') < col('stuRefSnapshotDate'), col('degProgSnapshotDate')).otherwise(
                to_timestamp(lit('1900-09-09'))).desc(),
            col('degProgSnapshotDate').desc(),
            col('degProgTermOrder').desc(),
            col('degProgStartDate').desc(),
            col('degProgRecordActivityDate').desc()))).filter(col('degProgRowNum') <= 1)

degree = degree_program.join(
    degree_in,
    (degree_program.degProgDegree == degree_in.degree) &
    (coalesce(degree_in.isIPEDSReportable, lit(True)) == True) &
    ((coalesce(to_date(degree_in.recordActivityDate, 'YYYY-MM-DD'),
               to_date(lit('9999-09-09'), 'YYYY-MM-DD')) == to_date(
        lit('9999-09-09'), 'YYYY-MM-DD'))
     | ((coalesce(to_date(degree_in.recordActivityDate, 'YYYY-MM-DD'),
                  to_date(lit('9999-09-09'), 'YYYY-MM-DD')) != to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
        & (to_date(degree_in.recordActivityDate, 'YYYY-MM-DD') <= to_date(degree_program.stuRefCensusDate,
                                                                       'YYYY-MM-DD'))))
    & (degree_in.snapshotDate <= degree_program.stuRefCensusDate), 'left').select(
    degree_program["*"],
    upper(degree_in.awardLevel).alias('degAwardLevel'),
    coalesce(degree_in.isNonDegreeSeeking, lit(False)).alias('degIsNonDegreeSeeking'),
    to_timestamp(degree_in.recordActivityDate).alias('degRecordActivityDate'),
    to_timestamp(degree_in.snapshotDate).alias('degSnapshotDate')
).withColumn(
    'degRowNum',
    row_number().over(
        Window.partitionBy(
            col('repRefYearType'),
            col('stuRefSurveySection'),
            col('regPersonId')).orderBy(
            when(col('degSnapshotDate') == col('stuRefSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
            when(col('degSnapshotDate') > col('stuRefSnapshotDate'), col('degSnapshotDate')).otherwise(
                to_timestamp(lit('9999-09-09'))).asc(),
            when(col('degSnapshotDate') < col('stuRefSnapshotDate'), col('degSnapshotDate')).otherwise(
                to_timestamp(lit('1900-09-09'))).desc(),
            col('degSnapshotDate').desc(),
            col('degRecordActivityDate').desc()))).filter(col('degRowNum') <= 1)

field_of_study = degree.join(
    field_of_study_in,
    (degree.degProgMajor == field_of_study_in.fieldOfStudy) &
    (field_of_study_in.fieldOfStudyType == 'Major') &
    (coalesce(field_of_study_in.isIPEDSReportable, lit(True)) == True) &
    ((coalesce(to_date(field_of_study_in.recordActivityDate, 'YYYY-MM-DD'),
               to_date(lit('9999-09-09'), 'YYYY-MM-DD')) == to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
     | ((coalesce(to_date(field_of_study_in.recordActivityDate, 'YYYY-MM-DD'),
                  to_date(lit('9999-09-09'), 'YYYY-MM-DD')) != to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
        & (to_date(field_of_study_in.recordActivityDate, 'YYYY-MM-DD') <= to_date(degree.stuRefCensusDate,
                                                                               'YYYY-MM-DD'))))
    & (field_of_study_in.snapshotDate <= degree.stuRefCensusDate), 'left').select(
    degree["*"],
    field_of_study_in.cipCode.alias('fldOfStdyCipCode'),
    to_timestamp(field_of_study_in.recordActivityDate).alias('fldOfStdyRecordActivityDate'),
    to_timestamp(field_of_study_in.snapshotDate).alias('fldOfStdySnapshotDate')
).withColumn(
    'fldOfStdyRowNum',
    row_number().over(
        Window.partitionBy(
            col('repRefYearType'),
            col('stuRefSurveySection'),
            col('regPersonId'),
            col('degProgMajor')).orderBy(
            when(col('fldOfStdySnapshotDate') == col('stuRefSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
            when(col('fldOfStdySnapshotDate') > col('stuRefSnapshotDate'), col('fldOfStdySnapshotDate')).otherwise(
                to_timestamp(lit('9999-09-09'))).asc(),
            when(col('fldOfStdySnapshotDate') < col('stuRefSnapshotDate'), col('fldOfStdySnapshotDate')).otherwise(
                to_timestamp(lit('1900-09-09'))).desc(),
            col('fldOfStdySnapshotDate').desc(),
            col('fldOfStdyRecordActivityDate').desc()))).filter(col('fldOfStdyRowNum') <= 1)

cohort = field_of_study.withColumn(
    'ipedsInclude',
    #    when((col('stuRefTotalCECourses') == col('stuRefTotalCourses'))
    #         | (col('stuRefTotalIntlCourses') == col('stuRefTotalCourses'))
    #         | (col('stuRefTotalAuditCourses') == col('stuRefTotalCourses'))
    #         | ((col('stuRefTotalRemCourses') == col('stuRefTotalCourses')) & (
    #            col('isNonDegreeSeeking_calc') == lit(False)))
    #         # | {ESLFilter}
    #         # | {GradFilter}
    #         | (col('stuRefTotalSAHomeCourses') > lit(0))
    #         | (col('stuRefTotalCreditHrs') > lit(0))
    #         | (col('stuRefTotalClockHrs') > lit(0)), lit(1)).otherwise(0))
    expr("""     
    (case when stuRefTotalCECourses = stuRefTotalCourses then 0 
        when stuRefTotalIntlCourses = stuRefTotalCourses then 0 
        when stuRefTotalAuditCourses = stuRefTotalCourses then 0 
        when stuRefTotalProfResidencyCourses > 0 then 0 
        when stuRefTotalThesisCourses > 0 then 0 
        when stuRefTotalRemCourses = stuRefTotalCourses and isNonDegreeSeeking_calc = false then 1 
        when stuRefTotalESLCourses = stuRefTotalCourses and isNonDegreeSeeking_calc = false then 1 
        when stuRefTotalSAHomeCourses > 0 then 1 
        when stuRefTotalCreditHrs > 0 then 1
        when stuRefTotalClockHrs > 0 then 1
        else 0
    end) 
    """)
)

def run_twelve_month_enrollment_query():
    cohort_out = cohort.select(
        cohort["*"],
        expr("""
        (case when studentLevelUGGR = 'GR' then '99' 
             when isNonDegreeSeeking_calc = true and timeStatus_calc = 'Full Time' then '7'
             when isNonDegreeSeeking_calc = true and timeStatus_calc = 'Part Time' then '21'
             when studentLevelUGGR = 'UG' then 
                (case when studentType_calc = 'First Time' and timeStatus_calc = 'Full Time' then '1' 
                        when studentType_calc = 'Transfer' and timeStatus_calc = 'Full Time' then '2'
                        when studentType_calc = 'Returning' and timeStatus_calc = 'Full Time' then '3'
                        when studentType_calc = 'First Time' and timeStatus_calc = 'Part Time' then '15' 
                        when studentType_calc = 'Transfer' and timeStatus_calc = 'Part Time' then '16'
                        when studentType_calc = 'Returning' and timeStatus_calc = 'Part Time' then '17' else '1' 
                 end)
            else null
        end)
        """).alias("ipedsPartAStudentLevel"),
        expr("""
        case when studentLevelUGGR = 'GR' then '3'
             when isNonDegreeSeeking_calc = 1 then '2'
             when studentLevelUGGR = 'UG' then '1'
             else null
        end
        """).alias("ipedsPartCStudentLevel")
    ).filter(cohort.ipedsInclude == 1)

    # CourseLevelCounts
    # course_type_counts = spark.sql(func_courseLevelCounts(repPeriod = 'global_reportingPeriodRefactor', termOrder = 'global_reportingPeriodOrder', instructionalActivityType = config_instructionalActivityType))
    course_type_counts = course_type_counts_in 
    
    course_type_counts_out = course_type_counts.join(
        cohort,
        (cohort.regPersonId == course_type_counts.regPersonId), 'inner').filter(cohort.ipedsInclude == 1).select(
        course_type_counts["*"]).agg(
        sum("UGCreditHours").alias("UGCreditHours"),
        sum("UGClockHours").alias("UGClockHours"),
        sum("GRCreditHours").alias("GRCreditHours"),
        sum("DPPCreditHours").alias("DPPCreditHours"))

    # Survey version output lists
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
    else:  # V4
        A_UgGrBoth = ["1", "3", "15", "17"]
        A_UgOnly = ["1", "3", "15", "17"]
        A_GrOnly = [""]
        C_UgGrBoth = ["1", "2", "3"]
        C_UgOnly = ["1", "2"]
        C_GrOnly = ["3"]

    # Part A
    if cohort_out.rdd.isEmpty() == False:
        # FormatPartA
        a_columns = ["regPersonId", "ipedsPartAStudentLevel", "persIpedsEthnValue", "persIpedsGender"]
        a_data = [("", "1", "", ""), ("", "2", "", ""), ("", "3", "", ""), ("", "7", "", ""), ("", "15", "", ""),
                  ("", "16", "", ""), ("", "17", "", ""), ("", "21", "", ""), ("", "99", "", "")]
        FormatPartA = sparkContext.parallelize(a_data)
        FormatPartA = spark.createDataFrame(FormatPartA).toDF(*a_columns)

        partA_out = cohort_out.select("regPersonId", "ipedsPartAStudentLevel", "persIpedsEthnValue",
                                      "persIpedsGender").filter(
            (cohort_out.ipedsPartAStudentLevel.isNotNull()) &
            (cohort_out.ipedsPartAStudentLevel != '')).union(FormatPartA)

        partA_out = partA_out.select(
            partA_out.ipedsPartAStudentLevel.alias("field1"),
            when(((col('persIpedsEthnValue') == lit('1')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
                lit('0')).alias("field2"),  # FYRACE01 - Nonresident alien - Men (1), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('1')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
                lit('0')).alias("field3"),  # FYRACE02 - Nonresident alien - Women (2), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('2')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
                lit('0')).alias("field4"),  # FYRACE25 - Hispanic/Latino - Men (25), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('2')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
                lit('0')).alias("field5"),  # FYRACE26 - Hispanic/Latino - Women (26), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('3')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
                lit('0')).alias("field6"),  # FYRACE27 - American Indian or Alaska Native - Men (27), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('3')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
                lit('0')).alias("field7"),  # FYRACE28 - American Indian or Alaska Native - Women (28), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('4')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
                lit('0')).alias("field8"),  # FYRACE29 - Asian - Men (29), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('4')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
                lit('0')).alias("field9"),  # FYRACE30 - Asian - Women (30), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('5')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
                lit('0')).alias("field10"),  # FYRACE31 - Black or African American - Men (31), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('5')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
                lit('0')).alias("field11"),  # FYRACE32 - Black or African American - Women (32), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('6')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
                lit('0')).alias("field12"),
            # FYRACE33 - Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('6')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
                lit('0')).alias("field13"),
            # FYRACE34 - Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('7')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
                lit('0')).alias("field14"),  # FYRACE35 - White - Men (35), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('7')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
                lit('0')).alias("field15"),  # FYRACE36 - White - Women (36), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('8')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
                lit('0')).alias("field16"),  # FYRACE37 - Two or more races - Men (37), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('8')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
                lit('0')).alias("field17"),  # FYRACE38 - Two or more races - Women (38), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('9')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
                lit('0')).alias("field18"),  # FYRACE13 - Race and ethnicity unknown - Men (13), 0 to 999999
            when(((col('persIpedsEthnValue') == lit('9')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
                lit('0')).alias("field19"))  # FYRACE14 - Race and ethnicity unknown - Women (14), 0 to 999999

        partA_out = partA_out.withColumn('part', lit('A')).groupBy("part", "field1").agg(
            sum("field2").alias("field2"),
            sum("field3").alias("field3"),
            sum("field4").alias("field4"),
            sum("field5").alias("field5"),
            sum("field6").alias("field6"),
            sum("field7").alias("field7"),
            sum("field8").alias("field8"),
            sum("field9").alias("field9"),
            sum("field10").alias("field10"),
            sum("field11").alias("field11"),
            sum("field12").alias("field12"),
            sum("field13").alias("field13"),
            sum("field14").alias("field14"),
            sum("field15").alias("field15"),
            sum("field16").alias("field16"),
            sum("field17").alias("field17"),
            sum("field18").alias("field18"),
            sum("field19").alias("field19")
        )

    else:
        a_columns = ["part", "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9",
                     "field10", "field11", "field12", "field13", "field14", "field15", "field16", "field17", "field18",
                     "field19"]
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

    # Part A output filter
    partA_out = partA_out.crossJoin(ipeds_client_config_in).filter(
        (((col('icOfferUndergradAwardLevel') == 'Y') & (col('icOfferGraduateAwardLevel') == 'Y') & (
            col('field1').isin(A_UgGrBoth)))
         | ((col('icOfferUndergradAwardLevel') == 'Y') & (col('icOfferGraduateAwardLevel') == 'N') & (
                    col('field1').isin(A_UgOnly)))
         | ((col('icOfferUndergradAwardLevel') == 'N') & (col('icOfferGraduateAwardLevel') == 'Y') & (
                    col('field1').isin(A_GrOnly)))
         | ((col('icOfferUndergradAwardLevel') == 'N') & (col('icOfferGraduateAwardLevel') == 'N')))).select(
        partA_out['*'])

    # Part C
    if cohort_out.rdd.isEmpty() == False:
        # FormatPartC
        c_columns = ["regPersonId", "ipedsLevel", "distanceEdInd"]
        c_data = [("", "1", ""), ("", "2", ""), ("", "3", "")]
        FormatPartC = sparkContext.parallelize(c_data)
        FormatPartC = spark.createDataFrame(FormatPartC).toDF(*c_columns)

        # Part C
        partC_out = cohort_out.select("regPersonId", "ipedsPartCStudentLevel", "distanceEdInd_calc").filter(
            (cohort_out.ipedsPartCStudentLevel.isNotNull()) & (cohort_out.ipedsPartCStudentLevel != '') & (
                    cohort_out.distanceEdInd_calc != 'None')).union(FormatPartC)
        partC_out = partC_out.select(
            partC_out.ipedsPartCStudentLevel.alias("field1"),
            when((col('distanceEdInd_calc') == lit('Exclusive DE')), lit('1')).otherwise(lit('0')).alias("field2"),
            # Enrolled exclusively in distance education courses
            when((col('distanceEdInd_calc') == lit('Some DE')), lit('1')).otherwise(lit('0')).alias(
                "field3"))  # Enrolled in at least one but not all distance education courses

        partC_out = partC_out.withColumn('part', lit('C')).groupBy("part", "field1").agg(
            sum("field2").alias("field2"),
            sum("field3").alias("field3"))

    else:
        c_columns = ["part", "field1", "field2", "field3"]
        c_data = [("C", "1", "0", "0"), ("C", "2", "0", "0"), ("C", "3", "0", "0")]
        partC_out = sparkContext.parallelize(c_data)
        partC_out = spark.createDataFrame(partC_out).toDF(*c_columns)

    # Part B
    if course_type_counts_out.rdd.isEmpty() == False:
        partB_out = course_type_counts_out.crossJoin(ipeds_client_config_in).withColumn('part', lit('B')).select(
            'part',
            # CREDHRSU - credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.
            when(((col('icOfferUndergradAwardLevel') == lit('Y')) & (col('instructionalActivityType') == lit('CL'))),
                 coalesce(col('UGCreditHours'), lit(0))).alias('field2'),
            # CONTHRS  - clock hour instructional activity at the undergraduate level, 0 to 9999999, blank = not applicable, if no undergraduate programs are measured in clock hours.
            when(((col('icOfferUndergradAwardLevel') == lit('Y')) & (col('instructionalActivityType') == lit('CR'))),
                 coalesce(col('UGClockHours'), lit(0))).alias('field3'),
            # CREDHRSG - credit hour instructional activity at the graduate level, 0 to 99999999, blank = not applicable
            expr(
                f"round((case when icOfferGraduateAwardLevel = 'Y' and '{var_surveyId}' = 'E1D' then coalesce(GRCreditHours, 0) else null end))").cast(
                'int').alias("field4"),
            # RDOCFTE  - reported Doctor'92s degree-professional practice student FTE, 0 to 99999999, blank = not applicable
            expr(f"""round((case when icOfferDoctorAwardLevel = 'Y' and '{var_surveyId}' = 'E1D' then 
                                (case when coalesce(DPPCreditHours, 0) > 0 then coalesce(cast(round(DPPCreditHours / tmAnnualDPPCreditHoursFTE, 0) as string), '0') 
                            else '0' end) else null end))""").cast('int').alias("field5"))

    else:
        b_columns = ["part", "field2", "field3", "field4", "field5"]
        b_data = [("B", "0", "", "0", "0")]
        partB_out = sparkContext.parallelize(b_data)
        partB_out = spark.createDataFrame(partB_out).toDF(*b_columns)

    # Survey out formatting
    for column in [column for column in partB_out.columns if column not in partA_out.columns]:
        partA_out = partA_out.withColumn(column, lit(None))

    for column in [column for column in partA_out.columns if column not in partC_out.columns]:
        partC_out = partC_out.withColumn(column, lit(None))

    for column in [column for column in partA_out.columns if column not in partB_out.columns]:
        partB_out = partB_out.withColumn(column, lit(None))

    surveyOutput = partA_out.unionByName(partC_out).unionByName(partB_out)

    # surveyOutput.show()

    return surveyOutput
