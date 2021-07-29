import logging
import sys
import boto3
import json
from uuid import uuid4
from common import query_helpers
from pyspark.sql.window import Window
#from queries.twelve_month_enrollment_query import run_twelve_month_enrollment_query
from pyspark.sql.functions import sum as sum, expr, col, lit, upper, to_timestamp, max, min, row_number, date_trunc, \
    to_date, when, coalesce, count, rank
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from awsglue.utils import getResolvedOptions

spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()
#spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).config("spark.dynamicAllocation.enabled", 'true').getOrCreate()
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

def ipeds_client_config_mcr(ipeds_client_config_partition, ipeds_client_config_order,
                            ipeds_client_config_partition_filter):
    ipeds_client_config_in = spark.sql('select * from ipedsClientConfig')

    ipeds_client_config = ipeds_client_config_in.filter(expr(f"{ipeds_client_config_partition_filter}")).select(
        coalesce(upper(col('acadOrProgReporter')), lit('A')).alias('acadOrProgReporter'),  # 'A'
        coalesce(upper(col('admAdmissionTestScores')), lit('R')).alias('admAdmissionTestScores'),  # 'R'
        coalesce(upper(col('admCollegePrepProgram')), lit('R')).alias('admCollegePrepProgram'),  # 'R'
        coalesce(upper(col('admDemoOfCompetency')), lit('R')).alias(' admDemoOfCompetency'),  # 'R'
        coalesce(upper(col('admOtherTestScores')), lit('R')).alias('admOtherTestScores'),  # 'R'
        coalesce(upper(col('admRecommendation')), lit('R')).alias('admRecommendation'),  # 'R'
        coalesce(upper(col('admSecSchoolGPA')), lit('R')).alias('admSecSchoolGPA'),  # 'R'
        coalesce(upper(col('admSecSchoolRank')), lit('R')).alias('admSecSchoolRank'),  # 'R'
        coalesce(upper(col('admSecSchoolRecord')), lit('R')).alias('admSecSchoolRecord'),  # 'R'
        coalesce(upper(col('admTOEFL')), lit('R')).alias('admTOEFL'),  # 'R'
        coalesce(upper(col('admUseForBothSubmitted')), lit('B')).alias('admUseForBothSubmitted'),  # 'B'
        coalesce(upper(col('admUseForMultiOfSame')), lit('H')).alias('admUseForMultiOfSame'),  # 'H'
        coalesce(upper(col('admUseTestScores')), lit('B')).alias('admUseTestScores'),  # 'B'
        coalesce(upper(col('compGradDateOrTerm')), lit('D')).alias('compGradDateOrTerm'),  # 'D'
        upper(col('eviReserved1')).alias('eviReserved1'),  # ' '
        upper(col('eviReserved2')).alias('eviReserved2'),  # ' '
        upper(col('eviReserved3')).alias('eviReserved3'),  # ' '
        upper(col('eviReserved4')).alias('eviReserved4'),  # ' '
        upper(col('eviReserved5')).alias('eviReserved5'),  # ' '
        coalesce(upper(col('feIncludeOptSurveyData')), lit('Y')).alias('feIncludeOptSurveyData'),  # 'Y'
        coalesce(upper(col('finAthleticExpenses')), lit('A')).alias('finAthleticExpenses'),  # 'A'
        coalesce(upper(col('finBusinessStructure')), lit('LLC')).alias('finBusinessStructure'),  # 'LLC'
        coalesce(upper(col('finEndowmentAssets')), lit('Y')).alias('finEndowmentAssets'),  # 'Y'
        coalesce(upper(col('finGPFSAuditOpinion')), lit('U')).alias('finGPFSAuditOpinion'),  # 'U'
        coalesce(upper(col('finParentOrChildInstitution')), lit('P')).alias('finParentOrChildInstitution'),  # 'P'
        coalesce(upper(col('finPellTransactions')), lit('P')).alias('finPellTransactions'),  # 'P'
        coalesce(upper(col('finPensionBenefits')), lit('Y')).alias('finPensionBenefits'),  # 'Y'
        coalesce(upper(col('finReportingModel')), lit('B')).alias('finReportingModel'),  # 'B'
        coalesce(upper(col('finTaxExpensePaid')), lit('B')).alias('finTaxExpensePaid'),  # 'B'
        coalesce(upper(col('fourYrOrLessInstitution')), lit('F')).alias('fourYrOrLessInstitution'),  # 'F'
        coalesce(upper(col('genderForNonBinary')), lit('F')).alias('genderForNonBinary'),  # 'F'
        coalesce(upper(col('genderForUnknown')), lit('F')).alias('genderForUnknown'),  # 'F'
        coalesce(upper(col('grReportTransferOut')), lit('N')).alias('grReportTransferOut'),  # 'N'
        coalesce(upper(col('hrIncludeSecondarySalary')), lit('N')).alias('hrIncludeSecondarySalary'),  # 'N'
        coalesce(upper(col('icOfferDoctorAwardLevel')), lit('Y')).alias('icOfferDoctorAwardLevel'),  # 'Y'
        coalesce(upper(col('icOfferGraduateAwardLevel')), lit('Y')).alias('icOfferGraduateAwardLevel'),  # 'Y'
        coalesce(upper(col('icOfferUndergradAwardLevel')), lit('Y')).alias('icOfferUndergradAwardLevel'),  # 'Y'
        coalesce(upper(col('includeNonDegreeAsUG')), lit('Y')).alias('includeNonDegreeAsUG'),  # 'Y'
        coalesce(upper(col('instructionalActivityType')), lit('CR')).alias('instructionalActivityType'),  # 'CR'
        coalesce(upper(col('ncBranchCode')), lit('00')).alias('ncBranchCode'),  # '00'
        coalesce(upper(col('ncSchoolCode')), lit('000000')).alias('ncSchoolCode'),  # '000000'
        coalesce(upper(col('ncSchoolName')), lit('XXXXX')).alias('ncSchoolName'),  # 'XXXXX'
        coalesce(upper(col('publicOrPrivateInstitution')), lit('U')).alias('publicOrPrivateInstitution'),  # 'U'
        to_timestamp(col('recordActivityDate')).alias('recordActivityDate'),  # '9999-09-09'
        coalesce(upper(col('sfaGradStudentsOnly')), lit('N')).alias('sfaGradStudentsOnly'),  # 'N'
        upper(col('sfaLargestProgCIPC')).alias('sfaLargestProgCIPC'),  # 'null
        coalesce(upper(col('sfaReportPriorYear')), lit('N')).alias('sfaReportPriorYear'),  # 'N'
        coalesce(upper(col('sfaReportSecondPriorYear')), lit('N')).alias('sfaReportSecondPriorYear'),  # 'N'
        coalesce(upper(col('surveyCollectionYear')), lit('2021')).alias('surveyCollectionYear'),  # '2021'
        coalesce(upper(col('tmAnnualDPPCreditHoursFTE')), lit('12')).alias('tmAnnualDPPCreditHoursFTE'),  # '12'
        to_timestamp(col('snapshotDate')).alias('snapshotDate'),
        ipeds_client_config_in.tags).withColumn(
        'clientConfigRowNum',
        row_number().over(Window.partitionBy(
            expr(f"({ipeds_client_config_partition})")).orderBy(expr(f"{ipeds_client_config_order}")))).filter(
        col('clientConfigRowNum') <= 1).limit(1).cache()

    return ipeds_client_config


def academic_term_mcr(academic_term_partition, academic_term_order, academic_term_partition_filter):
    academic_term_in = spark.sql('select * from academicTerm')

    # Should be able to switch to this\/ and remove this /\ when moving to a script
    academic_term_2 = academic_term_in.filter(expr(f"{academic_term_partition_filter}")).select(
        academic_term_in.academicYear,
        to_timestamp(academic_term_in.censusDate).alias('censusDate'),
        to_timestamp(academic_term_in.endDate).alias('endDate'),
        academic_term_in.financialAidYear,
        # academic_term_in.isIPEDSReportable,
        upper(academic_term_in.partOfTermCode).alias('partOfTermCode'),
        academic_term_in.partOfTermCodeDescription,
        to_timestamp(academic_term_in.recordActivityDate).alias('recordActivityDate'),
        academic_term_in.requiredFTCreditHoursGR,
        academic_term_in.requiredFTCreditHoursUG,
        academic_term_in.requiredFTClockHoursUG,
        # expr(col("requiredFTCreditHoursUG")/coalesce(col("requiredFTClockHoursUG"), col("requiredFTCreditHoursUG"))).alias("equivCRHRFactor"),
        to_timestamp(academic_term_in.startDate).alias('startDate'),
        academic_term_in.termClassification,
        upper(academic_term_in.termCode).alias('termCode'),
        # academic_term_in.termCodeDescription,
        academic_term_in.termType,
        to_timestamp(academic_term_in.snapshotDate).alias('snapshotDate'),
        academic_term_in.tags).withColumn(
        'acadTermRowNum',
        row_number().over(Window.partitionBy(
            expr(f"({academic_term_partition})")).orderBy(expr(f"{academic_term_order}")))).filter(
        col('acadTermRowNum') == 1)

    academic_term_order = academic_term_2.select(
        academic_term_2.termCode,
        academic_term_2.partOfTermCode,
        academic_term_2.censusDate,
        academic_term_2.startDate,
        academic_term_2.endDate).distinct()

    part_of_term_order = academic_term_order.select(
        academic_term_order["*"],
        rank().over(Window.orderBy(col('censusDate').asc(), col('startDate').asc())).alias('partOfTermOrder')).where(
        (col("termCode").isNotNull()) & (col("partOfTermCode").isNotNull()))

    academic_term_order_max = part_of_term_order.groupBy('termCode').agg(
        max(part_of_term_order.partOfTermOrder).alias('termCodeOrder'),
        max(part_of_term_order.censusDate).alias('maxCensus'),
        min(part_of_term_order.startDate).alias('minStart'),
        max("endDate").alias("maxEnd"))

    academic_term_3 = academic_term_2.join(
        part_of_term_order,
        (academic_term_2.termCode == part_of_term_order.termCode) &
        (academic_term_2.partOfTermCode == part_of_term_order.partOfTermCode), 'inner').select(
        academic_term_2["*"],
        part_of_term_order.partOfTermOrder).where(col("termCode").isNotNull())

    academic_term = academic_term_3.join(
        academic_term_order_max,
        (academic_term_3.termCode == academic_term_order_max.termCode), 'inner').select(
        academic_term_3["*"],
        academic_term_order_max.termCodeOrder,
        academic_term_order_max.maxCensus,
        academic_term_order_max.minStart,
        academic_term_order_max.maxEnd).distinct()  # .cache()

    return academic_term


def academic_term_reporting_refactor(
        ipeds_reporting_period_partition, ipeds_reporting_period_order,
        ipeds_reporting_period_partition_filter,
        academic_term_partition,
        academic_term_order,
        academic_term_partition_filter):
            
    academic_term = academic_term_mcr(academic_term_partition, academic_term_order, academic_term_partition_filter)

    ipeds_reporting_period_in = spark.sql("select * from ipedsReportingPeriod")

    # ipeds_reporting_period_2 = academic_term.join(broadcast(ipeds_reporting_period_in),
    ipeds_reporting_period_2 = academic_term.join(ipeds_reporting_period_in,
                                                  ((academic_term.termCode == upper(
                                                      ipeds_reporting_period_in.termCode)) &
                                                   (academic_term.partOfTermCode == coalesce(
                                                       upper(ipeds_reporting_period_in.partOfTermCode), lit('1')))),
                                                  'inner').filter(
        expr(f"{ipeds_reporting_period_partition_filter}")).select(
        upper(ipeds_reporting_period_in.partOfTermCode).alias('partOfTermCode'),
        to_timestamp(ipeds_reporting_period_in.recordActivityDate).alias('recordActivityDate'),
        ipeds_reporting_period_in.surveyCollectionYear,
        upper(ipeds_reporting_period_in.surveyId).alias('surveyId'),
        upper(ipeds_reporting_period_in.surveyName).alias('surveyName'),
        upper(ipeds_reporting_period_in.surveySection).alias('surveySection'),
        upper(ipeds_reporting_period_in.termCode).alias('termCode'),
        to_timestamp(ipeds_reporting_period_in.snapshotDate).alias('snapshotDate'),
        ipeds_reporting_period_in.tags,
        when(upper(ipeds_reporting_period_in.surveySection).isin('PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER'),
             'PY').when(
            upper(ipeds_reporting_period_in.surveySection).isin('COHORT', 'PRIOR SUMMER'), 'CY').alias('yearType'),
        academic_term.termCodeOrder,
        academic_term.partOfTermOrder,
        academic_term.maxCensus,
        academic_term.minStart,
        academic_term.maxEnd,
        academic_term.censusDate,
        academic_term.termClassification,
        academic_term.termType,
        academic_term.startDate,
        academic_term.endDate,
        academic_term.requiredFTCreditHoursGR,
        academic_term.requiredFTCreditHoursUG,
        academic_term.requiredFTClockHoursUG,
        academic_term.financialAidYear
    ).withColumn(
        'fullTermOrder',
        expr("""       
                    (case when termClassification = 'Standard Length' then 1
                        when termClassification is null then (case when termType in ('Fall', 'Spring') then 1 else 2 end)
                        else 2
                    end) 
                """)
    ).withColumn(
        'equivCRHRFactor',
        expr("(coalesce(requiredFTCreditHoursUG/coalesce(requiredFTClockHoursUG, requiredFTCreditHoursUG), 1))")
    ).withColumn(
        'ipedsRepPerRowNum',
        row_number().over(Window.partitionBy(
            expr(f"({ipeds_reporting_period_partition})")).orderBy(expr(f"{ipeds_reporting_period_order}")))).filter(
        (col('ipedsRepPerRowNum') == 1) & (col('termCode').isNotNull()) & (col('partOfTermCode').isNotNull())
    ).withColumn(
        'rowNum',
        row_number().over(Window.partitionBy(
            expr("(termCode, partOfTermCode)")).orderBy(
            expr("""
                            ((case when snapshotDate <= to_date(date_add(censusdate, 3), 'YYYY-MM-DD') 
                                        and snapshotDate >= to_date(date_sub(censusDate, 1), 'YYYY-MM-DD') 
                                        and ((array_contains(tags, 'Fall Census') and termType = 'Fall')
                                            or (array_contains(tags, 'Spring Census') and termType = 'Spring')
                                            or (array_contains(tags, 'Pre-Fall Summer Census') and termType = 'Summer')
                                            or (array_contains(tags, 'Post-Fall Summer Census') and termType = 'Summer')) then 1
                                when snapshotDate <= to_date(date_add(censusdate, 3), 'YYYY-MM-DD') 
                                    and snapshotDate >= to_date(date_sub(censusDate, 1), 'YYYY-MM-DD') then 2
                                else 3 end) asc,
                            (case when snapshotDate > censusDate then snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                            (case when snapshotDate < censusDate then snapshotDate else CAST('1900-09-09' as DATE) end) desc)
                        """)))).filter(col('rowNum') == 1).cache()

    max_term_order_summer = ipeds_reporting_period_2.filter(ipeds_reporting_period_2.termType == 'Summer').select(
        max(ipeds_reporting_period_2.termCodeOrder).alias('maxSummerTerm'))

    max_term_order_fall = ipeds_reporting_period_2.filter(ipeds_reporting_period_2.termType == 'Fall').select(
        max(ipeds_reporting_period_2.termCodeOrder).alias('maxFallTerm'))

    academic_term_reporting_refactor = ipeds_reporting_period_2.crossJoin(max_term_order_summer).crossJoin(
        max_term_order_fall).withColumn(
        'termTypeNew',
        expr(
            "(case when termType = 'Summer' and termClassification != 'Standard Length' then (case when maxSummerTerm < maxFallTerm then 'Pre-Fall Summer' else 'Post-Spring Summer' end) else termType end)")).cache()

    # ipeds_reporting_period_2.unpersist()

    return academic_term_reporting_refactor
    
def ipeds_course_type_counts():
    registration_in = spark.sql("select * from registration").filter(col('isIpedsReportable') == True)
    course_section_in = spark.sql("select * from courseSection").filter(col('isIpedsReportable') == True)
    course_section_schedule_in = spark.sql("select * from courseSectionSchedule").filter(
        col('isIpedsReportable') == True)
    course_in = spark.sql("select * from course").filter(col('isIpedsReportable') == True)
    campus_in = spark.sql("select * from campus").filter(col('isIpedsReportable') == True)

    registration = registration_in.join(
        #broadcast(academic_term_reporting_refactor),
        academic_term_reporting_refactor,
        (registration_in.termCode == academic_term_reporting_refactor.termCode) &
        (coalesce(registration_in.partOfTermCode, lit('1')) == academic_term_reporting_refactor.partOfTermCode) &
        (((registration_in.registrationStatusActionDate != to_timestamp(lit('9999-09-09'))) & (
                registration_in.registrationStatusActionDate <= academic_term_reporting_refactor.censusDate))
         | ((registration_in.registrationStatusActionDate == to_timestamp(lit('9999-09-09')))
            & (registration_in.recordActivityDate != to_timestamp(lit('9999-09-09')))
            & (registration_in.recordActivityDate <= academic_term_reporting_refactor.censusDate))
         | ((registration_in.registrationStatusActionDate == to_timestamp(lit('9999-09-09')))
            & (registration_in.recordActivityDate == to_timestamp(lit('9999-09-09'))))) &
        (registration_in.snapshotDate <= academic_term_reporting_refactor.censusDate) &
        (coalesce(registration_in.isIPEDSReportable, lit(True))), 'inner').select(
        registration_in.personId.alias('regPersonId'),
        to_timestamp(registration_in.snapshotDate).alias('regSnapshotDate'),
        upper(registration_in.termCode).alias('regTermCode'),
        coalesce(upper(registration_in.partOfTermCode), lit('1')).alias('regPartOfTermCode'),
        upper(registration_in.courseSectionNumber).alias('regCourseSectionNumber'),
        upper(registration_in.courseSectionCampusOverride).alias('regCourseSectionCampusOverride'),
        upper(registration_in.courseSectionLevelOverride).alias('regCourseSectionLevelOverride'),
        coalesce(registration_in.isAudited, lit(False)).alias('regIsAudited'),
        coalesce(registration_in.isEnrolled, lit(True)).alias('regIsEnrolled'),
        coalesce(registration_in.registrationStatusActionDate, to_timestamp(lit('9999-09-09'))).alias(
            'regStatusActionDate'),
        coalesce(registration_in.recordActivityDate, to_timestamp(lit('9999-09-09'))).alias('regRecordActivityDate'),
        registration_in.enrollmentHoursOverride.alias('regEnrollmentHoursOverride'),
        academic_term_reporting_refactor.snapshotDate.alias('repRefSnapshotDate'),
        academic_term_reporting_refactor.yearType.alias('repRefYearType'),
        academic_term_reporting_refactor.surveySection.alias('repRefSurveySection'),
        academic_term_reporting_refactor.financialAidYear.alias('repRefFinancialAidYear'),
        academic_term_reporting_refactor.termCodeOrder.alias('repRefTermCodeOrder'),
        academic_term_reporting_refactor.maxCensus.alias('repRefMaxCensus'),
        academic_term_reporting_refactor.fullTermOrder.alias('repRefFullTermOrder'),
        academic_term_reporting_refactor.termTypeNew.alias('repRefTermTypeNew'),
        academic_term_reporting_refactor.startDate.alias('repRefStartDate'),
        academic_term_reporting_refactor.censusDate.alias('repRefCensusDate'),
        academic_term_reporting_refactor.equivCRHRFactor.alias('repRefEquivCRHRFactor')).withColumn(
        'regRowNum',
        row_number().over(
            Window.partitionBy(
                col('repRefYearType'),
                col('repRefSurveySection'),
                col('regTermCode'),
                col('regPartOfTermCode'),
                col('regPersonId'),
                col('regCourseSectionNumber'),
                col('RegcourseSectionLevelOverride')).orderBy(
                when(col('regSnapshotDate') == col('repRefSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
                when(col('regSnapshotDate') > col('repRefSnapshotDate'), col('regSnapshotDate')).otherwise(
                    to_timestamp(lit('9999-09-09'))).asc(),
                when(col('regSnapshotDate') < col('repRefSnapshotDate'), col('regSnapshotDate')).otherwise(
                    to_timestamp(lit('1900-09-09'))).desc(),
                col('regSnapshotDate').desc(),
                col('regRecordActivityDate').desc(),
                col('regStatusActionDate').desc()))).filter(
        (col('regRowNum') == 1) & col('regIsEnrolled') == lit('True'))

    registration_course_section = registration.join(
        course_section_in,
        (registration.regTermCode == course_section_in.termCode) &
        (registration.regPartOfTermCode == course_section_in.partOfTermCode) &
        (registration.regCourseSectionNumber == course_section_in.courseSectionNumber) &
        (course_section_in.termCode.isNotNull()) &
        (coalesce(course_section_in.partOfTermCode, lit('1')).isNotNull()) &
        (((course_section_in.recordActivityDate != to_timestamp(lit('9999-09-09'))) & (
                course_section_in.recordActivityDate <= registration.repRefCensusDate))
         | (course_section_in.recordActivityDate == to_timestamp(lit('9999-09-09')))) &
        (coalesce(course_section_in.isIPEDSReportable, lit(True)) == lit(True)), 'left').select(
        to_timestamp(course_section_in.recordActivityDate).alias('crseSectRecordActivityDate'),
        course_section_in.courseSectionLevel.alias('crseSectCourseSectionLevel'),
        upper(course_section_in.subject).alias('crseSectSubject'),
        upper(course_section_in.courseNumber).alias('crseSectCourseNumber'),
        upper(course_section_in.section).alias('crseSectSection'),
        upper(course_section_in.customDataValue).alias('crseSectCustomDataValue'),
        course_section_in.courseSectionStatus.alias('crseSectCourseSectionStatus'),
        coalesce(course_section_in.isESL, lit(False)).alias('crseSectIsESL'),
        coalesce(course_section_in.isRemedial, lit(False)).alias('crseSectIsRemedial'),
        upper(course_section_in.college).alias('crseSectCollege'),
        upper(course_section_in.division).alias('crseSectDivision'),
        upper(course_section_in.department).alias('crseSectDepartment'),
        coalesce(course_section_in.isClockHours, lit(False)).alias('crseSectIsClockHours'),
        to_timestamp(course_section_in.snapshotDate).alias('crseSectSnapshotDate'),
        registration.repRefSurveySection,
        registration.repRefYearType,
        registration.regSnapshotDate,
        registration.regPersonId,
        registration.regTermCode,
        registration.regPartOfTermCode,
        registration.repRefFinancialAidYear,
        registration.repRefMaxCensus,
        registration.repRefCensusDate,
        registration.repRefTermTypeNew,
        registration.repRefTermCodeOrder,
        registration.regCourseSectionNumber,
        registration.regIsAudited,
        registration.repRefEquivCRHRFactor,
        registration.regCourseSectionCampusOverride,
        coalesce(registration.regCourseSectionLevelOverride, course_section_in.courseSectionLevel).alias(
            'newCourseSectionLevel'),
        coalesce(registration.regEnrollmentHoursOverride, course_section_in.enrollmentHours).alias(
            'newEnrollmentHours')).withColumn(
        'crseSectRowNum',
        row_number().over(
            Window.partitionBy(
                col('repRefYearType'), col('regTermCode'), col('regPartOfTermCode'), col('regPersonId'),
                col('regCourseSectionNumber')).orderBy(
                when(col('crseSectSnapshotDate') == col('regSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
                when(col('crseSectSnapshotDate') > col('regSnapshotDate'), col('crseSectSnapshotDate')).otherwise(
                    to_timestamp(lit('9999-09-09'))).asc(),
                when(col('crseSectSnapshotDate') < col('regSnapshotDate'), col('crseSectSnapshotDate')).otherwise(
                    to_timestamp(lit('1900-09-09'))).desc(),
                col('crseSectSnapshotDate').desc(),
                col('crseSectRecordActivityDate').desc()))).filter(col('crseSectRowNum') == 1)

    registration_course_section_schedule = registration_course_section.join(
        course_section_schedule_in,
        (registration_course_section.regTermCode == course_section_schedule_in.termCode) &
        (registration_course_section.regPartOfTermCode == course_section_schedule_in.partOfTermCode) &
        (registration_course_section.regCourseSectionNumber == course_section_schedule_in.courseSectionNumber) &
        (course_section_schedule_in.termCode.isNotNull()) &
        (coalesce(course_section_schedule_in.partOfTermCode, lit('1')).isNotNull()) &
        (((course_section_schedule_in.recordActivityDate != to_timestamp(lit('9999-09-09'))) &
          (course_section_schedule_in.recordActivityDate <= registration_course_section.repRefCensusDate))
         | (course_section_schedule_in.recordActivityDate == to_timestamp(lit('9999-09-09')))) &
        (coalesce(course_section_schedule_in.isIPEDSReportable, lit(True)) == lit(True)), 'left').select(
        to_timestamp(course_section_schedule_in.snapshotDate).alias('crseSectSchedSnapshotDate'),
        coalesce(to_timestamp(course_section_schedule_in.recordActivityDate), to_timestamp(lit('9999-09-09'))).alias(
            'crseSectSchedRecordActivityDate'),
        # upper(course_section_schedule_in.campus).alias('campus'),
        course_section_schedule_in.instructionType.alias('crseSectSchedInstructionType'),
        course_section_schedule_in.locationType.alias('crseSectSchedLocationType'),
        coalesce(course_section_schedule_in.distanceEducationType, lit('Not distance education')).alias(
            'crseSectSchedDistanceEducationType'),
        course_section_schedule_in.onlineInstructionType.alias('crseSectSchedOnlineInstructionType'),
        course_section_schedule_in.maxSeats.alias('crseSectSchedMaxSeats'),
        # course_section_schedule_in.isIPEDSReportable.alias('courseSectionScheduleIsIPEDSReportable'),
        registration_course_section.repRefYearType,
        registration_course_section.repRefSurveySection,
        registration_course_section.regSnapshotDate,
        registration_course_section.regPersonId,
        registration_course_section.regTermCode,
        registration_course_section.regPartOfTermCode,
        registration_course_section.repRefFinancialAidYear,
        registration_course_section.repRefMaxCensus,
        registration_course_section.repRefCensusDate,
        registration_course_section.repRefTermTypeNew,
        registration_course_section.repRefTermCodeOrder,
        registration_course_section.regCourseSectionNumber,
        registration_course_section.regIsAudited,
        registration_course_section.repRefEquivCRHRFactor,
        registration_course_section.regCourseSectionCampusOverride,
        registration_course_section.newCourseSectionLevel,
        registration_course_section.newEnrollmentHours,
        # registration_course_section.regRecordActivityDate,
        # registration_course_section.repRefSnapshotDate,
        registration_course_section.crseSectSubject,
        registration_course_section.crseSectCourseNumber,
        registration_course_section.crseSectSection,
        registration_course_section.crseSectCustomDataValue,
        registration_course_section.crseSectCourseSectionStatus,
        registration_course_section.crseSectIsESL,
        registration_course_section.crseSectIsRemedial,
        registration_course_section.crseSectCollege,
        registration_course_section.crseSectDivision,
        registration_course_section.crseSectDepartment,
        registration_course_section.crseSectIsClockHours,
        upper(
            coalesce(registration_course_section.regCourseSectionCampusOverride,
                     course_section_schedule_in.campus)).alias(
            'newCampus')).withColumn(
        'crseSectSchedRowNum',
        row_number().over(
            Window.partitionBy(
                col('repRefYearType'),
                col('regTermCode'),
                col('regPartOfTermCode'),
                col('regPersonId'),
                col('regCourseSectionNumber'),
                col('newCourseSectionLevel'),
                col('crseSectSubject'),
                col('crseSectCourseNumber')).orderBy(
                when(col('crseSectSchedSnapshotDate') == col('regSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
                when(col('crseSectSchedSnapshotDate') > col('regSnapshotDate'),
                     col('crseSectSchedSnapshotDate')).otherwise(
                    to_timestamp(lit('9999-09-09'))).asc(),
                when(col('crseSectSchedSnapshotDate') < col('regSnapshotDate'),
                     col('crseSectSchedSnapshotDate')).otherwise(
                    to_timestamp(lit('1900-09-09'))).desc(),
                col('crseSectSchedSnapshotDate').desc(),
                col('crseSectSchedRecordActivityDate').desc()))).filter(col('crseSectSchedRowNum') == 1)

    registration_course = registration_course_section_schedule.join(
        course_in,
        (registration_course_section_schedule.crseSectSubject == course_in.subject) &
        (registration_course_section_schedule.crseSectCourseNumber == course_in.courseNumber) &
        (((course_in.recordActivityDate != to_timestamp(lit('9999-09-09'))) & (
                course_in.recordActivityDate <= registration_course_section_schedule.repRefCensusDate))
         | (course_in.recordActivityDate == to_timestamp(lit('9999-09-09')))) &
        (coalesce(course_in.isIPEDSReportable, lit(True)) == lit(True)), 'left').join(
        #broadcast(academic_term),
        academic_term,
        (academic_term.termCode == course_in.termCodeEffective) &
        (academic_term.termCodeOrder <= registration_course_section_schedule.repRefTermCodeOrder), 'left').select(
        to_timestamp(course_in.snapshotDate).alias('crseSnapshotDate'),
        upper(course_in.termCodeEffective).alias('crseTermCodeEffective'),
        # upper(course_in.courseCollege).alias('crseCourseCollege'),
        # upper(course_in.courseDivision).alias('crseCourseDivision'),
        # upper(course_in.courseDepartment).alias('crseCourseDepartment'),
        coalesce(to_timestamp(course_in.recordActivityDate), to_timestamp(lit('9999-09-09'))).alias(
            'crseRecordActivityDate'),
        course_in.courseStatus.alias('crseCourseStatus'),
        registration_course_section_schedule.repRefYearType,
        registration_course_section_schedule.repRefSurveySection,
        registration_course_section_schedule.regSnapshotDate,
        registration_course_section_schedule.regPersonId,
        registration_course_section_schedule.regTermCode,
        registration_course_section_schedule.regPartOfTermCode,
        registration_course_section_schedule.repRefFinancialAidYear,
        registration_course_section_schedule.repRefMaxCensus,
        registration_course_section_schedule.repRefCensusDate,
        registration_course_section_schedule.repRefTermTypeNew,
        registration_course_section_schedule.repRefTermCodeOrder,
        registration_course_section_schedule.regCourseSectionNumber,
        registration_course_section_schedule.regIsAudited,
        registration_course_section_schedule.repRefEquivCRHRFactor,
        registration_course_section_schedule.newCourseSectionLevel,
        registration_course_section_schedule.newEnrollmentHours,
        # registration_course_section_schedule.repRefSnapshotDate,
        registration_course_section_schedule.crseSectSubject,
        registration_course_section_schedule.crseSectCourseNumber,
        registration_course_section_schedule.crseSectSection,
        registration_course_section_schedule.crseSectCustomDataValue,
        registration_course_section_schedule.crseSectCourseSectionStatus,
        registration_course_section_schedule.crseSectIsESL,
        registration_course_section_schedule.crseSectIsRemedial,
        registration_course_section_schedule.crseSectCollege,
        registration_course_section_schedule.crseSectDivision,
        registration_course_section_schedule.crseSectDepartment,
        registration_course_section_schedule.crseSectIsClockHours,
        registration_course_section_schedule.newCampus,
        registration_course_section_schedule.crseSectSchedInstructionType,
        registration_course_section_schedule.crseSectSchedLocationType,
        registration_course_section_schedule.crseSectSchedDistanceEducationType,
        registration_course_section_schedule.crseSectSchedOnlineInstructionType,
        coalesce(registration_course_section_schedule.crseSectCollege, course_in.courseCollege).alias('newCollege'),
        coalesce(registration_course_section_schedule.crseSectDivision, course_in.courseDivision).alias('newDivision'),
        coalesce(registration_course_section_schedule.crseSectDepartment, course_in.courseDepartment).alias(
            'newDepartment'),
        academic_term.termCodeOrder.alias('crseEffectiveTermCodeOrder')).withColumn(
        'crseRowNum',
        row_number().over(
            Window.partitionBy(
                col('repRefYearType'),
                col('regTermCode'),
                col('regPartOfTermCode'),
                col('regPersonId'),
                col('regCourseSectionNumber'),
                col('newCourseSectionLevel'),
                col('crseSectSubject'),
                col('crseSectCourseNumber')).orderBy(
                when(col('crseSnapshotDate') == col('regSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
                when(col('crseSnapshotDate') > col('regSnapshotDate'), col('crseSnapshotDate')).otherwise(
                    to_timestamp(lit('9999-09-09'))).asc(),
                when(col('crseSnapshotDate') < col('regSnapshotDate'), col('crseSnapshotDate')).otherwise(
                    to_timestamp(lit('1900-09-09'))).desc(),
                col('crseSnapshotDate').desc(),
                col('crseEffectiveTermCodeOrder').desc(),
                col('crseRecordActivityDate').desc()))).filter(col('crseRowNum') == 1)

    registration_course_campus = registration_course.join(
        campus_in,
        #broadcast(campus_in),
        (registration_course.newCampus == campus_in.campus) &
        (((campus_in.recordActivityDate != to_timestamp(lit('9999-09-09'))) & (
                campus_in.recordActivityDate <= registration_course.repRefCensusDate))
         | (campus_in.recordActivityDate == to_timestamp(lit('9999-09-09')))) &
        (coalesce(campus_in.isIPEDSReportable, lit(True)) == lit(True)), 'left').select(
        registration_course['*'],
        coalesce(campus_in.isInternational, lit(False)).alias('campIsInternational'),
        coalesce(to_timestamp(campus_in.recordActivityDate), to_timestamp(lit('9999-09-09'))).alias(
            'campRecordActivityDate'),
        to_timestamp(campus_in.snapshotDate).alias('campSnapshotDate')).withColumn(
        'campRowNum',
        row_number().over(
            Window.partitionBy(
                col('repRefYearType'),
                col('regTermCode'),
                col('regPartOfTermCode'),
                col('regPersonId'),
                col('regCourseSectionNumber'),
                col('newCourseSectionLevel'),
                col('newCampus')).orderBy(
                when(col('campSnapshotDate') == col('regSnapshotDate'), lit(1)).otherwise(lit('2')).asc(),
                when(col('campSnapshotDate') > col('regSnapshotDate'), col('campSnapshotDate')).otherwise(
                    to_timestamp(lit('9999-09-09'))).asc(),
                when(col('campSnapshotDate') < col('regSnapshotDate'), col('campSnapshotDate')).otherwise(
                    to_timestamp(lit('1900-09-09'))).desc(),
                col('campSnapshotDate').desc(),
                col('campRecordActivityDate').desc()))).filter(col('campRowNum') == 1)

    course_type_counts = registration_course_campus.crossJoin(ipeds_client_config).select(
        registration_course_campus.repRefSurveySection,
        registration_course_campus.repRefYearType,
        registration_course_campus.regTermCode,
        registration_course_campus.repRefMaxCensus,
        registration_course_campus.regPersonId,
        registration_course_campus.regCourseSectionNumber,
        registration_course_campus.newEnrollmentHours,
        registration_course_campus.crseSectIsClockHours,
        registration_course_campus.newCourseSectionLevel,
        registration_course_campus.crseSectSchedLocationType,
        registration_course_campus.crseSectIsESL,
        registration_course_campus.crseSectIsRemedial,
        registration_course_campus.campIsInternational,
        registration_course_campus.regIsAudited,
        registration_course_campus.crseSectSchedInstructionType,
        registration_course_campus.crseSectSchedDistanceEducationType,
        registration_course_campus.repRefEquivCRHRFactor,
        ipeds_client_config.instructionalActivityType
    ).withColumn(
        'newCourseSectionLevelUGGR',
        when(col('newCourseSectionLevel').isin('UNDERGRADUATE', 'CONTINUING EDUCATION', 'OTHER'), lit('UG')).otherwise(
            when(col('newCourseSectionLevel') == 'PROFESSIONAL PRACTICE DOCTORATE', lit('GR')).otherwise(
                when(col('newCourseSectionLevel').isin('MASTERS', 'DOCTORATE'), lit('DPP'))))
    ).withColumn(
        'newEnrollmentHoursCalc',
        when(col('instructionalActivityType') == 'CR', col('newEnrollmentHours')).otherwise(
            when(col('crseSectIsClockHours') == False, col('newEnrollmentHours')).otherwise(
                when((col('crseSectIsClockHours') == True) & (col('instructionalActivityType') == 'B'),
                     (col('newEnrollmentHours') * col('repRefEquivCRHRFactor'))).otherwise(col('newEnrollmentHours'))))
    ).distinct().groupBy(
        'repRefSurveySection',
        'repRefYearType',
        'regTermCode',
        'repRefMaxCensus',
        'regPersonId').agg(
        coalesce(count(col('regCourseSectionNumber')), lit(0)).alias('totalCourses'),
        sum(when((col('newEnrollmentHoursCalc') >= 0), lit(1)).otherwise(lit(0))).alias('totalCreditCourses'),
        sum(when((col('crseSectIsClockHours') == False), col('newEnrollmentHoursCalc')).otherwise(lit(0))).alias(
            'totalCreditHrs'),
        sum(when((col('crseSectIsClockHours') == True) & (col('newCourseSectionLevel') == 'UNDERGRADUATE'),
                 col('newEnrollmentHoursCalc')).otherwise(lit(0))).alias('totalClockHrs'),
        sum(when((col('newCourseSectionLevel') == 'CONTINUING EDUCATION'), lit(1)).otherwise(lit(0))).alias(
            'totalCECourses'),
        sum(when((col('crseSectSchedLocationType') == 'Foreign Country'), lit(1)).otherwise(lit(0))).alias(
            'totalSAHomeCourses'),
        sum(when((col('crseSectIsESL') == True), lit(1)).otherwise(lit(0))).alias('totalESLCourses'),
        sum(when((col('crseSectIsRemedial') == True), lit(1)).otherwise(lit(0))).alias('totalRemCourses'),
        sum(when((col('campIsInternational') == True), lit(1)).otherwise(lit(0))).alias('totalIntlCourses'),
        sum(when((col('regIsAudited') == True), lit(1)).otherwise(lit(0))).alias('totalAuditCourses'),
        sum(when((col('crseSectSchedInstructionType') == 'Thesis/Capstone'), lit(1)).otherwise(lit(0))).alias(
            'totalThesisCourses'),
        sum(when((col('crseSectSchedInstructionType').isin('Residency', 'Internship', 'Practicum')) & (
                col('repRefEquivCRHRFactor') == 'DPP'), lit(1)).otherwise(lit(0))).alias('totalProfResidencyCourses'),
        sum(when((col('crseSectSchedDistanceEducationType') != 'Not distance education'), lit(1)).otherwise(
            lit(0))).alias('totalDECourses'),
        sum(when(((col('instructionalActivityType') != 'CL') & (col('newCourseSectionLevelUGGR') == 'UG')),
                 col('newEnrollmentHoursCalc')).otherwise(lit(0))).alias('UGCreditHours'),
        sum(when(((col('instructionalActivityType') == 'CL') & (col('newCourseSectionLevelUGGR') == 'UG')),
                 col('newEnrollmentHoursCalc')).otherwise(lit(0))).alias('UGClockHours'),
        sum(when((col('newCourseSectionLevelUGGR') == 'GR'), col('newEnrollmentHoursCalc')).otherwise(
            lit(0))).alias('GRCreditHours'),
        sum(when((col('newCourseSectionLevelUGGR') == 'DPP'), col('newEnrollmentHoursCalc')).otherwise(
            lit(0))).alias('DPPCreditHours')).cache()

    return course_type_counts
