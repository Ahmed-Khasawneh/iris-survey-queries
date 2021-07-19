from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from pyspark.sql.functions import sum as sum, expr, col, lit, upper, to_timestamp, max, min, row_number, date_trunc, \
    to_date, when, coalesce, count, rank
from pyspark.sql.window import Window
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

def spark_refresh_entity_views_v2(tenant_id='11702b15-8db2-4a35-8087-b560bb233420', survey_type='TWELVE_MONTH_ENROLLMENT_1', stage='DEV', year=2021, user_id=None):
    lambda_client = boto3.client('lambda', 'us-east-1')
    invoke_response = lambda_client.invoke(
        FunctionName = "iris-connector-doris-{}-getReportPayload".format(stage),
        InvocationType = 'RequestResponse',
        LogType = "None",
        Payload = json.dumps({ 'tenantId': tenant_id, 'surveyType': survey_type, 'stateMachineExecutionId': '', 'calendarYear': year, 'userId': user_id }).encode('utf-8')
    )
    view_metadata_without_s3_paths = json.loads(invoke_response['Payload'].read().decode("utf-8"))
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

# delete once the spark-executor.py file has been updated to stop creating views from created dataframes
ipeds_client_config = spark.sql("select * from IPEDSClientConfig")
academic_term = spark.sql("select * from academicTerm")
ipeds_reporting_period = spark.sql("select * from IPEDSReportingPeriod")
registration = spark.sql("select * from registration")
course_section = spark.sql("select * from courseSection")
course_section_schedule = spark.sql("select * from courseSectionSchedule")
course = spark.sql("select * from course")
campus = spark.sql("select * from campus")
student = spark.sql("select * from student")
person = spark.sql("select * from person")
academic_track = spark.sql("select * from academicTrack")
degree_program = spark.sql("select * from degreeProgram")
degree = spark.sql("select * from degree")
field_of_study = spark.sql("select * from fieldOfStudy")


# def check_dataframe_exists(df_name):
#    from pyspark.sql import DataFrame
#    df_in_memory = [k for (k, v) in globals().items() if isinstance(v, DataFrame)]

#    if df_name in df_in_memory:
#        return True
#    else:
#        return False

def ipeds_client_config_mcr(ipeds_client_config_partition, ipeds_client_config_order,
                            ipeds_client_config_partition_filter):
    # ipeds_client_config = spark.sql("select * from IPEDSClientConfig")

    ipeds_client_config = ipeds_client_config.filter(expr(f"{ipeds_client_config_partition_filter}"))

    # Should be able to switch to this\/ and remove this /\ when moving to a script
    ipeds_client_config = ipeds_client_config.select(
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
        ipeds_client_config.tags).withColumn(
        'rowNum', row_number().over(Window.partitionBy(
            expr(f"({ipeds_client_config_partition})")).orderBy(expr(f"{ipeds_client_config_order}")))).filter(
        col('rowNum') == 1).limit(1)

    return ipeds_client_config


def academic_term_mcr(academic_term_partition, academic_term_order, academic_term_partition_filter):
    # academic_term = spark.sql("select * from academicTerm")

    academic_term = academic_term.filter(expr(f"{academic_term_partition_filter}"))

    # Should be able to switch to this\/ and remove this /\ when moving to a script
    academic_term = academic_term.select(
        academic_term.academicYear,
        to_timestamp(academic_term.censusDate).alias('censusDate'),
        to_timestamp(academic_term.endDate).alias('endDate'),
        academic_term.financialAidYear,
        academic_term.isIPEDSReportable,
        upper(academic_term.partOfTermCode).alias('partOfTermCode'),
        academic_term.partOfTermCodeDescription,
        to_timestamp(academic_term.recordActivityDate).alias('recordActivityDate'),
        academic_term.requiredFTCreditHoursGR,
        academic_term.requiredFTCreditHoursUG,
        academic_term.requiredFTClockHoursUG,
        # expr(col("requiredFTCreditHoursUG")/coalesce(col("requiredFTClockHoursUG"), col("requiredFTCreditHoursUG"))).alias("equivCRHRFactor"),
        to_timestamp(academic_term.startDate).alias('startDate'),
        academic_term.termClassification,
        upper(academic_term.termCode).alias('termCode'),
        academic_term.termCodeDescription,
        academic_term.termType,
        to_timestamp(academic_term.snapshotDate).alias('snapshotDate'),
        academic_term.tags)

    academic_term = academic_term.select(
        academic_term["*"],
        row_number().over(Window.partitionBy(
            expr(f"({academic_term_partition})")).orderBy(expr(f"{academic_term_order}"))).alias('rowNum'))

    academic_term = academic_term.filter(academic_term.rowNum == 1)

    academic_term_order = academic_term.select(
        academic_term.termCode,
        academic_term.partOfTermCode,
        academic_term.censusDate,
        academic_term.startDate,
        academic_term.endDate).distinct()

    part_of_term_order = academic_term_order.select(
        academic_term_order["*"],
        rank().over(Window.orderBy(col('censusDate').desc(), col('startDate').desc())).alias(
            'partOfTermOrder')).where((col("termCode").isNotNull()) & (col("partOfTermCode").isNotNull()))

    academic_term_order_max = part_of_term_order.groupBy('termCode').agg(
        max(part_of_term_order.partOfTermOrder).alias('termCodeOrder'),
        max(part_of_term_order.censusDate).alias('maxCensus'),
        min(part_of_term_order.startDate).alias('minStart'),
        max("endDate").alias("maxEnd"))

    academic_term = academic_term.join(
        part_of_term_order,
        (academic_term.termCode == part_of_term_order.termCode) &
        (academic_term.partOfTermCode == part_of_term_order.partOfTermCode), 'inner').select(
        academic_term["*"],
        part_of_term_order.partOfTermOrder).where(col("termCode").isNotNull())

    academic_term = academic_term.join(
        academic_term_order_max,
        (academic_term.termCode == academic_term_order_max.termCode), 'inner').select(
        academic_term["*"],
        academic_term_order_max.termCodeOrder,
        academic_term_order_max.maxCensus,
        academic_term_order_max.minStart,
        academic_term_order_max.maxEnd).distinct()

    return academic_term


def ipeds_reporting_period_mcr(
        ipeds_reporting_period_partition,
        ipeds_reporting_period_order,
        ipeds_reporting_period_partition_filter,
        academic_term_partition,
        academic_term_order,
        academic_term_partition_filter):
    # ipeds_reporting_period = spark.sql("select * from ipedsReportingPeriod")

    ipeds_reporting_period = ipeds_reporting_period.filter(expr(f"{ipeds_reporting_period_partition_filter}"))

    # Should be able to switch to this\/ and remove this /\ when moving to a script
    # ipeds_reporting_period = ipedsReportingPeriod.select(
    ipeds_reporting_period = ipeds_reporting_period.select(
        upper(ipeds_reporting_period.partOfTermCode).alias('partOfTermCode'),
        to_timestamp(ipeds_reporting_period.recordActivityDate).alias('recordActivityDate'),
        ipeds_reporting_period.surveyCollectionYear,
        upper(ipeds_reporting_period.surveyId).alias('surveyId'),
        upper(ipeds_reporting_period.surveyName).alias('surveyName'),
        upper(ipeds_reporting_period.surveySection).alias('surveySection'),
        upper(ipeds_reporting_period.termCode).alias('termCode'),
        to_timestamp(ipeds_reporting_period.snapshotDate).alias('snapshotDate'),
        ipeds_reporting_period.tags)

    ipeds_reporting_period = ipeds_reporting_period.select(
        ipeds_reporting_period["*"],
        row_number().over(Window.partitionBy(
            expr(f"({ipeds_reporting_period_partition})")).orderBy(
            expr(f"{ipeds_reporting_period_order}"))).alias(
            "rowNum"))

    ipeds_reporting_period = ipeds_reporting_period.filter(
        (ipeds_reporting_period.rowNum == 1) & (col('termCode').isNotNull()) & (col('partOfTermCode').isNotNull()))

    ipeds_reporting_period = ipeds_reporting_period.join(
        academic_term,
        (academic_term.termCode == ipeds_reporting_period.termCode) &
        (academic_term.partOfTermCode == ipeds_reporting_period.partOfTermCode), 'left').select(
        ipeds_reporting_period["*"],
        when(upper(col('surveySection')).isin('PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER'), 'PY').when(
            upper(col('surveySection')).isin('COHORT', 'PRIOR SUMMER'), 'CY').alias('yearType'),
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
        academic_term.financialAidYear)

    return ipeds_reporting_period


def academic_term_reporting_refactor(
        ipeds_reporting_period_partition,
        ipeds_reporting_period_order,
        ipeds_reporting_period_partition_filter,
        academic_term_partition,
        academic_term_order,
        academic_term_partition_filter):
    ipeds_reporting_period = ipeds_reporting_period_mcr(
        ipeds_reporting_period_partition,
        ipeds_reporting_period_order,
        ipeds_reporting_period_partition_filter,
        academic_term_partition,
        academic_term_order,
        academic_term_partition_filter)

    academic_term_reporting = ipeds_reporting_period.select(
        ipeds_reporting_period.surveySection,
        ipeds_reporting_period.termCode,
        expr("""       
          (case when termClassification = 'Standard Length' then 1
              when termClassification is null then (case when termType in ('Fall', 'Spring') then 1 else 2 end)
              else 2
          end) 
        """).alias('fullTermOrder'),
        ipeds_reporting_period.yearType,
        ipeds_reporting_period.partOfTermCode,
        # coalesce(acadterm.snapshotDate, repperiod.snapshotDate) snapshotDate,
        ipeds_reporting_period.snapshotDate,
        ipeds_reporting_period.tags,
        # coalesce(acadterm.censusDate, repperiod.censusDate) censusDate,
        ipeds_reporting_period.termCodeOrder,
        ipeds_reporting_period.partOfTermOrder,
        ipeds_reporting_period.maxCensus,
        ipeds_reporting_period.minStart,
        ipeds_reporting_period.maxEnd,
        ipeds_reporting_period.censusDate,
        ipeds_reporting_period.termClassification,
        ipeds_reporting_period.termType,
        ipeds_reporting_period.startDate,
        ipeds_reporting_period.endDate,
        ipeds_reporting_period.requiredFTCreditHoursGR,
        ipeds_reporting_period.requiredFTCreditHoursUG,
        ipeds_reporting_period.requiredFTClockHoursUG,
        ipeds_reporting_period.financialAidYear,
        expr(
            "(coalesce(requiredFTCreditHoursUG/coalesce(requiredFTClockHoursUG, requiredFTCreditHoursUG), 1))").alias(
            'equivCRHRFactor'))

    academic_term_reporting = academic_term_reporting.select(
        academic_term_reporting["*"],
        row_number().over(Window.partitionBy(
            expr("(termCode, partOfTermCode)")).orderBy(expr("""
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
    """))).alias('rowNum'))

    academic_term_reporting = academic_term_reporting.filter(academic_term_reporting.rowNum == 1)

    max_term_order_summer = academic_term_reporting.filter(academic_term_reporting.termType == 'Summer').select(
        max(academic_term_reporting.termCodeOrder).alias('maxSummerTerm'))

    max_term_order_fall = academic_term_reporting.filter(academic_term_reporting.termType == 'Fall').select(
        max(academic_term_reporting.termCodeOrder).alias('maxFallTerm'))

    academic_term_reporting_refactor = academic_term_reporting.crossJoin(max_term_order_summer).crossJoin(
        max_term_order_fall)

    academic_term_reporting_refactor = academic_term_reporting_refactor.withColumn(
        'termTypeNew',
        expr(
            "(case when termType = 'Summer' and termClassification != 'Standard Length' then (case when maxSummerTerm < maxFallTerm then 'Pre-Fall Summer' else 'Post-Spring Summer' end) else termType end)"))

    return academic_term_reporting_refactor


def ipeds_course_type_counts():
    # registration = spark.sql("select * from registration")
    # course_section = spark.sql("select * from courseSection")
    # course_section_schedule = spark.sql("select * from courseSectionSchedule")
    # course = spark.sql("select * from course")
    # campus = spark.sql("select * from campus")

    registration = registration.join(
        academic_term_reporting_refactor,
        (registration.termCode == academic_term_reporting_refactor.termCode) &
        (coalesce(registration.partOfTermCode, lit('1')) == academic_term_reporting_refactor.partOfTermCode) &
        (((registration.registrationStatusActionDate != to_timestamp(lit('9999-09-09'))) & (
                registration.registrationStatusActionDate <= academic_term_reporting_refactor.censusDate))
         | ((registration.registrationStatusActionDate == to_timestamp(lit('9999-09-09')))
            & (registration.recordActivityDate != to_timestamp(lit('9999-09-09')))
            & (registration.recordActivityDate <= academic_term_reporting_refactor.censusDate))
         | ((registration.registrationStatusActionDate == to_timestamp(lit('9999-09-09')))
            & (col('recordActivityDate') == to_timestamp(lit('9999-09-09'))))) &
        (registration.snapshotDate <= academic_term_reporting_refactor.censusDate) &
        (coalesce(registration.isIPEDSReportable, lit(True))), 'inner').select(
        registration.personId.alias('regPersonId'),
        to_timestamp(registration.snapshotDate).alias('regSnapshotDate'),
        upper(registration.termCode).alias('regTermCode'),
        coalesce(upper(registration.partOfTermCode), lit('1')).alias('regPartOfTermCode'),
        upper(registration.courseSectionNumber).alias('regCourseSectionNumber'),
        upper(registration.courseSectionCampusOverride).alias('regCourseSectionCampusOverride'),
        upper(registration.courseSectionLevelOverride).alias('regCourseSectionLevelOverride'),
        coalesce(registration.isAudited, lit(False)).alias('regIsAudited'),
        coalesce(registration.isEnrolled, lit(True)).alias('regIsEnrolled'),
        coalesce(registration.registrationStatusActionDate, to_timestamp(lit('9999-09-09'))).alias(
            'regStatusActionDate'),
        coalesce(registration.recordActivityDate, to_timestamp(lit('9999-09-09'))).alias('regRecordActivityDate'),
        registration.enrollmentHoursOverride.alias('regEnrollmentHoursOverride'),
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
        course_section,
        (registration.regTermCode == course_section.termCode) &
        (registration.regPartOfTermCode == course_section.partOfTermCode) &
        (registration.regCourseSectionNumber == course_section.courseSectionNumber) &
        (course_section.termCode.isNotNull()) &
        (coalesce(course_section.partOfTermCode, lit('1')).isNotNull()) &
        (((course_section.recordActivityDate != to_timestamp(lit('9999-09-09'))) & (
                    course_section.recordActivityDate <= registration.repRefCensusDate))
         | (course_section.recordActivityDate == to_timestamp(lit('9999-09-09')))) &
        (coalesce(course_section.isIPEDSReportable, lit(True)) == lit(True)), 'left').select(
        to_timestamp(course_section.recordActivityDate).alias('crseSectRecordActivityDate'),
        course_section.courseSectionLevel.alias('crseSectCourseSectionLevel'),
        upper(course_section.subject).alias('crseSectSubject'),
        upper(course_section.courseNumber).alias('crseSectCourseNumber'),
        upper(course_section.section).alias('crseSectSection'),
        upper(course_section.customDataValue).alias('crseSectCustomDataValue'),
        course_section.courseSectionStatus.alias('crseSectCourseSectionStatus'),
        coalesce(course_section.isESL, lit(False)).alias('crseSectIsESL'),
        coalesce(course_section.isRemedial, lit(False)).alias('crseSectIsRemedial'),
        upper(course_section.college).alias('crseSectCollege'),
        upper(course_section.division).alias('crseSectDivision'),
        upper(course_section.department).alias('crseSectDepartment'),
        coalesce(course_section.isClockHours, lit(False)).alias('crseSectIsClockHours'),
        to_timestamp(course_section.snapshotDate).alias('crseSectSnapshotDate'),
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
        coalesce(registration.regCourseSectionLevelOverride, course_section.courseSectionLevel).alias(
            'newCourseSectionLevel'),
        coalesce(registration.regEnrollmentHoursOverride, course_section.enrollmentHours).alias(
            'newEnrollmentHours')).withColumn(
        'crseSectRowNum', row_number().over(Window.partitionBy(
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
        course_section_schedule,
        (registration_course_section.regTermCode == course_section_schedule.termCode) &
        (registration_course_section.regPartOfTermCode == course_section_schedule.partOfTermCode) &
        (registration_course_section.regCourseSectionNumber == course_section_schedule.courseSectionNumber) &
        (course_section_schedule.termCode.isNotNull()) &
        (coalesce(course_section_schedule.partOfTermCode, lit('1')).isNotNull()) &
        (((course_section_schedule.recordActivityDate != to_timestamp(lit('9999-09-09'))) &
          (course_section_schedule.recordActivityDate <= registration_course_section.repRefCensusDate))
         | (course_section_schedule.recordActivityDate == to_timestamp(lit('9999-09-09')))) &
        (coalesce(course_section_schedule.isIPEDSReportable, lit(True)) == lit(True)), 'left').select(
        to_timestamp(course_section_schedule.snapshotDate).alias('crseSectSchedSnapshotDate'),
        coalesce(to_timestamp(course_section_schedule.recordActivityDate), to_timestamp(lit('9999-09-09'))).alias(
            'crseSectSchedRecordActivityDate'),
        # upper(course_section_schedule.campus).alias('campus'),
        course_section_schedule.instructionType.alias('crseSectSchedInstructionType'),
        course_section_schedule.locationType.alias('crseSectSchedLocationType'),
        coalesce(course_section_schedule.distanceEducationType, lit('Not distance education')).alias(
            'crseSectSchedDistanceEducationType'),
        course_section_schedule.onlineInstructionType.alias('crseSectSchedOnlineInstructionType'),
        course_section_schedule.maxSeats.alias('crseSectSchedMaxSeats'),
        # course_section_schedule.isIPEDSReportable.alias('courseSectionScheduleIsIPEDSReportable'),
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
            coalesce(registration_course_section.regCourseSectionCampusOverride, course_section_schedule.campus)).alias(
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
                     col('crseSectSchedSnapshotDate')).otherwise(to_timestamp(lit('9999-09-09'))).asc(),
                when(col('crseSectSchedSnapshotDate') < col('regSnapshotDate'),
                     col('crseSectSchedSnapshotDate')).otherwise(to_timestamp(lit('1900-09-09'))).desc(),
                col('crseSectSchedSnapshotDate').desc(),
                col('crseSectSchedRecordActivityDate').desc()))).filter(col('crseSectSchedRowNum') == 1)

    registration_course = registration_course_section_schedule.join(
        course,
        (registration_course_section_schedule.crseSectSubject == course.subject) &
        (registration_course_section_schedule.crseSectCourseNumber == course.courseNumber) &
        (((course.recordActivityDate != to_timestamp(lit('9999-09-09'))) & (
                    course.recordActivityDate <= registration_course_section_schedule.repRefCensusDate))
         | (course.recordActivityDate == to_timestamp(lit('9999-09-09')))) &
        (coalesce(course.isIPEDSReportable, lit(True)) == lit(True)), 'left').join(
        academic_term,
        (academic_term.termCode == course.termCodeEffective) &
        (academic_term.termCodeOrder <= registration_course_section_schedule.repRefTermCodeOrder), 'left').select(
        to_timestamp(course.snapshotDate).alias('crseSnapshotDate'),
        upper(course.termCodeEffective).alias('crseTermCodeEffective'),
        # upper(course.courseCollege).alias('crseCourseCollege'),
        # upper(course.courseDivision).alias('crseCourseDivision'),
        # upper(course.courseDepartment).alias('crseCourseDepartment'),
        coalesce(to_timestamp(course.recordActivityDate), to_timestamp(lit('9999-09-09'))).alias(
            'crseRecordActivityDate'),
        course.courseStatus.alias('crseCourseStatus'),
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
        coalesce(registration_course_section_schedule.crseSectCollege, course.courseCollege).alias('newCollege'),
        coalesce(registration_course_section_schedule.crseSectDivision, course.courseDivision).alias('newDivision'),
        coalesce(registration_course_section_schedule.crseSectDepartment, course.courseDepartment).alias(
            'newDepartment'),
        academic_term.termCodeOrder.alias('crseEffectiveTermCodeOrder')).withColumn(
        'crseRowNum',
        f.row_number().over(
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
        campus,
        (registration_course.newCampus == campus.campus) &
        (((campus.recordActivityDate != to_timestamp(lit('9999-09-09'))) & (
                    campus.recordActivityDate <= registration_course.repRefCensusDate))
         | (campus.recordActivityDate == to_timestamp(lit('9999-09-09')))) &
        (coalesce(campus.isIPEDSReportable, lit(True)) == lit(True)), 'left').select(
        registration_course['*'],
        coalesce(campus.isInternational, lit(False)).alias('campIsInternational'),
        coalesce(to_timestamp(campus.recordActivityDate), to_timestamp(lit('9999-09-09'))).alias(
            'campRecordActivityDate'),
        to_timestamp(campus.snapshotDate).alias('campSnapshotDate')).withColumn(
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
            lit(0))).alias('DPPCreditHours'))

    return course_type_counts


def ipeds_cohort():
    # student = spark.sql("select * from student")
    # person = spark.sql("select * from person")
    # academic_track = spark.sql("select * from academicTrack")
    # degree_program = spark.sql("select * from degreeProgram")
    # degree = spark.sql("select * from degree")
    # field_of_study = spark.sql("select * from fieldOfStudy")

    student = student.join(
        academic_term_reporting_refactor,
        ((upper(student.termCode) == academic_term_reporting_refactor.termCode)
         & (coalesce(student.isIPEDSReportable, lit(True)) == True)), 'inner').select(
        # academic_term_reporting_refactor['*'],
        academic_term_reporting_refactor.yearType.alias('repRefYearType'),
        academic_term_reporting_refactor.financialAidYear.alias('repRefFinancialAidYear'),
        academic_term_reporting_refactor.surveySection.alias('repRefSurveySection'),
        academic_term_reporting_refactor.termCodeOrder.alias('repRefTermCodeOrder'),
        academic_term_reporting_refactor.termTypeNew.alias('repRefTermTypeNew'),
        academic_term_reporting_refactor.startDate.alias('repRefStartDate'),
        academic_term_reporting_refactor.censusDate.alias('repRefCensusDate'),
        academic_term_reporting_refactor.requiredFTCreditHoursGR.alias('repRefRequiredFTCreditHoursGR'),
        academic_term_reporting_refactor.requiredFTCreditHoursUG.alias('repRefRequiredFTCreditHoursUG'),
        academic_term_reporting_refactor.requiredFTClockHoursUG.alias('repRefRequiredFTClockHoursUG'),
        academic_term_reporting_refactor.snapshotDate.alias('repRefSnapshotDate'),
        academic_term_reporting_refactor.fullTermOrder.alias('repRefFullTermOrder'),
        student.personId.alias('stuPersonId'),
        student.termCode.alias('stuTermCode'),
        (when((student.studentType).isin('High School', 'Visiting', 'Unknown'), lit(True)).otherwise(
            when((student.studentLevel).isin('Continuing Education', 'Other'), lit(True)).otherwise(
                when(student.studyAbroadStatus == 'Study Abroad - Host Institution', lit(True)).otherwise(
                    coalesce(col('isNonDegreeSeeking'), lit(False)))))).alias('stuIsNonDegreeSeeking'),
        student.studentLevel.alias('stuStudentLevel'),
        student.studentType.alias('stuStudentType'),
        student.residency.alias('stuResidency'),
        upper(student.homeCampus).alias('stuHomeCampus'),
        student.fullTimePartTimeStatus.alias('stuFullTimePartTimeStatus'),
        student.studyAbroadStatus.alias('stuStudyAbroadStatus'),
        coalesce(student.recordActivityDate, to_timestamp(lit('9999-09-09'))).alias('stuRecordActivityDate'),
        to_timestamp(student.snapshotDate).alias('stuSnapshotDate')).filter(
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
        course_type_counts,
        (student.stuPersonId == course_type_counts.regPersonId) &
        (student.stuTermCode == course_type_counts.regTermCode), 'left').filter(
        course_type_counts.regPersonId.isNotNull()
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
        course_type_counts.regPersonId,
        course_type_counts.repRefYearType,
        course_type_counts.regTermCode,
        col('FFTRn'),
        col('NDSRn'),
        when(student.stuIsNonDegreeSeeking == True, 1).otherwise(2).alias('stuRefIsNonDegreeSeeking'),
        when((student.stuIsNonDegreeSeeking == False) & (student.stuStudentLevel != 'Undergraduate'), None)
            .when((student.stuIsNonDegreeSeeking == False) & (col('NDSRn') == 1) & (col('FFTRn') == 1),
                  student.stuStudentType)
            .when((student.stuIsNonDegreeSeeking == False) & (col('NDSRn') == 1), 'Continuing')
            .otherwise(None).alias('stuRefStudentType'),
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
        when(col('FFTRn') == 1, course_type_counts.totalCourses).otherwise(None).alias("stuRefTotalCourses"),
        when(col('FFTRn') == 1, course_type_counts.totalCreditCourses).otherwise(None).alias(
            "stuRefTotalCreditCourses"),
        when(col('FFTRn') == 1, course_type_counts.totalCreditHrs).otherwise(None).alias("stuRefTotalCreditHrs"),
        when(col('FFTRn') == 1, course_type_counts.totalClockHrs).otherwise(None).alias("stuRefTotalClockHrs"),
        when(col('FFTRn') == 1, course_type_counts.totalCECourses).otherwise(None).alias("stuRefTotalCECourses"),
        when(col('FFTRn') == 1, course_type_counts.totalSAHomeCourses).otherwise(None).alias(
            "stuRefTotalSAHomeCourses"),
        when(col('FFTRn') == 1, course_type_counts.totalESLCourses).otherwise(None).alias("stuRefTotalESLCourses"),
        when(col('FFTRn') == 1, course_type_counts.totalRemCourses).otherwise(None).alias("stuRefTotalRemCourses"),
        when(col('FFTRn') == 1, course_type_counts.totalIntlCourses).otherwise(None).alias("stuRefTotalIntlCourses"),
        when(col('FFTRn') == 1, course_type_counts.totalAuditCourses).otherwise(None).alias("stuRefTotalAuditCourses"),
        when(col('FFTRn') == 1, course_type_counts.totalThesisCourses).otherwise(None).alias(
            "stuRefTotalThesisCourses"),
        when(col('FFTRn') == 1, course_type_counts.totalProfResidencyCourses).otherwise(None).alias(
            "stuRefTotalProfResidencyCourses"),
        when(col('FFTRn') == 1, course_type_counts.totalDECourses).otherwise(None).alias("stuRefTotalDECourses"),
        when(col('FFTRn') == 1, course_type_counts.UGCreditHours).otherwise(None).alias("stuRefUGCreditHours"),
        when(col('FFTRn') == 1, course_type_counts.UGClockHours).otherwise(None).alias("stuRefUGClockHours"),
        when(col('FFTRn') == 1, course_type_counts.GRCreditHours).otherwise(None).alias("stuRefGRCreditHours"),
        when(col('FFTRn') == 1, course_type_counts.DPPCreditHours).otherwise(None).alias("stuRefDPPCreditHours"))

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

    student_fft = student_fft.crossJoin(ipeds_client_config).select(
        student_fft['*'],
        ipeds_client_config.acadOrProgReporter.alias('configAcadOrProgReporter'),
        ipeds_client_config.admUseTestScores.alias('configAdmUseTestScores'),
        ipeds_client_config.compGradDateOrTerm.alias('configCompGradDateOrTerm'),
        ipeds_client_config.feIncludeOptSurveyData.alias('configFeIncludeOptSurveyData'),
        ipeds_client_config.fourYrOrLessInstitution.alias('configFourYrOrLessInstitution'),
        ipeds_client_config.genderForNonBinary.alias('configGenderForNonBinary'),
        ipeds_client_config.genderForUnknown.alias('configGenderForUnknown'),
        ipeds_client_config.grReportTransferOut.alias('configGrReportTransferOut'),
        ipeds_client_config.icOfferUndergradAwardLevel.alias('configIcOfferUndergradAwardLevel'),
        ipeds_client_config.icOfferGraduateAwardLevel.alias('configIcOfferGraduateAwardLevel'),
        ipeds_client_config.icOfferDoctorAwardLevel.alias('configIcOfferDoctorAwardLevel'),
        ipeds_client_config.includeNonDegreeAsUG.alias('configIncludeNonDegreeAsUG'),
        ipeds_client_config.instructionalActivityType.alias('configInstructionalActivityType'),
        ipeds_client_config.publicOrPrivateInstitution.alias('configPublicOrPrivateInstitution'),
        ipeds_client_config.recordActivityDate.alias('configRecordActivityDate'),
        ipeds_client_config.sfaGradStudentsOnly.alias('configSfaGradStudentsOnly'),
        ipeds_client_config.sfaLargestProgCIPC.alias('configSfaLargestProgCIPC'),
        ipeds_client_config.sfaReportPriorYear.alias('configSfaReportPriorYear'),
        ipeds_client_config.sfaReportSecondPriorYear.alias('configSfaReportSecondPriorYear'),
        ipeds_client_config.surveyCollectionYear.alias('configSurveyCollectionYear'),
        ipeds_client_config.tmAnnualDPPCreditHoursFTE.alias('configTmAnnualDPPCreditHoursFTE')
    ).withColumn(
        "isNonDegreeSeeking_calc",
        when(student_fft.stuRefStudyAbroadStatus != 'Study Abroad - Home Institution',
             student_fft.stuRefIsNonDegreeSeeking)
            .when((student_fft.stuRefTotalSAHomeCourses > 0) | (student_fft.stuRefTotalCreditHrs > 0) | (
                student_fft.stuRefTotalClockHrs > 0), 0)
            .otherwise(student_fft.stuRefIsNonDegreeSeeking)
    ).withColumn(
        "studentType_calc",
        when((student_fft.studentLevelUGGR == 'UG') & (student_fft.stuRefTypeTermType == 'Fall') & (
                student_fft.stuRefStudentType == 'Continuing') & (student_fft.preFallStudType.isNotNull()),
             student_fft.preFallStudType)
            .otherwise(student_fft.stuRefStudentType)
    ).withColumn(
        "timeStatus_calc",
        when((student_fft.studentLevelUGGR == 'UG') & (col('configInstructionalActivityType').isin('CR', 'B')) & (
                student_fft.stuRefTotalCreditHrs >= student_fft.stuRefRequiredFTCreditHoursUG), 'Full Time')
            .when((student_fft.studentLevelUGGR == 'UG') & (col('configInstructionalActivityType').isin('CR', 'B')) & (
                student_fft.stuRefTotalCreditHrs < student_fft.stuRefRequiredFTCreditHoursUG), 'Part Time')
            .when((student_fft.studentLevelUGGR == 'UG') & (col('configInstructionalActivityType') == 'CL') & (
                student_fft.stuRefTotalClockHrs >= student_fft.stuRefRequiredFTClockHoursUG), 'Full Time')
            .when((student_fft.studentLevelUGGR == 'UG') & (col('configInstructionalActivityType') == 'CL') & (
                student_fft.stuRefTotalClockHrs < student_fft.stuRefRequiredFTClockHoursUG), 'Part Time')
            .when((student_fft.studentLevelUGGR == 'GR') & (
                    col('configInstructionalActivityType') >= student_fft.stuRefRequiredFTCreditHoursGR), 'Full Time')
            .when((student_fft.studentLevelUGGR == 'GR') & (
                    col('configInstructionalActivityType') < student_fft.stuRefRequiredFTCreditHoursGR), 'Part Time')
            .otherwise(None)
    ).withColumn(
        "distanceEdInd_calc",
        when(student_fft.stuRefTotalDECourses == student_fft.stuRefTotalCourses, 'Exclusive DE')
            .when(student_fft.stuRefTotalDECourses > 0, 'Some DE')
            .otherwise('None'))

    cohort_person = student_fft.join(
        person,
        (student_fft.regPersonId == person.personId) &
        (coalesce(person.isIPEDSReportable, lit(True)) == True) &
        ((coalesce(to_date(person.recordActivityDate, 'YYYY-MM-DD'),
                   to_date(lit('9999-09-09'), 'YYYY-MM-DD')) == to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
         | ((coalesce(to_date(person.recordActivityDate, 'YYYY-MM-DD'),
                      to_date(lit('9999-09-09'), 'YYYY-MM-DD')) != to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
            & (to_date(person.recordActivityDate, 'YYYY-MM-DD') <= to_date(student_fft.stuRefCensusDate,
                                                                           'YYYY-MM-DD')))), 'left').select(
        student_fft["*"],
        to_date(person.birthDate, 'YYYY-MM-DD').alias("persBirthDate"),
        upper(person.nation).alias("persNation"),
        upper(person.state).alias("persState"),
        (when(person.gender == 'Male', 'M')
         .when(person.gender == 'Female', 'F')
         .when(person.gender == 'Non-Binary', student_fft.configGenderForNonBinary)
         .otherwise(student_fft.configGenderForUnknown)).alias('persIpedsGender'),
        (when(coalesce(person.isUSCitizen, lit(True)) == True, 'Y')
         .when(((coalesce(person.isInUSOnVisa, lit(False)) == True) |
                ((student_fft.stuRefCensusDate >= to_date(person.visaStartDate)) & (
                        student_fft.stuRefCensusDate <= to_date(person.visaEndDate)) & (
                     person.visaType.isin('Employee Resident', 'Other Resident')))), 'Y')
         .when(((person.isInUSOnVisa == 1) | ((student_fft.stuRefCensusDate >= to_date(person.visaStartDate)) & (
                student_fft.stuRefCensusDate <= to_date(person.visaEndDate))))
               & (person.visaType.isin('Student Non-resident', 'Other Resident', 'Other Non-resident')),
               '1')  # non-resident alien
         .otherwise('9')).alias('persIpedsEthnInd'),
        (when(coalesce(person.isMultipleRaces, lit(False)) == True, '8')
         .when(((person.ethnicity == 'Hispanic or Latino') | (coalesce(person.isHispanic, lit(False))) == True), '2')
         .when(person.ethnicity == 'American Indian or Alaskan Native', '3')
         .when(person.ethnicity == 'Asian', '4')
         .when(person.ethnicity == 'Black or African American', '5')
         .when(person.ethnicity == 'Native Hawaiian or Other Pacific Islander', '6')
         .when(person.ethnicity == 'Caucasian', '7')
         .otherwise('9')).alias('persIpedsEthnValue'),
        to_timestamp(person.recordActivityDate).alias('persRecordActivityDate'),
        to_timestamp(person.snapshotDate).alias('persSnapshotDate')
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
        academic_track,
        (cohort_person.regPersonId == academic_track.personId) &
        (coalesce(academic_track.isIPEDSReportable, lit(True)) == True) &
        (academic_track.fieldOfStudyType == 'Major') &
        (((academic_track.fieldOfStudyActionDate != to_date(lit('9999-09-09'), 'YYYY-MM-DD')) & (
                    academic_track.fieldOfStudyActionDate <= cohort_person.stuRefCensusDate))
         | ((academic_track.fieldOfStudyActionDate == to_date(lit('9999-09-09'), 'YYYY-MM-DD')) & (
                            academic_track.recordActivityDate != to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
            & (academic_track.recordActivityDate <= cohort_person.stuRefCensusDate))
         | ((academic_track.fieldOfStudyActionDate == to_date(lit('9999-09-09'), 'YYYY-MM-DD')) & (
                            academic_track.recordActivityDate == to_date(lit('9999-09-09'), 'YYYY-MM-DD'))))
        & (academic_track.snapshotDate <= cohort_person.stuRefCensusDate), 'left').join(
        academic_term,
        (academic_term.termCode == academic_track.termCodeEffective) &
        (academic_term.termCodeOrder <= cohort_person.stuRefTermCodeOrder), 'left').select(
        cohort_person["*"],
        upper(academic_track.degreeProgram).alias('acadTrkDegreeProgram'),
        academic_track.academicTrackStatus.alias('acadTrkAcademicTrackStatus'),
        coalesce(academic_track.fieldOfStudyPriority, lit(1)).alias('acadTrkFieldOfStudyPriority'),
        upper(academic_track.termCodeEffective).alias('acadTrkAcademicTrackTermCodeEffective'),
        academic_term.termCodeOrder.alias('acadTrkTermOrder'),
        to_timestamp(academic_track.fieldOfStudyActionDate).alias('acadTrkFieldOfStudyActionDate'),
        to_timestamp(academic_track.recordActivityDate).alias('acadTrkRecordActivityDate'),
        to_timestamp(academic_track.snapshotDate).alias('acadTrkSnapshotDate')
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
        degree_program,
        (academic_track.acadTrkDegreeProgram == degree_program.degreeProgram) &
        (coalesce(degree_program.isIPEDSReportable, lit(True)) == True) &
        ((coalesce(to_date(degree_program.recordActivityDate, 'YYYY-MM-DD'),
                   to_date(lit('9999-09-09'), 'YYYY-MM-DD')) == to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
         | ((coalesce(to_date(degree_program.recordActivityDate, 'YYYY-MM-DD'),
                      to_date(lit('9999-09-09'), 'YYYY-MM-DD')) != to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
            & (to_date(degree_program.recordActivityDate, 'YYYY-MM-DD') <= to_date(academic_track.stuRefCensusDate,
                                                                                   'YYYY-MM-DD'))))
        & (degree_program.snapshotDate <= academic_track.stuRefCensusDate), 'left').join(
        academic_term,
        (academic_term.termCode == degree_program.termCodeEffective) &
        (academic_term.termCodeOrder <= academic_track.stuRefTermCodeOrder), 'left').select(
        academic_track["*"],
        upper(degree_program.degreeProgram).alias('degProgDegreeProgram'),
        upper(degree_program.degree).alias('degProgDegree'),
        upper(degree_program.major).alias('degProgMajor'),
        degree_program.startDate.alias('degProgStartDate'),
        coalesce(degree_program.isESL, lit(False)).alias('degProgIsESL'),
        upper(degree_program.termCodeEffective).alias('degProgTermCodeEffective'),
        academic_term.termCodeOrder.alias('degProgTermOrder'),
        to_timestamp(degree_program.recordActivityDate).alias('degProgRecordActivityDate'),
        to_timestamp(degree_program.snapshotDate).alias('degProgSnapshotDate')
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
        degree,
        (degree_program.degProgDegree == degree.degree) &
        (coalesce(degree.isIPEDSReportable, lit(True)) == True) &
        ((coalesce(to_date(degree.recordActivityDate, 'YYYY-MM-DD'),
                   to_date(lit('9999-09-09'), 'YYYY-MM-DD')) == to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
         | ((coalesce(to_date(degree.recordActivityDate, 'YYYY-MM-DD'),
                      to_date(lit('9999-09-09'), 'YYYY-MM-DD')) != to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
            & (to_date(degree.recordActivityDate, 'YYYY-MM-DD') <= to_date(degree_program.stuRefCensusDate,
                                                                           'YYYY-MM-DD'))))
        & (degree.snapshotDate <= degree_program.stuRefCensusDate), 'left').select(
        degree_program["*"],
        upper(degree.awardLevel).alias('degAwardLevel'),
        coalesce(degree.isNonDegreeSeeking, lit(False)).alias('degIsNonDegreeSeeking'),
        to_timestamp(degree.recordActivityDate).alias('degRecordActivityDate'),
        to_timestamp(degree.snapshotDate).alias('degSnapshotDate')
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
        field_of_study,
        (degree.degProgMajor == field_of_study.fieldOfStudy) &
        (field_of_study.fieldOfStudyType == 'Major') &
        (coalesce(field_of_study.isIPEDSReportable, lit(True)) == True) &
        ((coalesce(to_date(field_of_study.recordActivityDate, 'YYYY-MM-DD'),
                   to_date(lit('9999-09-09'), 'YYYY-MM-DD')) == to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
         | ((coalesce(to_date(field_of_study.recordActivityDate, 'YYYY-MM-DD'),
                      to_date(lit('9999-09-09'), 'YYYY-MM-DD')) != to_date(lit('9999-09-09'), 'YYYY-MM-DD'))
            & (to_date(field_of_study.recordActivityDate, 'YYYY-MM-DD') <= to_date(degree.stuRefCensusDate,
                                                                                   'YYYY-MM-DD'))))
        & (field_of_study.snapshotDate <= degree.stuRefCensusDate), 'left').select(
        degree["*"],
        field_of_study.cipCode.alias('fldOfStdyCipCode'),
        to_timestamp(field_of_study.recordActivityDate).alias('fldOfStdyRecordActivityDate'),
        to_timestamp(field_of_study.snapshotDate).alias('fldOfStdySnapshotDate')
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
        when((col('stuRefTotalCECourses') == col('stuRefTotalCourses'))
             | (col('stuRefTotalIntlCourses') == col('stuRefTotalCourses'))
             | (col('stuRefTotalAuditCourses') == col('stuRefTotalCourses'))
             | ((col('stuRefTotalRemCourses') == col('stuRefTotalCourses')) & (
                col('isNonDegreeSeeking_calc') == lit(False)))
             # | {ESLFilter}
             # | {GradFilter}
             | (col('stuRefTotalSAHomeCourses') > lit(0))
             | (col('stuRefTotalCreditHrs') > lit(0))
             | (col('stuRefTotalClockHrs') > lit(0)), lit(1)).otherwise(0))

    return cohort
