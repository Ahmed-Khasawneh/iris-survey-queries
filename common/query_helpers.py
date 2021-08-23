import logging
import sys
import boto3
import json
from uuid import uuid4
#from queries.run_query import get_options
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

spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()
# spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).config("spark.dynamicAllocation.enabled", 'true').getOrCreate()
sparkContext = SparkContext.getOrCreate()
sqlContext = SQLContext(sparkContext)
glueContext = GlueContext(sparkContext)


def ipeds_client_config_mcr(surveyYear = ''):
    
    ipeds_client_config_in = spark.sql('select * from ipedsClientConfig')

    ipeds_client_config = ipeds_client_config_in.filter(col('surveyCollectionYear') == surveyYear).select(
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
            col('surveyCollectionYear')).orderBy(
            col('snapshotDate').desc(),
                col('recordActivityDate').desc()))).filter(
        col('clientConfigRowNum') <= 1).limit(1) #.cache()

    return ipeds_client_config


def ipeds_reporting_period_mcr(surveyYear = '', surveyVersionId = '', surveySectionValues = ''):
    
    CurrentYearSurveySection = ['COHORT', 'PRIOR SUMMER']
    PriorYear1SurveySection = ['PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER']
    PriorYear2SurveySection = ['PRIOR YEAR 2 COHORT', 'PRIOR YEAR 2 PRIOR SUMMER']
    
    ipeds_reporting_period_in = spark.sql('select * from ipedsReportingPeriod')

    ipeds_reporting_period = ipeds_reporting_period_in.filter(
        (ipeds_reporting_period_in.surveyCollectionYear == surveyYear) & (ipeds_reporting_period_in.surveyId == surveyVersionId) &
        (upper(ipeds_reporting_period_in.surveySection).isin(surveySectionValues) == True)).select(
        coalesce(upper(ipeds_reporting_period_in.partOfTermCode), lit('1')).alias('partOfTermCode'),
        to_timestamp(ipeds_reporting_period_in.recordActivityDate).alias('recordActivityDate'),
        ipeds_reporting_period_in.surveyCollectionYear,
        upper(ipeds_reporting_period_in.surveyId).alias('surveyId'),
        upper(ipeds_reporting_period_in.surveyName).alias('surveyName'),
        upper(ipeds_reporting_period_in.surveySection).alias('surveySection'),
        upper(ipeds_reporting_period_in.termCode).alias('termCode'),
        to_timestamp(ipeds_reporting_period_in.snapshotDate).alias('snapshotDate'),
        when(upper(ipeds_reporting_period_in.surveySection).isin(CurrentYearSurveySection), 'CY').when(
            upper(ipeds_reporting_period_in.surveySection).isin(PriorYear1SurveySection), 'PY1').when(
            upper(ipeds_reporting_period_in.surveySection).isin(PriorYear1SurveySection), 'PY2').alias('yearType'),
        ipeds_reporting_period_in.tags).withColumn(
        'ipedsRepPerRowNum',
        row_number().over(Window.partitionBy(
            col('surveySection'), col('termCode'), col('partOfTermCode')).orderBy(
            col('snapshotDate').desc(),
            col('recordActivityDate').desc()))).filter(
         (col('ipedsRepPerRowNum') == 1) & (col('termCode').isNotNull())) #.cache()
    
    return ipeds_reporting_period

    
def academic_term_mcr():

    academic_term_in = spark.sql('select * from academicTerm').filter(col('isIpedsReportable') == True)

    academic_term_2 = academic_term_in.select(
        academic_term_in.academicYear,
        to_timestamp(academic_term_in.censusDate).alias('censusDate'),
        to_timestamp(academic_term_in.endDate).alias('endDate'),
        academic_term_in.financialAidYear,
        coalesce(upper(academic_term_in.partOfTermCode), lit('1')).alias('partOfTermCode'),
        academic_term_in.partOfTermCodeDescription,
        to_timestamp(academic_term_in.recordActivityDate).alias('recordActivityDate'),
        academic_term_in.requiredFTCreditHoursGR,
        academic_term_in.requiredFTCreditHoursUG,
        academic_term_in.requiredFTClockHoursUG,
        # expr(col("requiredFTCreditHoursUG")/coalesce(col("requiredFTClockHoursUG"), col("requiredFTCreditHoursUG"))).alias("equivCRHRFactor"),
        to_timestamp(academic_term_in.startDate).alias('startDate'),
        academic_term_in.termClassification,
        upper(academic_term_in.termCode).alias('termCode'),
        academic_term_in.termCodeDescription,
        academic_term_in.termType,
        to_timestamp(academic_term_in.snapshotDate).alias('snapshotDate'),
        academic_term_in.tags).withColumn(
        'acadTermRowNum',
        row_number().over(Window.partitionBy(
            col('termCode'), col('partOfTermCode')).orderBy(
            col('snapshotDate').desc(),
            col('recordActivityDate').desc()))).filter(
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
        ipeds_reporting_period_in = None, academic_term_in = None, surveyYear = '', surveyVersionId = '', surveySectionValues = ''):

    if ipeds_reporting_period_in is None:
       ipeds_reporting_period_in = ipeds_reporting_period_mcr(surveyYear, surveyVersionId, surveySectionValues)
       
    if academic_term_in is None:
       academic_term_in = academic_term_mcr() 

    ipeds_reporting_period_2 = academic_term_in.join(ipeds_reporting_period_in,
                                                     ((academic_term_in.termCode == ipeds_reporting_period_in.termCode) &
                                                      (academic_term_in.partOfTermCode == ipeds_reporting_period_in.partOfTermCode)),
                                                     'inner').select(
        ipeds_reporting_period_in.partOfTermCode,
        ipeds_reporting_period_in.surveySection,
        ipeds_reporting_period_in.termCode,
        ipeds_reporting_period_in.snapshotDate,
        ipeds_reporting_period_in.tags,
        ipeds_reporting_period_in.yearType,
        academic_term_in.termCodeOrder,
        academic_term_in.partOfTermOrder,
        academic_term_in.maxCensus,
        academic_term_in.minStart,
        academic_term_in.maxEnd,
        academic_term_in.censusDate,
        academic_term_in.termClassification,
        academic_term_in.termType,
        academic_term_in.startDate,
        academic_term_in.endDate,
        academic_term_in.requiredFTCreditHoursGR,
        academic_term_in.requiredFTCreditHoursUG,
        academic_term_in.requiredFTClockHoursUG,
        academic_term_in.financialAidYear
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

    academic_term_reporting_refactor_out = ipeds_reporting_period_2.crossJoin(max_term_order_summer).crossJoin(
        max_term_order_fall).withColumn(
        'termTypeNew',
        expr(
            "(case when termType = 'Summer' and termClassification != 'Standard Length' then (case when maxSummerTerm < maxFallTerm then 'Pre-Fall Summer' else 'Post-Spring Summer' end) else termType end)")).cache()

    return academic_term_reporting_refactor_out

###  Modify ipeds_course_type_counts to accept a dataframe with personId to join to Registration - this is needed for Admissions
###  an empty dataframe would imply that records from Registration should be pulled as-is now (no join or filter on personId)

def ipeds_course_type_counts(
        ipeds_client_config_in = None, ipeds_reporting_period_in = None, academic_term_in = None, academic_term_reporting_refactor_in = None, 
        surveyYear = '', surveyVersionId = '', surveySectionValues = ''):

    if ipeds_client_config_in is None:
        ipeds_client_config_in = ipeds_client_config_mcr(surveyYear)
       
    if academic_term_in is None:
       academic_term_in = academic_term_mcr() 
    
    if academic_term_reporting_refactor_in is None:
        academic_term_reporting_refactor_in = academic_term_reporting_refactor(ipeds_reporting_period_in, academic_term_in, surveyYear, surveyVersionId, surveySectionValues)
       
    registration_in = spark.sql("select * from registration").filter(col('isIpedsReportable') == True)
    course_section_in = spark.sql("select * from courseSection").filter(col('isIpedsReportable') == True)
    course_section_schedule_in = spark.sql("select * from courseSectionSchedule").filter(
        col('isIpedsReportable') == True)
    course_in = spark.sql("select * from course").filter(col('isIpedsReportable') == True)
    campus_in = spark.sql("select * from campus").filter(col('isIpedsReportable') == True)

    registration = registration_in.join(
        academic_term_reporting_refactor_in,
        (registration_in.termCode == academic_term_reporting_refactor_in.termCode) &
        (coalesce(registration_in.partOfTermCode, lit('1')) == academic_term_reporting_refactor_in.partOfTermCode) &
        (((registration_in.registrationStatusActionDate != to_timestamp(lit('9999-09-09'))) & (
                registration_in.registrationStatusActionDate <= academic_term_reporting_refactor_in.censusDate))
         | ((registration_in.registrationStatusActionDate == to_timestamp(lit('9999-09-09')))
            & (registration_in.recordActivityDate != to_timestamp(lit('9999-09-09')))
            & (registration_in.recordActivityDate <= academic_term_reporting_refactor_in.censusDate))
         | ((registration_in.registrationStatusActionDate == to_timestamp(lit('9999-09-09')))
            & (registration_in.recordActivityDate == to_timestamp(lit('9999-09-09'))))) &
        (registration_in.snapshotDate <= academic_term_reporting_refactor_in.censusDate) &
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
        academic_term_reporting_refactor_in.snapshotDate.alias('repRefSnapshotDate'),
        academic_term_reporting_refactor_in.yearType.alias('repRefYearType'),
        academic_term_reporting_refactor_in.surveySection.alias('repRefSurveySection'),
        academic_term_reporting_refactor_in.financialAidYear.alias('repRefFinancialAidYear'),
        academic_term_reporting_refactor_in.termCodeOrder.alias('repRefTermCodeOrder'),
        academic_term_reporting_refactor_in.maxCensus.alias('repRefMaxCensus'),
        academic_term_reporting_refactor_in.fullTermOrder.alias('repRefFullTermOrder'),
        academic_term_reporting_refactor_in.termTypeNew.alias('repRefTermTypeNew'),
        academic_term_reporting_refactor_in.startDate.alias('repRefStartDate'),
        academic_term_reporting_refactor_in.censusDate.alias('repRefCensusDate'),
        academic_term_reporting_refactor_in.equivCRHRFactor.alias('repRefEquivCRHRFactor')).withColumn(
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
        # broadcast(academic_term_in),
        academic_term_in,
        (academic_term_in.termCode == course_in.termCodeEffective) &
        (academic_term_in.termCodeOrder <= registration_course_section_schedule.repRefTermCodeOrder), 'left').select(
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
        academic_term_in.termCodeOrder.alias('crseEffectiveTermCodeOrder')).withColumn(
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

    course_type_counts = registration_course_campus.crossJoin(ipeds_client_config_in).select(
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
        ipeds_client_config_in.instructionalActivityType
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

def ipeds_cohort(
        ipeds_client_config_in = None, ipeds_reporting_period_in = None, academic_term_in = None, academic_term_reporting_refactor_in = None, course_type_counts_in = None,
        surveyYear = '', surveyVersionId = '', surveySectionValues = ''):

    if ipeds_client_config_in is None:
        ipeds_client_config_in = ipeds_client_config_mcr(surveyYear)
       
    if academic_term_in is None:
       academic_term_in = academic_term_mcr() 
    
    if academic_term_reporting_refactor_in is None:
        academic_term_reporting_refactor_in = academic_term_reporting_refactor(ipeds_reporting_period_in, academic_term_in, surveyYear, surveyVersionId, surveySectionValues)
    
    if course_type_counts_in is None:
        course_type_counts_in = ipeds_course_type_counts(ipeds_client_config_in, ipeds_reporting_period_in, academic_term_in, academic_term_reporting_refactor_in, surveyYear, surveyVersionId, surveySectionValues)

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
        when(col('FFTRn') == 1, course_type_counts_in.totalAuditCourses).otherwise(None).alias(
            "stuRefTotalAuditCourses"),
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

    return cohort    
