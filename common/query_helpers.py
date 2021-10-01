import logging
import sys
import boto3
import json
from uuid import uuid4
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum, expr, col, lit, upper, to_timestamp, max, min, row_number, date_trunc, \
    to_date, when, coalesce, count, rank, concat, substring
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from awsglue.utils import getResolvedOptions

def ipeds_client_config_mcr(spark, survey_info_in):

    survey_year = survey_info_in['survey_year_doris']
    survey_id = survey_info_in['survey_id']
    survey_type = survey_info_in['survey_type']   
            
    ipeds_client_config_in = (spark.sql('select * from ipedsClientConfig')
        .filter(col('surveyCollectionYear') == survey_year))

    if ipeds_client_config_in.rdd.isEmpty() == False:
        ipeds_client_config_in = (ipeds_client_config_in
            .select(
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
                ipeds_client_config_in.tags)
            .withColumn('survey_id', lit(survey_id))
            .withColumn('survey_type', lit(survey_type))
            .withColumn('clientConfigRowNum', row_number().over(Window
                .partitionBy(
                    col('surveyCollectionYear'))
                .orderBy(
                    col('snapshotDate').desc(),
                    col('recordActivityDate').desc())))
            .filter(col('clientConfigRowNum') <= 1)
            .limit(1))
            
    return ipeds_client_config_in

def ipeds_reporting_period_mcr(spark, survey_info_in, default_values_in, ipeds_client_config_in = None):

    survey_year = survey_info_in['survey_year_doris']
    survey_id = survey_info_in['survey_id']
    survey_type = survey_info_in['survey_type']
        
    ipeds_reporting_period_in = (spark.sql('select * from ipedsReportingPeriod')
        .filter((col('surveyCollectionYear') == survey_year) & (col('surveyId') == survey_id)))

    if ipeds_reporting_period_in.rdd.isEmpty() == False:
    
        if not ipeds_client_config_in:
            ipeds_client_config_in = ipeds_client_config_mcr(spark, survey_info_in)
        
        if survey_type == 'SFA':
            report_prior = ipeds_client_config_in.first()['sfaReportPriorYear']
            report_prior_2 = ipeds_client_config_in.first()['sfaReportSecondPriorYear']
        
        #elif survey_type is in ('12ME', 'COM'):
        #    report_prior = ipeds_client_config_in.first()['compGradDateOrTerm']
            
        current_survey_sections = default_values_in['current_survey_sections']
        prior_survey_sections = default_values_in['prior_survey_sections']
        prior_2_survey_sections = default_values_in['prior_2_survey_sections']
            
        ipeds_reporting_period_in = (ipeds_reporting_period_in
            .filter(((upper(ipeds_reporting_period_in.surveySection).isin(current_survey_sections) == True) |
                (upper(ipeds_reporting_period_in.surveySection).isin(prior_survey_sections) == True) |
                (upper(ipeds_reporting_period_in.surveySection).isin(prior_2_survey_sections) == True)))
            .select(
                upper(col('termCode')).alias('termCode'),
                coalesce(upper(col('partOfTermCode')), lit('1')).alias('partOfTermCode'),
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'),
                coalesce(to_timestamp(col('recordActivityDate')), to_timestamp(lit('9999-09-09'))).alias('recordActivityDate'),
                ipeds_reporting_period_in.surveyCollectionYear,
                upper(ipeds_reporting_period_in.surveyId).alias('surveyId'),
                upper(ipeds_reporting_period_in.surveyName).alias('surveyName'),
                upper(ipeds_reporting_period_in.surveySection).alias('surveySection'),
                when(upper(ipeds_reporting_period_in.surveySection).isin(current_survey_sections), lit('CY'))
                    .when(upper(ipeds_reporting_period_in.surveySection).isin(prior_survey_sections), lit('PY1'))
                    .when(upper(ipeds_reporting_period_in.surveySection).isin(prior_2_survey_sections), lit('PY2')).alias('yearType'),
                ipeds_reporting_period_in.tags)
            .withColumn('ipedsRepPerRowNum', row_number().over(Window
                .partitionBy(
                    col('surveySection'), 
                    col('termCode'), 
                    col('partOfTermCode'))
                .orderBy(
                    col('snapshotDate').desc(),
                    col('recordActivityDate').desc())))
            .filter((col('ipedsRepPerRowNum') == 1) & (col('termCode').isNotNull())))
            
    return ipeds_reporting_period_in

def academic_term_mcr(spark):

    academic_term_in = spark.sql('select * from academicTerm').filter((col('isIpedsReportable') == True))
    
    if academic_term_in.rdd.isEmpty() == False:
        academic_term_2 = (academic_term_in
            .select(
                academic_term_in.academicYear,
                academic_term_in.censusDate,
                academic_term_in.endDate,
                academic_term_in.financialAidYear,
                upper(col('termCode')).alias('termCode'),
                coalesce(upper(col('partOfTermCode')), lit('1')).alias('partOfTermCode'),
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'),
                to_timestamp(coalesce(col('recordActivityDate'), lit('9999-09-09'))).alias('recordActivityDate'),
                academic_term_in.partOfTermCodeDescription,
                academic_term_in.requiredFTCreditHoursGR,
                academic_term_in.requiredFTCreditHoursUG,
                academic_term_in.requiredFTClockHoursUG,
                academic_term_in.startDate,
                academic_term_in.termClassification,
                academic_term_in.termCodeDescription,
                academic_term_in.termType,
                academic_term_in.tags)
            .withColumn('acadTermRowNum', row_number().over(Window
                .partitionBy(
                    col('termCode'), 
                    col('partOfTermCode'))
                .orderBy(
                    col('snapshotDate').desc(),
                    col('recordActivityDate').desc())))
            .filter(col('acadTermRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate')))
    
        academic_term_order = (academic_term_2
            .select(
                academic_term_2.termCode,
                academic_term_2.partOfTermCode,
                academic_term_2.censusDate,
                academic_term_2.startDate,
                academic_term_2.endDate)
            .distinct())
    
        part_of_term_order = (academic_term_order
            .select(
                academic_term_order['*'],
                rank().over(Window
                    .orderBy(
                        col('censusDate').asc(), 
                        col('startDate').asc())).alias('partOfTermOrder'))
            .where((col('termCode').isNotNull()) & (col('partOfTermCode').isNotNull())))
    
        academic_term_order_max = (part_of_term_order
            .groupBy('termCode')
            .agg(max(part_of_term_order.partOfTermOrder).alias('termCodeOrder'),
                max(part_of_term_order.censusDate).alias('maxCensus'),
                min(part_of_term_order.startDate).alias('minStart'),
                max('endDate').alias('maxEnd')))
    
        academic_term_3 = (academic_term_2
            .join(
                part_of_term_order,
                (academic_term_2.termCode == part_of_term_order.termCode) &
                (academic_term_2.partOfTermCode == part_of_term_order.partOfTermCode), 'inner')
            .select(
                academic_term_2['*'],
                part_of_term_order.partOfTermOrder)
            .where(col('termCode').isNotNull()))
    
        academic_term_in = (academic_term_3
            .join(
                academic_term_order_max,
                (academic_term_3.termCode == academic_term_order_max.termCode), 'inner')
            .select(
                academic_term_3['*'],
                academic_term_order_max.termCodeOrder,
                academic_term_order_max.maxCensus,
                academic_term_order_max.minStart,
                academic_term_order_max.maxEnd)
            .distinct())

    return academic_term_in

def reporting_periods(spark, survey_info_in, default_values_in, ipeds_reporting_period_in = None, academic_term_in = None):

    if not ipeds_reporting_period_in:
        ipeds_reporting_period_in = ipeds_reporting_period_mcr(spark, survey_info_in, default_values_in)

    if not academic_term_in:
       academic_term_in = academic_term_mcr(spark) 
       
    if (academic_term_in.rdd.isEmpty() == False) & (ipeds_reporting_period_in.rdd.isEmpty() == False):
        ipeds_reporting_period_2 = (academic_term_in
            .join(
                ipeds_reporting_period_in,
                ((academic_term_in.termCode == ipeds_reporting_period_in.termCode) &
                (academic_term_in.partOfTermCode == ipeds_reporting_period_in.partOfTermCode)), 'inner')
            .select(
                ipeds_reporting_period_in.yearType,
                ipeds_reporting_period_in.surveySection,
                ipeds_reporting_period_in.termCode,
                ipeds_reporting_period_in.partOfTermCode,
                to_timestamp(ipeds_reporting_period_in.snapshotDate).alias('snapshotDateTimestamp'),
                to_date(ipeds_reporting_period_in.snapshotDate, 'YYYY-MM-DD').alias('snapshotDate'),
                ipeds_reporting_period_in.tags,
                academic_term_in.termCodeOrder,
                academic_term_in.partOfTermOrder,
                to_date(academic_term_in.maxCensus, 'YYYY-MM-DD').alias('maxCensus'),
                to_date(academic_term_in.minStart, 'YYYY-MM-DD').alias('minStart'),
                to_date(academic_term_in.maxEnd, 'YYYY-MM-DD').alias('maxEnd'),
                to_date(academic_term_in.censusDate, 'YYYY-MM-DD').alias('censusDate'),
                academic_term_in.termClassification,
                academic_term_in.termType,
                to_date(academic_term_in.startDate, 'YYYY-MM-DD').alias('startDate'),
                to_date(academic_term_in.endDate, 'YYYY-MM-DD').alias('endDate'),
                academic_term_in.requiredFTCreditHoursGR,
                academic_term_in.requiredFTCreditHoursUG,
                academic_term_in.requiredFTClockHoursUG,
                academic_term_in.financialAidYear)
            .withColumn('dummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
            .withColumn('snapShotMaxDummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
            .withColumn('snapShotMinDummyDate', to_date(to_timestamp(lit('1900-09-09')), 'YYYY-MM-DD'))
            .withColumn('fullTermOrder',
                expr("""       
                        (case when termClassification = 'Standard Length' then 1
                            when termClassification is null then (case when termType in ('Fall', 'Spring') then 1 else 2 end)
                            else 2
                        end) 
                    """))
            .withColumn('equivCRHRFactor', 
                expr("(coalesce(requiredFTCreditHoursUG/coalesce(requiredFTClockHoursUG, requiredFTCreditHoursUG), 1))"))
            .withColumn('rowNum', row_number().over(Window
                .partitionBy(
                        col('termCode'), 
                        col('partOfTermCode'))
                .orderBy(
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
                                (case when snapshotDate > censusDate then snapshotDate else snapShotMaxDummyDate end) asc,
                                (case when snapshotDate < censusDate then snapshotDate else snapShotMinDummyDate end) desc)
                            """))))
            .filter(col('rowNum') == 1))
    
        max_term_order_summer = (ipeds_reporting_period_2
            .filter(ipeds_reporting_period_2.termType == 'Summer')
            .select(max(ipeds_reporting_period_2.termCodeOrder).alias('maxSummerTerm')))
    
        max_term_order_fall = (ipeds_reporting_period_2
            .filter(ipeds_reporting_period_2.termType == 'Fall')
            .select(max(ipeds_reporting_period_2.termCodeOrder).alias('maxFallTerm')))
    
        academic_term_reporting_refactor_out = (ipeds_reporting_period_2
            .crossJoin(max_term_order_summer)
            .crossJoin(max_term_order_fall)
            .withColumn('termTypeNew', when((col('termType') == 'Summer') & (col('termClassification') != 'Standard Length'),
                when(col('maxSummerTerm') < col('maxFallTerm'), lit('Pre-Fall Summer'))
                .otherwise(lit('Post-Spring Summer'))).otherwise(col('termType'))))
            #.withColumn(expr("""
                    #(case when termType = 'Summer' and termClassification != 'Standard Length' then 
                    #            (case when maxSummerTerm < maxFallTerm then 'Pre-Fall Summer' 
                    #                    else 'Post-Spring Summer' end) 
                    #      else termType end)
            #"""))))    
    
        return academic_term_reporting_refactor_out
    
    else:
        if academic_term_in.rdd.isEmpty() == False:
            return ipeds_reporting_period_in 
        else: return academic_term_in

def campus_mcr(spark, snapshotDateFilter_in = None):

    campus_in = spark.sql("select * from campus").filter(col('isIpedsReportable') == True)

    if campus_in.rdd.isEmpty() == False:       
        campus_in = (campus_in
                .select(
                    upper(col('campus')).alias('campus'),
                    col('campusDescription'),
                    coalesce(col('isInternational'), lit(False)).alias('isInternational'),
                    col('recordActivityDate').alias('recordActivityDateTimestamp'),
                    to_date(col('recordActivityDate'), 'YYYY-MM-DD').alias('recordActivityDate'), 
                    col('snapshotDate').alias('campusSnapshotDateTimestamp'),
                    to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('campusSnapshotDate'))
                .withColumn('snapshotDateFilter', lit(snapshotDateFilter_in))
                .withColumn('useSnapshotDatePartition', when(col('snapshotDateFilter').isNull(), col('campusSnapshotDateTimestamp')).otherwise(lit(None)))
                .withColumn('campRowNum', row_number().over(Window
                    .partitionBy(
                        col('useSnapshotDatePartition'),
                        col('campus'))
                    .orderBy(
                        when(col('campusSnapshotDateTimestamp') == snapshotDateFilter_in, lit(1)).otherwise(lit(2)).asc(),
                        col('campusSnapshotDateTimestamp').desc(),
                        col('recordActivityDateTimestamp').desc())))
            .filter(col('campRowNum') == 1)
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('campRowNum')))
    
    return campus_in

def financial_aid_mcr(spark, snapshotDateFilter_in = None, cohort_df_in = None):

    financial_aid_in = spark.sql("select * from financialAid").filter(col('isIpedsReportable') == True)

    if financial_aid_in.rdd.isEmpty() == False:
        financial_aid_in = (financial_aid_in
                .select(
                    col('personId'),
                    col('fundType'),
                    upper(col('fundCode')).alias('fundCode'),
                    col('fundSource'),
                    col('recordActivityDate').alias('recordActivityDateTimestamp'),
                    to_date(col('recordActivityDate'), 'YYYY-MM-DD').alias('recordActivityDate'), 
                    col('snapshotDate').alias('finAidSnapshotDateTimestamp'),
                    to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('finAidSnapshotDate'),
                    col('awardStatusActionDate').alias('awardStatusActionDateTimestamp'),
                    to_date(col('awardStatusActionDate'), 'YYYY-MM-DD').alias('awardStatusActionDate'), 
                    upper(col('termCode')).alias('termCode'),
                    col('awardStatus'),
                    coalesce(col('isPellGrant'), lit(False)).alias('isPellGrant'),
                    coalesce(col('isTitleIV'), lit(False)).alias('isTitleIV'),
                    coalesce(col('isSubsidizedDirectLoan'), lit(False)).alias('isSubsidizedDirectLoan'),
                    col('acceptedAmount'),
                    col('offeredAmount'),
                    col('paidAmount'),
                    when((col('IPEDSFinancialAidAmount').isNotNull()) & (col('IPEDSFinancialAidAmount') > 0), col('IPEDSFinancialAidAmount'))
                            .when(col('fundType') == 'Loan', col('acceptedAmount'))
                            .when(col('fundType').isin('Grant', 'Scholarship'), col('offeredAmount'))
                            .when(col('fundType') == 'Work Study', col('paidAmount'))
                            .otherwise(col('IPEDSFinancialAidAmount')).alias('IPEDSFinancialAidAmount'), 
                    when((col('IPEDSOutcomeMeasuresAmount').isNotNull()) & (col('IPEDSOutcomeMeasuresAmount') > 0), col('IPEDSOutcomeMeasuresAmount'))
                            .when(col('fundType') == 'Loan', col('acceptedAmount'))
                            .when(col('fundType').isin('Grant', 'Scholarship'), col('offeredAmount'))
                            .when(col('fundType') == 'Work Study', col('paidAmount'))
                            .otherwise(col('IPEDSOutcomeMeasuresAmount')).alias('IPEDSOutcomeMeasuresAmount'), 
                    col('IPEDSOutcomeMeasuresAmount'),
                    round(regexp_replace(col('familyIncome'), ',', ''), 0).alias('familyIncome'),
                    col('livingArrangement'))
                .withColumn('snapshotDateFilter', lit(snapshotDateFilter_in))
                .withColumn('useSnapshotDatePartition', when(col('snapshotDateFilter').isNull(), col('finAidSnapshotDateTimestamp')).otherwise(lit(None)))
                .withColumn('finAidRowNum', row_number().over(Window
                    .partitionBy(
                        col('useSnapshotDatePartition'),
                        col('financialAidYear'),
                        col('personId'),
                        col('termCode'),
                        col('fundCode'),
                        col('fundType'),
                        col('fundSource'))
                    .orderBy(
                        when(col('finAidSnapshotDateTimestamp') == snapshotDateFilter_in, lit(1)).otherwise(lit(2)).asc(),
                        col('finAidSnapshotDateTimestamp').desc(),
                        col('recordActivityDateTimestamp').desc(),
                        col('awardStatusActionDateTimestamp').desc())))
            .filter(col('finAidRowNum') == 1)
            .drop(col('recordActivityDate'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('finAidRowNum')))

    return financial_aid_in
    
def course_type_counts(spark, survey_info_in, default_values_in, ipeds_client_config_in = None, academic_term_in = None, reporting_periods_in = None):

    if not ipeds_client_config_in:
        ipeds_client_config_in = ipeds_client_config_mcr(spark, survey_info_in)
        
    if not academic_term_in:
        academic_term_in = academic_term_mcr(spark)

    if not reporting_periods_in:   
        if not ipeds_reporting_period_in:
            ipeds_reporting_period_in = ipeds_reporting_period_mcr(spark, survey_info_in, default_values_in)
        reporting_periods_in = reporting_periods(spark, survey_info_in, default_values_in, ipeds_reporting_period_in = ipeds_reporting_period_in, academic_term_in = academic_term_in)   
        
    registration_in = spark.sql("select * from registration").filter(col('isIpedsReportable') == True)

    if (reporting_periods_in.rdd.isEmpty() == False) & (registration_in.rdd.isEmpty() == False):
        
        course_section_in = spark.sql("select * from courseSection").filter(col('isIpedsReportable') == True)
        course_section_schedule_in = spark.sql("select * from courseSectionSchedule").filter(col('isIpedsReportable') == True)
        course_in = spark.sql("select * from course").filter(col('isIpedsReportable') == True)
    
        registration = (registration_in
            .join(
                reporting_periods_in,
                (upper(registration_in.termCode) == reporting_periods_in.termCode) &
                (coalesce(upper(registration_in.partOfTermCode), lit('1')) == reporting_periods_in.partOfTermCode) &
                (((coalesce(to_date(col('registrationStatusActionDate'), 'YYYY-MM-DD'), col('dummyDate')) != col('dummyDate')) & 
                    (coalesce(to_date(col('registrationStatusActionDate'), 'YYYY-MM-DD'), col('dummyDate')) <= col('censusDate')))
                | ((coalesce(to_date(col('registrationStatusActionDate'), 'YYYY-MM-DD'), col('dummyDate')) == col('dummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) != col('dummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) <= col('censusDate')))
                | ((coalesce(to_date(col('registrationStatusActionDate'), 'YYYY-MM-DD'), col('dummyDate')) == col('dummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) == col('dummyDate')))), 'inner')
            .select(
                registration_in.personId.alias('regPersonId'),
                to_timestamp(registration_in.snapshotDate).alias('regSnapshotDateTimestamp'),
                to_date(registration_in.snapshotDate, 'YYYY-MM-DD').alias('regSnapshotDate'),
                upper(registration_in.termCode).alias('regTermCode'),
                coalesce(upper(registration_in.partOfTermCode), lit('1')).alias('regPartOfTermCode'),
                upper(registration_in.courseSectionNumber).alias('regCourseSectionNumber'),
                upper(registration_in.courseSectionCampusOverride).alias('regCourseSectionCampusOverride'),
                registration_in.courseSectionLevelOverride.alias('regCourseSectionLevelOverride'),
                coalesce(registration_in.isAudited, lit(False)).alias('regIsAudited'),
                coalesce(registration_in.isEnrolled, lit(True)).alias('regIsEnrolled'),
                to_timestamp(coalesce(registration_in.registrationStatusActionDate, col('dummyDate'))).alias('regStatusActionDateTimestamp'),
                coalesce(to_date(registration_in.registrationStatusActionDate, 'YYYY-MM-DD'), col('dummyDate')).alias('regStatusActionDate'),
                to_timestamp(coalesce(col('recordActivityDate'), col('dummyDate'))).alias('regRecordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')).alias('regRecordActivityDate'),
                registration_in.enrollmentHoursOverride.alias('regEnrollmentHoursOverride'),
                reporting_periods_in.dummyDate.alias('repRefDummyDate'),
                reporting_periods_in.snapShotMaxDummyDate.alias('repRefSnapShotMaxDummyDate'),
                reporting_periods_in.snapShotMinDummyDate.alias('repRefSnapShotMinDummyDate'),
                reporting_periods_in.snapshotDateTimestamp.alias('repRefSnapshotDateTimestamp'),
                reporting_periods_in.snapshotDate.alias('repRefSnapshotDate'),
                reporting_periods_in.yearType.alias('repRefYearType'),
                reporting_periods_in.surveySection.alias('repRefSurveySection'),
                reporting_periods_in.financialAidYear.alias('repRefFinancialAidYear'),
                reporting_periods_in.termCodeOrder.alias('repRefTermCodeOrder'),
                reporting_periods_in.maxCensus.alias('repRefMaxCensus'),
                reporting_periods_in.fullTermOrder.alias('repRefFullTermOrder'),
                reporting_periods_in.termTypeNew.alias('repRefTermTypeNew'),
                reporting_periods_in.startDate.alias('repRefStartDate'),
                reporting_periods_in.censusDate.alias('repRefCensusDate'),
                reporting_periods_in.equivCRHRFactor.alias('repRefEquivCRHRFactor'))
            .withColumn('regRowNum', row_number().over(Window
                .partitionBy(
                    col('repRefYearType'),
                    col('repRefSurveySection'),
                    col('regTermCode'),
                    col('regPartOfTermCode'),
                    col('regPersonId'),
                    col('regCourseSectionNumber'),
                    col('RegcourseSectionLevelOverride'))
                .orderBy(
                    when(col('regSnapshotDateTimestamp') == col('repRefSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('regSnapshotDateTimestamp') > col('repRefSnapshotDateTimestamp'), col('regSnapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMaxDummyDate')).asc(),
                    when(col('regSnapshotDateTimestamp') < col('repRefSnapshotDateTimestamp'), col('regSnapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMinDummyDate')).desc(),
                    col('regSnapshotDateTimestamp').desc(),
                    col('regRecordActivityDateTimestamp').desc(),
                    col('regStatusActionDateTimestamp').desc())))
            .filter((col('regRowNum') == 1) & col('regIsEnrolled') == lit('True')))
            
        registration_course_section = (registration
            .join(
                course_section_in,
                (col('regTermCode') == upper(col('termCode'))) &
                (col('regPartOfTermCode') == upper(coalesce(col('partOfTermCode'), lit('1')))) &
                (col('regCourseSectionNumber') == upper(col('courseSectionNumber'))) &
                (col('termCode').isNotNull()) &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) != col('repRefDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) <= col('repRefCensusDate')))
                    | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) == col('repRefDummyDate'))), 'left')
            .select(
                col('regPersonId'), 
                col('repRefSnapshotDateTimestamp'),
                col('repRefSnapshotDate'),
                col('repRefSurveySection'),
                col('repRefYearType'),
                col('regTermCode'),
                col('regPartOfTermCode'),
                col('regCourseSectionNumber'),
                to_timestamp(coalesce(col('recordActivityDate'), col('repRefDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')).alias('recordActivityDate'),
                col('courseSectionLevel').alias('crseSectCourseSectionLevel'),
                upper(col('subject')).alias('crseSectSubject'),
                upper(col('courseNumber')).alias('crseSectCourseNumber'),
                upper(col('section')).alias('crseSectSection'),
                upper(col('customDataValue')).alias('crseSectCustomDataValue'),
                col('courseSectionStatus').alias('crseSectCourseSectionStatus'),
                coalesce(col('isESL'), lit(False)).alias('crseSectIsESL'),
                coalesce(col('isRemedial'), lit(False)).alias('crseSectIsRemedial'),
                upper(col('college')).alias('crseSectCollege'),
                upper(col('division')).alias('crseSectDivision'),
                upper(col('department')).alias('crseSectDepartment'),
                coalesce(col('isClockHours'), lit(False)).alias('crseSectIsClockHours'),
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'),
                col('repRefDummyDate'),
                col('repRefSnapShotMaxDummyDate'),
                col('repRefSnapShotMinDummyDate'),
                col('repRefFinancialAidYear'),
                col('repRefMaxCensus'),
                col('repRefCensusDate'),
                col('repRefTermTypeNew'),
                col('repRefTermCodeOrder'),
                col('regIsAudited'),
                col('repRefEquivCRHRFactor'),
                col('regCourseSectionCampusOverride'),
                coalesce(col('regCourseSectionLevelOverride'), col('courseSectionLevel')).alias('newCourseSectionLevel'),
                coalesce(col('regEnrollmentHoursOverride'), col('enrollmentHours')).alias('newEnrollmentHours'))
            .withColumn('crseSectRowNum', row_number().over(Window
                .partitionBy(
                    col('repRefYearType'), 
                    col('repRefSurveySection'),
                    col('regTermCode'), 
                    col('regPartOfTermCode'), 
                    col('regPersonId'),
                    col('regCourseSectionNumber'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repRefSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDateTimestamp').desc())))
            .filter(col('crseSectRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('crseSectRowNum')))
            
        registration_course_section_schedule = (registration_course_section
            .join(
                course_section_schedule_in,
                (col('regTermCode') == upper(col('termCode'))) &
                (col('regPartOfTermCode') == upper(coalesce(col('partOfTermCode'), lit('1')))) &
                (col('regCourseSectionNumber') == upper(col('courseSectionNumber'))) &
                (col('termCode').isNotNull()) &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) != col('repRefDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) <= col('repRefCensusDate')))
                    | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) == col('repRefDummyDate'))), 'left')
            .select(
                col('regPersonId'), 
                col('repRefSnapshotDateTimestamp'),
                col('repRefSnapshotDate'),
                col('repRefSurveySection'),
                col('repRefYearType'),
                col('regTermCode'),
                col('regPartOfTermCode'),
                col('regCourseSectionNumber'),
                to_timestamp(coalesce(col('recordActivityDate'), col('repRefDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')).alias('recordActivityDate'), 
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'),  
                col('instructionType').alias('crseSectSchedInstructionType'),
                col('locationType').alias('crseSectSchedLocationType'),
                coalesce(col('distanceEducationType'), lit('Not distance education')).alias('crseSectSchedDistanceEducationType'),
                col('onlineInstructionType').alias('crseSectSchedOnlineInstructionType'),
                col('maxSeats').alias('crseSectSchedMaxSeats'),
                col('repRefDummyDate'),
                col('repRefSnapShotMaxDummyDate'),
                col('repRefSnapShotMinDummyDate'),
                col('repRefFinancialAidYear'),
                col('repRefMaxCensus'),
                col('repRefCensusDate'),
                col('repRefTermTypeNew'),
                col('repRefTermCodeOrder'),
                col('regIsAudited'),
                col('repRefEquivCRHRFactor'),
                col('regCourseSectionCampusOverride'),
                col('newCourseSectionLevel'),
                col('newEnrollmentHours'),
                col('crseSectSubject'),
                col('crseSectCourseNumber'),
                col('crseSectSection'),
                col('crseSectCustomDataValue'),
                col('crseSectCourseSectionStatus'),
                col('crseSectIsESL'),
                col('crseSectIsRemedial'),
                col('crseSectCollege'),
                col('crseSectDivision'),
                col('crseSectDepartment'),
                col('crseSectIsClockHours'),
                upper(coalesce(col('regCourseSectionCampusOverride'), col('campus'))).alias('newCampus'))
            .withColumn('crseSectSchedRowNum', row_number().over(Window
                .partitionBy(
                    col('repRefYearType'),
                    col('repRefSurveySection'),
                    col('regTermCode'),
                    col('regPartOfTermCode'),
                    col('regPersonId'),
                    col('regCourseSectionNumber'),
                    col('newCourseSectionLevel'),
                    col('crseSectSubject'),
                    col('crseSectCourseNumber'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repRefSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDateTimestamp').desc())))
            .filter(col('crseSectSchedRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('crseSectSchedRowNum')))
                    
        registration_course = (registration_course_section_schedule
            .join(
                course_in,
                (col('crseSectSubject') == upper(col('subject'))) &
                (col('crseSectCourseNumber') == upper(col('courseNumber'))) &
                    (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) != col('repRefDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) <= col('repRefCensusDate')))
                    | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) == col('repRefDummyDate'))), 'left')
            .join(
                academic_term_in,
                (col('termCode') == upper(col('termCodeEffective'))) &
                (col('repRefTermCodeOrder') <= col('termCodeOrder')), 'left')
            .select(
                col('regPersonId'), 
                col('repRefSnapshotDateTimestamp'),
                col('repRefSnapshotDate'),
                col('repRefSurveySection'),
                col('repRefYearType'),
                col('regTermCode'),
                col('regPartOfTermCode'),
                col('regCourseSectionNumber'),
                to_timestamp(coalesce(col('recordActivityDate'), col('repRefDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')).alias('recordActivityDate'), 
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'), 
                upper(col('termCodeEffective')).alias('crseTermCodeEffective'),
                col('courseStatus').alias('crseCourseStatus'),
                col('repRefDummyDate'),
                col('repRefSnapShotMaxDummyDate'),
                col('repRefSnapShotMinDummyDate'),
                col('repRefFinancialAidYear'),
                col('repRefMaxCensus'),
                col('repRefCensusDate'),
                col('repRefTermTypeNew'),
                col('repRefTermCodeOrder'),
                col('regIsAudited'),
                col('repRefEquivCRHRFactor'),
                col('regCourseSectionCampusOverride'),
                col('newCourseSectionLevel'),
                col('newEnrollmentHours'),
                col('crseSectSubject'),
                col('crseSectCourseNumber'),
                col('crseSectSection'),
                col('crseSectCustomDataValue'),
                col('crseSectCourseSectionStatus'),
                col('crseSectIsESL'),
                col('crseSectIsRemedial'),
                col('crseSectCollege'),
                col('crseSectDivision'),
                col('crseSectDepartment'),
                col('crseSectIsClockHours'),
                col('newCampus'),
                col('crseSectSchedInstructionType'),
                col('crseSectSchedLocationType'),
                col('crseSectSchedDistanceEducationType'),
                col('crseSectSchedOnlineInstructionType'),
                coalesce(col('crseSectCollege'), upper(col('courseCollege'))).alias('newCollege'),
                coalesce(col('crseSectDivision'), upper(col('courseDivision'))).alias('newDivision'),
                coalesce(col('crseSectDepartment'), upper(col('courseDepartment'))).alias('newDepartment'),
                col('termCodeOrder').alias('crseEffectiveTermCodeOrder'))
            .withColumn('crseRowNum', row_number().over(Window
                .partitionBy(
                    col('repRefYearType'),
                    col('repRefSurveySection'),
                    col('regTermCode'),
                    col('regPartOfTermCode'),
                    col('regPersonId'),
                    col('regCourseSectionNumber'),
                    col('newCourseSectionLevel'),
                    col('crseSectSubject'),
                    col('crseSectCourseNumber'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repRefSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDateTimestamp').desc())))
            .filter(col('crseRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('crseRowNum')))
        
        
        registration_course_campus = (registration_course
            .join(
                campus_mcr(spark, registration_course.first()['repRefSnapshotDateTimestamp']),
                (col('newCampus') == upper(col('campus'))), 'left')
            .select(
                registration_course['*'],
                coalesce(col('isInternational'), lit(False)).alias('campIsInternational')))
                    
        course_type_counts = (registration_course_campus
            .crossJoin(ipeds_client_config_in)
            .select(
                col('regPersonId'), 
                col('repRefSnapshotDateTimestamp'),
                col('repRefSnapshotDate'),
                col('repRefSurveySection'),
                col('repRefYearType'),
                col('regTermCode'),
                col('repRefMaxCensus'),
                col('regCourseSectionNumber'),
                col('newCourseSectionLevel'),
                col('newEnrollmentHours'),
                col('crseSectIsESL'),
                col('crseSectIsRemedial'),
                col('crseSectIsClockHours'),
                col('regIsAudited'),
                col('repRefEquivCRHRFactor'),
                col('crseSectSchedInstructionType'),
                col('crseSectSchedLocationType'),
                col('crseSectSchedDistanceEducationType'),
                col('crseSectSchedOnlineInstructionType'),
                col('campIsInternational'),
                col('instructionalActivityType'),
                when(col('newCourseSectionLevel').isin('Undergraduate', 'Continuing Education', 'Other'), lit('UG'))
                    .when(col('newCourseSectionLevel').isin('Masters', 'Doctorate'), lit('GR'))
                    .when(col('newCourseSectionLevel').isin('Professional Practice Doctorate'), lit('DPP')).alias('newCourseSectionLevelUGGRDPP'))
            .withColumn('newEnrollmentHoursCalc',
                when(col('instructionalActivityType') == 'CR', col('newEnrollmentHours')).otherwise(
                    when(col('crseSectIsClockHours') == False, col('newEnrollmentHours')).otherwise(
                    when((col('crseSectIsClockHours') == True) & (col('instructionalActivityType') == 'B'),
                            (col('newEnrollmentHours') * col('repRefEquivCRHRFactor'))).otherwise(col('newEnrollmentHours')))))
            .distinct()
            .groupBy(
                'regPersonId',
                'repRefYearType',
                'repRefSurveySection',
                'regTermCode',
                'repRefMaxCensus')
            .agg(
                coalesce(count(col('regCourseSectionNumber')), lit(0)).alias('totalCourses'),
                sum(when((col('newEnrollmentHoursCalc') >= 0), lit(1)).otherwise(lit(0))).alias('totalCreditCourses'),
                sum(when((col('crseSectIsClockHours') == False), col('newEnrollmentHoursCalc')).otherwise(lit(0))).alias('totalCreditHrs'),
                sum(when((col('crseSectIsClockHours') == True) & (col('newCourseSectionLevel') == 'Undergraduate'), col('newEnrollmentHoursCalc'))
                    .otherwise(lit(0))).alias('totalClockHrs'),
                sum(when((col('newCourseSectionLevel') == 'Continuing Education'), lit(1)).otherwise(lit(0))).alias('totalCECourses'),
                sum(when((col('crseSectSchedLocationType') == 'Foreign Country'), lit(1)).otherwise(lit(0))).alias('totalSAHomeCourses'),
                sum(when((col('crseSectIsESL') == True), lit(1)).otherwise(lit(0))).alias('totalESLCourses'),
                sum(when((col('crseSectIsRemedial') == True), lit(1)).otherwise(lit(0))).alias('totalRemCourses'),
                sum(when((col('campIsInternational') == True), lit(1)).otherwise(lit(0))).alias('totalIntlCourses'),
                sum(when((col('regIsAudited') == True), lit(1)).otherwise(lit(0))).alias('totalAuditCourses'),
                sum(when((col('crseSectSchedInstructionType') == 'Thesis/Capstone'), lit(1)).otherwise(lit(0))).alias('totalThesisCourses'),
                sum(when((col('crseSectSchedInstructionType').isin('Residency', 'Internship', 'Practicum')) & (col('repRefEquivCRHRFactor') == 'DPP'), 
                    lit(1)).otherwise(lit(0))).alias('totalProfResidencyCourses'),
                sum(when((col('crseSectSchedDistanceEducationType') != 'Not distance education'), lit(1)).otherwise(lit(0))).alias('totalDECourses'),
                sum(when(((col('instructionalActivityType') != 'CL') & (col('newCourseSectionLevelUGGRDPP') == 'UG')),col('newEnrollmentHoursCalc'))
                    .otherwise(lit(0))).alias('UGCreditHours'),
                sum(when(((col('instructionalActivityType') == 'CL') & (col('newCourseSectionLevelUGGRDPP') == 'UG')),col('newEnrollmentHoursCalc'))
                    .otherwise(lit(0))).alias('UGClockHours'),
                sum(when((col('newCourseSectionLevelUGGRDPP') == 'GR'), col('newEnrollmentHoursCalc')).otherwise(lit(0))).alias('GRCreditHours'),
                sum(when((col('newCourseSectionLevelUGGRDPP') == 'DPP'), col('newEnrollmentHoursCalc')).otherwise(lit(0))).alias('DPPCreditHours')))
                                    
        return course_type_counts
    
    else:
        if reporting_periods_in.rdd.isEmpty() == False:
            return registration_in 
        else: return reporting_periods_in
    
def student_cohort(spark, survey_info_in, default_values_in, ipeds_client_config_in = None, academic_term_in = None, reporting_periods_in = None, course_type_counts_in = None):

    if ipeds_client_config_in is None:
        ipeds_client_config_in = ipeds_client_config_mcr(spark, survey_info_in)

    if not academic_term_in:
        academic_term_in = academic_term_mcr(spark)

    if not course_type_counts_in:
        if not reporting_periods_in:   
            if not ipeds_reporting_period_in:
                ipeds_reporting_period_in = ipeds_reporting_period_mcr(spark, survey_info_in, default_values_in)
            reporting_periods_in = reporting_periods(spark, survey_info_in, default_values_in, ipeds_reporting_period_in = ipeds_reporting_period_in, academic_term_in = academic_term_in)
        course_type_counts_in = course_type_counts(spark, survey_info_in, default_values_in, ipeds_client_config_in = ipeds_client_config_in, academic_term_in = academic_term_in, reporting_periods_in = reporting_period_in)  
        
    student_in = spark.sql("select * from student")     
        
    if (course_type_counts_in.rdd.isEmpty() == False) and (student_in.rdd.isEmpty() == False):
        study_abroad_filter_surveys = ['FE', 'OM', 'GR', '200GR']
        esl_enroll_surveys = ['12ME', 'ADM', 'SFA', 'FE']
        graduate_enroll_surveys = ['12ME', 'FE']
        academic_year_surveys = ['12ME', 'OM']
    
        person_in = spark.sql("select * from person")
        academic_track_in = spark.sql("select * from academicTrack")
        degree_program_in = spark.sql("select * from degreeProgram")
        degree_in = spark.sql("select * from degree")
        field_of_study_in = spark.sql("select * from fieldOfStudy")

        student = (student_in
            .join(
                reporting_periods_in,
                (upper(student_in.termCode) == reporting_periods_in.termCode) & 
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) != col('dummyDate'))
                & (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) <= col('censusDate')))
                | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) == col('dummyDate'))), 'inner')
            .select(
                student_in.personId.alias('stuPersonId'),
                reporting_periods_in.snapshotDateTimestamp.alias('repRefSnapshotDateTimestamp'),
                reporting_periods_in.snapshotDate.alias('repRefSnapshotDate'),
                reporting_periods_in.yearType.alias('repRefYearType'),
                reporting_periods_in.surveySection.alias('repRefSurveySection'),
                student_in.termCode.alias('stuTermCode'),
                when(student_in.studentType.isin('High School', 'Visiting', 'Unknown'), lit(True))
                    .when(student_in.studentLevel.isin('Continuing Education', 'Other'), lit(True))
                    .when(student_in.studyAbroadStatus == 'Study Abroad - Host Institution', lit(True))
                    .otherwise(col('isNonDegreeSeeking')).alias('stuIsNonDegreeSeeking'),
                student_in.isNonDegreeSeeking.alias('stuIsNonDegreeSeekingORIG'),
                student_in.studentLevel.alias('stuStudentLevel'),
                col('studentType').alias('stuStudentType'),
                student_in.residency.alias('stuResidency'),
                upper(student_in.homeCampus).alias('stuHomeCampus'),
                student_in.fullTimePartTimeStatus.alias('stuFullTimePartTimeStatus'),
                student_in.studyAbroadStatus.alias('stuStudyAbroadStatus'),
                to_timestamp(coalesce(student_in.recordActivityDate, col('dummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(student_in.recordActivityDate, 'YYYY-MM-DD'), col('dummyDate')).alias('recordActivityDate'), 
                to_timestamp(student_in.snapshotDate).alias('snapshotDateTimestamp'),
                to_date(student_in.snapshotDate, 'YYYY-MM-DD').alias('snapshotDate'),
                reporting_periods_in.financialAidYear.alias('repRefFinancialAidYear'),
                reporting_periods_in.termCodeOrder.alias('repRefTermCodeOrder'),
                reporting_periods_in.maxCensus.alias('repRefMaxCensus'),
                reporting_periods_in.fullTermOrder.alias('repRefFullTermOrder'),
                reporting_periods_in.termTypeNew.alias('repRefTermTypeNew'),
                reporting_periods_in.startDate.alias('repRefStartDate'),
                reporting_periods_in.censusDate.alias('repRefCensusDate'),
                reporting_periods_in.equivCRHRFactor.alias('repRefEquivCRHRFactor'),
                reporting_periods_in.requiredFTCreditHoursGR.alias('repRefRequiredFTCreditHoursGR'),
                reporting_periods_in.requiredFTCreditHoursUG.alias('repRefRequiredFTCreditHoursUG'),
                reporting_periods_in.requiredFTClockHoursUG.alias('repRefRequiredFTClockHoursUG'),
                reporting_periods_in.dummyDate.alias('repRefDummyDate'),
                reporting_periods_in.snapShotMaxDummyDate.alias('repRefSnapShotMaxDummyDate'),
                reporting_periods_in.snapShotMinDummyDate.alias('repRefSnapShotMinDummyDate'))
            .withColumn('studentRowNum', row_number().over(Window
                .partitionBy(
                    col('repRefYearType'),
                    col('repRefSurveySection'),
                    col('stuPersonId'),
                    col('stuTermCode'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repRefSnapshotDateTimestamp'), lit(1))
                        .otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDateTimestamp').desc())))
            .filter(col('studentRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('studentRowNum')))

        student_reg = (student
            .join(
                course_type_counts_in,
                (col('stuPersonId') == col('regPersonId')) &
                (col('stuTermCode') == col('regTermCode')) &
                (student.repRefYearType == course_type_counts_in.repRefYearType) &
                (student.repRefSurveySection == course_type_counts_in.repRefSurveySection), 'left')
            .filter(col('regPersonId').isNotNull())
            .select(
                col('stuPersonId'),
                col('repRefSnapshotDateTimestamp'),
                col('repRefSnapshotDate'),
                student.repRefYearType.alias('repRefYearType'),
                student.repRefSurveySection.alias('repRefSurveySection'),
                col('stuTermCode'),
                col('stuIsNonDegreeSeeking'),
                col('stuIsNonDegreeSeekingORIG'),
                col('stuStudentType'),
                col('stuStudentLevel'),
                when(col('stuStudentLevel').isin('Masters', 'Doctorate'), lit('GR'))
                    .when(col('stuStudentLevel') == 'Professional Practice Doctorate',lit('DPP'))
                    .otherwise(lit('UG')).alias('studentLevelUGGRDPP'),
                col('stuHomeCampus'),
                col('stuFullTimePartTimeStatus'),
                col('stuStudyAbroadStatus'),
                col('stuResidency'),
                student.repRefTermCodeOrder.alias('repRefTermCodeOrder'),
                student.repRefTermTypeNew.alias('repRefTermTypeNew'),
                student.repRefMaxCensus.alias('repRefMaxCensus'),
                col('repRefFullTermOrder'),
                col('repRefDummyDate'),
                col('repRefSnapShotMaxDummyDate'),
                col('repRefSnapShotMinDummyDate'),
                col('repRefCensusDate'),
                col('repRefStartDate'),
                col('repRefFinancialAidYear'),
                col('repRefRequiredFTCreditHoursGR'),
                col('repRefRequiredFTCreditHoursUG'),
                col('repRefRequiredFTClockHoursUG'),
                col('totalCourses'),
                col('totalCreditCourses'),
                col('totalCreditHrs'),
                col('totalClockHrs'),
                col('totalCECourses'),
                col('totalSAHomeCourses'),
                col('totalESLCourses'),
                col('totalRemCourses'),
                col('totalIntlCourses'),
                col('totalAuditCourses'),
                col('totalThesisCourses'),
                col('totalProfResidencyCourses'),
                col('totalDECourses'),
                col('UGCreditHours'),
                col('UGClockHours'),
                col('GRCreditHours'),
                col('DPPCreditHours')))
    
        student_out = (student_reg
            .crossJoin(ipeds_client_config_in)
            .select(
                student_reg['*'],
                col('survey_id').alias('survey_id'),
                col('survey_type').alias('survey_type'),
                col('surveyCollectionYear').alias('survey_year'),
                col('icOfferUndergradAwardLevel').alias('configIcOfferUndergradAwardLevel'),
                col('icOfferGraduateAwardLevel').alias('configIcOfferGraduateAwardLevel'),
                col('icOfferDoctorAwardLevel').alias('configIcOfferDoctorAwardLevel'),
                col('instructionalActivityType').alias('configInstructionalActivityType'),
                col('genderForNonBinary').alias('configGenderForNonBinary'),
                col('genderForUnknown').alias('configGenderForUnknown'),
                when(col('survey_type') == 'ADM', col('admUseTestScores')).alias('configAdmUseTestScores'),
                when(col('survey_type') == 'COM', col('compGradDateOrTerm')).alias('configCompGradDateOrTerm'),
                when(col('survey_type') == 'FE', col('feIncludeOptSurveyData')).alias('configFeIncludeOptSurveyData'),
                when(col('survey_type') == 'GR', col('grReportTransferOut')).alias('configGrReportTransferOut'),
                #col('fourYrOrLessInstitution').alias('configFourYrOrLessInstitution'),
                #col('acadOrProgReporter').alias('configAcadOrProgReporter'),
                #col('publicOrPrivateInstitution').alias('configPublicOrPrivateInstitution'),
                #col('recordActivityDate').alias('configRecordActivityDate'),
                #when(col('survey_type') == 'SFA', col('sfaGradStudentsOnly').alias('configSfaGradStudentsOnly'),
                when(col('survey_type') == 'SFA', col('sfaLargestProgCIPC')).alias('configSfaLargestProgCIPC'),
                when(col('survey_type') == 'SFA', col('sfaReportPriorYear')).alias('configSfaReportPriorYear'),
                when(col('survey_type') == 'SFA', col('sfaReportSecondPriorYear')).alias('configSfaReportSecondPriorYear'),
                when(col('survey_type') == '12ME', col('includeNonDegreeAsUG')).alias('configIncludeNonDegreeAsUG'),
                when(col('survey_type') == '12ME', col('tmAnnualDPPCreditHoursFTE')).alias('configTmAnnualDPPCreditHoursFTE'))
            .withColumn('isNonDegreeSeeking_calc',
                when(col('stuStudyAbroadStatus') != 'Study Abroad - Home Institution', col('stuIsNonDegreeSeeking'))
                    .when((col('totalSAHomeCourses') > 0) | (col('totalCreditHrs') > 0) | (col('totalClockHrs') > 0), False)
                    .otherwise(col('stuIsNonDegreeSeeking')))
            .withColumn('timeStatus_calc',
                expr("""
                        (case when studentLevelUGGRDPP = 'UG' and (totalCreditHrs is not null or totalClockHrs is not null) then
                                (case when configInstructionalActivityType in ('CR', 'B') then 
                                        (case when totalCreditHrs >= repRefRequiredFTCreditHoursUG then 'Full Time' else 'Part Time' end)
                                    when configInstructionalActivityType = 'CL' then 
                                        (case when totalClockHrs >= repRefRequiredFTClockHoursUG then 'Full Time' else 'Part Time' end) 
                                else null end)
                            when studentLevelUGGRDPP != 'UG' and totalCreditHrs is not null then
                                (case when totalCreditHrs >= repRefRequiredFTCreditHoursUG then 'Full Time' else 'Part Time' end)
                        else null end)
                """))
            .withColumn('distanceEdInd_calc',
                when(col('totalDECourses') == col('totalCourses'), 'Exclusive DE')
                    .when(col('totalDECourses') > 0, 'Some DE')
                    .otherwise('None')))  

        cohort_person = (student_out
            .join(
                person_in,
                (col('stuPersonId') == col('personId')) &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) != col('repRefDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) <= col('repRefCensusDate')))
                | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) == col('repRefDummyDate'))), 'left')
            .select(
                student_out['*'],
                to_date(col('birthDate'), 'YYYY-MM-DD').alias('persBirthDate'),
                upper(col('nation')).alias('persNation'),
                upper(col('state')).alias('persState'),
                (when(col('gender') == 'Male', 'M')
                    .when(col('gender') == 'Female', 'F')
                    .when(col('gender') == 'Non-Binary', col('configGenderForNonBinary'))
                    .otherwise(col('configGenderForUnknown'))).alias('persIpedsGender'),
                expr("""
                    (case when isUSCitizen = 1 or ((coalesce(isInUSOnVisa, false) = 1 or repRefCensusDate between visaStartDate and visaEndDate)
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
                        when ((coalesce(isInUSOnVisa, false) = 1 or repRefCensusDate between person.visaStartDate and person.visaEndDate)
                            and visaType in ('Student Non-resident', 'Employee Non-resident', 'Other Non-resident')) then '1'
                        else '9'
                    end) --ipedsEthnicity
                    """).alias('persIpedsEthnValue'),
                col('ethnicity').alias('persEthnicity'),
                to_timestamp(coalesce(col('recordActivityDate'), col('repRefDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')).alias('recordActivityDate'), 
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
            .withColumn('persRowNum',row_number().over(Window
                .partitionBy(
                    col('repRefYearType'),
                    col('repRefSurveySection'),
                    col('stuPersonId'),
                    col('stuTermCode'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repRefSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDateTimestamp').desc())))
            .filter(col('persRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('persRowNum')))

        academic_track = (cohort_person
            .join(
                academic_track_in,
                (col('stuPersonId') == col('personId')) &
                (col('fieldOfStudyType') == 'Major') &
                (((coalesce(to_date(col('fieldOfStudyActionDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) != col('repRefDummyDate')) & 
                    (coalesce(to_date(col('fieldOfStudyActionDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) <= col('repRefCensusDate')))
                | ((coalesce(to_date(col('fieldOfStudyActionDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) == col('repRefDummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) != col('repRefDummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) <= col('repRefCensusDate')))
                | ((coalesce(to_date(col('fieldOfStudyActionDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) == col('repRefDummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) == col('repRefDummyDate')))), 'left')
            .join(
                academic_term_in,
                (academic_term_in.termCode == upper(col('termCodeEffective'))) &
                (academic_term_in.termCodeOrder <= col('repRefTermCodeOrder')), 'left')
            .select(
                cohort_person['*'],
                upper(col('degreeProgram')).alias('acadTrkDegreeProgram'),
                col('academicTrackStatus').alias('acadTrkAcademicTrackStatus'),
                coalesce(col('fieldOfStudyPriority'), lit(1)).alias('acadTrkFieldOfStudyPriority'),
                upper(col('termCodeEffective')).alias('acadTrkTermCodeEffective'),
                col('termCodeOrder').alias('acadTrkTermOrder'),
                to_timestamp(coalesce(col('fieldOfStudyActionDate'), col('repRefDummyDate'))).alias('acadTrkFieldOfStudyActionDateTimestamp'),
                coalesce(to_date(col('fieldOfStudyActionDate'), 'YYYY-MM-DD'), col('repRefDummyDate')).alias('acadTrkFieldOfStudyActionDate'), 
                to_timestamp(coalesce(col('recordActivityDate'), col('repRefDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')).alias('recordActivityDate'), 
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
            .withColumn('acadTrkTrackRowNum', row_number().over(Window
                .partitionBy(
                    col('repRefYearType'),
                    col('repRefSurveySection'),
                    col('stuPersonId'),
                    col('stuTermCode'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repRefSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('acadTrkFieldOfStudyPriority').asc(),
                    col('acadTrkTermOrder').desc(),
                    col('acadTrkFieldOfStudyActionDate').desc(),
                    col('recordActivityDateTimestamp').desc(),
                    when(col('acadTrkAcademicTrackStatus') == 'In Progress', lit(1)).otherwise(lit(2)).asc())))
            .filter(col('acadTrkTrackRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('acadTrkTrackRowNum')))
    
        degree_program = (academic_track
            .join(
                degree_program_in,
                (col('acadTrkDegreeProgram') == upper(col('degreeProgram'))) &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) != col('repRefDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) <= col('repRefCensusDate')))
                | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) == col('repRefDummyDate'))), 'left')
            .join(
                academic_term_in,
                (academic_term_in.termCode == upper(degree_program_in.termCodeEffective)) &
                (academic_term_in.termCodeOrder <= col('repRefTermCodeOrder')), 'left')
            .select(
                academic_track['*'],
                upper(col('degreeProgram')).alias('degProgDegreeProgram'),
                upper(col('degree')).alias('degProgDegree'),
                upper(col('major')).alias('degProgMajor'),
                degree_program_in.startDate.alias('degProgStartDate'),
                coalesce(col('isESL'), lit(False)).alias('degProgIsESL'),
                upper(degree_program_in.termCodeEffective).alias('degProgTermCodeEffective'),
                academic_term_in.termCodeOrder.alias('degProgTermOrder'), 
                to_timestamp(coalesce(col('recordActivityDate'), col('repRefDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')).alias('recordActivityDate'), 
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
            .withColumn('degProgRowNum', row_number().over(Window
                .partitionBy(
                    col('repRefYearType'),
                    col('repRefSurveySection'),
                    col('stuPersonId'),
                    col('stuTermCode'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repRefSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('degProgTermOrder').desc(),
                    col('degProgStartDate').desc(),
                    col('recordActivityDate').desc(),
                    when(coalesce(col('degProgDegree'), lit(0)) == 0, lit(2)).otherwise(lit(1)).asc(),
                    when(coalesce(col('degProgMajor'), lit(0)) == 0, lit(2)).otherwise(lit(1)).asc())))
            .filter(col('degProgRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('degProgRowNum')))
        
        degree = (degree_program
            .join(
                degree_in,
                (col('degProgDegree') == upper(col('degree'))) &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) != col('repRefDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) <= col('repRefCensusDate')))
                | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) == col('repRefDummyDate'))), 'left')
             .select(
                degree_program['*'],
                col('awardLevel').alias('degAwardLevel'),
                coalesce(col('isNonDegreeSeeking_calc'), col('isNonDegreeSeeking'), lit(False)).alias('isNonDegreeSeeking_final'),
                to_timestamp(coalesce(col('recordActivityDate'), col('repRefDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')).alias('recordActivityDate'), 
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
            .withColumn('degRowNum', row_number().over(Window
                .partitionBy(
                    col('repRefYearType'),
                    col('repRefSurveySection'),
                    col('stuPersonId'),
                    col('stuTermCode'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repRefSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDate').desc())))
            .filter(col('degRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('degRowNum')))
        
        field_of_study = (degree
            .join(
                field_of_study_in,
                (col('degProgMajor') == upper(col('fieldOfStudy'))) &
                (col('fieldOfStudyType') == 'Major') &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) != col('repRefDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) <= col('repRefCensusDate')))
                | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')) == col('repRefDummyDate'))), 'left')               
            .select(
                degree['*'],
                col('cipCode').alias('fldOfStdyCipCode'), 
                to_timestamp(coalesce(col('recordActivityDate'), col('repRefDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repRefDummyDate')).alias('recordActivityDate'), 
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
            .withColumn('fldOfStdyRowNum', row_number().over(Window
                .partitionBy(
                    col('repRefYearType'),
                    col('repRefSurveySection'),
                    col('stuPersonId'),
                    col('stuTermCode'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repRefSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repRefSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repRefSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDate').desc())))
            .filter(col('fldOfStdyRowNum') <= 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('fldOfStdyRowNum')))

        cohort_priority = (field_of_study
            .withColumn('ipedsInclude',
                when(col('totalCECourses') == col('totalCourses'), lit(0))
                    .when(col('totalIntlCourses') == col('totalCourses'), lit(0))
                    .when(col('totalAuditCourses') == col('totalCourses'), lit(0))
                    .when(((col('totalRemCourses') == col('totalCourses')) & (col('isNonDegreeSeeking_final') == True)), lit(0))
                    .when((col('degProgIsESL') == True) & (col('survey_type').isin(esl_enroll_surveys)), lit(0))
                    .when((col('totalThesisCourses') > 0) & (col('survey_type').isin(graduate_enroll_surveys)), lit(1))
                    .when((col('totalProfResidencyCourses') > 0) & (col('survey_type').isin(graduate_enroll_surveys)), lit(0))
                    .when(((col('totalESLCourses') == col('totalCourses')) & (col('isNonDegreeSeeking_final') == True)), lit(0))
                    .when(col('totalSAHomeCourses') > 0, lit(1))
                    .when(col('totalCreditHrs') > 0, lit(1))
                    .when(col('totalClockHrs') > 0, lit(1))
                    .otherwise(lit(0)))
            .select(
                col('repRefYearType').alias('yearType'),
                col('repRefSurveySection').alias('surveySection'),
                when(col('survey_type').isin(academic_year_surveys), lit(True)).otherwise(lit(False)).alias('annualSurvey'),
                col('survey_type').alias('surveyType'),
                col('survey_year').alias('surveyYear'),
                col('survey_id').alias('surveyId'),
                col('stuPersonId').alias('personId'),
                col('stuTermCode').alias('termCode'),
                col('repRefFullTermOrder').alias('fullTermOrder'),
                col('repRefTermCodeOrder').alias('termCodeOrder'),
                col('repRefMaxCensus').alias('maxCensus'),
                col('repRefTermTypeNew').alias('termType'),
                col('repRefFinancialAidYear').alias('financialAidYear'),
                col('studentLevelUGGRDPP'),
                col('isNonDegreeSeeking_final').alias('isNonDegreeSeeking'),
                col('stuStudentType').alias('studentType'),
                col('timeStatus_calc').alias('timeStatus'),
                col('ipedsInclude'),
                col('persIpedsEthnValue').alias('ethnicity'),
                col('persIpedsGender').alias('gender'),
                col('distanceEdInd_calc').alias('distanceEducationType'),
                col('stuStudyAbroadStatus').alias('studyAbroadStatus'),
                col('stuResidency').alias('residency'),
                col('fldOfStdyCipCode').alias('cipCode'),
                col('degAwardLevel').alias('awardLevel'),
                col('UGCreditHours'),
                col('UGClockHours'),
                col('GRCreditHours'),
                col('DPPCreditHours'),
                col('configIcOfferUndergradAwardLevel').alias('icOfferUndergradAwardLevel'),
                col('configIcOfferGraduateAwardLevel').alias('icOfferGraduateAwardLevel'),
                col('configIcOfferDoctorAwardLevel').alias('icOfferDoctorAwardLevel'),
                col('configInstructionalActivityType').alias('instructionalActivityType'),
                col('configTmAnnualDPPCreditHoursFTE').alias('tmAnnualDPPCreditHoursFTE'),
                col('configIncludeNonDegreeAsUG').alias('includeNonDegreeAsUG'))
            .filter(col('ipedsInclude') == 1)
            .withColumn('NDSRn', row_number().over(Window
                .partitionBy(
                    col('yearType'),
                    col('surveySection'),
                    col('personId'))
                .orderBy(
                    col('isNonDegreeSeeking'),
                    col('fullTermOrder'),
                    col('termCodeOrder'))))
            .withColumn('FFTRn', row_number().over(Window
                .partitionBy(
                    col('yearType'),
                    col('surveySection'),
                    col('personId'))
                .orderBy(
                    col('fullTermOrder'),
                    col('termCodeOrder')))).cache())
        
        cohort_firstDegreeSeeking = (cohort_priority
            .select(
                col('personId').alias('personIdFirstDegreeSeeking'),
                col('yearType').alias('yearTypeFirstDegreeSeeking'),
                col('surveySection').alias('surveySectionFirstDegreeSeeking'),
                col('termCode').alias('termCodeFirstDegreeSeeking'), 
                when((col('annualSurvey') == True) & (col('isNonDegreeSeeking') == False) & (col('NDSRn') == 1) & (col('FFTRn') != 1) 
                    & (col('studentLevelUGGRDPP') == 'UG'), lit('Continuing')).alias('studentTypeFirstDegreeSeeking'),
                when((col('annualSurvey') == True) & (col('isNonDegreeSeeking') == False) & (col('NDSRn') == 1) & (col('FFTRn') != 1) 
                    & (col('studentLevelUGGRDPP') == 'UG'), lit(False)).alias('isNonDegreeSeekingFirstDegreeSeeking'))
            .groupBy(
                col('personIdFirstDegreeSeeking'),
                col('yearTypeFirstDegreeSeeking'))
            .agg(
                max(col('studentTypeFirstDegreeSeeking')).alias('studentTypeFirstDegreeSeeking'),
                max(col('isNonDegreeSeekingFirstDegreeSeeking')).alias('isNonDegreeSeekingFirstDegreeSeeking')))
        
        cohort_preFallSummer = (cohort_priority
            .select(
                col('personId').alias('personIdPreFallSummer'),
                col('yearType').alias('yearTypePreFallSummer'),
                col('surveySection').alias('surveySectionPreFallSummer'),
                col('termCode').alias('termCodePreFallSummer'), 
                when((col('termType') == 'Pre-Fall Summer') & (col('studentLevelUGGRDPP') == 'UG') & (col('isNonDegreeSeeking') == False), col('studentType')).alias('studentTypePreFallSummer'))
            .groupBy(
                col('personIdPreFallSummer'),
                col('yearTypePreFallSummer'))
            .agg(
                max(col('studentTypePreFallSummer')).alias('studentTypePreFallSummer')))
            
        cohort_maxStudentLevel = (cohort_priority
            .groupBy(
                col('personId').alias('personIdMaxStudentLevel'),
                col('yearType').alias('yearTypeMaxStudentLevel'))
            .agg( 
                min(col('studentLevelUGGRDPP')).alias('maxStudentLevel')))
            
        cohort = (cohort_priority
            .join(
                cohort_firstDegreeSeeking,
                (col('personId') == col('personIdFirstDegreeSeeking')) & (col('yearType') == col('yearTypeFirstDegreeSeeking')), 'left')
            .join(
                cohort_preFallSummer,
                (col('personId') == col('personIdPreFallSummer')) & (col('yearType') == col('yearTypePreFallSummer')), 'left')
            .join(
                cohort_maxStudentLevel,
                (col('personId') == col('personIdMaxStudentLevel')) & (col('yearType') == col('yearTypeMaxStudentLevel')), 'left')
            .select(
                col('yearType'),
                col('surveySection'),
                col('surveyYear'),
                col('surveyYear').cast('int').alias('surveyYear_int'),
                col('surveyType'),
                col('surveyId'),
                col('personId'),
                col('termCode'),
                col('NDSRn'),
                col('FFTRn'),
                col('fullTermOrder'),
                col('termCodeOrder'),
                col('maxCensus'),
                col('termType'),
                col('financialAidYear'),
                col('studentLevelUGGRDPP'), 
                col('maxStudentLevel'),
                col('isNonDegreeSeeking'),
                col('isNonDegreeSeekingFirstDegreeSeeking'),
                col('timeStatus'),
                col('studentType'),
                col('studentTypePreFallSummer'),
                col('studentTypeFirstDegreeSeeking'),
                col('ethnicity'),
                col('gender'),
                col('distanceEducationType'),
                col('studyAbroadStatus'),
                col('residency'),
                col('cipCode'),
                col('awardLevel'),
                col('UGCreditHours'),
                col('UGClockHours'),
                col('GRCreditHours'),
                col('DPPCreditHours'),
                col('icOfferUndergradAwardLevel'),
                col('icOfferGraduateAwardLevel'),
                col('icOfferDoctorAwardLevel'),
                col('instructionalActivityType'),
                col('tmAnnualDPPCreditHoursFTE'),
                col('includeNonDegreeAsUG')))

        return cohort
    
    else:
        if course_type_counts_in.rdd.isEmpty() == False:
            return student_in
        else: return course_type_counts_in
