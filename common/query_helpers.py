%pyspark

import logging
import sys
import boto3
import json
from uuid import uuid4
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum, expr, col, lit, upper, to_timestamp, max, min, row_number, date_trunc, \
    to_date, when, coalesce, count, rank, concat, substring, date_add, date_sub, array_contains, regexp_replace, abs
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
                #lit('N').alias('sfaReportPriorYear'),  # 'N'
                #lit('N').alias('sfaReportSecondPriorYear'),  # 'N'
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
        elif survey_type == 'FE':
            report_prior = 'Y'
            report_prior_2 = 'N'
        else:
            report_prior = 'N'
            report_prior_2 = 'N'

        term_or_date = ipeds_client_config_in.first()['compGradDateOrTerm']
            
        ipeds_reporting_period_in = (ipeds_reporting_period_in
            .withColumn('report_prior_year', lit(report_prior))
            .withColumn('report_prior_year_2', lit(report_prior_2))
            .filter(((upper(ipeds_reporting_period_in.surveySection).isin(default_values_in['current_survey_sections']) == True) |
                ((col('report_prior_year') == 'Y') & (upper(ipeds_reporting_period_in.surveySection).isin(default_values_in['prior_survey_sections']) == True)) |
                ((col('report_prior_year_2') == 'Y') & (upper(ipeds_reporting_period_in.surveySection).isin(default_values_in['prior_2_survey_sections']) == True))))
            .select(
                upper(col('termCode')).alias('termCode'),
                coalesce(upper(col('partOfTermCode')), lit('1')).alias('partOfTermCode'),
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'),
                coalesce(to_timestamp(col('recordActivityDate')), to_timestamp(lit('9999-09-09'))).alias('recordActivityDate'),
                ipeds_reporting_period_in.surveyCollectionYear,
                lit(term_or_date).alias('report_term_or_date'),
                upper(ipeds_reporting_period_in.surveyId).alias('surveyId'),
                upper(ipeds_reporting_period_in.surveyName).alias('surveyName'),
                upper(ipeds_reporting_period_in.surveySection).alias('surveySection'),
                lit(default_values_in['report_start']).alias('reportStartDate'),
                lit(default_values_in['report_end']).alias('reportEndDate'),
                when(upper(ipeds_reporting_period_in.surveySection).isin(default_values_in['current_survey_sections']), lit('CY'))
                    .when(upper(ipeds_reporting_period_in.surveySection).isin(default_values_in['prior_survey_sections']), lit('PY1'))
                    .when(upper(ipeds_reporting_period_in.surveySection).isin(default_values_in['prior_2_survey_sections']), lit('PY2')).alias('yearType'),
                ipeds_reporting_period_in.tags)
            .withColumn('ipedsRepPerRowNum', row_number().over(Window
                .partitionBy(
                    col('surveySection'), 
                    col('termCode'), 
                    col('partOfTermCode'))
                .orderBy(
                    col('snapshotDateTimestamp').desc(),
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
                academic_term_in.endDate.alias('termEndDate'),
                academic_term_in.financialAidYear,
                upper(col('termCode')).alias('termCode'),
                coalesce(upper(col('partOfTermCode')), lit('1')).alias('partOfTermCode'),
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'),
                to_timestamp(coalesce(col('recordActivityDate'), lit('9999-09-09'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), lit('9999-09-09')).alias('recordActivityDate'), 
                academic_term_in.partOfTermCodeDescription,
                academic_term_in.requiredFTCreditHoursGR,
                academic_term_in.requiredFTCreditHoursUG,
                academic_term_in.requiredFTClockHoursUG,
                academic_term_in.startDate.alias('termStartDate'),
                academic_term_in.termClassification,
                academic_term_in.termCodeDescription,
                academic_term_in.termType,
                academic_term_in.tags)
            .withColumn('acadTermRowNum', row_number().over(Window
                .partitionBy(
                    col('snapshotDateTimestamp'),
                    col('termCode'), 
                    col('partOfTermCode'))
                .orderBy(
                    col('recordActivityDateTimestamp').desc())))
            .filter(col('acadTermRowNum') == 1)
            #.drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            #.drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp')))
    
        academic_term_order = (academic_term_2
            .select(
                academic_term_2.termCode,
                academic_term_2.partOfTermCode,
                academic_term_2.censusDate,
                academic_term_2.termStartDate,
                academic_term_2.termEndDate)
            .distinct())
    
        part_of_term_order = (academic_term_order
            .select(
                academic_term_order['*'],
                rank().over(Window
                    .orderBy(
                        col('censusDate').asc(), 
                        col('termStartDate').asc())).alias('partOfTermOrder'))
            .where((col('termCode').isNotNull()) & (col('partOfTermCode').isNotNull())))
    
        academic_term_order_max = (part_of_term_order
            .groupBy('termCode')
            .agg(max(part_of_term_order.partOfTermOrder).alias('termCodeOrder'),
                max(part_of_term_order.censusDate).alias('maxCensus'),
                min(part_of_term_order.termStartDate).alias('minTermStart'),
                max('termEndDate').alias('maxTermEnd')))
    
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
                academic_term_order_max.minTermStart,
                academic_term_order_max.maxTermEnd)
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
                ipeds_reporting_period_in.yearType.alias('repPerYearType'),
                ipeds_reporting_period_in.surveySection.alias('repPerSurveySection'),
                ipeds_reporting_period_in.termCode.alias('repPerTermCode'),
                ipeds_reporting_period_in.partOfTermCode.alias('repPerPartOfTermCode'),
                academic_term_in.snapshotDateTimestamp.alias('repPerSnapshotDateTimestamp'),
                academic_term_in.snapshotDate.alias('repPerSnapshotDate'),
                academic_term_in.tags.alias('repPerTags'),
                academic_term_in.termCodeOrder.alias('repPerTermCodeOrder'),
                academic_term_in.partOfTermOrder.alias('repPerPartOfTermCodeOrder'),
                when(col('termClassification') == 'Standard Length', lit(1))
                    .when(col('termClassification').isNull(), when(col('termType').isin('Fall', 'Spring'), lit(1)).otherwise(lit(2)))
                    .otherwise(lit(2)).alias('repPerFullTermOrder'),
                to_date(academic_term_in.maxCensus, 'YYYY-MM-DD').alias('repPerMaxCensus'),
                to_date(academic_term_in.minTermStart, 'YYYY-MM-DD').alias('repPerMinTermStart'),
                to_date(academic_term_in.maxTermEnd, 'YYYY-MM-DD').alias('repPerMaxTermEnd'),
                coalesce(to_date(academic_term_in.censusDate, 'YYYY-MM-DD'), (when(col('termType') == 'Fall', lit(default_values_in['default_fall_census'])))).alias('repPerCensusDate'),
                academic_term_in.termClassification.alias('repPerTermClassification'),
                academic_term_in.termType.alias('repPerTermType'),
                to_date(academic_term_in.termStartDate, 'YYYY-MM-DD').alias('repPerTermStartDate'),
                to_date(academic_term_in.termEndDate, 'YYYY-MM-DD').alias('repPerTermEndDate'),
                when(ipeds_reporting_period_in.report_term_or_date == 'D', ipeds_reporting_period_in.reportStartDate).otherwise(to_date(academic_term_in.minTermStart, 'YYYY-MM-DD')).alias('repPerReportStartDate'),
                when(ipeds_reporting_period_in.report_term_or_date == 'D', ipeds_reporting_period_in.reportEndDate).otherwise(to_date(academic_term_in.maxTermEnd, 'YYYY-MM-DD')).alias('repPerReportEndDate'),
                ipeds_reporting_period_in.report_term_or_date.alias('repPerReport_term_or_date'),
                academic_term_in.requiredFTCreditHoursGR.alias('repPerRequiredFTCreditHoursGR'),
                academic_term_in.requiredFTCreditHoursUG.alias('repPerRequiredFTCreditHoursUG'),
                academic_term_in.requiredFTClockHoursUG.alias('repPerRequiredFTClockHoursUG'),
                academic_term_in.financialAidYear.alias('repPerFinancialAidYear'))
            .withColumn('repPerDummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
            .withColumn('repPerSnapShotMaxDummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
            .withColumn('repPerSnapShotMinDummyDate', to_date(to_timestamp(lit('1900-09-09')), 'YYYY-MM-DD'))
            .withColumn('repPerEquivCRHRFactor', expr("(coalesce(repPerRequiredFTCreditHoursUG/coalesce(repPerRequiredFTClockHoursUG, repPerRequiredFTCreditHoursUG), 1))"))
            .withColumn('rowNum', row_number().over(Window
                .partitionBy(
                        col('repPerTermCode'), 
                        col('repPerPartOfTermCode'))
                .orderBy(
                    when((array_contains(col('repPerTags'), default_values_in['cohort_academic_fall'])) & (col('repPerTermType') == 'Fall'), lit(1)).otherwise(lit(2)).asc(),
                    when((array_contains(col('repPerTags'), default_values_in['cohort_academic_spring'])) & (col('repPerTermType') == 'Spring'), lit(1)).otherwise(lit(2)).asc(),
                    when((array_contains(col('repPerTags'), default_values_in['cohort_academic_pre_fall_summer'])) & (col('repPerTermType') == 'Summer'), lit(1)).otherwise(lit(2)).asc(),
                    when((array_contains(col('repPerTags'), default_values_in['cohort_academic_post_spring_summer'])) & (col('repPerTermType') == 'Summer'), lit(1)).otherwise(lit(2)).asc(),
                    when((col('repPerSnapshotDate') <= to_date(date_add(col('repPerCensusDate'), 3), 'YYYY-MM-DD')) 
                        & (col('repPerSnapshotDate') >= to_date(date_sub(col('repPerCensusDate'), 1), 'YYYY-MM-DD')), lit(1)).otherwise(lit(2)).asc(),
                    when(col('repPerSnapshotDate') > col('repPerCensusDate'), col('repPerSnapshotDate')).otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                    when(col('repPerSnapshotDate') < col('repPerCensusDate'), col('repPerSnapshotDate')).otherwise(col('repPerSnapShotMinDummyDate')).desc())))
            .filter(col('rowNum') == 1))
    
        max_term_order_summer = (ipeds_reporting_period_2
            .filter(col('repPerTermType') == 'Summer')
            .select(max(col('repPerTermCodeOrder')).alias('maxSummerTerm')))
    
        max_term_order_fall = (ipeds_reporting_period_2
            .filter(col('repPerTermType') == 'Fall')
            .select(max(col('repPerTermCodeOrder')).alias('maxFallTerm')))
    
        academic_term_reporting_refactor_out = (ipeds_reporting_period_2
            .crossJoin(max_term_order_summer)
            .crossJoin(max_term_order_fall)
            .withColumn('repPerTermTypeNew', when((col('repPerTermType') == 'Summer') & (col('repPerTermClassification') != 'Standard Length'),
                when(col('maxSummerTerm') < col('maxFallTerm'), lit('Pre-Fall Summer'))
                .otherwise(lit('Post-Spring Summer'))).otherwise(col('repPerTermType'))))   
    
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
                    col('snapshotDate').alias('snapshotDateTimestamp'),
                    to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
                .withColumn('snapshotDateFilter', lit(snapshotDateFilter_in))
                .withColumn('useSnapshotDatePartition', when(col('snapshotDateFilter').isNull(), col('snapshotDateTimestamp')).otherwise(lit(None)))
                .withColumn('ENTRowNum', row_number().over(Window
                    .partitionBy(
                        col('useSnapshotDatePartition'),
                        col('campus'))
                    .orderBy(
                        when(col('snapshotDateTimestamp') == col('snapshotDateFilter'), lit(1)).otherwise(lit(2)).asc(),
                        col('snapshotDateTimestamp').desc(),
                        col('recordActivityDateTimestamp').desc())))
            .filter(col('ENTRowNum') == 1)
            .drop(col('ENTRowNum'))
            .drop(col('recordActivityDate'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('snapshotDate'))
            #.drop(col('snapshotDateTimestamp'))
            )
    
    return campus_in

def person_cohort_mcr(spark, cohort_df_in):

    person_in = (cohort_df_in
        .join(
            spark.sql("select * from person"),
            (col('isIpedsReportable') == True) &
            (col('stuPersonId') == col('personId')) &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
                | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate'))), 'left')
        .select(
            cohort_df_in['*'],
            to_date(col('birthDate'), 'YYYY-MM-DD').alias('persBirthDate'),
            upper(col('nation')).alias('persNation'),
            upper(col('state')).alias('persState'),
            (when(col('gender') == 'Male', 'M')
                .when(col('gender') == 'Female', 'F')
                .when(col('gender') == 'Non-Binary', col('configGenderForNonBinary'))
                .otherwise(col('configGenderForUnknown'))).alias('persIpedsGender'),
            expr("""
                (case when isUSCitizen = 1 or ((coalesce(isInUSOnVisa, false) = 1 or repPerCensusDate between visaStartDate and visaEndDate)
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
                    when ((coalesce(isInUSOnVisa, false) = 1 or repPerCensusDate between person.visaStartDate and person.visaEndDate)
                        and visaType in ('Student Non-resident', 'Employee Non-resident', 'Other Non-resident')) then '1'
                    else '9'
                end) --ipedsEthnicity
                """).alias('persIpedsEthnValue'),
            col('ethnicity').alias('persEthnicity'),
            to_timestamp(coalesce(col('recordActivityDate'), col('repPerDummyDate'))).alias('recordActivityDateTimestamp'),
            coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).alias('recordActivityDate'), 
            to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
            to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
        .withColumn('ENTRowNum',row_number().over(Window
            .partitionBy(
                col('repPerYearType'),
                col('repPerSurveySection'),
                col('stuPersonId'),
                col('stuTermCode'))
            .orderBy(
                when(col('snapshotDateTimestamp') == col('repPerSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                when(col('snapshotDateTimestamp') > col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                    .otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                when(col('snapshotDateTimestamp') < col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                    .otherwise(col('repPerSnapShotMinDummyDate')).desc(),
                col('snapshotDateTimestamp').desc(),
                col('recordActivityDateTimestamp').desc())))
        .filter(col('ENTRowNum') == 1)
        .drop(col('ENTRowNum'))
        .drop(col('recordActivityDate'))
        .drop(col('recordActivityDateTimestamp'))
        .drop(col('snapshotDate'))
        .drop(col('snapshotDateTimestamp')))

    return person_in

def academic_track_cohort_mcr(spark, cohort_df_in, academic_term_in, priority_type_in = 'first'):

#for highest priority major only, priority_type_in = 'first'
#for all majors, priority_type_in = 'all' ***will need to capture 1st and 2nd majors for Completions

    academic_track_in = (cohort_df_in
        .withColumn('priorityInd', lit(priority_type_in))
        .join(
            spark.sql("select * from academicTrack"),
            (col('isIpedsReportable') == True) &
            (col('stuPersonId') == col('personId')) &
            (col('fieldOfStudyType') == 'Major') &
            (((coalesce(to_date(col('fieldOfStudyActionDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                (coalesce(to_date(col('fieldOfStudyActionDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
            | ((coalesce(to_date(col('fieldOfStudyActionDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate')) & 
                (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
            | ((coalesce(to_date(col('fieldOfStudyActionDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate')) & 
                (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate')))), 'left')
        .join(
            academic_term_in,
            (academic_term_in.termCode == upper(col('termCodeEffective'))) &
            (academic_term_in.termCodeOrder <= col('repPerTermCodeOrder')), 'left')
        .drop(academic_term_in.snapshotDate)
        .drop(academic_term_in.snapshotDateTimestamp)
        .select(
            cohort_df_in['*'],
            upper(col('degreeProgram')).alias('degreeProgram_out'),
            upper(col('degreeProgram')).alias('acadTrkDegreeProgram'),
            col('academicTrackStatus').alias('academicTrackStatus'),
            coalesce(col('fieldOfStudyPriority'), lit(1)).alias('fieldOfStudyPriority'),
            col('priorityInd'),
            upper(col('termCodeEffective')).alias('acadTrkTermCodeEffective'),
            col('termCodeOrder').alias('acadTrkTermOrder'),
            upper(col('campusOverride')).alias('campusOverride_out'),
            to_timestamp(coalesce(col('fieldOfStudyActionDate'), col('repPerDummyDate'))).alias('fieldOfStudyActionDateTimestamp'),
            coalesce(to_date(col('fieldOfStudyActionDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).alias('fieldOfStudyActionDate'), 
            to_timestamp(coalesce(col('recordActivityDate'), col('repPerDummyDate'))).alias('recordActivityDateTimestamp'),
            coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).alias('recordActivityDate'), 
            to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
            to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
        .withColumn('ENTRowNum', row_number().over(Window
            .partitionBy(
                col('repPerYearType'),
                col('repPerSurveySection'),
                col('stuPersonId'),
                col('stuTermCode'),
                when(col('priorityInd') == 'all', col('fieldOfStudyPriority')).otherwise(col('priorityInd')))
            .orderBy(
                when(col('snapshotDateTimestamp') == col('repPerSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                when(col('snapshotDateTimestamp') > col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                    .otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                when(col('snapshotDateTimestamp') < col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                    .otherwise(col('repPerSnapShotMinDummyDate')).desc(),
                col('snapshotDateTimestamp').desc(),
                when(col('priorityInd') == 'all', col('priorityInd')).otherwise(col('fieldOfStudyPriority')).asc(),
                col('acadTrkTermOrder').desc(),
                col('fieldOfStudyActionDate').desc(),
                col('recordActivityDateTimestamp').desc(),
                when(col('academicTrackStatus') == 'In Progress', lit(1)).otherwise(lit(2)).asc())))
        .filter(col('ENTRowNum') == 1)
        .drop(col('ENTRowNum'))
        .drop(col('recordActivityDate'))
        .drop(col('recordActivityDateTimestamp'))
        .drop(col('snapshotDate'))
        .drop(col('snapshotDateTimestamp')))

    return academic_track_in
    
def degree_program_cohort_mcr(spark, cohort_df_in, academic_term_in, data_type_in = 'all'):

#for degree program data only, data_type_in = 'program_only'
#for degree program, degree and field of study data, data_type_in = 'all'

    degree_program_in = (cohort_df_in
        .join(
            spark.sql("select * from degreeProgram"),
            (col('isIpedsReportable') == True) &
            (col('degreeProgram_out') == upper(col('degreeProgram'))) &
            (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
                | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate'))), 'left')
        .join(
            academic_term_in,
            (academic_term_in.termCode == upper(col('termCodeEffective'))) &
            (academic_term_in.termCodeOrder <= col('repPerTermCodeOrder')), 'left')
        .drop(academic_term_in.snapshotDate)
        .drop(academic_term_in.snapshotDateTimestamp)
        .select(
            cohort_df_in['*'],
            upper(col('degreeProgram')).alias('degProgDegreeProgram'),
            upper(col('degree')).alias('degree_out'),
            upper(col('degree')).alias('degProgDegree'),
            upper(col('major')).alias('degProgMajor'),
            upper(col('major')).alias('major_out'),
            coalesce(col('campusOverride_out'), upper(col('campus'))).alias('degProgCampus'),
            col('startDate').alias('degProgStartDate'),
            coalesce(col('isESL'), lit(False)).alias('degProgIsESL'),
            col('lengthInMonths').alias('degProgLengthInMonths'),
            upper(col('termCodeEffective')).alias('degProgTermCodeEffective'),
            academic_term_in.termCodeOrder.alias('degProgTermOrder'), 
            to_timestamp(coalesce(col('recordActivityDate'), col('repPerDummyDate'))).alias('recordActivityDateTimestamp'),
            coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).alias('recordActivityDate'), 
            to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
            to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
        .withColumn('ENTRowNum', row_number().over(Window
            .partitionBy(
                col('repPerYearType'),
                col('repPerSurveySection'),
                col('stuPersonId'),
                col('stuTermCode'))
            .orderBy(
                when(col('snapshotDateTimestamp') == col('repPerSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                when(col('snapshotDateTimestamp') > col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                    .otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                when(col('snapshotDateTimestamp') < col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                    .otherwise(col('repPerSnapShotMinDummyDate')).desc(),
                col('snapshotDateTimestamp').desc(),
                col('degProgTermOrder').desc(),
                col('degProgStartDate').desc(),
                col('recordActivityDate').desc(),
                when(coalesce(col('degProgDegree'), lit(0)) == 0, lit(2)).otherwise(lit(1)).asc(),
                when(coalesce(col('degProgMajor'), lit(0)) == 0, lit(2)).otherwise(lit(1)).asc())))
        .filter(col('ENTRowNum') == 1)
        .drop(col('ENTRowNum'))
        .drop(col('recordActivityDate'))
        .drop(col('recordActivityDateTimestamp'))
        .drop(col('snapshotDate')))

    degree_program_campus = (degree_program_in
        .join(
            campus_mcr(spark, degree_program_in.first()['snapshotDateTimestamp']),
            (col('degProgCampus') == upper(col('campus'))), 'left')
        .select(
            degree_program_in['*'],
            coalesce(col('isInternational'), lit(False)).alias('campusIsInternational'))
        .drop(col('snapshotDateTimestamp')))

    if data_type_in == 'all':
        degree_program_degree = degree_cohort_mcr(spark, cohort_df_in = degree_program_campus)
        degree_program_field_of_study = field_of_study_cohort_mcr(spark, cohort_df_in = degree_program_degree)
        return degree_program_field_of_study
    else:
        return degree_program_campus
    
def degree_cohort_mcr(spark, cohort_df_in):

    degree_in = (cohort_df_in
        .join(
            spark.sql("select * from degree"),
            (col('isIpedsReportable') == True) &
            (col('degree_out') == upper(col('degree'))) &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
                | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate'))), 'left')
        .select(
            cohort_df_in['*'],
            col('awardLevel').alias('awardLevel'),
            col('degreeLevel'),
            coalesce(col('isNonDegreeSeeking_calc'), col('isNonDegreeSeeking'), lit(False)).alias('isNonDegreeSeeking_final'),
            to_timestamp(coalesce(col('recordActivityDate'), col('repPerDummyDate'))).alias('recordActivityDateTimestamp'),
            coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).alias('recordActivityDate'), 
            to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
            to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
        .withColumn('ENTRowNum', row_number().over(Window
            .partitionBy(
                col('repPerYearType'),
                col('repPerSurveySection'),
                col('stuPersonId'),
                col('stuTermCode'))
            .orderBy(
                when(col('snapshotDateTimestamp') == col('repPerSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                when(col('snapshotDateTimestamp') > col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                    .otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                when(col('snapshotDateTimestamp') < col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                    .otherwise(col('repPerSnapShotMinDummyDate')).desc(),
                col('snapshotDateTimestamp').desc(),
                col('recordActivityDate').desc())))
        .filter(col('ENTRowNum') == 1)
        .drop(col('ENTRowNum'))
        .drop(col('recordActivityDate'))
        .drop(col('recordActivityDateTimestamp'))
        .drop(col('snapshotDate'))
        .drop(col('snapshotDateTimestamp')))

    return degree_in
    
def field_of_study_cohort_mcr(spark, cohort_df_in):

    field_of_study_in = (cohort_df_in
        .join(
            spark.sql("select * from fieldOfStudy"),
            (col('isIpedsReportable') == True) &
            (col('major_out') == upper(col('fieldOfStudy'))) &
            (col('fieldOfStudyType') == 'Major') &
            (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
            | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate'))), 'left')              
        .select(
            cohort_df_in['*'],
             col('cipCode').alias('fldOfStdyCipCode'), 
                to_timestamp(coalesce(col('recordActivityDate'), col('repPerDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).alias('recordActivityDate'), 
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'))
        .withColumn('ENTRowNum', row_number().over(Window
            .partitionBy(
                col('repPerYearType'),
                col('repPerSurveySection'),
                col('stuPersonId'),
                col('stuTermCode'))
            .orderBy(
                when(col('snapshotDateTimestamp') == col('repPerSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                when(col('snapshotDateTimestamp') > col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                    .otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                when(col('snapshotDateTimestamp') < col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                    .otherwise(col('repPerSnapShotMinDummyDate')).desc(),
                col('snapshotDateTimestamp').desc(),
                col('recordActivityDate').desc())))
        .filter(col('ENTRowNum') == 1)
        .drop(col('ENTRowNum'))
        .drop(col('recordActivityDate'))
        .drop(col('recordActivityDateTimestamp'))
        .drop(col('snapshotDate'))
        .drop(col('snapshotDateTimestamp')))

    return field_of_study_in
    
def financial_aid_cohort_mcr(spark, default_values_in, cohort_df_in, ipeds_client_config_in):

    survey_type = cohort_df_in.first()['surveyType']

    financial_aid_snapshot = (spark.sql("select distinct to_date(snapshotDate, 'YYYY-MM-DD') ENTSnapshotDate, snapshotDate ENTSnapshotDateTimestamp, tags from financialAid")
        .withColumn('start_date', lit(default_values_in['financial_aid_start']))
        .withColumn('end_date', lit(default_values_in['financial_aid_end']))
        .withColumn('dummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
        .withColumn('snapShotMaxDummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
        .withColumn('snapShotMinDummyDate', to_date(to_timestamp(lit('1900-09-09')), 'YYYY-MM-DD'))
        .withColumn('ENTRowNum', row_number().over(Window
                .orderBy(
                        when(array_contains(col('tags'), default_values_in['financial_aid']), lit(1)).otherwise(lit(2)).asc(),
                        when((col('ENTSnapshotDate') <= date_add(col('end_date'), 15)) 
                            & (col('ENTSnapshotDate') >= date_sub(col('start_date'), 15)), lit(1)).otherwise(lit(2)).asc(),
                        when((col('ENTSnapshotDate') <= date_add(col('end_date'), 5)) 
                            & (col('ENTSnapshotDate') >= date_sub(col('end_date'), 5)), col('ENTSnapshotDateTimestamp')).otherwise(col('snapShotMinDummyDate')).desc(),
                        when(col('ENTSnapshotDate') > col('end_date'), col('ENTSnapshotDate')).otherwise(col('snapShotMaxDummyDate')).asc(),
                        when(col('ENTSnapshotDate') < col('end_date'), col('ENTSnapshotDate')).otherwise(col('snapShotMinDummyDate')).desc()
                        )))
        .filter(col('ENTRowNum') == 1)
        .drop(col('ENTRowNum')))

    financial_aid_in = (financial_aid_snapshot
        .join(
            spark.sql("select * from financialAid"),
            (col('snapshotDate') == col('ENTSnapshotDateTimestamp')) & 
                (((coalesce(to_date(col('awardStatusActionDate'), 'YYYY-MM-DD'), col('dummyDate')) != col('dummyDate')) & 
                    (coalesce(to_date(col('awardStatusActionDate'), 'YYYY-MM-DD'), col('dummyDate')) <= col('end_date')))
                | ((coalesce(to_date(col('awardStatusActionDate'), 'YYYY-MM-DD'), col('dummyDate')) == col('dummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) != col('dummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) <= col('end_date')))
                | ((coalesce(to_date(col('awardStatusActionDate'), 'YYYY-MM-DD'), col('dummyDate')) == col('dummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) == col('dummyDate')))), 'left')
        .filter(col('isIpedsReportable') == True))
        
    cohort_financial_aid = (cohort_df_in   
        .join(
            financial_aid_in,
            (cohort_df_in.personId == financial_aid_in.personId) & (cohort_df_in.financialAidYear == financial_aid_in.financialAidYear), 'left')
        .select(
            cohort_df_in['*'],
            financial_aid_in.personId.alias('faPersonId'),
            financial_aid_in.termCode.alias('faTermCode'),
            col('fundType'),
            upper(col('fundCode')).alias('fundCode'),
            col('fundSource'),
            col('recordActivityDate').alias('recordActivityDateTimestamp'),
            to_date(col('recordActivityDate'), 'YYYY-MM-DD').alias('recordActivityDate'), 
            col('snapshotDate').alias('snapshotDateTimestamp'),
            to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'),
            col('awardStatusActionDate').alias('awardStatusActionDateTimestamp'),
            to_date(col('awardStatusActionDate'), 'YYYY-MM-DD').alias('awardStatusActionDate'), 
            col('awardStatus'),
            coalesce(col('isPellGrant'), lit(False)).alias('isPellGrant'),
            coalesce(col('isTitleIV'), lit(False)).alias('isTitleIV'),
            coalesce(col('isSubsidizedDirectLoan'), lit(False)).alias('isSubsidizedDirectLoan'),
            col('acceptedAmount'),
            col('offeredAmount'),
            col('paidAmount'),
            col('IPEDSFinancialAidAmount'),
            col('IPEDSOutcomeMeasuresAmount'),
            round(regexp_replace(col('familyIncome'), ',', ''), 0).alias('familyIncome'),
            col('livingArrangement'))
        .withColumn('ENTRowNum', row_number().over(Window
            .partitionBy(
                col('yearType'),
                col('personId'),
                col('financialAidYear'),
                col('faTermCode'),
                col('fundCode'),
                col('fundType'),
                col('fundSource'))
            .orderBy(
                col('recordActivityDateTimestamp').desc(),
                col('awardStatusActionDateTimestamp').desc())))
        .filter(col('ENTRowNum') == 1))

    if survey_type == 'SFA':
        cohort_with_financial_aid = (cohort_financial_aid
            .withColumn('ipeds_survey_amount', 
                when((col('IPEDSFinancialAidAmount').isNotNull()) & (col('IPEDSFinancialAidAmount') > 0), col('IPEDSFinancialAidAmount'))
                .when(col('fundType') == 'Loan', col('acceptedAmount'))
                .when(col('fundType').isin('Grant', 'Scholarship'), col('offeredAmount'))
                .when(col('fundType') == 'Work Study', col('paidAmount'))
                .otherwise(col('IPEDSFinancialAidAmount')))
            .withColumn('awardedAidInd', when((~col('awardStatus').isin('Source Denied', 'Cancelled')) & (col('ipeds_survey_amount') > 0), lit(1)).otherwise(lit(0)))
            .filter(col('awardedAidInd') == 1))
                            
        student_fa_totals = (cohort_with_financial_aid
            .select(
                col('yearType'),
                col('personId'),
                col('financialAidYear'),
                col('faTermCode'),
                col('livingArrangement'),
                when((col('fundType') == 'Loan') & (col('fundSource') == 'Federal'), col('ipeds_survey_amount')).otherwise(lit(0)).alias('federalLoan'),
                when((col('fundType').isin('Grant', 'Scholarship')) & (col('fundSource') == 'Federal'), col('ipeds_survey_amount')).otherwise(lit(0)).alias('federalGrantSchol'),
                when((col('fundType') == 'Work Study') & (col('fundSource') == 'Federal'), col('ipeds_survey_amount')).otherwise(lit(0)).alias('federalWorkStudy'),
                when((col('fundType') == 'Loan') & (col('fundSource').isin('State', 'Local')), col('ipeds_survey_amount')).otherwise(lit(0)).alias('stateLocalLoan'),
                when((col('fundType').isin('Grant', 'Scholarship')) & (col('fundSource').isin('State', 'Local')), col('ipeds_survey_amount')).otherwise(lit(0)).alias('stateLocalGrantSchol'),
                when((col('fundType') == 'Work Study') & (col('fundSource').isin('State', 'Local')), col('ipeds_survey_amount')).otherwise(lit(0)).alias('stateLocalWorkStudy'),
                when((col('fundType') == 'Loan') & (col('fundSource') == 'Institution'), col('ipeds_survey_amount')).otherwise(lit(0)).alias('institutionLoan'),
                when((col('fundType').isin('Grant', 'Scholarship')) & (col('fundSource') == 'Institution'), col('ipeds_survey_amount')).otherwise(lit(0)).alias('institutionGrantSchol'),
                when((col('fundType') == 'Work Study') & (col('fundSource') == 'Institution'), col('ipeds_survey_amount')).otherwise(lit(0)).alias('institutionalWorkStudy'),
                when((col('fundType') == 'Loan') & (col('fundSource') == 'Other'), col('ipeds_survey_amount')).otherwise(lit(0)).alias('otherLoan'),
                when((col('fundType').isin('Grant', 'Scholarship')) & (col('fundSource') == 'Other'), col('ipeds_survey_amount')).otherwise(lit(0)).alias('otherGrantSchol'),
                when((col('fundType') == 'Work Study') & (col('fundSource') == 'Other'), col('ipeds_survey_amount')).otherwise(lit(0)).alias('otherWorkStudy'),
                when((col('isPellGrant') == True), col('ipeds_survey_amount')).otherwise(lit(0)).alias('pellGrant'),
                when((col('isTitleIV') == True), col('ipeds_survey_amount')).otherwise(lit(0)).alias('titleIV'),
                when((col('fundType').isin('Grant', 'Scholarship')), col('ipeds_survey_amount')).otherwise(lit(0)).alias('allGrantSchol'),
                when((col('fundType') == 'Loan'), col('ipeds_survey_amount')).otherwise(lit(0)).alias('allLoan'),
                when((col('fundType').isin('Grant', 'Scholarship')) & (col('fundSource') == 'Federal') & (col('isPellGrant') == False), col('ipeds_survey_amount')).otherwise(lit(0)).alias('nonPellFederalGrantSchol'),
                coalesce(col('ipeds_survey_amount'), lit(0)).alias('totalAid'),
                when(col('familyIncome') <= 30000, 1)
                    .when((col('familyIncome') > 30000) & (col('familyIncome') <= 48000), 2)
                    .when((col('familyIncome') > 48000) & (col('familyIncome') <= 75000), 3)
                    .when((col('familyIncome') > 75000) & (col('familyIncome') <= 110000), 4)
                    .when(col('familyIncome') > 110000, 5).otherwise(lit(1)).alias('familyIncomeCat'),
                when(col('isGroup2Ind') == 1, col('ipeds_survey_amount')).alias('group2aTotal'),
                when((col('isGroup2Ind') == 1) & (col('fundType').isin('Loan', 'Grant', 'Scholarship')) & (col('fundSource').isin('Federal', 'State', 'Local', 'Institution')), col('ipeds_survey_amount')).alias('group2bTotal'),
                when((col('isGroup2Ind') == 1) & (col('fundType').isin('Grant', 'Scholarship')) & (col('fundSource').isin('Federal', 'State', 'Local', 'Institution')), col('ipeds_survey_amount')).alias('group3Total'),
                when((col('isGroup2Ind') == 1) & (col('fundType').isin('Grant', 'Scholarship')) & (col('fundSource').isin('Federal', 'State', 'Local', 'Institution')) & (~col('fundCode').isin(col('sfaCaresAct1'), col('sfaCaresAct2'))), col('ipeds_survey_amount')).alias('group3Total_caresAct'),
                when((col('isGroup2Ind') == 1) & (col('isTitleIV') == True), col('ipeds_survey_amount')).alias('group4Total'),
                when((col('isGroup2Ind') == 1) & (col('isTitleIV') == True) & (~col('fundCode').isin(col('sfaCaresAct1'), col('sfaCaresAct2'))), col('ipeds_survey_amount')).alias('group4Total_caresAct')) 
            .groupBy(
                'personId', 
                #'financialAidYear', 
                'yearType')
            .agg(
                sum('federalLoan').alias('federalLoan'),
                sum('federalGrantSchol').alias('federalGrantSchol'),
                sum('federalWorkStudy').alias('federalWorkStudy'),
                sum('stateLocalLoan').alias('stateLocalLoan'),
                sum('stateLocalGrantSchol').alias('stateLocalGrantSchol'),
                sum('stateLocalWorkStudy').alias('stateLocalWorkStudy'),
                sum('institutionLoan').alias('institutionLoan'),
                sum('institutionGrantSchol').alias('institutionGrantSchol'),
                sum('institutionalWorkStudy').alias('institutionalWorkStudy'),
                sum('otherLoan').alias('otherLoan'),
                sum('otherGrantSchol').alias('otherGrantSchol'),
                sum('otherWorkStudy').alias('otherWorkStudy'),
                sum('pellGrant').alias('pellGrant'),
                sum('titleIV').alias('titleIV'),
                sum('allGrantSchol').alias('allGrantSchol'),
                sum('allLoan').alias('allLoan'),
                sum('nonPellFederalGrantSchol').alias('nonPellFederalGrantSchol'),
                sum('totalAid').alias('totalAid'),
                sum('group2aTotal').alias('group2aTotal'),
                sum('group2bTotal').alias('group2bTotal'),
                sum('group3Total').alias('group3Total'),
                sum('group3Total').alias('group3Total_caresAct'),
                sum('group4Total').alias('group4Total'),
                sum('group4Total').alias('group4Total_caresAct'),
                max('familyIncomeCat').alias('familyIncomeCat')
                ))

        cohort_fa = (cohort_df_in
            .join(
                student_fa_totals,
                (cohort_df_in.personId == student_fa_totals.personId) & 
                    #(cohort_df_in.financialAidYear == student_fa_totals.financialAidYear) & 
                    (cohort_df_in.yearType == student_fa_totals.yearType), 'left')
            .select(
                cohort_df_in.personId,
                #cohort_df_in.financialAidYear,
                cohort_df_in.yearType,
                cohort_df_in.isGroup2Ind,
                cohort_df_in.residency,
                student_fa_totals['*']))
                
    else:  #survey_type == 'GR': or 'OM'
        cohort_with_financial_aid = (cohort_financial_aid
            .withColumn('ipeds_survey_amount', 
                when((col('IPEDSOutcomeMeasuresAmount').isNotNull()) & (col('IPEDSOutcomeMeasuresAmount') > 0), col('IPEDSOutcomeMeasuresAmount'))
                .otherwise(col('paidAmount')))
            .withColumn('awardedAidInd', when((~col('awardStatus').isin('Source Denied', 'Cancelled')) & (col('ipeds_survey_amount') > 0), lit(1)).otherwise(lit(0)))
            .filter((col('awardedAidInd') == 1)))

        student_fa_totals = (cohort_with_financial_aid
            .select(
                col('personId'),
                col('yearType'),
                col('financialAidYear'),
                col('faTermCode'),
                when(col('isPellGrant') == True, col('ipeds_survey_amount')).otherwise(lit(0)).alias('pellGrantAmt'),
                when(col('isSubsidizedDirectLoan') == True, col('ipeds_survey_amount')).otherwise(lit(0)).alias('subsidLoanAmt'))
            .groupBy(
                col('personId'),
                #col('financialAidYear'),
                col('yearType'))
            .agg(
                sum('pellGrantAmt').alias('pellGrantAmt'),
                sum('subsidLoanAmt').alias('subsidLoanAmt')))

        cohort_fa = (cohort_df_in
            .join(
                student_fa_totals,
                (cohort_df_in.personId == student_fa_totals.personId) & 
                    #(cohort_df_in.financialAidYear == student_fa_totals.financialAidYear) & 
                    (cohort_df_in.yearType == student_fa_totals.yearType), 'left')
            .select(
                cohort_df_in['*'],
                when(col('pellGrantAmt') > 0, lit(1)).otherwise(lit(0)).alias('isPellRec'),
                when(col('pellGrantAmt') > 0, lit(0))
                    .when(col('subsidLoanAmt') > 0, lit(1))
                    .otherwise(lit(0)).alias('isSubLoanRec')))
        
    return cohort_fa

def military_benefit_mcr(spark, default_values_in, benefit_type_in):

    if benefit_type_in == 'GI Bill':
        tag = default_values_in['gi_bill']
        start_date = default_values_in['gi_bill_start']
        end_date = default_values_in['gi_bill_end']

    else: #benefit_type_in == 'Department of Defense'
        tag = default_values_in['department_of_defense']
        start_date = default_values_in['department_of_defense_start']
        end_date = default_values_in['department_of_defense_end']

    military_benefit_snapshot = (spark.sql("select distinct to_date(snapshotDate, 'YYYY-MM-DD') ENTSnapshotDate, snapshotDate ENTSnapshotDateTimestamp, tags from militaryBenefit")
        .withColumn('start_date', lit(start_date))
        .withColumn('end_date', lit(end_date))
        .withColumn('req_benefit_type', lit(benefit_type_in))
        .withColumn('dummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
        .withColumn('snapShotMaxDummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
        .withColumn('snapShotMinDummyDate', to_date(to_timestamp(lit('1900-09-09')), 'YYYY-MM-DD'))
        .withColumn('ENTRowNum', row_number().over(Window
                .orderBy(
                        when(array_contains(col('tags'), tag), lit(1)).otherwise(lit(2)).asc(),
                        when((col('ENTSnapshotDate') <= date_add(col('end_date'), 15)) 
                            & (col('ENTSnapshotDate') >= date_sub(col('start_date'), 15)), lit(1)).otherwise(lit(2)).asc(),
                        when((col('ENTSnapshotDate') <= date_add(col('end_date'), 5)) 
                            & (col('ENTSnapshotDate') >= date_sub(col('end_date'), 5)), col('ENTSnapshotDateTimestamp')).otherwise(col('snapShotMinDummyDate')).desc(),
                        when(col('ENTSnapshotDate') > col('end_date'), col('ENTSnapshotDate')).otherwise(col('snapShotMaxDummyDate')).asc(),
                        when(col('ENTSnapshotDate') < col('end_date'), col('ENTSnapshotDate')).otherwise(col('snapShotMinDummyDate')).desc()
                        )))
        .filter(col('ENTRowNum') == 1)
        .drop(col('ENTRowNum')))

    military_benefit_in = (military_benefit_snapshot
        .join(
            spark.sql("select * from militaryBenefit"),
            (col('snapshotDate') == col('ENTSnapshotDateTimestamp')) &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) != col('dummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')).between(col('start_date'), col('end_date'))))
                    | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) == col('dummyDate'))) &
                (to_date(col('transactionDate'), 'YYYY-MM-DD').between(col('start_date'), col('end_date'))) &
                (col('benefitType') == col('req_benefit_type')) &
                (col('isIpedsReportable') == True), 'left')
        .select(
            col('personId').alias('milbenPersonId'),
            upper(col('termCode')).alias('termCode'),
            abs(col('benefitAmount')).alias('benefitAmount'),
            col('transactionDate'),
            col('recordActivityDate').alias('recordActivityDateTimestamp'),
            to_date(col('recordActivityDate'), 'YYYY-MM-DD').alias('recordActivityDate'), 
            col('snapshotDate').alias('ENTSnapshotDateTimestamp'),
            to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('ENTSnapshotDate'),
            col('end_date'),
            col('dummyDate'),
            col('snapShotMaxDummyDate'),
            col('snapShotMinDummyDate'))
        .withColumn('ENTRowNum', row_number().over(Window
            .partitionBy(
                col('milbenPersonId'),
                col('termCode'),
                col('transactionDate'),
                col('benefitAmount'))
            .orderBy(
                col('recordActivityDateTimestamp').desc())))
        .filter(col('ENTRowNum') == 1)
        .groupBy(
            col('milbenPersonId'),
            col('ENTSnapshotDateTimestamp'),
            col('end_date'),
            col('dummyDate'),
            col('snapShotMaxDummyDate'),
            col('snapShotMinDummyDate'))
        .agg(
            sum(col('benefitAmount')).alias('benefitAmount')))

    military_benefit_level = (military_benefit_in
        .join(
            spark.sql("select * from student"),
            (col('milbenPersonId') == col('personId')) &
            (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) != col('dummyDate')) 
                & (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) <= col('end_date')))
                | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) == col('dummyDate'))) &
            (col('isIpedsReportable') == True), 'left')
        .select(
            col('milbenPersonId'),
            col('benefitAmount'),
            col('ENTSnapshotDateTimestamp'),
            when(col('studentLevel').isin('Masters', 'Doctorate', 'Professional Practice Doctorate'), lit(2)).otherwise(lit(1)).alias('studentLevel'),
            col('snapshotDate').alias('snapshotDateTimestamp'),
            col('recordActivityDate').alias('recordActivityDateTimestamp'),
            col('snapShotMaxDummyDate'),
            col('snapShotMinDummyDate'))
        .withColumn('ENTRowNum', row_number().over(Window
                .partitionBy(
                    col('milbenPersonId'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('ENTSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('ENTSnapshotDateTimestamp'), col('snapshotDateTimestamp')).otherwise(col('snapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('ENTSnapshotDateTimestamp'), col('snapshotDateTimestamp')).otherwise(col('snapShotMinDummyDate')).desc(),
                    col('recordActivityDateTimestamp').desc())))
        .filter(col('ENTRowNum') == 1)
        .groupBy(
            col('studentLevel'))
        .agg(
            count('*').alias('studentCount'),
            sum(col('benefitAmount')).alias('benefitAmount')))

    return military_benefit_level

def award_for_completions_mcr(spark, survey_info_in, default_values_in, reporting_periods_in):

    award_snapshot = (spark.sql("select distinct to_date(snapshotDate, 'YYYY-MM-DD') ENTSnapshotDate, snapshotDate ENTSnapshotDateTimestamp, tags from award")
        .withColumn('end_date', lit(reporting_periods_in.first()['repPerReportEndDate']))
        .withColumn('start_date', lit(reporting_periods_in.first()['repPerReportStartDate']))
        .withColumn('term_or_date', lit(reporting_periods_in.first()['repPerReport_term_or_date']))
        .withColumn('dummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
        .withColumn('snapShotMaxDummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
        .withColumn('snapShotMinDummyDate', to_date(to_timestamp(lit('1900-09-09')), 'YYYY-MM-DD'))
        .withColumn('ENTRowNum', row_number().over(Window
                .orderBy(
                        when((col('term_or_date') == 'D') & (array_contains(col('tags'), default_values_in['cohort_program'])), lit(1)).otherwise(lit(2)).asc(),
                        when((col('term_or_date') == 'T') & (array_contains(col('tags'), default_values_in['cohort_academic_fall'])), lit(1)).otherwise(lit(2)).asc(),
                        when((col('ENTSnapshotDate') <= date_add(col('end_date'), 15)) 
                            & (col('ENTSnapshotDate') >= date_sub(col('start_date'), 15)), lit(1)).otherwise(lit(2)).asc(),
                        when((col('ENTSnapshotDate') <= date_add(col('end_date'), 5)) 
                            & (col('ENTSnapshotDate') >= date_sub(col('end_date'), 5)), col('ENTSnapshotDateTimestamp')).otherwise(col('snapShotMinDummyDate')).desc(),
                        when(col('ENTSnapshotDate') > col('end_date'), col('ENTSnapshotDate')).otherwise(col('snapShotMaxDummyDate')).asc(),
                        when(col('ENTSnapshotDate') < col('end_date'), col('ENTSnapshotDate')).otherwise(col('snapShotMinDummyDate')).desc()
                        )))
        .filter(col('ENTRowNum') == 1)
        .select(
            col('ENTSnapshotDateTimestamp'),
            col('term_or_date')))

    award_in = (award_snapshot
            .join(
                spark.sql("select * from award"),
                (col('snapshotDate') == col('ENTSnapshotDateTimestamp')), 'left')
            .filter(col('isIpedsReportable') == True))
            
    award = (reporting_periods_in
        .join(
            award_in,
            (col('awardedDate').isNotNull()) &
            (to_date(col('awardedDate'),'YYYY-MM-DD') <= col('repPerReportEndDate')) &
            (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).between(col('repPerReportStartDate'), col('repPerReportEndDate'))))
                    | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate'))) &
            (((col('term_or_date') == 'D') & (to_date(col('awardedDate'),'YYYY-MM-DD').between(col('repPerReportStartDate'), col('repPerReportEndDate'))))
                | ((col('term_or_date') == 'T') & (upper(col('awardedTermCode')) == col('regTermCode')))), 'left')
        .select(
            col('personId').alias('stuPersonId'),
            col('repPerYearType'),
            col('repPerSurveySection'),
            col('recordActivityDate').alias('recordActivityDateTimestamp'),
            to_date(col('recordActivityDate'), 'YYYY-MM-DD').alias('recordActivityDate'), 
            award_in.snapshotDate.alias('snapshotDateTimestamp'),
            to_date(award_in.snapshotDate, 'YYYY-MM-DD').alias('snapshotDate'),
            upper(col('awardedTermCode')).alias('awardedTermCode'),
            to_date(col('awardedDate'),'YYYY-MM-DD').alias('awardedDate'),
            upper(col('degreeProgram')).alias('degreeProgram'),
            upper(col('degreeProgram')).alias('degreeProgram_out'),
            col('awardStatus').alias('awardStatus'),
            upper(col('collegeOverride')).alias('collegeOverride'),
            upper(col('divisionOverride')).alias('divisionOverride'),
            upper(col('departmentOverride')).alias('departmentOverride'),
            upper(col('campusOverride')).alias('campusOverride_out'),
            col('repPerReportStartDate'),
            col('repPerReportEndDate'),
            award_in.snapshotDate.alias('repPerSnapshotDateTimestamp'),
            col('repPerDummyDate'),
            col('repPerSnapShotMaxDummyDate'),
            col('repPerSnapShotMinDummyDate'),
            lit('xxxx').alias('stuTermCode'),
            to_date(col('awardedDate'),'YYYY-MM-DD').alias('repPerCensusDate'))
        .withColumn('ENTRowNum', row_number().over(Window
            .partitionBy(
                col('stuPersonId'),
                col('awardedDate'),
                col('degreeProgram_out'))
            .orderBy(
                col('recordActivityDateTimestamp').desc())))
        .filter(col('ENTRowNum') == 1)
        #.filter(col('awardStatus') == 'Awarded') #already checked if awardedDate is not null and <= reportEndDate
        .drop(col('ENTRowNum'))
        .drop(col('snapshotDate'))
        .drop(col('recordActivityDate'))
        .drop(col('snapshotDateTimestamp'))
        .drop(col('recordActivityDateTimestamp')))
         
    return award

def award_cohort_mcr(spark, cohort_df_in, default_tag_in, default_as_of_date_in):
    
    award_snapshot = (spark.sql("select distinct to_date(snapshotDate, 'YYYY-MM-DD') ENTSnapshotDate, snapshotDate ENTSnapshotDateTimestamp, tags from award")
        .withColumn('end_date', lit(default_as_of_date_in))
        .withColumn('dummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
        .withColumn('snapShotMaxDummyDate', to_date(to_timestamp(lit('9999-09-09')), 'YYYY-MM-DD'))
        .withColumn('snapShotMinDummyDate', to_date(to_timestamp(lit('1900-09-09')), 'YYYY-MM-DD'))
        .withColumn('ENTRowNum', row_number().over(Window
                .orderBy(
                    when(array_contains(col('tags'), default_tag_in), lit(1)).otherwise(lit(2)).asc(),
                    when((col('ENTSnapshotDate') <= date_add(col('end_date'), 5)) 
                            & (col('ENTSnapshotDate') >= date_sub(col('end_date'), 5)), col('ENTSnapshotDateTimestamp')).otherwise(col('snapShotMinDummyDate')).desc(),
                    when(col('ENTSnapshotDate') > col('end_date'), col('ENTSnapshotDate')).otherwise(col('snapShotMaxDummyDate')).asc(),
                    when(col('ENTSnapshotDate') < col('end_date'), col('ENTSnapshotDate')).otherwise(col('snapShotMinDummyDate')).desc()
                    )))
        .filter(col('ENTRowNum') == 1))

    award_in = (award_snapshot
            .join(
                spark.sql("select * from award"),
                (col('snapshotDate') == col('ENTSnapshotDateTimestamp')), 'left')
            .filter(col('isIpedsReportable') == True))

    cohort_award = (cohort_df_in
        .join(
            award_in,
            (cohort_df_in.personId == award_in.personId) &
            (col('awardedDate').isNotNull()) &
            (to_date(col('awardedDate'),'YYYY-MM-DD') <= col('end_date')) &
            (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) != col('dummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) <= col('end_date')))
                    | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('dummyDate')) == col('dummyDate'))), 'left')
        .select(
            cohort_df_in['*'],
            upper(col('awardedTermCode')).alias('awardedTermCode'),
            to_date(col('awardedDate'),'YYYY-MM-DD').alias('awardedDate'),
            upper(col('degreeProgram')).alias('degreeProgram'),
            upper(col('degreeProgram')).alias('degreeProgram_out'),
            col('awardStatus').alias('awardStatus'),
            upper(col('campusOverride')).alias('campusOverride_out'),
            award_in.recordActivityDate.alias('recordActivityDateTimestamp'),
            to_date(award_in.recordActivityDate, 'YYYY-MM-DD').alias('recordActivityDate'),
            to_timestamp(award_in.snapshotDate).alias('snapshotDateTimestamp'),
            to_date(award_in.snapshotDate, 'YYYY-MM-DD').alias('snapshotDate'))
        .withColumn('ENTRowNum', row_number().over(Window
            .partitionBy(
                cohort_df_in.personId,
                col('awardedDate'),
                col('degreeProgram'))
            .orderBy(
                col('recordActivityDateTimestamp').desc())))
        .filter(col('ENTRowNum') == 1))
        #.filter(col('awardStatus') == 'Awarded') #already checked if awardedDate is not null and <= reportEndDate

    cohort_award_campus = (cohort_award
        .join(
            campus_mcr(spark, cohort_award.first()['snapshotDateTimestamp']),
            (col('campusOverride_out') == upper(col('campus'))), 'left')
        .select(
            cohort_award['*'],
            coalesce(col('isInternational'), lit(False)).alias('campusIsInternational')))

    return cohort_award_campus
    
def course_type_counts(spark, survey_info_in, default_values_in, ipeds_client_config_in = None, academic_term_in = None, reporting_periods_in = None):

    if not ipeds_client_config_in:
        ipeds_client_config_in = ipeds_client_config_mcr(spark = spark, survey_info_in = survey_info_in)
        
    if not academic_term_in:
        academic_term_in = academic_term_mcr(spark)

    if not reporting_periods_in:   
        if not ipeds_reporting_period_in:
            ipeds_reporting_period_in = ipeds_reporting_period_mcr(spark = spark, survey_info_in = survey_info_in, default_values_in = default_values_in,)
        reporting_periods_in = reporting_periods(spark = spark, survey_info_in = survey_info_in, default_values_in = default_values_in, ipeds_reporting_period_in = ipeds_reporting_period_in, academic_term_in = academic_term_in)   
        
    registration_in = spark.sql("select * from registration").filter(col('isIpedsReportable') == True)

    if (reporting_periods_in.rdd.isEmpty() == False) & (registration_in.rdd.isEmpty() == False):
        
        course_section_in = spark.sql("select * from courseSection").filter(col('isIpedsReportable') == True)
        course_section_schedule_in = spark.sql("select * from courseSectionSchedule").filter(col('isIpedsReportable') == True)
        course_in = spark.sql("select * from course").filter(col('isIpedsReportable') == True)
    
        registration = (registration_in
            .join(
                reporting_periods_in,
                (upper(registration_in.termCode) == col('repPerTermCode')) &
                (coalesce(upper(registration_in.partOfTermCode), lit('1')) == col('repPerPartOfTermCode')) &
                (((coalesce(to_date(col('registrationStatusActionDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                    (coalesce(to_date(col('registrationStatusActionDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
                | ((coalesce(to_date(col('registrationStatusActionDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
                | ((coalesce(to_date(col('registrationStatusActionDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate')) & 
                    (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate')))), 'inner')
            .select(
                registration_in.personId.alias('regPersonId'),
                to_timestamp(registration_in.snapshotDate).alias('snapshotDateTimestamp'),
                to_date(registration_in.snapshotDate, 'YYYY-MM-DD').alias('snapshotDate'),
                col('repPerTermCode'),
                col('repPerPartOfTermCode'),
                upper(registration_in.courseSectionNumber).alias('regCourseSectionNumber'),
                upper(registration_in.courseSectionCampusOverride).alias('regCourseSectionCampusOverride'),
                registration_in.courseSectionLevelOverride.alias('regCourseSectionLevelOverride'),
                coalesce(registration_in.isAudited, lit(False)).alias('regIsAudited'),
                coalesce(registration_in.isEnrolled, lit(True)).alias('regIsEnrolled'),
                to_timestamp(coalesce(registration_in.registrationStatusActionDate, col('repPerDummyDate'))).alias('regStatusActionDateTimestamp'),
                coalesce(to_date(registration_in.registrationStatusActionDate, 'YYYY-MM-DD'), col('repPerDummyDate')).alias('regStatusActionDate'),
                to_timestamp(coalesce(col('recordActivityDate'), col('repPerDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).alias('recordActivityDate'),
                registration_in.enrollmentHoursOverride.alias('regEnrollmentHoursOverride'),
                col('repPerDummyDate'),
                col('repPerSnapShotMaxDummyDate'),
                col('repPerSnapShotMinDummyDate'),
                col('repPerSnapshotDateTimestamp'),
                col('repPerSnapshotDate'),
                col('repPerYearType'),
                col('repPerSurveySection'),
                col('repPerFinancialAidYear'),
                col('repPerTermCodeOrder'),
                col('repPerMaxCensus'),
                col('repPerFullTermOrder'),
                col('repPerTermTypeNew'),
                col('repPerTermStartDate'),
                col('repPerTermEndDate'),
                col('repPerReportStartDate'),
                col('repPerReportEndDate'),
                col('repPerCensusDate'),
                col('repPerEquivCRHRFactor'))
            .withColumn('ENTRowNum', row_number().over(Window
                .partitionBy(
                    col('repPerYearType'),
                    col('repPerSurveySection'),
                    col('repPerTermCode'),
                    col('repPerPartOfTermCode'),
                    col('regPersonId'),
                    col('regCourseSectionNumber'),
                    col('regcourseSectionLevelOverride'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repPerSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repPerSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDateTimestamp').desc(),
                    col('regStatusActionDateTimestamp').desc())))
            .filter((col('ENTRowNum') == 1) & col('regIsEnrolled') == lit('True'))
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('ENTRowNum')))
            
        registration_course_section = (registration
            .join(
                course_section_in,
                (col('repPerTermCode') == upper(col('termCode'))) &
                (col('repPerPartOfTermCode') == upper(coalesce(col('partOfTermCode'), lit('1')))) &
                (col('regCourseSectionNumber') == upper(col('courseSectionNumber'))) &
                (col('termCode').isNotNull()) &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
                    | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate'))), 'left')
            .select(
                registration['*'],
                to_timestamp(coalesce(col('recordActivityDate'), col('repPerDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).alias('recordActivityDate'),
                col('courseSectionLevel').alias('crseSectCourseSectionLevel'),
                upper(col('subject')).alias('crseSectSubject'),
                upper(col('courseNumber')).alias('crseSectCourseNumber'),
                col('courseSectionStatus').alias('crseSectCourseSectionStatus'),
                coalesce(col('isESL'), lit(False)).alias('crseSectIsESL'),
                coalesce(col('isRemedial'), lit(False)).alias('crseSectIsRemedial'),
                upper(col('college')).alias('crseSectCollege'),
                upper(col('division')).alias('crseSectDivision'),
                upper(col('department')).alias('crseSectDepartment'),
                coalesce(col('isClockHours'), lit(False)).alias('crseSectIsClockHours'),
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'),
                coalesce(col('regCourseSectionLevelOverride'), col('courseSectionLevel')).alias('newCourseSectionLevel'),
                coalesce(col('regEnrollmentHoursOverride'), col('enrollmentHours')).alias('newEnrollmentHours'))
            .withColumn('ENTRowNum', row_number().over(Window
                .partitionBy(
                    col('repPerYearType'), 
                    col('repPerSurveySection'),
                    col('repPerTermCode'), 
                    col('repPerPartOfTermCode'), 
                    col('regPersonId'),
                    col('regCourseSectionNumber'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repPerSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repPerSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDateTimestamp').desc())))
            .filter(col('ENTRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('ENTRowNum')))
            
        registration_course_section_schedule = (registration_course_section
            .join(
                course_section_schedule_in,
                (col('repPerTermCode') == upper(col('termCode'))) &
                (col('repPerPartOfTermCode') == upper(coalesce(col('partOfTermCode'), lit('1')))) &
                (col('regCourseSectionNumber') == upper(col('courseSectionNumber'))) &
                (col('termCode').isNotNull()) &
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
                    | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate'))), 'left')
            .select(
                registration_course_section['*'],
                to_timestamp(coalesce(col('recordActivityDate'), col('repPerDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).alias('recordActivityDate'), 
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'),  
                col('instructionType').alias('crseSectSchedInstructionType'),
                col('locationType').alias('crseSectSchedLocationType'),
                coalesce(col('distanceEducationType'), lit('Not distance education')).alias('crseSectSchedDistanceEducationType'),
                col('onlineInstructionType').alias('crseSectSchedOnlineInstructionType'),
                col('maxSeats').alias('crseSectSchedMaxSeats'),
                upper(coalesce(col('regCourseSectionCampusOverride'), col('campus'))).alias('newCampus'))
            .withColumn('ENTRowNum', row_number().over(Window
                .partitionBy(
                    col('repPerYearType'),
                    col('repPerSurveySection'),
                    col('repPerTermCode'),
                    col('repPerPartOfTermCode'),
                    col('regPersonId'),
                    col('regCourseSectionNumber'),
                    col('newCourseSectionLevel'),
                    col('crseSectSubject'),
                    col('crseSectCourseNumber'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repPerSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repPerSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDateTimestamp').desc())))
            .filter(col('ENTRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('ENTRowNum')))
                    
        registration_course = (registration_course_section_schedule
            .join(
                course_in,
                (col('crseSectSubject') == upper(col('subject'))) &
                (col('crseSectCourseNumber') == upper(col('courseNumber'))) &
                    (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate')) & 
                        (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
                    | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate'))), 'left')
            .join(
                academic_term_in,
                (col('termCode') == upper(col('termCodeEffective'))) &
                (col('repPerTermCodeOrder') <= col('termCodeOrder')), 'left')
            .drop(academic_term_in.snapshotDate)
            .drop(academic_term_in.snapshotDateTimestamp)
            .select(
                registration_course_section_schedule['*'],
                to_timestamp(coalesce(col('recordActivityDate'), col('repPerDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')).alias('recordActivityDate'), 
                to_timestamp(col('snapshotDate')).alias('snapshotDateTimestamp'),
                to_date(col('snapshotDate'), 'YYYY-MM-DD').alias('snapshotDate'), 
                upper(col('termCodeEffective')).alias('crseTermCodeEffective'),
                col('courseStatus').alias('crseCourseStatus'),
                coalesce(col('crseSectCollege'), upper(col('courseCollege'))).alias('newCollege'),
                coalesce(col('crseSectDivision'), upper(col('courseDivision'))).alias('newDivision'),
                coalesce(col('crseSectDepartment'), upper(col('courseDepartment'))).alias('newDepartment'),
                col('termCodeOrder').alias('crseEffectiveTermCodeOrder'))
            .withColumn('ENTRowNum', row_number().over(Window
                .partitionBy(
                    col('repPerYearType'),
                    col('repPerSurveySection'),
                    col('repPerTermCode'),
                    col('repPerPartOfTermCode'),
                    col('regPersonId'),
                    col('regCourseSectionNumber'),
                    col('newCourseSectionLevel'),
                    col('crseSectSubject'),
                    col('crseSectCourseNumber'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repPerSnapshotDateTimestamp'), lit(1)).otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repPerSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDateTimestamp').desc())))
            .filter(col('ENTRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('ENTRowNum')))
        
        registration_course_campus = (registration_course
            .join(
                campus_mcr(spark, registration_course.first()['repPerSnapshotDateTimestamp']),
                (col('newCampus') == upper(col('campus'))), 'left')
            .select(
                registration_course['*'],
                coalesce(col('isInternational'), lit(False)).alias('campIsInternational')))
                    
        course_type_counts = (registration_course_campus
            .crossJoin(ipeds_client_config_in)
            .select(
                col('regPersonId'), 
                col('repPerSnapshotDateTimestamp'),
                col('repPerSnapshotDate'),
                col('repPerSurveySection'),
                col('repPerYearType'),
                col('repPerTermCode'),
                col('repPerPartOfTermCode'),
                col('repPerMaxCensus'),
                col('regCourseSectionNumber'),
                col('newCourseSectionLevel'),
                col('newEnrollmentHours'),
                col('crseSectIsESL'),
                col('crseSectIsRemedial'),
                col('crseSectIsClockHours'),
                col('regIsAudited'),
                col('repPerEquivCRHRFactor'),
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
                            (col('newEnrollmentHours') * col('repPerEquivCRHRFactor'))).otherwise(col('newEnrollmentHours')))))
            .distinct()
            .groupBy(
                'regPersonId',
                'repPerYearType',
                'repPerSurveySection',
                'repPerTermCode',
                'repPerMaxCensus')
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
                sum(when((col('crseSectSchedInstructionType').isin('Residency', 'Internship', 'Practicum')) & (col('repPerEquivCRHRFactor') == 'DPP'), 
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
        ipeds_client_config_in = ipeds_client_config_mcr(spark = spark, survey_info_in = survey_info_in)

    if not academic_term_in:
        academic_term_in = academic_term_mcr(spark)

    if not course_type_counts_in:
        if not reporting_periods_in:   
            if not ipeds_reporting_period_in:
                ipeds_reporting_period_in = ipeds_reporting_period_mcr(spark = spark, survey_info_in = survey_info_in, default_values_in = default_values_in,)
            reporting_periods_in = reporting_periods(spark = spark, survey_info_in = survey_info_in, default_values_in = default_values_in, ipeds_reporting_period_in = ipeds_reporting_period_in, academic_term_in = academic_term_in)
        course_type_counts_in = course_type_counts(spark = spark, survey_info_in = survey_info_in, default_values_in = default_values_in, ipeds_client_config_in = ipeds_client_config_in, academic_term_in = academic_term_in, reporting_periods_in = reporting_period_in)
        
    student_in = spark.sql("select * from student")     
        
    if (course_type_counts_in.rdd.isEmpty() == False) and (student_in.rdd.isEmpty() == False):
        study_abroad_filter_surveys = ['FE', 'OM', 'GR', '200GR']
        esl_enroll_surveys = ['12ME', 'ADM', 'SFA', 'FE']
        graduate_enroll_surveys = ['12ME', 'FE']
        academic_year_surveys = ['12ME', 'OM']
    
        student = (student_in
            .join(
                reporting_periods_in,
                (upper(student_in.termCode) == col('repPerTermCode')) & 
                (((coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) != col('repPerDummyDate'))
                & (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) <= col('repPerCensusDate')))
                | (coalesce(to_date(col('recordActivityDate'), 'YYYY-MM-DD'), col('repPerDummyDate')) == col('repPerDummyDate'))), 'inner')
            .select(
                student_in.personId.alias('stuPersonId'),
                col('repPerSnapshotDateTimestamp'),
                col('repPerSnapshotDate'),
                col('repPerYearType'),
                col('repPerSurveySection'),
                upper(student_in.termCode).alias('stuTermCode'),
                col('repPerTermCode'),
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
                to_timestamp(coalesce(student_in.recordActivityDate, col('repPerDummyDate'))).alias('recordActivityDateTimestamp'),
                coalesce(to_date(student_in.recordActivityDate, 'YYYY-MM-DD'), col('repPerDummyDate')).alias('recordActivityDate'), 
                to_timestamp(student_in.snapshotDate).alias('snapshotDateTimestamp'),
                to_date(student_in.snapshotDate, 'YYYY-MM-DD').alias('snapshotDate'),
                col('repPerFinancialAidYear'),
                col('repPerTermCodeOrder'),
                col('repPerMaxCensus'),
                col('repPerFullTermOrder'),
                col('repPerTermTypeNew'),
                col('repPerTermStartDate'),
                col('repPerTermEndDate'),
                col('repPerReportStartDate'),
                col('repPerReportEndDate'),
                col('repPerCensusDate'),
                col('repPerEquivCRHRFactor'),
                col('repPerRequiredFTCreditHoursGR'),
                col('repPerRequiredFTCreditHoursUG'),
                col('repPerRequiredFTClockHoursUG'),
                col('repPerDummyDate'),
                col('repPerSnapShotMaxDummyDate'),
                col('repPerSnapShotMinDummyDate'))
            .withColumn('ENTRowNum', row_number().over(Window
                .partitionBy(
                    col('repPerYearType'),
                    col('repPerSurveySection'),
                    col('stuPersonId'),
                    col('stuTermCode'))
                .orderBy(
                    when(col('snapshotDateTimestamp') == col('repPerSnapshotDateTimestamp'), lit(1))
                        .otherwise(lit(2)).asc(),
                    when(col('snapshotDateTimestamp') > col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repPerSnapShotMaxDummyDate')).asc(),
                    when(col('snapshotDateTimestamp') < col('repPerSnapshotDateTimestamp'), col('snapshotDateTimestamp'))
                        .otherwise(col('repPerSnapShotMinDummyDate')).desc(),
                    col('snapshotDateTimestamp').desc(),
                    col('recordActivityDateTimestamp').desc())))
            .filter(col('ENTRowNum') == 1)
            .drop(col('snapshotDate'))
            .drop(col('recordActivityDate'))
            .drop(col('snapshotDateTimestamp'))
            .drop(col('recordActivityDateTimestamp'))
            .drop(col('ENTRowNum')))

        student_reg = (student
            .join(
                course_type_counts_in,
                (col('stuPersonId') == col('regPersonId')) &
                (col('stuTermCode') == course_type_counts_in.repPerTermCode) &
                (student.repPerYearType == course_type_counts_in.repPerYearType) &
                (student.repPerSurveySection == course_type_counts_in.repPerSurveySection), 'left')
            .filter(col('regPersonId').isNotNull()) #check this for Fall Enrollment
            .select(
                col('stuPersonId'),
                col('repPerSnapshotDateTimestamp'),
                col('repPerSnapshotDate'),
                student.repPerYearType.alias('repPerYearType'),
                student.repPerSurveySection.alias('repPerSurveySection'),
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
                student.repPerTermCodeOrder.alias('repPerTermCodeOrder'),
                student.repPerTermTypeNew.alias('repPerTermTypeNew'),
                student.repPerMaxCensus.alias('repPerMaxCensus'),
                col('repPerFullTermOrder'),
                col('repPerDummyDate'),
                col('repPerSnapShotMaxDummyDate'),
                col('repPerSnapShotMinDummyDate'),
                col('repPerCensusDate'),
                col('repPerTermStartDate'),
                col('repPerTermEndDate'),
                col('repPerReportStartDate'),
                col('repPerReportEndDate'),
                col('repPerFinancialAidYear'),
                col('repPerRequiredFTCreditHoursGR'),
                col('repPerRequiredFTCreditHoursUG'),
                col('repPerRequiredFTClockHoursUG'),
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
                when(col('survey_type') == 'SFA', col('sfaLargestProgCIPC')).alias('configSfaLargestProgCIPC'),
                when(col('survey_type') == 'SFA', col('sfaReportPriorYear')).alias('configSfaReportPriorYear'),
                when(col('survey_type') == 'SFA', col('sfaReportSecondPriorYear')).alias('configSfaReportSecondPriorYear'),
                when(col('survey_type') == 'SFA', coalesce(col('eviReserved1'), lit('XXXX'))).alias('configSfaCaresAct1'),
                when(col('survey_type') == 'SFA', coalesce(col('eviReserved2'), lit('XXXX'))).alias('configSfaCaresAct2'),
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
                                        (case when totalCreditHrs >= repPerRequiredFTCreditHoursUG then 'Full Time' else 'Part Time' end)
                                    when configInstructionalActivityType = 'CL' then 
                                        (case when totalClockHrs >= repPerRequiredFTClockHoursUG then 'Full Time' else 'Part Time' end) 
                                else null end)
                            when studentLevelUGGRDPP != 'UG' and totalCreditHrs is not null then
                                (case when totalCreditHrs >= repPerRequiredFTCreditHoursUG then 'Full Time' else 'Part Time' end)
                        else null end)
                """))
            .withColumn('distanceEdInd_calc',
                when(col('totalDECourses') == col('totalCourses'), 'Exclusive DE')
                    .when(col('totalDECourses') > 0, 'Some DE')
                    .otherwise('None')))  

        cohort_person = person_cohort_mcr(spark, cohort_df_in = student_out)

        academic_track = academic_track_cohort_mcr(spark, cohort_df_in = cohort_person, academic_term_in = academic_term_in, priority_type_in = 'first')
            
        degree_program = degree_program_cohort_mcr(spark, cohort_df_in = academic_track, academic_term_in = academic_term_in, data_type_in = 'all')

        cohort_priority = (degree_program
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
            .filter(col('ipedsInclude') == 1)
            .select(
                col('repPerYearType').alias('yearType'),
                col('repPerSurveySection').alias('surveySection'),
                when(col('survey_type').isin(academic_year_surveys), lit(True)).otherwise(lit(False)).alias('annualSurvey'),
                col('survey_type').alias('surveyType'),
                col('survey_year').alias('surveyYear'),
                col('survey_id').alias('surveyId'),
                col('stuPersonId').alias('personId'),
                col('repPerSnapshotDateTimestamp'),
                col('stuTermCode').alias('termCode'),
                col('repPerFullTermOrder').alias('fullTermOrder'),
                col('repPerTermCodeOrder').alias('termCodeOrder'),
                col('repPerTermStartDate').alias('termStartDate'),
                col('repPerTermEndDate').alias('termEndDate'),
                col('repPerReportStartDate').alias('reportStartDate'),
                col('repPerReportEndDate').alias('reportEndDate'),
                col('repPerMaxCensus').alias('maxCensus'),
                col('repPerTermTypeNew').alias('termType'),
                col('repPerFinancialAidYear').alias('financialAidYear'),
                col('studentLevelUGGRDPP'),
                col('isNonDegreeSeeking_final').alias('isNonDegreeSeeking'),
                col('stuStudentType').alias('studentType'),
                col('timeStatus_calc').alias('timeStatus'),
                col('persIpedsEthnValue').alias('ethnicity'),
                col('persIpedsGender').alias('gender'),
                col('distanceEdInd_calc').alias('distanceEducationType'),
                col('stuStudyAbroadStatus').alias('studyAbroadStatus'),
                col('stuResidency').alias('residency'),
                col('degProgLengthInMonths'),
                col('fldOfStdyCipCode').alias('cipCode'),
                col('awardLevel'),
                col('degreeLevel'),
                when(col('degreeLevel').isin('Masters', 'Doctorate'), lit('GR'))
                    .when(col('degreeLevel') == 'Professional Practice Doctorate',lit('DPP'))
                    .otherwise(lit('UG')).alias('degreeLevelUGGRDPP'), #currently using studentLevelUGGRDPP for level filtering - consider using this field
                col('UGCreditHours'),
                col('UGClockHours'),
                col('GRCreditHours'),
                col('DPPCreditHours'),
                col('configIcOfferUndergradAwardLevel').alias('icOfferUndergradAwardLevel'),
                col('configIcOfferGraduateAwardLevel').alias('icOfferGraduateAwardLevel'),
                col('configIcOfferDoctorAwardLevel').alias('icOfferDoctorAwardLevel'),
                col('configInstructionalActivityType').alias('instructionalActivityType'),
                col('configTmAnnualDPPCreditHoursFTE').alias('tmAnnualDPPCreditHoursFTE'),
                col('configGrReportTransferOut').alias('grReportTransferOut'),
                col('configIncludeNonDegreeAsUG').alias('includeNonDegreeAsUG'),
                col('configSfaReportPriorYear').alias('sfaReportPriorYear'),
                col('configSfaReportSecondPriorYear').alias('sfaReportSecondPriorYear'),
                col('configSfaCaresAct1').alias('sfaCaresAct1'),
                col('configSfaCaresAct2').alias('sfaCaresAct2'))
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
                cohort_priority['*'],
                col('surveyYear').cast('int').alias('surveyYear_int'),
                col('maxStudentLevel'),
                col('isNonDegreeSeekingFirstDegreeSeeking'),
                col('studentTypePreFallSummer'),
                col('studentTypeFirstDegreeSeeking')))

        return cohort
    
    else:
        if course_type_counts_in.rdd.isEmpty() == False:
            return student_in
        else: return course_type_counts_in
