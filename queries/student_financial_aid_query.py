import logging
import sys
import boto3
import json
from uuid import uuid4
from common import query_helpers
from common import survey_format
from common import default_values
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum, expr, col, lit, upper, to_timestamp, max, min, row_number, date_trunc, \
    to_date, when, coalesce, count, rank, round, concat, substring, instr
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from awsglue.utils import getResolvedOptions

sparkContext = SparkContext.getOrCreate()
sqlContext = SQLContext(sparkContext)
glueContext = GlueContext(sparkContext)

####****TEST uncomment survey_type and year for testing; these two values are set in run_query.py at runtime
#survey_type = 'STUDENT_FINANCIAL_AID_1'
#year = calendarYear = '2014'

#***************************************************************
#*
#***  run_student_financial_aid_query
#*
#*  <survey notes here>
#***************************************************************

def run_student_financial_aid_query(spark, survey_type, year):
    
    # ********** Survey Default Values
    year1 = str(year[2:4])
    year2 = str(int(year1) + 1)
    survey_year = year1 + year2

    if survey_type == 'STUDENT_FINANCIAL_AID_1':
        survey_ver_id = 'SFA1'
    elif survey_type == 'STUDENT_FINANCIAL_AID_2':
        survey_ver_id = 'SFA2'
    elif survey_type == 'STUDENT_FINANCIAL_AID_3':
        survey_ver_id = 'SFA3'
    elif survey_type == 'STUDENT_FINANCIAL_AID_4':
        survey_ver_id = 'SFA4'
    else:  # V5
        survey_ver_id = 'SFA5'

    survey_info = {'survey_type' : 'SFA',
        'survey_long_type' : survey_type,
        'survey_id' : 'SFA',
        'survey_ver_id' : survey_ver_id,
        'survey_year_iris' : year,
        'survey_year_doris' : year1 + year2}
        
    default_survey_values = default_values.get_survey_default_values(survey_info)

    survey_year_int = int(survey_info['survey_year_doris'])
    
    # ********** Survey Client Configuration
    ipeds_client_config = query_helpers.ipeds_client_config_mcr(spark, survey_info_in = survey_info)
    
    if survey_info['survey_ver_id'] != 'SFA5':

    # ********** Survey Reporting Period
        if ipeds_client_config.rdd.isEmpty() == False:
            ipeds_reporting_period = query_helpers.ipeds_reporting_period_mcr(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_client_config_in = ipeds_client_config)  

            if ipeds_reporting_period.rdd.isEmpty() == False:
                all_academic_terms = query_helpers.academic_term_mcr(spark)    
                
                if all_academic_terms.rdd.isEmpty() == False:
                    reporting_period_terms = query_helpers.reporting_periods(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_reporting_period_in = ipeds_reporting_period, academic_term_in = all_academic_terms)
                    
                    # ********** Course Type Counts
                    if reporting_period_terms.rdd.isEmpty() == False:
                        course_counts = query_helpers.course_type_counts(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_client_config_in = ipeds_client_config, academic_term_in = all_academic_terms, reporting_periods_in = reporting_period_terms) #.filter(col('regPersonId') == '36401')

                        # ********** Cohort
                        if course_counts.rdd.isEmpty() == False:
                            cohort_all = query_helpers.student_cohort(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_client_config_in = ipeds_client_config, academic_term_in = all_academic_terms, reporting_periods_in = reporting_period_terms, course_type_counts_in = course_counts).cache()

                            if cohort_all.rdd.isEmpty() == False:
                                cohort_fall_term = (cohort_all
                                ####****TEST uncomment next line for test data '1415'
                                    #.filter(col('surveySection').isin('COHORT','FALL','PRIOR YEAR 1 COHORT','PRIOR YEAR 2 COHORT','PRIOR YEAR 1 FALL','PRIOR YEAR 2 FALL'))
                                    .filter(col('surveySection').isin('COHORT','PRIOR YEAR 1 COHORT','PRIOR YEAR 2 COHORT'))
                                    .filter(col('studentLevelUGGRDPP') == 'UG') #change this to degreeLevelUGGRDPP?
                                    .select(
                                        col('yearType').alias('cohortYearType'),
                                        col('surveyId'),
                                        col('surveyType'),
                                        col('surveyYear_int'),
                                        col('financialAidYear').alias('cohortFinancialAidYear'),
                                        col('termCode').alias('cohortTermCode'),
                                        col('fullTermOrder'),
                                        col('termCodeOrder'),
                                        col('maxCensus'),
                                        col('personId').alias('cohortPersonId'),
                                        when(col('studentType') == 'Continuing', col('studentTypePreFallSummer')).otherwise(col('studentType')).alias('studentType'),
                                        col('isNonDegreeSeeking'),
                                        col('timeStatus'),
                                        col('residency'),
                                        col('sfaReportPriorYear'),
                                        col('sfaReportSecondPriorYear'),
                                        col('sfaCaresAct1'),
                                        col('sfaCaresAct2'))
                                    .withColumn('isGroup2Ind', when((col('timeStatus') == 'Full Time') & (col('isNonDegreeSeeking') == False) & (col('studentType') == 'First Time'), lit(1)).otherwise(lit(0))))

                                # ********** Cohort Financial Aid                              
                                cohort_financial_aid = (query_helpers.financial_aid_cohort_mcr(spark, default_values_in = default_survey_values, cohort_df_in = cohort_fall_term, ipeds_client_config_in = ipeds_client_config))

                                # ********** Survey Formatting
                                # Parts A - F

                                ####****NOTES 
                                # for survey years > 2019, use 'group3Total_caresAct' and 'group4Total_caresAct'

    # ********** Military Benefits                                                     
    gi_bill_counts = military_benefit_mcr(spark, default_values_in = default_survey_values, benefit_type_in = 'GI Bill') 

    dod_counts = military_benefit_mcr(spark, default_values_in = default_survey_values, benefit_type_in = 'Department of Defense') 

    # ********** Survey Formatting
    # Part G

    return cohort_financial_aid
    
#test = run_student_financial_aid_query()

#test.show()
