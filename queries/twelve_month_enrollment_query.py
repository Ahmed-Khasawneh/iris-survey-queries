import logging
import sys
import boto3
import json
from uuid import uuid4
from common import query_helpers
from common import survey_format
from common import default_values
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

# spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).config(
#     "spark.dynamicAllocation.enabled", 'true').getOrCreate()
sparkContext = SparkContext.getOrCreate()
sqlContext = SQLContext(sparkContext)
glueContext = GlueContext(sparkContext)

def run_twelve_month_enrollment_query(spark, survey_type, year):
    
# ********** Survey Default Values

### TEST - uncomment survey_type and year for testing; these two values are set in run_query.py at runtime
    #survey_type = 'TWELVE_MONTH_ENROLLMENT_1'
    #year = '2014'        #'2019' = 1920, '2020' = 2021, '2021' = 2122, '2022 = 2223
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

    survey_info = {'survey_type' : '12ME',
        'survey_long_type' : survey_type,
        'survey_id' : survey_id,
        'survey_ver_id' : survey_id,
        'survey_year_iris' : year,
        'survey_year_doris' : year1 + year2}
        
    survey_tags = default_values.get_survey_tags(survey_info)

    survey_dates = default_values.get_survey_dates(survey_info)

    # ********** Survey Reporting Period    
    ipeds_client_config = query_helpers.ipeds_client_config_mcr(survey_info_in = survey_info)
    
    if ipeds_client_config.rdd.isEmpty() == False:
        ipeds_reporting_period = query_helpers.ipeds_reporting_period_mcr(survey_info_in = survey_info, ipeds_client_config_in = ipeds_client_config,  survey_tags_in = survey_tags)
        all_academic_terms = query_helpers.academic_term_mcr()    
        reporting_period_terms = query_helpers.reporting_periods(survey_info_in = survey_info, ipeds_reporting_period_in = ipeds_reporting_period, academic_term_in = all_academic_terms, survey_tags_in = survey_tags, survey_dates_in = survey_dates)
            
        # ********** Course Type Counts
        course_counts = query_helpers.course_type_counts(survey_info_in = survey_info, ipeds_client_config_in = ipeds_client_config, academic_term_in = all_academic_terms, reporting_periods_in = reporting_period_terms, survey_tags_in = survey_tags, survey_dates_in = survey_dates)

        # ********** Cohort
        cohort_all = query_helpers.student_cohort(survey_info_in = survey_info, ipeds_client_config_in = ipeds_client_config, academic_term_in = all_academic_terms, reporting_periods_in = reporting_period_terms, course_type_counts_in = course_counts, survey_tags_in = survey_tags, survey_dates_in = survey_dates).cache()

        # ********** Survey Data Transformations  
        if cohort_all.rdd.isEmpty() == False:
            cohort_course_counts_out = cohort_all.select(
                cohort_all['*']).groupBy(
                col('yearType'),
                col('surveyId'),
                col('icOfferUndergradAwardLevel'),
                col('icOfferGraduateAwardLevel'),
                col('icOfferDoctorAwardLevel'),
                col('instructionalActivityType'),
                col('tmAnnualDPPCreditHoursFTE')).agg(
                sum(col('UGCreditHours')).alias('UGCreditHours'),
                sum(col('UGClockHours')).alias('UGClockHours'),
                sum(col('GRCreditHours')).alias('GRCreditHours'),
                sum(col('DPPCreditHours')).alias('DPPCreditHours'))
                
            cohort_first_full_term = cohort_all.filter(col('FFTRn') == 1).select(
                col('surveyId'),
                col('personId'),
                col('studentLevelUGGRDPP'),
                coalesce(when(col('surveyYear') < 2122, col('isNonDegreeSeekingFirstDegreeSeeking')), col('isNonDegreeSeeking')).alias('isNonDegreeSeeking'),
                col('timeStatus'),
                coalesce(when(col('surveyYear') < 2122, col('studentTypeFirstDegreeSeeking')), when((col('termType') == 'Fall') & (col('studentType') == 'Continuing'), col('studentTypePreFallSummer')), col('studentType')).alias('studentType'),
                col('ethnicity'),
                col('gender'),
                col('distanceEducationType')
                )
                
            cohort_out = cohort_first_full_term.select(
                cohort_first_full_term['*'],
                (when((col('studentLevelUGGRDPP') != 'UG') & (col('surveyId') == 'E1D') & (col('icOfferGraduateAwardLevel') == 'Y'), lit('99'))
                    .when((col('studentType') == 'First Time') & (col('timeStatus') == 'Full Time'), lit('1'))
                    .when((col('studentType') == 'First Time') & (col('timeStatus') == 'Part Time'), lit('15'))
                    .when((col('studentType') == 'Continuing') & (col('timeStatus') == 'Full Time') & (col('surveyId') != 'E1F'), lit('3'))
                    .when((col('studentType') == 'Continuing') & (col('timeStatus') == 'Part Time') & (col('surveyId') != 'E1F'), lit('17'))
                    .when((col('timeStatus') == 'Full Time') & (col('surveyId') == 'E1F'), lit('3'))
                    .when((col('timeStatus') == 'Part Time') & (col('surveyId') == 'E1F'), lit('17'))
                    .when((col('isNonDegreeSeeking') == True) & (col('timeStatus') == 'Full Time') & (col('surveyId') != 'E1F'), lit('7'))
                    .when((col('isNonDegreeSeeking') == True) & (col('timeStatus') == 'Part Time') & (col('surveyId') != 'E1F'), lit('21'))
                    .when((col('studentType') == 'Transfer') & (col('timeStatus') == 'Full Time') & (col('surveyId').isin('E1D', 'E12')), lit('2'))
                    .when((col('studentType') == 'Transfer') & (col('timeStatus') == 'Part Time') & (col('surveyId').isin('E1D', 'E12')), lit('16'))
                    .otherwise(lit('1'))).alias('ipedsPartAStudentLevel'), 
                (when((col('studentLevelUGGRDPP') != 'UG') & (col('surveyId') == 'E1D') & (col('icOfferGraduateAwardLevel') == 'Y'), lit('3'))
                    .when((col('isNonDegreeSeeking') == True) & (col('surveyId') != 'E1F'), lit('2'))
                    .otherwise(lit('1'))).alias('ipedsPartCStudentLevel'))
                    
             # ********** Survey Formatting
        
            # Part A
            a_data = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config)
            a_columns = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config)
            a_level_values = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'levels', ipeds_client_config_in = ipeds_client_config)
            
            FormatPartA = sparkContext.parallelize(a_data)
            FormatPartA = spark.createDataFrame(FormatPartA).toDF(*a_columns)
        
            partA_out = cohort_out.select(col('personId'), col('ipedsPartAStudentLevel'), col('ethnicity'), col('gender')).filter(
                (col('ipedsPartAStudentLevel')).isin(a_level_values)).union(FormatPartA)

            partA_out = partA_out.select(
                col('ipedsPartAStudentLevel').alias('field1'),
                when(((col('ethnicity') == '1') & (col('gender') == 'M')), lit(1)).otherwise(
                    lit(0)).alias('field2'),  # FYRACE01 - Nonresident alien - Men (1), 0 to 999999
                when(((col('ethnicity') == '1') & (col('gender') == 'F')), lit(1)).otherwise(
                    lit(0)).alias('field3'),  # FYRACE02 - Nonresident alien - Women (2), 0 to 999999
                when(((col('ethnicity') == '2') & (col('gender') == 'M')), lit(1)).otherwise(
                    lit(0)).alias('field4'),  # FYRACE25 - Hispanic/Latino - Men (25), 0 to 999999
                when(((col('ethnicity') == '2') & (col('gender') == 'F')), lit(1)).otherwise(
                    lit(0)).alias('field5'),  # FYRACE26 - Hispanic/Latino - Women (26), 0 to 999999
                when(((col('ethnicity') == '3') & (col('gender') == 'M')), lit(1)).otherwise(
                    lit(0)).alias('field6'),  # FYRACE27 - American Indian or Alaska Native - Men (27), 0 to 999999
                when(((col('ethnicity') == '3') & (col('gender') == 'F')), lit(1)).otherwise(
                    lit(0)).alias('field7'),  # FYRACE28 - American Indian or Alaska Native - Women (28), 0 to 999999
                when(((col('ethnicity') == '4') & (col('gender') == 'M')), lit(1)).otherwise(
                    lit(0)).alias('field8'),  # FYRACE29 - Asian - Men (29), 0 to 999999
                when(((col('ethnicity') == '4') & (col('gender') == 'F')), lit(1)).otherwise(
                    lit(0)).alias('field9'),  # FYRACE30 - Asian - Women (30), 0 to 999999
                when(((col('ethnicity') == '5') & (col('gender') == 'M')), lit(1)).otherwise(
                    lit(0)).alias('field10'),  # FYRACE31 - Black or African American - Men (31), 0 to 999999
                when(((col('ethnicity') == '5') & (col('gender') == 'F')), lit(1)).otherwise(
                    lit(0)).alias('field11'),  # FYRACE32 - Black or African American - Women (32), 0 to 999999
                when(((col('ethnicity') == '6') & (col('gender') == 'M')), lit(1)).otherwise(
                    lit(0)).alias('field12'),# FYRACE33 - Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
                when(((col('ethnicity') == '6') & (col('gender') == 'F')), lit(1)).otherwise(
                    lit(0)).alias('field13'),# FYRACE34 - Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
                when(((col('ethnicity') == '7') & (col('gender') == 'M')), lit(1)).otherwise(
                    lit(0)).alias('field14'),  # FYRACE35 - White - Men (35), 0 to 999999
                when(((col('ethnicity') == '7') & (col('gender') == 'F')), lit(1)).otherwise(
                    lit(0)).alias('field15'),  # FYRACE36 - White - Women (36), 0 to 999999
                when(((col('ethnicity') == '8') & (col('gender') == 'M')), lit(1)).otherwise(
                    lit(0)).alias('field16'),  # FYRACE37 - Two or more races - Men (37), 0 to 999999
                when(((col('ethnicity') == '8') & (col('gender') == 'F')), lit(1)).otherwise(
                    lit(0)).alias('field17'),  # FYRACE38 - Two or more races - Women (38), 0 to 999999
                when(((col('ethnicity') == '9') & (col('gender') == 'M')), lit(1)).otherwise(
                    lit(0)).alias('field18'),  # FYRACE13 - Race and ethnicity unknown - Men (13), 0 to 999999
                when(((col('ethnicity') == '9') & (col('gender') == 'F')), lit(1)).otherwise(
                    lit(0)).alias('field19'))  # FYRACE14 - Race and ethnicity unknown - Women (14), 0 to 999999

            partA_out = partA_out.withColumn('part', lit('A')).groupBy(col('part'), col('field1')).agg(
                sum(col('field2')).cast('int').alias('field2'),
                sum(col('field3')).cast('int').alias('field3'),
                sum(col('field4')).cast('int').alias('field4'),
                sum(col('field5')).cast('int').alias('field5'),
                sum(col('field6')).cast('int').alias('field6'),
                sum(col('field7')).cast('int').alias('field7'),
                sum(col('field8')).cast('int').alias('field8'),
                sum(col('field9')).cast('int').alias('field9'),
                sum(col('field10')).cast('int').alias('field10'),
                sum(col('field11')).cast('int').alias('field11'),
                sum(col('field12')).cast('int').alias('field12'),
                sum(col('field13')).cast('int').alias('field13'),
                sum(col('field14')).cast('int').alias('field14'),
                sum(col('field15')).cast('int').alias('field15'),
                sum(col('field16')).cast('int').alias('field16'),
                sum(col('field17')).cast('int').alias('field17'),
                sum(col('field18')).cast('int').alias('field18'),
                sum(col('field19')).cast('int').alias('field19'))
                
            # Part C
            c_data = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config)
            c_columns = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config)
            c_level_values = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'levels', ipeds_client_config_in = ipeds_client_config)
        
            FormatPartC = sparkContext.parallelize(c_data)
            FormatPartC = spark.createDataFrame(FormatPartC).toDF(*c_columns)
        
            partC_out = cohort_out.select('personId', 'ipedsPartCStudentLevel', 'distanceEducationType').filter(
                (cohort_out.ipedsPartCStudentLevel).isin(c_level_values) & (col('distanceEducationType') != 'None')).union(FormatPartC)
                
            partC_out = partC_out.select(
                partC_out.ipedsPartCStudentLevel.alias('field1'),
                when((col('distanceEducationType') == 'Exclusive DE'), lit(1)).otherwise(lit(0)).alias('field2'),
                when((col('distanceEducationType') == 'Some DE'), lit(1)).otherwise(lit(0)).alias('field3'))

            partC_out = partC_out.withColumn('part', lit('C')).groupBy(col('part'), col('field1')).agg(
                sum(col('field2')).cast('int').alias('field2'),
                sum(col('field3')).cast('int').alias('field3'))
                
            # Part B       
            partB_out = cohort_course_counts_out.withColumn('part', lit('B')).select(
                col('part'),
                when((col('icOfferUndergradAwardLevel') == 'Y') & (col('instructionalActivityType') != 'CL'),
                        coalesce(col('UGCreditHours'), lit(0))).cast('int').alias('field2'),
                when((col('icOfferUndergradAwardLevel') == 'Y') & (col('instructionalActivityType') != 'CR'),
                        coalesce(col('UGClockHours'), lit(0))).cast('int').alias('field3'),
                when((col('icOfferGraduateAwardLevel') == 'Y') & (col('surveyId') == 'E1D'),
                        coalesce(col('GRCreditHours'), lit(0))).cast('int').alias('field4'),
                when((col('icOfferDoctorAwardLevel') == 'Y') & (col('surveyId')== 'E1D'), 
                        when(coalesce(col('DPPCreditHours'), lit(0)) > 0, round(col('DPPCreditHours')/col('tmAnnualDPPCreditHoursFTE'), 0)).otherwise(lit(0))).cast('int').alias('field5'))
                
        else: 
            # Part A    
            a_data = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config)
            a_columns = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config)
            
            partA_out = sparkContext.parallelize(a_data)
            partA_out = spark.createDataFrame(partA_out).toDF(*a_columns)
            
            # Part C
            c_data = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config)
            c_columns = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config)
        
            partC_out = sparkContext.parallelize(c_data)
            partC_out = spark.createDataFrame(partC_out).toDF(*c_columns)
            
            # Part B
            b_data = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'B', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config)
            b_columns = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'B', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config)

            partB_out = sparkContext.parallelize(b_data)
            partB_out = spark.createDataFrame(partB_out).toDF(*b_columns)

    else:
        # Part A    
        a_data = survey_format.get_default_part_format_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'data')
        a_columns = survey_format.get_default_part_format_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'columns')
        
        partA_out = sparkContext.parallelize(a_data)
        partA_out = spark.createDataFrame(partA_out).toDF(*a_columns)
        
        # Part C
        c_data = survey_format.get_default_part_format_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'data')
        c_columns = survey_format.get_default_part_format_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'columns')

        partC_out = sparkContext.parallelize(c_data)
        partC_out = spark.createDataFrame(c_data).toDF(*c_columns)
        
        # Part B
        b_data = survey_format.get_default_part_format_string(survey_info_in = survey_info, part_in = 'B', part_type_in = 'data')
        b_columns = survey_format.get_default_part_format_string(survey_info_in = survey_info, part_in = 'B', part_type_in = 'columns')

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

    return surveyOutput 
    
#test = run_twelve_month_enrollment_query()
#test.explain()
#test.show()
#print(test)
