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
    #survey_type = 'TWELVE_MONTH_ENROLLMENT_1'
    #year = '2014'        #'2019' = 1920, '2020' = 2021, '2021' = 2122, '2022 = 2223
    
#***************************************************************
#*
#***  run_twelve_month_enrollment_query 
#*
#*  IPEDS 2019-20 - Two versions, E1D & E12
#*                  Two parts, A & B
#*
#*  IPEDS 2020-21 - Four versions, E1D, E12, E1E, E1F
#*                  Three parts, A, B & C
#*                  First-full term & non-degree-seeking logic
#*
#*  IPEDS 2021-22 - Four versions, E1D, E12, E1E, E1F
#*                  Three parts, A, B & C
#*                  First-full term logic
#*
#***************************************************************

def run_twelve_month_enrollment_query(spark, survey_type, year):
    
# ********** Survey Default Values
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
        
    default_survey_values = default_values.get_survey_default_values(survey_info)

    survey_year_int = int(survey_info['survey_year_doris'])

    # ********** Survey Client Configuration
    ipeds_client_config = query_helpers.ipeds_client_config_mcr(spark, survey_info_in = survey_info)
        
    # ********** Survey Reporting Period
    if ipeds_client_config.rdd.isEmpty() == False:
        ipeds_reporting_period = query_helpers.ipeds_reporting_period_mcr(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_client_config_in = ipeds_client_config)  

        if ipeds_reporting_period.rdd.isEmpty() == False:
            all_academic_terms = query_helpers.academic_term_mcr(spark)    
            
            if all_academic_terms.rdd.isEmpty() == False:  
                reporting_period_terms = query_helpers.reporting_periods(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_reporting_period_in = ipeds_reporting_period, academic_term_in = all_academic_terms)
                
                # ********** Course Type Counts
                if reporting_period_terms.rdd.isEmpty() == False: 
                    course_counts = query_helpers.course_type_counts(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_client_config_in = ipeds_client_config, academic_term_in = all_academic_terms, reporting_periods_in = reporting_period_terms)

                    # ********** Cohort
                    if course_counts.rdd.isEmpty() == False: 
                        cohort_all = query_helpers.student_cohort(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_client_config_in = ipeds_client_config, academic_term_in = all_academic_terms, reporting_periods_in = reporting_period_terms, course_type_counts_in = course_counts).cache()

                        # ********** Survey Data Transformations  
                        if cohort_all.rdd.isEmpty() == False:                           
                            cohort_first_full_term = (cohort_all
                                .filter(col('FFTRn') == 1)
                                .select(
                                    col('surveyId'),
                                    col('surveyYear_int'),
                                    col('personId'),
                                    coalesce(when(col('surveyYear_int') == 1920, col('maxStudentLevel')), col('studentLevelUGGRDPP')).alias('studentLevelUGGRDPP'),
                                    coalesce(when(col('surveyYear_int') <= 2021, col('isNonDegreeSeekingFirstDegreeSeeking')), col('isNonDegreeSeeking')).alias('isNonDegreeSeeking'),
                                    coalesce(when(col('surveyYear_int') == 2021, col('studentTypeFirstDegreeSeeking')), 
                                        when((col('termType') == 'Fall') & (col('studentType') == 'Continuing'), col('studentTypePreFallSummer')), col('studentType')).alias('studentType'),
                                    when(col('surveyYear_int') > 1920, lit('Y')).otherwise(col('includeNonDegreeAsUG')).alias('includeNonDegreeAsUG'),
                                    col('icOfferUndergradAwardLevel'),
                                    col('icOfferGraduateAwardLevel'),
                                    col('icOfferDoctorAwardLevel'),
                                    col('timeStatus'),
                                    col('ethnicity'),
                                    col('gender'),
                                    col('distanceEducationType')))
                            
                            if survey_year_int > 1920:    
                                cohort_out = (cohort_first_full_term
                                    .select(
                                        cohort_first_full_term['*'],
                                        when((col('studentLevelUGGRDPP') != 'UG') & (col('surveyId') == 'E1D') & ((col('icOfferGraduateAwardLevel') == 'Y') | 
                                                    (col('icOfferDoctorAwardLevel') == 'Y')), lit('99'))
                                            .when(col('icOfferUndergradAwardLevel') == 'Y', 
                                                when((col('studentType') == 'First Time') & (col('timeStatus') == 'Full Time'), lit('1'))
                                                .when((col('studentType') == 'First Time') & (col('timeStatus') == 'Part Time'), lit('15'))
                                                .when((col('studentType') == 'Continuing') & (col('timeStatus') == 'Full Time') & (col('surveyId') != 'E1F'), lit('3'))
                                                .when((col('studentType') == 'Continuing') & (col('timeStatus') == 'Part Time') & (col('surveyId') != 'E1F'), lit('17'))
                                                .when((col('timeStatus') == 'Full Time') & (col('surveyId') == 'E1F'), lit('3'))
                                                .when((col('timeStatus') == 'Part Time') & (col('surveyId') == 'E1F'), lit('17'))
                                                .when((col('isNonDegreeSeeking') == True) & (col('timeStatus') == 'Full Time') & (col('surveyId') != 'E1F'), lit('7'))
                                                .when((col('isNonDegreeSeeking') == True) & (col('timeStatus') == 'Part Time') & (col('surveyId') != 'E1F'), lit('21'))
                                                .when((col('studentType') == 'Transfer') & (col('timeStatus') == 'Full Time') & (col('surveyId').isin('E1D', 'E12')), lit('2'))
                                                .when((col('studentType') == 'Transfer') & (col('timeStatus') == 'Part Time') & (col('surveyId').isin('E1D', 'E12')), lit('16')))
                                            .otherwise(lit('1')).alias('ipedsPartAStudentLevel'), 
                                        when((col('studentLevelUGGRDPP') != 'UG') & (col('surveyId') == 'E1D') & ((col('icOfferGraduateAwardLevel') == 'Y') | (col('icOfferDoctorAwardLevel') == 'Y')), lit('3'))
                                            .when(col('icOfferUndergradAwardLevel') == 'Y', (when((col('isNonDegreeSeeking') == True) & (col('surveyId') != 'E1F'), lit('2'))))
                                            .otherwise(lit('1')).alias('ipedsPartCStudentLevel')))  
                                
                            else: # survey_year_int <= 1920:     
                                cohort_out = (cohort_first_full_term
                                    .filter((col('includeNonDegreeAsUG') == 'Y') | ((col('includeNonDegreeAsUG') == 'N') & (col('isNonDegreeSeeking') == False)))
                                    .select(
                                        cohort_first_full_term['*'],
                                        (when((col('studentLevelUGGRDPP') != 'UG') & (col('surveyId') == 'E1D') & ((col('icOfferGraduateAwardLevel') == 'Y') | (col('icOfferDoctorAwardLevel') == 'Y')), lit('3'))
                                        .otherwise(lit('1'))).alias('ipedsPartAStudentLevel'))) 
                            
                            cohort_course_counts_out = (cohort_all
                                .filter((col('includeNonDegreeAsUG') == 'Y') | ((col('includeNonDegreeAsUG') == 'N') & (col('isNonDegreeSeeking') == False)))
                                .select(
                                    cohort_all['*'])
                                .groupBy(
                                    col('yearType'),
                                    col('surveyId'),
                                    col('icOfferUndergradAwardLevel'),
                                    col('icOfferGraduateAwardLevel'),
                                    col('icOfferDoctorAwardLevel'),
                                    col('instructionalActivityType'),
                                    col('tmAnnualDPPCreditHoursFTE'))
                                .agg(
                                    sum(col('UGCreditHours')).alias('UGCreditHours'),
                                    sum(col('UGClockHours')).alias('UGClockHours'),
                                    sum(col('GRCreditHours')).alias('GRCreditHours'),
                                    sum(col('DPPCreditHours')).alias('DPPCreditHours')))
                                
                            # ********** Survey Formatting
                        
                            # Part A
                            a_data = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config)
                            a_columns = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config)
                            a_level_values = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'levels', ipeds_client_config_in = ipeds_client_config)
                            
                            FormatPartA = sparkContext.parallelize(a_data)
                            FormatPartA = spark.createDataFrame(FormatPartA).toDF(*a_columns)
                        
                            partA_out = (cohort_out
                                .select(col('personId'), col('ipedsPartAStudentLevel'), col('ethnicity'), col('gender'))
                                .filter((col('ipedsPartAStudentLevel')).isin(a_level_values))
                                .union(FormatPartA))

                            partA_out = (partA_out
                                .select(
                                    col('ipedsPartAStudentLevel').alias('field1'),
                                    when(((col('ethnicity') == '1') & (col('gender') == 'M')), lit(1)).otherwise(lit(0)).alias('field2'),  # Nonresident alien 
                                    when(((col('ethnicity') == '1') & (col('gender') == 'F')), lit(1)).otherwise(lit(0)).alias('field3'),  # Nonresident alien 
                                    when(((col('ethnicity') == '2') & (col('gender') == 'M')), lit(1)).otherwise(lit(0)).alias('field4'),  # Hispanic/Latino 
                                    when(((col('ethnicity') == '2') & (col('gender') == 'F')), lit(1)).otherwise(lit(0)).alias('field5'),  # Hispanic/Latino 
                                    when(((col('ethnicity') == '3') & (col('gender') == 'M')), lit(1)).otherwise(lit(0)).alias('field6'),  # American Indian or Alaska Native 
                                    when(((col('ethnicity') == '3') & (col('gender') == 'F')), lit(1)).otherwise(lit(0)).alias('field7'),  # American Indian or Alaska Native 
                                    when(((col('ethnicity') == '4') & (col('gender') == 'M')), lit(1)).otherwise(lit(0)).alias('field8'),  # Asian 
                                    when(((col('ethnicity') == '4') & (col('gender') == 'F')), lit(1)).otherwise(lit(0)).alias('field9'),  # Asian 
                                    when(((col('ethnicity') == '5') & (col('gender') == 'M')), lit(1)).otherwise(lit(0)).alias('field10'),  # Black or African American 
                                    when(((col('ethnicity') == '5') & (col('gender') == 'F')), lit(1)).otherwise(lit(0)).alias('field11'),  # Black or African American 
                                    when(((col('ethnicity') == '6') & (col('gender') == 'M')), lit(1)).otherwise(lit(0)).alias('field12'),  # Native Hawaiian or Other Pacific Islander 
                                    when(((col('ethnicity') == '6') & (col('gender') == 'F')), lit(1)).otherwise(lit(0)).alias('field13'),  # Native Hawaiian or Other Pacific Islander 
                                    when(((col('ethnicity') == '7') & (col('gender') == 'M')), lit(1)).otherwise(lit(0)).alias('field14'),  # White 
                                    when(((col('ethnicity') == '7') & (col('gender') == 'F')), lit(1)).otherwise(lit(0)).alias('field15'),  # White 
                                    when(((col('ethnicity') == '8') & (col('gender') == 'M')), lit(1)).otherwise(lit(0)).alias('field16'),  # Two or more races 
                                    when(((col('ethnicity') == '8') & (col('gender') == 'F')), lit(1)).otherwise(lit(0)).alias('field17'),  # Two or more races 
                                    when(((col('ethnicity') == '9') & (col('gender') == 'M')), lit(1)).otherwise(lit(0)).alias('field18'),  # Race and ethnicity unknown
                                    when(((col('ethnicity') == '9') & (col('gender') == 'F')), lit(1)).otherwise(lit(0)).alias('field19')))  # Race and ethnicity unknown 

                            partA_out = (partA_out
                                .withColumn('part', lit('A'))
                                .groupBy(col('part'), col('field1'))
                                .agg(
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
                                    sum(col('field19')).cast('int').alias('field19')))
                                
                            # Part C
                            if survey_year_int > 1920:
                                c_data = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config)
                                c_columns = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config)
                                c_level_values = survey_format.get_part_data_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'levels', ipeds_client_config_in = ipeds_client_config)
                            
                                FormatPartC = sparkContext.parallelize(c_data)
                                FormatPartC = spark.createDataFrame(FormatPartC).toDF(*c_columns)
                            
                                partC_out = (cohort_out
                                    .select('personId', 'ipedsPartCStudentLevel', 'distanceEducationType')
                                    .filter((cohort_out.ipedsPartCStudentLevel).isin(c_level_values) & (col('distanceEducationType') != 'None'))
                                    .union(FormatPartC))
                                    
                                partC_out = (partC_out
                                    .select(
                                        partC_out.ipedsPartCStudentLevel.alias('field1'),
                                        when((col('distanceEducationType') == 'Exclusive DE'), lit(1)).otherwise(lit(0)).alias('field2'),
                                        when((col('distanceEducationType') == 'Some DE'), lit(1)).otherwise(lit(0)).alias('field3')))

                                partC_out = (partC_out
                                    .withColumn('part', lit('C'))
                                    .groupBy(col('part'), col('field1'))
                                    .agg(
                                        sum(col('field2')).cast('int').alias('field2'),
                                        sum(col('field3')).cast('int').alias('field3')))
                                
                            # Part B
                        
                            partB_out = (cohort_course_counts_out
                                .withColumn('part', lit('B'))
                                .select(
                                    col('part'),
                                    when((col('icOfferUndergradAwardLevel') == 'Y') & (col('instructionalActivityType') != 'CL'),
                                            coalesce(col('UGCreditHours'), lit(0))).cast('int').alias('field2'),
                                    when((col('icOfferUndergradAwardLevel') == 'Y') & (col('instructionalActivityType') != 'CR'),
                                            coalesce(col('UGClockHours'), lit(0))).cast('int').alias('field3'),
                                    when((col('icOfferGraduateAwardLevel') == 'Y') & (col('surveyId') == 'E1D'),
                                            coalesce(col('GRCreditHours'), lit(0))).cast('int').alias('field4'),
                                    when((col('icOfferDoctorAwardLevel') == 'Y') & (col('surveyId')== 'E1D'), 
                                            when(coalesce(col('DPPCreditHours'), lit(0)) > 0, round(col('DPPCreditHours')/col('tmAnnualDPPCreditHoursFTE'), 0))
                                            .otherwise(lit(0))).cast('int').alias('field5')))
        
        # ********** Survey Formatting
        else:
            # Part A    
            a_data = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config)
            a_columns = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config)
            
            partA_out = sparkContext.parallelize(a_data)
            partA_out = spark.createDataFrame(partA_out).toDF(*a_columns)
            
            # Part C
            if survey_year_int > 1920:
                c_data = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config)
                c_columns = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'C', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config)
            
                partC_out = sparkContext.parallelize(c_data)
                partC_out = spark.createDataFrame(partC_out).toDF(*c_columns)
            
            # Part B
            b_data = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'B', part_type_in = 'data', ipeds_client_config_in = ipeds_client_config)
            b_columns = survey_format.get_part_format_string(survey_info_in = survey_info, part_in = 'B', part_type_in = 'columns', ipeds_client_config_in = ipeds_client_config)

            partB_out = sparkContext.parallelize(b_data)
            partB_out = spark.createDataFrame(partB_out).toDF(*b_columns)

    # ********** Survey Formatting          
    else:
        # Part A    
        a_data = survey_format.get_default_part_format_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'data')
        a_columns = survey_format.get_default_part_format_string(survey_info_in = survey_info, part_in = 'A', part_type_in = 'columns')
        
        partA_out = sparkContext.parallelize(a_data)
        partA_out = spark.createDataFrame(partA_out).toDF(*a_columns)
        
        # Part C
        if survey_year_int > 1920:
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

    if survey_year_int > 1920:
        for column in [column for column in partA_out.columns if column not in partC_out.columns]:
            partC_out = partC_out.withColumn(column, lit(None))

    for column in [column for column in partA_out.columns if column not in partB_out.columns]:
        partB_out = partB_out.withColumn(column, lit(None))

    if survey_year_int > 1920:
        surveyOutput = partA_out.unionByName(partC_out).unionByName(partB_out)
    else:
        surveyOutput = partA_out.unionByName(partB_out)

    return surveyOutput 
    
#test = run_twelve_month_enrollment_query()
#test.explain()
#test.show()
#print(test)    
