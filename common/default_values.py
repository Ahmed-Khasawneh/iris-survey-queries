import logging
import sys
import boto3
import json
from uuid import uuid4
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum, expr, col, lit, upper, to_timestamp, max, min, row_number, date_trunc, \
    to_date, when, coalesce, count, rank
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from awsglue.utils import getResolvedOptions

#***************************************************************
#*
#***  get_survey_tags 
#*
#***************************************************************

def get_survey_tags(survey_info_in):
    
    survey_year = survey_info_in['survey_year_doris']
    survey_ver_id = survey_info_in['survey_ver_id']
    survey_type = survey_info_in['survey_type']
    survey_id = survey_info_in['survey_id']
    year = survey_info_in['survey_year_iris']
    
    cohort_academic_fall_tag = 'Fall Census'
    cohort_academic_pre_fall_summer_tag = 'Pre-Fall Summer Census'
    cohort_academic_spring_tag = 'Spring Census'
    cohort_academic_post_spring_summer_tag = 'Post-Spring Summer Census'
    cohort_hybrid_tag = 'October End'
    cohort_program_tag = 'Academic Year End'
    gi_bill_tag = 'GI Bill'
    department_of_defense_tag = 'Department of Defense'
    financial_aid_tag = 'Financial Aid Year End'
    cohort_status_tag_1 = 'August End'
    cohort_status_tag_2 = 'Academic Year End'
    transfer_tag = 'Student Transfer Data'
    fiscal_year_tag = 'Fiscal Year Lockdown'
    hr_tag = 'HR Reporting End'

    current_survey_sections = ['COHORT', 'PRIOR SUMMER']    
####****TEST uncomment to test older IPEDSReportingPeriod snapshots
    #current_survey_sections = ['FALL', 'COHORT', 'PRIOR SUMMER']
    prior_survey_sections = ['PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER']
    prior_2_survey_sections = ['PRIOR YEAR 2 COHORT', 'PRIOR YEAR 2 PRIOR SUMMER']

    if survey_type == 'SFA':
        cohort_academic_spring_tag = ''
        cohort_academic_post_spring_summer_tag = ''
        cohort_status_tag_1 = ''
        cohort_status_tag_2 = ''
        transfer_tag = ''
        fiscal_year_tag = ''
        hr_tag = ''
 
        if survey_ver_id == 'SFA1':
            cohort_program_tag = ''
        elif survey_ver_id == 'SFA2':
            cohort_program_tag = ''
        elif survey_ver_id == 'SFA3':
            cohort_academic_fall_tag = ''
            cohort_academic_pre_fall_summer_tag = ''
            cohort_hybrid_tag = ''
        elif survey_ver_id == 'SFA4':
            cohort_academic_fall_tag = ''
            cohort_academic_pre_fall_summer_tag = ''
            cohort_hybrid_tag = ''
        else:  # V5
            cohort_academic_fall_tag = ''
            cohort_academic_pre_fall_summer_tag = ''
            cohort_program_tag = ''
            cohort_hybrid_tag = ''
            financial_aid_tag = ''
    
    if survey_type == '12ME':
        cohort_hybrid_tag = ''
        gi_bill_tag = ''
        department_of_defense_tag = ''
        financial_aid_tag = ''
        transfer_tag = ''
        fiscal_year_tag = ''
        hr_tag = ''
        current_survey_sections = ['COHORT']
        prior_survey_sections = ''
        prior_2_survey_sections = ''
 
    tag_dictionary = {'cohort_academic_fall_tag' : cohort_academic_fall_tag,
        'cohort_academic_pre_fall_summer_tag' : cohort_academic_pre_fall_summer_tag,
        'cohort_academic_spring_tag' : cohort_academic_spring_tag,
        'cohort_academic_post_spring_summer_tag' : cohort_academic_post_spring_summer_tag,
        'cohort_hybrid_tag' : cohort_hybrid_tag,
        'cohort_program_tag' : cohort_program_tag,
        'gi_bill_tag' : gi_bill_tag,
        'department_of_defense_tag' : department_of_defense_tag,
        'financial_aid_tag' : financial_aid_tag,
        'cohort_status_tag_1' : cohort_status_tag_1,
        'cohort_status_tag_2' : cohort_status_tag_2,
        'transfer_tag' : transfer_tag,
        'fiscal_year_tag' : fiscal_year_tag,
        'hr_tag' : hr_tag,
        'current_survey_sections' : current_survey_sections,
        'prior_survey_sections' : prior_survey_sections,
        'prior_2_survey_sections' : prior_2_survey_sections}
 
    return tag_dictionary

#***************************************************************
#*
#***  get_survey_dates  
#*
#***************************************************************

def get_survey_dates(survey_info_in):

    survey_year = survey_info_in['survey_year_doris']
    survey_ver_id = survey_info_in['survey_ver_id']
    survey_type = survey_info_in['survey_type']
    survey_id = survey_info_in['survey_id']
    year = survey_info_in['survey_year_iris'] 
    
    year2 = str(int(year) + 1) 
    prior_year = str(int(year) - 1) 

    report_start_date = datetime.strftime(datetime.strptime((prior_year + '-07-01'), "%Y-%m-%d"), "%Y-%m-%d")
    report_end_date = datetime.strftime(datetime.strptime((year + '-06-30'), "%Y-%m-%d"), "%Y-%m-%d")
    default_fall_census_date = datetime.strftime(datetime.strptime((prior_year + '-10-15'), "%Y-%m-%d"), "%Y-%m-%d")
    cohort_program_or_hybrid_start_date = datetime.strftime(datetime.strptime((prior_year + '-10-01'), "%Y-%m-%d"), "%Y-%m-%d")
    cohort_program_or_hybrid_end_date = datetime.strftime(datetime.strptime((prior_year + '-10-31'), "%Y-%m-%d"), "%Y-%m-%d")
    gi_bill_start_date = datetime.strftime(datetime.strptime((prior_year + '-07-01'), "%Y-%m-%d"), "%Y-%m-%d")
    gi_bill_end_date = datetime.strftime(datetime.strptime((year + '-06-30'), "%Y-%m-%d"), "%Y-%m-%d")
    department_of_defense_start_date = datetime.strftime(datetime.strptime((prior_year + '-10-01'), "%Y-%m-%d"), "%Y-%m-%d")
    department_of_defense_end_date = datetime.strftime(datetime.strptime((year + '-09-30'), "%Y-%m-%d"), "%Y-%m-%d")
    financial_aid_start_date = datetime.strftime(datetime.strptime((year + '-07-01'), "%Y-%m-%d"), "%Y-%m-%d")
    financial_aid_end_date = datetime.strftime(datetime.strptime((year2 + '-06-30'), "%Y-%m-%d"), "%Y-%m-%d")
    earliest_status_date = ''
    mid_status_date = ''
    latest_status_date = ''
    fiscal_year_as_of_date = ''
    hr_as_of_date = ''

    if survey_type == 'SFA':
        report_start_date = '' 
        report_end_date = ''
    
        if survey_ver_id == 'SFA1':
            financial_aid_start_date = ''
            financial_aid_end_date = ''
        elif survey_ver_id == 'SFA2':
            financial_aid_start_date = ''
            financial_aid_end_date = ''
        elif survey_ver_id == 'SFA3':
            cohort_program_or_hybrid_start_date = ''
            cohort_program_or_hybrid_end_date = ''
        elif survey_ver_id == 'SFA4':
            cohort_program_or_hybrid_start_date = ''
            cohort_program_or_hybrid_end_date = ''
            default_fall_census_date = ''
        else:  # V5
            cohort_program_or_hybrid_start_date = ''
            cohort_program_or_hybrid_end_date = ''
            default_fall_census_date = ''

    if survey_type == '12ME':
        default_fall_census_date = ''
        cohort_program_or_hybrid_start_date = ''
        cohort_program_or_hybrid_end_date = ''
        gi_bill_start_date = ''
        gi_bill_end_date = ''
        department_of_defense_start_date = ''
        department_of_defense_end_date = ''
        financial_aid_start_date = ''
        financial_aid_end_date = ''
            
    dates_dictionary = {'report_start_date' : report_start_date,
        'report_end_date' : report_end_date,
        'default_fall_census_date' : default_fall_census_date,
        'cohort_program_or_hybrid_start_date' : cohort_program_or_hybrid_start_date,
        'cohort_program_or_hybrid_end_date' : cohort_program_or_hybrid_end_date,
        'gi_bill_start_date' : gi_bill_start_date,
        'gi_bill_end_date' : gi_bill_end_date,
        'department_of_defense_start_date' : department_of_defense_start_date,
        'department_of_defense_end_date' : department_of_defense_end_date,
        'financial_aid_start_date' : financial_aid_start_date,
        'financial_aid_end_date' : financial_aid_end_date,
        'earliest_status_date' : earliest_status_date,
        'mid_status_date' : mid_status_date,
        'latest_status_date' : latest_status_date,
        'fiscal_year_as_of_date' : fiscal_year_as_of_date,
        'hr_as_of_date' : hr_as_of_date}
 
    return dates_dictionary
