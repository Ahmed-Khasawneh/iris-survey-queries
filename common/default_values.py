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

def get_12me_dict(survey_ver_id, year_in, default_dictionary_in):
    
#E1D 12-month Enrollment for 4-year degree-granting institutions
#E12 12-month Enrollment for 2-year degree-granting institutions        
#E1E 12-month Enrollment for public 2-year and less-than-2-year non-degree-granting institutions
#E1F 12-month Enrollment for private 2-year and less-than-2-year non-degree-granting institutions

    year = year_in
    next_year = str(int(year) + 1) 
    prior_year = str(int(year) - 1) 

    default_dictionary_in['report_start'] = datetime.strftime(datetime.strptime((prior_year + '-07-01'), "%Y-%m-%d"), "%Y-%m-%d")
    default_dictionary_in['report_end'] = datetime.strftime(datetime.strptime((year + '-06-30'), "%Y-%m-%d"), "%Y-%m-%d")
    default_dictionary_in['cohort_academic_fall'] = 'Fall Census'
    default_dictionary_in['cohort_academic_pre_fall_summer'] = 'Pre-Fall Summer Census'
    default_dictionary_in['cohort_academic_spring_tag'] = 'Spring Census'
    default_dictionary_in['cohort_academic_post_spring_summer_tag'] = 'Post-Spring Summer Census'
    default_dictionary_in['cohort_program'] = 'Academic Year End'
    default_dictionary_in['cohort_program_2'] = 'June End'
    default_dictionary_in['current_survey_sections'] = ['COHORT']
        
    return default_dictionary_in

def get_gr_dict(survey_ver_id, year_in, default_dictionary_in):

#GR1 Graduation Rates for 4-year institutions reporting on a fall cohort (academic reporters)
#GR2 Graduation Rates for 4-year institutions reporting on a full-year cohort (program reporters)
#GR3 Graduation Rates for 2-year institutions reporting on a fall cohort (academic reporters)
#GR4 Graduation Rates for 2-year institutions reporting on a full-year cohort (program reporters)
#GR5 Graduation Rates for less-than-2-year institutions reporting on a fall cohort (academic reporters)
#GR6 Graduation Rates for less-than-2-year institutions reporting on a full-year cohort (program reporters)

    year = year_in
    next_year = str(int(year) + 1) 
    prior_year = str(int(year) - 1)
    prior_year_2 = str(int(year) - 2) 
    gr_4yr_cohort_year = str(int(year) - 6)
    gr_4yr_cohort_next_year = str(int(year) - 5)
    gr_2yr_cohort_year = str(int(year) - 3)
    gr_2yr_cohort_next_year = prior_year_2
    
    default_dictionary_in['financial_aid'] = 'Financial Aid Year End'
    default_dictionary_in['transfer'] = 'Student Transfer Data'
    default_dictionary_in['latest_or_only_status'] = 'August End'
    default_dictionary_in['latest_or_only_status_as_of'] = datetime.strftime(datetime.strptime((year + '-08-30'), "%Y-%m-%d"), "%Y-%m-%d")
    default_dictionary_in['current_survey_sections'] = ['COHORT', 'PRIOR SUMMER']   #['FALL', 'COHORT', 'PRIOR SUMMER']  
    
    if survey_ver_id == 'GR1':
        default_dictionary_in['cohort_academic_fall'] = 'Fall Census'
        default_dictionary_in['default_fall_census'] = datetime.strftime(datetime.strptime((gr_4yr_cohort_year + '-10-15'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['cohort_academic_pre_fall_summer'] = 'Pre-Fall Summer Census'
        default_dictionary_in['earliest_or_only_status'] = 'August End'
        default_dictionary_in['earliest_or_only_status_as_of'] = datetime.strftime(datetime.strptime((prior_year_2 + '-08-30'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['mid_or_only_status'] = 'August End'
        default_dictionary_in['mid_or_only_status_as_of'] = datetime.strftime(datetime.strptime((prior_year + '-08-30'), "%Y-%m-%d"), "%Y-%m-%d")

    elif survey_ver_id == 'GR2':        
        default_dictionary_in['cohort_program'] = 'August End'
        default_dictionary_in['cohort_program_start'] = datetime.strftime(datetime.strptime((gr_4yr_cohort_year + '-09-01'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['cohort_program_end'] = datetime.strftime(datetime.strptime((gr_4yr_cohort_next_year + '-08-31'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['earliest_or_only_status'] = 'August End'
        default_dictionary_in['earliest_or_only_status_as_of'] = datetime.strftime(datetime.strptime((prior_year_2 + '-08-30'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['mid_or_only_status'] = 'August End'
        default_dictionary_in['mid_or_only_status_as_of'] = datetime.strftime(datetime.strptime((prior_year + '-08-30'), "%Y-%m-%d"), "%Y-%m-%d")
        
    elif survey_ver_id in ('GR3', 'GR5'):
        default_dictionary_in['cohort_academic_fall'] = 'Fall Census'
        default_dictionary_in['default_fall_census'] = datetime.strftime(datetime.strptime((gr_2yr_cohort_year + '-10-15'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['cohort_academic_pre_fall_summer'] = 'Pre-Fall Summer Census'
        
    else: # survey_ver_id in ('GR4', 'GR6'):        
        default_dictionary_in['cohort_program'] = 'August End'
        default_dictionary_in['cohort_program_start'] = datetime.strftime(datetime.strptime((gr_2yr_cohort_year + '-09-01'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['cohort_program_end'] = datetime.strftime(datetime.strptime((gr_2yr_cohort_next_year + '-08-31'), "%Y-%m-%d"), "%Y-%m-%d")
        
    return default_dictionary_in
    
    
def get_sfa_dict(survey_ver_id, year_in, default_dictionary_in):

#SFA1 Student Financial Aid for public institutions reporting on a fall cohort (academic reporters)
#SFA2 Student Financial Aid for private institutions reporting on a fall cohort (academic reporters)
#SFA3 Student Financial Aid for institutions reporting on a full-year cohort (public program reporters)
#SFA4 Student Financial Aid for institutions reporting on a full-year cohort (private program reporters)
#SFA5 Student Financial Aid for institutions with graduate students only

    year = year_in
    next_year = str(int(year) + 1) 
    prior_year = str(int(year) - 1) 
    
    default_dictionary_in['financial_aid'] = 'Financial Aid Year End'
    default_dictionary_in['gi_bill'] = 'GI Bill'
    default_dictionary_in['gi_bill_start'] = datetime.strftime(datetime.strptime((prior_year + '-07-01'), "%Y-%m-%d"), "%Y-%m-%d")
    default_dictionary_in['gi_bill_end'] = datetime.strftime(datetime.strptime((year + '-06-30'), "%Y-%m-%d"), "%Y-%m-%d")
    default_dictionary_in['department_of_defense'] = 'Department of Defense'
    default_dictionary_in['department_of_defense_start'] = datetime.strftime(datetime.strptime((prior_year + '-10-01'), "%Y-%m-%d"), "%Y-%m-%d")
    default_dictionary_in['department_of_defense_end'] = datetime.strftime(datetime.strptime((year + '-09-30'), "%Y-%m-%d"), "%Y-%m-%d")
    default_dictionary_in['current_survey_sections'] = ['COHORT', 'PRIOR SUMMER']   #['FALL', 'COHORT', 'PRIOR SUMMER']  
    default_dictionary_in['prior_survey_sections'] = ['PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER']
    default_dictionary_in['prior_2_survey_sections'] = ['PRIOR YEAR 2 COHORT', 'PRIOR YEAR 2 PRIOR SUMMER']
    
    if survey_ver_id in ('SFA1', 'SFA2'):
        default_dictionary_in['cohort_academic_fall'] = 'Fall Census'
        default_dictionary_in['default_fall_census'] = datetime.strftime(datetime.strptime((prior_year + '-10-15'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['cohort_academic_pre_fall_summer'] = 'Pre-Fall Summer Census'
        default_dictionary_in['cohort_hybrid'] = 'October End'
        default_dictionary_in['cohort_hybrid_start'] = datetime.strftime(datetime.strptime((prior_year + '-10-01'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['cohort_hybrid_end'] = datetime.strftime(datetime.strptime((prior_year + '-10-31'), "%Y-%m-%d"), "%Y-%m-%d")
        
    elif survey_ver_id in ('SFA3', 'SFA4'):
        default_dictionary_in['financial_aid_start'] = datetime.strftime(datetime.strptime((year + '-07-01'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['financial_aid_end'] = datetime.strftime(datetime.strptime((next_year + '-06-30'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['cohort_program'] = 'Academic Year End'
        default_dictionary_in['cohort_program_start'] = datetime.strftime(datetime.strptime((prior_year + '-10-01'), "%Y-%m-%d"), "%Y-%m-%d")
        default_dictionary_in['cohort_program_end'] = datetime.strftime(datetime.strptime((prior_year + '-10-31'), "%Y-%m-%d"), "%Y-%m-%d")
        
    else:  # V5
        default_dictionary_in['financial_aid'] = ''
        
    return default_dictionary_in

    
def get_updated_dictionary(survey_info_in, default_dictionary_in):
    
    survey_type = survey_info_in['survey_type']
    survey_ver_id = survey_info_in['survey_ver_id']
    year = survey_info_in['survey_year_iris']  
    
    if survey_type == 'SFA':
        survey_default_values = get_sfa_dict(survey_ver_id, year, default_dictionary_in)
    elif survey_type == '12ME':
        survey_default_values = get_12me_dict(survey_ver_id, year, default_dictionary_in)
    else: # survey_type == 'GR':
        survey_default_values = get_gr_dict(survey_ver_id, year, default_dictionary_in)
        
    return survey_default_values
    
    
#***************************************************************
#*
#***  get_survey_tags 
#*
#***************************************************************

def get_survey_default_values(survey_info_in):

    default_dictionary = dict.fromkeys(['report_start',
        'report_end',
        'cohort_academic_fall',
        'default_fall_census',
        'cohort_academic_pre_fall_summer',
        'cohort_academic_spring',
        'cohort_academic_post_spring_summer',
        'cohort_hybrid',
        'cohort_hybrid_start',
        'cohort_hybrid_end',
        'cohort_program',
        'cohort_program_2', 
        'cohort_program_start',
        'cohort_program_end', 
        'gi_bill',
        'gi_bill_start', 
        'gi_bill_end',
        'department_of_defense', 
        'department_of_defense_start', 
        'department_of_defense_end', 
        'financial_aid', 
        'financial_aid_start', 
        'financial_aid_end', 
        'transfer',
        'earliest_status', 
        'earliest_status_as_of', 
        'mid_status', 
        'mid_status_as_of', 
        'latest_or_only_status', 
        'latest_or_only_status_as_of', 
        'fiscal_year', 
        'fiscal_year_as_of', 
        'hr', 
        'hr_as_of', 
        'ic', 
        'ic_as_of', 
        'current_survey_sections', 
        'prior_survey_sections', 
        'prior_2_survey_sections'])
        
    survey_dictionary = get_updated_dictionary(survey_info_in, default_dictionary)
        
    return survey_dictionary
