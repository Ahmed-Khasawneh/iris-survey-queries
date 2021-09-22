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

spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()
sparkContext = SparkContext.getOrCreate()
sqlContext = SQLContext(sparkContext)
glueContext = GlueContext(sparkContext)
#*  survey_info_in is required in all survey format functions

#***************************************************************************************************
#*
#***  get_part_data_string 
#*
#* use function if IPEDSClientConfig and other entities contain data
#* returns formatting strings based on survey, config values, part, and type of data requested
#*
#***************************************************************************************************

def get_part_data_string(survey_info_in, part_in = None, part_type_in = None, ipeds_client_config_in = None):

    if 'survey_year_doris' in survey_info_in:
        survey_year = survey_info_in['survey_year_doris']
    else: survey_year = 'xxxx'
    
    if 'survey_ver_id' in survey_info_in:
        survey_ver_id = survey_info_in['survey_ver_id']
    else: survey_ver_id = 'xxx'
    
    if 'survey_type' in survey_info_in:
        survey_type = survey_info_in['survey_type']
    else: survey_type = 'xxx'
    
    offer_undergraduate_award = ipeds_client_config_in.first()['icOfferUndergradAwardLevel'] if ipeds_client_config_in.rdd.isEmpty() == False else 'Y'
    offer_graduate_award = ipeds_client_config_in.first()['icOfferGraduateAwardLevel'] if ipeds_client_config_in.rdd.isEmpty() == False else 'Y'
    offer_doctor_award = ipeds_client_config_in.first()['icOfferDoctorAwardLevel'] if ipeds_client_config_in.rdd.isEmpty() == False else 'Y'
    instructional_activity_type = ipeds_client_config_in.first()['instructionalActivityType'] if ipeds_client_config_in.rdd.isEmpty() == False else 'CR'
    
    if survey_type == '12ME': 

            if part_in == 'A':
                a_columns = ['personId', 'ipedsPartAStudentLevel', 'ethnicity', 'gender']                 
                if part_type_in == 'columns': return a_columns

                #formatting based on client config values
                if offer_undergraduate_award == 'Y':
                    if (offer_graduate_award == 'Y') & (survey_ver_id == 'E1D'): 
                        a_data = [('', '1', '', ''), ('', '2', '', ''), ('', '3', '', ''), ('', '7', '', ''), ('', '15', '', ''),
                                ('', '16', '', ''), ('', '17', '', ''), ('', '21', '', ''), ('', '99', '', '')]
                        a_levels = ['1', '2', '3', '7', '15', '16', '17', '21', '99']
                    elif (survey_ver_id == 'E1D') | (survey_ver_id == 'E12'): 
                        a_data = [('', '1', '', ''), ('', '2', '', ''), ('', '3', '', ''), ('', '7', '', ''), ('', '15', '', ''),
                                ('', '16', '', ''), ('', '17', '', ''), ('', '21', '', '')]
                        a_levels = ['1', '2', '3', '7', '15', '16', '17', '21']
                    elif (survey_ver_id == 'E1E'): 
                        a_data = [('', '1', '', ''), ('', '3', '', ''), ('', '7', '', ''), ('', '15', '', ''), ('', '17', '', ''), ('', '21', '', '')]
                        a_levels = ['1', '3', '7', '15', '17', '21']
                    elif (survey_ver_id == 'E1F'):
                        a_data = [('', '1', '', ''), ('', '3', '', ''), ('', '15', '', ''), ('', '17', '', '')]
                        a_levels = ['1', '3', '15', '17']
                elif (offer_graduate_award == 'Y') & (survey_ver_id == 'E1D'): 
                        a_data = [('', '99', '', '')]
                        a_levels = ['99']

                if part_type_in == 'data': return a_data
                else: return a_levels

            elif part_in == 'C':
                c_columns = ['personId', 'ipedsPartCStudentLevel', 'distanceEducationType'] 
                if part_type_in == 'columns': return c_columns

                #formatting based on client config values
                if offer_undergraduate_award == 'Y':
                    if (offer_graduate_award == 'Y') & (survey_ver_id == 'E1D'): 
                        c_data = [('', '1', ''), ('', '2', ''), ('', '3', '')]
                        c_levels = ['1', '2', '3']
                    elif survey_ver_id != 'E1F':
                        c_data = [('', '1', ''), ('', '2', '')]
                        c_levels = ['1', '2']
                    else: # survey_ver_id == 'E1F':
                        c_data = [('', '1', '')]
                        c_levels = ['1']
                elif (offer_graduate_award == 'Y') & (survey_ver_id == 'E1D'): 
                        c_data = [('', '3', '')]
                        c_levels = ['3']

                if part_type_in == 'data': return c_data
                else: return c_levels

#***************************************************************************************************
#*
#***  get_part_format_string 
#*
#* use function if IPEDSClientConfig returns a record but other entities don't
#* returns formatting strings based on survey, config values, part, and type of data requested
#*
#***************************************************************************************************

def get_part_format_string(survey_info_in, part_in = None, part_type_in = None, ipeds_client_config_in = None):

    if 'survey_year_doris' in survey_info_in:
        survey_year = survey_info_in['survey_year_doris']
    else: survey_year = 'xxxx'
    
    if 'survey_ver_id' in survey_info_in:
        survey_ver_id = survey_info_in['survey_ver_id']
    else: survey_ver_id = 'xxx'
    
    if 'survey_type' in survey_info_in:
        survey_type = survey_info_in['survey_type']
    else: survey_type = 'xxx'
    
    offer_undergraduate_award = ipeds_client_config_in.first()['icOfferUndergradAwardLevel'] if ipeds_client_config_in.rdd.isEmpty() == False else 'Y'
    offer_graduate_award = ipeds_client_config_in.first()['icOfferGraduateAwardLevel'] if ipeds_client_config_in.rdd.isEmpty() == False else 'Y'
    offer_doctor_award = ipeds_client_config_in.first()['icOfferDoctorAwardLevel'] if ipeds_client_config_in.rdd.isEmpty() == False else 'Y'
    instructional_activity_type = ipeds_client_config_in.first()['instructionalActivityType'] if ipeds_client_config_in.rdd.isEmpty() == False else 'CR'
    
    if survey_type == '12ME': 

            if part_in == 'A':
                a_columns = ['part', 'field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9',
                                'field10', 'field11', 'field12', 'field13', 'field14', 'field15', 'field16', 'field17', 'field18',
                                'field19']  
                if part_type_in == 'columns': return a_columns

                #formatting based on client config values
                if offer_undergraduate_award == 'Y':
                    if (offer_graduate_award == 'Y') & (survey_ver_id == 'E1D'): 
                        a_data = [('A', '1', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '2', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '3', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '7', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '15', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '16', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '17', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '21', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '99', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0')]
                    elif (survey_ver_id == 'E1D') | (survey_ver_id == 'E12'):
                        a_data = [('A', '1', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '2', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '3', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '7', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '15', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '16', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '17', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '21', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0')]
                    elif (survey_ver_id == 'E1E'):
                        a_data = [('A', '1', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '3', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '7', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '15', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '17', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '21', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0')] 
                    elif (survey_ver_id == 'E1F'):
                        a_data = [('A', '1', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '3', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '15', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                                    ('A', '17', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0')] 
                elif (offer_graduate_award == 'Y') & (survey_ver_id == 'E1D'): 
                    a_data = [('A', '99', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0')]

                return a_data

            elif part_in == 'C':
                c_columns = ['part', 'field1', 'field2', 'field3']  
                if part_type_in == 'columns': return c_columns

                #formatting based on client config values
                if offer_undergraduate_award == 'Y':
                    if (offer_graduate_award == 'Y') & (survey_ver_id == 'E1D'): 
                        c_data = [('C', '1', '0', '0'), ('C', '2', '0', '0'), ('C', '3', '0', '0')]
                    elif survey_ver_id != 'E1F':
                        c_data = [('C', '1', '0', '0'), ('C', '2', '0', '0')]
                    else: #survey_ver_id == 'E1F'
                        c_data = [('C', '1', '0', '0')]
                elif (offer_graduate_award == 'Y') & (survey_ver_id == 'E1D'): 
                    c_data = [('C', '3', '0', '0')]

                return c_data

            else: # part_in == 'B':

                #formatting based on client config values
                if offer_undergraduate_award == 'Y':
                    if (offer_graduate_award == 'Y') & (survey_ver_id == 'E1D'):
                        if (offer_doctor_award == 'Y') & (survey_ver_id == 'E1D'): 
                            if instructional_activity_type == 'CR':
                                b_columns = ['part', 'field2', 'field4', 'field5']
                                b_data = [('B', '0', '0', '0')]
                            elif instructional_activity_type == 'CL':
                                b_columns = ['part', 'field3', 'field4', 'field5']
                                b_data = [('B', '0', '0', '0')]
                            else: # instructional_activity_type == 'B':
                                b_columns = ['part', 'field2', 'field3', 'field4', 'field5']
                                b_data = [('B', '0', '0', '0', '0')]
                        else:
                            if instructional_activity_type == 'CR':
                                b_columns = ['part', 'field2', 'field4']
                                b_data = [('B', '0', '0')]
                            elif instructional_activity_type == 'CL':
                                b_columns = ['part', 'field3', 'field4']
                                b_data = [('B', '0', '0')]
                            else: # instructional_activity_type == 'B':
                                b_columns = ['part', 'field2', 'field3', 'field4']
                                b_data = [('B', '0', '0', '0')]
                    else:
                        if instructional_activity_type == 'CR':
                            b_columns = ['part', 'field2']
                            b_data = [('B', '0')]
                        elif instructional_activity_type == 'CL':
                            b_columns = ['part', 'field3']
                            b_data = [('B', '0')]
                        else: # instructional_activity_type == 'B':
                            b_columns = ['part', 'field2', 'field3']
                            b_data = [('B', '0', '0')] 
                elif (offer_graduate_award == 'Y') & (offer_doctor__award == 'Y') & (survey_ver_id == 'E1D'):
                    b_columns = ['part', 'field4', 'field5']
                    b_data = [('B', '0', '0')]
                elif (offer_graduate_award == 'Y') & (survey_ver_id == 'E1D'):
                        b_columns = ['part', 'field4']
                        b_data = [('B', '0')] 
                elif (offer_doctor__award == 'Y') & (survey_ver_id == 'E1D'):
                        b_columns = ['part', 'field5']
                        b_data = [('B', '0')]

                if part_type_in == 'columns': 
                    return b_columns
                else: return b_data

#***************************************************************************************************
#*
#***  get_default_part_format_string  
#*
#* use function if IPEDSClientConfig does NOT return a record
#* returns formatting strings based on survey, part, and type of data requested
#*
#***************************************************************************************************

def get_default_part_format_string(survey_info_in, part_in = None, part_type_in = None):

    if 'survey_year_doris' in survey_info_in:
        survey_year = survey_info_in['survey_year_doris']
    else: survey_year = 'xxxx'
    
    if 'survey_ver_id' in survey_info_in:
        survey_ver_id = survey_info_in['survey_ver_id']
    else: survey_ver_id = 'xxx'
    
    if 'survey_type' in survey_info_in:
        survey_type = survey_info_in['survey_type']
    else: survey_type = 'xxx'

    if survey_type == '12ME': 

        if part_in == 'A':
            a_columns = ['part', 'field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9',
                            'field10', 'field11', 'field12', 'field13', 'field14', 'field15', 'field16', 'field17', 'field18',
                            'field19']
            if part_type_in == 'columns': return a_columns

            #formatting based on version
            if survey_ver_id == 'E1D': 
                a_data = [('A', '1', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '2', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '3', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '7', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '15', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '16', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '17', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '21', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '99', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0')]
            elif survey_ver_id == 'E12':
                a_data = [('A', '1', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '2', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '3', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '7', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '15', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '16', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '17', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '21', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0')]
            elif survey_ver_id == 'E1E':
                a_data = [('A', '1', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '3', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '7', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '15', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '17', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '21', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0')] 
            else: #survey_ver_id == 'E1F':
                a_data = [('A', '1', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '3', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '15', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'),
                            ('A', '17', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0')]

            return a_data

        elif part_in == 'C':    
            c_columns = ['part', 'field1', 'field2', 'field3'] 
            if part_type_in == 'columns': return c_columns
        
            #formatting based on client config values
            if survey_ver_id == 'E1D': 
                c_data = [('C', '1', '0', '0'),
                            ('C', '2', '0', '0'),
                            ('C', '3', '0', '0')]
            elif survey_ver_id != 'E1F':
                c_data = [('C', '1', '0', '0'),
                            ('C', '2', '0', '0')]
            else: # survey_ver_id == 'E1F'
                c_data = [('C', '1', '0', '0')]

            return c_data

        else: # part_in == 'B':
               
            #formatting based on client config values
            if survey_ver_id == 'E1D': 
                b_columns = ['part', 'field2', 'field3', 'field4', 'field5'] 
                b_data = [('B', '0', '0', '0', '0')]
            else:
                b_columns = ['part', 'field2', 'field3'] 
                b_data = [('B', '0', '0')]

            if part_type_in == 'columns': 
                return b_columns
            else: return b_data
