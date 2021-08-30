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

spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()
# spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).config("spark.dynamicAllocation.enabled", 'true').getOrCreate()
sparkContext = SparkContext.getOrCreate()
sqlContext = SQLContext(sparkContext)
glueContext = GlueContext(sparkContext)

def get_part_format_string(survey_type_in = '', survey_id_in = '', part_in = '', part_type_in = '',  ipeds_client_config_in = None, cohort_flag_in = None, course_flag_in = None):

    offer_undergraduate_award = ipeds_client_config_in.first()['icOfferUndergradAwardLevel'] if ipeds_client_config_in.rdd.isEmpty() == False else None
    offer_graduate_award = ipeds_client_config_in.first()['icOfferGraduateAwardLevel'] if ipeds_client_config_in.rdd.isEmpty() == False else None
    offer_doctor_award = ipeds_client_config_in.first()['icOfferDoctorAwardLevel'] if ipeds_client_config_in.rdd.isEmpty() == False else None
    instructional_activity_type = ipeds_client_config_in.first()['instructionalActivityType'] if ipeds_client_config_in.rdd.isEmpty() == False else None
    dpp_fte = ipeds_client_config_in.first()['tmAnnualDPPCreditHoursFTE'] if ipeds_client_config_in.rdd.isEmpty() == False else None
    
    a_columns = ["part", "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9",
                                "field10", "field11", "field12", "field13", "field14", "field15", "field16", "field17", "field18",
                                "field19"]
    
    c_columns = ["part", "field1", "field2", "field3"] 
    b_columns = ["part", "field2", "field3", "field4", "field5"]

    if survey_type_in == '12ME':    
        if survey_id_in == 'E1D':
        
            # default formatting values
            a_data = [("A", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "2", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "3", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "7", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "15", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "16", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "17", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "21", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "99", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")]
            a_level_values = ["1", "2", "3", "7", "15", "16", "17", "21", "99"]
            c_data = [("C", "1", "0", "0"), ("C", "2", "0", "0"), ("C", "3", "0", "0")]
            c_level_values = ["1", "2", "3"]
            b_data = [("B", "0", "0", "0", "0")]

            # cohort for parts A and C           
            if cohort_flag_in == False:
                a_columns = ["regPersonId", "ipedsPartAStudentLevel", "persIpedsEthnValue", "persIpedsGender"]
                c_columns = ["regPersonId", "ipedsLevel", "distanceEdInd"]
                if offer_undergraduate_award == 'Y':
                    if offer_graduate_award == 'Y': 
                        a_data = [("", "1", "", ""), ("", "2", "", ""), ("", "3", "", ""), ("", "7", "", ""), ("", "15", "", ""),
                            ("", "16", "", ""), ("", "17", "", ""), ("", "21", "", ""), ("", "99", "", "")]
                        c_data = [("", "1", ""), ("", "2", ""), ("", "3", "")]
                    else: 
                        a_level_values = ["1", "2", "3", "7", "15", "16", "17", "21"]
                        a_data = [("", "1", "", ""), ("", "2", "", ""), ("", "3", "", ""), ("", "7", "", ""), ("", "15", "", ""),
                            ("", "16", "", ""), ("", "17", "", ""), ("", "21", "", "")]
                        c_level_values = ["1", "2"]
                        c_data = [("", "1", ""), ("", "2", "")]
                elif (offer_undergraduate_award != 'Y') & (offer_graduate_award == 'Y'):  
                    a_level_values = ["99"]
                    a_data = [("", "99", "", "")]
                    c_level_values = ["3"]
                    c_data = [("", "3", "")] 
            else:
                if (offer_undergraduate_award == 'Y') and (offer_graduate_award != 'Y'):
                        a_data = [("A", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "2", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "3", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "7", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "15", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "16", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "17", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "21", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")]
                        c_data = [("C", "1", "0", "0"), ("C", "2", "0", "0")] 
                elif (offer_undergraduate_award != 'Y') and (offer_graduate_award == 'Y'):
                    a_data = [("A", "99", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")]
                    c_data = [("C", "3", "0", "0")]

            # course counts for part B                    
            if course_flag_in:
                if offer_undergraduate_award == 'Y':
                    if offer_graduate_award == 'Y':
                        if offer_doctor_award == 'Y':
                            if instructional_activity_type == 'CR':
                                b_columns = ["part", "field2", "field4", "field5"]
                                b_data = [("B", "0", "0", "0")]
                            elif instructional_activity_type == 'CL':
                                b_columns = ["part", "field3", "field4", "field5"]
                                b_data = [("B", "0", "0", "0")]
                        else: 
                            if instructional_activity_type == 'CR':
                                b_columns = ["part", "field2",  "field4"]
                                b_data = [("B", "0", "0")]
                            elif instructional_activity_type == 'CL':
                                b_columns = ["part", "field3", "field4"]
                                b_data = [("B", "0", "0")]
                            else: # instructional_activity_type == 'B':
                                b_columns = ["part", "field2", "field3", "field4"]
                                b_data = [("B", "0", "0", "0")]
                    else: 
                        if instructional_activity_type == 'CR':
                            b_columns = ["part", "field2"]
                            b_data = [("B", "0")]
                        elif instructional_activity_type == 'CL':
                            b_columns = ["part", "field3"]
                            b_data = [("B", "0")]
                        else: # instructional_activity_type == 'B':
                            b_columns = ["part", "field2", "field3"]
                            b_data = [("B", "0", "0")] 
                elif (offer_undergraduate_award != 'Y') & (offer_graduate_award == 'Y'):
                    if offer_doctor_award == 'Y':
                        b_columns = ["part", "field4", "field5"]
                        b_data = [("B", "0", "0")]
                    else: 
                        b_columns = ["part", "field4"]
                        b_data = [("B", "", "", "0", "")] 
                        
        elif survey_id_in == 'E12':
        
            # default formatting values
            a_data = [("A", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "2", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "3", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "7", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "15", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "16", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "17", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                                  ("A", "21", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")]
            a_level_values = ["1", "2", "3", "7", "15", "16", "17", "21"]
            c_data = [("C", "1", "0", "0"), ("C", "2", "0", "0")]
            c_level_values = ["1", "2"]
            b_columns = ["part", "field2", "field3"]
            b_data = [("B", "0", "0")]
            
            # cohort for parts A and C 
            if cohort_flag_in == False:
                a_columns = ["regPersonId", "ipedsPartAStudentLevel", "persIpedsEthnValue", "persIpedsGender"]
                c_columns = ["regPersonId", "ipedsLevel", "distanceEdInd"]
                a_data = [("", "1", "", ""), ("", "2", "", ""), ("", "3", "", ""), ("", "7", "", ""), ("", "15", "", ""),
                            ("", "16", "", ""), ("", "17", "", ""), ("", "21", "", "")]
                c_data = [("", "1", ""), ("", "2", "")]

            # course counts for part B                    
            if course_flag_in:
                if instructional_activity_type == 'CR':
                    b_columns = ["part", "field2"]
                    b_data = [("B", "0")]
                elif instructional_activity_type == 'CL':
                    b_columns = ["part", "field3"]
                    b_data = [("B", "0")]
                else: # instructional_activity_type == 'B':
                    b_columns = ["part", "field2", "field3"]
                    b_data = [("B", "0", "0")]
                    
        elif survey_id_in == 'E1E':
                
            # default formatting values
            a_data = [("A", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                          ("A", "3", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                          ("A", "7", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                          ("A", "15", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                          ("A", "17", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                          ("A", "21", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")] 
            a_level_values = ["1", "3", "7", "15", "17", "21"]
            c_data = [("C", "1", "0", "0"), ("C", "2", "0", "0")]
            c_level_values = ["1", "2"]
            b_columns = ["part", "field2", "field3"]
            b_data = [("B", "0", "0")]
            
            # cohort for parts A and C 
            if cohort_flag_in == False:          
                a_columns = ["regPersonId", "ipedsPartAStudentLevel", "persIpedsEthnValue", "persIpedsGender"]
                c_columns = ["regPersonId", "ipedsLevel", "distanceEdInd"]
                a_data = [("", "1", "", ""), ("", "3", "", ""), ("", "7", "", ""), ("", "15", "", ""),
                      ("", "17", "", ""), ("", "21", "", "")]
                c_data = [("", "1", ""), ("", "2", "")]

            # course counts for part B                    
            if course_flag_in:
                if instructional_activity_type == 'CR':
                    b_columns = ["part", "field2"]
                    b_data = [("B", "0")]
                elif instructional_activity_type == 'CL':
                    b_columns = ["part", "field3"]
                    b_data = [("B", "0")]
                else: # instructional_activity_type == 'B':
                    b_columns = ["part", "field2", "field3"]
                    b_data = [("B", "0", "0")]
                    
        else:  # survey_id_in == 'E1F'
                        
            # default formatting values 
            a_data = [("A", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                          ("A", "3", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                          ("A", "15", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
                          ("A", "17", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")] 
            a_level_values = ["1", "3", "15", "17"]
            c_data = [("C", "1", "0", "0")]
            c_level_values = ["1"]
            b_columns = ["part", "field2", "field3"]
            b_data = [("B", "0", "0")]
            
            # cohort for parts A and C 
            if cohort_flag_in == False:
                a_columns = ["regPersonId", "ipedsPartAStudentLevel", "persIpedsEthnValue", "persIpedsGender"]
                c_columns = ["regPersonId", "ipedsLevel", "distanceEdInd"]
                a_data = [("", "1", "", ""), ("", "3", "", ""), ("", "15", "", ""), ("", "17", "", "")]
                c_data = [("", "1", "")]

            # course counts for part B                    
            if course_flag_in:
                if instructional_activity_type == 'CR':
                    b_columns = ["part", "field2"]
                    b_data = [("B", "0")]
                elif instructional_activity_type == 'CL':
                    b_columns = ["part", "field3"]
                    b_data = [("B", "0")]
                else: # instructional_activity_type == 'B':
                    b_columns = ["part", "field2", "field3"]
                    b_data = [("B", "0", "0")]
    
        if part_in == 'A':
            if part_type_in == 'columns': return a_columns
            elif part_type_in == 'levels': return a_level_values
            else: return a_data
        elif part_in == 'B':
            if part_type_in == 'columns': return b_columns
            else: return b_data
        elif part_in == 'C':
            if part_type_in == 'columns': return c_columns
            elif part_type_in == 'levels': return c_level_values
            else: return c_data
