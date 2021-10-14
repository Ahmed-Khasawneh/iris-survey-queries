%pyspark

from pyspark.sql.window import Window
# from queries.twelve_month_enrollment_query import run_twelve_month_enrollment_query
from pyspark.sql.functions import sum as sum, expr, col, lit, upper, to_timestamp, max, min, row_number, date_trunc, \
    to_date, when, coalesce, count, rank, round, concat, substring, instr
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from awsglue.utils import getResolvedOptions

spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", -1).config(
    "spark.dynamicAllocation.enabled", 'true').getOrCreate()
sparkContext = SparkContext.getOrCreate()
sqlContext = SQLContext(sparkContext)
glueContext = GlueContext(sparkContext)

####****TEST uncomment survey_type and year for testing; these two values are set in run_query.py at runtime
survey_type = 'COMPLETIONS_1'
year = calendarYear = '2014'

#***************************************************************
#*
#***  run_completions_query
#*
#*  <survey notes here>
#***************************************************************

def run_completions_query(spark, survey_type, year):
    
    # ********** Survey Default Values
    year1 = str(year[2:4])
    year2 = str(int(year1) + 1)
    survey_year = year1 + year2

    survey_info = {'survey_type' : 'COM',
        'survey_long_type' : survey_type,
        'survey_id' : 'COM',
        'survey_ver_id' : 'COM',
        'survey_year_iris' : year,
        'survey_year_doris' : year1 + year2}
        
    default_survey_values = get_survey_default_values(survey_info)

    survey_year_int = int(survey_info['survey_year_doris'])
    
    # ********** Survey Client Configuration
    ipeds_client_config = ipeds_client_config_mcr(spark, survey_info_in = survey_info)

    # ********** Survey Reporting Period
    if ipeds_client_config.rdd.isEmpty() == False:
        ipeds_reporting_period = ipeds_reporting_period_mcr(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_client_config_in = ipeds_client_config)  

        if ipeds_reporting_period.rdd.isEmpty() == False:
            all_academic_terms = academic_term_mcr(spark)    
            
            if all_academic_terms.rdd.isEmpty() == False:
                reporting_period_terms = reporting_periods(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_reporting_period_in = ipeds_reporting_period, academic_term_in = all_academic_terms)

                # ********** Completions
                if reporting_period_terms.rdd.isEmpty() == False:
                    completions = (award_for_completions_mcr(spark, survey_info_in = survey_info, default_values_in = default_survey_values, reporting_periods_in = reporting_period_terms)
                        .withColumn('configGenderForNonBinary', lit(ipeds_client_config.first()['genderForNonBinary']))
                        .withColumn('configGenderForUnknown', lit(ipeds_client_config.first()['genderForUnknown'])))
                        
                    # ********** Person
                    if completions.rdd.isEmpty() == False:
                        completions_person = person_cohort_mcr(spark, cohort_df_in = completions)
                        
                        # ********** Degree Program
                        if completions_person.rdd.isEmpty() == False:
                            completions_degree_program = degree_program_cohort_mcr(spark, cohort_df_in = completions_person, academic_term_in = all_academic_terms, data_type_in = 'all')
                            
                # ***** Done with Completers
                
                # ***** Get all Programs
                    # Create degree_program_mcr
                    #all_degree_programs = degree_program_mcr()
                    
                # ***** Join completions_degree_program with all_degree_programs
                
                # ********** Survey Formatting
                
    return completions_degree_program
    
test = run_completions_query(spark, survey_type, year)
test.show()

#if test is None: # and isinstance(test,DataFrame): #exists(test): #test.isEmpty:
#    test = test
#else:
#    test.createOrReplaceTempView('test')

#test.count() #3m 31s
#test.print
#print(test)
