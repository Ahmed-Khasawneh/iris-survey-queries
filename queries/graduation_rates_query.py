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
survey_type = 'GRADUATION_RATES_1'
year = calendarYear = '2014'

#***************************************************************
#*
#***  run_graduation_rates_query
#*
#*  <survey notes here>
#***************************************************************

def run_graduation_rates_query(spark, survey_type, year):
    
    # ********** Survey Default Values
    year1 = str(year[2:4])
    year2 = str(int(year1) + 1)

    if survey_type == 'GRADUATION_RATES_1':
        survey_id = 'GR1'
        survey_ver_id = 'GR1'
    elif survey_type == 'GRADUATION_RATES_2':
        survey_id = 'GR1'
        survey_ver_id = 'GR2'
    elif survey_type == 'GRADUATION_RATES_3':
        survey_id = 'GR2'
        survey_ver_id = 'GR3'
    elif survey_type == 'GRADUATION_RATES_4':
        survey_id = 'GR2'
        survey_ver_id = 'GR4'
    elif survey_type == 'GRADUATION_RATES_5':
        survey_id = 'GR3'
        survey_ver_id = 'GR5'
    else:  # V6
        survey_id = 'GR3'
        survey_ver_id = 'GR6'

    survey_info = {'survey_type' : 'GR',
        'survey_long_type' : survey_type,
        'survey_id' : survey_id,
        'survey_ver_id' : survey_ver_id,
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
                
                # ********** Course Type Counts
                if reporting_period_terms.rdd.isEmpty() == False:
                    course_counts = course_type_counts(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_client_config_in = ipeds_client_config, academic_term_in = all_academic_terms, reporting_periods_in = reporting_period_terms) #.filter(col('regPersonId') == '36401')

                    # ********** Cohort
                    if course_counts.rdd.isEmpty() == False:
                        cohort_all = student_cohort(spark, survey_info_in = survey_info, default_values_in = default_survey_values, ipeds_client_config_in = ipeds_client_config, academic_term_in = all_academic_terms, reporting_periods_in = reporting_period_terms, course_type_counts_in = course_counts).cache()

                        if cohort_all.rdd.isEmpty() == False:
                            cohort_fall_term = (cohort_all
                            ####****TEST uncomment next line for test data '1415'
                                .filter((col('surveySection').isin('COHORT','FALL')) & (col('studentLevelUGGRDPP') == 'UG') & (col('isNonDegreeSeeking') == False) & (col('timeStatus') == 'Full Time'))
                                #.filter((col('surveySection') == 'COHORT') & (col('studentLevelUGGRDPP') == 'UG') & (col('isNonDegreeSeeking') == False) & (col('timeStatus') == 'Full Time'))
                                .select(
                                    col('yearType'),
                                    col('surveyId'),
                                    col('surveyType'),
                                    col('surveyYear_int'),
                                    col('financialAidYear'),
                                    col('repPerSnapshotDateTimestamp'),
                                    col('reportStartDate'),
                                    col('reportEndDate'),
                                    col('grReportTransferOut'),
                                    col('termCode'),
                                    col('termStartDate'),
                                    col('termEndDate'),
                                    col('fullTermOrder'),
                                    col('termCodeOrder'),
                                    col('maxCensus'),
                                    col('personId'),
                                    when(col('studentType') == 'Continuing', col('studentTypePreFallSummer')).otherwise(col('studentType')).alias('studentType'),
                                    col('degProgLengthInMonths'),
                                    col('awardLevel'),
                                    col('ethnicity'),
                                    col('gender'))
                                .filter(col('studentType') == 'First Time') #344
                                .withColumn('gradDate150', least(to_date(add_months(col('termStartDate'), (col('degProgLengthInMonths')* 1.5)),'YYYY-MM-DD'), default_survey_values['latest_or_only_status_as_of']))
                                .withColumn('awardLevelNo', when(col('awardLevel') == 'Bachelors Degree', lit(3))
                                                .when(col('awardLevel').isin('Associates Degree', 'Postsecondary (>1800 clock/>60 semester credit/>90 quarter credit hours)'), lit(2))
                                                .when(col('awardLevel').isin('Postsecondary (<300 clock/<9 semester credit/<13 quarter credit hours)', 
                                                                                'Postsecondary (300-899 clock/9-29 semester credit/13-44 quarter credit hours)', 
                                                                                'Postsecondary (900-1800 clock/30-60 semester credit/45-90 quarter credit hours)'), lit(1))
                                                .otherwise(lit(0))))

                            # ********** Cohort Financial Aid                              
                            cohort_financial_aid = financial_aid_cohort_mcr(spark, default_values_in = default_survey_values, cohort_df_in = cohort_fall_term, ipeds_client_config_in = ipeds_client_config)

                            
                            # ********** Cohort Award                               
                            cohort_award_200 = award_cohort_mcr(spark, cohort_df_in = cohort_financial_aid, default_tag_in = default_survey_values['latest_or_only_status'], default_as_of_date_in = default_survey_values['latest_or_only_status_as_of'])

                            # ********** Add check for award in earliest, mid or latest status here
                            # ********** Cohort Status
                                # ********** Transfer
                                # ********** CohortExclusion (2nd priority after transfer status ?)
                                # ********** Registration (still registered)


    return cohort_award_200
    
test = run_graduation_rates_query(spark, survey_type, year)
test.show()
#test.count()

#if test is None: # and isinstance(test,DataFrame): #exists(test): #test.isEmpty:
#    test = test
#else:
#    test.createOrReplaceTempView('test')


#test.print
#print(test)
