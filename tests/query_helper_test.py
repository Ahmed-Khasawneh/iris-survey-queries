from common.query_helpers import *
from pyspark_test import assert_pyspark_df_equal
from common.survey_format import *
from common.default_values import *
from pyspark.sql import HiveContext
from .Mock_data import create_mock_data
import pytest


var_surveyId = 'E1D'  # survey_id_map[args['survey_type']]
var_surveyType = '12ME'
var_surveyYear = '2021'
var_repPeriodTag1 = 'Academic Year End'
var_repPeriodTag2 = 'June End'
var_repPeriodTag3 = 'Fall Census'
var_repPeriodTag4 = 'Fall Census'
var_repPeriodTag5 = 'Fall Census'

#Completed
def test_ipeds_client_config_mcr(sql_context):
    expected = "[Row(acadOrProgReporter='', admAdmissionTestScores='', admCollegePrepProgram='',  admDemoOfCompetency='', admOtherTestScores='', admRecommendation='', admSecSchoolGPA='', admSecSchoolRank='', admSecSchoolRecord='', admTOEFL='', admUseForBothSubmitted='', admUseForMultiOfSame='', admUseTestScores='', compGradDateOrTerm='', eviReserved1='', eviReserved2='', eviReserved3='', eviReserved4='', eviReserved5='', feIncludeOptSurveyData='', finAthleticExpenses='', finBusinessStructure='', finEndowmentAssets='', finGPFSAuditOpinion='', finParentOrChildInstitution='', finPellTransactions='', finPensionBenefits='', finReportingModel='', finTaxExpensePaid='', fourYrOrLessInstitution='', genderForNonBinary='', genderForUnknown='', grReportTransferOut='', hrIncludeSecondarySalary='', icOfferDoctorAwardLevel='', icOfferGraduateAwardLevel='', icOfferUndergradAwardLevel='', includeNonDegreeAsUG='', instructionalActivityType='', ncBranchCode='', ncSchoolCode='', ncSchoolName='', publicOrPrivateInstitution='', recordActivityDate=None, sfaGradStudentsOnly='', sfaLargestProgCIPC='', sfaReportPriorYear='', sfaReportSecondPriorYear='', surveyCollectionYear='2021', tmAnnualDPPCreditHoursFTE='0', snapshotDate=None, tags=['Fall Census'], survey_id='E1D', survey_type='12ME', clientConfigRowNum=1)]"
    survey_info_in = {"survey_year_doris": var_surveyYear, "survey_id": var_surveyId, "survey_type": var_surveyType}
    ipeds_client_config_output = ipeds_client_config_mcr(sql_context, survey_info_in)
    actual = str(ipeds_client_config_output.collect())
    assert actual == expected
#Completed
def test_ipeds_reporting_period_mcr(sql_context):
    surveySectionValues='COHORT'
    expected = "[Row(termCode='202110', partOfTermCode='', snapshotDateTimestamp=None, snapshotDate=None, recordActivityDate=datetime.datetime(9999, 9, 9, 0, 0), surveyCollectionYear='2021', surveyId='E1D', surveyName='', surveySection='COHORT', yearType='CY', tags=['Fall Census'], ipedsRepPerRowNum=1)]"
    ipeds_reporting_period_in = sql_context.read.parquet('./tests/entities/IPEDSReportingPeriod.parquet')
    print('Testing ipeds_reporting_period_mcr expected output...')
    survey_info_in = {"survey_year_doris": var_surveyYear, "survey_id":var_surveyId, "survey_type":var_surveyType}
    default_info_in = {"survey_year_iris": var_surveyYear, "survey_ver_id": var_surveyId, "survey_type": var_surveyType}
    default_values = get_survey_default_values(default_info_in)
    survey_tags_in = {"current_survey_sections":"[Fall Census]","prior_survey_sections":['PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER'],"prior_2_survey_sections":['PRIOR YEAR 2 COHORT', 'PRIOR YEAR 2 PRIOR SUMMER']}
    ipeds_reporting_period_output = ipeds_reporting_period_mcr(sql_context, survey_info_in, default_values, ipeds_reporting_period_in)
    actual = str(ipeds_reporting_period_output.collect())
    print(actual)
    assert actual == expected
#Completed
def test_academic_term_mcr(sql_context):
    expected = "[Row(academicYear='', censusDate='0909999', endDate='0909999', financialAidYear='', termCode='202110', partOfTermCode='', snapshotDateTimestamp=None, snapshotDate=None, partOfTermCodeDescription='', requiredFTCreditHoursGR=0, requiredFTCreditHoursUG=0, requiredFTClockHoursUG=0, startDate='0909999', termClassification='Non-Standard Length', termCodeDescription='202110', termType='Summer', tags=['Fall Census'], acadTermRowNum=1, partOfTermOrder=1, termCodeOrder=1, maxCensus='0909999', minStart='0909999', maxEnd='0909999')]"
    actual = str(academic_term_mcr(sql_context).collect())
    assert actual == expected
#AttributeError: 'SparkSession' object has no attribute 'runJob'
def test_reporting_period(sql_context):
    ipeds_reporting_period_in = sql_context.read.parquet('./tests/entities/IPEDSReportingPeriod.parquet')
    academic_term_in = sql_context.read.parquet('./tests/entities/AcademicTerm.parquet')
    survey_info_in = {"survey_year_doris": var_surveyYear, "survey_id": var_surveyId, "survey_type": var_surveyType}
    survey_tags_in = {"current_survey_sections":"[Fall Census]", "prior_survey_sections":['PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER'], "prior_2_survey_sections": ['PRIOR YEAR 2 COHORT', 'PRIOR YEAR 2 PRIOR SUMMER']}
    default_info_in = {"survey_year_iris": var_surveyYear, "survey_ver_id": var_surveyId, "survey_type": var_surveyType}
    default_values = get_survey_default_values(default_info_in)
    reporting_period_output = reporting_periods(sql_context,survey_info_in,default_values,ipeds_reporting_period_in,academic_term_in)
    reporting_period_output.show()
#In Progress
def test_campus_mcr(sql_context):
    print('In Progress')
#In Progress
def test_financial_aid_mcr(sql_context):
    print('In Progress')
#In Progress
def test_military_benefit_mcr(sql_context):
    print('In Progress')
#In Progress
def test_course_type_counts(sql_context):
    ipeds_reporting_period_in = sql_context.read.parquet('./tests/entities/IPEDSReportingPeriod.parquet')
    ipeds_client_config_in = sql_context.read.parquet('./tests/entities/IPEDSClientConfig.parquet')
    academic_term_in = sql_context.read.parquet('./tests/entities/AcademicTerm.parquet')
    survey_info_in = {"survey_year_doris": var_surveyYear, "survey_id": var_surveyId, "survey_type": var_surveyType}
    default_info_in = {"survey_year_iris": var_surveyYear, "survey_ver_id": var_surveyId, "survey_type": var_surveyType}
    default_values = get_survey_default_values(default_info_in)
    course_type_counts(sql_context,survey_info_in,default_values,ipeds_client_config_in,academic_term_in,)

def test_military_benefit_mcr(sql_context):
    print('In Progress')




