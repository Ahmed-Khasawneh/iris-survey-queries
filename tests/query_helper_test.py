import pandas as pd

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


def test_ipeds_client_config_mcr(sql_context):
    expected = "[Row(acadOrProgReporter='', admAdmissionTestScores='', admCollegePrepProgram='',  admDemoOfCompetency='', admOtherTestScores='', admRecommendation='', admSecSchoolGPA='', admSecSchoolRank='', admSecSchoolRecord='', admTOEFL='', admUseForBothSubmitted='', admUseForMultiOfSame='', admUseTestScores='', compGradDateOrTerm='', eviReserved1='', eviReserved2='', eviReserved3='', eviReserved4='', eviReserved5='', feIncludeOptSurveyData='', finAthleticExpenses='', finBusinessStructure='', finEndowmentAssets='', finGPFSAuditOpinion='', finParentOrChildInstitution='', finPellTransactions='', finPensionBenefits='', finReportingModel='', finTaxExpensePaid='', fourYrOrLessInstitution='', genderForNonBinary='', genderForUnknown='', grReportTransferOut='', hrIncludeSecondarySalary='', icOfferDoctorAwardLevel='', icOfferGraduateAwardLevel='', icOfferUndergradAwardLevel='', includeNonDegreeAsUG='', instructionalActivityType='', ncBranchCode='', ncSchoolCode='', ncSchoolName='', publicOrPrivateInstitution='', recordActivityDate=None, sfaGradStudentsOnly='', sfaLargestProgCIPC='', sfaReportPriorYear='', sfaReportSecondPriorYear='', surveyCollectionYear='2021', tmAnnualDPPCreditHoursFTE='0', snapshotDate=None, tags=['Fall Census'], survey_id='E1D', survey_type='12ME', clientConfigRowNum=1)]"
    survey_info_in = {"survey_year_doris": var_surveyYear, "survey_id": var_surveyId, "survey_type": var_surveyType}
    ipeds_client_config_output = ipeds_client_config_mcr(sql_context, survey_info_in)
    actual = str(ipeds_client_config_output.collect())
    assert actual == expected

def test_ipeds_reporting_period_mcr(sql_context):
    expected = "[Row(termCode='202110', partOfTermCode='', snapshotDateTimestamp=None, snapshotDate=None, recordActivityDate=datetime.datetime(9999, 9, 9, 0, 0), surveyCollectionYear='2021', surveyId='E1D', surveyName='', surveySection='COHORT', yearType='CY', tags=['Fall Census'], ipedsRepPerRowNum=1)]"
    ipeds_reporting_period_in = sql_context.read.parquet('./tests/entities/IPEDSReportingPeriod.parquet')
    print('Testing ipeds_reporting_period_mcr expected output...')
    survey_info_in = {"survey_year_doris": var_surveyYear, "survey_id":var_surveyId, "survey_type":var_surveyType}
    default_info_in = {"survey_year_iris": var_surveyYear, "survey_ver_id": var_surveyId, "survey_type": var_surveyType}
    default_values = get_survey_default_values(default_info_in)
    ipeds_reporting_period_output = ipeds_reporting_period_mcr(sql_context, survey_info_in, default_values, ipeds_reporting_period_in)
    actual = str(ipeds_reporting_period_output.collect())
    assert actual == expected

def test_academic_term_mcr(sql_context):
    expected = "[Row(academicYear='', censusDate='0909999', endDate='0909999', financialAidYear='', termCode='202110', partOfTermCode='', snapshotDateTimestamp=None, snapshotDate=None, partOfTermCodeDescription='', requiredFTCreditHoursGR=0, requiredFTCreditHoursUG=0, requiredFTClockHoursUG=0, startDate='0909999', termClassification='Non-Standard Length', termCodeDescription='202110', termType='Summer', tags=['Fall Census'], acadTermRowNum=1, partOfTermOrder=1, termCodeOrder=1, maxCensus='0909999', minStart='0909999', maxEnd='0909999')]"
    actual = str(academic_term_mcr(sql_context).collect())
    assert actual == expected

def test_reporting_period(sql_context):
    expected = "[Row(yearType='CY', surveySection='COHORT', termCode='202110', partOfTermCode='', snapshotDateTimestamp=None, snapshotDate=None, tags=['Fall Census'], termCodeOrder=1, partOfTermOrder=1, fullTermOrder=2, maxCensus=None, minStart=None, maxEnd=None, censusDate=None, termClassification='Non-Standard Length', termType='Summer', startDate=None, endDate=None, requiredFTCreditHoursGR=0, requiredFTCreditHoursUG=0, requiredFTClockHoursUG=0, financialAidYear='', dummyDate=datetime.date(9999, 9, 9), snapShotMaxDummyDate=datetime.date(9999, 9, 9), snapShotMinDummyDate=datetime.date(1900, 9, 9), equivCRHRFactor=1.0, rowNum=1, maxSummerTerm=1, maxFallTerm=None, termTypeNew='Post-Spring Summer')]"
    survey_info_in = {"survey_year_doris": var_surveyYear, "survey_id": var_surveyId, "survey_type": var_surveyType}
    survey_tags_in = {"current_survey_sections":"[Fall Census]", "prior_survey_sections":['PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER'], "prior_2_survey_sections": ['PRIOR YEAR 2 COHORT', 'PRIOR YEAR 2 PRIOR SUMMER']}
    default_info_in = {"survey_year_iris": var_surveyYear, "survey_ver_id": var_surveyId, "survey_type": var_surveyType}
    default_values = get_survey_default_values(default_info_in)
    actual = str(reporting_periods(sql_context,survey_info_in,default_values).collect())
    assert expected == actual

def test_campus_mcr(sql_context):
    expected = "[Row(campus='', campusDescription='', isInternational=True, campusSnapshotDateTimestamp='0909999', campusSnapshotDate=None, snapshotDateFilter=None, useSnapshotDatePartition='0909999')]"
    actual = str(campus_mcr(sql_context).collect())
    assert actual == expected
#In Progress
def test_financial_aid_mcr(sql_context):
    default_info_in = {"survey_year_iris": var_surveyYear, "survey_ver_id": var_surveyId, "survey_type": var_surveyType}
    default_values = get_survey_default_values(default_info_in)
    survey_info_in = {"survey_year_doris": var_surveyYear, "survey_id": var_surveyId, "survey_type": var_surveyType}
    ipeds_client_config = ipeds_client_config_mcr(sql_context, survey_info_in)
    all_academic_terms = academic_term_mcr(sql_context)
    reporting_period_terms = reporting_periods(sql_context, survey_info_in, default_values)
    course_counts = course_type_counts(sql_context, survey_info_in, default_values)
    cohort_df = student_cohort(sql_context, survey_info_in=survey_info_in, default_values_in=default_values,
                                ipeds_client_config_in=ipeds_client_config, academic_term_in=all_academic_terms,
                                reporting_periods_in=reporting_period_terms,
                                course_type_counts_in=course_counts)
    actual = str(financial_aid_mcr(sql_context,default_values,cohort_df,ipeds_client_config).collect())

def test_military_benefit_mcr(sql_context):
    expected = "[Row(studentLevel=1, studentCount=1, benefitAmount=None)]"
    default_info_in = {"survey_year_iris": var_surveyYear, "survey_ver_id": var_surveyId, "survey_type": var_surveyType}
    default_values = get_survey_default_values(default_info_in)
    actual = str(military_benefit_mcr(sql_context,default_values, 'GI Bill').collect())
    assert actual == expected

def test_course_type_counts(sql_context):
    expected = "[Row(regPersonId='12345', repRefYearType='CY', repRefSurveySection='COHORT', regTermCode='202110', repRefMaxCensus=None, totalCourses=1, totalCreditCourses=1, totalCreditHrs=0.0, totalClockHrs=0.0, totalCECourses=0, totalSAHomeCourses=0, totalESLCourses=1, totalRemCourses=1, totalIntlCourses=1, totalAuditCourses=1, totalThesisCourses=0, totalProfResidencyCourses=0, totalDECourses=0, UGCreditHours=0.0, UGClockHours=0.0, GRCreditHours=0.0, DPPCreditHours=0.0)]"
    survey_info_in = {"survey_year_doris": var_surveyYear, "survey_id": var_surveyId, "survey_type": var_surveyType}
    default_info_in = {"survey_year_iris": var_surveyYear, "survey_ver_id": var_surveyId, "survey_type": var_surveyType}
    default_values = get_survey_default_values(default_info_in)
    actual = str(course_type_counts(sql_context,survey_info_in,default_values).collect())
    assert actual == expected
# Empty dataframe is result
def test_student_cohort(sql_context):
    expected = ""
    default_info_in = {"survey_year_iris": var_surveyYear, "survey_ver_id": var_surveyId, "survey_type": var_surveyType}
    default_values = get_survey_default_values(default_info_in)
    survey_info_in = {"survey_year_doris": var_surveyYear, "survey_id": var_surveyId, "survey_type": var_surveyType}
    ipeds_client_config = ipeds_client_config_mcr(sql_context, survey_info_in)
    all_academic_terms = academic_term_mcr(sql_context)
    reporting_period_terms = reporting_periods(sql_context,survey_info_in,default_values)
    course_counts = course_type_counts(sql_context,survey_info_in,default_values)
    actual = str(student_cohort(sql_context, survey_info_in=survey_info_in, default_values_in=default_values,
                                ipeds_client_config_in=ipeds_client_config, academic_term_in=all_academic_terms,
                                reporting_periods_in=reporting_period_terms,
                                course_type_counts_in=course_counts).cache().collect())
    print(actual)
    # assert actual == expected





