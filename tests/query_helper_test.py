from common.query_helpers import *
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

def test_ipeds_reporting_period_mcr(sql_context):
    print('Creating mock IPEDSReportingPeriod data...')

    # calls data mock to create test data
    entity = '' \
             ''
    count_num = 1
    surveySectionValues = 'COHORT'

    ipeds_reporting_period_in = create_mock_data(
        doris_entity_name=entity,
        record_count=count_num,
        entity_data_override={'surveyCollectionYear': var_surveyYear, 'surveyId': var_surveyId,
                              'surveySection': surveySectionValues},
        entity_data_override_type='R',
        # entity_metadata_override={}
    )



    print('Testing ipeds_reporting_period_mcr expected output...')
    survey_info_in = {"survey_year_doris":var_surveyYear, "survey_id":var_surveyId, "survey_type":var_surveyType}
    survey_tags_in = {"current_survey_sections":"Fall Census","prior_survey_sections":['PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER'],"prior_2_survey_sections":['PRIOR YEAR 2 COHORT', 'PRIOR YEAR 2 PRIOR SUMMER']}
    # survey_tags_in = get_survey_tags(survey_info_in)
    ipeds_reporting_period_in.show()
    ipeds_reporting_period_in.createOrReplaceTempView("ipedsReportingPeriod")


    ipeds_reporting_period_output = ipeds_reporting_period_mcr(sql_context, survey_info_in, ipeds_reporting_period_in, survey_tags_in)
    ipeds_reporting_period_output_collect = str(ipeds_reporting_period_output.collect())
    ipeds_reporting_period_expected_output = "[Row(partOfTermCode='', recordActivityDate=None, surveyCollectionYear='2021', surveyId='E1D', surveyName='', surveySection='COHORT', termCode='202110', snapshotDate=None, yearType='CY', tags=['Fall Census'], ipedsRepPerRowNum=1)]"
    assert ipeds_reporting_period_output == ipeds_reporting_period_expected_output






