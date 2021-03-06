import boto3
from common import s3_utility
from .Mock_data import create_mock_data
import os
import shutil
import pytest
import yaml
import csv


entity_list =["AcademicTerm","AcademicTrack","Admission","Award","Campus","ChartOfAccounts","CohortExclusion","Course","CourseSection","CourseSectionSchedule","Degree","DegreeProgram","Employee","EmployeeAssignment","EmployeePosition","Faculty","FacultyAppointment","FieldOfStudy","FinancialAid","FiscalYear","GeneralLedgerReporting","InstitCharDoctorate","InstitCharUndergradGrad","InstructionalAssignment","InterlibraryLoanStatistic","IPEDSClientConfig","IPEDSReportingPeriod","LibraryBranch","LibraryCirculationStatistic","LibraryCollectionStatistic","LibraryExpenses","LibraryInventory","LibraryItemTransaction","MilitaryBenefit","OperatingLedgerReporting","Person","Registration","Student","TestScore","Transfer"]
var_surveyId = 'E1D'  # survey_id_map[args['survey_type']]
var_surveyType = '12ME'
var_surveyYear = '2021'
var_repPeriodTag1 = 'Academic Year End'
var_repPeriodTag2 = 'June End'
var_repPeriodTag3 = 'Fall Census'
var_repPeriodTag4 = 'Fall Census'
var_repPeriodTag5 = 'Fall Census'
count_num = 5

def test_check_model_verion():
    entity_list_versions = {}
    actual_entity_versions = []

    # for each entity
    for i in entity_list:
        with open(f"./tests/Data_Model/{i}.yml") as f:  # relative path here
            doris_entity_yml = yaml.load(f, Loader=yaml.FullLoader)
        dm_version = doris_entity_yml['version']

        print(f'Checking {i} for data model changes...')
        entity_list_versions.update({i:dm_version})

    # assert the DM looks correct
    with open('./tests/entities/mock_data_dm_versions.csv', 'r') as infile:
        dict_reader = csv.DictReader(infile)
        entity_list_versions_expected = list(dict_reader)

    actual_entity_versions.append(entity_list_versions)

    if actual_entity_versions != entity_list_versions_expected:
        print('Creating updated mock data...')
        test_mock_data_creation()

        print('Writing new data model version file...')
        with open('./tests/entities/mock_data_dm_versions.csv', 'w') as f:  # You will need 'wb' mode in Python 2.x
            w = csv.DictWriter(f, entity_list_versions.keys())
            w.writeheader()
            w.writerow(entity_list_versions)

    # if it doesnt match, update the version file after updating entities
    else:
        print('No data model updates')

    assert actual_entity_versions == entity_list_versions_expected

def test_mock_data_creation():
    for i in entity_list:

        entity = i
        # Unsure where this gets brought in from - need to see if this gets brought in from anywhere else.
        surveySectionValues="COHORT"

        if os.path.isdir(f'./tests/entities/{i}.parquet') == True:
            print(f'Removing parquet files for {i}...')
            shutil.rmtree(f'./tests/entities/{i}.parquet')
        else:
            print(f'No files exist for {i}')

        print(f'Creating mock {i} data...')

        mock_data = create_mock_data(
            doris_entity_name=i,
            record_count=count_num,
            entity_data_override={'surveyCollectionYear': var_surveyYear, 'surveyId': var_surveyId,
                                  'surveySection': surveySectionValues},
            entity_data_override_type='R',
            # entity_metadata_override={}
        )

        mock_data.write.format('parquet').option("header", "true").save(f'./tests/entities/{i}.parquet')


#TODO: Get the s3 upload working
def skip_upload(sql_context):
    role_info = {
        'RoleArn': 'arn:aws:iam::102184641170:role/developer',
        'RoleSessionName': 'test_session'
    }

    sts = boto3.client('sts')
    credentials = sts.assume_role(**role_info)

    session = boto3.session.Session(
        aws_access_key_id=credentials['Credentials']['AccessKeyId'],
        aws_secret_access_key=credentials['Credentials']['SecretAccessKey'],
        aws_session_token=credentials['Credentials']['SessionToken']
    )
    print(session)
    surveySectionValues='COHORT'
    for i in entity_list:
        mock_data = create_mock_data(
            doris_entity_name=i,
            record_count=count_num,
            entity_data_override={'surveyCollectionYear': var_surveyYear, 'surveyId': var_surveyId,
                                  'surveySection': surveySectionValues},
            entity_data_override_type='R',
            # entity_metadata_override={}
        )

        s3_utility.write_dataframe_as_parquet_to_s3(mock_data, 's3a://testing-bucket-qa/iris-report-query-mock-data/', mode='overwrite',file_format="parquet")
        # mock_data.write.parquet("s3a://testing-bucket-qa/iris-report-query-mock-data/")
        # s3 = boto3.client('s3')
        # s3.upload_file(f'./tests/entities/{i}.parquet', 'testing-bucket-qa', f'/iris-report-query-mock-data/{i}.parquet')


