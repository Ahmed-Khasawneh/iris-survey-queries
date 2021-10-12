from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pytest

entity_list =["AcademicTerm","AcademicTrack","Admission","Award","Campus","ChartOfAccounts","CohortExclusion","Course","CourseSection","CourseSectionSchedule","Degree","DegreeProgram","Employee","EmployeeAssignment","EmployeePosition","Faculty","FacultyAppointment","FieldOfStudy","FinancialAid","FiscalYear","GeneralLedgerReporting","InstitCharDoctorate","InstitCharUndergradGrad","InstructionalAssignment","InterlibraryLoanStatistic","IPEDSClientConfig","IPEDSReportingPeriod","LibraryBranch","LibraryCirculationStatistic","LibraryCollectionStatistic","LibraryExpenses","LibraryInventory","LibraryItemTransaction","MilitaryBenefit","OperatingLedgerReporting","Person","Registration","Student","TestScore","Transfer"]

@pytest.fixture(scope='session')
def sql_context():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Local Testing") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


    for i in entity_list:
        parquetFile = spark.read.parquet(f'./tests/entities/{i}.parquet')
        if i in ['IPEDSClientConfig']:
            parquetFile.createOrReplaceTempView(f'{i[0:5].lower() + i[5:]}')
            print(f'{i[0:5].lower() + i[5:]}')
            # ipeds_client_config_in = spark.sql('select * from ipedsClientConfig')
            # ipeds_client_config_in.show()
        else:
            parquetFile.createOrReplaceTempView(f'{i[0].lower() + i[1:]}')


    sql_context = SQLContext(spark)

    yield sql_context
    spark.stop()
