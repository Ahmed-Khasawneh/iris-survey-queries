import sys
from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext, types as T, functions as f, SparkSession
from pyspark.sql.functions import sum as _sum, expr, col, lit
from awsglue.utils import getResolvedOptions
from common import query_helpers
import pandas as pd  # todo: replace pandas with pyspark
import re
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from uuid import uuid4


def run_twelve_month_enrollment_query(options, spark, sparkContext):
    OUTPUT_BUCKET = 'doris-survey-reports-dev'
    S3_URI_REGEX = re.compile(r"s3://([^/]+)/?(.*)")

    survey_id_map = {
        'TWELVE_MONTH_ENROLLMENT_1': 'E1D',
        'TWELVE_MONTH_ENROLLMENT_2': 'E12',
        'TWELVE_MONTH_ENROLLMENT_3': 'E1E',
        'TWELVE_MONTH_ENROLLMENT_4': 'E1F'
    }

    var_surveyId = survey_id_map[options['surveyType']]
    var_surveyType = '12ME'
    var_repEndTag = 'June End'
    var_surveyYear = '2021'


	############
	
	ipeds_client_config_partition = "surveyCollectionYear"
	ipeds_client_config_order = f"""
		((case when array_contains(tags, '{repPeriodTag1}') then 1
			 when array_contains(tags, '{repPeriodTag2}') then 2
			 else 3 end) asc,
		snapshotDate desc,
		coalesce(recordActivityDate, CAST('9999-09-09' as DATE)) desc)
		 """
	ipeds_client_config_partition_filter = f"surveyCollectionYear = '{surveyYear}'"  # f"surveyId = '{var_surveyId}' and surveyYear = '{surveyYear}"
	
	############
	
	ipeds_reporting_period_partition = "surveyCollectionYear, surveyId, surveySection, termCode, partOfTermCode"
	ipeds_reporting_period_order = f"""
		((case when array_contains(tags, '{repPeriodTag1}') then 1
			 when array_contains(tags, '{repPeriodTag2}') then 2
			 else 3 end) asc,
		snapshotDate desc,
		coalesce(recordActivityDate, CAST('9999-09-09' as DATE)) desc)
		 """
	ipeds_reporting_period_partition_filter = f"surveyId = '{var_surveyId}'"
	
	############
	
	academic_term_partition = "termCode, partOfTermCode"
	academic_term_order = "(snapshotDate desc, recordActivityDate desc)"
	academic_term_partition_filter = "coalesce(isIpedsReportable, true) = true"
	
	############
	
	
	ipeds_client_config = ipeds_client_config_mcr(ipeds_client_config_partition=ipeds_client_config_partition,
												  ipeds_client_config_order=ipeds_client_config_order,
												  ipeds_client_config_partition_filter=ipeds_client_config_partition_filter).cache()
	
	academic_term = academic_term_mcr(academic_term_partition=academic_term_partition,
									  academic_term_order=academic_term_order,
									  academic_term_partition_filter=academic_term_partition_filter).cache()
	
	academic_term_reporting_refactor = academic_term_reporting_refactor(
		ipeds_reporting_period_partition=ipeds_reporting_period_partition,
		ipeds_reporting_period_order=ipeds_reporting_period_order,
		ipeds_reporting_period_partition_filter=ipeds_reporting_period_partition_filter,
		academic_term_partition=academic_term_partition,
		academic_term_order=academic_term_order,
		academic_term_partition_filter=academic_term_partition_filter).cache()
	
	course_type_counts = ipeds_course_type_counts()
	
	cohort = ipeds_cohort()
	
	cohort_out = cohort.select(
		cohort["*"],
		expr("""
		case when studentLevelUGGR = 'GR' then '99'
			 when isNonDegreeSeeking_calc = 1 and timeStatus_calc = 'Full Time' then '7'
			 when isNonDegreeSeeking_calc = 1 and timeStatus_calc = 'Part Time' then '21'
			 when studentType_calc = 'First Time' and timeStatus_calc = 'Full Time' then '1'
			 when studentType_calc = 'Transfer' and timeStatus_calc = 'Full Time' then '2'
			 when studentType_calc = 'Continuing' and timeStatus_calc = 'Full Time' then '3'
			 when studentType_calc = 'First Time' and timeStatus_calc = 'Part Time' then '15'
			 when studentType_calc = 'Transfer' and timeStatus_calc = 'Part Time' then '16'
			 when studentType_calc = 'Continuing' and timeStatus_calc = 'Part Time' then '17'
			 else '1'
		end
		""").alias("ipedsPartAStudentLevel"),
		expr("""
		case when studentLevelUGGR = 'GR' then '3'
			 when isNonDegreeSeeking_calc = 1 then '2'
			 when studentLevelUGGR = 'UG' then '1'
			 else null
		end
		""").alias("ipedsPartCStudentLevel")
	).filter(cohort.ipedsInclude == 1)
	
	# CourseLevelCounts
	# course_type_counts = spark.sql(func_courseLevelCounts(repPeriod = 'global_reportingPeriodRefactor', termOrder = 'global_reportingPeriodOrder', instructionalActivityType = config_instructionalActivityType))
	
	course_type_counts_out = course_type_counts.join(
		cohort,
		(cohort.regPersonId == course_type_counts.regPersonId), 'inner').filter(cohort.ipedsInclude == 1).select(
		course_type_counts["*"]).agg(
		sum("UGCreditHours").alias("UGCreditHours"),
		sum("UGClockHours").alias("UGClockHours"),
		sum("GRCreditHours").alias("GRCreditHours"),
		sum("DPPCreditHours").alias("DPPCreditHours"))
	
	# Survey version output lists
	if var_surveyId == 'E1D':
		A_UgGrBoth = ["1", "2", "3", "7", "15", "16", "17", "21", "99"]
		A_UgOnly = ["1", "2", "3", "7", "15", "16", "17", "21"]
		A_GrOnly = ["99"]
		C_UgGrBoth = ["1", "2", "3"]
		C_UgOnly = ["1", "2"]
		C_GrOnly = ["3"]
	elif var_surveyId == 'E12':
		A_UgGrBoth = ["1", "2", "3", "7", "15", "16", "17", "21"]
		A_UgOnly = ["1", "2", "3", "7", "15", "16", "17", "21"]
		A_GrOnly = [""]
		C_UgGrBoth = ["1", "2"]
		C_UgOnly = ["1", "2"]
		C_GrOnly = [""]
	elif var_surveyId == 'E1E':
		A_UgGrBoth = ["1", "3", "7", "15", "17", "21"]
		A_UgOnly = ["1", "3", "7", "15", "17", "21"]
		A_GrOnly = [""]
		C_UgGrBoth = ["1", "2"]
		C_UgOnly = ["1", "2"]
		C_GrOnly = [""]
	else:  # V4
		A_UgGrBoth = ["1", "3", "15", "17"]
		A_UgOnly = ["1", "3", "15", "17"]
		A_GrOnly = [""]
		C_UgGrBoth = ["1", "2", "3"]
		C_UgOnly = ["1", "2"]
		C_GrOnly = ["3"]
	
	# Part A
	if cohort_out.rdd.isEmpty() == False:
		# FormatPartA
		a_columns = ["regPersonId", "ipedsPartAStudentLevel", "persIpedsEthnValue", "persIpedsGender"]
		a_data = [("", "1", "", ""), ("", "2", "", ""), ("", "3", "", ""), ("", "7", "", ""), ("", "15", "", ""),
				  ("", "16", "", ""), ("", "17", "", ""), ("", "21", "", ""), ("", "99", "", "")]
		FormatPartA = sparkContext.parallelize(a_data)
		FormatPartA = spark.createDataFrame(FormatPartA).toDF(*a_columns)
	
		partA_out = cohort_out.select("regPersonId", "ipedsPartAStudentLevel", "persIpedsEthnValue",
									  "persIpedsGender").filter(
			(cohort_out.ipedsPartAStudentLevel.isNotNull()) &
			(cohort_out.ipedsPartAStudentLevel != '')).union(FormatPartA)
	
		partA_out = partA_out.select(
			partA_out.ipedsPartAStudentLevel.alias("field1"),
			when(((col('persIpedsEthnValue') == lit('1')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
				lit('0')).alias("field2"),  # FYRACE01 - Nonresident alien - Men (1), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('1')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
				lit('0')).alias("field3"),  # FYRACE02 - Nonresident alien - Women (2), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('2')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
				lit('0')).alias("field4"),  # FYRACE25 - Hispanic/Latino - Men (25), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('2')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
				lit('0')).alias("field5"),  # FYRACE26 - Hispanic/Latino - Women (26), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('3')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
				lit('0')).alias("field6"),  # FYRACE27 - American Indian or Alaska Native - Men (27), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('3')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
				lit('0')).alias("field7"),  # FYRACE28 - American Indian or Alaska Native - Women (28), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('4')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
				lit('0')).alias("field8"),  # FYRACE29 - Asian - Men (29), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('4')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
				lit('0')).alias("field9"),  # FYRACE30 - Asian - Women (30), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('5')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
				lit('0')).alias("field10"),  # FYRACE31 - Black or African American - Men (31), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('5')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
				lit('0')).alias("field11"),  # FYRACE32 - Black or African American - Women (32), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('6')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
				lit('0')).alias("field12"),  # FYRACE33 - Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('6')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
				lit('0')).alias("field13"),
			# FYRACE34 - Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('7')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
				lit('0')).alias("field14"),  # FYRACE35 - White - Men (35), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('7')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
				lit('0')).alias("field15"),  # FYRACE36 - White - Women (36), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('8')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
				lit('0')).alias("field16"),  # FYRACE37 - Two or more races - Men (37), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('8')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
				lit('0')).alias("field17"),  # FYRACE38 - Two or more races - Women (38), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('9')) & (col('persIpedsGender') == lit('M'))), lit('1')).otherwise(
				lit('0')).alias("field18"),  # FYRACE13 - Race and ethnicity unknown - Men (13), 0 to 999999
			when(((col('persIpedsEthnValue') == lit('9')) & (col('persIpedsGender') == lit('F'))), lit('1')).otherwise(
				lit('0')).alias("field19"))  # FYRACE14 - Race and ethnicity unknown - Women (14), 0 to 999999
	
		partA_out = partA_out.withColumn('part', f.lit('A')).groupBy("part", "field1").agg(
			sum("field2").alias("field2"),
			sum("field3").alias("field3"),
			sum("field4").alias("field4"),
			sum("field5").alias("field5"),
			sum("field6").alias("field6"),
			sum("field7").alias("field7"),
			sum("field8").alias("field8"),
			sum("field9").alias("field9"),
			sum("field10").alias("field10"),
			sum("field11").alias("field11"),
			sum("field12").alias("field12"),
			sum("field13").alias("field13"),
			sum("field14").alias("field14"),
			sum("field15").alias("field15"),
			sum("field16").alias("field16"),
			sum("field17").alias("field17"),
			sum("field18").alias("field18"),
			sum("field19").alias("field19")
		)
	
	else:
		a_columns = ["part", "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9",
					 "field10", "field11", "field12", "field13", "field14", "field15", "field16", "field17", "field18",
					 "field19"]
		a_data = [("A", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
				  ("A", "2", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
				  ("A", "3", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
				  ("A", "7", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
				  ("A", "15", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
				  ("A", "16", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
				  ("A", "17", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
				  ("A", "21", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
				  ("A", "99", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")]
	
		partA_out = sparkContext.parallelize(a_data)
		partA_out = spark.createDataFrame(partA_out).toDF(*a_columns)
	
	# Part A output filter
	partA_out = partA_out.crossJoin(ipeds_client_config).filter(
		(((col('icOfferUndergradAwardLevel') == 'Y') & (col('icOfferGraduateAwardLevel') == 'Y') & (
			col('field1').isin(A_UgGrBoth)))
		 | ((col('icOfferUndergradAwardLevel') == 'Y') & (col('icOfferGraduateAwardLevel') == 'N') & (
					col('field1').isin(A_UgOnly)))
		 | ((col('icOfferUndergradAwardLevel') == 'N') & (col('icOfferGraduateAwardLevel') == 'Y') & (
					col('field1').isin(A_GrOnly)))
		 | ((col('icOfferUndergradAwardLevel') == 'N') & (col('icOfferGraduateAwardLevel') == 'N')))).select(partA_out['*'])
	
	# Part C
	if cohort_out.rdd.isEmpty() == False:
		# FormatPartC
		c_columns = ["regPersonId", "ipedsLevel", "distanceEdInd"]
		c_data = [("", "1", ""), ("", "2", ""), ("", "3", "")]
		FormatPartC = sparkContext.parallelize(c_data)
		FormatPartC = spark.createDataFrame(FormatPartC).toDF(*c_columns)
	
		# Part C
		partC_out = cohort_out.select("regPersonId", "ipedsPartCStudentLevel", "distanceEdInd_calc").filter(
			(cohort_out.ipedsPartCStudentLevel.isNotNull()) & (cohort_out.ipedsPartCStudentLevel != '') & (
						cohort_out.distanceEdInd_calc != 'None')).union(FormatPartC)
		partC_out = partC_out.select(
			partC_out.ipedsPartCStudentLevel.alias("field1"),
			when((col('distanceEdInd_calc') == lit('Exclusive DE')), lit('1')).otherwise(lit('0')).alias("field2"),
			# Enrolled exclusively in distance education courses
			when((col('distanceEdInd_calc') == lit('Some DE')), lit('1')).otherwise(lit('0')).alias(
				"field3"))  # Enrolled in at least one but not all distance education courses
	
		partC_out = partC_out.withColumn('part', f.lit('C')).groupBy("part", "field1").agg(
			sum("field2").alias("field2"),
			sum("field3").alias("field3"))
	
	else:
		c_columns = ["part", "field1", "field2", "field3"]
		c_data = [("C", "1", "0", "0"), ("C", "2", "0", "0"), ("C", "3", "0", "0")]
		partC_out = sparkContext.parallelize(c_data)
		partC_out = spark.createDataFrame(partC_out).toDF(*c_columns)
	
	# Part C output filter
	partC_out = partC_out.crossJoin(ipeds_client_config).filter(
		(((col('icOfferUndergradAwardLevel') == 'Y') & (col('icOfferGraduateAwardLevel') == 'Y') & (
			col('field1').isin(C_UgGrBoth)))
		 | ((col('icOfferUndergradAwardLevel') == 'Y') & (col('icOfferGraduateAwardLevel') == 'N') & (
					col('field1').isin(C_UgOnly)))
		 | ((col('icOfferUndergradAwardLevel') == 'N') & (col('icOfferGraduateAwardLevel') == 'Y') & (
					col('field1').isin(C_GrOnly)))
		 | ((col('icOfferUndergradAwardLevel') == 'N') & (col('icOfferGraduateAwardLevel') == 'N')))).select(partC_out['*'])
	
	# Part B
	if course_type_counts_out.rdd.isEmpty() == False:
		partB_out = course_type_counts_out.crossJoin(ipeds_client_config).withColumn('part', f.lit('B')).select(
			'part',
			# CREDHRSU - credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.  
			when(((col('icOfferUndergradAwardLevel') == lit('Y')) & (col('instructionalActivityType') == lit('CL'))),
				 f.coalesce(col('UGCreditHours'), lit(0))).alias('field2'),
			# CONTHRS  - clock hour instructional activity at the undergraduate level, 0 to 9999999, blank = not applicable, if no undergraduate programs are measured in clock hours.
			when(((col('icOfferUndergradAwardLevel') == lit('Y')) & (col('instructionalActivityType') == lit('CR'))),
				 f.coalesce(col('UGClockHours'), lit(0))).alias('field3'),
			# CREDHRSG - credit hour instructional activity at the graduate level, 0 to 99999999, blank = not applicable
			expr(
				f"round((case when icOfferGraduateAwardLevel = 'Y' and '{var_surveyId}' = 'E1D' then coalesce(GRCreditHours, 0) else null end))").cast(
				'int').alias("field4"),
			# RDOCFTE  - reported Doctor'92s degree-professional practice student FTE, 0 to 99999999, blank = not applicable
			expr(f"""round((case when icOfferDoctorAwardLevel = 'Y' and '{var_surveyId}' = 'E1D' then 
								(case when coalesce(DPPCreditHours, 0) > 0 then coalesce(cast(round(DPPCreditHours / tmAnnualDPPCreditHoursFTE, 0) as string), '0') 
							else '0' end) else null end))""").cast('int').alias("field5"))
	
	else:
		b_columns = ["part", "field2", "field3", "field4", "field5"]
		b_data = [("B", "0", "", "0", "0")]
		partB_out = sparkContext.parallelize(b_data)
		partB_out = spark.createDataFrame(partB_out).toDF(*b_columns)
	
	# Survey out formatting
	for column in [column for column in partB_out.columns if column not in partA_out.columns]:
		partA_out = partA_out.withColumn(column, lit(None))
	
	for column in [column for column in partA_out.columns if column not in partC_out.columns]:
		partC_out = partC_out.withColumn(column, lit(None))
	
	for column in [column for column in partA_out.columns if column not in partB_out.columns]:
		partB_out = partB_out.withColumn(column, lit(None))
	
	surveyOutput = partA_out.unionByName(partC_out).unionByName(partB_out)
	
	# surveyOutput = create_json_format(surveyOutput)
	# write_dataframe_as_json_to_s3(surveyOutput.repartition(1), s3_path, constants.SPARK_OVERWRITE_MODE, 'json')
	
	# global_courseLevelCounts.show()
	surveyOutput.show()
	
	return surveyOutput
