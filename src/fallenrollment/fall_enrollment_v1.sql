/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Spring Collection
FILE NAME:      Fall Enrollment v1 (EF1)
FILE DESC:      Fall Enrollment for 4-year, degree-granting institutions
AUTHOR:         Ahmed Khasawneh
CREATED:        20200103

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Cohort Creation
Retention Cohort Creation
Student-to-Faculty Ratio Calculation  
Formatting Views
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)   Author             	Tag             	Comments
----------- 	--------------------	-------------   	-------------------------------------------------
20200519		akhasawneh				ak 20200519			Changes for credit hours and FT/PT determination (PF-1480) -Run time 12m 24s
20200429		akhasawneh				ak 20200419			Adding study abroad considerations to cohort and retention inclusions (PF-1435) -Run time 11m 23s
20200427 		akhasawneh 				ak 20200427			Adding Degree entity to filter retention cohort on Degree.awardLevel (PF-1434)
20200423 		jhanicak 				jh 20200423			Fixed survey part levels PF-1436
20200422 		jhanicak 				jh 20200422			Added default values PF-1403 and misc changes/fixes
20200421 		akhasawneh      		ak 20200421			Modification to use 'isInUSOnVisa' and 'isUSCitizen' for ethnicity
															assignments.(PF-1417)
20200413 		akhasawneh 				ak 20200413			Moving all formatting to the 'Formatting Views' section. (PF-1371)
20200410 		akhasawneh      		ak 20200410			Reordering entities and modifying partition fields to increase performance. (PF-1404 & PF-1406)
20200406 		akhasawneh      		ak 20200406			Including dummy date changes. (PF-1368)
20200330 		akhasawneh 				ak 20200330			Including changes for PF-1253, PF-1320, PF-1382 and PF-1317
20200319    	jhanicak/akhasawneh 						Major rework of report query
20200122    	akhasawneh      		ak 20200122			Modified part B&C logic to prevent validation failure if an 
															IPEDSReportingPeriod record is not present
20200106    	akhasawneh 									Move original code to template 
20200103    	akhasawneh 									Initial version 
	
********************/

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

--jh 20200422 Added default values

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.

select '1920' surveyYear, 
        CAST('2019-08-01' AS TIMESTAMP) startDateProg,
        CAST('2019-10-31' AS TIMESTAMP) endDateProg, 
        'EF1' surveyId,
		'Fall' sectionFall,
		'Retention Fall' sectionRetFall,
		'Summer' sectionSummer,
		'Retention Summer' sectionRetSummer,
		'202010' termCodeFall, 
		'201930' termCodeSummer, 
		'201910' termCodeRetFall, 
		'201830' termCodeRetSummer, 
		CAST('2019-10-15' AS TIMESTAMP) censusDateFall,
		CAST('2019-07-31' AS TIMESTAMP) censusDateSummer, 
		CAST('2018-10-15' AS TIMESTAMP) censusDateRetFall, 
		CAST('2018-07-31' AS TIMESTAMP) censusDateRetSummer,
		'1' partOfTermCode,
		'HR%' surveyIdHR,
		CAST('2019-11-01' AS TIMESTAMP) asOfDateHR, 
        'A' acadOrProgReporter, --A = Academic, P = Program
		'Y' feIncludeOptSurveyData, --Y = Yes, N = No
		'Y' includeNonDegreeAsUG, --Y = Yes, N = No
		'M' genderForUnknown, --M = Male, F = Female
		'F' genderForNonBinary  --M = Male, F = Female

--Use for testing internally only
/*
select '1516' surveyYear, --testing '1516'
        CAST('2015-08-01' AS TIMESTAMP) startDateProg,
        CAST('2015-10-31' AS TIMESTAMP) endDateProg, 
        'EF1' surveyId,
		'Fall' sectionFall,
		'Retention Fall' sectionRetFall,
		'Summer' sectionSummer,
		'Retention Summer' sectionRetSummer,
		'201610' termCodeFall,
		'201530' termCodeSummer,
		'201510' termCodeRetFall,
		'201430' termCodeRetSummer,
		CAST('2015-10-15' AS TIMESTAMP) censusDateFall,
		CAST('2015-07-31' AS TIMESTAMP) censusDateSummer, 
		CAST('2014-10-15' AS TIMESTAMP) censusDateRetFall, 
		CAST('2014-07-31' AS TIMESTAMP) censusDateRetSummer,
		'1' partOfTermCode,
		'HR%' surveyIdHR,
		CAST('2015-11-01' AS TIMESTAMP) asOfDateHR, 
        'A' acadOrProgReporter, --A = Academic, P = Program
		'Y' feIncludeOptSurveyData, --Y = Yes, N = No
		'Y' includeNonDegreeAsUG, --Y = Yes, N = No
		'M' genderForUnknown, --M = Male, F = Female
		'F' genderForNonBinary  --M = Male, F = Female
*/
),

--jh 20200422 Added default values to ConfigPerAsOfDate and moved before ReportingPeriod

ConfigPerAsOfDate as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

select ConfigLatest.surveyYear surveyYear,
			ConfigLatest.acadOrProgReporter acadOrProgReporter,
			case when mod(ConfigLatest.surveyYear,2) = 0 then 'Y' --then odd first year and age is required
				 else ConfigLatest.feIncludeOptSurveyData
			end reportAge,
			case when mod(ConfigLatest.surveyYear,2) != 0 then 'Y' --then even first year and resid is required
				 else ConfigLatest.feIncludeOptSurveyData
			end reportResidency,
			ConfigLatest.feIncludeOptSurveyData feIncludeOptSurveyData,
			ConfigLatest.includeNonDegreeAsUG includeNonDegreeAsUG,
			ConfigLatest.sfaLargestProgCIPC sfaLargestProgCIPC,
			ConfigLatest.genderForUnknown genderForUnknown,
			ConfigLatest.genderForNonBinary genderForNonBinary,
			ConfigLatest.surveyId surveyId,
		    ConfigLatest.sectionFall sectionFall,
		    ConfigLatest.sectionRetFall sectionRetFall,
		    ConfigLatest.sectionSummer sectionSummer,
		    ConfigLatest.sectionRetSummer sectionRetSummer,
		    ConfigLatest.termCodeFall termCodeFall, 
		    ConfigLatest.termCodeSummer termCodeSummer, 
		    ConfigLatest.termCodeRetFall termCodeRetFall, 
		    ConfigLatest.termCodeRetSummer termCodeRetSummer, 
		    ConfigLatest.censusDateFall censusDateFall,
		    ConfigLatest.censusDateSummer censusDateSummer, 
		    ConfigLatest.censusDateRetFall censusDateRetFall, 
		    ConfigLatest.censusDateRetSummer censusDateRetSummer,
		    ConfigLatest.partOfTermCode partOfTermCode,
		    ConfigLatest.surveyIdHR surveyIdHR,
		    ConfigLatest.asOfDateHR asOfDateHR
from (
		select config.surveyCollectionYear surveyYear,
				nvl(config.acadOrProgReporter, defaultValues.acadOrProgReporter) acadOrProgReporter,
				nvl(config.feIncludeOptSurveyData, defaultValues.feIncludeOptSurveyData) feIncludeOptSurveyData,
				nvl(config.includeNonDegreeAsUG, defaultValues.includeNonDegreeAsUG) includeNonDegreeAsUG,
				case when nvl(config.acadOrProgReporter, defaultValues.acadOrProgReporter) = 'P' then config.sfaLargestProgCIPC else NULL end sfaLargestProgCIPC,
				nvl(config.genderForUnknown, defaultValues.genderForUnknown) genderForUnknown,
				nvl(config.genderForNonBinary, defaultValues.genderForNonBinary) genderForNonBinary,
				defaultValues.surveyId surveyId,
		        defaultValues.sectionFall sectionFall,
		        defaultValues.sectionRetFall sectionRetFall,
		        defaultValues.sectionSummer sectionSummer,
		        defaultValues.sectionRetSummer sectionRetSummer,
		        defaultValues.termCodeFall termCodeFall, 
		        defaultValues.termCodeSummer termCodeSummer, 
		        defaultValues.termCodeRetFall termCodeRetFall, 
		        defaultValues.termCodeRetSummer termCodeRetSummer, 
		        defaultValues.censusDateFall censusDateFall,
		        defaultValues.censusDateSummer censusDateSummer, 
		        defaultValues.censusDateRetFall censusDateRetFall, 
		        defaultValues.censusDateRetSummer censusDateRetSummer,
		        defaultValues.partOfTermCode partOfTermCode,
		        defaultValues.surveyIdHR surveyIdHR,
		        defaultValues.asOfDateHR asOfDateHR,
		        row_number() over (
					partition by
						config.surveyCollectionYear
					order by
						config.recordActivityDate desc
				) configRn
		from IPEDSClientConfig config
			cross join DefaultValues defaultValues
		where config.surveyCollectionYear = defaultValues.surveyYear
		
		union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
        select defaultValues.surveyYear surveyYear,
				defaultValues.acadOrProgReporter acadOrProgReporter,
				defaultValues.feIncludeOptSurveyData feIncludeOptSurveyData,
				defaultValues.includeNonDegreeAsUG includeNonDegreeAsUG,
				null sfaLargestProgCIPC,
				defaultValues.genderForUnknown genderForUnknown,
				defaultValues.genderForNonBinary genderForNonBinary,
				defaultValues.surveyId surveyId,
		        defaultValues.sectionFall sectionFall,
		        defaultValues.sectionRetFall sectionRetFall,
		        defaultValues.sectionSummer sectionSummer,
		        defaultValues.sectionRetSummer sectionRetSummer,
		        defaultValues.termCodeFall termCodeFall, 
		        defaultValues.termCodeSummer termCodeSummer, 
		        defaultValues.termCodeRetFall termCodeRetFall, 
		        defaultValues.termCodeRetSummer termCodeRetSummer, 
		        defaultValues.censusDateFall censusDateFall,
		        defaultValues.censusDateSummer censusDateSummer, 
		        defaultValues.censusDateRetFall censusDateRetFall, 
		        defaultValues.censusDateRetSummer censusDateRetSummer,
		        defaultValues.partOfTermCode partOfTermCode,
		        defaultValues.surveyIdHR surveyIdHR,
		        defaultValues.asOfDateHR asOfDateHR, 
				1 configRn
        from DefaultValues defaultValues
		where defaultValues.surveyYear not in (select config.surveyCollectionYear
												from IPEDSClientConfig config
												where config.surveyCollectionYear = defaultValues.surveyYear)
		) ConfigLatest
where ConfigLatest.configRn = 1
),

AcadTermLatestRecord as (
--Returns most recent (recordActivityDate) term code record for each term and part of term code. 
--PartOfTerm code defines a subcategory of a termCode that may have different start, end and census dates. 

select *
from ( 
		select acadTerm.*,
				row_number() over (
					partition by 
						acadTerm.termCode,
						acadTerm.partOfTermCode
					order by  
						acadTerm.recordActivityDate desc
					) acadTermRn
		from AcademicTerm acadTerm
		where acadTerm.isIPEDSReportable = 1 
	)
where acadTermRn = 1
),

--jh 20200422 Added default values from ConfigPerAsOfDate and reduced select statements

ReportingPeriod as (
--Returns applicable term/part of term codes for this survey submission year. 
--Need current survey terms ('Fall', 'Summer') and prior year retention ('RetFall', 'RetSummer')
--Introduces 'cohortInd' which is used through out the query to associate records with the appropriate cohort. 

select RepDates.surveyYear surveyYear,
			RepDates.surveySection cohortInd,
			RepDates.termCode termCode,	
			RepDates.partOfTermCode partOfTermCode,	
		    nvl(nvl(AcademicTerm.censusDate, DATE_ADD(AcademicTerm.startDate, 15)), RepDates.censusDate) censusDate,
			RepDates.acadOrProgReporter acadOrProgReporter,
		    RepDates.feIncludeOptSurveyData feIncludeOptSurveyData,
		    RepDates.includeNonDegreeAsUG includeNonDegreeAsUG,
		    RepDates.genderForUnknown genderForUnknown,
		    RepDates.genderForNonBinary genderForNonBinary,
-- ak 20200519 adding consideration for credit vs clock hour (PF-1480)
            NVL(AcademicTerm.requiredFTCreditHoursUG/
                NVL(AcademicTerm.requiredFTClockHoursUG, AcademicTerm.requiredFTCreditHoursUG), 1) equivCRHRFactor
from (
--Pulls reporting period and default configuration values for Fall, Summer, Retention Fall and Retention Summer from IPEDSReportingPeriod entity
	select surveyYear surveyYear,
			surveySection surveySection,
			termCode termCode,
			censusDate censusDate,
			partOfTermCode partOfTermCode,
			acadOrProgReporter acadOrProgReporter,
			feIncludeOptSurveyData feIncludeOptSurveyData,
			includeNonDegreeAsUG includeNonDegreeAsUG,
			genderForUnknown genderForUnknown,
			genderForNonBinary genderForNonBinary
	from (
		select ReportPeriod.surveyCollectionYear surveyYear,
				ReportPeriod.surveySection surveySection,
				ReportPeriod.termCode termCode,
				case when ReportPeriod.surveySection = defaultValues.sectionFall then defaultValues.censusDateFall
					 when ReportPeriod.surveySection = defaultValues.sectionSummer then defaultValues.censusDateSummer
					 when ReportPeriod.surveySection = defaultValues.sectionRetFall then defaultValues.censusDateRetFall
					 when ReportPeriod.surveySection = defaultValues.sectionRetSummer then defaultValues.censusDateRetSummer
				end censusDate,
				ReportPeriod.partOfTermCode partOfTermCode,
				defaultValues.acadOrProgReporter acadOrProgReporter,
				defaultValues.feIncludeOptSurveyData feIncludeOptSurveyData,
				defaultValues.includeNonDegreeAsUG includeNonDegreeAsUG,
				defaultValues.genderForUnknown genderForUnknown,
				defaultValues.genderForNonBinary genderForNonBinary,
				row_number() over (	
					partition by 
						ReportPeriod.surveyCollectionYear,
						ReportPeriod.surveyId,
						ReportPeriod.surveySection,
						ReportPeriod.termCode,
						ReportPeriod.partOfTermCode	
					order by ReportPeriod.recordActivityDate desc
				) reportPeriodRn	
		from IPEDSReportingPeriod ReportPeriod --IPEDSTest ReportPeriod
			cross join ConfigPerAsOfDate defaultValues
		where ReportPeriod.surveyCollectionYear = defaultValues.surveyYear
			and ReportPeriod.surveyId = defaultValues.surveyId
			and ReportPeriod.termCode is not null
			and ReportPeriod.partOfTermCode is not null
		)
	where reportPeriodRn = 1
	
	--ak 20200122 added unions to use default values when IPEDSReportingPeriod record doesn't exist so valid output file will always be generated
	
	union
	
	--Pulls default values for Fall when IPEDSReportingPeriod record doesn't exist
	select defValues.surveyYear surveyYear,
		   defValues.sectionFall surveySection,
			defValues.termCodeFall termCode,
			defValues.censusDateFall censusDate,
			defValues.partOfTermCode partOfTermCode,
			defValues.acadOrProgReporter acadOrProgReporter,
			defValues.feIncludeOptSurveyData feIncludeOptSurveyData,
			defValues.includeNonDegreeAsUG includeNonDegreeAsUG,
			defValues.genderForUnknown genderForUnknown,
			defValues.genderForNonBinary genderForNonBinary
	from ConfigPerAsOfDate defValues
	where defValues.surveyYear not in (select ReportPeriod.surveyCollectionYear
										from ConfigPerAsOfDate defaultValues
											cross join IPEDSReportingPeriod ReportPeriod
										where ReportPeriod.surveyCollectionYear = defaultValues.surveyYear
											and ReportPeriod.surveyId = defaultValues.surveyId
											and ReportPeriod.surveySection = defaultValues.sectionFall
											and ReportPeriod.termCode is not null
											and ReportPeriod.partOfTermCode is not null)
	
	union
	
	--Pulls default values for Retention Fall when IPEDSReportingPeriod record doesn't exist
	select defValues.surveyYear surveyYear,
			defValues.sectionRetFall surveySection,
			defValues.termCodeRetFall termCode,
			defValues.censusDateRetFall censusDate,
			defValues.partOfTermCode partOfTermCode,
			defValues.acadOrProgReporter acadOrProgReporter,
			defValues.feIncludeOptSurveyData feIncludeOptSurveyData,
			defValues.includeNonDegreeAsUG includeNonDegreeAsUG,
			defValues.genderForUnknown genderForUnknown,
			defValues.genderForNonBinary genderForNonBinary
	from ConfigPerAsOfDate defValues
	where defValues.surveyYear not in (select ReportPeriod.surveyCollectionYear
										from ConfigPerAsOfDate defaultValues
											cross join IPEDSReportingPeriod ReportPeriod
										where ReportPeriod.surveyCollectionYear = defaultValues.surveyYear
											and ReportPeriod.surveyId = defaultValues.surveyId
											and ReportPeriod.surveySection = defaultValues.sectionRetFall
											and ReportPeriod.termCode is not null
											and ReportPeriod.partOfTermCode is not null)
	
	union
	
	--Pulls default values for Summer when IPEDSReportingPeriod record doesn't exist
	select defValues.surveyYear surveyYear,
			defValues.sectionSummer surveySection,
			defValues.termCodeSummer termCode,
			defValues.censusDateSummer censusDate,
			defValues.partOfTermCode partOfTermCode,
			defValues.acadOrProgReporter acadOrProgReporter,
			defValues.feIncludeOptSurveyData feIncludeOptSurveyData,
			defValues.includeNonDegreeAsUG includeNonDegreeAsUG,
			defValues.genderForUnknown genderForUnknown,
			defValues.genderForNonBinary genderForNonBinary
	from ConfigPerAsOfDate defValues
	where defValues.surveyYear not in (select ReportPeriod.surveyCollectionYear
										from ConfigPerAsOfDate defaultValues
											cross join IPEDSReportingPeriod ReportPeriod
										where ReportPeriod.surveyCollectionYear = defaultValues.surveyYear
											and ReportPeriod.surveyId = defaultValues.surveyId
											and ReportPeriod.surveySection = defaultValues.sectionSummer
											and ReportPeriod.termCode is not null
											and ReportPeriod.partOfTermCode is not null)										
	
	union
	
	--Pulls default values for Retention Summer when IPEDSReportingPeriod record doesn't exist
	select defValues.surveyYear surveyYear,
			defValues.sectionRetSummer surveySection,
			defValues.termCodeRetSummer termCode,
			defValues.censusDateRetSummer censusDate,
			defValues.partOfTermCode partOfTermCode,
			defValues.acadOrProgReporter acadOrProgReporter,
			defValues.feIncludeOptSurveyData feIncludeOptSurveyData,
			defValues.includeNonDegreeAsUG includeNonDegreeAsUG,
			defValues.genderForUnknown genderForUnknown,
			defValues.genderForNonBinary genderForNonBinary
	from ConfigPerAsOfDate defValues
	where defValues.surveyYear not in (select ReportPeriod.surveyCollectionYear
										from ConfigPerAsOfDate defaultValues
											cross join IPEDSReportingPeriod ReportPeriod
										where ReportPeriod.surveyCollectionYear = defaultValues.surveyYear
											and ReportPeriod.surveyId = defaultValues.surveyId
											and ReportPeriod.surveySection = defaultValues.sectionRetSummer
											and ReportPeriod.termCode is not null
											and ReportPeriod.partOfTermCode is not null)	
	)  RepDates
	left join AcadTermLatestRecord AcademicTerm on RepDates.termCode = AcademicTerm.termCode	
		and RepDates.partOfTermCode = AcademicTerm.partOfTermCode
),

--jh 20200422 Added default values from ConfigPerAsOfDate

HRReportingPeriod as (
--Returns client specified reporting period for HR reporting and defaults to IPEDS specified date range if not otherwise defined 

select surveyYear surveyYear,
	   asOfDate asOfDate,
	   termCodeFall termCodeFall
from (
		select ReportPeriod.surveyCollectionYear surveyYear,
		        nvl(ReportPeriod.asOfDate, defaultValues.asOfDateHR) asOfDate,
				defaultValues.termCodeFall termCodeFall,
				row_number() over (
					partition by
						reportPeriod.surveyCollectionYear
					order by
						reportPeriod.recordActivityDate desc
				) reportPeriodRn
		from ConfigPerAsOfDate defaultValues
			cross join IPEDSReportingPeriod ReportPeriod
		where reportPeriod.surveyCollectionYear = defaultValues.surveyYear
			and reportPeriod.surveyId like defaultValues.surveyIdHR
		)
where reportPeriodRn = 1
	
union

select defValues.surveyYear surveyYear,
		defValues.asOfDateHR asOfDate,
		defValues.termCodeFall termCodeFall
from ConfigPerAsOfDate defValues
	where defValues.surveyYear not in (select reportPeriod.surveyCollectionYear
										from ConfigPerAsOfDate defaultValues
											cross join IPEDSReportingPeriod reportPeriod
										where reportPeriod.surveyCollectionYear = defaultValues.surveyYear
											and reportPeriod.surveyId like defaultValues.surveyIdHR)
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusPerAsOfCensus as ( 
-- Returns most recent campus record for each campus available per the reporting terms and part of terms (ReportingPeriod).

select *
from (
		select campus.*,
				row_number() over (
					partition by
						campus.campus
					order by
						campus.recordActivityDate desc
				) campusRn
		from ReportingPeriod ReportingDates
		cross join Campus campus 
		where campus.isIpedsReportable = 1
--ak 20200406 Including dummy date changes. (PF-1368)
		and ((campus.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
		    and campus.recordActivityDate <= ReportingDates.censusDate)
		        or campus.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where campusRn = 1
),

RegPerTermAsOfCensus as ( 
--Returns all student enrollment records as of the ReportingPeriod and where course is viable

select cohortInd cohortInd,
		personId personId,
        regTermCode termCode,
        partOfTermCode partOfTermCode,
        censusDate censusDate,
        crn crn,
		crnLevel crnLevel,
        isInternational isInternational,
		crnGradingMode crnGradingMode,
		equivCRHRFactor equivCRHRFactor
from (
		select ReportingDates.cohortInd cohortInd,
				reg.personId personId,
				ReportingDates.censusDate censusDate,
				ReportingDates.termCode regTermCode,
				ReportingDates.partOfTermCode partOfTermCode,
				campus.isInternational isInternational,
-- ak 20200330 added Registration.crnGradingMode (PF-1253)
				reg.crnGradingMode crnGradingMode,
-- ak 20200519 Added equivalent hours for PT/FT hours multiplier (PF-1480).
				ReportingDates.equivCRHRFactor equivCRHRFactor,
				reg.crn crn,
-- ak 20200330 Added crnLevel ENUM field with values Undergrad,Graduate,Postgraduate,Professional,Continuing Ed (PF-1253)
				reg.crnLevel crnLevel,
				row_number() over (
					partition by
						reg.personId,
						reg.termCode,
						reg.partOfTermCode,
						reg.crn,
						reg.crnLevel
					order by
						reg.recordActivityDate desc
				) regRn
		from ReportingPeriod ReportingDates
			inner join Registration reg on ReportingDates.termCode = reg.termCode 
-- ak 20200330 added Registration.partOfTermCode (PF-1253)
				and reg.partOfTermCode = ReportingDates.partOfTermCode
-- ak 20200330 added Registration.registrationStatusDate (PF-1253)
--ak 20200406 Including dummy date changes. (PF-1368)
                and ((reg.registrationStatusDate != CAST('9999-09-09' AS TIMESTAMP)
				    and reg.registrationStatusDate <= ReportingDates.censusDate)
			            or reg.registrationStatusDate = CAST('9999-09-09' AS TIMESTAMP))
				--and reg.recordActivityDate <= ReportingDates.censusDate
-- ak 20200330 Modified to use 'isEnrolled' field instead of registrationStatus = 'Enrolled'(PF-1253)
				--and reg.registrationStatus = 'Enrolled'
				and reg.isEnrolled = 1
				and reg.isIpedsReportable = 1 
			left join CampusPerAsOfCensus campus on reg.campus = campus.campus
	)
where regRn = 1
),

StudentPerTermReg as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods. 

select *
from (
		select distinct reg.cohortInd cohortInd,
				reg.crn,
				student.*,
				row_number() over (
					partition by
						student.personId,
						student.termCode
					order by
						student.recordActivityDate desc
				) studRn
		from RegPerTermAsOfCensus reg
		inner join Student student ON reg.personId = student.personId
--ak 20200406 Including dummy date changes. (PF-1368)
--jh 20200422 Due to Student ingestion query timeout issue, commenting this filter out for testing
--77 results with filter, 5335 results without filter
        and ((student.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) --77
				and student.recordActivityDate <= reg.censusDate)
			        or student.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and reg.termCode = student.termCode
			and student.isIpedsReportable = 1
	)
where studRn = 1
),

PersonPerReg as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 
	
select *
from (
		select reg.cohortInd cohortInd,
				person.*,
				row_number() over (
					partition by
						person.personId
					order by
						person.recordActivityDate desc
				) personRn
        from RegPerTermAsOfCensus reg
		inner join Person person ON reg.personId = person.personId
--ak 20200406 Including dummy date changes. (PF-1368)
            and ((person.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and person.recordActivityDate <= reg.censusDate)
			        or person.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and person.isIpedsReportable = 1
	)
where personRn = 1
),

AcadTrackPerTermReg as (
--Returns most up to date student field of study information as of the reporting term codes and part of term census periods.
--Only required to report based on field of study based categorization for even-numbered IPEDS reporting years. 

select * 
from (
        select reg.cohortInd cohortInd,
				reg.termCode termCode,
				reg.partOfTermCode partOfTermCode,
				acadtrack.*,
				reg.censusDate censusDate,
				row_number() over (
					partition by
						acadtrack.personId,
						acadtrack.termCodeEffective,
						acadtrack.academicTrackLevel
					order by
                        acadTrack.termCodeEffective desc,
                        acadTrack.recordActivityDate desc
				) acadtrackRn
        from RegPerTermAsOfCensus reg
		left join AcademicTrack acadtrack ON acadtrack.personId = reg.personId
			and acadtrack.termCodeEffective <= reg.termCode
--ak 20200406 Including dummy date changes. (PF-1368)
            and ((acadtrack.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and acadtrack.recordActivityDate <= reg.censusDate)
			        or acadtrack.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and acadtrack.fieldOfStudyType = 'Major'
			and acadtrack.fieldOfStudyPriority = 1
			and acadtrack.isIpedsReportable = 1
	)
where acadtrackRn = 1
),

MajorPerAcadTrack as (
--Returns most up to 'major' information as of the reporting term codes and part of term census periods.
--This information is only required for even-numbered IPEDS reporting years. 

select *
from (
		select acadTrack.cohortInd,
				acadtrack.termCode,
				acadtrack.partOfTermCode,
				major.*,
				row_number() over (
					partition by
						major.major
					order by
						major.recordActivityDate desc
				) majorRn
		from AcadTrackPerTermReg acadtrack
		left join Major major ON major.major = acadtrack.curriculumCode
--ak 20200406 Including dummy date changes. (PF-1368)
            and ((major.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and major.recordActivityDate <= acadtrack.censusDate)
			        or major.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and major.isIpedsReportable = 1
		where (select config.reportResidency
			   from ConfigPerAsOfDate config) = 'Y'
	)
where majorRn = 1
),

-- ak 20200427 Adding Degree entitiy to pull award level for 'Bachelor level' Retention cohort filtering. (PF-1434)
DegreePerAcadTrack as (
--Returns most up to 'degree' information as of the reporting term codes and part of term census periods.
--This information is used for Retention cohort filtering on awardLevel

select *
from (
		select acadTrack.personId,
		        acadTrack.cohortInd,
				acadtrack.termCode,
				acadtrack.partOfTermCode,
				degree.*,
				row_number() over (
					partition by
						degree.degree
					order by
						degree.recordActivityDate desc
				) degreeRn
		from AcadTrackPerTermReg acadtrack
        inner join ConfigPerAsOfDate defaultValues ON acadTrack.cohortInd = defaultValues.sectionRetFall
		inner join Degree degree ON degree.degree = acadtrack.degree
--ak 20200406 Including dummy date changes. (PF-1368)
            and ((degree.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and degree.recordActivityDate <= acadtrack.censusDate)
			        or degree.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and degree.curriculumRuleTermCodeEff <= acadTrack.termCode
			and degree.isIpedsReportable = 1
	)
where degreeRn = 1
),

--jh 20200422 moved CourseSectionPerTerm prior to CourseSectionSchedulePerTerm and added section field

CourseSectionPerTerm as (
--Included to get enrollment hours of a CRN

select *, 
-- ak 20200519 Adding logic to convert clock hours using the the equivalent credit hour factor.
        case when isClockHours = 1 
            then enrollmentHours * equivCRHRFactor
        else enrollmentHours
        end creditHourEquivalent
	from (
		select distinct 
		       reg.cohortInd cohortInd,
				courseSect.crn crn,
				courseSect.section section,
				courseSect.subject subject,
				courseSect.courseNumber courseNumber,
				courseSect.termCode termCode,
				courseSect.partOfTermCode partOfTermCode,
				reg.crnLevel crnLevel,
				reg.censusDate censusDate,
				reg.equivCRHRFactor equivCRHRFactor,
				courseSect.recordActivityDate recordActivityDate,
--ak 20200519 Changing CourseSchedule.enrollmentCreditHours to enrollmentHours (PF-1480)
				CAST(courseSect.enrollmentHours as decimal(2,0)) enrollmentHours,
				--CAST(courseSect.creditOrClockHours as decimal(2,0)) enrollmentHours,
				courseSect.isClockHours isClockHours, 
				row_number() over (
					partition by
						courseSect.subject,
						courseSect.courseNumber,
						courseSect.crn,
						courseSect.section,
						courseSect.termCode,
						courseSect.partOfTermCode
					order by
						courseSect.recordActivityDate desc
				) courseRn
		from RegPerTermAsOfCensus reg 
		inner join CourseSection courseSect ON courseSect.termCode = reg.termCode
			and courseSect.partOfTermCode = reg.partOfTermCode
			and courseSect.crn = reg.crn
			and courseSect.isIpedsReportable = 1
--ak 20200406 Including dummy date changes. (PF-1368)
            and ((courseSect.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and courseSect.recordActivityDate <= reg.censusDate)
			        or courseSect.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where courseRn = 1
),

CourseSectionSchedulePerTerm as (
--Returns course scheduling related info for the registration CRN.
--AcademicTerm.partOfTermCode, CourseSectionSchedule.partOfTermCode & AcademicTerm.censusDate together are used to define the period 
--of valid course registration attempts. 

select *
from (
		select courseSect.cohortInd cohortInd,
		        courseSect.censusDate censusDate,
				courseSectSched.crn crn,
				courseSectSched.section section,
				courseSectSched.termCode termCode,
				courseSectSched.partOfTermCode partOfTermCode,
				courseSect.creditHourEquivalent creditHourEquivalent,
				courseSect.subject subject,
				courseSect.courseNumber courseNumber,
				courseSect.crnLevel crnLevel,
				nvl(courseSectSched.meetingType, 'Standard') meetingType,
				row_number() over (
					partition by
						courseSectSched.crn,
						courseSectSched.section,
						courseSectSched.termCode,
						courseSectSched.partOfTermCode
					order by
						courseSectSched.recordActivityDate desc
				) courseSectSchedRn
		--from CourseSectionPerTerm courseSect
		from CourseSectionPerTerm courseSect
        left join CourseSectionSchedule courseSectSched ON courseSectSched.termCode = courseSect.termCode
			and courseSectSched.partOfTermCode = courseSect.partOfTermCode
			and courseSectSched.crn = courseSect.crn
			and courseSectSched.section = courseSect.section
			and courseSectSched.isIpedsReportable = 1 
--ak 20200406 Including dummy date changes. (PF-1368)
			and ((courseSectSched.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and courseSectSched.recordActivityDate <= courseSect.censusDate)
			        or courseSectSched.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where courseSectSchedRn = 1
),

--jh 20200422 removed ReportingPeriod and CourseSectionPerTerm joins and added section field
CoursePerTerm as (
--Included to get course type information

select *
from (
		select distinct courseSect.cohortInd cohortInd,
				courseSect.crn crn,
				courseSect.section section,
				course.subject,
				course.courseNumber,
				courseSect.termCode termCode,
				courseSect.partOfTermCode partOfTermCode,
				courseSect.censusDate censusDate,
				course.courseLevel courseLevel,
				course.isRemedial isRemedial,
				course.isESL isESL,
				courseSect.meetingType meetingType,
				CAST(courseSect.creditHourEquivalent as DECIMAL(2,0)) creditHrs,
				row_number() over (
					partition by
						course.subject,
						course.courseNumber,
						course.termCodeEffective,
						course.courseLevel
					order by
						course.recordActivityDate desc
				) courseRn
        from CourseSectionSchedulePerTerm courseSect
		    inner join Course course ON course.subject = courseSect.subject
			    and course.courseNumber = courseSect.courseNumber
			    and course.courseLevel = courseSect.crnLevel
			    and course.termCodeEffective <= courseSect.termCode
			    and course.isIpedsReportable = 1 --true
--ak 20200406 Including dummy date changes. (PF-1368)
                and ((course.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				    and course.recordActivityDate <= courseSect.censusDate)
			            or course.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where courseRn = 1
),

--jh 20200422 removed CourseSectionSchedulePerTerm join

CourseCountsPerStudent as (
-- View used to break down course category type counts by type

select reg.cohortInd cohortInd,
		reg.personId personId,
        reg.termCode termCode,
        reg.partOfTermCode partOfTermCode,
        sum(case when nvl(course.creditHrs, 0) is not null then 1 else 0 end) totalCourses,
		sum(nvl(course.creditHrs, 0)) totalHrs,
        sum(case when nvl(course.creditHrs, 0) = 0 then 1 else 0 end) totalNonCredCourses,
        sum(case when nvl(course.creditHrs, 0) != 0 then 1 else 0 end) totalCredCourses,
        sum(case when course.meetingType = 'Online/Distance Learning' then 1 else 0 end) totalDECourses,
        sum(case when course.courseLevel = 'Undergrad' then 1 else 0 end) totalUGCourses,
        sum(case when course.courseLevel in ('Graduate', 'Professional') then 1 else 0 end) totalGRCourses,
        sum(case when course.courseLevel = 'Postgraduate' then 1 else 0 end) totalPostGRCourses,
		sum(case when course.courseLevel = 'Continuing Ed' then 1 else 0 end) totalCECourses,
        sum(case when course.isESL = 'Y' then 1 else 0 end) totalESLCourses,
        sum(case when course.isRemedial = 'Y' then 1 else 0 end) totalRemCourses,
        sum(case when reg.isInternational = 1 then 1 else 0 end) totalIntlCourses,
-- ak 20200330 added Registration.crnGradingMode (PF-1253)
        sum(case when reg.crnGradingMode = 'Audit' then 1 else 0 end) totalAuditCourses
from RegPerTermAsOfCensus reg
	inner join CoursePerTerm course on course.termCode = reg.termCode
	    and course.partOfTermCode = reg.partOfTermCode
	    and course.crn = reg.crn
-- ak 20200330 Added crnLevel ENUM field with values Undergrad,Graduate,Postgraduate,Professional,Continuing Ed (PF-1253)
		and course.courseLevel = reg.crnLevel 
group by reg.cohortInd,
		reg.personId,
		reg.termCode,
		reg.partOfTermCode
--order by reg.cohortInd,
--		reg.personId,
--		reg.termCode
),

--jh 20200422 modified to only pull first time students from summer term
 
StuReportableType as (
--View used to determine reporting year IPEDS defined student type using prior summer considerations
--Student is considered 'First Time' if they enrolled for the first time in the reporting fall term or 
--if they enrolled for the first time in the prior summer and continued on to also take fall courses. 

select stu.personId personId
from ConfigPerAsOfDate defaultValues
   cross join StudentPerTermReg stu 
where stu.cohortInd = defaultValues.sectionSummer 
   and stu.studentType = 'First Time'
),

--jh 20200422 modified to only pull first time students from retention summer term

RetentionStuReportableType as (
--This view is the same as 'StuReportableType' but it applies to the retention cohort

select stu.personId personId
from ConfigPerAsOfDate defaultValues
   cross join StudentPerTermReg stu 
where stu.cohortInd = defaultValues.sectionRetSummer 
   and stu.studentType = 'First Time'
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

--jh 20200422 Added variables from DefaultValues to remove hard-coding
--			  Added alias to cohortInd field
--			  Made aliases for course consistent
--			  Removed termCode and partOfTermCode fields from Person and Student joins

StudentCohort as (
--View used to build the reportable 'Fall' student cohort. (CohortInd = 'Fall)
--An indicator 'IPEDSinclude' is built out in this view to indicate if a student cohort record meets IPEDS reporting 
--specs to be included. 

select CohView.*,
		case when CohView.studentLevel = 'Undergraduate' then 
				(case when CohView.totalHrs >= CohView.requiredUGhrs then 'Full' else 'Part' end)
			 when CohView.studentLevel = 'Graduate' then 
		        (case when CohView.totalHrs >= CohView.requiredGRhrs then 'Full' else 'Part' end)
			 else 'Part' 
		end timeStatus
from (	
		select distinct reg.cohortInd cohortInd,
				reg.personId personId,
				reg.termCode termCode,
				reg.censusDate censusDate,
--jh 20200422 added config fields
				defaultValues.acadOrProgReporter acadOrProgReporter,
				defaultValues.reportResidency reportResidency,
				defaultValues.reportAge reportAge,
				defaultValues.genderForNonBinary genderForNonBinary,
				defaultValues.genderForUnknown genderForUnknown,
				--major.cipCode cipCode,
				--major.major major,
				--AcademicTrackCurrent.curriculumCode degree,
				stuReg.isNonDegreeSeeking isNonDegreeSeeking,
-- ak 20200330 	(PF-1320) Modification to pull residency from student instead of person.
				stuReg.residency residency,
				stuReg.highSchoolGradDate hsGradDate,
-- ak 20200519 Adding FT/PT credit hour comparison points (PF-1480)
				NVL(term.requiredFTCreditHoursUG, 12) requiredUGhrs,
				NVL(term.requiredFTCreditHoursGR, 9) requiredGRhrs,
				person.gender gender,
-- ak 20200421 (PF-1417) Modification to replace 'isInternational' with 'isInUSOnVisa' and 'isUSCitizen'
				person.isInUSOnVisa isInUSOnVisa,
                person.isUSCitizen isUSCitizen,
				person.isHispanic isHispanic,
				person.isMultipleRaces isMultipleRaces,
				person.ethnicity ethnicity,
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'
				floor(datediff(to_date(reg.censusDate), person.birthDate) / 365) asOfAge,
				person.nation nation,
				person.state state,
				--courseSch.meetingType meetingType,
				courseCnt.totalHrs totalHrs,
				courseCnt.totalCourses totalcourses,
				courseCnt.totalDEcourses totalDEcourses,
				courseCnt.totalUGCourses totalUGCourses,
				courseCnt.totalGRCourses totalGRCourses,
				courseCnt.totalPostGRCourses totalPostGRCourses,
				courseCnt.totalNonCredCourses totalNonCredCourses,
				courseCnt.totalESLCourses totalESLCourses,
				courseCnt.totalRemCourses totalRemCourses,
-- ak 20200330 Continuing ed is no longer considered a student type (PF-1382)
--jh 20200422 if cohort student type is not first time, check is summer is. If not, value is student type
				case when stuReg.studentType = 'First Time' then 'First Time'
				     when stuReg.personId = reportType.personId then 'First Time'
					 else stuReg.studentType
				end studentType,
				case when stuReg.studentLevel = 'Continuing Ed' and defaultValues.includeNonDegreeAsUG = 'Y' then 'UnderGrad' 
					 when stuReg.studentLevel in ('Professional', 'Postgraduate') then 'Graduate' 
					 else stuReg.studentLevel 
				end studentLevel,
				case 
--jh 20200422 changed courseCnt.totalCredCourses to courseCnt.totalCourses
					when courseCnt.totalRemCourses = courseCnt.totalCourses and stuReg.isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
				    when courseCnt.totalCredCourses > 0 --exclude students not enrolled for credit
						then (case when courseCnt.totalESLCourses = courseCnt.totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
								   when courseCnt.totalCECourses = courseCnt.totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
								   when courseCnt.totalIntlCourses = courseCnt.totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
-- ak 20200330 added Registration.crnGradingMode (PF-1253)
								   when courseCnt.totalAuditCourses = courseCnt.totalCredCourses then 0 --exclude students exclusively auditing classes
								   -- when... then 0 --exclude PHD residents or interns
								   -- when... then 0 --students studying abroad if enrollment at home institution is an admin only record
								   -- when... then 0 --exclude students in experimental Pell programs
								   else 1
							  end)
					 else 0 
				 end ipedsInclude
		from ConfigPerAsOfDate defaultValues
--jh 20200422 added join to filter on fall cohort term code
			inner join RegPerTermAsOfCensus reg on reg.cohortInd = defaultValues.sectionFall 
			inner join PersonPerReg person on reg.personId = person.personId
			    and person.cohortInd = reg.cohortInd
			inner join StudentPerTermReg stuReg on reg.personId = stuReg.personId
				and stuReg.cohortInd = reg.cohortInd
			inner join AcadTermLatestRecord term on term.termCode = reg.termCode
--jh 20200422 changed inner join to left join
			left join CourseCountsPerStudent courseCnt on reg.personId = courseCnt.personId --5299
				and courseCnt.cohortInd = reg.cohortInd
			left join StuReportableType reportType on reportType.personId = reg.personId
--ak 20200429 Adding student status filter. 
			where stuReg.studentStatus = 'Active'
	) as CohView
),

StudentCohortRefactor as (
-- This view is used to refactor the 'StudentCohort' view records in terms of IPEDS reportable values

select cohort.personId personId,
        cohort.cohortInd cohortInd,
		cohort.censusDate censusDate,
--jh 20200422 added config fields
		cohort.acadOrProgReporter acadOrProgReporter,
		cohort.reportResidency reportResidency,
		cohort.reportAge reportAge,
        cohort.studentType studentType,
        cohort.ipedsInclude ipedsInclude,
        cohort.isNonDegreeSeeking isNonDegreeSeeking,
        cohort.studentLevel studentLevel,
--jh 20200423 Broke up ipedsLevel into PartG, PartB and PartA
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'
		case when cohort.studentLevel = 'Graduate' then 3
			 when cohort.isNonDegreeSeeking = true then 2
			 else 1
		end ipedsPartGStudentLevel,
		case when cohort.studentLevel = 'Graduate' then 3
			 else 1
		end ipedsPartBStudentLevel,
        cohort.timeStatus timeStatus,
        cohort.hsGradDate hsGradDate,
		case when cohort.timeStatus = 'Full'
				and cohort.studentType = 'First Time'
				and cohort.isNonDegreeSeeking = 'false'
				and cohort.studentLevel = 'Undergrad'  
				then 1 -- 1 - Full-time, first-time degree/certificate-seeking undergraduate
			when cohort.timeStatus = 'Full'
				and cohort.studentType = 'Transfer'
				and cohort.isNonDegreeSeeking = 'false'
				and cohort.studentLevel = 'Undergrad'
				then 2 -- 2 - Full-time, transfer-IN degree/certificate-seeking undergraduate
			when cohort.timeStatus = 'Full'
				and cohort.studentType = 'Returning'
				and cohort.isNonDegreeSeeking = 'false'
				and cohort.studentLevel = 'Undergrad'
				then 3 -- 3 - Full-time, continuing degree/certificate-seeking undergraduate
			when cohort.timeStatus = 'Full'
				and cohort.isNonDegreeSeeking = 'true'
				and cohort.studentLevel = 'Undergrad'
				then 7 -- 7 - Full-time, non-degree/certificate-seeking undergraduate
			when cohort.timeStatus = 'Full'
				and cohort.studentLevel = 'Graduate'
				then 11 -- 11 - Full-time graduate
			when cohort.timeStatus = 'Part'
				and cohort.studentType = 'First Time'
				and cohort.isNonDegreeSeeking = 'false'
				and cohort.studentLevel = 'Undergrad'
				then 15 -- 15 - Part-time, first-time degree/certificate-seeking undergraduate
			when cohort.timeStatus = 'Part'
				and cohort.studentType = 'Transfer'
				and cohort.isNonDegreeSeeking = 'false'
				and cohort.studentLevel = 'Undergrad'
				then 16 -- 16 - Part-time, transfer-IN degree/certificate-seeking undergraduate
			when cohort.timeStatus = 'Part'
				and cohort.studentType = 'Returning'
				and cohort.isNonDegreeSeeking = 'false'
				and cohort.studentLevel = 'Undergrad'
				then 17 -- 17 - Part-time, continuing degree/certificate-seeking undergraduate
			when cohort.timeStatus = 'Part'
				and cohort.isNonDegreeSeeking = 'true'
				and cohort.studentLevel = 'Undergrad'
				then 21 -- 21 - Part-time, non-degree/certificate-seeking undergraduate
			when cohort.timeStatus = 'Part'
				and cohort.studentLevel = 'Graduate'
				then 25 -- 25 - Part-time graduate
		end as ipedsPartAStudentLevel,
-- ak 20200330 (PF-1253) Including client preferences for non-binary and unknown gender assignments
        case when cohort.gender = 'Male' then 'M'
			when cohort.gender = 'Female' then 'F'
--jh 20200223 added config values
			when cohort.gender = 'Non-Binary' then cohort.genderForNonBinary
			else cohort.genderForUnknown
		end ipedsGender,
-- ak 20200421 (PF-1417) Modification to use 'isInUSOnVisa' and 'isUSCitizen' for ethnicity assignments
        case when cohort.isUSCitizen = 1 then 
		        (case when cohort.isHispanic = true then 2 -- 'hispanic/latino'
	    	        when cohort.isMultipleRaces = true then 8 -- 'two or more races'
			        when cohort.ethnicity != 'Unknown' and cohort.ethnicity is not null
				    then (case when cohort.ethnicity = 'Hispanic or Latino' then 2
                            when cohort.ethnicity = 'American Indian or Alaskan Native' then 3
							when cohort.ethnicity = 'Asian' then 4
							when cohort.ethnicity = 'Black or African American' then 5
							when cohort.ethnicity = 'Native Hawaiian or Other Pacific Islander' then 6
							when cohort.ethnicity = 'Caucasian' then 7
							else 9 end) 
			    else 9 end) -- 'race and ethnicity unknown'
            else (case when cohort.isInUSOnVisa = 1 then 1 -- 'nonresident alien'
                    else 9 end) -- 'race and ethnicity unknown'
		end ipedsEthnicity,
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'
		case when cohort.timeStatus = 'Full' 
				then (
					case when cohort.asOfAge < 18 then 1 --1 - Full-time, under 18
						when cohort.asOfAge >= 18 and cohort.asOfAge <= 19 then 2 --2 - Full-time, 18-19
						when cohort.asOfAge >= 20 and cohort.asOfAge <= 21 then 3 --3 - Full-time, 20-21
						when cohort.asOfAge >= 22 and cohort.asOfAge <= 24 then 4 --4 - Full-time, 22-24
						when cohort.asOfAge >= 25 and cohort.asOfAge <= 29 then 5 --5 - Full-time, 25-29
						when cohort.asOfAge >= 30 and cohort.asOfAge <= 34 then 6 --6 - Full-time, 30-34
						when cohort.asOfAge >= 35 and cohort.asOfAge <= 39 then 7 --7 - Full-time, 35-39
						when cohort.asOfAge >= 40 and cohort.asOfAge <= 49 then 8 --8 - Full-time, 40-49
						when cohort.asOfAge >= 50 and cohort.asOfAge <= 64 then 9 --9 - Full-time, 50-64
						when cohort.asOfAge >= 65 then 10 --10 - Full-time, 65 and over
					end )
			else (
				case when cohort.asOfAge < 18 then 13 --13 - Part-time, under 18
					when cohort.asOfAge >= 18 and cohort.asOfAge <= 19 then 14 --14 - Part-time, 18-19
					when cohort.asOfAge >= 20 and cohort.asOfAge <= 21 then 15 --15 - Part-time, 20-21
					when cohort.asOfAge >= 22 and cohort.asOfAge <= 24 then 16 --16 - Part-time, 22-24
					when cohort.asOfAge >= 25 and cohort.asOfAge <= 29 then 17 --17 - Part-time, 25-29
					when cohort.asOfAge >= 30 and cohort.asOfAge <= 34 then 18 --18 - Part-time, 30-34
					when cohort.asOfAge >= 35 and cohort.asOfAge <= 39 then 19 --19 - Part-time, 35-39
					when cohort.asOfAge >= 40 and cohort.asOfAge <= 49 then 20 --20 - Part-time, 40-49
					when cohort.asOfAge >= 50 and cohort.asOfAge <= 64 then 21 --21 - Part-time, 50-64
					when cohort.asOfAge >= 65 then 22 --22 - Part-time, 65 and over
				end )
		end ipedsAgeGroup,
		cohort.nation,
		case when cohort.nation = 'US' then 1 else 0 end isInAmerica,
		case when cohort.residency = 'In District' then 'IN'
			 when cohort.residency = 'In State' then 'IN'
			 when cohort.residency = 'Out of State' then 'OUT'
--jh 20200423 added value of Out of US
			 when cohort.residency = 'Out of US' then 'OUT'
			 else 'UNKNOWN'
		end residentStatus,
--check 'IPEDSClientConfig' entity to confirm that optional data is reported
        case when (select config.reportResidency from ConfigPerAsOfDate config) = 'Y' 
				then (case when cohort.state IS NULL then 57 --57 - State unknown
							when cohort.state = 'AL' then 01 --01 - Alabama
							when cohort.state = 'AK' then 02 --02 - Alaska
							when cohort.state = 'AZ' then 04 --04 - Arizona
							when cohort.state = 'AR' then 05 --05 - Arkansas
							when cohort.state = 'CA' then 06 --06 - California
							when cohort.state = 'CO' then 08 --08 - Colorado
							when cohort.state = 'CT' then 09 --09 - CONnecticut
							when cohort.state = 'DE' then 10 --10 - Delaware
							when cohort.state = 'DC' then 11 --11 - District of Columbia
							when cohort.state = 'FL' then 12 --12 - Florida
							when cohort.state = 'GA' then 13 --13 - Georgia
							when cohort.state = 'HI' then 15 --15 - Hawaii
							when cohort.state = 'ID' then 16 --16 - Idaho
							when cohort.state = 'IL' then 17 --17 - Illinois
							when cohort.state = 'IN' then 18 --18 - Indiana
							when cohort.state = 'IA' then 19 --19 - Iowa
							when cohort.state = 'KS' then 20 --20 - Kansas
							when cohort.state = 'KY' then 21 --21 - Kentucky
							when cohort.state = 'LA' then 22 --22 - Louisiana
							when cohort.state = 'ME' then 23 --23 - Maine
							when cohort.state = 'MD' then 24 --24 - Maryland
							when cohort.state = 'MA' then 25 --25 - Massachusetts
							when cohort.state = 'MI' then 26 --26 - Michigan
							when cohort.state = 'MS' then 27 --27 - Minnesota
							when cohort.state = 'MO' then 28 --28 - Mississippi
							when cohort.state = 'MN' then 29 --29 - Missouri
							when cohort.state = 'MT' then 30 --30 - Montana
							when cohort.state = 'NE' then 31 --31 - Nebraska
							when cohort.state = 'NV' then 32 --32 - Nevada
							when cohort.state = 'NH' then 33 --33 - New Hampshire
							when cohort.state = 'NJ' then 34 --34 - New Jersey
							when cohort.state = 'NM' then 35 --35 - New Mexico
							when cohort.state = 'NY' then 36 --36 - New York
							when cohort.state = 'NC' then 37 --37 - North Carolina
							when cohort.state = 'ND' then 38 --38 - North Dakota
							when cohort.state = 'OH' then 39 --39 - Ohio	
							when cohort.state = 'OK' then 40 --40 - Oklahoma
							when cohort.state = 'OR' then 41 --41 - Oregon
							when cohort.state = 'PA' then 42 --42 - Pennsylvania
							when cohort.state = 'RI' then 44 --44 - Rhode Island
							when cohort.state = 'SC' then 45 --45 - South Carolina
							when cohort.state = 'SD' then 46 --46 - South Dakota
							when cohort.state = 'TN' then 47 --47 - Tennessee
							when cohort.state = 'TX' then 48 --48 - Texas
							when cohort.state = 'UT' then 49 --49 - Utah
							when cohort.state = 'VT' then 50 --50 - Vermont
							when cohort.state = 'VA' then 51 --51 - Virginia
							when cohort.state = 'WA' then 53 --53 - Washington
							when cohort.state = 'WV' then 54 --54 - West Virginia
							when cohort.state = 'WI' then 55 --55 - Wisconsin
							when cohort.state = 'WY' then 56 --56 - Wyoming
							when cohort.state = 'UNK' then 57 --57 - State unknown
							when cohort.state = 'American Samoa' then 60
							when cohort.state = 'FM' then 64 --64 - Federated States of Micronesia
							when cohort.state = 'GU' then 66 --66 - Guam
							when cohort.state = 'Marshall Islands' then 68
							when cohort.state = 'Northern Marianas' then 69
							when cohort.state = 'Palau' then 70
							when cohort.state = 'PUE' then 72 --72 - Puerto Rico
							when cohort.state = 'VI' then 78 --78 - Virgin Islands
							else 90 --90 - Foreign countries
					  end)
			else NULL
        end ipedsStateCode,
        cohort.totalcourses totalCourses,
        cohort.totalDECourses,
        case when cohort.totalDECourses > 0 and cohort.totalDECourses < cohort.totalCourses then 'Some DE'
			 when cohort.totalDECourses = cohort.totalCourses then 'Exclusive DE'
		end distanceEdInd
from StudentCohort cohort
where cohort.ipedsInclude = 1
		and cohort.studentLevel in ('Undergrad', 'Graduate') --filters out CE when client does not want to count CE taking a credit course as Undergrad
),

/*****
BEGIN SECTION - Retention Cohort Creation
This set of views is used to look at prior year data and return retention cohort information of the group as of the current reporting year.
*****/

--jh 20200409 Removed inclusion and enrolled from values; moved exclusionReason to filter in joins
--			  Added variables from DefaultValues to remove hard-coding			  

StudentRetentionExclusions as (
--View used to build the retention student cohort. (CohortInd = 'Retention Fall')

select *
from (
		select exclusion.personId,
				exclusion.termCodeEffective,
				row_number() over (
					partition by
						exclusion.personId,
						exclusion.termCodeEffective
					order by
						exclusion.recordActivityDate desc
				) exclusionRn        
		from ConfigPerAsOfDate defaultValues
			inner join ReportingPeriod ReportingDates on ReportingDates.cohortInd = defaultValues.sectionRetFall
--jh 20200422 changed termCodeEffective to >= to retention termCode (not <=)
			inner join cohortExclusion exclusion on exclusion.termCodeEffective >= ReportingDates.termCode
				and exclusion.exclusionReason in ('Died', 'Medical Leave', 'Military Leave', 'Foreign Aid Service', 'Religious Leave')
				and exclusion.isIPEDSReportable = 1
	)   
where exclusionRn = 1
),

StudentRetentionInclusions as (
--View used to determine inclusions for retention cohort.

select stu.personId personId
from ConfigPerAsOfDate defaultValues
   cross join StudentPerTermReg stu
where stu.studentStatus = 'Active'
	and stu.cohortInd = defaultValues.sectionSummer
),   

--jh 20200422 Added variables from DefaultValues to remove hard-coding
--			  Added alias to cohortInd field
--			  Removed termCode and partOfTermCode fields from Person and Student joins
--			  Removed Graduate part of timeStatus case statement in outer query
--			  Added code to calculate Audit courses
--			  Modified exclusionInd field
--			  Added inclusionInd placeholder field
--			  Add enrolledInd field and join fo StudentCohortRefactor to determine if student enrolled in Fall cohort

RetentionStudentCohort as (
--View used to build the reportable retention student cohort. (CohortInd = 'Retention Fall')
--An indicator 'IPEDSinclude' is built out in this view to indicate if a student cohort record meets IPEDS reporting 
--specs to be included. 

--jh 20200422 added filters for retention requirements, removed excess fields

	select CohView.personId personId,
	        case when CohView.totalHrs >= CohView.requiredUGhrs then 'Full' else 'Part' end timeStatus,
	        CohView.exclusionInd exclusionInd,
			CohView.inclusionInd inclusionInd,
	        CohView.enrolledInd enrolledInd
	from (
		select reg.personId personId,
				case when excl.personId is not null then 1 else null end exclusionInd,
--ak 20200429 adding inclusion determination code (PF-1435)
                case when stuReg.studentStatus = 'Active' then null --only look at Study Abroad status
					 when fallCohort.personId is not null then 1 --student enrolled in second year Fall semester
					 when incl.personId is not null then 1 --student enrolled in second year Summer semester 
					 else null 
				end inclusionInd,
				case when fallCohort.personId is not null then 1 else null end enrolledInd,
-- ak 20200519 Adding FT/PT credit hour comparison points (PF-1480)
				NVL(term.requiredFTCreditHoursUG, 12) requiredUGhrs,
				courseCnt.totalHrs totalHrs,
-- ak 20200330 Removed Continuing ed - it is no longer considered a student type (PF-1382)
--jh 20200422 if cohort student type is not first time, check is summer is. If not, value is student type
				case when stuReg.studentType = 'First Time' then 'First Time'
				     when stuReg.personId = reportType.personId then 'First Time'
					 else stuReg.studentType
				end studentType,
				--confirm group and assign student levels based on IPEDS specs.
				case when stuReg.studentLevel = 'Continuing Ed' and defaultValues.includeNonDegreeAsUG = 'Y' then 'UnderGrad' 
					when stuReg.studentLevel in ('Professional', 'Postgraduate') then 'Graduate' 
					else stuReg.studentLevel 
				end studentLevel,
				stuReg.studentStatus studentStatus,
			    deg.awardLevel awardLevel,
			    --confirm if students should be excluded from the cohort based on IPEDS specs.
			    case 
--jh 20200422 changed courseCnt.totalCredCourses to courseCnt.totalCourses
					when courseCnt.totalRemCourses = courseCnt.totalCourses 
						and stuReg.isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
					when courseCnt.totalCredCourses > 0 --exclude students not enrolled for credit
					then (case when courseCnt.totalESLCourses = courseCnt.totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
							   when courseCnt.totalCECourses = courseCnt.totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
							   when courseCnt.totalIntlCourses = courseCnt.totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
-- ak 20200330 added Registration.crnGradingMode (PF-1253)
							   when courseCnt.totalAuditCourses = courseCnt.totalCredCourses then 0 --exclude students exclusively auditing classes
							   -- when... then 0 --exclude PHD residents or interns
							   -- when... then 0 --students studying abroad if enrollment at home institution is an admin only record
							   -- when... then 0 --exclude students in experimental Pell programs
							   else 1
						  end)

				 else 0 
			end ipedsInclude
		from ConfigPerAsOfDate defaultValues
			inner join RegPerTermAsOfCensus reg on reg.cohortInd = defaultValues.sectionRetFall
			inner join PersonPerReg person on reg.personId = person.personId
				and person.cohortInd = reg.cohortInd
			inner join StudentPerTermReg stuReg on reg.personId = stuReg.personId
				and stuReg.cohortInd = reg.cohortInd
-- ak 20200429 adding student status filter				
				and stuReg.studentStatus in ('Active', 'Study Abroad')
-- ak 20200427 adding Degree entity to filter retention cohort on Degree.awardLevel (PF-1434)
			inner join DegreePerAcadTrack deg on deg.personId = stuReg.personId
				and stuReg.cohortInd = deg.cohortInd
			left join CourseCountsPerStudent courseCnt on reg.personId = courseCnt.personId
				and courseCnt.cohortInd = reg.cohortInd
			inner join AcadTermLatestRecord term on term.termCode = reg.termCode
			left join RetentionStuReportableType reportType on reportType.personId = reg.personId 
			left join StudentRetentionExclusions excl on excl.personId = reg.personId
			left join StudentCohortRefactor fallCohort on fallCohort.cohortInd = defaultValues.sectionFall
			left join StudentRetentionInclusions incl on reg.personId = incl.personId
		) as CohView
where CohView.ipedsInclude = 1
--Retention is first-time Bachelor's degree seeking ONLY 
    and CohView.awardLevel = "Bachelor's Degree"
	and CohView.studentLevel = 'Undergrad'
	and CohView.studentType = 'First Time'
-- ak 20200429 adding student status filter
	and (CohView.studentStatus = 'Active'
        or CohView.inclusionInd = 1)
),

/*****
BEGIN SECTION - Student-to-Faculty Ratio Calculation
The views below pull the Instuctor count and calculates the Student-to-Faculty Ratio 
*****/

EmployeePerAsOfDate as (
--returns all employees based on HR asOfDate

select *
from (
		select employee.*,
				ReportingDates.asOfDate asOfDate,
				ReportingDates.termCodeFall termCodeFall,
				row_number() over (
					partition by
						employee.personId
					order by
						employee.recordActivityDate desc
				) employeeRn
		from HRReportingPeriod ReportingDates
			cross join Employee employee
--jh 20200422 Including dummy date changes. (PF-1368)
		where ((employee.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				    and employee.recordActivityDate <= ReportingDates.asOfDate)
			            or employee.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
				and ((employee.terminationDate IS NULL) 
					or (employee.terminationDate > ReportingDates.asOfDate
						and employee.hireDate <= ReportingDates.asOfDate))
				and employee.isIpedsReportable = 1 
	)
where employeeRn = 1
),

PrimaryJobPerAsOfDate as (
--returns all employee assignments for employees applicable for IPEDS requirements

select *
from (
		select job.*,
				employee.asOfDate asOfDate,
				row_number() over (
					partition by
						job.personId,
						job.position,
						job.suffix
					order by
						job.recordActivityDate desc
				) jobRn
		from EmployeePerAsOfDate employee
			inner join EmployeeAssignment job on job.personId = employee.personId
--ak 20200406 Including dummy date changes. (PF-1368)
                and ((job.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				    and job.recordActivityDate <= employee.asOfDate)
			            or job.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
				and job.assignmentStartDate <= employee.asOfDate
				and (job.assignmentEndDate IS NULL 
					or job.assignmentEndDate >= employee.asOfDate)
				and job.isUndergradStudent = 0
				and job.isWorkStudy = 0
				and job.isTempOrSeasonal = 0
				and job.isIpedsReportable = 1 
				and job.assignmentType = 'Primary'
	)
where jobRn = 1
),

PositionPerAsOfDate as (
--returns ESOC (standardOccupationalCategory) for all employee assignments

select *
from (
		select position.*,
				row_number() over (
					partition by
						position.position
					order by
						--position.startDate desc,
						position.recordActivityDate desc
				) positionRn
		from PrimaryJobPerAsOfDate job
			inner join EmployeePosition position on position.position = job.position
--ak 20200406 Including dummy date changes. (PF-1368)
                and ((position.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				    and position.recordActivityDate <= job.asOfDate)
			            or position.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
				and position.startDate <= job.asOfDate
				and (position.endDate IS NULL 
					or position.endDate >= job.asOfDate)
				and position.isIpedsReportable = 1 
	)
where positionRn = 1
),

--jh 20200422 added fields to row_number function

InstrAssignPerAsOfDate as (
--returns all instructional assignments for all employees

select *
from (
		select instruct.*,
				employee.asOfDate asOfDate,
				row_number() over (
					partition by
						instruct.personId,
						instruct.termCode,
						instruct.partOfTermCode,
						instruct.crn,
						instruct.section
					order by
						instruct.recordActivityDate desc
				) jobRn
		from EmployeePerAsOfDate employee
			inner join InstructionalAssignment instruct on instruct.personId = employee.personId
--ak 20200406 Including dummy date changes. (PF-1368)
                and ((instruct.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				    and instruct.recordActivityDate <= employee.asOfDate)
			            or instruct.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
				and instruct.termCode = employee.termCodeFall
				and instruct.isIpedsReportable = 1 --true
	)
where jobRn = 1
),

--jh 20200422 added section and partOfTermCode to course join

CreditCoursesPerEmployee as (
--view used to break down course category type counts by type

select instruct.personId personId,
		sum(case when course.crn is not null then 1 else 0 end) totalCourses,
        sum(case when course.creditHrs = 0 then 1 else 0 end) totalNonCredCourses,
        sum(case when course.creditHrs > 0 then 1 else 0 end) totalCredCourses,
        sum(case when course.courseLevel = 'Continuing Ed' then 1 else 0 end) totalCECourses
from InstrAssignPerAsOfDate instruct
    left join CoursePerTerm course on course.crn = instruct.crn
		and course.section = instruct.section
		and course.termCode = instruct.termCode
		and course.partOfTermCode = instruct.partOfTermCode
group by instruct.personId
),

EmployeeBase as (
--view used to build the employee cohort to calculate instructional full time equivalency

select employee.personId personId,
        nvl(job.fullOrPartTimeStatus, 'Full Time') fullOrPartTimeStatus,
        case when employee.primaryFunction in (
					'Instruction with Research/Public Service',
					'Instruction - Credit',
					'Instruction - Non-Credit',
					'Instruction - Combined Credit/Non-credit'
				) then 1
		     when position.standardOccupationalCategory = '25-1000' then 1
		     else 0
	    end isInstructional,
	    credCourses.totalCourses totalCourses,
	    credCourses.totalCredCourses totalCredCourses,
	    credCourses.totalNonCredCourses totalNonCredCourses,
	    credCourses.totalCECourses totalCECourses
from EmployeePerAsOfDate employee
	left join PrimaryJobPerAsOfDate job on employee.personId = job.personId
	left join PositionPerAsOfDate position on job.position = position.position
	left join CreditCoursesPerEmployee credCourses on employee.personId = credCourses.personId
where employee.isIpedsMedicalOrDental = 0
),

InstructionalFTE as (
--instructional full time equivalency calculation
--**-add exclusions for teaching exclusively in stand-alone graduate or professional programs

select round(FTInst + ((PTInst + PTNonInst) * 1/3), 2) FTE
from (
		select sum(case when base.fullOrPartTimeStatus = 'Full Time' 
							and base.isInstructional = 1 --instructional staff
							and ((base.totalNonCredCourses < base.totalCourses) --not exclusively non credit
								or (base.totalCECourses < base.totalCourses) --not exclusively CE (non credit)
								or (base.totalCourses IS NULL)) then 1 
						else 0 
					end) FTInst, --FT instructional staff not teaching exclusively non-credit courses
			   sum(case when base.fullOrPartTimeStatus = 'Part Time' 
							and base.isInstructional = 1 --instructional staff
							and ((base.totalNonCredCourses < base.totalCourses) --not exclusively non credit
								or (base.totalCECourses < base.totalCourses) --not exclusively CE (non credit)
								or (base.totalCourses IS NULL)) then 1 
						else 0 
					end) PTInst, --PT instructional staff not teaching exclusively non-credit courses
			   sum(case when base.isInstructional = 0 
							and nvl(base.totalCredCourses, 0) > 0 then 1 
						else 0 
					end) PTNonInst --administrators or other staff not reported in the HR as instructors who are teaching a credit course in Fall 2019
		from EmployeeBase base 
	)
),

StudentFTE as (
--student full time equivalency calculation
--**add exclusions for teaching exclusively in stand-alone graduate or professional programs

select round(FTStud + (PTStud * 1/3), 2) FTE
from (
		select sum(case when cohort.timeStatus = 'Full' then 1 else 0 end) FTStud,
			   sum(case when cohort.timeStatus = 'Part' then 1 else 0 end) PTStud
		from StudentCohort cohort
    )
),

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/
--jh 20200423 Broke up ipedsLevel into PartG, PartB and PartA

--Part A	
FormatPartAStudentLevel as (
select *
from (
		VALUES 
			(1), --Full-time, first-time degree/certificate-seeking undergraduate
			(2), --Full-time, transfer-in degree/certificate-seeking undergraduate
			(3), --Full-time, continuing degree/certificate-seeking undergraduate
			(7), --Full-time, non-degree/certificate-seeking undergraduate
			(11), --Full-time graduate
			(15), --Part-time, first-time degree/certificate-seeking undergraduate
			(16), --Part-time, transfer-in degree/certificate-seeking undergraduate
			(17), --Part-time, continuing degree/certificate-seeking undergraduate
			(21), --Part-time, non-degree/certificate-seeking undergraduate
			(25)  --Part-time graduate
	) as StudentLevel(ipedsLevel)
),

FormatFallEnrlCipCodeGroup as (
select *
from (
		VALUES 
			('13.0000'),
			('14.0000'),
			('26.0000'),
			('27.0000'),
			('40.0000'),
			('52.0000'),
			('22.0101'),
			('51.0401'),
			('51.1201')
	) as CipCodeGroup(ipedsCipCodeGroup)
),

--Part G
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'

FormatPartGStudentLevel as (
select *
from (
	VALUES 
		(1), --Degree/Certificate seeking undergraduate students
		(2), --Non-Degree/Certificate seeking undergraduate Students
		(3)  --Graduate students
	) as StudentLevel(ipedsLevel)

),

--Part B
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'
--jh 20200422 modified ipedsLevel based on requirements for Part B
          
FormatPartBStudentLevel as (
select *
from (
	VALUES 
		(1), --Undergraduate students
		(3)  --Graduate students
	) as StudentLevel(ipedsLevel)

),

--Part B
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'					  
FormatPartBAgeGroup as (
select * 
from (
		VALUES 
			(1), --1 - Full-time, under 18
			(2), --2 - Full-time, 18-19
			(3), --3 - Full-time, 20-21
			(4), --4 - Full-time, 22-24
			(5), --5 - Full-time, 25-29
			(6), --6 - Full-time, 30-34
			(7), --7 - Full-time, 35-39
			(8), --8 - Full-time, 40-49
			(9), --9 - Full-time, 50-64
			(10), --10 - Full-time, 65 and over
			(13), --13 - Part-time, under 18
			(14), --14 - Part-time, 18-19
			(15), --15 - Part-time, 20-21
			(16), --16 - Part-time, 22-24
			(17), --17 - Part-time, 25-29
			(18), --18 - Part-time, 30-34
			(19), --19 - Part-time, 35-39
			(20), --20 - Part-time, 40-49
			(21), --21 - Part-time, 50-64
			(22) --22 - Part-time, 65 and over
	) as AgeGroup(ipedsAgeGroup)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs


fields are used as follows...
Part                      - Should reflect the appropriate survey part
sortorder                 - Numeric field that is used to overide 'part' for ordering the output. IPEDS specs request ordering of parts in input file that are not alphabetic.
                            The fall enrollment survey is reported differently in odd years than in even. In the year to follow, additional union queries will need to be added in 'Part A'. 
                            These union queries should get be given VALUES in the 'sortorder' field that increment by tenths (1.1, 1.2 ...) to maintain ordering control. 
Field1                    - Used and reserved for CIPCodes. Part A needs to be ordered by CIPCodes so field 1 is reserved for ordering. The value of Field1 will be null where the value
                            should not impact ordering of the result set.
Field2 through Field20    - Fields2 through field 20 are used for numeric VALUES that will be impacted by the grouping/ordering of sortorder and/or Field1
*****/

-- Part A: Fall Enrollment by Student Level, Attendance Status, Race/Ethnicity, and Gender
--undergraduate and graduate

select 'A' part,
		1 sortorder,
		'99.0000' field1, --Classification of instructional program (CIP) code default (99.0000) for all institutions. 'field1' will be utilized for client CIPCodes every other reporting year
		ipedsLevel field2, -- Student level, 1,2,3,7,11,15,16,17,21, and 25 (refer to student level table (Part A) in appendix) (6,8,14,20,22, 28, 29 and 99 are for export only.)
		coalesce(sum(case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end), 0) field3, -- Nonresident alien - Men (1), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end), 0) field4, -- Nonresident alien - Women (2), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end), 0) field5, -- Hispanic/Latino - Men (25), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end), 0) field6, -- Hispanic/Latino - Women (26), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end), 0) field7, -- American Indian or Alaska Native - Men (27), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end), 0) field8, -- American Indian or Alaska Native - Women (28), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end), 0) field9, -- Asian - Men (29), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end), 0) field10, -- Asian - Women (30), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end), 0) field11, -- Black or African American - Men (31), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end), 0) field12, -- Black or African American - Women (32), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end), 0) field13, -- Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end), 0) field14, -- Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end), 0) field15, -- White - Men (35), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end), 0) field16, -- White - Women (36), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end), 0) field17, -- Two or more races - Men (37), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end), 0) field18, -- Two or more races - Women (38), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end), 0) field19, -- Race and ethnicity unknown - Men (13), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end), 0) field20 -- Race and ethnicity unknown - Women (14), 0 to 999999
from (
	select StuCohRef.personId personId,
			StuCohRef.ipedsPartAStudentLevel ipedsLevel,
			StuCohRef.ipedsGender ipedsGender,
			StuCohRef.ipedsEthnicity ipedsEthnicity
	from StudentCohortRefactor StuCohRef
	where StuCohRef.ipedsPartAStudentLevel is not null

	
	union
	
	select NULL, --personId,
			StudentLevel.ipedsLevel,
			NULL, -- ipedsGender,
			NULL -- ipedsEthnicity
	from FormatPartAStudentLevel StudentLevel
	)
group by ipedsLevel

union

--Part G: Distance Education Status
--undergraduate and graduate

select 'G', -- part,
		'2', -- sortorder,
		NULL, -- field1,
		ipedsLevel, -- field2, --1,2 and 3 (refer to student level table (Part G) in appendix)
		sum(case when distanceEdInd = 'Exclusive DE' then 1 else 0 end), -- field3,-- Enrolled exclusively in distance education courses 0 to 999999
		sum(case when distanceEdInd = 'Some DE' then 1 else 0 end), -- field4,-- Enrolled in some but not all distance education courses 0 to 999999
		sum(case when distanceEdInd = 'Exclusive DE' and residentStatus = 'IN' then 1 else 0 end), -- field5,-- Of those students exclusively enrolled in de courses - Located in the state/jurisdiction of institution 0 to 999999
		sum(case when distanceEdInd = 'Exclusive DE' and isInAmerica = true and residentStatus = 'OUT' then 1 else 0 end), -- field6,-- Of those students exclusively enrolled in de courses - Located in the U.S. but not in state/jurisdiction of institution 0 to 999999
		sum(case when distanceEdInd = 'Exclusive DE' and isInAmerica = true and residentStatus = 'UNKNOWN' then 1 else 0 end), -- field7,-- Of those students exclusively enrolled in de courses - Located in the U.S. but state/jurisdiction unknown 0 to 999999
		sum(case when distanceEdInd = 'Exclusive DE' and isInAmerica = false then 1 else 0 end), -- field8,-- Of those students exclusively enrolled in de courses - Located outside the U.S. 0 to 999999
		NULL, -- field9,
		NULL, -- field10,
		NULL, -- field11,
		NULL, -- field12,
		NULL, -- field13,
		NULL, -- field14,
		NULL, -- field15,
		NULL, -- field16,
		NULL, -- field17,
		NULL, -- field18,
		NULL, -- field19,
		NULL  -- field20
from (

-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'	
	select StuCohRef.personId personId,
			StuCohRef.ipedsPartGStudentLevel ipedsLevel,
			StuCohRef.distanceEdInd distanceEdInd,
			StuCohRef.residentStatus residentStatus,
			StuCohRef.isInAmerica
	from StudentCohortRefactor StuCohRef
	
	union
	
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'	
	select NULL, --personId,
			StudentLevel.ipedsLevel,
			0, --distanceEdInd,
			NULL, -- residentStatus,
			0 --isInAmerica
	from FormatPartGStudentLevel StudentLevel
	)
group by ipedsLevel

union

--Part B - Fall Enrollment by Age and Gender
--**(Part B is mandatory in this collection)**
--undergraduate and graduate

select 'B', -- part,
		'3', -- sortorder,
		NULL, -- field1,
		ipedsLevel, -- field2, --1 and 3 (refer to student level table (Part B)) in appendix
		ipedsAgeGroup, -- field3, --1–24 (refer to age category table in appendix) (11, 12, 23, and 24 are for export only)
		sum(case when ipedsGender = 'M' then 1 else 0 end), -- field4, --Men, 0 to 999999 
		sum(case when ipedsGender = 'F' then 1 else 0 end), -- field5, --Female, 0 to 999999
		NULL, -- field6,
		NULL, -- field7,
		NULL, -- field8,
		NULL, -- field9,
		NULL, -- field10,
		NULL, -- field11,
		NULL, -- field12,
		NULL, -- field13,
		NULL, --  field14,
		NULL, --  field15,
		NULL, --  field16,
		NULL, --  field17,
		NULL, --  field18,
		NULL, --  field19,
		NULL  -- field20
from (

-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'
	select StuCohRef.personId personId,
	    StuCohRef.ipedsPartBStudentLevel ipedsLevel,
		StuCohRef.ipedsAgeGroup ipedsAgeGroup,
		StuCohRef.ipedsGender
	from StudentCohortRefactor StuCohRef
	
	union
	
	select NULL, -- personId,
		StudentLevel.ipedsLevel, 
		AgeGroup.ipedsAgeGroup, 
		NULL -- ipedsGender
	from FormatPartBStudentLevel StudentLevel
		 cross join FormatPartBAgeGroup AgeGroup
    )
where ipedsAgeGroup is not null
--use client config value to apply optional/required survey section logic.
	and (select config.reportAge
		 from ConfigPerAsOfDate config) = 'Y'
group by ipedsLevel,
		 ipedsAgeGroup
		 
union

--Part C: Residence of First-Time Degree/Certificate-Seeking Undergraduate Students
--**(Part C is optional in this collection)**
--undergraduate ONLY

--jh 20200422 Removed extra select statement

select 'C', -- part,
		4, -- sortorder,
		NULL, -- field1,
		StuCohRef.ipedsStateCode, -- field2, --State of residence, 1, 2, 4–6, 8–13, 15–42, 44–51, 53–57, 60, 64, 66, 68–70, 72, 78, 90 (valid FIPS codes, refer to state table in appendix) (98 and 99 are for export only)
		case when count(StuCohRef.personId) > 0 then count(StuCohRef.personId) else 1 end, -- field3, --Total first-time degree/certificate-seeking undergraduates, 1 to 999999 ***error in spec - not allowing 0
		nvl(sum(case when date_add(StuCohRef.hsGradDate, 365) >= StuCohRef.censusDate then 1 else 0 end), 0), -- field4, --Total first-time degree/certificate-seeking undergraduates who enrolled within 12 months of graduating high school 0 to 999999 
		NULL, -- field5,
		NULL, -- field6,
		NULL, -- field7,
		NULL, -- field8,
		NULL, -- field9,
		NULL, -- field10,
		NULL, -- field11,
		NULL, -- field12,
		NULL, -- field13,
		NULL, -- field14,
		NULL, -- field15,
		NULL, -- field16,
		NULL, -- field17,
		NULL, -- field18,
		NULL, -- field19,
		NULL  -- field20
from StudentCohortRefactor StuCohRef
	where StuCohRef.studentType = 'First Time'
		and StuCohRef.studentLevel = 'Undergrad' --all versions
--use client config value to apply optional/required survey section logic.
		and StuCohRef.reportResidency = 'Y'
group by StuCohRef.ipedsStateCode

union

--Part D: Total Undergraduate Entering Class
--**This section is only applicable to degree-granting, academic year reporters (calendar system = semester, quarter, trimester, 
--**or 4-1-4) that offer undergraduate level programs and reported full-time, first-time degree/certificate seeking students in Part A.
--undergraduate ONLY
--acad reporters ONLY

select 'D', -- part,
		5, -- sortorder,
		NULL, -- field1,
		case when Count(StuCohRef.personId) = 0 then 1 else Count(StuCohRef.personId) end, -- field2, --Total number of non-degree/certificate seeking undergraduates that are new to the institution in the Fall 1 to 999999 ***error in spec - not allowing 0
		NULL, -- field3,
		NULL, -- field4,
		NULL, -- field5,
		NULL, -- field6,
		NULL, -- field7,
		NULL, -- field8,
		NULL, -- field9,
		NULL, -- field10,
		NULL, -- field11,
		NULL, -- field12,
		NULL, -- field13,
		NULL, -- field14,
		NULL, -- field15,
		NULL, -- field16,
		NULL, -- field17,
		NULL, -- field18,
		NULL, -- field19,
		NULL  -- field20
from StudentCohortRefactor StuCohRef
where StuCohRef.studentType in ('First Time', 'Transfer')
	and StuCohRef.studentLevel = 'Undergrad' --all versions
	and StuCohRef.acadOrProgReporter = 'A'

union

--jh 20200422 removed extra select and modified exclusionInd, inclusionInd and enrolledInd code

--Part E: First-time Bachelor's Cohort Retention Rates

select 'E', -- part,
		6, -- sortorder,
		NULL, -- field1,
		case when Ftft = 0 then 1 else Ftft end, -- field2, --Full-time, first-time bachelor's cohort, 1 to 999999 ***error in spec - not allowing 0
		case when FtftEx = 0 then 1 else FtftEx end, -- field3, --Full-time, first-time bachelor's cohort exclusions, 1 to 999999 ***error in spec - not allowing 0
		case when FtftIn = 0 then 1 else FtftIn end, -- field4, --Full-time, first-time bachelor's cohort inclusions, 1 to 999999 ***error in spec - not allowing 0
		case when FtftEn = 0 then 1 else FtftEn end, -- field5, --Full-time, first-time bachelor's cohort students still enrolled in current fall term, 1 to 999999 ***error in spec - not allowing 0
		case when Ptft = 0 then 1 else Ptft end, -- field6, --Part-time, first-time bachelor's cohort, 1 to 999999 ***error in spec - not allowing 0
		case when PtftEx = 0 then 1 else PtftEx end, -- field7, --Part-time, first-time bachelor's cohort exclusions, 1 to 999999 ***error in spec - not allowing 0
		case when PtftIn = 0 then 1 else PtftIn end, -- field8, --Part-time, first-time bachelor's cohort inclusions, 1 to 999999 ***error in spec - not allowing 0
		case when PtftEn = 0 then 1 else PtftEn end, -- field9, --Part-time, first-time bachelor's cohort students still enrolled in current fall term, 1 to 999999 ***error in spec - not allowing 0
		NULL, -- field10,
		NULL, -- field11,
		NULL, -- field12,
		NULL, -- field13,
		NULL, -- field14,
		NULL, -- field15,
		NULL, -- field16,
		NULL, -- field17,
		NULL, -- field18,
		NULL, -- field19,
		NULL  -- field20
from (
	select nvl(sum(case when timeStatus = 'Full'
	                            and exclusionInd is null 
	                            and inclusionInd is null 
	                   then 1 else 0 end), 0) Ftft, --Full-time, first-time bachelor's cohort, 1 to 999999
			nvl(sum(case when timeStatus = 'Full' and exclusionInd = 1 then 1 end), 0) FtftEx, --Full-time, first-time bachelor's cohort exclusions, 1 to 999999
			nvl(sum(case when timeStatus = 'Full' and inclusionInd = 1 then 1 end), 0) FtftIn, --Full-time, first-time bachelor's cohort inclusions, 1 to 999999
			nvl(sum(case when timeStatus = 'Full' and enrolledInd = 1 then 1 end), 0) FtftEn, --Full-time, first-time bachelor's cohort students still enrolled in current fall term, 1 to 999999
			nvl(sum(case when timeStatus = 'Part' 
	                            and exclusionInd is null 
	                            and inclusionInd is null 
			            then 1 else 0 end), 0) Ptft, --Part-time, first-time bachelor's cohort, 1 to 999999
			nvl(sum(case when timeStatus = 'Part' and exclusionInd = 1 then 1 end), 0) PtftEx, --Part-time, first-time bachelor's cohort exclusions, 1 to 999999
			nvl(sum(case when timeStatus = 'Part' and inclusionInd = 1 then 1 end), 0) PtftIn, --Part-time, first-time bachelor's cohort inclusions, 1 to 999999
			nvl(sum(case when timeStatus = 'Part' and enrolledInd = 1 then 1 end), 0) PtftEn
	from RetentionStudentCohort
)

union

--jh 20200422 Moved inline views to from section instead of in select field

--Part F: Student-to-Faculty Ratio

select 'F', -- part,
		7, -- sortorder,
		NULL, -- field1,
		cast(ROUND(NVL(StudentFTE.FTE, 0)/NVL(InstructionalFTE.FTE, 1)) as int), -- field2, --Student-to-faculty ratio, 0 - 100
		NULL, -- field3,
		NULL, -- field4,
		NULL, -- field5,
		NULL, -- field6,
		NULL, -- field7,
		NULL, -- field8,
		NULL, -- field9,
		NULL, -- field10,
		NULL, -- field11,
		NULL, -- field12,
		NULL, -- field13,
		NULL, -- field14,
		NULL, -- field15,
		NULL, -- field16,
		NULL, -- field17,
		NULL, -- field18,
		NULL, -- field19,
		NULL  -- field20
		from StudentFTE StudentFTE
	        cross join InstructionalFTE InstructionalFTE
