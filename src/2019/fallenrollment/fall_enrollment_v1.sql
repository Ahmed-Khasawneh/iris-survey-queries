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
20200706		akhasawneh				ak 20200706			Added new Person fields visaStartDate and visaEndDate
															Change curriculumRuleActivityDate to curriculumRuleActionDate in both Major and Degree
															Changed registrationStatusActionDate to registrationStatusActionDate (PF-1536) -Run time 13m 36s
20200618		akhasawneh				ak 20200618			Modify FE report query with standardized view naming/aliasing convention (PF-1531) -Run time 13m 06s
20200611        akhasawneh              ak 20200611         Modified to not reference term code as a numeric indicator of term ordering (PF-1494) -Run time 12m 13s
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
	'F' genderForNonBinary,  --M = Male, F = Female
    'CR' instructionalActivityType --CR = Credit, CL = Clock, B = Both

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
	'F' genderForNonBinary,  --M = Male, F = Female
    'CR' instructionalActivityType --CR = Credit, CL = Clock, B = Both
*/
),

--jh 20200422 Added default values to ConfigPerAsOfDate and moved before ReportingPeriod

ClientConfigMCR as (
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
	ConfigLatest.asOfDateHR asOfDateHR,
	ConfigLatest.instructionalActivityType instructionalActivityType 
from (
	select clientconfigENT.surveyCollectionYear surveyYear,
		NVL(clientconfigENT.acadOrProgReporter, defvalues.acadOrProgReporter) acadOrProgReporter,
		NVL(clientconfigENT.feIncludeOptSurveyData, defvalues.feIncludeOptSurveyData) feIncludeOptSurveyData,
		NVL(clientconfigENT.includeNonDegreeAsUG, defvalues.includeNonDegreeAsUG) includeNonDegreeAsUG,
		case when NVL(clientconfigENT.acadOrProgReporter, defvalues.acadOrProgReporter) = 'P' then clientconfigENT.sfaLargestProgCIPC else NULL end sfaLargestProgCIPC,
		NVL(clientconfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		NVL(clientconfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
		defvalues.surveyId surveyId,
		defvalues.sectionFall sectionFall,
		defvalues.sectionRetFall sectionRetFall,
		defvalues.sectionSummer sectionSummer,
		defvalues.sectionRetSummer sectionRetSummer,
		defvalues.termCodeFall termCodeFall, 
		defvalues.termCodeSummer termCodeSummer, 
		defvalues.termCodeRetFall termCodeRetFall, 
		defvalues.termCodeRetSummer termCodeRetSummer, 
		defvalues.censusDateFall censusDateFall,
		defvalues.censusDateSummer censusDateSummer, 
		defvalues.censusDateRetFall censusDateRetFall, 
		defvalues.censusDateRetSummer censusDateRetSummer,
		defvalues.partOfTermCode partOfTermCode,
		defvalues.surveyIdHR surveyIdHR,
		defvalues.asOfDateHR asOfDateHR,
		defvalues.instructionalActivityType instructionalActivityType,
		row_number() over (
			partition by
				clientconfigENT.surveyCollectionYear
			order by
				clientconfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientconfigENT
		cross join DefaultValues defvalues
	where clientconfigENT.surveyCollectionYear = defvalues.surveyYear
		
	union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select defvalues.surveyYear surveyYear,
		defvalues.acadOrProgReporter acadOrProgReporter,
		defvalues.feIncludeOptSurveyData feIncludeOptSurveyData,
		defvalues.includeNonDegreeAsUG includeNonDegreeAsUG,
		null sfaLargestProgCIPC,
		defvalues.genderForUnknown genderForUnknown,
		defvalues.genderForNonBinary genderForNonBinary,
		defvalues.surveyId surveyId,
		defvalues.sectionFall sectionFall,
		defvalues.sectionRetFall sectionRetFall,
		defvalues.sectionSummer sectionSummer,
		defvalues.sectionRetSummer sectionRetSummer,
		defvalues.termCodeFall termCodeFall, 
		defvalues.termCodeSummer termCodeSummer, 
		defvalues.termCodeRetFall termCodeRetFall, 
		defvalues.termCodeRetSummer termCodeRetSummer, 
		defvalues.censusDateFall censusDateFall,
		defvalues.censusDateSummer censusDateSummer, 
		defvalues.censusDateRetFall censusDateRetFall, 
		defvalues.censusDateRetSummer censusDateRetSummer,
		defvalues.partOfTermCode partOfTermCode,
		defvalues.surveyIdHR surveyIdHR,
		defvalues.asOfDateHR asOfDateHR, 
        defvalues.instructionalActivityType instructionalActivityType,
		1 configRn
	from DefaultValues defvalues
	where defvalues.surveyYear not in (select clientconfigENT.surveyCollectionYear
										from IPEDSClientConfig clientconfigENT
										where clientconfigENT.surveyCollectionYear = defvalues.surveyYear)
	) ConfigLatest
where ConfigLatest.configRn = 1
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for each term and part of term code. 
--PartOfTerm code defines a subcategory of a termCode that may have different start, end and census dates. 

select *
from ( 
	select acadTermENT.*,
		row_number() over (
			partition by 
				acadTermENT.termCode,
				acadTermENT.partOfTermCode
			order by  
				acadTermENT.recordActivityDate desc
		) acadTermRn
	from AcademicTerm acadTermENT
	where acadTermENT.isIPEDSReportable = 1 
	)
where acadTermRn = 1
),

--ak 20200616 Adding term order indicator (PF-1494)
AcadTermOrder as (
-- Orders term codes based on date span and keeps the numeric value of the greatest term/part of term record. 

select termCode termCode, 
    max(termOrder) termOrder
from (
	select acadterm.termCode termCode,
	   row_number() over (
			order by  
				acadterm.startDate asc,
				acadterm.endDate asc
        ) termOrder    
	from AcademicTermMCR acadterm
	)
group by termCode
),

--jh 20200422 Added default values from ConfigPerAsOfDate and reduced select statements

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 
--Need current survey terms ('Fall', 'Summer') and prior year retention ('RetFall', 'RetSummer')
--Introduces 'cohortInd' which is used through out the query to associate records with the appropriate cohort. 

select RepDates.surveyYear surveyYear,
	RepDates.surveySection cohortInd,
	RepDates.termCode termCode,	
--ak 20200611 Added term order to do time based term comparisons (PF-1494)
	termorder.termOrder termOrder,
	RepDates.partOfTermCode partOfTermCode,	
	NVL(NVL(acadterm.censusDate, DATE_ADD(acadterm.startDate, 15)), RepDates.censusDate) censusDate,
	RepDates.acadOrProgReporter acadOrProgReporter,
	RepDates.feIncludeOptSurveyData feIncludeOptSurveyData,
	RepDates.includeNonDegreeAsUG includeNonDegreeAsUG,
	RepDates.genderForUnknown genderForUnknown,
	RepDates.genderForNonBinary genderForNonBinary,
-- ak 20200519 adding consideration for credit vs clock hour (PF-1480)
	NVL(acadterm.requiredFTCreditHoursUG/
		NVL(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor
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
		select repperiodENT.surveyCollectionYear surveyYear,
			repperiodENT.surveySection surveySection,
			repperiodENT.termCode termCode,
			case when repperiodENT.surveySection = clientconfig.sectionFall then clientconfig.censusDateFall
				when repperiodENT.surveySection = clientconfig.sectionSummer then clientconfig.censusDateSummer
				when repperiodENT.surveySection = clientconfig.sectionRetFall then clientconfig.censusDateRetFall
				when repperiodENT.surveySection = clientconfig.sectionRetSummer then clientconfig.censusDateRetSummer
			end censusDate,
			repperiodENT.partOfTermCode partOfTermCode,
			clientconfig.acadOrProgReporter acadOrProgReporter,
			clientconfig.feIncludeOptSurveyData feIncludeOptSurveyData,
			clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
			clientconfig.genderForUnknown genderForUnknown,
			clientconfig.genderForNonBinary genderForNonBinary,
			row_number() over (	
				partition by 
					repperiodENT.surveyCollectionYear,
					repperiodENT.surveyId,
					repperiodENT.surveySection,
					repperiodENT.termCode,
					repperiodENT.partOfTermCode	
				order by repperiodENT.recordActivityDate desc
			) reportPeriodRn	
		from IPEDSReportingPeriod repperiodENT
			cross join ClientConfigMCR clientconfig
		where repperiodENT.surveyCollectionYear = clientconfig.surveyYear
			and repperiodENT.surveyId = clientconfig.surveyId
			and repperiodENT.termCode is not null
			and repperiodENT.partOfTermCode is not null
		)
	where reportPeriodRn = 1
	
	--ak 20200122 added unions to use default values when IPEDSReportingPeriod record doesn't exist so valid output file will always be generated
	
	union
	
	--Pulls default values for Fall when IPEDSReportingPeriod record doesn't exist
	select clientconfig.surveyYear surveyYear,
		clientconfig.sectionFall surveySection,
		clientconfig.termCodeFall termCode,
		clientconfig.censusDateFall censusDate,
		clientconfig.partOfTermCode partOfTermCode,
		clientconfig.acadOrProgReporter acadOrProgReporter,
		clientconfig.feIncludeOptSurveyData feIncludeOptSurveyData,
		clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary
	from ClientConfigMCR clientconfig
	where clientconfig.surveyYear not in (select repperiodENT1.surveyCollectionYear
										  from ClientConfigMCR clientconfig1
											cross join IPEDSReportingPeriod repperiodENT1
										  where repperiodENT1.surveyCollectionYear = clientconfig1.surveyYear
											and repperiodENT1.surveyId = clientconfig1.surveyId
											and repperiodENT1.surveySection = clientconfig1.sectionFall
											and repperiodENT1.termCode is not null
											and repperiodENT1.partOfTermCode is not null)
	
	union
	
	--Pulls default values for Retention Fall when IPEDSReportingPeriod record doesn't exist
	select clientconfig.surveyYear surveyYear,
		clientconfig.sectionRetFall surveySection,
		clientconfig.termCodeRetFall termCode,
		clientconfig.censusDateRetFall censusDate,
		clientconfig.partOfTermCode partOfTermCode,
		clientconfig.acadOrProgReporter acadOrProgReporter,
		clientconfig.feIncludeOptSurveyData feIncludeOptSurveyData,
		clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary
	from ClientConfigMCR clientconfig
	where clientconfig.surveyYear not in (select repperiodENT1.surveyCollectionYear
										  from ClientConfigMCR clientconfig1
										  cross join IPEDSReportingPeriod repperiodENT1
										  where repperiodENT1.surveyCollectionYear = clientconfig1.surveyYear
										  and repperiodENT1.surveyId = clientconfig1.surveyId
										  and repperiodENT1.surveySection = clientconfig1.sectionRetFall
										  and repperiodENT1.termCode is not null
										  and repperiodENT1.partOfTermCode is not null)
	
	union
	
	--Pulls default values for Summer when IPEDSReportingPeriod record doesn't exist
	select clientconfig.surveyYear surveyYear,
		clientconfig.sectionSummer surveySection,
		clientconfig.termCodeSummer termCode,
		clientconfig.censusDateSummer censusDate,
		clientconfig.partOfTermCode partOfTermCode,
		clientconfig.acadOrProgReporter acadOrProgReporter,
		clientconfig.feIncludeOptSurveyData feIncludeOptSurveyData,
		clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary
	from ClientConfigMCR clientconfig
	where clientconfig.surveyYear not in (select repperiodENT1.surveyCollectionYear
										  from ClientConfigMCR clientconfig1
										  cross join IPEDSReportingPeriod repperiodENT1
										  where repperiodENT1.surveyCollectionYear = clientconfig1.surveyYear
										  and repperiodENT1.surveyId = clientconfig1.surveyId
										  and repperiodENT1.surveySection = clientconfig1.sectionSummer
										  and repperiodENT1.termCode is not null
										  and repperiodENT1.partOfTermCode is not null)										
	
	union
	
	--Pulls default values for Retention Summer when IPEDSReportingPeriod record doesn't exist
	select clientconfig.surveyYear surveyYear,
		clientconfig.sectionRetSummer surveySection,
		clientconfig.termCodeRetSummer termCode,
		clientconfig.censusDateRetSummer censusDate,
		clientconfig.partOfTermCode partOfTermCode,
		clientconfig.acadOrProgReporter acadOrProgReporter,
		clientconfig.feIncludeOptSurveyData feIncludeOptSurveyData,
		clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary
	from ClientConfigMCR clientconfig
	where clientconfig.surveyYear not in (select repperiodENT1.surveyCollectionYear
										  from ClientConfigMCR clientconfig1
										  cross join IPEDSReportingPeriod repperiodENT1
										  where repperiodENT1.surveyCollectionYear = clientconfig1.surveyYear
										  and repperiodENT1.surveyId = clientconfig1.surveyId
										  and repperiodENT1.surveySection = clientconfig1.sectionRetSummer
										  and repperiodENT1.termCode is not null
										  and repperiodENT1.partOfTermCode is not null)	
	)  RepDates
	inner join AcadTermOrder termorder on RepDates.termCode = termorder.termCode
	left join AcademicTermMCR acadterm on RepDates.termCode = acadterm.termCode	
		and RepDates.partOfTermCode = acadterm.partOfTermCode
),

--jh 20200422 Added default values from ConfigPerAsOfDate

ReportingPeriodMCR_HR as (
--Returns client specified reporting period for HR reporting and defaults to IPEDS specified date range if not otherwise defined 

select surveyYear surveyYear,
	asOfDate asOfDate,
	termCodeFall termCodeFall
from (
	select repperiodENT.surveyCollectionYear surveyYear,
		NVL(repperiodENT.asOfDate, clientconfig.asOfDateHR) asOfDate,
		clientconfig.termCodeFall termCodeFall,
		row_number() over (
			partition by
				repperiodENT.surveyCollectionYear
			order by
				repperiodENT.recordActivityDate desc
		) reportPeriodRn
	from ClientConfigMCR clientconfig
		cross join IPEDSReportingPeriod repperiodENT
	where repperiodENT.surveyCollectionYear = clientconfig.surveyYear
		and repperiodENT.surveyId like clientconfig.surveyIdHR
		)
where reportPeriodRn = 1
	
union

select clientconfig.surveyYear surveyYear,
	clientconfig.asOfDateHR asOfDate,
	clientconfig.termCodeFall termCodeFall
from ClientConfigMCR clientconfig
where clientconfig.surveyYear not in (select repperiodENT.surveyCollectionYear
									  from ClientConfigMCR clientconfig1
									  cross join IPEDSReportingPeriod repperiodENT
									  where repperiodENT.surveyCollectionYear = clientconfig1.surveyYear
									  and repperiodENT.surveyId like clientconfig1.surveyIdHR)
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusMCR as ( 
-- Returns most recent campus record for each campus available per the reporting terms and part of terms (ReportingPeriod).

select *
from (
	select campusENT.*,
		row_number() over (
			partition by
				campusENT.campus
			order by
				campusENT.recordActivityDate desc
		   ) campusRn
	from ReportingPeriodMCR repperiod
		cross join campus campusENT 
	where campusENT.isIpedsReportable = 1
--ak 20200406 Including dummy date changes. (PF-1368)
		and ((campusENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
			and campusENT.recordActivityDate <= repperiod.censusDate)
				or campusENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where campusRn = 1
),

RegistrationMCR as ( 
--Returns all student enrollment records as of the ReportingPeriod and where course is viable

select cohortInd cohortInd,
	personId personId,
	regTermCode termCode,
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
	termOrder termOrder,
	partOfTermCode partOfTermCode,
	censusDate censusDate,
	crn crn,
	crnLevel crnLevel,
	isInternational isInternational,
	crnGradingMode crnGradingMode,
	equivCRHRFactor equivCRHRFactor
from (
	select repperiod.cohortInd cohortInd,
		regENT.personId personId,
		repperiod.censusDate censusDate,
		repperiod.termCode regTermCode,
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
		termorder.termOrder termOrder,
		repperiod.partOfTermCode partOfTermCode,
		campus.isInternational isInternational,
-- ak 20200330 added Registration.crnGradingMode (PF-1253)
		regENT.crnGradingMode crnGradingMode,
-- ak 20200519 Added equivalent hours for PT/FT hours multiplier (PF-1480).
		repperiod.equivCRHRFactor equivCRHRFactor,
		regENT.crn crn,
-- ak 20200330 Added crnLevel ENUM field with values Undergrad,Graduate,Postgraduate,Professional,Continuing Ed (PF-1253)
		regENT.crnLevel crnLevel,
		row_number() over (
			partition by
				regENT.personId,
				regENT.termCode,
				regENT.partOfTermCode,
				regENT.crn,
				regENT.crnLevel
			order by
				regENT.recordActivityDate desc
		) regRn
	from ReportingPeriodMCR repperiod
		inner join AcadTermOrder termorder on repperiod.termOrder = termorder.termOrder
		inner join Registration regENT on repperiod.termCode = regENT.termCode 
-- ak 20200330 added Registration.partOfTermCode (PF-1253)
			and regENT.partOfTermCode = repperiod.partOfTermCode
-- ak 20200330 added Registration.registrationStatusActionDate  (PF-1253)
-- ak 20200406 Including dummy date changes. (PF-1368)
-- ak 20200707 Change registrationStatusDate to registrationStatusActionDate (PF-1536)
			and ((regENT.registrationStatusActionDate  != CAST('9999-09-09' AS TIMESTAMP)
				and regENT.registrationStatusActionDate  <= repperiod.censusDate)
					or regENT.registrationStatusActionDate  = CAST('9999-09-09' AS TIMESTAMP))
			--and regENT.recordActivityDate <= repperiod.censusDate
-- ak 20200330 Modified to use 'isEnrolled' field instead of registrationStatus = 'Enrolled'(PF-1253)
			--and regENT.registrationStatus = 'Enrolled'
			and regENT.isEnrolled = 1
			and regENT.isIpedsReportable = 1 
		left join CampusMCR campus on regENT.campus = campus.campus
	)
where regRn = 1
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods. 

select *
from (
	select distinct reg.cohortInd cohortInd,
		reg.crn,
		studentENT.*,
		row_number() over (
			partition by
				studentENT.personId,
				studentENT.termCode
			order by
				studentENT.recordActivityDate desc
		) studRn
	from RegistrationMCR reg
		inner join Student studentENT ON reg.personId = studentENT.personId
--ak 20200406 Including dummy date changes. (PF-1368)
--jh 20200422 Due to Student ingestion query timeout issue, commenting this filter out for testing
--77 results with filter, 5335 results without filter
			and ((studentENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) --77
				and studentENT.recordActivityDate <= reg.censusDate)
					or studentENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and reg.termCode = studentENT.termCode
			and studentENT.isIpedsReportable = 1
	)
where studRn = 1
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 
	
select *
from (
	select reg.cohortInd cohortInd,
		personENT.*,
		row_number() over (
			partition by
				personENT.personId
			order by
				personENT.recordActivityDate desc
			) personRn
	from RegistrationMCR reg
		inner join Person personENT ON reg.personId = personENT.personId
--ak 20200406 Including dummy date changes. (PF-1368)
			and ((personENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and personENT.recordActivityDate <= reg.censusDate)
					or personENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and personENT.isIpedsReportable = 1
	)
where personRn = 1
),

AcademicTrackMCR as (
--Returns most up to date student field of study information as of the reporting term codes and part of term census periods.
--Only required to report based on field of study based categorization for even-numbered IPEDS reporting years. 

select * 
from (
	select reg.cohortInd cohortInd,
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
		reg.termOrder termOrder,
		reg.termCode termCode,
		reg.partOfTermCode partOfTermCode,
		acadtrackENT.*,
		reg.censusDate censusDate,
		row_number() over (
			partition by
				acadtrackENT.personId,
				acadtrackENT.termCodeEffective,
				acadtrackENT.academicTrackLevel
			order by
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
				termorder.termOrder desc,
				acadtrackENT.recordActivityDate desc
		) acadtrackRn
	from RegistrationMCR reg
		left join AcademicTrack acadtrackENT ON acadtrackENT.personId = reg.personId
--ak 20200406 Including dummy date changes. (PF-1368)
            and ((acadtrackENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and acadtrackENT.recordActivityDate <= reg.censusDate)
			        or acadtrackENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and acadtrackENT.fieldOfStudyType = 'Major'
			and acadtrackENT.fieldOfStudyPriority = 1
			and acadtrackENT.isIpedsReportable = 1
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
		inner join AcadTermOrder termorder
		    on termorder.termCode = acadtrackENT.termCodeEffective
	where termorder.termOrder <= reg.termOrder
	)
where acadtrackRn = 1
), 

MajorMCR as (
--Returns most up to 'major' information as of the reporting term codes and part of term census periods.
--This information is only required for even-numbered IPEDS reporting years. 

select *
from (
	select acadTrack.cohortInd,
		acadtrack.termCode,
		acadtrack.partOfTermCode,
		majorENT.*,
		row_number() over (
			partition by
				majorENT.major
			order by
				majorENT.curriculumRuleActionDate desc,
				majorENT.recordActivityDate desc
		) majorRn
	from AcademicTrackMCR acadtrack
		left join Major majorENT ON majorENT.major = acadtrack.curriculumCode
-- ak 20200406 Including dummy date changes. (PF-1368)
-- ak 20200707 change curriculumRuleActivityDate to curriculumRuleActionDate in both Major and Degree (PF-1536)
			and majorENT.curriculumRuleActivityDate <= acadtrack.censusDate
			and ((majorENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and majorENT.recordActivityDate  <= acadtrack.censusDate)
					or majorENT.recordActivityDate  = CAST('9999-09-09' AS TIMESTAMP)) 
			and majorENT.isIpedsReportable = 1
	where (select clientconfig.reportResidency
		   from ClientConfigMCR clientconfig) = 'Y'
	)
where majorRn = 1
),

-- ak 20200427 Adding Degree entitiy to pull award level for 'Bachelor level' Retention cohort filtering. (PF-1434)
DegreeMCR as (
--Returns most up to 'degree' information as of the reporting term codes and part of term census periods.
--This information is used for Retention cohort filtering on awardLevel

select *
from (
	select acadTrack.personId,
		acadTrack.cohortInd,
		acadtrack.termCode,
		acadtrack.partOfTermCode,
		degreeENT.*,
		row_number() over (
			partition by
				degreeENT.degree
			order by
				degreeENT.curriculumRuleActionDate desc,
				degreeENT.recordActivityDate desc
		) degreeRn
	from AcademicTrackMCR acadtrack
		inner join ClientConfigMCR clientconfig ON acadTrack.cohortInd = clientconfig.sectionRetFall
		inner join Degree degreeENT ON degreeENT.degree = acadtrack.degree
--ak 20200406 Including dummy date changes. (PF-1368)
-- ak 20200707 change curriculumRuleActivityDate to curriculumRuleActionDate in both Major and Degree (PF-1536)
			and degreeENT.curriculumRuleActivityDate <= acadtrack.censusDate
			and ((degreeENT.recordActivityDate  != CAST('9999-09-09' AS TIMESTAMP)
				and degreeENT.recordActivityDate  <= acadtrack.censusDate)
					or degreeENT.recordActivityDate  = CAST('9999-09-09' AS TIMESTAMP)) 
			and degreeENT.isIpedsReportable = 1
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
		inner join AcadTermOrder termorder
			on termorder.termCode = degreeENT.curriculumRuleTermCodeEff
	where termorder.termOrder <= acadTrack.TermOrder
	)
where degreeRn = 1
),

--jh 20200422 moved CourseSectionMCR prior to CourseSectionScheduleMCR and added section field

CourseSectionMCR as (
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
		coursesectENT.crn crn,
		coursesectENT.section section,
		coursesectENT.subject subject,
		coursesectENT.courseNumber courseNumber,
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
		reg.termOrder termOrder,
		coursesectENT.termCode termCode,
		coursesectENT.partOfTermCode partOfTermCode,
		reg.crnLevel crnLevel,
		reg.censusDate censusDate,
		reg.equivCRHRFactor equivCRHRFactor,
		coursesectENT.recordActivityDate recordActivityDate,
--ak 20200519 Changing CourseSchedule.enrollmentCreditHours to enrollmentHours (PF-1480)
		CAST(coursesectENT.enrollmentHours as DECIMAL(2,0)) enrollmentHours,
		--CAST(coursesectENT.creditOrClockHours as DECIMAL(2,0)) enrollmentHours,
		coursesectENT.isClockHours isClockHours, 
		row_number() over (
			partition by
				coursesectENT.subject,
				coursesectENT.courseNumber,
				coursesectENT.crn,
				coursesectENT.section,
				coursesectENT.termCode,
				coursesectENT.partOfTermCode
			order by
				coursesectENT.recordActivityDate desc
		) courseRn
	from RegistrationMCR reg 
		inner join CourseSection coursesectENT ON coursesectENT.termCode = reg.termCode
			and coursesectENT.partOfTermCode = reg.partOfTermCode
			and coursesectENT.crn = reg.crn
			and coursesectENT.isIpedsReportable = 1
--ak 20200406 Including dummy date changes. (PF-1368)
			and ((coursesectENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and coursesectENT.recordActivityDate <= reg.censusDate)
					or coursesectENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where courseRn = 1
),

CourseSectionScheduleMCR as (
--Returns course scheduling related info for the registration CRN.
--AcademicTerm.partOfTermCode, CourseSectionSchedule.partOfTermCode & AcademicTerm.censusDate together are used to define the period 
--of valid course registration attempts. 

select *
from (
	select coursesect.cohortInd cohortInd,
		coursesect.censusDate censusDate,
		coursesectschedENT.crn crn,
		coursesectschedENT.section section,
		coursesectschedENT.termCode termCode,
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
		coursesect.termOrder termOrder, 
		coursesectschedENT.partOfTermCode partOfTermCode,
		coursesect.creditHourEquivalent creditHourEquivalent,
		coursesect.subject subject,
		coursesect.courseNumber courseNumber,
		coursesect.crnLevel crnLevel,
		NVL(coursesectschedENT.meetingType, 'Standard') meetingType,
		row_number() over (
			partition by
				coursesectschedENT.crn,
				coursesectschedENT.section,
				coursesectschedENT.termCode,
				coursesectschedENT.partOfTermCode
			order by
				coursesectschedENT.recordActivityDate desc
		) courseSectSchedRn
	from CourseSectionMCR coursesect
		left join CourseSectionSchedule coursesectschedENT ON coursesectschedENT.termCode = coursesect.termCode
			and coursesectschedENT.partOfTermCode = coursesect.partOfTermCode
			and coursesectschedENT.crn = coursesect.crn
			and coursesectschedENT.section = coursesect.section
			and coursesectschedENT.isIpedsReportable = 1 
--ak 20200406 Including dummy date changes. (PF-1368)
			and ((coursesectschedENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and coursesectschedENT.recordActivityDate <= coursesect.censusDate)
					or coursesectschedENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where courseSectSchedRn = 1
),

--jh 20200422 removed ReportingPeriod and CourseSectionMCR joins and added section field
CourseMCR as (
--Included to get course type information

select *
from (
	select distinct coursesectsched.cohortInd cohortInd,
		coursesectsched.crn crn,
		coursesectsched.section section,
		courseENT.subject subject,
		courseENT.courseNumber courseNumber,
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
		coursesectsched.termOrder,
		coursesectsched.termCode termCode,
		coursesectsched.partOfTermCode partOfTermCode,
		coursesectsched.censusDate censusDate,
		courseENT.courseLevel courseLevel,
		courseENT.isRemedial isRemedial,
		courseENT.isESL isESL,
		coursesectsched.meetingType meetingType,
		CAST(coursesectsched.creditHourEquivalent as DECIMAL(2,0)) creditHrs,
		row_number() over (
			partition by
				courseENT.subject,
				courseENT.courseNumber,
				courseENT.termCodeEffective,
				courseENT.courseLevel
			order by
				courseENT.recordActivityDate desc
		) courseRn
	from CourseSectionScheduleMCR coursesectsched
		inner join Course courseENT ON courseENT.subject = coursesectsched.subject
			and courseENT.courseNumber = coursesectsched.courseNumber
			and courseENT.courseLevel = coursesectsched.crnLevel
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
		inner join AcadTermOrder termorder on termorder.termCode = courseENT.termCodeEffective
			and courseENT.isIpedsReportable = 1 --true
--ak 20200406 Including dummy date changes. (PF-1368)
			and ((courseENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and courseENT.recordActivityDate <= coursesectsched.censusDate)
					or courseENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	where termorder.termOrder <= coursesectsched.termOrder
	)
where courseRn = 1
),

--jh 20200422 removed CourseSectionScheduleMCR join

CourseTypeCountsSTU as (
-- View used to break down course category type counts by type

select reg.cohortInd cohortInd,
	reg.personId personId,
	reg.termCode termCode,
	reg.partOfTermCode partOfTermCode,
	COUNT(case when NVL(course.creditHrs, 0) is not null then 1 else 0 end) totalCourses,
	SUM(NVL(course.creditHrs, 0)) totalHrs,
	COUNT(case when NVL(course.creditHrs, 0) = 0 then 1 else 0 end) totalNonCredCourses,
	COUNT(case when NVL(course.creditHrs, 0) != 0 then 1 else 0 end) totalCredCourses,
	COUNT(case when course.meetingType = 'Online/Distance Learning' then 1 else 0 end) totalDECourses,
	COUNT(case when course.courseLevel = 'Undergrad' then 1 else 0 end) totalUGCourses,
	COUNT(case when course.courseLevel in ('Graduate', 'Professional') then 1 else 0 end) totalGRCourses,
	COUNT(case when course.courseLevel = 'Postgraduate' then 1 else 0 end) totalPostGRCourses,
	COUNT(case when course.courseLevel = 'Continuing Ed' then 1 else 0 end) totalCECourses,
	COUNT(case when course.isESL = 'Y' then 1 else 0 end) totalESLCourses,
	COUNT(case when course.isRemedial = 'Y' then 1 else 0 end) totalRemCourses,
	COUNT(case when reg.isInternational = 1 then 1 else 0 end) totalIntlCourses,
-- ak 20200330 added Registration.crnGradingMode (PF-1253)
	COUNT(case when reg.crnGradingMode = 'Audit' then 1 else 0 end) totalAuditCourses
from RegistrationMCR reg
	inner join CourseMCR course on course.termCode = reg.termCode
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

--jh 20200422 modified to only pull first time students from SUMmer term
 
StudentTypeRefactor as (
--View used to determine reporting year IPEDS defined student type using prior SUMmer considerations
--Student is considered 'First Time' if they enrolled for the first time in the reporting fall term or 
--if they enrolled for the first time in the prior SUMmer and continued on to also take fall courses. 

select student.personId personId
from ClientConfigMCR clientconfig
	cross join StudentMCR student 
where student.cohortInd = clientconfig.sectionSUMmer 
	and student.studentType = 'First Time'
),

--jh 20200422 modified to only pull first time students from retention SUMmer term

StudentTypeRefactor_RET as (
--This view is the same as 'StuReportableType' but it applies to the retention cohort

select student.personId personId
from ClientConfigMCR clientconfig
	cross join StudentMCR student 
where student.cohortInd = clientconfig.sectionRetSUMmer 
	and student.studentType = 'First Time'
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

--jh 20200422 Added variables from DefaultValues to remove hard-coding
--			  Added alias to cohortInd field
--			  Made aliases for course consistent
--			  Removed termCode and partOfTermCode fields from Person and Student joins

CohortSTU as (
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
		clientconfig.acadOrProgReporter acadOrProgReporter,
		clientconfig.reportResidency reportResidency,
		clientconfig.reportAge reportAge,
		clientconfig.genderForNonBinary genderForNonBinary,
		clientconfig.genderForUnknown genderForUnknown,
		--major.cipCode cipCode,
		--major.major major,
		--AcademicTrackCurrent.curriculumCode degree,
		student.isNonDegreeSeeking isNonDegreeSeeking,
-- ak 20200330 	(PF-1320) Modification to pull residency from student instead of person.
		student.residency residency,
		student.highSchoolGradDate hsGradDate,
-- ak 20200519 Adding FT/PT credit hour comparison points (PF-1480)
		NVL(acadterm.requiredFTCreditHoursUG, 12) requiredUGhrs,
		NVL(acadterm.requiredFTCreditHoursGR, 9) requiredGRhrs,
		person.gender gender,
-- ak 20200421 (PF-1417) Modification to replace 'isInternational' with 'isInUSOnVisa' and 'isUSCitizen'
		person.isInUSOnVisa isInUSOnVisa,
-- ak 20200707 Modify the cohort or cohort refactor view to include the visaStartDate and visaEndDate (PF-1536)
		person.visaStartDate visaStartDate,
		person.visaEndDate visaEndDate,
		person.isUSCitizen isUSCitizen,
		person.isHispanic isHispanic,
		person.isMultipleRaces isMultipleRaces,
		person.ethnicity ethnicity,
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'
		FLOOR(DATEDIFF(TO_DATE(reg.censusDate), person.birthDate) / 365) asOfAge,
		person.nation nation,
		person.state state,
		--courseSch.meetingType meetingType,
		coursecnt.totalHrs totalHrs,
		coursecnt.totalCourses totalcourses,
		coursecnt.totalDEcourses totalDEcourses,
		coursecnt.totalUGCourses totalUGCourses,
		coursecnt.totalGRCourses totalGRCourses,
		coursecnt.totalPostGRCourses totalPostGRCourses,
		coursecnt.totalNonCredCourses totalNonCredCourses,
		coursecnt.totalESLCourses totalESLCourses,
		coursecnt.totalRemCourses totalRemCourses,
-- ak 20200330 Continuing ed is no longer considered a student type (PF-1382)
--jh 20200422 if cohort student type is not first time, check is SUMmer is. If not, value is student type
		case when student.studentType = 'First Time' then 'First Time'
			when student.personId = studtype.personId then 'First Time'
			else student.studentType
		end studentType,
		case when student.studentLevel = 'Continuing Ed' and clientconfig.includeNonDegreeAsUG = 'Y' then 'UnderGrad' 
			when student.studentLevel in ('Professional', 'Postgraduate') then 'Graduate' 
			else student.studentLevel 
		end studentLevel,
		case 
--jh 20200422 changed coursecnt.totalCredCourses to coursecnt.totalCourses
			when coursecnt.totalRemCourses = coursecnt.totalCourses and student.isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
			when coursecnt.totalCredCourses > 0 --exclude students not enrolled for credit
			then (case when coursecnt.totalESLCourses = coursecnt.totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
						when coursecnt.totalCECourses = coursecnt.totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
						when coursecnt.totalIntlCourses = coursecnt.totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
-- ak 20200330 added Registration.crnGradingMode (PF-1253)
						when coursecnt.totalAuditCourses = coursecnt.totalCredCourses then 0 --exclude students exclusively auditing classes
						-- when... then 0 --exclude PHD residents or interns
						-- when... then 0 --students studying abroad if enrollment at home institution is an admin only record
						-- when... then 0 --exclude students in experimental Pell programs
						else 1
					end)
			else 0 
		end ipedsInclude
		from ClientConfigMCR clientconfig
--jh 20200422 added join to filter on fall cohort term code
			inner join RegistrationMCR reg on reg.cohortInd = clientconfig.sectionFall 
			inner join PersonMCR person on reg.personId = person.personId
				and person.cohortInd = reg.cohortInd
			inner join StudentMCR student on reg.personId = student.personId
				and student.cohortInd = reg.cohortInd
			inner join AcademicTermMCR acadterm on acadterm.termCode = reg.termCode
--jh 20200422 changed inner join to left join
			left join CourseTypeCountsSTU coursecnt on reg.personId = coursecnt.personId --5299
				and coursecnt.cohortInd = reg.cohortInd
			left join StudentTypeRefactor studtype on studtype.personId = reg.personId
--ak 20200429 Adding student status filter. 
		where student.studentStatus = 'Active'
	) as CohView
),

CohortRefactorSTU as (
-- This view is used to refactor the 'StudentCohort' view records in terms of IPEDS reportable values

select cohortstu.personId personId,
	cohortstu.cohortInd cohortInd,
	cohortstu.censusDate censusDate,
--jh 20200422 added config fields
	cohortstu.acadOrProgReporter acadOrProgReporter,
	cohortstu.reportResidency reportResidency,
	cohortstu.reportAge reportAge,
	cohortstu.studentType studentType,
	cohortstu.ipedsInclude ipedsInclude,
	cohortstu.isNonDegreeSeeking isNonDegreeSeeking,
	cohortstu.studentLevel studentLevel,
--jh 20200423 Broke up ipedsLevel into PartG, PartB and PartA
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'
	case when cohortstu.studentLevel = 'Graduate' then 3
		when cohortstu.isNonDegreeSeeking = true then 2
		else 1
	end ipedsPartGStudentLevel,
	case when cohortstu.studentLevel = 'Graduate' then 3
		else 1
	end ipedsPartBStudentLevel,
	cohortstu.timeStatus timeStatus,
	cohortstu.hsGradDate hsGradDate,
	case when cohortstu.timeStatus = 'Full'
			and cohortstu.studentType = 'First Time'
			and cohortstu.isNonDegreeSeeking = 'false'
			and cohortstu.studentLevel = 'Undergrad'  
		then 1 -- 1 - Full-time, first-time degree/certificate-seeking undergraduate
		when cohortstu.timeStatus = 'Full'
			and cohortstu.studentType = 'Transfer'
			and cohortstu.isNonDegreeSeeking = 'false'
			and cohortstu.studentLevel = 'Undergrad'
		then 2 -- 2 - Full-time, transfer-IN degree/certificate-seeking undergraduate
		when cohortstu.timeStatus = 'Full'
			and cohortstu.studentType = 'Returning'
			and cohortstu.isNonDegreeSeeking = 'false'
			and cohortstu.studentLevel = 'Undergrad'
		then 3 -- 3 - Full-time, continuing degree/certificate-seeking undergraduate
		when cohortstu.timeStatus = 'Full'
			and cohortstu.isNonDegreeSeeking = 'true'
			and cohortstu.studentLevel = 'Undergrad'
		then 7 -- 7 - Full-time, non-degree/certificate-seeking undergraduate
		when cohortstu.timeStatus = 'Full'
			and cohortstu.studentLevel = 'Graduate'
		then 11 -- 11 - Full-time graduate
		when cohortstu.timeStatus = 'Part'
			and cohortstu.studentType = 'First Time'
			and cohortstu.isNonDegreeSeeking = 'false'
			and cohortstu.studentLevel = 'Undergrad'
		then 15 -- 15 - Part-time, first-time degree/certificate-seeking undergraduate
		when cohortstu.timeStatus = 'Part'
			and cohortstu.studentType = 'Transfer'
			and cohortstu.isNonDegreeSeeking = 'false'
			and cohortstu.studentLevel = 'Undergrad'
		then 16 -- 16 - Part-time, transfer-IN degree/certificate-seeking undergraduate
		when cohortstu.timeStatus = 'Part'
			and cohortstu.studentType = 'Returning'
			and cohortstu.isNonDegreeSeeking = 'false'
			and cohortstu.studentLevel = 'Undergrad'
		then 17 -- 17 - Part-time, continuing degree/certificate-seeking undergraduate
		when cohortstu.timeStatus = 'Part'
			and cohortstu.isNonDegreeSeeking = 'true'
			and cohortstu.studentLevel = 'Undergrad'
		then 21 -- 21 - Part-time, non-degree/certificate-seeking undergraduate
		when cohortstu.timeStatus = 'Part'
			and cohortstu.studentLevel = 'Graduate'
		then 25 -- 25 - Part-time graduate
		end as ipedsPartAStudentLevel,
-- ak 20200330 (PF-1253) Including client preferences for non-binary and unknown gender assignments
	case when cohortstu.gender = 'Male' then 'M'
		when cohortstu.gender = 'Female' then 'F'
--jh 20200223 added config values
		when cohortstu.gender = 'Non-Binary' then cohortstu.genderForNonBinary
		else cohortstu.genderForUnknown
	end ipedsGender,
-- ak 20200421 (PF-1417) Modification to use 'isInUSOnVisa' and 'isUSCitizen' for ethnicity assignments
	case when cohortstu.isUSCitizen = 1 then 
		(case when cohortstu.isHispanic = true then 2 -- 'hispanic/latino'
			when cohortstu.isMultipleRaces = true then 8 -- 'two or more races'
			when cohortstu.ethnicity != 'Unknown' and cohortstu.ethnicity is not null
			then (case when cohortstu.ethnicity = 'Hispanic or Latino' then 2
					when cohortstu.ethnicity = 'American Indian or Alaskan Native' then 3
					when cohortstu.ethnicity = 'Asian' then 4
					when cohortstu.ethnicity = 'Black or African American' then 5
					when cohortstu.ethnicity = 'Native Hawaiian or Other Pacific Islander' then 6
					when cohortstu.ethnicity = 'Caucasian' then 7
					else 9 end) 
			    else 9 end) -- 'race and ethnicity unknown'
-- ak 20200707 Modify the cohort or cohort refactor view to include the visaStartDate and visaEndDate (PF-1536)
            else (case when cohortstu.isInUSOnVisa = 1 
						and cohortstu.censusDate between cohortstu.visaStartDate and cohortstu.visaEndDate then '1' -- 'nonresident alien'
		else 9 end) -- 'race and ethnicity unknown'
	end ipedsEthnicity,
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'
	case when cohortstu.timeStatus = 'Full' 
		then (
			case when cohortstu.asOfAge < 18 then 1 --1 - Full-time, under 18
				when cohortstu.asOfAge >= 18 and cohortstu.asOfAge <= 19 then 2 --2 - Full-time, 18-19
				when cohortstu.asOfAge >= 20 and cohortstu.asOfAge <= 21 then 3 --3 - Full-time, 20-21
				when cohortstu.asOfAge >= 22 and cohortstu.asOfAge <= 24 then 4 --4 - Full-time, 22-24
				when cohortstu.asOfAge >= 25 and cohortstu.asOfAge <= 29 then 5 --5 - Full-time, 25-29
				when cohortstu.asOfAge >= 30 and cohortstu.asOfAge <= 34 then 6 --6 - Full-time, 30-34
				when cohortstu.asOfAge >= 35 and cohortstu.asOfAge <= 39 then 7 --7 - Full-time, 35-39
				when cohortstu.asOfAge >= 40 and cohortstu.asOfAge <= 49 then 8 --8 - Full-time, 40-49
				when cohortstu.asOfAge >= 50 and cohortstu.asOfAge <= 64 then 9 --9 - Full-time, 50-64
				when cohortstu.asOfAge >= 65 then 10 --10 - Full-time, 65 and over
				end )
		else (
			case when cohortstu.asOfAge < 18 then 13 --13 - Part-time, under 18
				when cohortstu.asOfAge >= 18 and cohortstu.asOfAge <= 19 then 14 --14 - Part-time, 18-19
				when cohortstu.asOfAge >= 20 and cohortstu.asOfAge <= 21 then 15 --15 - Part-time, 20-21
				when cohortstu.asOfAge >= 22 and cohortstu.asOfAge <= 24 then 16 --16 - Part-time, 22-24
				when cohortstu.asOfAge >= 25 and cohortstu.asOfAge <= 29 then 17 --17 - Part-time, 25-29
				when cohortstu.asOfAge >= 30 and cohortstu.asOfAge <= 34 then 18 --18 - Part-time, 30-34
				when cohortstu.asOfAge >= 35 and cohortstu.asOfAge <= 39 then 19 --19 - Part-time, 35-39
				when cohortstu.asOfAge >= 40 and cohortstu.asOfAge <= 49 then 20 --20 - Part-time, 40-49
				when cohortstu.asOfAge >= 50 and cohortstu.asOfAge <= 64 then 21 --21 - Part-time, 50-64
				when cohortstu.asOfAge >= 65 then 22 --22 - Part-time, 65 and over
		end )
	end ipedsAgeGroup,
	cohortstu.nation,
	case when cohortstu.nation = 'US' then 1 else 0 end isInAmerica,
	case when cohortstu.residency = 'In District' then 'IN'
		when cohortstu.residency = 'In State' then 'IN'
		when cohortstu.residency = 'Out of State' then 'OUT'
--jh 20200423 added value of Out of US
		when cohortstu.residency = 'Out of US' then 'OUT'
		else 'UNKNOWN'
	end residentStatus,
--check 'IPEDSClientConfig' entity to confirm that optional data is reported
	case when (select clientconfig.reportResidency from ClientConfigMCR clientconfig) = 'Y' 
				then (case when cohortstu.state IS NULL then 57 --57 - State unknown
							when cohortstu.state = 'AL' then 01 --01 - Alabama
							when cohortstu.state = 'AK' then 02 --02 - Alaska
							when cohortstu.state = 'AZ' then 04 --04 - Arizona
							when cohortstu.state = 'AR' then 05 --05 - Arkansas
							when cohortstu.state = 'CA' then 06 --06 - California
							when cohortstu.state = 'CO' then 08 --08 - Colorado
							when cohortstu.state = 'CT' then 09 --09 - CONnecticut
							when cohortstu.state = 'DE' then 10 --10 - Delaware
							when cohortstu.state = 'DC' then 11 --11 - District of Columbia
							when cohortstu.state = 'FL' then 12 --12 - Florida
							when cohortstu.state = 'GA' then 13 --13 - Georgia
							when cohortstu.state = 'HI' then 15 --15 - Hawaii
							when cohortstu.state = 'ID' then 16 --16 - Idaho
							when cohortstu.state = 'IL' then 17 --17 - Illinois
							when cohortstu.state = 'IN' then 18 --18 - Indiana
							when cohortstu.state = 'IA' then 19 --19 - Iowa
							when cohortstu.state = 'KS' then 20 --20 - Kansas
							when cohortstu.state = 'KY' then 21 --21 - Kentucky
							when cohortstu.state = 'LA' then 22 --22 - Louisiana
							when cohortstu.state = 'ME' then 23 --23 - Maine
							when cohortstu.state = 'MD' then 24 --24 - Maryland
							when cohortstu.state = 'MA' then 25 --25 - Massachusetts
							when cohortstu.state = 'MI' then 26 --26 - Michigan
							when cohortstu.state = 'MS' then 27 --27 - Minnesota
							when cohortstu.state = 'MO' then 28 --28 - Mississippi
							when cohortstu.state = 'MN' then 29 --29 - Missouri
							when cohortstu.state = 'MT' then 30 --30 - Montana
							when cohortstu.state = 'NE' then 31 --31 - Nebraska
							when cohortstu.state = 'NV' then 32 --32 - Nevada
							when cohortstu.state = 'NH' then 33 --33 - New Hampshire
							when cohortstu.state = 'NJ' then 34 --34 - New Jersey
							when cohortstu.state = 'NM' then 35 --35 - New Mexico
							when cohortstu.state = 'NY' then 36 --36 - New York
							when cohortstu.state = 'NC' then 37 --37 - North Carolina
							when cohortstu.state = 'ND' then 38 --38 - North Dakota
							when cohortstu.state = 'OH' then 39 --39 - Ohio	
							when cohortstu.state = 'OK' then 40 --40 - Oklahoma
							when cohortstu.state = 'OR' then 41 --41 - Oregon
							when cohortstu.state = 'PA' then 42 --42 - Pennsylvania
							when cohortstu.state = 'RI' then 44 --44 - Rhode Island
							when cohortstu.state = 'SC' then 45 --45 - South Carolina
							when cohortstu.state = 'SD' then 46 --46 - South Dakota
							when cohortstu.state = 'TN' then 47 --47 - Tennessee
							when cohortstu.state = 'TX' then 48 --48 - Texas
							when cohortstu.state = 'UT' then 49 --49 - Utah
							when cohortstu.state = 'VT' then 50 --50 - Vermont
							when cohortstu.state = 'VA' then 51 --51 - Virginia
							when cohortstu.state = 'WA' then 53 --53 - Washington
							when cohortstu.state = 'WV' then 54 --54 - West Virginia
							when cohortstu.state = 'WI' then 55 --55 - Wisconsin
							when cohortstu.state = 'WY' then 56 --56 - Wyoming
							when cohortstu.state = 'UNK' then 57 --57 - State unknown
							when cohortstu.state = 'American Samoa' then 60
							when cohortstu.state = 'FM' then 64 --64 - Federated States of Micronesia
							when cohortstu.state = 'GU' then 66 --66 - Guam
							when cohortstu.state = 'Marshall Islands' then 68
							when cohortstu.state = 'Northern Marianas' then 69
							when cohortstu.state = 'Palau' then 70
							when cohortstu.state = 'PUE' then 72 --72 - Puerto Rico
							when cohortstu.state = 'VI' then 78 --78 - Virgin Islands
							else 90 --90 - Foreign countries
					end)
			else NULL
	end ipedsStateCode,
	cohortstu.totalcourses totalCourses,
	cohortstu.totalDECourses,
	case when cohortstu.totalDECourses > 0 and cohortstu.totalDECourses < cohortstu.totalCourses then 'Some DE'
		when cohortstu.totalDECourses = cohortstu.totalCourses then 'Exclusive DE'
	end distanceEdInd
from CohortSTU cohortstu
where cohortstu.ipedsInclude = 1
	and cohortstu.studentLevel in ('Undergrad', 'Graduate') --filters out CE when client does not want to count CE taking a credit course as Undergrad
),

/*****
BEGIN SECTION - Retention Cohort Creation
This set of views is used to look at prior year data and return retention cohort information of the group as of the current reporting year.
*****/

--jh 20200409 Removed inclusion and enrolled from values; moved exclusionReason to filter in joins
-- Added variables from DefaultValues to remove hard-coding			  
CohortExclusion_RET as (
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
	from ClientConfigMCR clientconfig
		inner join ReportingPeriodMCR repperiod on repperiod.cohortInd = clientconfig.sectionRetFall
--jh 20200422 changed termCodeEffective to >= to retention termCode (not <=)
		cross join cohortExclusion exclusion 
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
		inner join AcadTermOrder termorder
			on termorder.termCode = exclusion.termCodeEffective
	where termorder.termOrder >= repperiod.termOrder
		and exclusion.exclusionReason in ('Died', 'Medical Leave', 'Military Leave', 'Foreign Aid Service', 'Religious Leave')
		and exclusion.isIPEDSReportable = 1			
	)   
where exclusionRn = 1
),

CohortInclusion_RET as (
--View used to determine inclusions for retention cohort.

select student.personId personId
from ClientConfigMCR clientconfig
	cross join StudentMCR student
where student.studentStatus = 'Active'
	and student.cohortInd = clientconfig.sectionSUMmer
),   

--jh 20200422 Added variables from DefaultValues to remove hard-coding
--			  Added alias to cohortInd field
--			  Removed termCode and partOfTermCode fields from Person and Student joins
--			  Removed Graduate part of timeStatus case statement in outer query
--			  Added code to calculate Audit courses
--			  Modified exclusionInd field
--			  Added inclusionInd placeholder field
--			  Add enrolledInd field and join fo CohortRefactorSTU to determine if student enrolled in Fall cohort

CohortSTU_RET as (
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
		case when exclusionret.personId is not null then 1 else null end exclusionInd,
--ak 20200429 adding inclusion determination code (PF-1435)
		case when student.studentStatus = 'Active' then null --only look at Study Abroad status
			when refactorstu.personId is not null then 1 --student enrolled in second year Fall semester
			when inclusionret.personId is not null then 1 --student enrolled in second year SUMmer semester 
			else null 
		end inclusionInd,
		case when refactorstu.personId is not null then 1 else null end enrolledInd,
-- ak 20200519 Adding FT/PT credit hour comparison points (PF-1480)
		NVL(acadterm.requiredFTCreditHoursUG, 12) requiredUGhrs,
		coursecnt.totalHrs totalHrs,
-- ak 20200330 Removed Continuing ed - it is no longer considered a student type (PF-1382)
--jh 20200422 if cohort student type is not first time, check is SUMmer is. If not, value is student type
		case when student.studentType = 'First Time' then 'First Time'
			when student.personId = studtyperet.personId then 'First Time'
			else student.studentType
		end studentType,
--confirm group and assign student levels based on IPEDS specs.
		case when student.studentLevel = 'Continuing Ed' and clientconfig.includeNonDegreeAsUG = 'Y' then 'UnderGrad' 
			when student.studentLevel in ('Professional', 'Postgraduate') then 'Graduate' 
			else student.studentLevel 
		end studentLevel,
		student.studentStatus studentStatus,
		degree.awardLevel awardLevel,
--confirm if students should be excluded from the cohort based on IPEDS specs.
		case 
--jh 20200422 changed coursecnt.totalCredCourses to coursecnt.totalCourses
			when coursecnt.totalRemCourses = coursecnt.totalCourses 
				and student.isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
			when coursecnt.totalCredCourses > 0 --exclude students not enrolled for credit
			then (case when coursecnt.totalESLCourses = coursecnt.totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
					when coursecnt.totalCECourses = coursecnt.totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
					when coursecnt.totalIntlCourses = coursecnt.totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
-- ak 20200330 added Registration.crnGradingMode (PF-1253)
					when coursecnt.totalAuditCourses = coursecnt.totalCredCourses then 0 --exclude students exclusively auditing classes
					-- when... then 0 --exclude PHD residents or interns
					-- when... then 0 --students studying abroad if enrollment at home institution is an admin only record
					-- when... then 0 --exclude students in experimental Pell programs
					else 1
				end)
			else 0 
		end ipedsInclude
	from ClientConfigMCR clientconfig
		inner join RegistrationMCR reg on reg.cohortInd = clientconfig.sectionRetFall
		inner join PersonMCR person on reg.personId = person.personId
			and person.cohortInd = reg.cohortInd
		inner join StudentMCR student on reg.personId = student.personId
			and student.cohortInd = reg.cohortInd
-- ak 20200429 adding student status filter				
			and student.studentStatus in ('Active', 'Study Abroad')
-- ak 20200427 adding Degree entity to filter retention cohort on Degree.awardLevel (PF-1434)
		inner join DegreeMCR degree on degree.personId = student.personId
			and student.cohortInd = degree.cohortInd
		left join CourseTypeCountsSTU coursecnt on reg.personId = coursecnt.personId
			and coursecnt.cohortInd = reg.cohortInd
		inner join AcademicTermMCR acadterm on acadterm.termCode = reg.termCode
		left join StudentTypeRefactor_RET studtyperet on studtyperet.personId = reg.personId 
		left join CohortExclusion_RET exclusionret on exclusionret.personId = reg.personId
		left join CohortRefactorSTU refactorstu on refactorstu.cohortInd = clientconfig.sectionFall
		left join CohortInclusion_RET inclusionret on reg.personId = inclusionret.personId
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

EmployeeMCR as (
--returns all employees based on HR asOfDate

select *
from (
	select empENT.*,
		repperiodhr.asOfDate asOfDate,
		repperiodhr.termCodeFall termCodeFall,
		row_number() over (
			partition by
				empENT.personId
			order by
				empENT.recordActivityDate desc
		) employeeRn
	from ReportingPeriodMCR_HR repperiodhr
		cross join Employee empENT
--jh 20200422 Including dummy date changes. (PF-1368)
	where ((empENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
		and empENT.recordActivityDate <= repperiodhr.asOfDate)
			or empENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
		and ((empENT.terminationDate IS NULL) 
			or (empENT.terminationDate > repperiodhr.asOfDate
				and empENT.hireDate <= repperiodhr.asOfDate))
		and empENT.isIpedsReportable = 1 
	)
where employeeRn = 1
),

EmployeeAssignmentMCR as (
--returns all employee assignments for employees applicable for IPEDS requirements

select *
from (
	select empassignENT.*,
		emp.asOfDate asOfDate,
		row_number() over (
			partition by
				empassignENT.personId,
				empassignENT.position,
				empassignENT.suffix
			order by
				empassignENT.recordActivityDate desc
		) jobRn
	from EmployeeMCR emp
		inner join EmployeeAssignment empassignENT on empassignENT.personId = emp.personId
--ak 20200406 Including dummy date changes. (PF-1368)
		and ((empassignENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
			and empassignENT.recordActivityDate <= emp.asOfDate)
				or empassignENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
		and empassignENT.assignmentStartDate <= emp.asOfDate
		and (empassignENT.assignmentEndDate IS NULL 
			or empassignENT.assignmentEndDate >= emp.asOfDate)
		and empassignENT.isUndergradStudent = 0
		and empassignENT.isWorkStudy = 0
		and empassignENT.isTempOrSeasonal = 0
		and empassignENT.isIpedsReportable = 1 
		and empassignENT.assignmentType = 'Primary'
	)
where jobRn = 1
),

EmployeePositionMCR as (
--returns ESOC (standardOccupationalCategory) for all employee assignments

select *
from (
	select empposENT.*,
		row_number() over (
			partition by
				empposENT.position
			order by
				--empposENT.startDate desc,
				empposENT.recordActivityDate desc
		) positionRn
	from EmployeeAssignmentMCR empassign
		inner join EmployeePosition empposENT on empposENT.position = empassign.position
--ak 20200406 Including dummy date changes. (PF-1368)
			and ((empposENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and empposENT.recordActivityDate <= empassign.asOfDate)
					or empposENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
		and empposENT.startDate <= empassign.asOfDate
		and (empposENT.endDate IS NULL 
			or empposENT.endDate >= empassign.asOfDate)
		and empposENT.isIpedsReportable = 1 
	)
where positionRn = 1
),

--jh 20200422 added fields to row_number function

InstructionalAssignmentMCR as (
--returns all instructional assignments for all employees

select *
from (
	select instructassignENT.*,
		emp.asOfDate asOfDate,
		row_number() over (
			partition by
				instructassignENT.personId,
				instructassignENT.termCode,
				instructassignENT.partOfTermCode,
				instructassignENT.crn,
				instructassignENT.section
			order by
				instructassignENT.recordActivityDate desc
		) jobRn
	from EmployeeMCR emp
		inner join InstructionalAssignment instructassignENT on instructassignENT.personId = emp.personId
--ak 20200406 Including dummy date changes. (PF-1368)
			and ((instructassignENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and instructassignENT.recordActivityDate <= emp.asOfDate)
					or instructassignENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and instructassignENT.termCode = emp.termCodeFall
			and instructassignENT.isIpedsReportable = 1 --true
	)
where jobRn = 1
),

--jh 20200422 added section and partOfTermCode to course join

CourseTypeCountsEMP as (
--view used to break down course category type counts by type

select instructassign.personId personId,
	SUM(case when course.crn is not null then 1 else 0 end) totalCourses,
	SUM(case when course.creditHrs = 0 then 1 else 0 end) totalNonCredCourses,
	SUM(case when course.creditHrs > 0 then 1 else 0 end) totalCredCourses,
	SUM(case when course.courseLevel = 'Continuing Ed' then 1 else 0 end) totalCECourses
from instructionalAssignmentMCR instructassign
	left join CourseMCR course on course.crn = instructassign.crn
		and course.section = instructassign.section
		and course.termCode = instructassign.termCode
		and course.partOfTermCode = instructassign.partOfTermCode
group by instructassign.personId
),

CohortEMP as (
--view used to build the employee cohort to calculate instructional full time equivalency

select emp.personId personId,
	NVL(empassign.fullOrPartTimeStatus, 'Full Time') fullOrPartTimeStatus,
	case when emp.primaryFunction in (
					'Instruction with Research/Public Service',
					'Instruction - Credit',
					'Instruction - Non-Credit',
					'Instruction - Combined Credit/Non-credit'
			) then 1
		when emppos.standardOccupationalCategory = '25-1000' then 1
		else 0
	end isInstructional,
	coursetypecnt.totalCourses totalCourses,
	coursetypecnt.totalCredCourses totalCredCourses,
	coursetypecnt.totalNonCredCourses totalNonCredCourses,
	coursetypecnt.totalCECourses totalCECourses
from EmployeeMCR emp
	left join EmployeeAssignmentMCR empassign on emp.personId = empassign.personId
	left join EmployeePositionMCR emppos on empassign.position = emppos.position
	left join CourseTypeCountsEMP coursetypecnt on emp.personId = coursetypecnt.personId
where emp.isIpedsMedicalOrDental = 0
),

FTE_EMP as (
--instructional full time equivalency calculation
--**-add exclusions for teaching exclusively in stand-alone graduate or professional programs

select round(FTInst + ((PTInst + PTNonInst) * 1/3), 2) FTE
from (
	select SUM(case when cohortemp.fullOrPartTimeStatus = 'Full Time' 
							and cohortemp.isInstructional = 1 --instructional staff
							and ((cohortemp.totalNonCredCourses < cohortemp.totalCourses) --not exclusively non credit
								or (cohortemp.totalCECourses < cohortemp.totalCourses) --not exclusively CE (non credit)
								or (cohortemp.totalCourses IS NULL)) then 1 
						else 0 
		end) FTInst, --FT instructional staff not teaching exclusively non-credit courses
		SUM(case when cohortemp.fullOrPartTimeStatus = 'Part Time' 
					and cohortemp.isInstructional = 1 --instructional staff
					and ((cohortemp.totalNonCredCourses < cohortemp.totalCourses) --not exclusively non credit
						or (cohortemp.totalCECourses < cohortemp.totalCourses) --not exclusively CE (non credit)
							or (cohortemp.totalCourses IS NULL)) then 1 
				else 0 
		end) PTInst, --PT instructional staff not teaching exclusively non-credit courses
		SUM(case when cohortemp.isInstructional = 0 
					and NVL(cohortemp.totalCredCourses, 0) > 0 then 1 
			else 0 
		end) PTNonInst --administrators or other staff not reported in the HR as instructors who are teaching a credit course in Fall 2019
	from CohortEMP cohortemp 
	)
),

FTE_STU as (
--student full time equivalency calculation
--**add exclusions for teaching exclusively in stand-alone graduate or professional programs

select ROUND(FTStud + (PTStud * 1/3), 2) FTE
from (
	select SUM(case when cohortstu.timeStatus = 'Full' then 1 else 0 end) FTStud,
		SUM(case when cohortstu.timeStatus = 'Part' then 1 else 0 end) PTStud
	from CohortSTU cohortstu
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
	COALESCE(SUM(case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end), 0) field3, -- Nonresident alien - Men (1), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end), 0) field4, -- Nonresident alien - Women (2), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end), 0) field5, -- Hispanic/Latino - Men (25), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end), 0) field6, -- Hispanic/Latino - Women (26), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end), 0) field7, -- American Indian or Alaska Native - Men (27), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end), 0) field8, -- American Indian or Alaska Native - Women (28), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end), 0) field9, -- Asian - Men (29), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end), 0) field10, -- Asian - Women (30), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end), 0) field11, -- Black or African American - Men (31), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end), 0) field12, -- Black or African American - Women (32), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end), 0) field13, -- Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end), 0) field14, -- Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end), 0) field15, -- White - Men (35), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end), 0) field16, -- White - Women (36), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end), 0) field17, -- Two or more races - Men (37), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end), 0) field18, -- Two or more races - Women (38), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end), 0) field19, -- Race and ethnicity unknown - Men (13), 0 to 999999
	COALESCE(SUM(case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end), 0) field20 -- Race and ethnicity unknown - Women (14), 0 to 999999
from (
	select refactorstu.personId personId,
		refactorstu.ipedsPartAStudentLevel ipedsLevel,
		refactorstu.ipedsGender ipedsGender,
		refactorstu.ipedsEthnicity ipedsEthnicity
	from CohortRefactorSTU refactorstu
	where refactorstu.ipedsPartAStudentLevel is not null

	
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
	SUM(case when distanceEdInd = 'Exclusive DE' then 1 else 0 end), -- field3,-- Enrolled exclusively in distance education courses 0 to 999999
	SUM(case when distanceEdInd = 'Some DE' then 1 else 0 end), -- field4,-- Enrolled in some but not all distance education courses 0 to 999999
	SUM(case when distanceEdInd = 'Exclusive DE' and residentStatus = 'IN' then 1 else 0 end), -- field5,-- Of those students exclusively enrolled in de courses - Located in the state/jurisdiction of institution 0 to 999999
	SUM(case when distanceEdInd = 'Exclusive DE' and isInAmerica = true and residentStatus = 'OUT' then 1 else 0 end), -- field6,-- Of those students exclusively enrolled in de courses - Located in the U.S. but not in state/jurisdiction of institution 0 to 999999
	SUM(case when distanceEdInd = 'Exclusive DE' and isInAmerica = true and residentStatus = 'UNKNOWN' then 1 else 0 end), -- field7,-- Of those students exclusively enrolled in de courses - Located in the U.S. but state/jurisdiction unknown 0 to 999999
	SUM(case when distanceEdInd = 'Exclusive DE' and isInAmerica = false then 1 else 0 end), -- field8,-- Of those students exclusively enrolled in de courses - Located outside the U.S. 0 to 999999
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
	select refactorstu.personId personId,
		refactorstu.ipedsPartGStudentLevel ipedsLevel,
		refactorstu.distanceEdInd distanceEdInd,
		refactorstu.residentStatus residentStatus,
		refactorstu.isInAmerica
	from CohortRefactorSTU refactorstu
	
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
	SUM(case when ipedsGender = 'M' then 1 else 0 end), -- field4, --Men, 0 to 999999 
	SUM(case when ipedsGender = 'F' then 1 else 0 end), -- field5, --Female, 0 to 999999
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
	select refactorstu.personId personId,
	    refactorstu.ipedsPartBStudentLevel ipedsLevel,
		refactorstu.ipedsAgeGroup ipedsAgeGroup,
		refactorstu.ipedsGender
	from CohortRefactorSTU refactorstu
	
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
	and (select clientconfig.reportAge
		 from ClientConfigMCR clientconfig) = 'Y'
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
	refactorstu.ipedsStateCode, -- field2, --State of residence, 1, 2, 4–6, 8–13, 15–42, 44–51, 53–57, 60, 64, 66, 68–70, 72, 78, 90 (valid FIPS codes, refer to state table in appendix) (98 and 99 are for export only)
	case when count(refactorstu.personId) > 0 then count(refactorstu.personId) else 1 end, -- field3, --Total first-time degree/certificate-seeking undergraduates, 1 to 999999 ***error in spec - not allowing 0
	NVL(SUM(case when date_add(refactorstu.hsGradDate, 365) >= refactorstu.censusDate then 1 else 0 end), 0), -- field4, --Total first-time degree/certificate-seeking undergraduates who enrolled within 12 months of graduating high school 0 to 999999 
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
from CohortRefactorSTU refactorstu
where refactorstu.studentType = 'First Time'
	and refactorstu.studentLevel = 'Undergrad' --all versions
--use client config value to apply optional/required survey section logic.
	and refactorstu.reportResidency = 'Y'
group by refactorstu.ipedsStateCode

union

--Part D: Total Undergraduate Entering Class
--**This section is only applicable to degree-granting, academic year reporters (calendar system = semester, quarter, trimester, 
--**or 4-1-4) that offer undergraduate level programs and reported full-time, first-time degree/certificate seeking students in Part A.
--undergraduate ONLY
--acad reporters ONLY

select 'D', -- part,
	5, -- sortorder,
	NULL, -- field1,
	case when COUNT(refactorstu.personId) = 0 then 1 else COUNT(refactorstu.personId) end, -- field2, --Total number of non-degree/certificate seeking undergraduates that are new to the institution in the Fall 1 to 999999 ***error in spec - not allowing 0
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
from CohortRefactorSTU refactorstu
where refactorstu.studentType in ('First Time', 'Transfer')
	and refactorstu.studentLevel = 'Undergrad' --all versions
	and refactorstu.acadOrProgReporter = 'A'

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
	select NVL(SUM(case when cohortret.timeStatus = 'Full'
							and cohortret.exclusionInd is null 
							and cohortret.inclusionInd is null 
	                   then 1 
					   else 0 
		end), 0) Ftft, --Full-time, first-time bachelor's cohort, 1 to 999999
		NVL(SUM(case when cohortret.timeStatus = 'Full' and cohortret.exclusionInd = 1 then 1 end), 0) FtftEx, --Full-time, first-time bachelor's cohort exclusions, 1 to 999999
		NVL(SUM(case when cohortret.timeStatus = 'Full' and cohortret.inclusionInd = 1 then 1 end), 0) FtftIn, --Full-time, first-time bachelor's cohort inclusions, 1 to 999999
		NVL(SUM(case when cohortret.timeStatus = 'Full' and cohortret.enrolledInd = 1 then 1 end), 0) FtftEn, --Full-time, first-time bachelor's cohort students still enrolled in current fall term, 1 to 999999
		NVL(SUM(case when cohortret.timeStatus = 'Part' 
						and cohortret.exclusionInd is null 
						and cohortret.inclusionInd is null 
					then 1 
					else 0 
		end), 0) Ptft, --Part-time, first-time bachelor's cohort, 1 to 999999
		NVL(SUM(case when cohortret.timeStatus = 'Part' and cohortret.exclusionInd = 1 then 1 end), 0) PtftEx, --Part-time, first-time bachelor's cohort exclusions, 1 to 999999
		NVL(SUM(case when cohortret.timeStatus = 'Part' and cohortret.inclusionInd = 1 then 1 end), 0) PtftIn, --Part-time, first-time bachelor's cohort inclusions, 1 to 999999
		NVL(SUM(case when cohortret.timeStatus = 'Part' and cohortret.enrolledInd = 1 then 1 end), 0) PtftEn
	from CohortSTU_RET cohortret
)

union

--jh 20200422 Moved inline views to from section instead of in select field

--Part F: Student-to-Faculty Ratio

select 'F', -- part,
	7, -- sortorder,
	NULL, -- field1,
	CAST(ROUND(NVL(ftestu.FTE, 0)/NVL(fteemp.FTE, 1)) as int), -- field2, --Student-to-faculty ratio, 0 - 100
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
from FTE_STU ftestu
	cross join FTE_EMP fteemp
