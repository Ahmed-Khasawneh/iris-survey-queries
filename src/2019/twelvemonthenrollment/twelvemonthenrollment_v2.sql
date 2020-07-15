/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey  
FILE NAME:      12 Month Enrollment v2 (E12)
FILE DESC:      12 Month Enrollment for less than 4-year institutions
AUTHOR:         jhanicak
CREATED:        20200609

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Cohort Creation
Formatting Views
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)      Author             	    Tag             	Comments
-----------         --------------------	-------------   	-------------------------------------------------
20200715            jhanicak                                    Bug fixes in CourseMCR, CourseTypeCountsSTU, PersonMCR and mods to support changes to these views (PF-1533) Run time 3m 53s
20200713			akhasawneh				ak 20200713 		Modification to course/hour counts (PF-1553) -Run time 2m 23s
																	Added course section status filter. 
																	Added course status filter.
20200706            jhanicak                                    Added new IPEDSClientConfig fields tmAnnualDPPCreditHoursFTE, instructionalActivityType, 
                                                                    icOfferUndergradAwardLevel, icOfferGraduateAwardLevel, icOfferDoctorAwardLevel PF-1536
                                                                Added new Person fields visaStartDate and visaEndDate PF-1536
                                                                Changed registrationStatusActionDate to registrationStatusActionDate PF-1536 
                                            jh 20200707         Added censusDate to DefaultValues and used as third choice in ReportPeriodMCR. Due to this new field, it allowed
                                                                    the join to AcademicTerm to be a left join, so that at least one record will always return.
                                            jh 20200707         Added an inline view to pull config values from ClientConfigMCR to ensure that there are always values for the indicators. 
                                                                    They were previously pulled in from CohortSTU.
                                            jh 20200707         Removed termCode and partOfTermCode from grouping, since not in select fields (runtime: 2m, 12m)
20200618			akhasawneh				ak 20200618			Modify 12 MO report query with standardized view naming/aliasing convention (PF-1535) (runtime: 1m, 33s)
20200616	       	akhasawneh              ak 20200616         Modified to not reference term code as a numeric indicator of term ordering (PF-1494) (runtime: 1m, 38s)
20200609	        jhanicak				                    Initial version PF-1410 (runtime: 1m, 29s)

********************/ 

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
/*******************************************************************
 Assigns all hard-coded values to variables. All date and version 
 adjustments and default values should be modified here. 

 In contrast to the Fall Enrollment report which is based on specific terms
 the 12 month enrollment is based on a full academic year and includes any
 full or partial term that starts within the academic year 7/1/2018 thru 6/30/2019
  ------------------------------------------------------------------
 Each client will need to determine how to identify and/or pull the 
 terms for their institution based on how they track the terms.  
 For some schools, it could be based on dates or academic year and for others,
 it may be by listing specific terms. 
 *******************************************************************/ 
select '1920' surveyYear, 
	'E1D' surveyId,  
	CAST('2018-07-01' AS TIMESTAMP) startDateProg,
	CAST('2019-06-30' AS TIMESTAMP) endDateProg,
	'202010' termCode,
	'1' partOfTermCode,
	CAST('2019-10-15' AS TIMESTAMP) censusDate, 
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

/*
--Use for testing internally only
--union 

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201330' termCode,
	'1' partOfTermCode, 
	CAST('2013-07-15' AS TIMESTAMP) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201410' termCode,
	'1' partOfTermCode,
	CAST('2013-10-15' AS TIMESTAMP) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201410' termCode,
	'A' partOfTermCode, 
	CAST('2013-10-15' AS TIMESTAMP) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201410' termCode,
	'B' partOfTermCode, 
	CAST('2013-10-15' AS TIMESTAMP) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No
    
union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201420' termCode,
	'1' partOfTermCode,
	CAST('2014-02-01' AS TIMESTAMP) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No
    
union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201420' termCode,
	'A' partOfTermCode,
	CAST('2014-02-01' AS TIMESTAMP) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No
    
union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201430' termCode,
	'1' partOfTermCode,
	CAST('2014-06-15' AS TIMESTAMP) censusDate, 
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No
*/
), 

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

select surveyYear,
	surveyId, 
	startDateProg,
	endDateProg,
	termCode, 
	partOfTermCode,
	censusDate,   
	includeNonDegreeAsUG,
	genderForUnknown,
	genderForNonBinary,
    tmAnnualDPPCreditHoursFTE,
    instructionalActivityType,
    icOfferUndergradAwardLevel,
    icOfferGraduateAwardLevel,
    icOfferDoctorAwardLevel
from (
    select configENT.surveyCollectionYear surveyYear,
		defvalues.surveyId surveyId, 
		defvalues.startDateProg	startDateProg,
		defvalues.endDateProg endDateProg,
		defvalues.termCode termCode, 
		defvalues.partOfTermCode partOfTermCode, 
		defvalues.censusDate censusDate,   
		nvl(configENT.includeNonDegreeAsUG, defvalues.includeNonDegreeAsUG) includeNonDegreeAsUG,
		nvl(configENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		nvl(configENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
		nvl(configENT.tmAnnualDPPCreditHoursFTE, defvalues.tmAnnualDPPCreditHoursFTE) tmAnnualDPPCreditHoursFTE,
        nvl(configENT.instructionalActivityType, defvalues.instructionalActivityType) instructionalActivityType,
        nvl(configENT.icOfferUndergradAwardLevel, defvalues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
		nvl(configENT.icOfferGraduateAwardLevel, defvalues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
        nvl(configENT.icOfferDoctorAwardLevel, defvalues.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,
		row_number() over (
			partition by
				configENT.surveyCollectionYear
			order by
				configENT.recordActivityDate desc
		) configRn
		from IPEDSClientConfig configENT
			cross join DefaultValues defvalues
		where configENT.surveyCollectionYear = defvalues.surveyYear 

    union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select defvalues.surveyYear surveyYear,
		defvalues.surveyId surveyId, 
		defvalues.startDateProg	startDateProg,
		defvalues.endDateProg endDateProg,
		defvalues.termCode termCode, 
		defvalues.partOfTermCode partOfTermCode,
		defvalues.censusDate censusDate,
		defvalues.includeNonDegreeAsUG includeNonDegreeAsUG,  
		defvalues.genderForUnknown genderForUnknown,
		defvalues.genderForNonBinary genderForNonBinary,
        defvalues.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
        defvalues.instructionalActivityType instructionalActivityType,
        defvalues.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		defvalues.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        defvalues.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
		1 configRn
    from DefaultValues defvalues
    where defvalues.surveyYear not in (select configENT.surveyCollectionYear
										from IPEDSClientConfig configENT
										where configENT.surveyCollectionYear = defvalues.surveyYear)
    	)
where configRn = 1	
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for each term and part of term code. 
--PartOfTerm code defines a subcategory of a termCode that may have different start, end and census dates. 
-- 1 record per term/partOfTerm

select termCode, 
	partOfTermCode, 
	startDate,
	endDate,
	academicYear,
	censusDate				   
from ( 
    select distinct acadtermENT.termCode, 
		acadtermENT.partOfTermCode, 
		acadtermENT.recordActivityDate, 
		acadtermENT.termCodeDescription,       
		acadtermENT.partOfTermCodeDescription, 
		acadtermENT.startDate,
		acadtermENT.endDate,
		acadtermENT.academicYear,
		acadtermENT.censusDate,
		acadtermENT.isIPEDSReportable,
		row_number() over (
			partition by 
				acadtermENT.termCode,
				acadtermENT.partOfTermCode
			order by  
				acadtermENT.recordActivityDate desc
		) acadTermRn
		from AcademicTerm acadtermENT 
		where acadtermENT.isIPEDSReportable = 1  
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

ReportingPeriodMCR as (
-- ReportingPeriodMCR combines the dates from the IPEDSReportingPeriod with the 
-- AcademicTermMCR subquery, If no records comes IPEDSReportingPeriod
-- it uses the default dates & default term information in the ConfigPerAsOfDate
-- to insure that there are some records to keep IRIS from failing .  --JDH

select distinct RepDates.surveyYear	surveyYear,
	RepDates.surveyId surveyId,
	RepDates.startDatePeriod startDatePeriod,
	RepDates.endDatePeriod endDatePeriod,
	RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,	
--jh 20200707 Added censusDate to DefaultValues and used as third choice. Due to this new field, it allowed
--   the join to AcademicTerm to be a left join, so that at least one record will always return.
	coalesce(acadtermENT.censusDate, DATE_ADD(acadtermENT.startDate, 15), RepDates.censusDate) censusDate, 
	RepDates.includeNonDegreeAsUG includeNonDegreeAsUG,
	RepDates.genderForUnknown genderForUnknown,
	RepDates.genderForNonBinary genderForNonBinary,
    RepDates.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
    RepDates.instructionalActivityType instructionalActivityType,
    RepDates.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
	RepDates.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
    RepDates.icOfferDoctorAwardLevel icOfferDoctorAwardLevel
from (
    select repperiodENT.surveyCollectionYear surveyYear,
		clientconfig.surveyId surveyId, 
		nvl(repperiodENT.reportingDateStart, clientconfig.startDateProg) startDatePeriod,
		nvl(repperiodENT.reportingDateEnd, clientconfig.endDateProg) endDatePeriod,
		nvl(repperiodENT.termCode, clientconfig.termCode) termCode,
		nvl(repperiodENT.partOfTermCode, clientconfig.partOfTermCode) partOfTermCode, 
		clientconfig.censusDate censusDate,
		clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary,
		clientconfig.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
        clientconfig.instructionalActivityType instructionalActivityType,
        clientconfig.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		clientconfig.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        clientconfig.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
		row_number() over (	
			partition by 
				repperiodENT.surveyCollectionYear,
				repperiodENT.surveyId, 
				repperiodENT.termCode,
				repperiodENT.partOfTermCode	
			order by 
				repperiodENT.recordActivityDate desc	
		) reportPeriodRn	
	from IPEDSReportingPeriod repperiodENT
		cross join ClientConfigMCR clientconfig
	where repperiodENT.surveyCollectionYear = clientconfig.surveyYear
		and repperiodENT.surveyId = clientconfig.surveyId
		and repperiodENT.termCode is not null
		and repperiodENT.partOfTermCode is not null
	
-- Added union to use default data when IPEDSReportingPeriod record doesn't exist so valid output file will always be generated
union 
 
	select clientconfig.surveyYear surveyYear, 
		clientconfig.surveyId surveyId, 
		clientconfig.startDateProg startDatePeriod,
		clientconfig.endDateProg endDatePeriod,
		clientconfig.termCode termCode,
		clientconfig.partOfTermCode partOfTermCode, 
		clientconfig.censusDate censusDate, 
		clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary,
        clientconfig.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
        clientconfig.instructionalActivityType instructionalActivityType,
        clientconfig.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		clientconfig.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        clientconfig.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
		1
	from ClientConfigMCR clientconfig
    where clientconfig.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = clientconfig.surveyYear
											and repperiodENT.surveyId = clientconfig.surveyId 
											and repperiodENT.termCode is not null
											and repperiodENT.partOfTermCode is not null)
    ) RepDates
		left join AcademicTermMCR acadtermENT on RepDates.termCode = acadtermENT.termCode	
			and RepDates.partOfTermCode = acadtermENT.partOfTermCode
where RepDates.reportPeriodRn = 1
), 

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusMCR as ( 
-- Returns most recent campus record for the ReportingPeriod. 
select campus,
	isInternational
from ( 
	select campusENT.campus,
		campusENT.campusDescription,
		campusENT.isInternational,
		row_number() over (
			partition by
				campusENT.campus
			order by
				campusENT.recordActivityDate desc
		) campusRn
	from ReportingPeriodMCR repperiod
		cross join Campus campusENT 
	where campusENT.isIpedsReportable = 1 
		and ((campusENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
			and campusENT.recordActivityDate <= repperiod.endDatePeriod)
                 or campusENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where campusRn = 1
), 
 
RegistrationMCR as ( 
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 

select personId,
	termCode,
	partOfTermCode, 
	censusDate,
	isInternational,    
	crnGradingMode,                    
	crn,
	crnLevel, 
	includeNonDegreeAsUG,
	genderForUnknown,
	genderForNonBinary,
    tmAnnualDPPCreditHoursFTE,
    instructionalActivityType,
    icOfferUndergradAwardLevel,
	icOfferGraduateAwardLevel,
    icOfferDoctorAwardLevel
from ( 
    select regENT.personId personId,
		regENT.termCode termCode,
		regENT.partOfTermCode partOfTermCode, 
		repperiod.censusDate censusDate,
		nvl(campus.isInternational, false) isInternational,    
		regENT.crnGradingMode crnGradingMode,                    
		regENT.crn crn,
		regENT.crnLevel crnLevel, 
		repperiod.includeNonDegreeAsUG includeNonDegreeAsUG,
		repperiod.genderForUnknown genderForUnknown,
		repperiod.genderForNonBinary genderForNonBinary,
        repperiod.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
        repperiod.instructionalActivityType instructionalActivityType,
        repperiod.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		repperiod.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        repperiod.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
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
		inner join Registration regENT on regENT.termCode = repperiod.termCode  
			and repperiod.partOfTermCode = regENT.partOfTermCode 
			and ((regENT.registrationStatusActionDate != CAST('9999-09-09' AS TIMESTAMP)
				and regENT.registrationStatusActionDate <= repperiod.censusDate)
					or regENT.registrationStatusActionDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and regENT.isEnrolled = 1
			and regENT.isIpedsReportable = 1 
		left join CampusMCR campus on regENT.campus = campus.campus  
	)
where regRn = 1
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  

select personId,
	termCode,
	isNonDegreeSeeking,
	studentLevel
from ( 
    select distinct studentENT.personId personId,
		studentENT.termCode termCode, 
		studentENT.isNonDegreeSeeking isNonDegreeSeeking,
		studentENT.studentLevel studentLevel,
		row_number() over (
			partition by
				studentENT.personId,
				studentENT.termCode
			order by
				studentENT.recordActivityDate desc
		) studRn
	from RegistrationMCR reg
		inner join Student studentENT on reg.personId = studentENT.personId 
			and reg.termCode = studentENT.termCode
			and ((studentENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)  
				and studentENT.recordActivityDate <= reg.censusDate)
					or studentENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and studentENT.studentStatus = 'Active' --do not report Study Abroad students
			and (studentENT.studentLevel = 'UnderGrad'
				or (studentENT.studentLevel = 'Continuing Ed' and reg.includeNonDegreeAsUG = 'Y')) 
			and studentENT.isIpedsReportable = 1
	)
where studRn = 1
),

CourseSectionMCR as (
--Included to get enrollment hours of a CRN

select distinct reg2.termCode,
	reg2.partOfTermCode,
	reg2.crn,
	CourseSect.section,
	CourseSect.subject,
	CourseSect.courseNumber,
	reg2.crnLevel,
	reg2.censusDate,
	CourseSect.enrollmentHours,
	CourseSect.isClockHours
from RegistrationMCR reg2 
    left join (
    select distinct coursesectENT.termCode termCode,
		coursesectENT.partOfTermCode partOfTermCode,
		coursesectENT.crn crn,
		coursesectENT.section section,
		coursesectENT.subject subject,
		coursesectENT.courseNumber courseNumber,
		coursesectENT.recordActivityDate recordActivityDate,
		CAST(coursesectENT.enrollmentHours as decimal(2,0)) enrollmentHours,
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
        inner join CourseSection coursesectENT on coursesectENT.termCode = reg.termCode
            and coursesectENT.partOfTermCode = reg.partOfTermCode
            and coursesectENT.crn = reg.crn
-- ak 20200713 Added course section status filter (PF-1553)
			and coursesectENT.sectionStatus = 'Active'
            and coursesectENT.isIpedsReportable = 1 
            and ((coursesectENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
                and coursesectENT.recordActivityDate <= reg.censusDate)
                    or coursesectENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	) CourseSect on reg2.termCode = CourseSect.termCode
            and reg2.partOfTermCode = CourseSect.partOfTermCode
            and reg2.crn = CourseSect.crn 
where CourseSect.courseRn = 1
), 

CourseSectionScheduleMCR  as (
--Returns course scheduling related info for the registration CRN.
--AcademicTerm.partOfTermCode, CourseSectionSchedule.partOfTermCode & AcademicTerm.censusDate together 
--are used to define the period of valid course registration attempts. 

select coursesect2.termCode termCode,
--ak 20200616 Adding term order indicator (PF-1494)
	termOrder.termOrder termOrder,
	coursesect2.censusDate censusDate,
	coursesect2.crn crn,
	coursesect2.section section,
	CourseSched.section sectionSched,
	coursesect2.partOfTermCode partOfTermCode,
	coursesect2.enrollmentHours enrollmentHours,
	coursesect2.isClockHours isClockHours,
	coursesect2.subject subject,
	coursesect2.courseNumber courseNumber,
	coursesect2.crnLevel crnLevel,
	nvl(CourseSched.meetingType, 'Standard') meetingType
from CourseSectionMCR coursesect2
--ak 20200616 Adding term order indicator (PF-1494)
	inner join AcadTermOrder termOrder 
		on termOrder.TermCode = coursesect2.termCode
	left join (
		select courseSectSchedENT.termCode termCode,
			courseSectSchedENT.crn crn,
			courseSectSchedENT.section section,
			courseSectSchedENT.partOfTermCode partOfTermCode,
			nvl(courseSectSchedENT.meetingType, 'Standard') meetingType,
			row_number() over (
				partition by
					courseSectSchedENT.crn,
					courseSectSchedENT.section,
					courseSectSchedENT.termCode,
					courseSectSchedENT.partOfTermCode
				order by
					courseSectSchedENT.recordActivityDate desc
			) courseSectSchedRn 
		from CourseSectionMCR coursesect
			inner join CourseSectionSchedule courseSectSchedENT on courseSectSchedENT.termCode = coursesect.termCode
				and courseSectSchedENT.partOfTermCode = coursesect.partOfTermCode
				and courseSectSchedENT.crn = coursesect.crn
				and (courseSectSchedENT.section = coursesect.section or coursesect.section is null)
				and courseSectSchedENT.isIpedsReportable = 1  
				and ((courseSectSchedENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
					and courseSectSchedENT.recordActivityDate <= coursesect.censusDate)
						or courseSectSchedENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
		) CourseSched 
			on CourseSched.termCode = coursesect2.termCode
			and CourseSched.partOfTermCode = coursesect2.partOfTermCode
			and CourseSched.crn = coursesect2.crn
			and (courseSched.section = coursesect2.section or coursesect2.section is null)
			and CourseSched.courseSectSchedRn = 1		 
), 

CourseMCR as (
-- Included to get course type information and it has included the most recent version
-- prior to the census date. 

select *
from (
select distinct 
        courseENT.subject subject,
        courseENT.courseNumber courseNumber,
        courseENT.courseLevel courseLevel,
        courseENT.isRemedial isRemedial,
        courseENT.isESL isESL,
        row_number() over (
            partition by
                courseENT.subject,
                courseENT.courseNumber,
                courseENT.courseLevel
            order by
                courseENT.termCodeEffective desc,
                courseENT.recordActivityDate desc
        ) courseRn
    from CourseSectionScheduleMCR coursesectsched 
        left join Course courseENT
            on courseENT.subject = coursesectsched.subject
            and courseENT.courseNumber = coursesectsched.courseNumber
            and courseENT.courseLevel = coursesectsched.crnLevel
            -- ak 20200713 Added course status filter (PF-1553)
			and courseENT.courseStatus = 'Active'
            and courseENT.isIpedsReportable = 1  
            and ((courseENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
                and courseENT.recordActivityDate <= coursesectsched.censusDate)
                    or courseENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
--ak 20200616 Adding term order indicator (PF-1494)
		inner join AcadTermOrder termOrder
			on termOrder.termCode = courseENT.termCodeEffective
	where termOrder.termOrder <= coursesectsched.termOrder
	)
where courseRn = 1
), 

PersonMCR as (
--Returns most up to date student personal information as of the reporting period and term census periods.
--I used the RegistrationMCR to limit list to only students in the reportable terms, but limit the
--Person Record to the latest record. 
	
select regId,
	personId,
	censusDate,
	birthDate,
	ethnicity,
	isHispanic,
	isMultipleRaces,
	isInUSOnVisa,
    visaStartDate,
    visaEndDate,
	isUSCitizen,
	gender
from (  
    select distinct reg.personId regId,
		reg.censusDate censusDate, 
		personENT.personId personId,
		personENT.birthDate birthDate,
		personENT.ethnicity ethnicity,
		personENT.isHispanic isHispanic,
		personENT.isMultipleRaces isMultipleRaces,
		personENT.isInUSOnVisa isInUSOnVisa,
        personENT.visaStartDate visaStartDate,
        personENT.visaEndDate visaEndDate,
		personENT.isUSCitizen isUSCitizen,
		personENT.gender gender,
		row_number() over (
			partition by
				personENT.personId
			order by
				personENT.recordActivityDate desc
		) personRn
	from RegistrationMCR reg
		left join Person personENT on reg.personId = personENT.personId 
			and ((personENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and personENT.recordActivityDate <= reg.censusDate) 
					or personENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
			and personENT.isIpedsReportable = 1
	)
where personRn = 1
),

/*****
BEGIN SECTION - Transformations
This set of views is used to transform and aggregate records from MCR views above.
*****/

CourseTypeCountsSTU as (
-- View used to break down course category type totals by type

select reg.personId personId,
	sum(case when coursesectsched.enrollmentHours >= 0 then 1 else 0 end) totalCourses,
	sum(case when coursesectsched.isClockHours = 0 and coursesectsched.enrollmentHours > 0 then coursesectsched.enrollmentHours else 0 end) totalCreditHrs,
	sum(case when coursesectsched.isClockHours = 1 and coursesectsched.enrollmentHours > 0 then coursesectsched.enrollmentHours else 0 end) totalClockHrs,
	sum(case when coursesectsched.enrollmentHours = 0 then 1 else 0 end) totalNonCredCourses,
	sum(case when coursesectsched.enrollmentHours > 0 then 1 else 0 end) totalCredCourses,
	sum(case when coursesectsched.meetingType = 'Online/Distance Learning' then 1 else 0 end) totalDECourses,
	sum(case when course.courseLevel = 'Continuing Ed' then 1 else 0 end) totalCECourses,
	sum(case when course.isESL = 'Y' then 1 else 0 end) totalESLCourses,
	sum(case when course.isRemedial = 'Y' then 1 else 0 end) totalRemCourses,
	sum(case when reg.isInternational = 1 then 1 else 0 end) totalIntlCourses, 
	sum(case when reg.crnGradingMode = 'Audit' then 1 else 0 end) totalAuditCourses
from RegistrationMCR reg
	left join CourseSectionScheduleMCR coursesectsched on reg.termCode = coursesectsched.termCode
	    and reg.partOfTermCode = coursesectsched.partOfTermCode
	    and reg.crn = coursesectsched.crn 
		and reg.crnLevel = coursesectsched.crnLevel
    left join CourseMCR course on coursesectsched.subject = course.subject
        and coursesectsched.courseNumber = course.courseNumber
        and coursesectsched.crnLevel = course.courseLevel
--jh 20200707 Removed termCode and partOfTermCode from grouping, since not in select fields
group by reg.personId
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

CohortSTU as (
--View used to get one record per student and studentLevel

select personID,  
	ipedsGender,
	ipedsEthnicity,  
    totalCreditHrs,
    totalClockHrs,
    tmAnnualDPPCreditHoursFTE,
    instructionalActivityType,
    icOfferUndergradAwardLevel,
	icOfferGraduateAwardLevel,
    icOfferDoctorAwardLevel 
from (
     select distinct reg.personID personID,
        reg.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
        reg.instructionalActivityType instructionalActivityType,
        reg.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		reg.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        reg.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
        student.studentLevel studentLevel,
		case when person.gender = 'Male' then 'M'
             when person.gender = 'Female' then 'F' 
             when person.gender = 'Non-Binary' then reg.genderForNonBinary
			else reg.genderForUnknown
		end ipedsGender,
		case when person.isUSCitizen = 1 then 
			  (case when person.isHispanic = true then '2' -- 'hispanic/latino'
				    when person.isMultipleRaces = true then '8' -- 'two or more races'
				    when person.ethnicity != 'Unknown' and person.ethnicity is not null then
				           (case when person.ethnicity = 'Hispanic or Latino' then '2'
				                when person.ethnicity = 'American Indian or Alaskan Native' then '3'
				                when person.ethnicity = 'Asian' then '4'
				                when person.ethnicity = 'Black or African American' then '5'
				                when person.ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
				                when person.ethnicity = 'Caucasian' then '7'
		                        else '9' 
			                end) 
			         else '9' end) -- 'race and ethnicity unknown'
		        when person.isInUSOnVisa = 1 and person.censusDate between person.visaStartDate and person.visaEndDate then '1' -- 'nonresident alien'
		        else '9' -- 'race and ethnicity unknown'
		end ipedsEthnicity,  
        coalesce(coursecnt.totalCreditHrs, 0) totalCreditHrs,
        coalesce(coursecnt.totalClockHrs, 0) totalClockHrs, 
-- Exclude Flag for those students whose total course counts equal excluded groups		
        case when coursecnt.totalRemCourses = coursecnt.totalCourses and student.isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
			 when coursecnt.totalCredCourses > 0 --exclude students not enrolled for credit
                        then (case when coursecnt.totalESLCourses = coursecnt.totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
                                    when coursecnt.totalCECourses = coursecnt.totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
                                    when coursecnt.totalIntlCourses = coursecnt.totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
                                    when coursecnt.totalAuditCourses = coursecnt.totalCredCourses then 0 --exclude students exclusively auditing classes
                                        -- when... then 0 --exclude PHD residents or interns 
                                        -- when... then 0 --exclude students in experimental Pell programs
                                    else 1
                            end)
            else 0 
        end ipedsInclude
    from RegistrationMCR reg  
        left join PersonMCR person on reg.personId = person.personId 
        inner join CourseTypeCountsSTU coursecnt on reg.personId = coursecnt.personId  
        inner join StudentMCR student on student.personId = reg.personId
    )
where ipedsInclude = 1
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

/* Part A: Unduplicated Count by Student Level, Gender, and Race/Ethnicity
Report all students enrolled for credit at any time during the July 1, 2018 - June 30, 2019 reporting period. 
Students are reported by gender, race/ethnicity, and their level of standing with the institution.
*/

select 'A' part,
       1 field1,  -- SLEVEL   - valid values are 1 for undergraduate and 3 for graduate
       coalesce(sum(case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end), 0) field2,  -- FYRACE01 - Nonresident alien - Men (1), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end), 0) field3,  -- FYRACE02 - Nonresident alien - Women (2), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end), 0) field4,  -- FYRACE25 - Hispanic/Latino - Men (25), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end), 0) field5,  -- FYRACE26 - Hispanic/Latino - Women (26), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end), 0) field6,  -- FYRACE27 - American Indian or Alaska Native - Men (27), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end), 0) field7,  -- FYRACE28 - American Indian or Alaska Native - Women (28), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end), 0) field8,  -- FYRACE29 - Asian - Men (29), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end), 0) field9,  -- FYRACE30 - Asian - Women (30), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end), 0) field10, -- FYRACE31 - Black or African American - Men (31), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end), 0) field11, -- FYRACE32 - Black or African American - Women (32), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end), 0) field12, -- FYRACE33 - Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end), 0) field13, -- FYRACE34 - Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end), 0) field14, -- FYRACE35 - White - Men (35), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end), 0) field15, -- FYRACE36 - White - Women (36), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end), 0) field16, -- FYRACE37 - Two or more races - Men (37), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end), 0) field17, -- FYRACE38 - Two or more races - Women (38), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end), 0) field18, -- FYRACE13 - Race and ethnicity unknown - Men (13), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end), 0) field19  -- FYRACE14 - Race and ethnicity unknown - Women (14), 0 to 999999               
from CohortSTU cohortstu


union

/* Part B: Instructional Activity and Full-Time Equivalent Enrollment
Report the total clock hour and/or credit hour activity attempted during the 12-month period of July 1, 2018 - June 30, 2019. 
The instructional activity data reported will be used to calculate full-time equivalent (FTE) student enrollment at the institution.
*/

select 'B' part,
       null field1,
       case when icOfferUndergradAwardLevel = 'Y' and instructionalActivityType != 'CL' then coalesce(totalCreditHrs, 0) 
            else null 
        end field2, -- CREDHRSU - credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.
       case when icOfferUndergradAwardLevel = 'Y' and instructionalActivityType != 'CR' then coalesce(totalClockHrs, 0) 
            else null 
        end field3, -- CONTHRS  - clock hour instructional activity at the undergraduate level
       null field4,
       null field5,
       null field6,
       null field7,
       null field8,
       null field9,
       null field10,
       null field11,
       null field12,
       null field13,
       null field14,
       null field15,
       null field16,
       null field17,
       null field18,
       null field19
from  ( 

--jh 20200707 Part B was not in the output if no records existed in CohortSTU. Added an inline view to pull config values from ClientConfigMCR
--   to ensure that there are always values for the indicators. They were previously pulled in from CohortSTU.

    select distinct hourTotals.totalCreditHrs,
                   hourTotals.totalClockHrs,
                   configValues.instructionalActivityType,
                   configValues.icOfferUndergradAwardLevel
    from (select sum(cohortstu.totalCreditHrs) totalCreditHrs,
                   sum(cohortstu.totalClockHrs) totalClockHrs
          from CohortSTU cohortstu) hourTotals
                cross join (select config.instructionalActivityType instructionalActivityType,
                                config.icOfferUndergradAwardLevel icOfferUndergradAwardLevel
                            from ClientConfigMCR config) configValues
    )
