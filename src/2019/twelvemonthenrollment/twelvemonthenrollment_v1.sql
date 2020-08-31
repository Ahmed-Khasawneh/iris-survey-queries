/********************

EVI PRODUCT:	DORIS 2019-20 IPEDS Survey  
FILE NAME: 		12 Month Enrollment v1 (E1D)
FILE DESC:      12 Month Enrollment for 4-year institutions
AUTHOR:         jhanicak/JD Hysler
CREATED:        20200609

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Transformations
Cohort Creation
Formatting Views
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)  	Author             	    Tag             	Comments
----------- 		--------------------	-------------   	-------------------------------------------------
20200825            akhasawneh              ak 20200825         Mods to default to dummy output where data is lacking (PF-1654) Run time 11m 26s
20200814            jhanicak                jh 20200814         Additional mods to support multi snapshot (PF-1449) Run time 6m 46s
20200729			akhasawneh				ak 20200729			Added support for multiple/historic ingestions (PF-1449) -Run time 6m 04s
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
                                            jh 20200707         Removed termCode and partOfTermCode from grouping, since not in select fields (runtime: 2m, 17m)
20200618			akhasawneh				ak 20200618			Modify 12 MO report query with standardized view naming/aliasing convention (PF-1535) (runtime: 1m, 10s)
20200616	       	akhasawneh              ak 20200616         Modified to not reference term code as a numeric indicator of term ordering (PF-1494) (runtime: 1m, 37s)
20200609	        jhanicak				                    Initial version PF-1409 (runtime: 1m, 40s)

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
	CAST('2018-07-01' AS DATE) reportingDateStart,
	CAST('2019-06-30' AS DATE) reportingDateEnd,
	'201910' termCode, --Fall 2018
	'1' partOfTermCode, 
	CAST('2019-10-15' AS DATE) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = No
/*
select '1920' surveyYear, 
	'E1D' surveyId,  
	CAST('2018-07-01' AS DATE) reportingDateStart,
	CAST('2019-06-30' AS DATE) reportingDateEnd,
	'201920' termCode, --Spring 2019
	'1' partOfTermCode, 
	CAST('2019-01-15' AS DATE) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = No

select '1415' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS DATE) reportingDateStart,
	CAST('2014-06-30' AS DATE) reportingDateEnd, 
	'201420' termCode,
	'1' partOfTermCode, 
	CAST('2014-01-13' AS DATE) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = Nounion

union

select '1415' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
	'201420' termCode,
	'A' partOfTermCode,
	CAST('2014-01-10' AS DATE) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = No
 
union 

select '1415' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201410' termCode,
	'1' partOfTermCode,
	CAST('2013-09-13' AS DATE) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
	'201410' termCode,
	'A' partOfTermCode, 
	CAST('2013-09-13' AS DATE) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201410' termCode,
	'B' partOfTermCode, 
	CAST('2013-11-08' AS DATE) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = No
 
union 

select '1415' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS DATE) reportingDateStart,
	CAST('2014-06-30' AS DATE) reportingDateEnd, 
	'201430' termCode,
	'1' partOfTermCode,
	CAST('2014-06-01' AS DATE) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201430' termCode,
	'A' partOfTermCode, 
	CAST('2014-06-01' AS DATE) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201430' termCode,
	'B' partOfTermCode, 
	CAST('2014-07-10' AS DATE) censusDate,
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = No
*/
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

select ConfigLatest.surveyYear surveyYear,
    upper(ConfigLatest.surveyId) surveyId,
    to_date(ConfigLatest.snapshotDate,'YYYY-MM-DD') snapshotDate,
    ConfigLatest.tags tags,
    ConfigLatest.termCode termCode,
    ConfigLatest.partOfTermCode partOfTermCode,
    ConfigLatest.reportingDateStart reportingDateStart,
    ConfigLatest.reportingDateEnd reportingDateEnd,
	ConfigLatest.censusDate censusDate,
	upper(ConfigLatest.includeNonDegreeAsUG) includeNonDegreeAsUG,
	upper(ConfigLatest.genderForUnknown) genderForUnknown,
	upper(ConfigLatest.genderForNonBinary) genderForNonBinary,
    upper(ConfigLatest.tmAnnualDPPCreditHoursFTE) tmAnnualDPPCreditHoursFTE,
    upper(ConfigLatest.instructionalActivityType) instructionalActivityType,
    upper(ConfigLatest.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
    upper(ConfigLatest.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
    upper(ConfigLatest.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'config' source,
-- ak 20200729 Adding snapshotDate reference
		clientConfigENT.snapshotDate snapshotDate, 
		clientConfigENT.tags tags,
		defvalues.surveyId surveyId, 
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		defvalues.termCode termCode, 
		defvalues.partOfTermCode partOfTermCode, 
		defvalues.censusDate censusDate,
		coalesce(clientConfigENT.includeNonDegreeAsUG, defvalues.includeNonDegreeAsUG) includeNonDegreeAsUG,
		coalesce(clientConfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		coalesce(clientConfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
		coalesce(clientConfigENT.tmAnnualDPPCreditHoursFTE, defvalues.tmAnnualDPPCreditHoursFTE) tmAnnualDPPCreditHoursFTE,
        coalesce(clientConfigENT.instructionalActivityType, defvalues.instructionalActivityType) instructionalActivityType,
        coalesce(clientConfigENT.icOfferUndergradAwardLevel, defvalues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
		coalesce(clientConfigENT.icOfferGraduateAwardLevel, defvalues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
        coalesce(clientConfigENT.icOfferDoctorAwardLevel, defvalues.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,
		row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
				clientConfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientConfigENT
		cross join DefaultValues defvalues
	where clientConfigENT.surveyCollectionYear = defvalues.surveyYear
	    and (array_contains(clientConfigENT.tags, 'June End')
			        or array_contains(clientConfigENT.tags, 'Full Year June End'))

    union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select defvalues.surveyYear surveyYear,
	    'default' source,
-- ak 20200729 Adding snapshotDate reference
	    CAST('9999-09-09' as DATE) snapshotDate,
	    null tags,
		defvalues.surveyId surveyId, 
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
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
    where defvalues.surveyYear not in (select max(configENT.surveyCollectionYear)
										from IPEDSClientConfig configENT
										where configENT.surveyCollectionYear = defvalues.surveyYear)
	) ConfigLatest
where ConfigLatest.configRn = 1	
),

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 

select distinct RepDates.surveyYear	surveyYear,
-- ak 20200729 Adding snapshotDate reference
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
    RepDates.tags tags,
    RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,
    to_date(RepDates.censusDate,'YYYY-MM-DD') censusDate,
	to_date(RepDates.reportingDateStart,'YYYY-MM-DD') reportingDateStart,
    to_date(RepDates.reportingDateEnd,'YYYY-MM-DD') reportingDateEnd,
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
	    'repperiod' source,
-- ak 20200729 Adding snapshotDate reference
		repperiodENT.snapshotDate snapshotDate,
		repPeriodENT.tags tags,
		repPeriodENT.surveyId surveyId, 
		coalesce(repperiodENT.reportingDateStart, clientconfig.reportingDateStart) reportingDateStart,
		coalesce(repperiodENT.reportingDateEnd, clientconfig.reportingDateEnd) reportingDateEnd,
		coalesce(repperiodENT.termCode, clientconfig.termCode) termCode,
		coalesce(repperiodENT.partOfTermCode, clientconfig.partOfTermCode) partOfTermCode,
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
				repPeriodENT.surveyCollectionYear,
                repPeriodENT.surveyId,
                repPeriodENT.surveySection, 
				repperiodENT.termCode,
				repperiodENT.partOfTermCode	
			order by 
                repperiodENT.recordActivityDate desc	
		) reportPeriodRn	
		from IPEDSReportingPeriod repperiodENT
			inner join ClientConfigMCR clientconfig on repperiodENT.surveyCollectionYear = clientconfig.surveyYear
			    and upper(repperiodENT.surveyId) = clientconfig.surveyId
			    and repperiodENT.termCode is not null
			    and repperiodENT.partOfTermCode is not null
			    and (array_contains(repperiodENT.tags, 'June End')
			        or array_contains(repperiodENT.tags, 'Full Year June End'))
	
-- Added union to use default data when IPEDSReportingPeriod record doesn't exist so valid output file will always be generated
union 
 
	select clientconfig.surveyYear surveyYear,
	    'default' source, 
-- ak 20200729 Adding snapshotDate reference
		clientconfig.snapshotDate snapshotDate,
		clientConfig.tags tags,
		clientconfig.surveyId surveyId, 
		clientconfig.reportingDateStart reportingDateStart,
		clientconfig.reportingDateEnd reportingDateEnd,
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
											and upper(repperiodENT.surveyId) = clientconfig.surveyId 
											and repperiodENT.termCode is not null
											and repperiodENT.partOfTermCode is not null) 
    ) RepDates
    where RepDates.reportPeriodRn = 1
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for all term codes and parts of term code for all snapshots. 

-- ak 20200729 Adding snapshotDate reference and determining termCode tied to the snapshotDate
-- ak 20200728 Move to after the ReportingPeriodMCR in order to bring in snapshot dates needed for reporting

select termCode, 
	partOfTermCode, 
	to_date(startDate, 'YYYY-MM-DD') startDate,
	to_date(endDate, 'YYYY-MM-DD') endDate,
	academicYear,
	to_date(censusDate, 'YYYY-MM-DD') censusDate,
	requiredFTCreditHoursGR,
	requiredFTCreditHoursUG,
	requiredFTClockHoursUG,
    to_date(snapshotDate, 'YYYY-MM-DD') snapshotDate,    
    tags
from (
    select distinct acadtermENT.termCode, 
        row_number() over (
            partition by 
                acadTermENT.termCode,
                acadTermENT.partOfTermCode
            order by
                case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                            and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
                    then acadTermENT.termCode 
                else acadTermENT.snapshotDate end desc,
                case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                            and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
                    then acadTermENT.partOfTermCode 
                else acadTermENT.snapshotDate end desc,
                acadTermENT.recordActivityDate desc
        ) acadTermRn,
        acadTermENT.snapshotDate,
        acadTermENT.tags,
		acadtermENT.partOfTermCode, 
		acadtermENT.recordActivityDate, 
		acadtermENT.termCodeDescription,       
		acadtermENT.partOfTermCodeDescription, 
		acadtermENT.startDate,
		acadtermENT.endDate,
		acadtermENT.academicYear,
		acadtermENT.censusDate,
		acadtermENT.requiredFTCreditHoursGR,
	    acadtermENT.requiredFTCreditHoursUG,
	    acadtermENT.requiredFTClockHoursUG,
		acadtermENT.isIPEDSReportable
	from AcademicTerm acadtermENT 
	where acadtermENT.isIPEDSReportable = 1
	)
where acadTermRn = 1
),

AcadTermOrder as (
-- Orders term codes based on date span and keeps the numeric value of the greatest term/part of term record. 

--ak 20200616 View created to determine max term order by term code (PF-1494)

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

AcademicTermReporting as (
--Combines ReportingPeriodMCR and AcademicTermMCR in order to use the correct snapshot dates for the reporting terms

-- ak 20200728 Added view to pull all academic term data for the terms associated with the reporting period
-- jh 20200814 Added fullTermOrder field to narrow the student and person data to the first full term the student is registered in RegistrationMinTerm

select repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        termorder.termOrder termOrder,
        acadterm.snapshotDate snapshotDate,
        case when array_contains(acadterm.tags, 'Fall Census') then 1
             when array_contains(acadterm.tags, 'Spring Census') then 2
             else 3
        end fullTermOrder,
        acadterm.startDate startDate,
        acadterm.endDate endDate,
        coalesce(acadterm.censusDate, repperiod.censusDate) censusDate,
        acadterm.requiredFTCreditHoursGR,
	    acadterm.requiredFTCreditHoursUG,
	    acadterm.requiredFTClockHoursUG,
	    coalesce(acadterm.requiredFTCreditHoursUG/
		    coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
        minTermStart.startDateMin reportingDateStart,
        maxTermEnd.endDateMax reportingDateEnd,
        repperiod.includeNonDegreeAsUG includeNonDegreeAsUG,
        repperiod.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
        repperiod.instructionalActivityType instructionalActivityType,
        repperiod.genderForUnknown genderForUnknown,
        repperiod.genderForNonBinary genderForNonBinary,
        repperiod.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
        repperiod.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        repperiod.icOfferDoctorAwardLevel icOfferDoctorAwardLevel
    from ReportingPeriodMCR repperiod 
        left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
	            and repperiod.partOfTermCode = acadterm.partOfTermCode
		inner join AcadTermOrder termorder
			on termOrder.termCode = repperiod.termCode
		left join (select min(acadterm1.startDate) startDateMin,
		            acadterm1.termCode termCode
                    from ReportingPeriodMCR repperiod1
		            left join AcademicTermMCR acadterm1 on repperiod1.termCode = acadterm1.termCode
		                and repperiod1.partOfTermCode = acadterm1.partOfTermCode
			         group by acadterm1.termCode
			         ) minTermStart on repperiod.termCode = minTermStart.termCode
		left join (select max(acadterm1.endDate) endDateMax,
		            acadterm1.termCode termCode
                    from ReportingPeriodMCR repperiod1
		            left join AcademicTermMCR acadterm1 on repperiod1.termCode = acadterm1.termCode
		                and repperiod1.partOfTermCode = acadterm1.partOfTermCode
			         group by acadterm1.termCode
			         ) maxTermEnd on repperiod.termCode = maxTermEnd.termCode
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusMCR as ( 
-- Returns most recent campus record for the ReportingPeriod

--We only use campus for international status. We are maintaining the ability to look at a campus at different points in time through relevant snapshots. will there ever be a case where a campus changes international status? 
--Otherwise, we could just get all unique campus codes and forget about when the record was made.

--Future consideration - Could we capture all recordActivityDate values for each snapshotDate and then filter when we join campus in a subsequent view?
--and/or - shouldn't the filter on recordActivityDate = the snapshotDate?

select campus,
	isInternational,
	snapshotDate
from ( 
    select campusENT.campus,
		campusENT.campusDescription,
		boolean(campusENT.isInternational),
		campusENT.snapshotDate,
		row_number() over (
			partition by
			    campusENT.snapshotDate, 
				campusENT.campus
			order by
				campusENT.recordActivityDate desc
		) campusRn
	from ReportingPeriodMCR repperiod
		cross join Campus campusENT 
	where campusENT.isIpedsReportable = 1 
		and ((to_date(campusENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS TIMESTAMP)
			and to_date(campusENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.reportingDateEnd)
				or to_date(campusENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS TIMESTAMP))
	)
where campusRn = 1
),

RegistrationMCR as ( 
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 

select personId,
    snapshotDate,
	termCode,
	partOfTermCode, 
	termorder,
	fullTermOrder,
	censusDate,
	startDate,
	requiredFTCreditHoursGR,
	requiredFTCreditHoursUG,
	requiredFTClockHoursUG,
	equivCRHRFactor,
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
        to_date(regENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		regENT.termCode termCode,
		regENT.partOfTermCode partOfTermCode, 
		repperiod.termorder termorder,
		repperiod.fullTermOrder fullTermOrder,
		repperiod.startDate startDate,
		repperiod.censusDate censusDate,
		repperiod.requiredFTCreditHoursGR,
	    repperiod.requiredFTCreditHoursUG,
	    repperiod.requiredFTClockHoursUG,
	    repperiod.equivCRHRFactor,
        coalesce(campus.isInternational, false) isInternational,
		regENT.crnGradingMode crnGradingMode,                    
		upper(regENT.crn) crn,
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
			order by regENT.recordActivityDate desc
		) regRn
	from AcademicTermReporting repperiod   
		inner join Registration regENT on regENT.termCode = repperiod.termCode
			and repperiod.partOfTermCode = regENT.partOfTermCode 
			and to_date(regENT.snapshotDate,'YYYY-MM-DD') = repperiod.snapshotDate
			and ((to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
				and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
					or to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
			and regENT.isEnrolled = 1
			and regENT.isIpedsReportable = 1 
		left join CampusMCR campus on regENT.campus = campus.campus  
		    and campus.snapshotDate = regENT.snapshotDate
	)
where regRn = 1
),

RegistrationMinTerm as (
-- Determine first full term in which student was enrolled 

-- jh 20200814 View added to satisfy the following IPEDS requirement: The enrollment level should be determined at the first “full” term (e.g., Fall) at entry. 

select personId,
        termCode,
        censusDate,
        snapshotDate
from (
    select personId personId,
            termCode termCode,
            censusDate censusDate,
            snapshotDate snapshotDate,
            row_number() over (
			    partition by
				    personId
			order by fullTermOrder asc,
			        startDate asc
	        ) regRn
	  from RegistrationMCR
)
where regRn = 1
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  
-- jh 20200814 
select personId,
    snapshotDate,
	termCode, 
	stuLevelCalc,
	isNonDegreeSeeking,
	studentLevel
from ( 
	select distinct studentENT.personId personId,
	    to_date(studentENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
		studentENT.termCode termCode, 
		case when studentENT.studentLevel = 'Continuing Ed' and reg.includeNonDegreeAsUG = 'Y' then 1 
			when studentENT.studentLevel = 'Undergrad' then 1
			when studentENT.studentLevel in ('Professional', 'Postgraduate','Graduate') then 3 
			else 0 
		end stuLevelCalc, -- translating to numeric for MaxStuLevel later on 
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
		    and reg.snapshotDate = to_date(studentENT.snapshotDate,'YYYY-MM-DD')
			and ((to_date(studentENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)  
				and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= reg.censusDate)
					or to_date(studentENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
			and studentENT.studentStatus = 'Active' --do not report Study Abroad students	    		
			and studentENT.isIpedsReportable = 1
		
	)
where studRn = 1
),

StudentMinTerm as (
-- Determine the student status based on the first full term in which student was enrolled 
-- jh 20200814 Added to satisfy the following IPEDS requirement:
-- The enrollment level should be determined at the first “full” term (e.g., Fall) at entry. 

select stu.personId,
	stu.isNonDegreeSeeking,
	stu.studentLevel
from StudentMCR stu
    inner join RegistrationMinTerm regmin on stu.personId = regmin.personId
        and stu.termCode = regmin.termCode
),


PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 
-- jh 20200814 Modified to use the first full term within the full year period

select *
from (
    select distinct personENT.personId personId,
            reg.censusDate censusDate,
            to_date(personENT.birthDate,'YYYY-MM-DD') birthDate,
            personENT.ethnicity ethnicity,
            personENT.isHispanic isHispanic,
            personENT.isMultipleRaces isMultipleRaces,
            personENT.isInUSOnVisa isInUSOnVisa,
            to_date(personENT.visaStartDate,'YYYY-MM-DD') visaStartDate,
            to_date(personENT.visaEndDate,'YYYY-MM-DD') visaEndDate,
            personENT.isUSCitizen isUSCitizen,
            personENT.gender gender,
            upper(personENT.nation) nation,
            upper(personENT.state) state,
            row_number() over (
                partition by
                    personENT.personId
                order by
                    personENT.recordActivityDate desc
            ) personRn
    from RegistrationMinTerm reg
        inner join Person personENT on reg.personId = personENT.personId
            and to_date(personENT.snapshotDate,'YYYY-MM-DD') = reg.snapshotDate
            and personENT.isIpedsReportable = 1
--ak 20200406 Including dummy date changes. (PF-1368)
            and ((to_date(personENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
               and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= reg.censusDate) 
                or to_date(personENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
    )
where personRn = 1
),

CourseSectionMCR as (
--Included to get enrollment hours of a CRN

select *
from (
    select distinct CourseSect.snapshotDate,
        reg2.termCode termCode,
        reg2.partOfTermCode partOfTermCode,
        reg2.censusDate,
        reg2.termOrder,
        reg2.crn,
        CourseSect.section,
        CourseSect.subject,
        CourseSect.courseNumber,
        reg2.crnLevel,
        reg2.censusDate,
        CourseSect.enrollmentHours,
        reg2.equivCRHRFactor,
        CourseSect.isClockHours,
        reg2.crnGradingMode,
        reg2.instructionalActivityType,
            row_number() over (
                partition by
                    reg2.censusDate,
                    reg2.crn,
                    CourseSect.section,
                    reg2.crnLevel,
                    CourseSect.subject,
                    CourseSect.courseNumber
                order by
                    CourseSect.recordActivityDate desc
            ) courseRn
    from RegistrationMCR reg2 
        left join (
            select distinct to_date(coursesectENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
                coursesectENT.termCode termCode,
                coursesectENT.partOfTermCode partOfTermCode,
                upper(coursesectENT.crn) crn,
                upper(coursesectENT.section) section,
                upper(coursesectENT.subject) subject,
                upper(coursesectENT.courseNumber) courseNumber,
                to_date(coursesectENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
                CAST(coursesectENT.enrollmentHours as decimal(2,0)) enrollmentHours,
                coursesectENT.isClockHours isClockHours
            from RegistrationMCR reg 
                inner join CourseSection coursesectENT on coursesectENT.termCode = reg.termCode
                    and coursesectENT.partOfTermCode = reg.partOfTermCode
                    and to_date(coursesectENT.snapshotDate,'YYYY-MM-DD') = reg.snapshotDate
                    and upper(coursesectENT.crn) = reg.crn
    -- ak 20200713 Added course section status filter (PF-1553)
                    and coursesectENT.sectionStatus = 'Active'
                    and coursesectENT.isIpedsReportable = 1                 
        ) CourseSect on reg2.termCode = CourseSect.termCode
    and reg2.partOfTermCode = CourseSect.partOfTermCode
    and reg2.crn = CourseSect.crn 
    and reg2.snapshotDate = CourseSect.snapshotDate
    and ((CourseSect.recordActivityDate != CAST('9999-09-09' AS DATE)
            and CourseSect.recordActivityDate <= reg2.censusDate)
                or CourseSect.recordActivityDate = CAST('9999-09-09' AS DATE))  
    )
where courseRn = 1
),

CourseSectionScheduleMCR as (
--Returns course scheduling related info for the registration CRN.
--AcademicTerm.partOfTermCode, CourseSectionSchedule.partOfTermCode & AcademicTerm.censusDate together are used to define the period 
--of valid course registration attempts. 

select *
from (
	select coursesect2.snapshotDate snapshotDate,
		coursesect2.censusDate censusDate,
		coursesect2.crn crn,
		coursesect2.section section,
		coursesect2.termCode termCode,
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
		coursesect2.termOrder termOrder, 
		coursesect2.partOfTermCode partOfTermCode,
		coursesect2.equivCRHRFactor equivCRHRFactor,
		coursesect2.subject subject,
		coursesect2.courseNumber courseNumber,
		coursesect2.crnLevel crnLevel,
		coalesce(SchedRec.meetingType, 'Standard') meetingType,
		coursesect2.enrollmentHours enrollmentHours,
		coursesect2.isClockHours isClockHours,
        coursesect2.crnGradingMode crnGradingMode,
-- ak 20200707 Adding additional client config values for level offerings and instructor activity type (PF-1552)
		coursesect2.instructionalActivityType instructionalActivityType,
		row_number() over (
			partition by
			    coursesect2.termCode, 
				coursesect2.partOfTermCode,
			    coursesect2.crn,
			    coursesect2.section
			order by
			    SchedRec.recordActivityDate desc
		) courseSectSchedRn
	from CourseSectionMCR coursesect2
	    left join (select upper(coursesectschedENT.crn) crn,
				    upper(coursesectschedENT.section) section,
				    coursesectschedENT.termCode,
				    coursesectschedENT.partOfTermCode,
				    to_date(courseSectSchedENT.snapshotDate,'yyyy-MM-dd') snapshotDate,
				    coalesce(coursesectschedENT.meetingType, 'Standard') meetingType,
				    to_date(coursesectschedENT.recordActivityDate,'yyyy-MM-dd') recordActivityDate
			    from CourseSectionMCR coursesect
		            left join CourseSectionSchedule coursesectschedENT ON coursesectschedENT.termCode = coursesect.termCode
			            and coursesectschedENT.partOfTermCode = coursesect.partOfTermCode
			            and to_date(courseSectSchedENT.snapshotDate,'yyyy-MM-dd') = coursesect.snapshotDate
			            and upper(coursesectschedENT.crn) = coursesect.crn
			            and (upper(courseSectSchedENT.section) = coursesect.section or coursesect.section is null)
			            and coursesectschedENT.isIpedsReportable = 1 
	            ) SchedRec on SchedRec.termCode = coursesect2.termCode
	                and SchedRec.partOfTermCode = coursesect2.partOfTermCode
	                and SchedRec.snapshotDate = coursesect2.snapshotDate
	                and SchedRec.crn = coursesect2.crn
	                and (SchedRec.section = coursesect2.section or coursesect2.section is null)
--ak 20200406 Including dummy date changes. (PF-1368)
	                and ((SchedRec.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
	            	    and SchedRec.recordActivityDate <= coursesect2.censusDate) --use same censusDate as CourseSectionMCR
			                or SchedRec.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where courseSectSchedRn = 1
),

CourseMCR as (
--Included to get course type information
--jh 20200422 removed ReportingPeriod and CourseSectionMCR joins and added section field

select *
from (
	select distinct coursesectsched2.snapshotDate snapshotDate,
	    coursesectsched2.termCode termCode,
		coursesectsched2.partOfTermCode partOfTermCode,
		CourseRec.termOrder courseRecTermOrder,
		coursesectsched2.termOrder courseSectTermOrder,
		coursesectsched2.censusDate censusDate,
		coursesectsched2.crn crn,
		coursesectsched2.section section,
		coursesectsched2.subject subject,
		coursesectsched2.courseNumber courseNumber,
		coursesectsched2.crnLevel courseLevel,
		CourseRec.isRemedial isRemedial,
		CourseRec.isESL isESL,
		coursesectsched2.meetingType meetingType,
		coalesce(coursesectsched2.enrollmentHours, 0) enrollmentHours,
		coursesectsched2.isClockHours isClockHours,
        coursesectsched2.equivCRHRFactor equivCRHRFactor,
        coursesectsched2.crnGradingMode crnGradingMode,
-- ak 20200707 Adding additional client config values for level offerings and instructor activity type (PF-1552)
        coursesectsched2.instructionalActivityType instructionalActivityType,
		row_number() over (
			partition by
			    coursesectsched2.termCode,
				coursesectsched2.partOfTermCode,
			    coursesectsched2.subject,
				coursesectsched2.courseNumber,
				coursesectsched2.crn,
				coursesectsched2.section
			order by
				CourseRec.termOrder desc,
				CourseRec.recordActivityDate desc
		) courseRn
	from CourseSectionScheduleMCR coursesectsched2
	    left join (
            select upper(courseENT.subject) subject,
				upper(courseENT.courseNumber) courseNumber,
				courseENT.termCodeEffective termCodeEffective,
                to_date(courseENT.snapshotDate,'yyyy-MM-dd') snapshotDate,
				max(termorder.termOrder) termOrder,
				courseENT.courseLevel courseLevel,
				to_date(courseENT.recordActivityDate,'yyyy-MM-dd') recordActivityDate,
				courseENT.isRemedial isRemedial,
		        courseENT.isESL isESL
	        from CourseSectionScheduleMCR coursesectsched
		        inner join Course courseENT on upper(courseENT.subject) = coursesectsched.subject
			        and upper(courseENT.courseNumber) = coursesectsched.courseNumber
                    and to_date(courseENT.snapshotDate,'yyyy-MM-dd') = coursesectsched.snapshotDate
			        and courseENT.courseLevel = coursesectsched.crnLevel
			        and courseENT.isIpedsReportable = 1
			        and courseENT.courseStatus = 'Active'
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
				inner join AcadTermOrder termorder on termorder.termCode = courseENT.termCodeEffective
			group by courseENT.subject,
				courseENT.courseNumber,
				courseENT.termCodeEffective,
                courseENT.snapshotDate,
				courseENT.courseLevel,
				courseENT.recordActivityDate,
				courseENT.isRemedial,
		        courseENT.isESL
			) CourseRec on CourseRec.subject = coursesectsched2.subject
			        and CourseRec.courseNumber = coursesectsched2.courseNumber
                    and CourseRec.snapshotDate = coursesectsched2.snapshotDate
			        and CourseRec.courseLevel = coursesectsched2.crnLevel
--ak 20200406 Including dummy date changes. (PF-1368)
			        and ((CourseRec.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				        and CourseRec.recordActivityDate <= coursesectsched2.censusDate) 
					        or CourseRec.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	                and CourseRec.termOrder <= coursesectsched2.termOrder
	)
where courseRn = 1
),

/*****
BEGIN SECTION - Transformations
This set of views is used to transform and aggregate records from MCR views above.
*****/

StudentLevelRefactor as (
-- Included to get the Max Student level for the Reporting Period as a whole. 

select student.personId,  
	max(student.stuLevelCalc) maxStuLevelCalcA
from StudentMCR student
group by student.personId 
), 

CourseTypeCountsSTU as (
-- View used to break down course category type counts by type

select reg.personId personId,
	sum(case when course.enrollmentHours >= 0 then 1 else 0 end) totalCourses,
	sum(case when course.isClockHours = 0 and course.enrollmentHours > 0 then course.enrollmentHours else 0 end) totalCreditHrs,
	sum(case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then nvl(course.enrollmentHours, 0) else 0 end) totalCreditUGHrs,
	sum(case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel in ('Graduate', 'Professional') then nvl(course.enrollmentHours, 0) else 0 end) totalCreditGRHrs,
	sum(case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel = 'Postgraduate' then nvl(course.enrollmentHours, 0) else 0 end) totalCreditPostGRHrs,
	sum(case when course.isClockHours = 1 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then course.enrollmentHours else 0 end) totalClockHrs,
	sum(case when course.enrollmentHours = 0 then 1 else 0 end) totalNonCredCourses,
	sum(case when course.enrollmentHours > 0 then 1 else 0 end) totalCredCourses,
	sum(case when course.meetingType = 'Online/Distance Learning' then 1 else 0 end) totalDECourses,
	sum(case when course.courseLevel = 'Undergrad' then 1 else 0 end) totalUGCourses,
	sum(case when course.courseLevel in ('Graduate', 'Professional') then 1 else 0 end) totalGRCourses,
	sum(case when course.courseLevel = 'Postgraduate' then 1 else 0 end) totalPostGRCourses,
	sum(case when course.courseLevel = 'Continuing Ed' then 1 else 0 end) totalCECourses,
	sum(case when course.isESL = 'Y' then 1 else 0 end) totalESLCourses,
	sum(case when course.isRemedial = 'Y' then 1 else 0 end) totalRemCourses,
	sum(case when reg.isInternational = 1 then 1 else 0 end) totalIntlCourses, 
	sum(case when reg.crnGradingMode = 'Audit' then 1 else 0 end) totalAuditCourses
from RegistrationMCR reg
	left join CourseMCR course on reg.termCode = course.termCode
        and reg.partOfTermCode = course.partOfTermCode
		and reg.crn = course.crn
		and reg.crnLevel = course.courseLevel 
		and reg.snapshotDate = course.snapshotDate
--jh 20200707 Removed termCode and partOfTermCode from grouping, since not in select fields
group by personId
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

CohortSTU as (
--View used to get one record per student and studentLevel

select personID,  
    studentLevel,
    ipedsPartAStudentLevel,
    ipedsGender,
	ipedsEthnicity,  
    totalCreditHrs,
    totalCreditUGHrs,
    totalCreditGRHrs,
    totalCreditPostGRHrs,
    totalClockUGHrs,
    tmAnnualDPPCreditHoursFTE,
    instructionalActivityType,
    icOfferUndergradAwardLevel,
	icOfferGraduateAwardLevel,
    icOfferDoctorAwardLevel
from ( 
     select distinct reg.personID personID,  
        reg.censusDate censusDate, 
        stulevelref.maxStuLevelCalcA ipedsPartAStudentLevel, 
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
			  (case when person.isHispanic = true then '2' 
				    when person.isMultipleRaces = true then '8' 
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
        coalesce(coursecnt.totalClockHrs, 0) totalClockUGHrs,
        coalesce(coursecnt.totalCreditUGHrs, 0) totalCreditUGHrs,
        coalesce(coursecnt.totalCreditGRHrs, 0) totalCreditGRHrs,
        coalesce(coursecnt.totalCreditPostGRHrs, 0) totalCreditPostGRHrs,
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
        left join CourseTypeCountsSTU coursecnt on reg.personId = coursecnt.personId  
        inner join StudentMinTerm student on student.personId = reg.personId 
        inner join StudentLevelRefactor stulevelref on stulevelref.personId = reg.personId
    )
where ipedsInclude = 1
),

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
****/

StudentLevelFMT_A as (
select *
from (
	VALUES 
		(1), --Undergraduate students
		(3)  --Graduate students
	) as StudentLevel(ipedsLevel)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

/* Part A: Unduplicated Count by Student Level, Gender, and Race/Ethnicity
Report all students enrolled for credit at any time during the July 1, 2018 - June 30, 2019 reporting period. 
Students are reported by gender, race/ethnicity, and their level of standing with the institution.
*/

select 'A'                                                                                      part,
       ipedsLevel                                                                               field1,  -- SLEVEL   - valid values are 1 for undergraduate and 3 for graduate
       coalesce(sum(case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end), 0) field2,  -- FYRACE01 - Nonresident alien - Men (1), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end), 0) field3,  -- FYRACE02 - Nonresident alien - Women (2), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end), 0) field4,  -- FYRACE25 - Hispanic/Latino - Men (25), 0 to 999999
       cast(coalesce(sum(case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end), 0) as string) field5,  -- FYRACE26 - Hispanic/Latino - Women (26), 0 to 999999
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
from (
		select cohortstu.personId               personId,
			cohortstu.ipedsPartAStudentLevel ipedsLevel,
			cohortstu.ipedsGender            ipedsGender,
			cohortstu.ipedsEthnicity         ipedsEthnicity
		from CohortSTU cohortstu
		where cohortstu.ipedsPartAStudentLevel = 1
			or cohortstu.ipedsPartAStudentLevel = 3

		union
	
		select null, --personId,
			stulevela.ipedsLevel,
			null, -- ipedsGender,
			null -- ipedsEthnicity
		from StudentLevelFMT_A stulevela
	
		union 
	
-- ak 20200825 Dummy set to return default formatting if no cohortSTU records exist.
		select *
		from (
			VALUES
				(null, 1, null, null),
				(null, 3, null, null)
			) as dummySet(personId, ipedsLevel, ipedsGender, ipedsEthnicity)
		where not exists (select a.personId from CohortSTU a) 	    
	)
group by ipedsLevel

union

select 'B' part,
       null field1,
       case when icOfferUndergradAwardLevel = 'Y' and instructionalActivityType != 'CL' then coalesce(totalCreditUGHrs, 0) 
            else null 
        end field2, -- CREDHRSU - credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.
       case when icOfferUndergradAwardLevel = 'Y' and instructionalActivityType != 'CR' then coalesce(totalClockUGHrs, 0) 
            else null 
        end field3, -- CONTHRS  - clock hour instructional activity at the undergraduate level, 0 to 9999999, blank = not applicable, if no undergraduate programs are measured in clock hours.
       case when icOfferGraduateAwardLevel = 'N' then null
            else coalesce(totalCreditGRHrs, 0)
       end field4, -- CREDHRSG - credit hour instructional activity at the graduate level, 0 to 99999999, blank = not applicable
       case when icOfferDoctorAwardLevel = 'N' then null
            when coalesce(totalCreditPostGRHrs, 0) > 0 then cast(round(totalCreditPostGRHrs / tmAnnualDPPCreditHoursFTE, 0) as string)
         else '0' 
       end field5, -- RDOCFTE  - reported Doctor'92s degree-professional practice student FTE, 0 to 99999999, blank = not applicable
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

    select distinct hourTotals.totalCreditUGHrs totalCreditUGHrs,
                   hourTotals.totalClockUGHrs totalClockUGHrs,
                   hourTotals.totalCreditGRHrs totalCreditGRHrs,
                   hourTotals.totalCreditPostGRHrs totalCreditPostGRHrs,
                   configValues.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
                   configValues.instructionalActivityType instructionalActivityType,
                   configValues.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
                   configValues.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
                   configValues.icOfferDoctorAwardLevel icOfferDoctorAwardLevel
    from (select sum(cohortstu.totalCreditUGHrs) totalCreditUGHrs,
                   sum(cohortstu.totalClockUGHrs) totalClockUGHrs,
                   sum(cohortstu.totalCreditGRHrs) totalCreditGRHrs,
                   sum(cohortstu.totalCreditPostGRHrs) totalCreditPostGRHrs
          from CohortSTU cohortstu) hourTotals
                cross join (select config.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
                                config.instructionalActivityType instructionalActivityType,
                                config.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
                                config.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
                                config.icOfferDoctorAwardLevel icOfferDoctorAwardLevel
                            from ClientConfigMCR config) configValues
)
where exists (select a.personId from CohortSTU a) 

union 
	
-- ak 20200825 Dummy set to return default formatting if no cohortSTU records exist.
	select *
	from (
		VALUES
			('B', null, 0, 0, 0, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
		) as dummySet(part, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11,
									field12, field13, field14, field15, field16, field17, field18, field19)
	where not exists (select a.personId from CohortSTU a) 
