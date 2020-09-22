/********************

EVI PRODUCT:	DORIS 2020-21 IPEDS Survey  
FILE NAME: 		12 Month Enrollment v1 (E1D)
FILE DESC:      12 Month Enrollment for  4-year institutions
AUTHOR:         jhanicak
CREATED:        20200911

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Transformations
Cohort Creation
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)  	Author             	    Tag             	Comments
----------- 		--------------------	-------------   	-------------------------------------------------
20200917            jhanicak                jh 20200917         Commented out all references to tags field, fixed row_number() in AcademicTermReporting,
                                                                fixed enum strings in StudentRefactor, changed AcadTermOrder to AcademicTermOrder PF-1681
20200911            jhanicak                jh 20200911         New 20-21 version Run time 14m 6s, Test data 20m, 23s
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

select '2021' surveyYear, 
	'E1D' surveyId,  
	CAST('2019-07-01' AS DATE) reportingDateStart,
	CAST('2020-06-30' AS DATE) reportingDateEnd,
	'202010' termCode, --Fall 2018
	'1' partOfTermCode, 
	CAST('2020-10-15' AS DATE) censusDate,
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary,      --M = Male, F = Female 
    12 tmAnnualDPPCreditHoursFTE, --1 to 99
    'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'Y' icOfferDoctorAwardLevel --Y = Yes, N = No

/*
select '1415' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS DATE) reportingDateStart,
	CAST('2014-06-30' AS DATE) reportingDateEnd, 
	'201420' termCode,
	'1' partOfTermCode, 
	CAST('2014-01-13' AS DATE) censusDate,
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
	'201330' termCode,
	'1' partOfTermCode,
	CAST('2013-06-10' AS DATE) censusDate,
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
	'201330' termCode,
	'A' partOfTermCode, 
	CAST('2013-06-10' AS DATE) censusDate,
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
	'201330' termCode,
	'B' partOfTermCode, 
	CAST('2013-07-10' AS DATE) censusDate,
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

-- jh 20200911 Removed select field for includeNonDegreeAsUG from IPEDSClientConfig - no longer needed since we report non-degree seeking
-- jh 20200911 Removed filter in first union for tag values and added a case stmt to the row_number() function to handle no snapshots that match tags 
--          1st union 1st order - pull snapshot for 'Full Year Term End' 
--          1st union 2nd order - pull snapshot for 'Full Year June End'
--          1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--          2nd union - pull default values if no record in IPEDSClientConfig
-- ak 20200729 Adding snapshotDate reference

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.source source,
    upper(ConfigLatest.surveyId) surveyId,
    to_date(ConfigLatest.snapshotDate,'YYYY-MM-DD') snapshotDate,
    --ConfigLatest.tags tags,
    ConfigLatest.termCode termCode,
    ConfigLatest.partOfTermCode partOfTermCode,
    ConfigLatest.reportingDateStart reportingDateStart,
    ConfigLatest.reportingDateEnd reportingDateEnd,
	ConfigLatest.censusDate censusDate,
	upper(ConfigLatest.genderForUnknown) genderForUnknown,
	upper(ConfigLatest.genderForNonBinary) genderForNonBinary,
    upper(ConfigLatest.tmAnnualDPPCreditHoursFTE) tmAnnualDPPCreditHoursFTE,
    upper(ConfigLatest.instructionalActivityType) instructionalActivityType,
    upper(ConfigLatest.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
    upper(ConfigLatest.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
    upper(ConfigLatest.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'configFullYearTag' source,
		clientConfigENT.snapshotDate snapshotDate, 
		--clientConfigENT.tags tags,
		defvalues.surveyId surveyId, 
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		defvalues.termCode termCode, 
		defvalues.partOfTermCode partOfTermCode, 
		defvalues.censusDate censusDate,
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
                (case when array_contains(clientConfigENT.tags, 'Full Year Term End') then 1
                     when array_contains(clientConfigENT.tags, 'Full Year June End') then 1
			         else 2 end) asc,
			    clientConfigENT.snapshotDate desc,
				clientConfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientConfigENT
		cross join DefaultValues defvalues
	where clientConfigENT.surveyCollectionYear = defvalues.surveyYear

    union

	select defvalues.surveyYear surveyYear,
	    'default' source,
	    CAST('9999-09-09' as DATE) snapshotDate,
	    --array() tags,
		defvalues.surveyId surveyId, 
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		defvalues.termCode termCode, 
		defvalues.partOfTermCode partOfTermCode,
		defvalues.censusDate censusDate,  
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

-- jh 20200911 Removed filter in first union for tag values and added a case stmt to the row_number() function to handle no snapshots that match tags
--          1st union 1st order - pull snapshot for 'Full Year Term End' 
--          1st union 2nd order - pull snapshot for 'Full Year June End'
--          1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--          2nd union - pull default values if no record in IPEDSReportingPeriod
-- ak 20200729 Adding snapshotDate reference

select distinct RepDates.surveyYear	surveyYear,
    RepDates.source source,
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
    --RepDates.tags tags,
    RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,
    to_date(RepDates.censusDate,'YYYY-MM-DD') censusDate,
	to_date(RepDates.reportingDateStart,'YYYY-MM-DD') reportingDateStart,
    to_date(RepDates.reportingDateEnd,'YYYY-MM-DD') reportingDateEnd,
	RepDates.genderForUnknown genderForUnknown,
	RepDates.genderForNonBinary genderForNonBinary,
    RepDates.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
    RepDates.instructionalActivityType instructionalActivityType,
    RepDates.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
	RepDates.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
    RepDates.icOfferDoctorAwardLevel icOfferDoctorAwardLevel
from (
    select repperiodENT.surveyCollectionYear surveyYear,
	    'repperiodSnapshotMatch' source,
		repperiodENT.snapshotDate snapshotDate,
		--repPeriodENT.tags tags,
		repPeriodENT.surveyId surveyId, 
		coalesce(repperiodENT.reportingDateStart, clientconfig.reportingDateStart) reportingDateStart,
		coalesce(repperiodENT.reportingDateEnd, clientconfig.reportingDateEnd) reportingDateEnd,
		coalesce(repperiodENT.termCode, clientconfig.termCode) termCode,
		coalesce(repperiodENT.partOfTermCode, clientconfig.partOfTermCode) partOfTermCode,
		clientconfig.censusDate censusDate,
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
			    (case when array_contains(repperiodENT.tags, 'Full Year Term End') then 1
                     when array_contains(repperiodENT.tags, 'Full Year June End') then 1
			         else 2 end) asc,
			    repperiodENT.snapshotDate desc,
                repperiodENT.recordActivityDate desc	
		) reportPeriodRn	
		from IPEDSReportingPeriod repperiodENT
			inner join ClientConfigMCR clientconfig on repperiodENT.surveyCollectionYear = clientconfig.surveyYear
			    and upper(repperiodENT.surveyId) = clientconfig.surveyId
			    and repperiodENT.termCode is not null
			    and repperiodENT.partOfTermCode is not null
	
    union 
 
	select clientconfig.surveyYear surveyYear,
	    'default' source,
		clientconfig.snapshotDate snapshotDate,
		--clientConfig.tags tags,
		clientconfig.surveyId surveyId, 
		clientconfig.reportingDateStart reportingDateStart,
		clientconfig.reportingDateEnd reportingDateEnd,
		clientconfig.termCode termCode,
		clientconfig.partOfTermCode partOfTermCode, 
		clientconfig.censusDate censusDate,
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

-- jh 20200922 Modified the row_number() order by to remove the 'else snapshotDate' from the two case stmts and moved to third item in list
-- ak 20200729 Adding snapshotDate reference and determining termCode tied to the snapshotDate
-- ak 20200728 Move to after the ReportingPeriodMCR in order to bring in snapshot dates needed for reporting

select termCode, 
	partOfTermCode, 
	to_date(startDate, 'YYYY-MM-DD') startDate,
	to_date(endDate, 'YYYY-MM-DD') endDate,
	academicYear,
	to_date(censusDate, 'YYYY-MM-DD') censusDate,
    termType,
    termClassification,
	requiredFTCreditHoursGR,
	requiredFTCreditHoursUG,
	requiredFTClockHoursUG,
    to_date(snapshotDate, 'YYYY-MM-DD') snapshotDate--,    
    --tags
from ( 
    select distinct acadtermENT.termCode, 
        row_number() over (
            partition by 
                acadTermENT.termCode,
                acadTermENT.partOfTermCode
            order by
                (case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                            and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
                    then acadTermENT.termCode end) desc,
                (case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                            and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
                    then acadTermENT.partOfTermCode end) desc,
                acadTermENT.snapshotDate desc,
                acadTermENT.recordActivityDate desc
        ) acadTermRn,
        acadTermENT.snapshotDate,
        --acadTermENT.tags,
		acadtermENT.partOfTermCode, 
		acadtermENT.recordActivityDate, 
		acadtermENT.termCodeDescription,       
		acadtermENT.partOfTermCodeDescription, 
		acadtermENT.startDate,
		acadtermENT.endDate,
		acadtermENT.academicYear,
		acadtermENT.censusDate,
        acadtermENT.termType,
        acadtermENT.termClassification,
		acadtermENT.requiredFTCreditHoursGR,
	    acadtermENT.requiredFTCreditHoursUG,
	    acadtermENT.requiredFTClockHoursUG,
		acadtermENT.isIPEDSReportable
	from AcademicTerm acadtermENT 
	where acadtermENT.isIPEDSReportable = 1
	)
where acadTermRn = 1
),

AcademicTermOrder as (
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

-- jh 20200911 Added fullTermOrder field to narrow the student and person data to the first full term the student is registered in RegistrationMinTerm
-- ak 20200728 Added view to pull all academic term data for the terms associated with the reporting period

select repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        termorder.termOrder termOrder,
        acadterm.snapshotDate snapshotDate,
        (case when acadterm.termClassification = 'Standard Length' then 1
             when acadterm.termClassification is null then (case when acadterm.termType in ('Fall', 'Spring') then 1 else 2 end)
             else 2
        end) fullTermOrder,
        acadterm.termClassification termClassification,
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
		inner join AcademicTermOrder termorder
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
--We only use campus for international status. We are maintaining the ability to look at a campus at different points in time through relevant snapshots. will there ever be a case where a campus changes international status? Otherwise, we could just get all unique campus codes and forget about when the record was made.

-- jh 20200911 Removed ReportingPeriodMCR reference and changed filter date from regper.reportingperiodend to campusENT.snapshotDate 

select campus,
	isInternational,
	snapshotDate
from ( 
    select upper(campusENT.campus) campus,
		campusENT.campusDescription,
		campusENT.isInternational,
		to_date(campusENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
		row_number() over (
			partition by
			    campusENT.snapshotDate, 
				campusENT.campus
			order by
				campusENT.recordActivityDate desc
		) campusRn
	from Campus campusENT 
	where campusENT.isIpedsReportable = 1 
		and ((to_date(campusENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS TIMESTAMP)
			and to_date(campusENT.recordActivityDate,'YYYY-MM-DD') <= to_date(campusENT.snapshotDate,'YYYY-MM-DD'))
				or to_date(campusENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS TIMESTAMP))
	)
where campusRn = 1
),

RegistrationMCR as ( 
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 

select personId,
    snapshotDate,
    --tags,
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
        --regENT.tags tags,
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
            and regENT.registrationStatus in ('Registered', 'Web Registered')
			and regENT.isIpedsReportable = 1 
		left join CampusMCR campus on regENT.campus = campus.campus  
		    and campus.snapshotDate = regENT.snapshotDate
	)
where regRn = 1
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  

--jh 20200911 Added studentType field for new 2020-21 requirements: Unduplicated enrollment counts of undergraduate 
--     students are first-time (entering), transfer-in (non-first-time entering), continuing/returning, and degree/certificate-seeking statuses.
--jh 20200911 Added join to CampusMCR to determine the following status: (FAQ) Students who are enrolled in your institution and attend classes 
--     in a foreign country should NOT be included in your enrollment report if: The students are enrolled at a branch campus of your institution in a foreign country
--jh 20200911 Removed stuLevelCalc for the following change this year: (FAQ) How do I report a student who changes enrollment levels during the 12-month period? (4-year institutions only) 
--     The enrollment level should be determined at the first “full” term at entry. For example, a student enrolled as an undergraduate 
--     in the fall and then as a graduate student in the spring should be reported as an undergraduate student on the 12-month Enrollment survey component.

select personId,
    snapshotDate,
	termCode, 
    termOrder,
    fullTermOrder,
    startDate,
    isNonDegreeSeeking,
	studentLevel,
    studentType, 
    isInternational
from ( 
	select distinct studentENT.personId personId,
	    to_date(studentENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
		reg.termCode termCode, 
        reg.termOrder termOrder,
        reg.startDate startDate,
        reg.fullTermOrder fullTermOrder, --1 for 'full' (standard), 2 for non-standard
		studentENT.isNonDegreeSeeking isNonDegreeSeeking,
		studentENT.studentLevel studentLevel,
        studentENT.studentType studentType,
        campus.isInternational isInternational,
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
		left join CampusMCR campus on studentENT.campus = campus.campus  
		    and campus.snapshotDate = studentENT.snapshotDate
		    and campus.isInternational = false
	)
where studRn = 1
),

StudentRefactor as (
--Determine student info based on full term and degree-seeking status

/*
1. Student level:

use first full term to determine student level
if no term is a full (standard-length) term, use first term

2. Degree-seeking status for Undergrad:

if student's degree-seeking status is the same for all terms, use first full (standard-length) term to determine degree-seeking status
    if no term is a full (standard-length) term, use first term
if status changes, use first full (standard-length) term where student is degree-seeking
    if no term is a full (standard-length) term, use first term where student is degree-seeking

3. Student Type for degree-seeking status: 

student is ds for all terms - then use first full (standard-length) term to determine student type
    if no term is a full (standard-length) term, use first term
 ***still need to add: if student type is Continuing, check if a snapshot with tag 'Pre-Fall Summer Census' exists if student was enrolled, use that student type
    
if degree-seeking status changes from non-degree... to degree... then student type is Continuing

if a student's type is not indicated directly and the student does not enroll with prior credits or transcripts from another institution, 
    then assume the student is first-time
    
4. Attendance Status/Enrollment Level (Full-Time vs. Part-Time):

use first full term to determine attendence status
if no term is a full (standard-length) term, use first term
*/

-- jh 20200922 Fixed string spelling for student type Undergrad and changed Continuing to Returning
-- jh 20200911 Created view for 20-21 requirements

select stu.personId,
        stu.isNonDegreeSeeking,
        stu.studentType,
        stu.firstFullTerm,
        stu.studentLevel,
        reg.snapshotDate,
        reg.censusDate,
        reg.termOrder,
        reg.instructionalActivityType,
        reg.requiredFTCreditHoursGR,
	    reg.requiredFTCreditHoursUG,
	    reg.requiredFTClockHoursUG
from ( 
select distinct personId,
    min(isNonDegreeSeeking) isNonDegreeSeeking,
    max(studentType) studentType,
    max(firstFullTerm) firstFullTerm,
    max(studentLevel) studentLevel
from (
    select distinct personId,
        termCode,
        FFTRn,
        NDSRn,
        (case when isNonDegreeSeeking = true then 1 else 0 end) isNonDegreeSeeking, 
        (case when isNonDegreeSeeking = false then
            (case when studentLevel != 'Undergrad' then null
                when NDSRn = 1 and FFTRn = 1 then studentType
                when NDSRn = 1 then 'Returning'
            end)
            else null
        end) studentType,
        (case when FFTRn = 1 then studentLevel else null end) studentLevel,
        (case when FFTRn = 1 then termCode else null end) firstFullTerm
    from (
        select personId,
            snapshotDate,
            termCode, 
            termOrder,
            fullTermOrder,
            isNonDegreeSeeking,
            studentLevel,
            studentType, 
            isInternational,
            row_number() over (
                        partition by
                            personId
                    order by isNonDegreeSeeking asc,
                            fullTermOrder asc, --all standard length terms first
                            termOrder asc, --order by term to find first standard length term
                            startDate asc --get record for term with earliest start date (consideration for parts of term only)
                    ) NDSRn,
               row_number() over (
                        partition by
                            personId
                    order by fullTermOrder asc, --all standard length terms first
                            termOrder asc, --order by term to find first standard length term
                            startDate asc --get record for term with earliest start date (consideration for parts of term only)
                    ) FFTRn
              from StudentMCR stu
        )
    )
group by personId
) stu
--join RegistrationMCR to get termOrder, censusDate and snapshotDate of firstFullTerm
    inner join (select personId personId,
                        termCode termCode,
                        termOrder termOrder,
                        censusDate censusDate,
                        snapshotDate snapshotDate,
                        instructionalActivityType instructionalActivityType,
                        requiredFTCreditHoursGR requiredFTCreditHoursGR,
	                    requiredFTCreditHoursUG requiredFTCreditHoursUG,
	                    requiredFTClockHoursUG requiredFTClockHoursUG
                from (
                    select personId personId,
                        termCode termCode,
                        termOrder termOrder,
                        censusDate censusDate,
                        snapshotDate snapshotDate,
                        instructionalActivityType instructionalActivityType,
                        requiredFTCreditHoursGR requiredFTCreditHoursGR,
	                    requiredFTCreditHoursUG requiredFTCreditHoursUG,
	                    requiredFTClockHoursUG requiredFTClockHoursUG,
                        row_number() over (
			                partition by
				                personId,
                                termOrder
			                order by 
			                    termOrder asc, --order by term to find first standard length term
			                    startDate asc --get record for term with earliest start date (consideration for parts of term only)
	                    ) regRn
	                from RegistrationMCR
	                )
                where regRn = 1) reg on stu.personId = reg.personId
                    and stu.firstFullTerm = reg.termCode 
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

--jh 20200911 Modified to use the first full term within the full year period
--ak 20200406 Including dummy date changes. (PF-1368)

select *
from (
    select distinct personENT.personId personId,
            sturef.censusDate censusDate,
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
    from StudentRefactor sturef
        inner join Person personENT on sturef.personId = personENT.personId
            and to_date(personENT.snapshotDate,'YYYY-MM-DD') = sturef.snapshotDate
            and personENT.isIpedsReportable = 1
            and ((to_date(personENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
               and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= sturef.censusDate) 
                or to_date(personENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
    )
where personRn = 1
),

CourseSectionMCR as (
--Included to get enrollment hours of a CRN

-- ak 20200713 Added course section status filter (PF-1553)
    
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

-- ak 20200707 Adding additional client config values for level offerings and instructor activity type (PF-1552)
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
--ak 20200406 Including dummy date changes. (PF-1368)

select *
from (
	select coursesect2.snapshotDate snapshotDate,
		coursesect2.censusDate censusDate,
		coursesect2.crn crn,
		coursesect2.section section,
		coursesect2.termCode termCode,

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
	                and ((SchedRec.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
	            	    and SchedRec.recordActivityDate <= coursesect2.censusDate) --use same censusDate as CourseSectionMCR
			                or SchedRec.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where courseSectSchedRn = 1
),

CourseMCR as (
--Included to get course type information

-- ak 20200707 Adding additional client config values for level offerings and instructor activity type (PF-1552)
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
--jh 20200422 removed ReportingPeriod and CourseSectionMCR joins and added section field
--ak 20200406 Including dummy date changes. (PF-1368)

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
				inner join AcademicTermOrder termorder on termorder.termCode = courseENT.termCodeEffective
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

CourseTypeCountsSTU as (
-- View used to break down course category type counts by type

--jh 20200707 Removed termCode and partOfTermCode from grouping, since not in select fields

select reg.personId personId,
	sum((case when course.enrollmentHours >= 0 then 1 else 0 end)) totalCourses,
	sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 then course.enrollmentHours else 0 end)) totalCreditHrs,
	sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then nvl(course.enrollmentHours, 0) else 0 end)) totalCreditUGHrs,
	sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel in ('Graduate', 'Professional') then nvl(course.enrollmentHours, 0) else 0 end)) totalCreditGRHrs,
	sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel = 'Postgraduate' then nvl(course.enrollmentHours, 0) else 0 end)) totalCreditPostGRHrs,
	sum((case when course.isClockHours = 1 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then course.enrollmentHours else 0 end)) totalClockHrs,
	sum((case when course.enrollmentHours = 0 then 1 else 0 end)) totalNonCredCourses,
	sum((case when course.enrollmentHours > 0 then 1 else 0 end)) totalCredCourses,
	sum((case when course.meetingType = 'Online/Distance Learning' then 1 else 0 end)) totalDECourses,
	sum((case when course.courseLevel = 'Undergrad' then 1 else 0 end)) totalUGCourses,
	sum((case when course.courseLevel in ('Graduate', 'Professional') then 1 else 0 end)) totalGRCourses,
	sum((case when course.courseLevel = 'Postgraduate' then 1 else 0 end)) totalPostGRCourses,
	sum((case when course.courseLevel = 'Continuing Ed' then 1 else 0 end)) totalCECourses,
	sum((case when course.isESL = 'Y' then 1 else 0 end)) totalESLCourses,
	sum((case when course.isRemedial = 'Y' then 1 else 0 end)) totalRemCourses,
	sum((case when reg.isInternational = 1 then 1 else 0 end)) totalIntlCourses, 
	sum((case when reg.crnGradingMode = 'Audit' then 1 else 0 end)) totalAuditCourses
from RegistrationMCR reg
	left join CourseMCR course on reg.termCode = course.termCode
        and reg.partOfTermCode = course.partOfTermCode
		and reg.crn = course.crn
		and reg.crnLevel = course.courseLevel 
		and reg.snapshotDate = course.snapshotDate
group by personId
),

StuLevel as (
-- View used to break down course category type counts by type for students enrolled for at least one credit/clock hour

--jh 20200910 Created StuLevel view to determine new 12Mo requirements for Part A and Part C

--Student level table (Part A)
--1 - Full-time, first-time degree/certificate-seeking undergraduate
--2 - Full-time, transfer-in degree/certificate-seeking undergraduate
--3 - Full-time, continuing degree/certificate-seeking undergraduate
--7 - Full-time, non-degree/certificate-seeking undergraduate
--15 - Part-time, first-time degree/certificate-seeking undergraduate
--16 - Part-time, transfer-in degree/certificate-seeking undergraduate
--17 - Part-time, continuing degree/certificate-seeking undergraduate
--21 - Part-time, non-degree/certificate-seeking undergraduate
--99 - Total graduate

--Student level table (Part C)
--1 - Degree/Certificate seeking undergraduate students
--2 - Non-Degree/Certificate seeking undergraduate Students
--3 - Graduate students

select personId,
    (case when studentLevel = 'Graduate' then '99' 
         when isNonDegreeSeeking = true and timeStatus = 'FT' then '7'
         when isNonDegreeSeeking = true and timeStatus = 'PT' then '21'
         when studentLevel = 'Undergrad' then 
            (case when studentType = 'First Time' and timeStatus = 'FT' then '1' 
                    when studentType = 'Transfer' and timeStatus = 'FT' then '2'
                    when studentType = 'Returning' and timeStatus = 'FT' then '3'
                    when studentType = 'First Time' and timeStatus = 'PT' then '15' 
                    when studentType = 'Transfer' and timeStatus = 'PT' then '16'
                    when studentType = 'Returning' and timeStatus = 'PT' then '17' else '1' end)
        else null
    end) studentLevelPartA, --only Graduate and Undergrad (and Continuing Ed, who go in Undergrad) students are counted in headcount
    (case when studentLevel = 'Graduate' then '3' 
         when isNonDegreeSeeking = true then '2'
         when studentLevel = 'Undergrad' then '1'
         else null
    end) studentLevelPartC ----only Graduate and Undergrad (and Continuing Ed, who go in Undergrad) students are counted in headcount
from (
    select personId personId,
            (case when studentLevel = 'Undergrad' and instructionalActivityType in ('CR', 'B') then 
                            (case when totalCreditHrsCalc >= requiredFTCreditHoursUG then 'FT' else 'PT' end)
                  when studentLevel = 'Undergrad' and instructionalActivityType = 'CL' then 
                            (case when totalClockHrs >= requiredFTClockHoursUG then 'FT' else 'PT' end) 
            else null end) timeStatus,
            studentLevel studentLevel,
            studentType studentType,
            isNonDegreeSeeking isNonDegreeSeeking
    from ( 
        select distinct stu.personId personId,
                stu.requiredFTCreditHoursGR,
                stu.requiredFTCreditHoursUG,
                stu.requiredFTClockHoursUG,
                stu.studentLevel studentLevel,
                stu.studentType studentType,
                stu.isNonDegreeSeeking isNonDegreeSeeking,
                stu.instructionalActivityType instructionalActivityType,
                sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 then course.enrollmentHours else 0 end)) totalCreditHrs,
                sum((case when course.isClockHours = 1 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then course.enrollmentHours else 0 end)) totalClockHrs,
                sum((case when stu.instructionalActivityType = 'CR' and course.enrollmentHours >= 0 then course.enrollmentHours 
                        when stu.instructionalActivityType = 'B' and course.isClockHours = 0 then course.enrollmentHours 
                        when stu.instructionalActivityType = 'B' and course.isClockHours = 1 then course.equivCRHRFactor * course.enrollmentHours
                        else 0 
                    end)) totalCreditHrsCalc
        from RegistrationMCR reg
            inner join StudentRefactor stu on reg.personId = stu.personId
                and reg.termCode = stu.firstFullTerm
            left join CourseMCR course on reg.termCode = course.termCode
                and reg.partOfTermCode = course.partOfTermCode
                and reg.crn = course.crn
                and reg.crnLevel = course.courseLevel 
                and reg.snapshotDate = course.snapshotDate
        group by stu.personId,
                stu.requiredFTCreditHoursGR,
                stu.requiredFTCreditHoursUG,
                stu.requiredFTClockHoursUG,
                stu.studentLevel,
                stu.studentType,
                stu.isNonDegreeSeeking,
                stu.instructionalActivityType
        )
--filter out any student not enrolled for a credit course
    where totalCreditHrsCalc > 0 
        or totalClockHrs > 0
    )
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

CohortSTU as (
--View used to get one record per student and studentLevel
--Need all instructional activity for Undergrad, Graduate, Postgraduate/Professional levels
--Student headcount only for Undergrad and Graduate

select distinct reg.personID personID,  
	reg.censusDate censusDate, 
	reg.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
	reg.instructionalActivityType instructionalActivityType,
	reg.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
	reg.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
	reg.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
	stu.studentLevelPartA ipedsPartAStudentLevel, --null for students not counted in headcount
	stu.studentLevelPartC ipedsPartCStudentLevel, --null for students not counted in headcount
	(case when person.gender = 'Male' then 'M'
			when person.gender = 'Female' then 'F' 
			when person.gender = 'Non-Binary' then reg.genderForNonBinary
		else reg.genderForUnknown
	end) ipedsGender,
	(case when person.isUSCitizen = 1 then 
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
	end) ipedsEthnicity,  
	coalesce(coursecnt.totalCreditHrs, 0) totalCreditHrs,
	coalesce(coursecnt.totalClockHrs, 0) totalClockUGHrs,
	coalesce(coursecnt.totalCreditUGHrs, 0) totalCreditUGHrs,
	coalesce(coursecnt.totalCreditGRHrs, 0) totalCreditGRHrs,
	coalesce(coursecnt.totalCreditPostGRHrs, 0) totalCreditPostGRHrs,
	(case when coursecnt.totalCourses = courseCnt.totalDECourses then 'DE Exclusively'
		when courseCnt.totalDECourses > 0 then 'DE Some'
		else 'DE None'
	end) DEStatus
from RegistrationMCR reg  
	left join PersonMCR person on reg.personId = person.personId 
	left join CourseTypeCountsSTU coursecnt on reg.personId = coursecnt.personId  
	left join StuLevel stu on reg.personId = stu.personId
),

InstructionHours as (
--Sums instructional hours and filters for aggregation

--jh 20200910 Created InstructionHours view for Part B

select (case when icOfferUndergradAwardLevel = 'Y' and instructionalActivityType != 'CL' then coalesce(UGCredit, 0) 
            else null 
        end) field2, -- CREDHRSU - credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.
       (case when icOfferUndergradAwardLevel = 'Y' and instructionalActivityType != 'CR' then coalesce(UGClock, 0) 
            else null 
        end) field3, -- CONTHRS  - clock hour instructional activity at the undergraduate level, 0 to 9999999, blank = not applicable, if no undergraduate programs are measured in clock hours.
       (case when icOfferGraduateAwardLevel = 'N' then null
            else coalesce(GRCredit, 0)
       end) field4, -- CREDHRSG - credit hour instructional activity at the graduate level, 0 to 99999999, blank = not applicable
       (case when icOfferDoctorAwardLevel = 'N' then null
            when coalesce(PostGRCredit, 0) > 0 then cast(round(PostGRCredit / tmAnnualDPPCreditHoursFTE, 0) as string)
         else '0' 
       end) field5
from ( 
    select sum(totalCreditUGHrs) UGCredit,
           sum(totalClockUGHrs) UGClock,
           sum(totalCreditGRHrs) GRCredit,
           sum(totalCreditPostGRHrs) PostGRCredit,
           icOfferUndergradAwardLevel,
           icOfferGraduateAwardLevel,
           icOfferDoctorAwardLevel,
           instructionalActivityType,
           tmAnnualDPPCreditHoursFTE
    from CohortSTU
    group by icOfferUndergradAwardLevel,
            icOfferGraduateAwardLevel,
            icOfferDoctorAwardLevel,
            instructionalActivityType,
            tmAnnualDPPCreditHoursFTE
    )
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

-- Part A: Unduplicated Count by Student Level, Gender, and Race/Ethnicity

-- ipedsStudentLevelPartA valid values - Student level table (Part A)
--1 - Full-time, first-time degree/certificate-seeking undergraduate
--2 - Full-time, transfer-in degree/certificate-seeking undergraduate
--3 - Full-time, continuing degree/certificate-seeking undergraduate
--7 - Full-time, non-degree/certificate-seeking undergraduate
--15 - Part-time, first-time degree/certificate-seeking undergraduate
--16 - Part-time, transfer-in degree/certificate-seeking undergraduate
--17 - Part-time, continuing degree/certificate-seeking undergraduate
--21 - Part-time, non-degree/certificate-seeking undergraduate
--99 - Total Graduate

select 'A'                                                                                      part,
       ipedsPartAStudentLevel                                                                   field1,  -- SLEVEL   - valid values are 1 for undergraduate and 3 for graduate
       coalesce(sum((case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end)), 0) field2,  -- FYRACE01 - Nonresident alien - Men (1), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end)), 0) field3,  -- FYRACE02 - Nonresident alien - Women (2), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end)), 0) field4,  -- FYRACE25 - Hispanic/Latino - Men (25), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end)), 0) field5,  -- FYRACE26 - Hispanic/Latino - Women (26), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end)), 0) field6,  -- FYRACE27 - American Indian or Alaska Native - Men (27), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end)), 0) field7,  -- FYRACE28 - American Indian or Alaska Native - Women (28), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end)), 0) field8,  -- FYRACE29 - Asian - Men (29), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end)), 0) field9,  -- FYRACE30 - Asian - Women (30), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end)), 0) field10, -- FYRACE31 - Black or African American - Men (31), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end)), 0) field11, -- FYRACE32 - Black or African American - Women (32), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end)), 0) field12, -- FYRACE33 - Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end)), 0) field13, -- FYRACE34 - Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end)), 0) field14, -- FYRACE35 - White - Men (35), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end)), 0) field15, -- FYRACE36 - White - Women (36), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end)), 0) field16, -- FYRACE37 - Two or more races - Men (37), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end)), 0) field17, -- FYRACE38 - Two or more races - Women (38), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end)), 0) field18, -- FYRACE13 - Race and ethnicity unknown - Men (13), 0 to 999999
       coalesce(sum((case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end)), 0) field19  -- FYRACE14 - Race and ethnicity unknown - Women (14), 0 to 999999
from CohortSTU
where ipedsPartAStudentLevel is not null --only Undergrad and Graduate students
group by ipedsPartAStudentLevel 

union

-- Part C: 12-month Unduplicated Count - Distance Education Status

-- ipedsPartCStudentLevel valid values - Student level table (Part C)
--1 - Degree/Certificate seeking undergraduate students
--2 - Non-Degree/Certificate seeking undergraduate Students
--3 - Graduate students

select 'C' part,
       ipedsPartCStudentLevel field1,
       sum(coalesce(case when DEStatus = 'DE Exclusively' then 1 else 0 end, 0)) field2,  --Enrolled exclusively in distance education courses
       sum(coalesce(case when DEStatus = 'DE Some' then 1 else 0 end, 0)) field3,  --Enrolled in at least one but not all distance education courses
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
from CohortSTU
where ipedsPartCStudentLevel is not null --only Undergrad and Graduate students
and DEStatus != 'DE None'
group by ipedsPartCStudentLevel

union

-- Part B: Instructional Activity

select 'B' part,
       null field1,
       field2 field2, -- CREDHRSU - credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.
       field3 field3, -- CONTHRS  - clock hour instructional activity at the undergraduate level, 0 to 9999999, blank = not applicable, if no undergraduate programs are measured in clock hours.
       field4 field4, -- CREDHRSG - credit hour instructional activity at the graduate level, 0 to 99999999, blank = not applicable
       field5 field5, -- RDOCFTE  - reported Doctor'92s degree-professional practice student FTE, 0 to 99999999, blank = not applicable
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
from InstructionHours 
--where exists (select a.personId from CohortSTU a) 

union 

-- jh 20200911 Added lines for Parts A and C	
-- ak 20200825 Dummy set to return default formatting if no cohortSTU records exist.

	select *
	from (
		VALUES
			('A', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
			('C', 1, 0, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
			('B', null, 0, 0, 0, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
		) as dummySet(part, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11,
									field12, field13, field14, field15, field16, field17, field18, field19)
	where not exists (select a.personId from CohortSTU a) 
