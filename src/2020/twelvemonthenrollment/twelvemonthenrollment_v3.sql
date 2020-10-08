/********************

EVI PRODUCT:	DORIS 2020-21 IPEDS Survey  
FILE NAME: 		12 Month Enrollment v3 (E1E)
FILE DESC:      12 Month Enrollment for public 2 year and less than 2-year non-degree-granting institutions
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
20201008			jhanicak									Uncommented the graduate level lines except in StuLevel
																and survey formatting section PF-1706 Run time prod 13m 26s
20201007            jhanicak                jh 20201007         Multiple fixes PF-1698
                                                                Updated enrollment requirements PF-1681 Run time 10m 12s, Test data 16m 7s			   
20200922            jhanicak                jh 20200917         Commented out all references to tags field, fixed row_number() in AcademicTermReporting,
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

--mod from v1 - Default values for icOfferGraduateAwardLevel and icOfferDoctorAwardLevel indicators are 'N'
--mod from v1 - Part A doesn't contain graduate student level value 99
--mod from v1 - Part C doesn't contain graduate student level value 3
--mod from v2 - Part A doesn't contain transfer student level values 2, 16

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
	'E1E' surveyId,  
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

/*
select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No
 
union 

select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No
 
union 

select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No
 
union 

select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No

union

select '1415' surveyYear,  
	'E1E' surveyId,   
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
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No
    'N' icOfferDoctorAwardLevel --Y = Yes, N = No
*/
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

-- jh 20201007 Moved snapshot and tag fields from row function order by to partition by in order to return all snapshots
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
			    clientConfigENT.snapshotDate,			
				clientConfigENT.surveyCollectionYear
			order by
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

ConfigIndicators as (
--Return one set of IPEDSClientConfig indicators

-- jh 20201007 Added view for quick access to indicators

select max(config.termCode) maxTerm,
        max(config.partOfTermCode) partOfTermCode,
        config.genderForUnknown genderForUnknown,
		config.genderForNonBinary genderForNonBinary,
        config.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
        config.instructionalActivityType instructionalActivityType,
        config.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		config.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        config.icOfferDoctorAwardLevel icOfferDoctorAwardLevel
from ClientConfigMCR config
group by config.genderForUnknown,
		config.genderForNonBinary,
        config.tmAnnualDPPCreditHoursFTE,
        config.instructionalActivityType,
        config.icOfferUndergradAwardLevel,
		config.icOfferGraduateAwardLevel,
        config.icOfferDoctorAwardLevel
),										 

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 

-- jh 20201007 Removed indicators from ClientConfigMCR - no need to pull thru for each record
-- jh 20200911 Removed filter in first union for tag values and added a case stmt to the row_number() function to handle no snapshots that match tags
--          1st union 1st order - pull snapshot for 'Full Year Term End' 
--          1st union 2nd order - pull snapshot for 'Full Year June End'
--          1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--          2nd union - pull default values if no record in IPEDSReportingPeriod
-- ak 20200729 Adding snapshotDate reference

select distinct RepDates.surveyYear	surveyYear,
    RepDates.source source,
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
    RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,
    to_date(RepDates.censusDate,'YYYY-MM-DD') censusDate,
	to_date(RepDates.reportingDateStart,'YYYY-MM-DD') reportingDateStart,
    to_date(RepDates.reportingDateEnd,'YYYY-MM-DD') reportingDateEnd
from (
    select repperiodENT.surveyCollectionYear surveyYear,
	    'repperiodSnapshotMatch' source,
		repperiodENT.snapshotDate snapshotDate,
		repPeriodENT.surveyId surveyId, 
		coalesce(repperiodENT.reportingDateStart, clientconfig.reportingDateStart) reportingDateStart,
		coalesce(repperiodENT.reportingDateEnd, clientconfig.reportingDateEnd) reportingDateEnd,
		coalesce(repperiodENT.termCode, clientconfig.termCode) termCode,
		coalesce(repperiodENT.partOfTermCode, clientconfig.partOfTermCode) partOfTermCode,
		clientconfig.censusDate censusDate,
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
		clientconfig.surveyId surveyId, 
		clientconfig.reportingDateStart reportingDateStart,
		clientconfig.reportingDateEnd reportingDateEnd,
		clientconfig.termCode termCode,
		clientconfig.partOfTermCode partOfTermCode, 
		clientconfig.censusDate censusDate,
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

-- jh 20201007 Included tags field to pull thru for 'Pre-Fall Summer Census' check
--				Moved check on termCode censusDate and snapshotDate to view AcademicTermReporting
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
    to_date(snapshotDate, 'YYYY-MM-DD') snapshotDate,    
    tags
from ( 
    select distinct acadtermENT.termCode, 
        row_number() over (
            partition by 
                acadTermENT.snapshotDate,
                acadTermENT.termCode,
                acadTermENT.partOfTermCode
            order by
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

-- jh 20201007 Included fields for maxCensus date and termType
-- ak 20200616 View created to determine max term order by term code (PF-1494)

select termCode termCode, 
    max(termOrder) termOrder,
    max(censusDate) maxCensus,
    termType termType
from (
	select acadterm.termCode termCode,
	    acadterm.partOfTermCode partOfTermCode,
	    acadterm.termType termType,
	    acadterm.censusDate censusDate,
		row_number() over (
			order by  
				acadterm.startDate asc,
				acadterm.endDate asc
        ) termOrder
	from AcademicTermMCR acadterm
	) 
group by termCode, termType
),

AcademicTermReporting as (
--Combines ReportingPeriodMCR and AcademicTermMCR in order to use the correct snapshot dates for the reporting terms

-- jh 20201007 Reworked view; changed left join to AcademicTermOrder to a left join
-- jh 20200911 Added fullTermOrder field to narrow the student and person data to the first full term the student is registered in RegistrationMinTerm
-- ak 20200728 Added view to pull all academic term data for the terms associated with the reporting period

select repPerTerms.termCode termCode,
        repPerTerms.partOfTermCode partOfTermCode,
        repPerTerms.termOrder termOrder,
        repPerTerms.maxCensus maxCensus,
        repPerTerms.caseSnapshotDate snapshotDate,
        repPerTerms.termClassification termClassification,
        repPerTerms.termType termType,
        repPerTerms.startDate startDate,
        repPerTerms.endDate endDate,
        repPerTerms.censusDate censusDate,
        repPerTerms.requiredFTCreditHoursGR,
	    repPerTerms.requiredFTCreditHoursUG,
	    repPerTerms.requiredFTClockHoursUG,
	    repPerTerms.equivCRHRFactor equivCRHRFactor,
        (case when repPerTerms.termClassification = 'Standard Length' then 1
             when repPerTerms.termClassification is null then (case when repPerTerms.termType in ('Fall', 'Spring') then 1 else 2 end)
             else 2
        end) fullTermOrder
from (
select distinct repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        termorder.termOrder termOrder,
        termorder.maxCensus maxCensus,
        (case when to_date(acadterm.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and to_date(acadterm.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') 
                    then acadterm.snapshotDate else repperiod.snapshotDate end) caseSnapshotDate,
		acadterm.termClassification termClassification,
        acadterm.termType termType,
        acadterm.startDate startDate,
        acadterm.endDate endDate,
        coalesce(acadterm.censusDate, repperiod.censusDate) censusDate,
        acadterm.requiredFTCreditHoursGR,
	    acadterm.requiredFTCreditHoursUG,
	    acadterm.requiredFTClockHoursUG,
	    coalesce(acadterm.requiredFTCreditHoursUG/
		    coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
		row_number() over (
            partition by 
                repperiod.termCode,
                repperiod.partOfTermCode
            order by
                (case when to_date(acadterm.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and to_date(acadterm.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') 
                    then acadterm.snapshotDate else repperiod.snapshotDate end) asc
            ) acadTermRn
    from ReportingPeriodMCR repperiod 
        left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
	            and repperiod.partOfTermCode = acadterm.partOfTermCode
		left join AcademicTermOrder termorder
			on termOrder.termCode = repperiod.termCode
		) repPerTerms
where repPerTerms.acadTermRn = 1
),

AcademicTermReportingRefactor as (
--Returns all records from AcademicTermReporting, converts Summer terms to Pre-Fall or Post-Spring and creates reportingDateStart/End

-- jh 20201007 Added to return new Summer termTypes and reportingDateStart/End

select rep.*,
        (case when rep.termType = 'Summer' then 
                    case when (select max(rep2.termOrder)
                    from AcademicTermReporting rep2
                    where rep2.termType = 'Summer') < (select max(rep2.termOrder)
                                                        from AcademicTermReporting rep2
                                                        where rep2.termType = 'Fall') then 'Pre-Fall Summer'
                    else 'Post-Spring Summer' end
                else rep.termType end) termTypeNew,
        (select min(rep2.startDate)
            from AcademicTermReporting rep2) reportingDateStart,
        (select max(rep2.endDate)
            from AcademicTermReporting rep2) reportingDateEnd,
        potMax.partOfTermCode maxPOT
from AcademicTermReporting rep
    inner join (select rep3.termCode,
                        rep3.partOfTermCode,
                       row_number() over (
			                partition by
			                    rep3.termCode--, 
				                --campusENT.campus
			                order by
				                rep3.censusDate desc,
				                rep3.endDate desc
		                ) potRn
                from AcademicTermReporting rep3) potMax on rep.termCode = potMax.termCode
                        and potMax.potRn = 1
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusMCR as ( 
-- Returns most recent campus record for all snapshots in the ReportingPeriod
-- We only use campus for international status. We are maintaining the ability to look at a campus at different points in time through relevant snapshots. 
-- Will there ever be a case where a campus changes international status? 
-- Otherwise, we could just get all unique campus codes and forget about when the record was made.					   

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

-- jh 20201007 Removed config indicators; pulled termTypeNew from AcademicTermReportingRefactor

select personId,
    snapshotDate,
	termCode,
	partOfTermCode, 
	termorder,
	maxCensus,
	fullTermOrder,
	termType,
	censusDate,
	startDate,
	requiredFTCreditHoursGR,
	requiredFTCreditHoursUG,
	requiredFTClockHoursUG,
	equivCRHRFactor,
	isInternational,    
	crnGradingMode,                    
	crn,
	crnLevel
from ( 
    select regENT.personId personId,
        to_date(regENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		regENT.termCode termCode,
		regENT.partOfTermCode partOfTermCode, 
		repperiod.termorder termorder,
		repperiod.maxCensus maxCensus,
		repperiod.fullTermOrder fullTermOrder,
		repperiod.termTypeNew termType,
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
		row_number() over (
			partition by
				regENT.personId,
				regENT.termCode,
				regENT.partOfTermCode,
				regENT.crn,
				regENT.crnLevel
			order by regENT.recordActivityDate desc
		) regRn
	from AcademicTermReportingRefactor repperiod   
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

	-- jh 20201007 Pulled thru the personId and snapshotDate from RegistrationMCR instead of Student;
--				Included more conditions for isNonDegreeSeeking status															
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
    maxCensus,
    termType,
    fullTermOrder,
    startDate,
    coalesce((case when studentType = 'High School' then true
          when studentLevel = 'Continuing Ed' then true
          else isNonDegreeSeeking end), false) isNonDegreeSeeking,
	studentLevel,
    studentType
from ( 
	select distinct reg.personId personId,
	    reg.snapshotDate snapshotDate,
		reg.termCode termCode, 
        reg.termOrder termOrder,
        reg.maxCensus maxCensus,
        reg.termType termType,
        reg.startDate startDate,
        reg.fullTermOrder fullTermOrder, --1 for 'full' (standard), 2 for non-standard
		studentENT.isNonDegreeSeeking isNonDegreeSeeking,
		studentENT.studentLevel studentLevel,
        studentENT.studentType studentType,
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
		    and campus.snapshotDate = reg.snapshotDate
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

-- jh 20201007 Added Pre-Fall Summer Census check for Continuing Fall Census students;
--				Changed join for term info from RegistrationMCR to AcademicTermReportingRefactor
-- jh 20200922 Fixed string spelling for student type Undergrad and changed Continuing to Returning
-- jh 20200911 Created view for 20-21 requirements

select stu2.personId,
        stu2.isNonDegreeSeeking,
        stu2.studentType,
        stu2.firstFullTerm,
        stu2.studentLevel,
        acadTermCode.snapshotDate,
        acadTermCode.censusDate censusDate,
        acadTermCode.termOrder,
        acadTermCode.requiredFTCreditHoursGR,
	    acadTermCode.requiredFTCreditHoursUG,
	    acadTermCode.requiredFTClockHoursUG
from (
    select stu.personId,
            stu.isNonDegreeSeeking,
            (case when stu.studentLevel = 'Undergrad' then 
                (case when stu.studentTypeTermType = 'Fall' and stu.studentType = 'Returning' and stu.preFallStudType is not null then stu.preFallStudType
                    else stu.studentType end)
                else stu.studentType end) studentType,
            stu.firstFullTerm,
            stu.studentLevel
    from (
        select distinct personId,
            min(isNonDegreeSeeking) isNonDegreeSeeking,
            max(studentType) studentType,
            max(firstFullTerm) firstFullTerm,
            max(studentLevel) studentLevel,
            max(studentTypeTermType) studentTypeTermType,
            max(preFallStudType) preFallStudType
        from (
            select distinct personId,
                termCode,
                FFTRn,
                NDSRn,
                termType,
                (case when isNonDegreeSeeking = true then 1 else 0 end) isNonDegreeSeeking, 
                (case when isNonDegreeSeeking = false then
                    (case when studentLevel != 'Undergrad' then null
                        when NDSRn = 1 and FFTRn = 1 then studentType
                        when NDSRn = 1 then 'Returning'
                    end)
                    else null
                end) studentType,
                (case when isNonDegreeSeeking = false then
                    (case when studentLevel != 'Undergrad' then null
                        when NDSRn = 1 and FFTRn = 1 then termType
                        when NDSRn = 1 then termType
                    end)
                    else null
                end) studentTypeTermType,
                (case when termType = 'Pre-Fall Summer' then studentType else null end) preFallStudType,
                (case when FFTRn = 1 then studentLevel else null end) studentLevel,
                (case when FFTRn = 1 then termCode else null end) firstFullTerm
            from (
                select personId,
                    snapshotDate,
                    termCode, 
                    termOrder,
                    termType,
                    fullTermOrder,
                    isNonDegreeSeeking,
                    studentLevel,
                    studentType,
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
    ) stu2
    inner join AcademicTermReportingRefactor acadTermCode on acadTermCode.termCode = stu2.firstFullTerm
        and acadTermCode.partOfTermCode = acadTermCode.maxPOT
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

-- jh 20201007 Pulled thru the snapshotDate from RegistrationMCR instead of CourseSection;
--				Included isInternational field and removed instructionalActivityType
-- ak 20200713 Added course section status filter (PF-1553)
    
select *
from (
    select reg2.snapshotDate snapshotDate,
        CourseSect.snapshotDate courseSectDate,
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
        reg2.isInternational,
        CourseSect.isClockHours,
        reg2.crnGradingMode,
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

-- jh 20201007 Modified meetingType default from 'Standard' to 'Classroom/On Campus';
--				Added crnLevel to row_number partition by;
--				Changed inner query join from left to inner;
--				Included isInternational field and removed instructionalActivityType
-- ak 20200707 Adding additional client config values for level offerings and instructor activity type (PF-1552)
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
--ak 20200406 Including dummy date changes. (PF-1368)

select *
from (
	select coursesect2.snapshotDate snapshotDate, 
	    coursesect2.courseSectDate courseSectDate,
	    SchedRec.snapshotDate courseSectSchedDate,
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
		coalesce(SchedRec.meetingType, 'Classroom/On Campus') meetingType,
		coursesect2.enrollmentHours enrollmentHours,
		coursesect2.isClockHours isClockHours,
        coursesect2.crnGradingMode crnGradingMode,
        coursesect2.isInternational isInternational,
		row_number() over (
			partition by
			    coursesect2.termCode, 
				coursesect2.partOfTermCode,
				coursesect2.crnLevel,
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
				    to_date(courseSectSchedENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
				    coalesce(coursesectschedENT.meetingType, 'Classroom/On Campus') meetingType,
				    to_date(coursesectschedENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate
			    from CourseSectionMCR coursesect
		            inner join CourseSectionSchedule coursesectschedENT ON coursesectschedENT.termCode = coursesect.termCode
			            and coursesectschedENT.partOfTermCode = coursesect.partOfTermCode
			            and to_date(courseSectSchedENT.snapshotDate,'YYYY-MM-DD') = coursesect.snapshotDate
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

-- jh 20201007 Added default values to isRemedial and isESL;
--				Added crnLevel to row_number partition by;
--				Included isInternational field and removed instructionalActivityType
-- ak 20200707 Adding additional client config values for level offerings and instructor activity type (PF-1552)
-- ak 20200611 Adding changes for termCode ordering. (PF-1494)
-- jh 20200422 removed ReportingPeriod and CourseSectionMCR joins and added section field
-- ak 20200406 Including dummy date changes. (PF-1368)

select *
from (
	select coursesectsched2.snapshotDate snapshotDate, 
	    coursesectsched2.courseSectDate courseSectDate,
	    coursesectsched2.courseSectSchedDate courseSectSchedDate,
	    CourseRec.snapshotDate courseDate,
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
		coalesce(CourseRec.isRemedial, false) isRemedial,
		coalesce(CourseRec.isESL, false) isESL,
		coursesectsched2.meetingType meetingType,
		coalesce(coursesectsched2.enrollmentHours, 0) enrollmentHours,
		coursesectsched2.isClockHours isClockHours,
        coursesectsched2.equivCRHRFactor equivCRHRFactor,
        coursesectsched2.crnGradingMode crnGradingMode,
        coursesectsched2.isInternational isInternational,
		row_number() over (
			partition by
			    coursesectsched2.termCode,
				coursesectsched2.partOfTermCode,
			    coursesectsched2.subject,
				coursesectsched2.courseNumber,
				coursesectsched2.crnLevel,
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
                to_date(courseENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
				max(termorder.termOrder) termOrder,
				courseENT.courseLevel courseLevel,
				to_date(courseENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
				courseENT.isRemedial isRemedial,
		        courseENT.isESL isESL
	        from CourseSectionScheduleMCR coursesectsched
		        inner join Course courseENT on upper(courseENT.subject) = coursesectsched.subject
			        and upper(courseENT.courseNumber) = coursesectsched.courseNumber
                    and to_date(courseENT.snapshotDate,'YYYY-MM-DD') = coursesectsched.snapshotDate
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
-- View used to break down course category type counts for student

-- jh 20201007 Added totalCreditHrsCalc field and join to ClientConfigMCR to get indicator
-- jh 20200707 Removed termCode and partOfTermCode from grouping, since not in select fields

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
	sum((case when reg.crnGradingMode = 'Audit' then 1 else 0 end)) totalAuditCourses,
	sum((case when course.courseLevel = 'Undergrad' then
	        (case when config.instructionalActivityType in ('CR', 'B') and course.isClockHours = 0 then course.enrollmentHours 
                  when config.instructionalActivityType = 'B' and course.isClockHours = 1 then course.equivCRHRFactor * course.enrollmentHours
                  else 0 end)
        else 0 end)) totalCreditHrsCalc
from RegistrationMCR reg 
	left join CourseMCR course on reg.termCode = course.termCode
        and reg.partOfTermCode = course.partOfTermCode
		and reg.crn = course.crn
		and reg.crnLevel = course.courseLevel 
		and to_date(reg.snapshotDate,'YYYY-MM-DD') = to_date(course.snapshotDate,'YYYY-MM-DD')
	cross join (select first(instructionalActivityType) instructionalActivityType
                  from ClientConfigMCR) config
group by reg.personId
),

CourseTypeCountsCRN as (
-- View used to calculate credit hours by course level

--jh 20201007 Created view to simplify course level credit counts

    select sum(UGCreditHours) UGCreditHours,
            sum(UGClockHours) UGClockHours,
            sum(GRCreditHours) GRCreditHours,
            sum(PGRCreditHours) PGRCreditHours
    from (
    select crn crn,
            UGCreditHours * studentCount UGCreditHours,
            UGClockHours * studentCount UGClockHours,
            GRCreditHours * studentCount GRCreditHours,
            PGRCreditHours * studentCount PGRCreditHours
    from (
        select distinct course.crn crn,
                course.courseLevel courseLevel,
                (case when course.isClockHours = 0 and course.courseLevel in ('Undergrad', 'Continuing Ed') then coalesce(course.enrollmentHours, 0) else 0 end) UGCreditHours,
                (case when course.isClockHours = 1 and course.courseLevel in ('Undergrad', 'Continuing Ed') then coalesce(course.enrollmentHours, 0) else 0 end) UGClockHours,
                (case when course.isClockHours = 0 and course.courseLevel in ('Graduate', 'Professional') then coalesce(course.enrollmentHours, 0) else 0 end) GRCreditHours,
                (case when course.isClockHours = 0 and course.courseLevel = 'Postgraduate' then coalesce(course.enrollmentHours, 0) else 0 end) PGRCreditHours,
                count(reg.personId) studentCount
        from CourseMCR course 
         inner join Registration reg on course.crn = reg.crn
            and reg.partOfTermCode = course.partOfTermCode
            and reg.crn = course.crn
            and reg.crnLevel = course.courseLevel 
            and to_date(reg.snapshotDate,'YYYY-MM-DD') = to_date(course.snapshotDate,'YYYY-MM-DD') 
        group by course.crn,
                course.courseLevel,
                course.enrollmentHours,
                course.isClockHours
        )
	)
),

StuLevel as (
-- View used to break down course category type counts by type for students enrolled for at least one credit/clock hour

-- jh 20201007 Added student levels to Undergrad and Graduate values; added isNonDegreeSeeking students to timeStatus calculation
-- jh 20200910 Created StuLevel view to determine new 12Mo requirements for Part A and Part C

--mod from v1 - Part A doesn't contain graduate student level value 99
--mod from v2 - Part A doesn't contain transfer student level values 2, 16

--Student level table (Part A)
--1 - Full-time, first-time degree/certificate-seeking undergraduate
--3 - Full-time, continuing degree/certificate-seeking undergraduate
--7 - Full-time, non-degree/certificate-seeking undergraduate
--15 - Part-time, first-time degree/certificate-seeking undergraduate
--17 - Part-time, continuing degree/certificate-seeking undergraduate
--21 - Part-time, non-degree/certificate-seeking undergraduate

--mod from v1 - Part C doesn't contain graduate student level value 3
--mod from v2 - Part A doesn't contain transfer student type values 2 & 16

--Student level table (Part C)
--1 - Degree/Certificate seeking undergraduate students
--2 - Non-Degree/Certificate seeking undergraduate Students
					   
select personId,
    timeStatus,
    studentLevel,
    studentType,
    isNonDegreeSeeking,
    totalCreditHrs totalCreditHrs,
    totalClockHrs totalClockHrs,
    totalCreditHrsCalc totalCreditHrsCalc,
    totalCreditGRHrs totalCreditGRHrs,
    totalCreditPostGRHrs totalCreditPostGRHrs,
    totalCourses totalCourses,
    totalDECourses totalDECourses,
    totalESLCourses totalESLCourses,
    totalRemCourses totalRemCourses,
    totalIntlCourses totalIntlCourses,
    DEStatus DEStatus,					  
    (case --when studentLevel = 'GR' then '99' 
         when isNonDegreeSeeking = true and timeStatus = 'FT' then '7'
         when isNonDegreeSeeking = true and timeStatus = 'PT' then '21'
         when studentLevel = 'UG' then 
            (case when studentType = 'First Time' and timeStatus = 'FT' then '1' 
                    --when studentType = 'Transfer' and timeStatus = 'FT' then '2'
                    when studentType = 'Returning' and timeStatus = 'FT' then '3'
                    when studentType = 'First Time' and timeStatus = 'PT' then '15' 
                    --when studentType = 'Transfer' and timeStatus = 'PT' then '16'
                    when studentType = 'Returning' and timeStatus = 'PT' then '17' else '1' 
			 end)
        else null
    end) studentLevelPartA, --only Undergrad (and Continuing Ed, who go in Undergrad) students are counted in headcount
    (case --when studentLevel = 'GR' then '3'
         when isNonDegreeSeeking = true then '2'
         when studentLevel = 'UG' then '1'
         else null
    end) studentLevelPartC ----only Undergrad (and Continuing Ed, who go in Undergrad) students are counted in headcount
from (
    select personId personId,
            (case when studentLevel = 'UG' or isNonDegreeSeeking = true then
                (case when instructionalActivityType in ('CR', 'B') then 
                            (case when totalCreditHrsCalc >= requiredFTCreditHoursUG then 'FT' else 'PT' end)
                      when instructionalActivityType = 'CL' then 
                            (case when totalClockHrs >= requiredFTClockHoursUG then 'FT' else 'PT' end) 
                      else 'UG null' end)
            else null end) timeStatus,
            studentLevel studentLevel,
            studentType studentType,
            isNonDegreeSeeking isNonDegreeSeeking,
            totalCreditHrs totalCreditHrs,
            totalClockHrs totalClockHrs,
            totalCreditHrsCalc totalCreditHrsCalc,
            totalCreditGRHrs totalCreditGRHrs,
            totalCreditPostGRHrs totalCreditPostGRHrs,
            totalCourses totalCourses,
            totalDECourses totalDECourses,
            totalESLCourses totalESLCourses,
            totalRemCourses totalRemCourses,
            totalIntlCourses totalIntlCourses,
            DEStatus DEStatus
    from ( 
        select distinct stu.personId personId,
                stu.requiredFTCreditHoursGR,
                stu.requiredFTCreditHoursUG,
                stu.requiredFTClockHoursUG,
                (case when stu.studentLevel in ('Undergrad', 'Continuing Ed')  then 'UG'
                      when stu.studentLevel in ('Graduate', 'Postgraduate', 'Professional') then 'GR'
                      else null 
                end) studentLevel,
                stu.studentType studentType,
                stu.isNonDegreeSeeking isNonDegreeSeeking,
                config.instructionalActivityType instructionalActivityType,
                course.totalCreditHrs totalCreditHrs,
                course.totalClockHrs totalClockHrs,
                course.totalCreditHrsCalc totalCreditHrsCalc,
                course.totalCreditGRHrs totalCreditGRHrs,
                course.totalCreditPostGRHrs totalCreditPostGRHrs,
                course.totalCourses totalCourses,
                course.totalDECourses totalDECourses,
                course.totalESLCourses totalESLCourses,
                course.totalRemCourses totalRemCourses,
                course.totalIntlCourses totalIntlCourses,
                (case when course.totalCourses = course.totalDECourses then 'DE Exclusively'
		              when course.totalDECourses > 0 then 'DE Some'
		              else 'DE None'
	            end) DEStatus
        from RegistrationMCR reg
            inner join StudentRefactor stu on reg.personId = stu.personId
                and reg.termCode = stu.firstFullTerm
            left join CourseTypeCountsSTU course on reg.personId = course.personId
            cross join (select first(instructionalActivityType) instructionalActivityType
                          from ClientConfigMCR) config
        )
--filter out any student not enrolled for a credit course
    where totalCreditHrs > 0 
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

-- jh 20201007 Simplified to pull records from StuLevel and PersonMCR only with indicators pulled from ClientConfigMCR

select distinct stu.personID personID,  
	stu.studentLevelPartA ipedsPartAStudentLevel, --null for students not counted in headcount
	stu.studentLevelPartC ipedsPartCStudentLevel, --null for students not counted in headcount
	(case when person.gender = 'Male' then 'M'
			when person.gender = 'Female' then 'F' 
			when person.gender = 'Non-Binary' then config.genderForNonBinary
		else config.genderForUnknown
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
			when person.isInUSOnVisa = 1 or person.censusDate between person.visaStartDate and person.visaEndDate then '1' -- 'nonresident alien'
			else '9' -- 'race and ethnicity unknown'
	end) ipedsEthnicity,  
	stu.DEStatus DEStatus
from StuLevel stu
	left join PersonMCR person on stu.personId = person.personId
    cross join (select first(genderForNonBinary) genderForNonBinary,
                        first(genderForUnknown) genderForUnknown
                  from ClientConfigMCR) config
),

InstructionHours as (
--Sums instructional hours and filters for aggregation

-- jh 20201007 Changed to pull from CourseTypeCountsCRN instead of CohortSTU
-- jh 20200910 Created InstructionHours view for Part B

select (case when icOfferUndergradAwardLevel = 'Y' and instructionalActivityType != 'CL' then coalesce(UGCredit, 0) 
            else null 
        end) field2, -- CREDHRSU - credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.
        (case when icOfferUndergradAwardLevel = 'Y' and instructionalActivityType != 'CR' then coalesce(UGClock, 0) 
            else null 
        end) field3, -- CONTHRS  - clock hour instructional activity at the undergraduate level, 0 to 9999999, blank = not applicable, if no undergraduate programs are measured in clock hours.
        (case when icOfferGraduateAwardLevel = 'Y' then coalesce(GRCredit, 0)
            else null
        end) field4, -- CREDHRSG - credit hour instructional activity at the graduate level, 0 to 99999999, blank = not applicable
        (case when icOfferDoctorAwardLevel = 'Y' then
                (case when coalesce(PostGRCredit, 0) > 0 then coalesce(cast(round(PostGRCredit / tmAnnualDPPCreditHoursFTE, 0) as string), '0')
                      else '0'
                 end)
             else null
       end) field5
from ( 
    select UGCreditHours UGCredit,
           UGClockHours UGClock,
           GRCreditHours GRCredit,
           PGRCreditHours PostGRCredit,
           icOfferUndergradAwardLevel,
           icOfferGraduateAwardLevel,
           icOfferDoctorAwardLevel,
           instructionalActivityType,
           tmAnnualDPPCreditHoursFTE
    from CourseTypeCountsCRN
        cross join ConfigIndicators
    )
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

-- Part A: Unduplicated Count by Student Level, Gender, and Race/Ethnicity

-- ipedsStudentLevelPartA valid values - Student level table (Part A)
--1 - Full-time, first-time degree/certificate-seeking undergraduate
--3 - Full-time, continuing degree/certificate-seeking undergraduate
--7 - Full-time, non-degree/certificate-seeking undergraduate
--15 - Part-time, first-time degree/certificate-seeking undergraduate
--17 - Part-time, continuing degree/certificate-seeking undergraduate
--21 - Part-time, non-degree/certificate-seeking undergraduate

select 'A' part,
       ipedsPartAStudentLevel field1,  -- SLEVEL   - valid values are 1 for undergraduate and 3 for graduate
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
where ipedsPartAStudentLevel is not null --only Undergrad students
group by ipedsPartAStudentLevel 

union

-- Part C: 12-month Unduplicated Count - Distance Education Status

-- ipedsPartCStudentLevel valid values - Student level table (Part C)
--1 - Degree/Certificate seeking undergraduate students
--2 - Non-Degree/Certificate seeking undergraduate Students

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
where ipedsPartCStudentLevel is not null --only Undergrad students
and DEStatus != 'DE None'
group by ipedsPartCStudentLevel

union

-- Part B: Instructional Activity

--mod from v1 - remove Graduate and higher levels

select 'B' part,
       null field1,
       field2 field2, -- CREDHRSU - credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.
       field3 field3, -- CONTHRS  - clock hour instructional activity at the undergraduate level, 0 to 9999999, blank = not applicable, if no undergraduate programs are measured in clock hours.
       null field4, -- CREDHRSG - credit hour instructional activity at the graduate level, 0 to 99999999, blank = not applicable
       null field5, -- RDOCFTE  - reported Doctor'92s degree-professional practice student FTE, 0 to 99999999, blank = not applicable
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

union 

-- jh 20201007 Changed part B, field 3 to null instead of 0
-- jh 20200911 Added lines for Parts A and C	
-- ak 20200825 Dummy set to return default formatting if no cohortSTU records exist.

select *
from (
		VALUES
			('A', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
			('C', 1, 0, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
--mod from v1 - remove Graduate and higher levels
			('B', null, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
		) as dummySet(part, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11,
									field12, field13, field14, field15, field16, field17, field18, field19)
where not exists (select a.personId from CohortSTU a) 
