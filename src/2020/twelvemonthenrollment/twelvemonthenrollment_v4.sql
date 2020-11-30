/********************

EVI PRODUCT:	DORIS 2020-21 IPEDS Survey  
FILE NAME: 		12 Month Enrollment v4 (E1F)
FILE DESC:      12 Month Enrollment for private 2 year and less than 2-year non-degree-granting institutions
AUTHOR:         jhanicak
CREATED:        20200911

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Student Counts
Course Counts
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)  	Author             	    Tag             	Comments
----------- 		--------------------	-------------   	-------------------------------------------------
20201119			jhanicak									Added surveySection field throughout survey, changed order of fields for consistency,
                                                                PF-1750 mods to ipedsEthnicity code block prod 11m 29s test 8m 4s
																Updated to latest views PF-1768
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
--mod from v2 - Part A doesn't contain transfer student level values 2, 16
--mod from v3 - Part A doesn't contain non-degree-seeking values 7, 21
--mod from v1 - Part C doesn't contain graduate student level value 3
--mod from v3 - Part C doesn't contain student level value 2 and value 1 is now All undergrad students

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

--Prod blocks (2)
 select '2021' surveyYear, 
	'E1F' surveyId,  
	'Full Year Term End' repPeriodTag1,
	'Full Year June End' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2019-07-01' AS DATE) reportingDateStart,
	CAST('2020-06-30' AS DATE) reportingDateEnd,
	'202010' termCode, --Fall 2019
	'1' partOfTermCode, 
	CAST('2019-10-15' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No; Default value (if no record or null value): N'
    'N' icOfferDoctorAwardLevel, --Y = Yes, N = No; Default value (if no record or null value): N'
--***** start survey-specific mods
	 12 tmAnnualDPPCreditHoursFTE --1 to 99
--***** end survey-specific mods

union

select '2021' surveyYear, 
	'E1F' surveyId,  
	'Full Year Term End' repPeriodTag1,
	'Full Year June End' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2019-07-01' AS DATE) reportingDateStart,
	CAST('2020-06-30' AS DATE) reportingDateEnd,
	'201930' termCode, --Summer 2019
	'1' partOfTermCode, 
	CAST('2019-06-10' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No; Default value (if no record or null value): N'
    'N' icOfferDoctorAwardLevel, --Y = Yes, N = No; Default value (if no record or null value): N'
--***** start survey-specific mods
	 12 tmAnnualDPPCreditHoursFTE --1 to 99
--***** end survey-specific mods

/*
--Testing blocks (2 min)
select '1415' surveyYear,  
	'E1F' surveyId,  
	'Full Year Term End' repPeriodTag1,
	'Full Year June End' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201410' termCode,
	'1' partOfTermCode,
	CAST('2013-09-13' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No; Default value (if no record or null value): N'
    'N' icOfferDoctorAwardLevel, --Y = Yes, N = No; Default value (if no record or null value): N'
--***** start survey-specific mods
	 12 tmAnnualDPPCreditHoursFTE --1 to 99
--***** end survey-specific mods


union

select '1415' surveyYear,  
	'E1F' surveyId,   
	'Full Year Term End' repPeriodTag1,
	'Full Year June End' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201330' termCode,
	'1' partOfTermCode,
	CAST('2013-06-10' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'N' icOfferGraduateAwardLevel, --Y = Yes, N = No; Default value (if no record or null value): N'
    'N' icOfferDoctorAwardLevel, --Y = Yes, N = No; Default value (if no record or null value): N'
--***** start survey-specific mods
	 12 tmAnnualDPPCreditHoursFTE --1 to 99
--***** end survey-specific mods
*/
),

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 

--  1st union 1st order - pull snapshot for defvalues.repPeriodTag1 
--  1st union 2nd order - pull snapshot for defvalues.repPeriodTag2
--  1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--  2nd union - pull default values if no record in IPEDSReportingPeriod

select distinct RepDates.surveyYear	surveyYear,
    RepDates.source source,
    coalesce(upper(RepDates.surveySection), 'COHORT') surveySection,
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
    RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,
    to_date(RepDates.censusDate,'YYYY-MM-DD') censusDate,
	to_date(RepDates.reportingDateStart,'YYYY-MM-DD') reportingDateStart,
    to_date(RepDates.reportingDateEnd,'YYYY-MM-DD') reportingDateEnd,
    RepDates.repPeriodTag1 repPeriodTag1,
	RepDates.repPeriodTag2 repPeriodTag2
from (
    select repperiodENT.surveyCollectionYear surveyYear,
	    'IPEDSReportingPeriod' source,
		repperiodENT.snapshotDate snapshotDate,
		repPeriodENT.surveyId surveyId,
		repPeriodENT.surveySection surveySection,
		coalesce(repperiodENT.reportingDateStart, defvalues.reportingDateStart) reportingDateStart,
		coalesce(repperiodENT.reportingDateEnd, defvalues.reportingDateEnd) reportingDateEnd,
		coalesce(repperiodENT.termCode, defvalues.termCode) termCode,
		coalesce(repperiodENT.partOfTermCode, defvalues.partOfTermCode) partOfTermCode,
		defvalues.censusDate censusDate,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
		row_number() over (	
			partition by 
				repPeriodENT.surveyCollectionYear,
                repPeriodENT.surveyId,
                repPeriodENT.surveySection, 
				repperiodENT.termCode,
				repperiodENT.partOfTermCode	
			order by 
			    (case when array_contains(repperiodENT.tags, defvalues.repPeriodTag1) then 1
                     when array_contains(repperiodENT.tags, defvalues.repPeriodTag2) then 2
			         else 3 end) asc,
			     repperiodENT.snapshotDate desc,
                repperiodENT.recordActivityDate desc	
		) reportPeriodRn	
		from IPEDSReportingPeriod repperiodENT
		    inner join DefaultValues defvalues on repperiodENT.surveyId = defvalues.surveyId
	    and repperiodENT.surveyCollectionYear = defvalues.surveyYear
	    where repperiodENT.termCode is not null
		and repperiodENT.partOfTermCode is not null
	
    union 
 
	select defvalues.surveyYear surveyYear,
	    'DefaultValues' source,
		CAST('9999-09-09' as DATE) snapshotDate,
		defvalues.surveyId surveyId, 
		null surveySection,
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		defvalues.termCode termCode,
		defvalues.partOfTermCode partOfTermCode, 
		defvalues.censusDate censusDate,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
		1
	from DefaultValues defvalues
    where defvalues.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = defvalues.surveyYear
											and upper(repperiodENT.surveyId) = defvalues.surveyId 
											and repperiodENT.termCode is not null
											and repperiodENT.partOfTermCode is not null) 
    ) RepDates
where RepDates.reportPeriodRn = 1
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 
 
--  1st union 1st order - pull snapshot where same as ReportingPeriodMCR snapshotDate
--  1st union 2nd order - pull closet snapshot before ReportingPeriodMCR snapshotDate
--  1st union 3rd order - pull closet snapshot after ReportingPeriodMCR snapshotDate
--  2nd union - pull default values if no record in IPEDSClientConfig

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.source source,
    to_date(ConfigLatest.snapshotDate,'YYYY-MM-DD') snapshotDate,
    ConfigLatest.repperiodSnapshotDate repperiodSnapshotDate,
	upper(ConfigLatest.genderForUnknown) genderForUnknown,
	upper(ConfigLatest.genderForNonBinary) genderForNonBinary,
    upper(ConfigLatest.instructionalActivityType) instructionalActivityType,
    upper(ConfigLatest.acadOrProgReporter) acadOrProgReporter,
    upper(ConfigLatest.publicOrPrivateInstitution) publicOrPrivateInstitution,
    upper(ConfigLatest.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
    upper(ConfigLatest.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
    upper(ConfigLatest.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
	ConfigLatest.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
    upper(ConfigLatest.tmAnnualDPPCreditHoursFTE) tmAnnualDPPCreditHoursFTE
--***** end survey-specific mods

from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'configFullYearTag' source,
		clientConfigENT.snapshotDate snapshotDate, 
		repperiod.snapshotDate repperiodSnapshotDate,
		coalesce(clientConfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		coalesce(clientConfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
        coalesce(clientConfigENT.instructionalActivityType, defvalues.instructionalActivityType) instructionalActivityType,
        coalesce(clientconfigENT.acadOrProgReporter, defvalues.acadOrProgReporter) acadOrProgReporter,
        coalesce(clientconfigENT.publicOrPrivateInstitution, defvalues.publicOrPrivateInstitution) publicOrPrivateInstitution,
        coalesce(clientConfigENT.icOfferUndergradAwardLevel, defvalues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
		coalesce(clientConfigENT.icOfferGraduateAwardLevel, defvalues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
        coalesce(clientConfigENT.icOfferDoctorAwardLevel, defvalues.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,		
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
		coalesce(clientConfigENT.tmAnnualDPPCreditHoursFTE, defvalues.tmAnnualDPPCreditHoursFTE) tmAnnualDPPCreditHoursFTE,
--***** end survey-specific mods
		row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
			    (case when to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
				clientConfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientConfigENT
	    inner join DefaultValues defvalues on clientConfigENT.surveyCollectionYear = defvalues.surveyYear
		inner join ReportingPeriodMCR repperiod on clientConfigENT.surveyCollectionYear = repperiod.surveyYear

    union

	select defvalues.surveyYear surveyYear,
	    'default' source,
	    CAST('9999-09-09' as DATE) snapshotDate,
	    null repperiodSnapshotDate,  
		defvalues.genderForUnknown genderForUnknown,
		defvalues.genderForNonBinary genderForNonBinary,
        defvalues.instructionalActivityType instructionalActivityType,
        defvalues.acadOrProgReporter acadOrProgReporter,
        defvalues.publicOrPrivateInstitution publicOrPrivateInstitution,
        defvalues.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		defvalues.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        defvalues.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
        defvalues.tmAnnualDPPCreditHoursFTE tmAnnualDPPCreditHoursFTE,
--***** end survey-specific mods
		1 configRn
    from DefaultValues defvalues
    where defvalues.surveyYear not in (select max(configENT.surveyCollectionYear)
										from IPEDSClientConfig configENT
										where configENT.surveyCollectionYear = defvalues.surveyYear)
	) ConfigLatest
where ConfigLatest.configRn = 1	
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for all term codes and parts of term code for all snapshots. 

-- jh 20201007 Included tags field to pull thru for 'Pre-Fall Summer Census' check
--				Moved check on termCode censusDate and snapshotDate to view AcademicTermReporting

select termCode, 
	partOfTermCode, 
	financialAidYear,
	to_date(snapshotDate, 'YYYY-MM-DD') snapshotDate,
	to_date(startDate, 'YYYY-MM-DD') startDate,
	to_date(endDate, 'YYYY-MM-DD') endDate,
	academicYear,
	to_date(censusDate, 'YYYY-MM-DD') censusDate,
    termType,
    termClassification,
	requiredFTCreditHoursGR,
	requiredFTCreditHoursUG,
	requiredFTClockHoursUG,
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
		acadtermENT.financialAidYear,
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

select termCode termCode, 
    max(termOrder) termOrder,
    to_date(max(censusDate), 'YYYY-MM-DD') maxCensus,
    to_date(min(startDate), 'YYYY-MM-DD') minStart,
    to_date(max(endDate), 'YYYY-MM-DD') maxEnd,
    termType termType
from (
	select acadterm.termCode termCode,
	    acadterm.partOfTermCode partOfTermCode,
	    acadterm.termType termType,
	    acadterm.censusDate censusDate,
	    acadterm.startDate startDate,
	    acadterm.endDate endDate,
		row_number() over (
			order by  
				acadterm.censusDate asc
        ) termOrder
	from AcademicTermMCR acadterm
	) 
group by termCode, termType
),

AcademicTermReporting as (
--Combines ReportingPeriodMCR and AcademicTermMCR in order to use the correct snapshot dates for the reporting terms

--	first order field: assign 1 if acadterm.snapshotDate falls within the acadterm.census range and the acadterm.tags value matches with the acadterm.termType
--  first order field: assign 2 if acadterm.snapshotDate falls within the acadterm.census range but acadterm.tags/acadterm.termType values don't match
--  first order field: assign 3 if acadterm.snapshotDate doesn't fall within the acadterm.census range and acadterm.tags/acadterm.termType values don't match
--  second order field: acadterm.snapshotDates before the acadterm.census range ordered so the snapshotDate closest to the censusDate is first - ascending
--  third order field: acadterm.snapshotDates after the acadterm.census range ordered so the snapshotDate closest to the censusDate is first - descending
--  if none of the above, order by repperiod.snapshotDate

select repPerTerms.yearType yearType,
        repPerTerms.surveySection surveySection,
        repPerTerms.termCode termCode,
        repPerTerms.partOfTermCode partOfTermCode,
        repPerTerms.financialAidYear financialAidYear,
        repPerTerms.termOrder termOrder,
        repPerTerms.maxCensus maxCensus,
        coalesce(repPerTerms.acadTermSSDate, repPerTerms.repPeriodSSDate) snapshotDate,
        repPerTerms.termClassification termClassification,
        repPerTerms.termType termType,
        repPerTerms.startDate startDate,
        repPerTerms.endDate endDate,
        repPerTerms.censusDate censusDate,
        repPerTerms.requiredFTCreditHoursGR,
	    repPerTerms.requiredFTCreditHoursUG,
	    repPerTerms.requiredFTClockHoursUG,
	    repPerTerms.genderForUnknown,
		repPerTerms.genderForNonBinary,
		repPerTerms.instructionalActivityType,
		repPerTerms.acadOrProgReporter,
	    repPerTerms.equivCRHRFactor equivCRHRFactor,
        (case when repPerTerms.termClassification = 'Standard Length' then 1
             when repPerTerms.termClassification is null then (case when repPerTerms.termType in ('Fall', 'Spring') then 1 else 2 end)
             else 2
        end) fullTermOrder
from (
select distinct 'CY' yearType,
        repperiod.surveySection surveySection,
        repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        acadterm.financialAidYear financialAidYear,
        acadterm.snapshotDate acadTermSSDate,
        repperiod.snapshotDate repPeriodSSDate,
        acadterm.tags tags,
        coalesce(acadterm.censusDate, repperiod.censusDate) censusDate,
		row_number() over (
            partition by 
                repperiod.termCode,
                repperiod.partOfTermCode
            order by
                (case when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') 
                            and ((array_contains(acadterm.tags, 'Fall Census') and acadterm.termType = 'Fall')
                                or (array_contains(acadterm.tags, 'Spring Census') and acadterm.termType = 'Spring')
                                or (array_contains(acadterm.tags, 'Pre-Fall Summer Census') and acadterm.termType = 'Summer')
                                or (array_contains(acadterm.tags, 'Post-Fall Summer Census') and acadterm.termType = 'Summer')) then 1
                      when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') then 2
                     else 3 end) asc,
                (case when acadterm.snapshotDate < acadterm.censusDate then acadterm.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                (case when acadterm.snapshotDate > acadterm.censusDate then acadterm.snapshotDate else CAST('9999-09-09' as DATE) end) asc
            ) acadTermRnReg,
            termorder.termOrder termOrder,
        termorder.maxCensus maxCensus,
        acadterm.termClassification termClassification,
        acadterm.termType termType,
        acadterm.startDate startDate,
        acadterm.endDate endDate,
        acadterm.requiredFTCreditHoursGR,
	    acadterm.requiredFTCreditHoursUG,
	    acadterm.requiredFTClockHoursUG,
	    clientconfig.genderForUnknown,
		clientconfig.genderForNonBinary,
		clientconfig.instructionalActivityType,
		clientconfig.acadOrProgReporter,
	    coalesce(acadterm.requiredFTCreditHoursUG/
		    coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor
     from ReportingPeriodMCR repperiod 
        left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
	            and repperiod.partOfTermCode = acadterm.partOfTermCode
		left join AcademicTermOrder termorder on termOrder.termCode = repperiod.termCode
		inner join ClientConfigMCR clientconfig on repperiod.surveyYear = clientconfig.surveyYear
		) repPerTerms
where repPerTerms.acadTermRnReg = 1 
),

AcademicTermReportingRefactor as (
--Returns all records from AcademicTermReporting, converts Summer terms to Pre-Fall or Post-Spring and creates reportingDateStart/End

-- jh 20201007 Added to return new Summer termTypes and reportingDateStart/End

select rep.*,
        (case when rep.termType = 'Summer' and rep.termClassification != 'Standard Length' then 
                    case when (select max(rep2.termOrder)
                    from AcademicTermReporting rep2
                    where rep2.termType = 'Summer') < (select max(rep2.termOrder)
                                                        from AcademicTermReporting rep2
                                                        where rep2.termType = 'Fall') then 'Pre-Fall Summer'
                    else 'Post-Spring Summer' end
                else rep.termType end) termTypeNew,
        potMax.partOfTermCode maxPOT
from AcademicTermReporting rep
    inner join (select rep3.termCode,
                        rep3.partOfTermCode,
                       row_number() over (
			                partition by
			                    rep3.termCode
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
        inner join AcademicTermReportingRefactor acadterm on acadterm.snapshotDate = campusENT.snapshotDate
	where campusENT.isIpedsReportable = 1 
		and ((to_date(campusENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
			and to_date(campusENT.recordActivityDate,'YYYY-MM-DD') <= to_date(campusENT.snapshotDate,'YYYY-MM-DD'))
				or to_date(campusENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
	)
where campusRn = 1
),

RegistrationMCR as ( 
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 

select *
from (
    select regData.yearType, 
        regData.surveySection,
        regData.snapshotDate,
        regData.termCode,
        regData.partOfTermCode,
        regData.regENTSSD regENTSSD,
        regData.repSSD repSSD,
        campus.snapshotDate campusSSD,
        regData.financialAidYear,
        regData.termorder,
        regData.maxCensus,
        regData.censusDate,
        regData.fullTermOrder,
        regData.termType,
        regData.startDate,
        regData.requiredFTCreditHoursGR,
        regData.requiredFTCreditHoursUG,
        regData.requiredFTClockHoursUG,
        regData.genderForUnknown,
		regData.genderForNonBinary,
		regData.instructionalActivityType,
		regData.acadOrProgReporter,
        regData.equivCRHRFactor,
        regData.personId,                    
        regData.crn,
        regData.crnLevel,    
        regData.crnGradingMode,
        regData.campus,
        coalesce(campus.isInternational, false) isInternational,
        row_number() over (
                partition by
                    regData.yearType,
                    regData.surveySection,
                    regData.termCode,
                    regData.partOfTermCode,
                    regData.personId,
                    regData.crn,
                    regData.crnLevel
                order by 
                    (case when campus.snapshotDate = regData.snapshotDate then 1 else 2 end) asc,
                    (case when campus.snapshotDate < regData.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                    (case when campus.snapshotDate > regData.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc
            ) regCampRn
    from ( 
        select regENT.personId personId,
            repperiod.snapshotDate snapshotDate,
            to_date(regENT.snapshotDate, 'YYYY-MM-DD') regENTSSD,
            repperiod.snapshotDate repSSD,
            regENT.termCode termCode,
            regENT.partOfTermCode partOfTermCode, 
            repperiod.surveySection surveySection,
            repperiod.financialAidYear financialAidYear,
            repperiod.termorder termorder,
            repperiod.maxCensus maxCensus,
            repperiod.fullTermOrder fullTermOrder,
            repperiod.termTypeNew termType,
            repperiod.yearType yearType,
            repperiod.startDate startDate,
            repperiod.censusDate censusDate,
            repperiod.requiredFTCreditHoursGR,
            repperiod.requiredFTCreditHoursUG,
            repperiod.requiredFTClockHoursUG,
            repperiod.genderForUnknown,
		    repperiod.genderForNonBinary,
		    repperiod.instructionalActivityType,
		    repperiod.acadOrProgReporter,
            repperiod.equivCRHRFactor,
            upper(regENT.campus) campus,
            coalesce(regENT.crnGradingMode, 'Standard') crnGradingMode,                    
            upper(regENT.crn) crn,
            regENT.crnLevel crnLevel,
            row_number() over (
                partition by
                    repperiod.yearType,
                    repperiod.surveySection,
                    regENT.termCode,
                    regENT.partOfTermCode,
                    regENT.personId,
                    regENT.crn,
                    regENT.crnLevel
                order by 
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    regENT.recordActivityDate desc
            ) regRn
        from AcademicTermReportingRefactor repperiod   
            inner join Registration regENT on regENT.termCode = repperiod.termCode
                and repperiod.partOfTermCode = regENT.partOfTermCode
                and ((to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                    and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
                        or to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
                and regENT.isEnrolled = 1
                and regENT.isIpedsReportable = 1 
                ) regData
            left join CampusMCR campus on regData.campus = campus.campus
        where regData.regRn = 1
    )
where regCampRn = 1
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  

select stuData.yearType,
        stuData.surveySection,
        stuData.snapshotDate snapshotDate,
        stuData.termCode, 
        stuData.termOrder,
        stuData.financialAidYear,
        stuData.maxCensus,
        stuData.termType,
        stuData.startDate,
        stuData.censusDate,
        stuData.maxCensus,
        stuData.fullTermOrder,
        stuData.startDate,
        stuData.personId,
        coalesce((case when stuData.studentType = 'High School' then true
                    when stuData.studentLevel = 'Continuing Ed' then true
                    when stuData.studentLevel = 'Occupational/Professional' then true
                  else stuData.isNonDegreeSeeking end), false) isNonDegreeSeeking,
        stuData.studentLevel,
        stuData.studentType,
        stuData.residency,
        stuData.campus
from ( 
	 select reg.yearType yearType,
            reg.snapshotDate snapshotDate,
            to_date(studentENT.snapshotDate,'YYYY-MM-DD') stuSSD,
            reg.surveySection surveySection,
            reg.termCode termCode, 
            reg.termOrder termOrder,
            reg.censusDate censusDate,
            reg.maxCensus maxCensus,
            reg.termType termType,
            reg.startDate startDate,
            reg.fullTermOrder fullTermOrder, --1 for 'full' (standard), 2 for non-standard
            reg.financialAidYear financialAidYear,
            studentENT.personId personId,
            studentENT.isNonDegreeSeeking isNonDegreeSeeking,
            studentENT.studentLevel studentLevel,
            studentENT.studentType studentType,
            studentENT.residency residency,
            studentENT.campus campus,
            row_number() over (
                partition by
                    reg.yearType,
                    reg.surveySection,
                    studentENT.personId,                    
                    studentENT.termCode
                order by
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') = reg.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    studentENT.recordActivityDate desc
            ) studRn
	from RegistrationMCR reg
		inner join Student studentENT on reg.personId = studentENT.personId 
			and reg.termCode = studentENT.termCode
			and ((to_date(studentENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)  
				and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= reg.censusDate
				and studentENT.studentStatus = 'Active') --do not report Study Abroad students
					or to_date(studentENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
			and studentENT.isIpedsReportable = 1
	) stuData
where stuData.studRn = 1 
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
select *
from ( 
select stu.yearType yearType,
        acadTermCode.surveySection surveySection,
        acadTermCode.snapshotDate snapshotDate,
        stu.firstFullTerm firstFullTerm,
        acadTermCode.censusDate censusDate,
        acadTermCode.maxCensus maxCensus,
        acadTermCode.financialAidYear,
        acadTermCode.termOrder,
        acadTermCode.termType,
	    acadTermCode.genderForUnknown,
		acadTermCode.genderForNonBinary,
        stu.personId,
       stu.isNonDegreeSeeking,
       (case when stu.studentLevel = 'Undergrad' then 
                (case when stu.studentTypeTermType = 'Fall' and stu.studentType = 'Returning' and stu.preFallStudType is not null then stu.preFallStudType
                      else stu.studentType 
                end)
             else stu.studentType 
        end) studentType,
        stu.studentLevel studentLevelORIG,
        (case when stu.studentLevel in ('Undergrad', 'Continuing Ed', 'Occupational/Professional')  then 'UG'
                          when stu.studentLevel in ('Graduate') then 'GR'
                          else null 
                    end) studentLevel,
        stu.campus campus,
        coalesce(campus.isInternational, false) isInternational,        
	    row_number() over (
                partition by
                    stu.yearType,
                    acadTermCode.surveySection,
                    stu.personId
                order by 
                    (case when campus.snapshotDate = acadTermCode.snapshotDate then 1 else 2 end) asc,
                    (case when campus.snapshotDate < acadTermCode.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                    (case when campus.snapshotDate > acadTermCode.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc
            ) regCampRn
    from ( 
        select distinct personId,
            yearType,
            min(isNonDegreeSeeking) isNonDegreeSeeking,
            max(studentType) studentType,
            max(firstFullTerm) firstFullTerm,
            max(studentLevel) studentLevel,
            max(studentTypeTermType) studentTypeTermType,
            max(preFallStudType) preFallStudType,
            max(campus) campus
        from (
            select distinct personId,
                yearType,
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
                (case when FFTRn = 1 then termCode else null end) firstFullTerm,
				(case when FFTRn = 1 then campus else null end) campus
            from (
                select yearType,
                    surveySection,
                    snapshotDate,
                    termCode, 
                    termOrder,
                    termType,
                    fullTermOrder,
                    personId,
                    isNonDegreeSeeking,
                    studentType,
                    studentLevel,
					campus,
                    row_number() over (
                                partition by
                                    personId,
                                    yearType
                            order by isNonDegreeSeeking asc,
                                    fullTermOrder asc, --all standard length terms first
                                    termOrder asc, --order by term to find first standard length term
                                    startDate asc --get record for term with earliest start date (consideration for parts of term only)
                            ) NDSRn,
                    row_number() over (
                                partition by
                                    personId,
                                    yearType
                            order by fullTermOrder asc, --all standard length terms first
                                    termOrder asc, --order by term to find first standard length term
                                    startDate asc --get record for term with earliest start date (consideration for parts of term only)
                            ) FFTRn
                      from StudentMCR stu
                    )
                )
            group by personId, yearType
       ) stu    
    inner join AcademicTermReportingRefactor acadTermCode on acadTermCode.termCode = stu.firstFullTerm
        and acadTermCode.partOfTermCode = acadTermCode.maxPOT
        and coalesce(acadTermCode.yearType, 'CY') = coalesce(stu.yearType, 'CY')
    left join CampusMCR campus on stu.campus = campus.campus
    )
where regCampRn = 1 
and isInternational = false
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 
-- Per PF-1750, modified ipedsEthnicity subquery to account for new visaType field 
select pers.personId personId,
        pers.yearType yearType,
        pers.surveySection surveySection,
        pers.snapshotDate snapshotDate,
        pers.financialAidYear financialAidYear,
        pers.termCode termCode,
        pers.censusDate censusDate,
        pers.maxCensus maxCensus,
        pers.termOrder termOrder,
        pers.isNonDegreeSeeking isNonDegreeSeeking,
        pers.studentLevel studentLevel,
        pers.studentType studentType,
        (case when pers.gender = 'Male' then 'M'
            when pers.gender = 'Female' then 'F' 
            when pers.gender = 'Non-Binary' then pers.genderForNonBinary
            else pers.genderForUnknown
        end) ipedsGender,
        (case when pers.isUSCitizen = 1 or ((pers.isInUSOnVisa = 1 or pers.censusDate between pers.visaStartDate and pers.visaEndDate)
                            and pers.visaType in ('Employee Resident', 'Other Resident')) then 
            (case when pers.isHispanic = true then '2' 
                when pers.isMultipleRaces = true then '8' 
                when pers.ethnicity != 'Unknown' and pers.ethnicity is not null then
                    (case when pers.ethnicity = 'Hispanic or Latino' then '2'
                        when pers.ethnicity = 'American Indian or Alaskan Native' then '3'
                        when pers.ethnicity = 'Asian' then '4'
                        when pers.ethnicity = 'Black or African American' then '5'
                        when pers.ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
                        when pers.ethnicity = 'Caucasian' then '7'
                        else '9' 
                    end) 
                else '9' end) -- 'race and ethnicity unknown'
            when ((pers.isInUSOnVisa = 1 or pers.censusDate between pers.visaStartDate and pers.visaEndDate)
                and pers.visaType in ('Student Non-resident', 'Employee Non-resident', 'Other Non-resident')) then '1' -- 'nonresident alien'
            else '9' -- 'race and ethnicity unknown'
        end) ipedsEthnicity
from (
    select distinct 
            stu.yearType yearType,
            stu.surveySection surveySection,
            to_date(stu.snapshotDate,'YYYY-MM-DD') snapshotDate,
            stu.firstFullTerm termCode,
            stu.censusDate censusDate,
            stu.maxCensus maxCensus,
            stu.financialAidYear,
            stu.termOrder termOrder,
            stu.personId personId,
            stu.isNonDegreeSeeking isNonDegreeSeeking,
            stu.studentLevel studentLevel,
            stu.studentType studentType,
	        stu.genderForUnknown,
		    stu.genderForNonBinary,
            to_date(personENT.birthDate,'YYYY-MM-DD') birthDate,
            personENT.ethnicity ethnicity,
            personENT.isHispanic isHispanic,
            personENT.isMultipleRaces isMultipleRaces,
            personENT.isInUSOnVisa isInUSOnVisa,
            to_date(personENT.visaStartDate,'YYYY-MM-DD') visaStartDate,
            to_date(personENT.visaEndDate,'YYYY-MM-DD') visaEndDate,
            personENT.visaType visaType,
            personENT.isUSCitizen isUSCitizen,
            personENT.gender gender,
            upper(personENT.nation) nation,
            upper(personENT.state) state,
            row_number() over (
                partition by
                    stu.yearType,
                    stu.surveySection,
                    stu.personId,
                    personENT.personId
                order by
                    (case when to_date(personENT.snapshotDate,'YYYY-MM-DD') = stu.snapshotDate then 1 else 2 end) asc,
			        (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') < stu.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') > stu.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    personENT.recordActivityDate desc
            ) personRn
    from StudentRefactor stu 
        left join Person personENT on stu.personId = personENT.personId
            and personENT.isIpedsReportable = 1
            and ((to_date(personENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
               and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= stu.censusDate) 
                or to_date(personENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
    ) pers
where pers.personRn = 1
),

CourseSectionMCR as (
--Included to get enrollment hours of a CRN
    
select *
from (
    select stu.yearType,
        reg.surveySection surveySection,
        reg.snapshotDate snapshotDate,
        stu.snapshotDate stuSSD,
        to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') courseSectionSSD,
        reg.termCode termCode,
        reg.partOfTermCode partOfTermCode,
        stu.financialAidYear,
        reg.censusDate,
        reg.termType,
        reg.termOrder,
        reg.requiredFTCreditHoursUG,
	    reg.requiredFTClockHoursUG,
	    reg.instructionalActivityType,
		reg.acadOrProgReporter,
	    stu.personId personId,
        stu.studentLevel,
	    stu.studentType,
	    stu.isNonDegreeSeeking,
	    stu.ipedsGender ipedsGender,
	    stu.ipedsEthnicity ipedsEthnicity,
        to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
        reg.crn,
        reg.crnLevel,
        coursesectENT.subject,
        coursesectENT.courseNumber,
        coursesectENT.section,
        coursesectENT.enrollmentHours,
        reg.equivCRHRFactor,
        reg.isInternational,
        coursesectENT.isClockHours,
        reg.crnGradingMode,
        row_number() over (
                partition by
                    reg.yearType,
                    reg.surveySection,
                    reg.termCode,
                    reg.partOfTermCode,
                    reg.personId,
                    reg.crn,
                    coursesectENT.crn,
                    reg.crnLevel,
                    coursesectENT.subject,
                    coursesectENT.courseNumber
                order by
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') = reg.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    coursesectENT.recordActivityDate desc
            ) courseRn
    from RegistrationMCR reg   
        inner join PersonMCR stu on stu.personId = reg.personId
            and stu.termCode = reg.termCode
            and coalesce(reg.yearType, 'CY') = coalesce(stu.yearType, 'CY')
            and stu.surveySection = reg.surveySection
        left join CourseSection coursesectENT on reg.termCode = coursesectENT.termCode
            and reg.partOfTermCode = coursesectENT.partOfTermCode
            and reg.crn = upper(coursesectENT.crn)
            and coursesectENT.isIpedsReportable = 1
            and ((to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                    and to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') <= reg.censusDate
				    and coursesectENT.sectionStatus = 'Active')
                or to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))  
    )
where courseRn = 1
),

CourseSectionScheduleMCR as (
--Returns course scheduling related info for the registration CRN. 
--AcademicTerm.partOfTermCode, CourseSectionSchedule.partOfTermCode & AcademicTerm.censusDate together are used to define the period 
--of valid course registration attempts. 

select *
from (
	select coursesect.yearType yearType,
	    coursesect.surveySection surveySection,
	    coursesect.snapshotDate snapshotDate, 
	    coursesect.courseSectionSSD courseSectionSSD,
	    to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') courseSectSchedSSD,
	    coursesect.termCode termCode,
	    coursesect.partOfTermCode partOfTermCode,
	    coursesect.financialAidYear,
		coursesect.censusDate censusDate,
		coursesect.termType termType,
		coursesect.termOrder termOrder, 
		coursesect.requiredFTCreditHoursUG,
	    coursesect.requiredFTClockHoursUG,
	    coursesect.instructionalActivityType,
	    coursesect.acadOrProgReporter,
        coursesect.personId personId,
	    coursesect.studentLevel,
	    coursesect.studentType,
	    coursesect.isNonDegreeSeeking,
	    coursesect.ipedsGender,
	    coursesect.ipedsEthnicity,
		to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
	    coursesect.crn crn,
		coursesect.subject subject,
		coursesect.courseNumber courseNumber,
		coursesect.section section,
		coursesectschedENT.section schedSection,
		coursesect.crnLevel crnLevel,
		coursesect.enrollmentHours enrollmentHours,
		coursesect.equivCRHRFactor equivCRHRFactor,
		coursesect.isInternational isInternational,
		coursesect.isClockHours isClockHours,
        coursesect.crnGradingMode crnGradingMode,
        coalesce(coursesectschedENT.meetingType, 'Classroom/On Campus') meetingType,
		row_number() over (
			partition by
			    coursesect.yearType,
			    coursesect.surveySection,
			    coursesect.termCode, 
				coursesect.partOfTermCode,
                coursesect.personId,
			    coursesect.crn,
			    coursesect.crnLevel,
			    coursesect.subject,
                coursesect.courseNumber
			order by
			    (case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') = coursesect.snapshotDate then 1 else 2 end) asc,
                (case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') < coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                (case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') > coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
			    coursesectschedENT.recordActivityDate desc
		) courseSectSchedRn
	from CourseSectionMCR coursesect
	    left join CourseSectionSchedule coursesectschedENT ON coursesect.termCode = coursesectschedENT.termCode 
			            and coursesect.partOfTermCode = coursesectschedENT.partOfTermCode
			            and coursesect.crn = upper(coursesectschedENT.crn)
			            and coursesectschedENT.isIpedsReportable = 1 
	                    and ((to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                            and to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') <= coursesect.censusDate)
                                or to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))  
	)
where courseSectSchedRn = 1
),

CourseMCR as (
--Included to get course type information

select *
from (
	select coursesectsched.yearType yearType,
	    coursesectsched.surveySection surveySection,
	    coursesectsched.snapshotDate snapshotDate,
	    coursesectsched.courseSectionSSD courseSectionSSD,
	    coursesectsched.courseSectSchedSSD courseSectSchedSSD,
	    to_date(courseENT.snapshotDate, 'YYYY-MM-DD') courseSSD,
	    coursesectsched.termCode termCode,
		coursesectsched.partOfTermCode partOfTermCode,
		coursesectsched.financialAidYear,
	    termorder.termOrder courseTermOrder,
	    coursesectsched.termOrder courseSectTermOrder,
	    coursesectsched.censusDate censusDate,
	    coursesectsched.termType termType,
	    coursesectsched.requiredFTCreditHoursUG,
	    coursesectsched.requiredFTClockHoursUG,
	    coursesectsched.instructionalActivityType,
	    coursesectsched.acadOrProgReporter,
        coursesectsched.personId personId,
	    coursesectsched.studentType,
	    coursesectsched.studentLevel,
	    coursesectsched.isNonDegreeSeeking,
	    coursesectsched.ipedsGender,
	    coursesectsched.ipedsEthnicity,
	    coursesectsched.crn crn,
		coursesectsched.section section,
		coursesectsched.section schedSection,
		coursesectsched.subject subject,
		coursesectsched.courseNumber courseNumber,
		coursesectsched.crnLevel courseLevel,
		coalesce(courseENT.isRemedial, false) isRemedial,
		coalesce(courseENT.isESL, false) isESL,
		coursesectsched.meetingType meetingType,
		coursesectsched.enrollmentHours enrollmentHours,
		coursesectsched.isClockHours isClockHours,
        coursesectsched.equivCRHRFactor equivCRHRFactor,
        coursesectsched.crnGradingMode crnGradingMode,
        coursesectsched.isInternational isInternational,
	    to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
	    row_number() over (
			partition by
			    coursesectsched.yearType,
                coursesectsched.surveySection,
			    coursesectsched.termCode, 
				coursesectsched.partOfTermCode,
                coursesectsched.personId,
			    coursesectsched.crn,
			    coursesectsched.crnLevel,
			    coursesectsched.subject,
                coursesectsched.courseNumber
			order by
			    (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') = coursesectsched.snapshotDate then 1 else 2 end) asc,
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') < coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') > coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
			    termorder.termOrder desc,
			    courseENT.recordActivityDate desc
		) courseRn
	from CourseSectionScheduleMCR coursesectsched
	    left join Course courseENT on coursesectsched.subject = upper(courseENT.subject) 
			        and coursesectsched.courseNumber = upper(courseENT.courseNumber) 
			        and coursesectsched.crnLevel = courseENT.courseLevel 
			        and courseENT.isIpedsReportable = 1
			        and ((to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
				        and to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') <= coursesectsched.censusDate
				        and courseENT.courseStatus = 'Active') 
					        or to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
		left join AcademicTermOrder termorder on termorder.termCode = courseENT.termCodeEffective
            and termorder.termOrder <= coursesectsched.termOrder
	)
where courseRn = 1
),

/*****
BEGIN SECTION - Student Counts
This set of views is used to transform and aggregate records from MCR views above for unduplicated student count
*****/

CourseTypeCountsSTU as (
-- View used to break down course category type counts for student

--mod from v1 - Part A doesn't contain graduate student level value 99
--AF 20201124: mod from v3 - Part A doesn't contain non-degree-seeking values 7, 21

--Student level table (Part A)
--1 - Full-time, first-time degree/certificate-seeking undergraduate
--3 - Full-time, All other undergraduate
--15 - Part-time, first-time degree/certificate-seeking undergraduate
--17 - Part-time, All other undergraduate

--mod from v1 - Part C doesn't contain graduate student level value 3

--Student level table (Part C)
--1 - Degree/Certificate seeking undergraduate students
--2 - Non-Degree/Certificate seeking undergraduate Students

select yearType,
    surveySection,
    personId,    
    ipedsGender,
	ipedsEthnicity,
    DEStatus DEStatus,
    (case --when studentLevel = 'GR' then '99' 
         --when isNonDegreeSeeking = true and timeStatus = 'FT' then '7'
         --when isNonDegreeSeeking = true and timeStatus = 'PT' then '21'
         when studentLevel = 'UG' then 
            (case when studentType = 'First Time' and timeStatus = 'FT' and isNonDegreeSeeking = false then '1' 
                    --when studentType = 'Transfer' and timeStatus = 'FT' then '2'
                    --when studentType = 'Returning' and timeStatus = 'FT' then '3'
					when timeStatus = 'FT' then '3'
                    when studentType = 'First Time' and timeStatus = 'PT' and isNonDegreeSeeking = false then '15' 
                    --when studentType = 'Transfer' and timeStatus = 'PT' then '16'
                    --when studentType = 'Returning' and timeStatus = 'PT' then '17' 
					when timeStatus = 'PT' then '17' else '1' 
			 end)
        else null
    end) ipedsPartAStudentLevel, --only Undergrad (and Continuing Ed, who go in Undergrad) students are counted in headcount
    (case --when studentLevel = 'GR' then '3' 
         --when isNonDegreeSeeking = true then '2' --AF 20201124: mod from v3 - Part C doesn't contain student level value 2 and value 1 is now All undergrad students
         when studentLevel = 'UG' then '1' 
         else null
    end) ipedsPartCStudentLevel ----only Undergrad (and Continuing Ed, who go in Undergrad) students are counted in headcount
from ( 
    select yearType,
            surveySection,
            snapshotDate,
            censusDate,
            acadOrProgReporter,
            personId,
            (case when studentLevel = 'UG' or isNonDegreeSeeking = true then
                    (case when instructionalActivityType in ('CR', 'B') then 
                                (case when totalCreditHrsCalc >= requiredFTCreditHoursUG then 'FT' else 'PT' end)
                          when instructionalActivityType = 'CL' then 
                                (case when totalClockHrs >= requiredFTClockHoursUG then 'FT' else 'PT' end) 
                          else 'UG null' end)
                else null end) timeStatus,
            studentLevel,
            studentType,
            isNonDegreeSeeking,
            ipedsGender,
            ipedsEthnicity,
            (case when totalCredCourses > 0 --exclude students not enrolled for credit
                            then (case when totalESLCourses = totalCourses then 0 --exclude students enrolled only in ESL courses/programs
                                       --when totalCECourses = totalCourses then 0 --exclude students enrolled only in continuing ed courses
                                       when totalIntlCourses = totalCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
                                       when totalAuditCourses = totalCourses then 0 --exclude students exclusively auditing classes
                                       -- when... then 0 --exclude PHD residents or interns
                                       -- when... then 0 --students studying abroad if enrollment at home institution is an admin only record
                                       -- when... then 0 --exclude students in experimental Pell programs
                                       else 1
                                  end)
                         when totalRemCourses = totalCourses and isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
                         else 0 
                     end) ipedsInclude,
            (case when totalDECourses = totalCourses then 'DE Exclusively'
                          when totalDECourses > 0 then 'DE Some'
                          else 'DE None'
                    end) DEStatus
    from (
         select course.yearType yearType,
                course.surveySection surveySection,
                course.snapshotDate,
                course.censusDate censusDate,
                course.instructionalActivityType,
                course.acadOrProgReporter,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId personId,
                course.studentLevel,
                course.studentType,
                course.isNonDegreeSeeking,
                course.ipedsGender,
                course.ipedsEthnicity,
                sum((case when course.enrollmentHours >= 0 then 1 else 0 end)) totalCourses,
                sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 then course.enrollmentHours else 0 end)) totalCreditHrs,
                sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then coalesce(course.enrollmentHours, 0) else 0 end)) totalCreditUGHrs,
                sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel = 'Graduate' then coalesce(course.enrollmentHours, 0) else 0 end)) totalCreditGRHrs,
                sum((case when course.isClockHours = 1 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then course.enrollmentHours else 0 end)) totalClockHrs,
                sum((case when course.enrollmentHours = 0 then 1 else 0 end)) totalNonCredCourses,
                sum((case when course.enrollmentHours > 0 then 1 else 0 end)) totalCredCourses,
                sum((case when course.meetingType = 'Online/Distance Learning' then 1 else 0 end)) totalDECourses,
                sum((case when course.courseLevel = 'Undergrad' then 1 else 0 end)) totalUGCourses,
                sum((case when course.courseLevel = 'Graduate' then 1 else 0 end)) totalGRCourses,
                sum((case when course.courseLevel = 'Continuing Ed' then 1 else 0 end)) totalCECourses,
                sum((case when course.courseLevel = 'Occupational/Professional' then 1 else 0 end)) totalOccCourses,
                sum((case when course.isESL = 'Y' then 1 else 0 end)) totalESLCourses,
                sum((case when course.isRemedial = 'Y' then 1 else 0 end)) totalRemCourses,
                sum((case when course.isInternational = 1 then 1 else 0 end)) totalIntlCourses,
                sum((case when course.crnGradingMode = 'Audit' then 1 else 0 end)) totalAuditCourses,
                sum((case when course.courseLevel = 'Undergrad' then
                        (case when course.instructionalActivityType in ('CR', 'B') and course.isClockHours = 0 then course.enrollmentHours
                              when course.instructionalActivityType = 'B' and course.isClockHours = 1 then course.equivCRHRFactor * course.enrollmentHours
                              else 0 end)
                    else 0 end)) totalCreditHrsCalc
        from CourseMCR course
        group by course.yearType, course.surveySection, course.personId, course.financialAidYear, course.censusDate, course.requiredFTCreditHoursUG, course.requiredFTClockHoursUG, course.studentLevel, course.studentType, course.isNonDegreeSeeking, course.ipedsGender, course.ipedsEthnicity, course.snapshotDate, course.instructionalActivityType, course.acadOrProgReporter
        )
    )
where ipedsInclude = 1
),

/*****
BEGIN SECTION - Course Counts
This set of views is used to transform and aggregate records from MCR views above for creditHour counts by level
*****/

--mod from v1 - AcademicTrackMCR and DegreeMCR removed - do not need to report Doctor level
--mod form v1 - assign null to DPPCreditHours since no Doctor level

CourseTypeCountsCRN as (
-- View used to calculate credit hours by course level

select yearType yearType,
        sum(UGCreditHours) UGCreditHours,
        sum(UGClockHours) UGClockHours,
        sum(GRCreditHours) GRCreditHours,
        sum(DPPCreditHours) DPPCreditHours 
from (
            select distinct course.yearType yearType,
                    course.surveySection surveySection,
                    course.personId personId,
                    course.crn crn,
                    course.courseLevel courseLevel,
                    course.enrollmentHours enrollmentHours,
                    (case when course.isClockHours = 0 and course.courseLevel != 'Graduate' then coalesce(course.enrollmentHours, 0) else 0 end) UGCreditHours,
                    (case when course.isClockHours = 1 and course.courseLevel != 'Graduate' then coalesce(course.enrollmentHours, 0) else 0 end) UGClockHours,
                    (case when course.courseLevel = 'Graduate' then coalesce(course.enrollmentHours, 0) else 0 end) GRCreditHours,
                    null DPPCreditHours
            from CourseMCR course
             )
group by yearType
),

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/
	
FormatPartA as (
select *
from (
	VALUES
		(1), -- Full-time, first-time degree/certificate-seeking undergraduate
		(2), -- Full-time, transfer-in degree/certificate-seeking undergraduate
		(3), -- Full-time, continuing degree/certificate-seeking undergraduate
		(7), -- Full-time, non-degree/certificate-seeking undergraduate
		(15), -- Part-time, first-time degree/certificate-seeking undergraduate
		(16), -- Part-time, transfer-in degree/certificate-seeking undergraduate
		(17), -- Part-time, continuing degree/certificate-seeking undergraduate
		(21), -- Part-time, non-degree/certificate-seeking undergraduate
		(99) -- Total graduate
	) as studentLevel (ipedsLevel)
),

FormatPartC as (
select *
from (
	VALUES
		(1), -- Degree/Certificate seeking undergraduate students
		(2), -- Non-Degree/Certificate seeking undergraduate Students
		(3) -- Graduate students
	) as studentLevel (ipedsLevel)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

-- Part A: Unduplicated Count by Student Level, Gender, and Race/Ethnicity

-- ipedsStudentLevelPartA valid values - Student level table (Part A)
--1 - Full-time, first-time degree/certificate-seeking undergraduate
--3 - Full-time, All other undergraduate
--15 - Part-time, first-time degree/certificate-seeking undergraduate
--17 - Part-time, All other undergraduate

--mod from v1 - remove check of config.icOfferGraduateAwardLevel in filter, since no graduate is reported
--AF 20201124: mod from v3 - Part A doesn't contain non-degree-seeking values 7, 21

select 'A' part,
       ipedsPartAStudentLevel field1,  --1,2,3,7,15,16,17,21
       round(coalesce(sum((case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end))), 0) field2,  -- FYRACE01 - Nonresident alien - Men (1), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end))), 0) field3,  -- FYRACE02 - Nonresident alien - Women (2), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end))), 0) field4,  -- FYRACE25 - Hispanic/Latino - Men (25), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end))), 0) field5,  -- FYRACE26 - Hispanic/Latino - Women (26), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end))), 0) field6,  -- FYRACE27 - American Indian or Alaska Native - Men (27), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end))), 0) field7,  -- FYRACE28 - American Indian or Alaska Native - Women (28), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end))), 0) field8,  -- FYRACE29 - Asian - Men (29), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end))), 0) field9,  -- FYRACE30 - Asian - Women (30), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end))), 0) field10, -- FYRACE31 - Black or African American - Men (31), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end))), 0) field11, -- FYRACE32 - Black or African American - Women (32), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end))), 0) field12, -- FYRACE33 - Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end))), 0) field13, -- FYRACE34 - Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end))), 0) field14, -- FYRACE35 - White - Men (35), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end))), 0) field15, -- FYRACE36 - White - Women (36), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end))), 0) field16, -- FYRACE37 - Two or more races - Men (37), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end))), 0) field17, -- FYRACE38 - Two or more races - Women (38), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end))), 0) field18, -- FYRACE13 - Race and ethnicity unknown - Men (13), 0 to 999999
       round(coalesce(sum((case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end))), 0) field19  -- FYRACE14 - Race and ethnicity unknown - Women (14), 0 to 999999
from (
    select personId personId,
            ipedsPartAStudentLevel ipedsPartAStudentLevel,
            ipedsEthnicity ipedsEthnicity,
            ipedsGender ipedsGender
    from CourseTypeCountsSTU
    
    union

    select null, --personId
            studentLevel.ipedsLevel,
            null, --ipedsEthnicity
            null --ipedsGender
    from FormatPartA studentLevel
    )
where ipedsPartAStudentLevel in ('1', '3', '15', '17') --AF 20201124: added values '1', '3', '15', '17' to be included
group by ipedsPartAStudentLevel
    
   
union

-- Part C: 12-month Unduplicated Count - Distance Education Status

-- ipedsPartCStudentLevel valid values - Student level table (Part C)
--1 - Degree/Certificate seeking undergraduate students
--2 - Non-Degree/Certificate seeking undergraduate Students
--3 - Graduate students

--mod from v1 - remove check of config.icOfferGraduateAwardLevel in filter, since no graduate is reported
--AF 20201124: mod from v3 - Part C doesn't contain student level value 2 and value 1 is now All undergrad students

select 'C' part,
       ipedsPartCStudentLevel field1,
       round(coalesce(sum((case when DEStatus = 'DE Exclusively' then 1 else 0 end))), 0) field2,  --Enrolled exclusively in distance education courses
       round(coalesce(sum((case when DEStatus = 'DE Some' then 1 else 0 end))), 0) field3,  --Enrolled in at least one but not all distance education courses
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
from (
    select personId personId,
            ipedsPartCStudentLevel ipedsPartCStudentLevel,
            DEStatus DEStatus
    from CourseTypeCountsSTU
    where DEStatus != 'DE None'
    
    union

    select null, --personId
            studentLevel.ipedsLevel,
            null --DEStatus
    from FormatPartC studentLevel
    )
where ipedsPartCStudentLevel ='1'
group by ipedsPartCStudentLevel

union

-- Part B: Instructional Activity

--mod from v1 - fields4 and 5 set to null - remove Graduate and higher levels

select 'B' part,
       null field1,
       round((case when config.icOfferUndergradAwardLevel = 'Y' and config.instructionalActivityType != 'CL' then coalesce(UGCreditHours, 0) 
            else null 
        end)) field2, -- CREDHRSU - credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.
        round((case when config.icOfferUndergradAwardLevel = 'Y' and config.instructionalActivityType != 'CR' then coalesce(UGClockHours, 0) 
            else null 
        end)) field3, -- CONTHRS  - clock hour instructional activity at the undergraduate level, 0 to 9999999, blank = not applicable, if no undergraduate programs are measured in clock hours.
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
from CourseTypeCountsCRN
    cross join (select first(icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
							first(icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
							first(icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,
							first(tmAnnualDPPCreditHoursFTE) tmAnnualDPPCreditHoursFTE,
							first(instructionalActivityType) instructionalActivityType
					  from ClientConfigMCR) config
					  
union 

-- If no records are returned, print the minimum necessary for the survey
-- Parts A and C will always print at least one record due to the union with formatting views

select *
from (
    VALUES
        ('B', null, 0, null, 0, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
    ) as dummySet(part, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11,
									field12, field13, field14, field15, field16, field17, field18, field19)
where not exists (select a.yearType from CourseTypeCountsCRN a) 
