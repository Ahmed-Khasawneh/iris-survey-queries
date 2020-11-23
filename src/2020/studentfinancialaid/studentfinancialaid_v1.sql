/********************

EVI PRODUCT:    DORIS 2020-21 IPEDS Survey Winter Collection
FILE NAME:      Student Financial Aid v1 (SFA)
FILE DESC:      Student Financial Aid for public institutions reporting on a fall cohort (academic reporters)
AUTHOR:         Ahmed Khasawneh
CREATED:        20201110

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Cohort Creation
Formatting Views
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)   Author             	Tag             	Comments
----------- 	--------------------	-------------   	-------------------------------------------------
20201120        jhanicak                                    Update to most current views and formatting; 
                                                            Fixes including filter for First Time students 
                                                            Added financialAidEndDate and changed repPeriodTag2 value
                                                               to Dod to accommodate pulling financial aid for year
                                                            PF-1771 Run time: prod 16m 45s  test (all years) 26m 54s 
20201110    	akhasawneh 									Initial version prod run 21m 34s test run 18m 48s

	
Snapshot tag requirements:

Multiple snapshots - one for each term/part of term combination:
Fall Census - enrollment cohort
Pre-Fall Summer Census - check for studentType of 'First Time' or 'Transfe'r if Fall studentType is 'Continuing'

*If client is reporting the prior year and/or second prior year numbers, there could be up to 3 years of Fall and 
Summer census snapshots.

One snapshot of each:
GI Bill - end of GI Bill reporting date
Department of Defense - end of DoD reporting date

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

--jh 20201120 changed value of repPeriodTag2 and created new financialAidEndDate field
--				to use for FinancialAidMCR in order to get full financial aid year data;
--				corrected dates in prod censusDates; added updated formatting

--prod default blocks (2)
select '2021' surveyYear, 
	'SFA' surveyId,
	'Fall Census' repPeriodTag1,
	'Department of Defense' repPeriodTag2,
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
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
	CAST('2019-07-01' as DATE) giBillStartDate,
    CAST('2020-06-30' as DATE) giBillEndDate,
    CAST('2019-10-01' as DATE) dodStartDate,
    CAST('2020-09-30' as DATE) dodEndDate,
    CAST('2020-09-30' as DATE) financialAidEndDate,
    '' sfaLargestProgCIPC, --'CIPC (no dashes, just numeric characters); Default value (if no record): null'
    'N' sfaGradStudentsOnly, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportPriorYear, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportSecondPriorYear --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
--***** end survey-specific mods

union

select '2021' surveyYear, 
	'SFA' surveyId,
	'Fall Census' repPeriodTag1,
	'Department of Defense' repPeriodTag2,
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
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
	CAST('2019-07-01' as DATE) giBillStartDate,
    CAST('2020-06-30' as DATE) giBillEndDate,
    CAST('2019-10-01' as DATE) dodStartDate,
    CAST('2020-09-30' as DATE) dodEndDate,
    CAST('2020-09-30' as DATE) financialAidEndDate,
    '' sfaLargestProgCIPC, --'CIPC (no dashes, just numeric characters); Default value (if no record): null'
    'N' sfaGradStudentsOnly, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportPriorYear, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportSecondPriorYear --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
--***** end survey-specific mods

/*
--testing default blocks (2)
select '1415' surveyYear,  
	'SFA' surveyId, 
	'Fall Census' repPeriodTag1,
	'Department of Defense' repPeriodTag2,
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
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
    CAST('2013-07-01' as DATE) giBillStartDate,
    CAST('2014-06-30' as DATE) giBillEndDate,
    CAST('2013-10-01' as DATE) dodStartDate,
    CAST('2014-09-30' as DATE) dodEndDate,
    CAST('2014-09-30' as DATE) financialAidEndDate,
    '' sfaLargestProgCIPC, --'CIPC (no dashes, just numeric characters); Default value (if no record): null'
    'N' sfaGradStudentsOnly, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportPriorYear, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportSecondPriorYear --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
--***** end survey-specific mods

union

select '1415' surveyYear,  
	'SFA' surveyId, 
	'Fall Census' repPeriodTag1,
	'Department of Defense' repPeriodTag2,
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
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
    CAST('2013-07-01' as DATE) giBillStartDate,
    CAST('2014-06-30' as DATE) giBillEndDate,
    CAST('2013-10-01' as DATE) dodStartDate,
    CAST('2014-09-30' as DATE) dodEndDate,
    CAST('2014-09-30' as DATE) financialAidEndDate,
    '' sfaLargestProgCIPC, --'CIPC (no dashes, just numeric characters); Default value (if no record): null'
    'N' sfaGradStudentsOnly, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportPriorYear, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportSecondPriorYear --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
--***** end survey-specific mods
*/
),

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 

--  1st union 1st order - pull snapshot for defvalues.repPeriodTag1 
--  1st union 2nd order - pull snapshot for defvalues.repPeriodTag2
--  1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--  2nd union - pull default values if no record in IPEDSReportingPeriod

--jh 20201120 set default value of surveySection field for surveys like Completions that do not have section values;
--				added repPeriodTag fields to capture values for other views, as needed

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
	    and repPeriodENT.surveySection not in ('GI BILL', 'DEPT OF DEFENSE')
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
--          1st union 1st order - pull snapshot for 'Full Year Term End' 
--          1st union 2nd order - pull snapshot for 'Full Year June End'
--          1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--          2nd union - pull default values if no record in IPEDSClientConfig

--jh 20201120 added repPeriodTag fields to capture values for other views, as needed

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
    ConfigLatest.giBillStartDate giBillStartDate,
    ConfigLatest.giBillEndDate giBillEndDate,
    ConfigLatest.dodStartDate dodStartDate,
    ConfigLatest.dodEndDate dodEndDate,
    ConfigLatest.financialAidEndDate financialAidEndDate,
    upper(ConfigLatest.sfaLargestProgramCIPC) sfaLargestProgramCIPC,
    upper(ConfigLatest.sfaGradStudentsOnly) sfaGradStudentsOnly,
    upper(ConfigLatest.sfaReportPriorYear) sfaReportPriorYear, --'N' sfaReportPriorYear, --
	upper(ConfigLatest.sfaReportSecondPriorYear) sfaReportSecondPriorYear --'N' sfaReportSecondPriorYear --
--***** end survey-specific mods
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'IPEDSClientConfig' source,
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
		defvalues.giBillStartDate giBillStartDate,
		defvalues.giBillEndDate giBillEndDate,
        defvalues.dodStartDate dodStartDate,
        defvalues.dodEndDate dodEndDate,
        defvalues.financialAidEndDate financialAidEndDate,
        coalesce(clientConfigENT.sfaLargestProgCIPC, defvalues.sfaLargestProgCIPC) sfaLargestProgramCIPC,
        coalesce(clientConfigENT.sfaGradStudentsOnly, defvalues.sfaGradStudentsOnly) sfaGradStudentsOnly,
        coalesce(clientConfigENT.sfaReportPriorYear, defvalues.sfaReportPriorYear) sfaReportPriorYear,
        coalesce(clientConfigENT.sfaReportSecondPriorYear, defvalues.sfaReportSecondPriorYear) sfaReportSecondPriorYear,
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
	    'DefaultValues' source,
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
		defvalues.giBillStartDate giBillStartDate,
		defvalues.giBillEndDate giBillEndDate,
        defvalues.dodStartDate dodStartDate,
        defvalues.dodEndDate dodEndDate,
        defvalues.financialAidEndDate financialAidEndDate,
        defvalues.sfaLargestProgCIPC sfaLargestProgramCIPC,
        defvalues.sfaGradStudentsOnly sfaGradStudentsOnly,
        defvalues.sfaReportPriorYear sfaReportPriorYear,
        defvalues.sfaReportSecondPriorYear sfaReportSecondPriorYear,
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

--jh 20201120 Added new fields to capture min start and max end date for terms;
--				changed order by for row_number to censusDate

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

--jh 20201120 Reordered fields for consistency; removed indicators not needed for course views; removed excess fields

select coalesce(repPerTerms.yearType, 'CY') yearType,
        repPerTerms.surveySection surveySection,
        repPerTerms.termCode termCode,
        repPerTerms.partOfTermCode partOfTermCode,
        repPerTerms.financialAidYear financialAidYear,
        repPerTerms.termOrder termOrder,
        repPerTerms.maxCensus maxCensus,
        coalesce(repPerTerms.acadTermSSDate, repPerTerms.repPeriodSSDate) snapshotDate,
        repPerTerms.reportingDateStart reportingDateStart,
        repPerTerms.reportingDateEnd reportingDateEnd,
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
	    repPerTerms.equivCRHRFactor equivCRHRFactor,
        (case when repPerTerms.termClassification = 'Standard Length' then 1
             when repPerTerms.termClassification is null then (case when repPerTerms.termType in ('Fall', 'Spring') then 1 else 2 end)
             else 2
        end) fullTermOrder
from ( 
select distinct repperiod.surveySection surveySection,
        repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        acadterm.financialAidYear financialAidYear,
        acadterm.snapshotDate acadTermSSDate,
        repperiod.snapshotDate repPeriodSSDate,
        repperiod.reportingDateStart reportingDateStart,
        repperiod.reportingDateEnd reportingDateEnd,
        acadterm.tags tags,
        (case when repperiod.surveySection in ('COHORT', 'PRIOR SUMMER') then 'CY'
              when repperiod.surveySection in ('PRIOR YEAR 1', 'PRIOR YEAR 1 PRIOR SUMMER') then 'PY1'
              when repperiod.surveySection in ('PRIOR YEAR 2', 'PRIOR YEAR 2 PRIOR SUMMER') then 'PY2'
              end) yearType,
        coalesce(acadterm.censusDate, repperiod.censusDate) censusDate,
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
	    coalesce(acadterm.requiredFTCreditHoursUG/
		    coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
		row_number() over (
            partition by 
                repperiod.termCode,
                repperiod.partOfTermCode
            order by
                (case when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') 
                            and ((array_contains(acadterm.tags, 'Fall Census') and acadterm.termType = 'Fall' and repperiod.surveySection in ('COHORT', 'PRIOR YEAR 1', 'PRIOR YEAR 2'))
                                or (array_contains(acadterm.tags, 'Pre-Fall Summer Census') and acadterm.termType = 'Summer' and repperiod.surveySection in ('PRIOR SUMMER', 'PRIOR YEAR 1 PRIOR SUMMER', 'PRIOR YEAR 2 PRIOR SUMMER'))) then 1
                      when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') then 2
                     else 3 end) asc,
                (case when acadterm.snapshotDate < acadterm.censusDate then acadterm.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                (case when acadterm.snapshotDate > acadterm.censusDate then acadterm.snapshotDate else CAST('9999-09-09' as DATE) end) asc
            ) acadTermRnReg
    from ReportingPeriodMCR repperiod 
        left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
	            and repperiod.partOfTermCode = acadterm.partOfTermCode
		left join AcademicTermOrder termorder on termOrder.termCode = repperiod.termCode
		inner join ClientConfigMCR clientconfig on repperiod.surveyYear = clientconfig.surveyYear
where ((clientconfig.sfaReportPriorYear = 'N' and clientconfig.sfaReportSecondPriorYear = 'N'
			            and upper(repperiod.surveySection) in ('COHORT', 'PRIOR SUMMER'))
		    or (clientconfig.sfaReportPriorYear = 'Y' and clientconfig.sfaReportSecondPriorYear = 'N'
			            and upper(repperiod.surveySection) in ('PRIOR YEAR 1', 'PRIOR YEAR 1 PRIOR SUMMER', 'COHORT', 'PRIOR SUMMER'))
			or (clientconfig.sfaReportPriorYear = 'N' and clientconfig.sfaReportSecondPriorYear = 'Y'
			            and upper(repperiod.surveySection) in ('PRIOR YEAR 2', 'PRIOR YEAR 2 PRIOR SUMMER', 'COHORT', 'PRIOR SUMMER'))
			or (clientconfig.sfaReportPriorYear = 'Y' and clientconfig.sfaReportSecondPriorYear = 'Y'
			            and upper(repperiod.surveySection) in ('PRIOR YEAR 1', 'PRIOR YEAR 1 PRIOR SUMMER', 'PRIOR YEAR 2', 'PRIOR YEAR 2 PRIOR SUMMER', 'COHORT', 'PRIOR SUMMER')))
		) repPerTerms
where repPerTerms.acadTermRnReg = 1 
),

AcademicTermReportingRefactor as (
--Returns all records from AcademicTermReporting, converts Summer terms to Pre-Fall or Post-Spring and creates reportingDateStart/End

--jh 20201120 Removed reportingDateStart and End fields, since they were added to AcademicTermOrder

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
		and ((to_date(campusENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
			and to_date(campusENT.recordActivityDate,'YYYY-MM-DD') <= to_date(campusENT.snapshotDate,'YYYY-MM-DD'))
				or to_date(campusENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
	)
where campusRn = 1
),

RegistrationMCR as ( 
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 

--jh 20201120 Reordered fields for consistency and accuracy; other minor mods

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

--jh 20201120 Reordered fields for consistency and accuracy; other minor mods

select stuData.yearType,
        stuData.surveySection,
        stuData.snapshotDate,
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
--Drop surveySection from select fields and use yearType only going forward - Prior Summer sections only used to determine student type

--jh 20201120 Removed surveySection field; removed duplicate filter on campus.isInternational; reordered fields for consistency and accuracy; other minor mods

select *
from ( 
select stu.yearType yearType,
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
        coalesce(stu.residency, 'In District') residency,
        coalesce(campus.isInternational, false) isInternational,        
	    row_number() over (
                partition by
                    stu.yearType,
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
            max(residency) residency,
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
                (case when FFTRn = 1 then residency else null end) residency,
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
                    residency,
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
        and acadTermCode.yearType = stu.yearType
    left join CampusMCR campus on stu.campus = campus.campus
    )
where regCampRn = 1 
    and isInternational = false
    and studentLevel = 'UG'
),

CourseSectionMCR as (
--Included to get enrollment hours of a CRN

--jh 20201120 Removed surveySection field; reordered fields for consistency and accuracy; other minor mods
    
select *
from (
    select stu.yearType,
        reg.snapshotDate snapshotDate,
        stu.snapshotDate stuSSD,
        to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') courseSectionSSD,
        reg.termCode,
        reg.partOfTermCode,
        reg.financialAidYear,
        reg.censusDate,
        reg.termType,
        reg.termOrder,
        reg.requiredFTCreditHoursUG,
	    reg.requiredFTClockHoursUG,
	    reg.instructionalActivityType,
	    stu.personId personId,
        stu.studentLevel,
	    stu.studentType,
	    stu.isNonDegreeSeeking,
	    stu.residency,
--***** start survey-specific mods - SFA doesn't report ethnicity and gender
	    null ipedsGender,
	    null ipedsEthnicity,
--***** end survey-specific mods
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
--***** start survey-specific mods - join on StudentRefactor since SFA doesn't use Person
        inner join StudentRefactor stu on stu.personId = reg.personId
            and stu.firstFullTerm = reg.termCode
--***** end survey-specific mods
            and reg.yearType = stu.yearType
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

--jh 20201120 Removed surveySection field; reordered fields for consistency and accuracy; other minor mods

select *
from (
	select coursesect.yearType yearType,
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
        coursesect.personId personId,
	    coursesect.studentLevel,
	    coursesect.studentType,
	    coursesect.isNonDegreeSeeking,
	    coursesect.ipedsGender,
	    coursesect.ipedsEthnicity,
	    coursesect.residency,
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

--jh 20201120 Removed surveySection field; reordered fields for consistency and accuracy; other minor mods

select *
from (
	select coursesectsched.yearType yearType,
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
        coursesectsched.personId personId,
	    coursesectsched.studentType,
	    coursesectsched.studentLevel,
	    coursesectsched.isNonDegreeSeeking,
	    coursesectsched.ipedsGender,
	    coursesectsched.ipedsEthnicity,
	    coursesectsched.residency,
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

CourseTypeCountsSTU as (
-- View used to break down course category type counts for student

--jh 20201120 Added isGroup2Ind field; changed timeStatus block to remove duplicate studentLevel filter (included in StudentRefactor) 
--				and add the studentType filter; reordered fields for consistency and accuracy; other minor mods

select *,
        (case when timeStatus = 'FT' and isNonDegreeSeeking = 0 and studentType = 'First Time' then 1 else 0 end) isGroup2Ind
from (
    select yearType,
            snapshotDate,
            censusDate,
            financialAidYear,
            personId,
            (case when studentType = 'First Time' and isNonDegreeSeeking = 0 then
                    (case when instructionalActivityType in ('CR', 'B') then 
                                (case when totalCreditHrsCalc >= requiredFTCreditHoursUG then 'FT' else 'PT' end)
                          when instructionalActivityType = 'CL' then 
                                (case when totalClockHrs >= requiredFTClockHoursUG then 'FT' else 'PT' end) 
                          else 'UG null' end)
                else null end) timeStatus,
            studentLevel,
            studentType,
            isNonDegreeSeeking,
            residency,
            ipedsGender,
            ipedsEthnicity,
            residency,
            (case when totalCredCourses > 0 --exclude students not enrolled for credit
                            then (case when totalESLCourses = totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
                                       --when totalCECourses = totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
                                       when totalIntlCourses = totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
                                       when totalAuditCourses = totalCredCourses then 0 --exclude students exclusively auditing classes
                                       -- when... then 0 --exclude PHD residents or interns
                                       -- when... then 0 --students studying abroad if enrollment at home institution is an admin only record
                                       -- when... then 0 --exclude students in experimental Pell programs
                                       else 1
                                  end)
                  when totalRemCourses = totalCourses and isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
                  else 0 
             end) ipedsInclude
    from (
         select course.yearType,
                course.snapshotDate,
                course.censusDate,
                course.financialAidYear,
                course.instructionalActivityType,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId,
                course.studentLevel,
                course.studentType,
                course.isNonDegreeSeeking,
                course.residency,
                course.ipedsGender,
                course.ipedsEthnicity,
	            course.residency,
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
        group by course.yearType,
                course.snapshotDate,
                course.censusDate,
                course.financialAidYear,
                course.instructionalActivityType,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId,
                course.studentLevel,
                course.studentType,
                course.isNonDegreeSeeking,
                course.residency,
                course.ipedsGender,
                course.ipedsEthnicity,
	            course.residency
        )
    )
where ipedsInclude = 1
),

FinancialAidMCR as (
-- included to get student Financial aid information paid any time during the academic year.
-- Report grant or scholarship aid that was awarded to students. 
-- Report loans that were awarded to and accepted by the student.
-- For public institutions, include only those students paying the in-state or in-district tuition rate. For program reporters, include only those students enrolled in the institution’s largest program.

--jh 20201120 Extended valid recordActivityDate by using the financialAidEndDate (set in DefaultValues) in order to get full financial aid year records;
--				prioritized using the snapshot for Dept of Defense in order to get full financial aid year records;
-- 				used isGroup2Ind (defined in CourseTypeCountsSTU) to conditionally populate the other group2 fields

select *,
    isGroup2Ind isGroup2,
    (case when isGroup2Ind = 1 and group2aTotal > 0 then 1 else 0 end) isGroup2a,
    (case when isGroup2Ind = 1 and group2bTotal > 0 then 1 else 0 end) isGroup2b,
    (case when isGroup2Ind = 1 and group3Total > 0 then 1 else 0 end) isGroup3,
    (case when isGroup2Ind = 1 and group4Total > 0 then 1 else 0 end) isGroup4 
from (
select course2.personId personId,
        course2.yearType yearType,
        course2.financialAidYear financialAidYear,
        course2.isGroup2Ind isGroup2Ind,
        first(finaid.livingArrangement) livingArrangement,
        course2.residency residency,
        round(sum(coalesce(case when finaid.fundType = 'Loan' and finaid.fundSource = 'Federal' then finaid.IPEDSFinancialAidAmount end, 0)), 0) federalLoan,
        round(sum(coalesce(case when finaid.fundType in ('Grant', 'Scholarship') and finaid.fundSource = 'Federal' then finaid.IPEDSFinancialAidAmount end, 0)), 0) federalGrantSchol,
        round(sum(coalesce(case when finaid.fundType = 'Work Study' and finaid.fundSource = 'Federal' then finaid.IPEDSFinancialAidAmount end, 0)), 0) federalWorkStudy,
        round(sum(coalesce(case when finaid.fundType = 'Loan' and finaid.fundSource in ('State', 'Local') then finaid.IPEDSFinancialAidAmount end, 0)), 0) stateLocalLoan,
        round(sum(coalesce(case when finaid.fundType in ('Grant', 'Scholarship') and finaid.fundSource in ('State', 'Local') then finaid.IPEDSFinancialAidAmount end, 0)), 0) stateLocalGrantSchol,
        round(sum(coalesce(case when finaid.fundType = 'Work Study' and finaid.fundSource in ('State', 'Local') then finaid.IPEDSFinancialAidAmount end, 0)), 0) stateLocalWorkStudy,
        round(sum(coalesce(case when finaid.fundType = 'Loan' and finaid.fundSource = 'Institution' then finaid.IPEDSFinancialAidAmount end, 0)), 0) institutionLoan,
        round(sum(coalesce(case when finaid.fundType in ('Grant', 'Scholarship') and finaid.fundSource = 'Institution' then finaid.IPEDSFinancialAidAmount end, 0)), 0) institutionGrantSchol,
        round(sum(coalesce(case when finaid.fundType = 'Work Study' and finaid.fundSource = 'Institution' then finaid.IPEDSFinancialAidAmount end, 0)), 0) institutionalWorkStudy,        
        round(sum(coalesce(case when finaid.fundType = 'Loan' and finaid.fundSource = 'Other' then finaid.IPEDSFinancialAidAmount end, 0)), 0) otherLoan, 
        round(sum(coalesce(case when finaid.fundType in ('Grant', 'Scholarship') and finaid.fundSource = 'Other' then finaid.IPEDSFinancialAidAmount end, 0)), 0) otherGrantSchol, 
        round(sum(coalesce(case when finaid.fundType = 'Work Study' and finaid.fundSource = 'Other' then finaid.IPEDSFinancialAidAmount end, 0)), 0) otherWorkStudy, 
        round(sum(coalesce(case when finaid.isPellGrant = 1 then finaid.IPEDSFinancialAidAmount end, 0)), 0) pellGrant, 
        round(sum(coalesce(case when finaid.isTitleIV = 1 then finaid.IPEDSFinancialAidAmount end, 0)), 0) titleIV, 
        round(sum(coalesce(case when finaid.fundType in ('Grant', 'Scholarship') then finaid.IPEDSFinancialAidAmount end, 0)), 0) allGrantSchol, 
        round(sum(coalesce(case when finaid.fundType = 'Loan' then finaid.IPEDSFinancialAidAmount end, 0)), 0) allLoan, 
        round(sum(coalesce(case when finaid.fundType in ('Grant', 'Scholarship') and finaid.fundSource = 'Federal' and finaid.isPellGrant != 1 then finaid.IPEDSFinancialAidAmount end, 0)), 0) nonPellFederalGrantSchol, 
        round(sum(finaid.IPEDSFinancialAidAmount), 0) totalAid,
        round(sum(case when course2.isGroup2Ind = 1 then finaid.IPEDSFinancialAidAmount end)) group2aTotal,
		round(sum(case when course2.isGroup2Ind = 1 and finaid.fundType in ('Loan', 'Grant', 'Scholarship') and finaid.fundSource in ('Federal', 'State', 'Local', 'Institution') then finaid.IPEDSFinancialAidAmount end)) group2bTotal,
        round(sum(case when course2.isGroup2Ind = 1 and finaid.fundType in ('Grant', 'Scholarship') and finaid.fundSource in ('Federal', 'State', 'Local', 'Institution') then finaid.IPEDSFinancialAidAmount end)) group3Total,
        round(sum(case when course2.isGroup2Ind = 1 and finaid.isTitleIV = 1 then finaid.IPEDSFinancialAidAmount end)) group4Total,
        (case when course2.isGroup2Ind = 1 then (case when first(finaid.familyIncome) <= 30000 then 1
		                                            when first(finaid.familyIncome) between 30001 and 48000 then 2
		                                            when first(finaid.familyIncome) between 48001 and 75000 then 3
		                                            when first(finaid.familyIncome) between 75001 and 110000 then 4
		                                            when first(finaid.familyIncome) > 110000 then 5
	                                                else 1 end)
	       end) familyIncome
from CourseTypeCountsSTU course2
    left join (    
        select DISTINCT
            course.personId personId,
            course.yearType yearType,
            course.financialAidYear financialAidYear,
            FinancialAidENT.fundType fundType,
            FinancialAidENT.fundCode fundCode,
            FinancialAidENT.fundSource fundSource,
            FinancialAidENT.recordActivityDate recordActivity,
            FinancialAidENT.termCode termCode,
            FinancialAidENT.awardStatus awardStatus,
            FinancialAidENT.isPellGrant isPellGrant,
            FinancialAidENT.isTitleIV isTitleIV,
            FinancialAidENT.isSubsidizedDirectLoan isSubsidizedDirectLoan,
            FinancialAidENT.acceptedAmount acceptedAmount,
            FinancialAidENT.offeredAmount offeredAmount,
            FinancialAidENT.paidAmount paidAmount,
            (case when FinancialAidENT.IPEDSFinancialAidAmount is not null and FinancialAidENT.IPEDSFinancialAidAmount > 0 then FinancialAidENT.IPEDSFinancialAidAmount
                 else (case when FinancialAidENT.fundType = 'Loan' then FinancialAidENT.acceptedAmount
                        when FinancialAidENT.fundType in ('Grant', 'Scholarship') then FinancialAidENT.offeredAmount
                        when FinancialAidENT.fundType = 'Work Study' then FinancialAidENT.paidAmount
                        else FinancialAidENT.IPEDSFinancialAidAmount end)
            end) IPEDSFinancialAidAmount, 
            FinancialAidENT.IPEDSOutcomeMeasuresAmount IPEDSOutcomeMeasuresAmount,
            round(regexp_replace(FinancialAidENT.familyIncome, ',', ''), 0) familyIncome,
            FinancialAidENT.livingArrangement livingArrangement,
            FinancialAidENT.isIPEDSReportable isIPEDSReportable,
            row_number() over (
                partition by
                     course.yearType,
                    FinancialAidENT.financialAidYear,
                    course.personId,
			        FinancialAidENT.fundCode,
			        FinancialAidENT.fundType,
			        FinancialAidENT.fundSource
		        order by 		    
                    (case when array_contains(FinancialAidENT.tags, config.repPeriodTag2) then 1
                          when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.financialAidEndDate, 1) and date_add(config.financialAidEndDate, 3) then 2 else 3 end) asc,
                    (case when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') < config.financialAidEndDate then to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    (case when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') > config.financialAidEndDate then to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    FinancialAidENT.recordActivityDate desc
            ) finAidRn
        from CourseTypeCountsSTU course   
        cross join (select first(financialAidEndDate) financialAidEndDate,
                            first(repPeriodTag2) repPeriodTag2
                    from ClientConfigMCR) config
        inner join FinancialAid FinancialAidENT on course.personId = FinancialAidENT.personId
	        and course.financialAidYear = FinancialAidENT.financialAidYear
		    and FinancialAidENT.isIpedsReportable = 1
		    and ((to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
			    and to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD') <= config.financialAidEndDate
                and FinancialAidENT.awardStatus not in ('Source Declined', 'Cancelled'))
				    or to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
        ) finaid on course2.personId = finaid.personId
	        and course2.financialAidYear = finaid.financialAidYear
            and finaid.finAidRn = 1
group by course2.personId, 
        course2.yearType, 
        course2.financialAidYear,
        course2.isGroup2Ind,
        course2.residency
    )
),

MilitaryBenefitMCR as (
-- Returns GI Bill and Dept of Defense military benefits
-- do absolute value on amount or note in the ingestion query, since source records could be stored as debits or credits

--GI Bill
select personId, 
        benefitType, 
        termCode,
        sum(benefitAmount) benefitAmount,
        snapshotDate,
        StartDate, 
        EndDate
from ( 
    select distinct MilitaryBenefitENT.personID personID,
        MilitaryBenefitENT.termCode termCode,
        MilitaryBenefitENT.benefitType benefitType,
        abs(MilitaryBenefitENT.benefitAmount) benefitAmount,
		to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		to_date(MilitaryBenefitENT.transactionDate, 'YYYY-MM-DD') transactionDate,
        MilitaryBenefitENT.tags tags,
        config.giBillStartDate StartDate,
        config.giBillEndDate EndDate,
        to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
        row_number() over (
            partition by
                MilitaryBenefitENT.personId,
			    MilitaryBenefitENT.benefitType,
			    MilitaryBenefitENT.termCode,
			    MilitaryBenefitENT.transactionDate,
			    MilitaryBenefitENT.benefitAmount
		    order by
				(case when array_contains(MilitaryBenefitENT.tags, 'GI Bill') and MilitaryBenefitENT.benefitType = 'GI Bill'
				            and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.giBillStartDate, 1) and date_add(config.giBillEndDate, 3) then 1
				      else 2 end) asc,
				(case when MilitaryBenefitENT.benefitType = 'GI Bill' and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.giBillStartDate, 1) and date_add(config.giBillEndDate, 3) then 3 
				    else 4 end) asc,
                (case when MilitaryBenefitENT.benefitType = 'GI Bill' and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') < config.giBillStartDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                (case when MilitaryBenefitENT.benefitType = 'GI Bill' and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') > config.giBillEndDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                MilitaryBenefitENT.recordActivityDate desc
	    ) militarybenefitRn
    from MilitaryBenefit MilitaryBenefitENT
        cross join (select giBillStartDate,
                            giBillEndDate
                    from ClientConfigMCR) config
    where MilitaryBenefitENT.isIPEDSReportable = 1 
        and ((to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
            and (MilitaryBenefitENT.benefitType = 'GI Bill'
                    and to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') between config.giBillStartDate and config.giBillEndDate
                    and MilitaryBenefitENT.transactionDate between config.giBillStartDate and config.giBillEndDate
                ))
            or (to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                    and (MilitaryBenefitENT.benefitType = 'GI Bill'
                        and MilitaryBenefitENT.transactionDate between config.giBillStartDate and config.giBillEndDate
                      )))
    )   
    where militarybenefitRn = 1
group by personId, benefitType, termCode, snapshotDate, StartDate, EndDate --, recordActivityDate

union

--Dept of Defense
select personId, 
        benefitType, 
        termCode,
        sum(benefitAmount) benefitAmount,
        snapshotDate, 
        StartDate, 
        EndDate
from (
    select distinct MilitaryBenefitENT.personID personID,
        MilitaryBenefitENT.termCode termCode,
        MilitaryBenefitENT.benefitType benefitType,
        abs(MilitaryBenefitENT.benefitAmount) benefitAmount,
		to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		to_date(MilitaryBenefitENT.transactionDate, 'YYYY-MM-DD') transactionDate,
        MilitaryBenefitENT.tags tags,
        config.dodStartDate StartDate,
        config.dodEndDate EndDate,
        to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
        row_number() over (
            partition by
                MilitaryBenefitENT.personId,
			    MilitaryBenefitENT.benefitType,
			    MilitaryBenefitENT.termCode,
			    MilitaryBenefitENT.transactionDate,
			    MilitaryBenefitENT.benefitAmount
		    order by
				(case when array_contains(MilitaryBenefitENT.tags, 'Department of Defense') and MilitaryBenefitENT.benefitType = 'Dept of Defense'
				            and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.dodStartDate, 1) and date_add(config.dodEndDate, 3) then 1
				      else 2 end) asc,
				(case when MilitaryBenefitENT.benefitType = 'Dept of Defense' and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.dodStartDate, 1) and date_add(config.dodEndDate, 3) then 3 
				    else 4 end) asc,
                (case when MilitaryBenefitENT.benefitType = 'Dept of Defense' and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') < config.dodStartDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                (case when MilitaryBenefitENT.benefitType = 'Dept of Defense' and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') > config.dodEndDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                MilitaryBenefitENT.recordActivityDate desc
	    ) militarybenefitRn
    from MilitaryBenefit MilitaryBenefitENT
        cross join (select dodStartDate,
                            dodEndDate
                    from ClientConfigMCR) config
    where MilitaryBenefitENT.isIPEDSReportable = 1
        and ((to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
            and (MilitaryBenefitENT.benefitType = 'Dept of Defense'
                    and to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') between config.dodStartDate and config.dodEndDate
                    and MilitaryBenefitENT.transactionDate between config.dodStartDate and config.dodEndDate
                ))
            or (to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                    and (MilitaryBenefitENT.benefitType = 'Dept of Defense'
                        and MilitaryBenefitENT.transactionDate between config.dodStartDate and config.dodEndDate
                      )))
    )   
where militarybenefitRn = 1
group by personId, benefitType, termCode, snapshotDate, StartDate, EndDate --, recordActivityDate 
),

MilitaryStuLevel as (
--Returns level of student receiving military benefit at time of reporting end date

select count(personId) recCount,
        sum(giCount) giCount,
        sum(giBillAmt) giBillAmt,
        sum(dodCount) dodCount,
        sum(dodAmt) dodAmt,
        studentLevel
from ( 
    select stu.personId personId,
        sum((case when stu.benefitType = 'GI Bill' then stu.benefitAmount else 0 end)) giBillAmt,
        sum((case when stu.benefitType = 'Dept of Defense' then stu.benefitAmount else 0 end)) dodAmt,
        max(stu.studentLevel) studentLevel,
        (case when stu.benefitType = 'GI Bill' then 1 else 0 end) giCount,
        (case when stu.benefitType = 'Dept of Defense' then 1 else 0 end) dodCount
    from (
        select stud.personId,
            stud.termCode,
            coalesce(stud.studentLevel, (case when config.icOfferUndergradAwardLevel = 'Y' then 'Undergrad' else 'Graduate' end)) studentLevel,
            stud.benefitType,
            stud.benefitAmount,
            row_number() over (
                    partition by
                        stud.personId,
                        stud.benefitType,
                        stud.benefitAmount
                    order by
                        stud.termOrder desc
                ) termRn
        from (
            select distinct miliben.personId personId,
                miliben.termCode termCode,
                termorder.termOrder termOrder,
                studentENT.studentLevel studentLevel,
                miliben.benefitType,
                miliben.benefitAmount benefitAmount,
                row_number() over (
                    partition by
                        miliben.personId,
                        miliben.termCode,
                        miliben.benefitType,
                        miliben.benefitAmount
                    order by
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') = miliben.snapshotDate then 1 else 2 end) asc,
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') < miliben.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') > miliben.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else miliben.snapshotDate end) asc,
                        studentENT.recordActivityDate desc
                ) studRn
            from MilitaryBenefitMCR miliben
                left join Student studentENT on miliben.personId = studentENT.personId
                    and miliben.termCode = studentENT.termCode
                    and ((to_date(studentENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)  
                        and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= miliben.EndDate
                        and studentENT.studentStatus = 'Active') --do not report Study Abroad students
                            or to_date(studentENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
                    and studentENT.isIpedsReportable = 1
                    and studentENT.studentLevel in ('Undergrad', 'Graduate')
                inner join AcademicTermOrder termorder on termorder.termCode = miliben.termCode
            ) stud
        cross join (select first(icOfferUndergradAwardLevel) icOfferUndergradAwardLevel
                    from ClientConfigMCR) config
        where stud.studRn = 1
        ) stu 
    group by personId, benefitType
    )
group by studentLevel
),

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/

--Part F	
FormatPartFStudentIncome as (
select *
from (
	VALUES
		(1), -- 1=0-30,000
		(2), -- 2=30,001-48,000
		(3), -- 3=48,001-75,000
		(4), -- 4=75,001-110,000
		(5) -- 5=110,001 and more
	) as studentIncome(ipedsIncome)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

--Part A All undergraduate counts and financial aid
--Group 1 -> All undergraduate students

--Valid values
--Number of students: 0-999999, -2 or blank = not-applicable
--Total amount of aid: 0-999999999999

select PART,
        sum(FIELD2_1) FIELD2_1,
        sum(FIELD3_1) FIELD3_1,
        sum(FIELD4_1) FIELD4_1,
        sum(FIELD5_1) FIELD5_1,
        sum(FIELD6_1) FIELD6_1,
        sum(FIELD7_1) FIELD7_1,
        sum(FIELD8_1) FIELD8_1,
        sum(FIELD9_1) FIELD9_1,
        null FIELD10_1,
       null FIELD11_1,
       null FIELD12_1,
       null FIELD13_1,
       null FIELD14_1,
       null FIELD15_1,
       null FIELD16_1
from (
    select 'A' PART,
           (case when config.acadOrProgReporter = 'A' then coalesce(count(*), 0) else null end) FIELD2_1, --Public and private academic reporters - Count of Group 1
           (case when config.acadOrProgReporter = 'P' then coalesce(count(*), 0) else null end) FIELD3_1, --Program reporters - Count of unduplicated Group 1
           coalesce(SUM(case when cohortstu.totalAid > 0 then 1 else 0 end), 0) FIELD4_1, --Count of Group 1 with awarded aid or work study from any source
           coalesce(SUM(case when cohortstu.pellGrant > 0  then 1 else 0 end), 0) FIELD5_1, --Count of Group 1 with awarded PELL grants
           coalesce(SUM(case when cohortstu.federalLoan > 0  then 1 else 0 end), 0) FIELD6_1, --Count of Group 1 with awarded and accepted federal loans
           ROUND(coalesce(SUM(case when cohortstu.allGrantSchol > 0  then cohortstu.allGrantSchol else 0 end), 0)) FIELD7_1, --Total grant aid amount awarded to Group 1 from all sources
           ROUND(coalesce(SUM(case when cohortstu.pellGrant > 0  then cohortstu.pellGrant else 0 end), 0)) FIELD8_1, --Total PELL grant amount awarded to Group 1
           ROUND(coalesce(SUM(case when cohortstu.federalLoan > 0  then cohortstu.federalLoan else 0 end), 0)) FIELD9_1 --Total federal loan amount awarded and accepted by Group 1
    from FinancialAidMCR cohortstu
        cross join (select first(acadOrProgReporter) acadOrProgReporter
                from clientConfigMCR) config
    where cohortstu.yearType = 'CY'
    group by config.acadOrProgReporter
    
    union
    
    select 'A',
            null,
            null,
            0,
            0,
            0,
            0,
            0,
            0
    )
group by PART
    
union

--Part B Full-time, first-time undergraduate counts
--Group 2 -> full-time, first-time degree/certificate-seeking
--Group 2a -> Work study, grant or scholarship aid from the federal, state/local govt or institution, loan from all sources;
--            Do not include grant or scholarship aid from private or other sources
--Group 2b -> awarded any loans or grants or scholarship from federal, state, local or institution

select PART,
        sum(FIELD2_1) FIELD2_1,
        sum(FIELD3_1) FIELD3_1,
        sum(FIELD4_1) FIELD4_1,
        sum(FIELD5_1) FIELD5_1,
        sum(FIELD6_1) FIELD6_1,
        sum(FIELD7_1) FIELD7_1,
        sum(FIELD8_1) FIELD8_1,
        null FIELD9_1,
        null FIELD10_1,
       null FIELD11_1,
       null FIELD12_1,
       null FIELD13_1,
       null FIELD14_1,
       null FIELD15_1,
       null FIELD16_1
from (
    select 'B' PART, --3m 31s
           (case when config.acadOrProgReporter = 'A' then sum(cohortstu.isGroup2) else null end) FIELD2_1, --Public and private academic reporters - Count of Group 2 --SUM(isGroup2)
           (case when config.publicOrPrivateInstitution = 'U' then SUM(case when cohortstu.Residency = 'In District' then cohortstu.isGroup2 end) else null end) FIELD3_1, --Public reporters - Count of Group 2 paying in-district tuition rates, 0 to 999999, -2 or blank = not-applicable
           (case when config.publicOrPrivateInstitution = 'U' then SUM(case when cohortstu.Residency = 'In State' then cohortstu.isGroup2 end) else null end) FIELD4_1, --Public reporters - Count of Group 2 paying in-state tuition rates, -2 or blank = not-applicable
           (case when config.publicOrPrivateInstitution = 'U' then SUM(case when cohortstu.Residency in ('Out of State', 'Out of US') then cohortstu.isGroup2 end) else null end) FIELD5_1, --Public reporters - Count of Group 2 paying out-of-state tuition rates, -2 or blank = not-applicable
           (case when config.acadOrProgReporter = 'P' then sum(cohortstu.isGroup2) else null end) FIELD6_1, --Program reporters - Count of unduplicated Group 2, -2 or blank = not-applicable
           SUM(cohortstu.isGroup2a) FIELD7_1, --Count of Group 2a, -2 or blank = not-applicable ***have not figured out a scenario where this would be null/not applicable
           SUM(cohortstu.isGroup2b) FIELD8_1 --Count of Group 2b, -2 or blank = not-applicable ***have not figured out a scenario where this would be null/not applicable
    from FinancialAidMCR cohortstu
    cross join (select first(acadOrProgReporter) acadOrProgReporter,
                        first(publicOrPrivateInstitution) publicOrPrivateInstitution
                from clientConfigMCR) config
    where cohortstu.yearType = 'CY'
        and cohortstu.isGroup2 = 1
    group by config.acadOrProgReporter,
            config.publicOrPrivateInstitution
            
    union
    
    select 'B',
            null,
            null,
            null,
            null,
            null,
            null,
            null
)
group by PART

union

--Part C Full-time, first-time financial aid
--Group 2 -> full-time, first-time degree/certificate-seeking
--Group 2a -> Work study, grant or scholarship aid from the federal, state/local govt or institution, loan from all sources;
--            Do not include grant or scholarship aid from private or other sources
--Group 2b -> awarded any loans or grants or scholarship from federal, state, local or institution

--Valid values
--Number of students: 0-999999
--Total amount of aid: 0-999999999999, if associated count > 0, else blank or -2 (not applicable)

select 'C' PART,
       FIELD2_1 FIELD2_1,
       FIELD3_1 FIELD3_1,
       FIELD4_1 FIELD4_1,
       FIELD5_1 FIELD5_1,
       FIELD6_1 FIELD6_1,
       FIELD7_1 FIELD7_1,
       FIELD8_1 FIELD8_1,
       FIELD9_1 FIELD9_1,
       FIELD10_1 FIELD10_1,
       (case when FIELD4_1 > 0 then ROUND(coalesce(FIELD11_1, 0)) else null end) FIELD11_1,
       (case when FIELD5_1 > 0 then ROUND(coalesce(FIELD12_1, 0)) else null end) FIELD12_1,
       (case when FIELD6_1 > 0 then ROUND(coalesce(FIELD13_1, 0)) else null end) FIELD13_1,
       (case when FIELD7_1 > 0 then ROUND(coalesce(FIELD14_1, 0)) else null end) FIELD14_1,
       (case when FIELD9_1 > 0 then ROUND(coalesce(FIELD15_1, 0)) else null end) FIELD15_1,
       (case when FIELD10_1 > 0 then ROUND(coalesce(FIELD16_1, 0)) else null end) FIELD16_1
from (
    select --counts
       coalesce(SUM(isGroup3), 0) FIELD2_1, --Count of Group 2 awarded grant or schol aid from fed, state/local govt or institution, 0 to 99999
       coalesce(SUM(case when cohortstu.federalGrantSchol > 0 then isGroup2 end), 0) FIELD3_1, --Count of Group 2 awarded federal grants, 0 to 99999
       coalesce(SUM(case when cohortstu.pellGrant > 0 then isGroup2 end), 0) FIELD4_1, --Count of Group 2 awarded PELL grants, 0 to 99999
       coalesce(SUM(case when cohortstu.federalGrantSchol - cohortstu.pellGrant > 0 then isGroup2 end), 0) FIELD5_1, --Count of Group 2 awarded other federal grants, 0 to 99999
       coalesce(SUM(case when cohortstu.stateLocalGrantSchol > 0 then isGroup2 end), 0) FIELD6_1, --Count of Group 2 awarded state/local govt grants, 0 to 99999
       coalesce(SUM(case when cohortstu.institutionGrantSchol > 0 then isGroup2 end), 0) FIELD7_1, --Count of Group 2 awarded institutional grants, 0 to 99999
       coalesce(SUM(case when cohortstu.stateLocalLoan + cohortstu.institutionLoan + cohortstu.otherLoan > 0 then isGroup2 end), 0) FIELD8_1, --Count of Group 2 awarded and accepted loans from all sources, 0 to 99999
       coalesce(SUM(case when cohortstu.federalLoan > 0 then isGroup2 end), 0) FIELD9_1, --Count of Group 2 awarded and accepted federal loans, 0 to 99999
       coalesce(SUM(case when cohortstu.stateLocalLoan + cohortstu.institutionLoan + cohortstu.otherLoan > 0 then isGroup2 end), 0) FIELD10_1, --Count of Group 2 awarded and accepted other loans to students (including private loans), 0 to 99999
--aid totals
       SUM(case when cohortstu.pellGrant > 0 then cohortstu.pellGrant end) FIELD11_1, --Total PELL grant amount awarded to Group 2, 0-999999999999, if associated count > 0, else blank or -2 (not applicable)
       SUM(case when nonPellFederalGrantSchol > 0 then nonPellFederalGrantSchol end) FIELD12_1, --Total other federal grant amount awarded to Group 2, 0-999999999999, if associated count > 0, else blank or -2 (not applicable)
       SUM(case when cohortstu.stateLocalGrantSchol > 0 then cohortstu.stateLocalGrantSchol end) FIELD13_1, --Total state/local govt grant amount awarded to Group 2, 0-999999999999, if associated count > 0, else blank or -2 (not applicable)
       SUM(case when cohortstu.institutionGrantSchol > 0 then cohortstu.institutionGrantSchol end) FIELD14_1, --Total inst grant amount awarded to Group 2, 0-999999999999, if associated count > 0, else blank or -2 (not applicable)
       SUM(case when cohortstu.federalLoan > 0 then cohortstu.federalLoan end) FIELD15_1, --Total federal loan amount awarded and accepted by Group 2, 0-999999999999, if associated count > 0, else blank or -2 (not applicable)
       SUM(case when cohortstu.otherLoan > 0 then cohortstu.otherLoan end) FIELD16_1  --Total other loan amount awarded and accepted by Group 2, 0-999999999999, if associated count > 0, else blank or -2 (not applicable)
    from FinancialAidMCR cohortstu
    where cohortstu.yearType = 'CY'
        and cohortstu.isGroup2 = 1
)

union

--Part D Full-time, first-time who were awarded aid
--Group 3 -> Awarded grant or scholarship aid from federal, state, local govt or the institution;
--           enrolled in the largest program for program reporters

--Valid values
--Number of students: 0-999999
--Total amount of aid: 0-999999999999
--       For public institutions, include only those students paying the in-state or in-district tuition rate.
--       For program reporters, include only those students enrolled in the institution’s largest program.
--****still need to add program reporter requirement of largest program

select PART,
        sum(FIELD2_1) FIELD2_1,
        sum(FIELD3_1) FIELD3_1,
        sum(FIELD4_1) FIELD4_1,
        sum(FIELD5_1) FIELD5_1,
        sum(FIELD6_1) FIELD6_1,
        sum(FIELD7_1) FIELD7_1,
        sum(FIELD8_1) FIELD8_1,
        sum(FIELD9_1) FIELD9_1,
        sum(FIELD10_1) FIELD10_1, 
       sum(FIELD11_1) FIELD11_1,
       sum(FIELD12_1) FIELD12_1,
       sum(FIELD13_1) FIELD13_1,
       sum(FIELD14_1) FIELD14_1,
       sum(FIELD15_1) FIELD15_1,
       sum(FIELD16_1) FIELD16_1
from (
    select 'D' PART,
            coalesce(SUM(case when cohortstu.yearType = 'CY' then isGroup3 end), 0) FIELD2_1, --Count of Group 3 current year, 0-999999
            (case when config.sfaReportPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY1' then isGroup3 end), 0) else null end) FIELD3_1, --Count of Group 3 prior year, 0-999999
            (case when config.sfaReportSecondPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY2' then isGroup3 end), 0) else null end) FIELD4_1, --Count of Group 3 prior2 year, 0-999999
            coalesce(SUM(case when cohortstu.yearType = 'CY' and cohortstu.livingArrangement = 'On Campus' then isGroup3 end), 0) FIELD5_1, --Count of Group 3 current year living on campus, 0-999999
            (case when config.sfaReportPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY1' and cohortstu.livingArrangement = 'On Campus' then isGroup3 end), 0) else null end) FIELD6_1, --Count of Group 3 prior year living on campus, 0-999999
            (case when config.sfaReportSecondPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY2' and cohortstu.livingArrangement = 'On Campus' then isGroup3 end), 0) else null end) FIELD7_1, --Count of Group 3 prior2 year living on campus, 0-999999
            coalesce(SUM(case when cohortstu.yearType = 'CY' and cohortstu.livingArrangement = 'Off Campus with Family' then isGroup3 end), 0) FIELD8_1, --Count of Group 3 current year living off campus with family, 0-999999
            (case when config.sfaReportPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY1' and cohortstu.livingArrangement = 'Off Campus with Family' then isGroup3 end), 0) else null end) FIELD9_1, --Count of Group 3 prior year living off campus with family, 0-999999
            (case when config.sfaReportSecondPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY2' and cohortstu.livingArrangement = 'Off Campus with Family' then isGroup3 end), 0) else null end) FIELD10_1, --Count of Group 3 prior2 year living off campus with family, 0-999999
            coalesce(SUM(case when cohortstu.yearType = 'CY' and cohortstu.livingArrangement = 'Off Campus' then isGroup3 end), 0) FIELD11_1, --Count of Group 3 current year living off campus not with family, 0-999999
            (case when config.sfaReportPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY1' and cohortstu.livingArrangement = 'Off Campus' then isGroup3 end), 0) else null end) FIELD12_1, --Count of Group 3 prior year living off campus not with family, 0-999999
            (case when config.sfaReportSecondPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY2' and cohortstu.livingArrangement = 'Off Campus' then isGroup3 end), 0) else null end) FIELD13_1, --Count of Group 3 prior2 year living off campus not with family, 0-999999
            ROUND(coalesce(SUM(case when cohortstu.yearType = 'CY' then cohortstu.totalAid end), 0)) FIELD14_1, --Total aid for Group 3 current year, 0-999999999999
            ROUND((case when config.sfaReportPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY1' then cohortstu.totalAid end), 0) else null end)) FIELD15_1, --Total aid for Group 3 prior year, 0-999999999999
            ROUND((case when config.sfaReportSecondPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY2' then cohortstu.totalAid end), 0) else null end)) FIELD16_1  --Total aid for Group 3 prior2 year, 0-999999999999
    from FinancialAidMCR cohortstu
        cross join (select first(sfaReportPriorYear) sfaReportPriorYear,
                            first(sfaReportSecondPriorYear) sfaReportSecondPriorYear,
                            first(acadOrProgReporter) acadOrProgReporter,
                            first(publicOrPrivateInstitution) publicOrPrivateInstitution
                from clientConfigMCR) config
    where cohortstu.isGroup3 = 1
    and ((config.publicOrPrivateInstitution = 'U' 
             and cohortstu.residency in ('In State', 'In District'))
          or config.publicOrPrivateInstitution = 'P')
    --and ((config.acadOrProgReporter = 'P' 
    --         and 'P' = 'P') --student in largest program
    --      or config.acadOrProgReporter = 'A')    
    group by config.sfaReportPriorYear, config.sfaReportSecondPriorYear
    
    union
    
    select 'D',
            0 FIELD2_1,
            null FIELD3_1,
            null FIELD4_1,
            0 FIELD5_1,
            null FIELD6_1,
            null FIELD7_1,
            0 FIELD8_1,
            null FIELD9_1,
            null FIELD10_1, 
           0 FIELD11_1,
           null FIELD12_1,
           null FIELD13_1,
           0 FIELD14_1,
           null FIELD15_1,
           null FIELD16_1
)
group by PART

union

--Part E Full-time, first-time who were awarded Title IV aid
--Group 4 -> Awarded any Title IV aid
--Federal Pell Grant, Federal Supplemental Educational Opportunity Grant (FSEOG), Academic Competitiveness Grant (ACG),
--National Science and Mathematics Access to Retain Talent Grant (National SMART Grant), Teacher Education Assistance for College and Higher Education (TEACH) Grant
--Federal Work Study
--Federal Perkins Loan, Subsidized Direct or FFEL Stafford Loan, and Unsubsidized Direct or FFEL Stafford Loan

--Valid values
--Academic Years: 1=academic year 2017-18, 2=academic year 2016-17, 3=academic year 2015-16. If you have previously reported prior year values to IPEDS, report only YEAR=1.
--Number of students: 0-999999
--       For public institutions, include only those students paying the in-state or in-district tuition rate.
--       For program reporters, include only those students enrolled in the institution’s largest program.
--****still need to add program reporter requirement of largest program

select 'E' PART,
       yearType FIELD2_1, --Acad Year, current
       coalesce(sum(isGroup4), 0) FIELD3_1, --Count of Group 4 students
       coalesce(SUM(case when livingArrangement = 'On Campus' then isGroup4 end), 0) FIELD4_1, 				--Count of Group 4 living on campus
       coalesce(SUM(case when livingArrangement = 'Off Campus with Family' then isGroup4 end), 0) FIELD5_1, 	--Count of Group 4 living off campus with family
       coalesce(SUM(case when livingArrangement = 'Off Campus' then isGroup4 end), 0) FIELD6_1, 				--Count of Group 4 living off campus not with family
       null FIELD7_1,
       null FIELD8_1,
       null FIELD9_1,
       null FIELD10_1, 
       null FIELD11_1, 
       null FIELD12_1, 
       null FIELD13_1, 
       null FIELD14_1, 
       null FIELD15_1, 
       null FIELD16_1 
from (
    select cohortstu.personId personId,
        cohortstu.isGroup4 isGroup4,
        cohortstu.livingArrangement livingArrangement,
        (case when cohortstu.yearType = 'CY' then 1
              when cohortstu.yearType = 'PY1' 
                and (select first(upper(sfaReportPriorYear))
                    from clientConfigMCR) = 'Y' then 2
              when cohortstu.yearType = 'PY2' 
                and (select first(upper(sfaReportSecondPriorYear))
                    from clientConfigMCR) = 'Y' then 3 
        end) yearType
    from FinancialAidMCR cohortstu
    where cohortstu.isGroup4 = 1
        and cohortstu.TitleIV > 0
        and (((select first(upper(publicOrPrivateInstitution))
                    from clientConfigMCR) = 'U'
                and cohortstu.residency in ('In State', 'In District'))
          or (select first(upper(publicOrPrivateInstitution))
                    from clientConfigMCR) = 'P')
    --and (((select first(upper(publicOrPrivateInstitution))
    --                from clientConfigMCR) = 'P' 
    --         and 'P' = 'P') --student in largest program
    --      or (select first(upper(publicOrPrivateInstitution))
    --                from clientConfigMCR) = 'A')
    
    union
    
    select null, --personId
            0, --isGroup4
            null, --livingArrangement
            1 --yearType
)
group by yearType

union

select 'F' PART,
       (case when yearType = 'CY' then 1
          when yearType = 'PY1' 
            and (select first(upper(sfaReportPriorYear))
                from clientConfigMCR) = 'Y' then 2
          when yearType = 'PY2' 
            and (select first(upper(sfaReportSecondPriorYear))
                from clientConfigMCR) = 'Y' then 3 
        end) FIELD2_1, --Acad Year, prior
       familyIncome FIELD3_1, --Income range of Group 4 students (values 1 - 5)
       sum(isGroup4) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       SUM(case when allGrantSchol > 0 then isGroup4 else 0 end) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       SUM(allGrantSchol) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
       null FIELD7_1,
       null FIELD8_1,
       null FIELD9_1,
       null FIELD10_1,
       null FIELD11_1,
       null FIELD12_1,
       null FIELD13_1,
       null FIELD14_1,
       null FIELD15_1,
       null FIELD16_1
from (
    select cohortstu.personId personId,
        cohortstu.isGroup4 isGroup4,
        cohortstu.familyIncome familyIncome,
        cohortstu.allGrantSchol allGrantSchol,
        cohortstu.yearType yearType
    from FinancialAidMCR cohortstu
where cohortstu.isGroup4 = 1
	and cohortstu.TitleIV > 0
    and (((select first(upper(publicOrPrivateInstitution))
                from clientConfigMCR) = 'U'
            and cohortstu.residency in ('In State', 'In District'))
      or (select first(upper(publicOrPrivateInstitution))
                from clientConfigMCR) = 'P')
--and (((select first(upper(publicOrPrivateInstitution))
--                from clientConfigMCR) = 'P' 
--         and 'P' = 'P') --student in largest program
--      or (select first(upper(publicOrPrivateInstitution))
--                from clientConfigMCR) = 'A')

    union

    select null, -- personId
        0, --isGroup4
        studentIncome.ipedsIncome, --familyIncome
        0, --allGrantSchol
        'CY' --yearType
    from FormatPartFStudentIncome studentIncome
    
    union

    select null, -- personId
        0, --isGroup4
        studentIncome.ipedsIncome, --familyIncome
        0, --allGrantSchol
        'PY1' --yearType
    from FormatPartFStudentIncome studentIncome
    where (select first(upper(sfaReportPriorYear))
                from clientConfigMCR) = 'Y'
                
    union

    select null, -- personId
        0, --isGroup4
        studentIncome.ipedsIncome, --familyIncome
        0, --allGrantSchol
        'PY2' --yearType
    from FormatPartFStudentIncome studentIncome
    where (select first(upper(sfaReportSecondPriorYear))
                from clientConfigMCR) = 'Y'
    )
group by yearType, familyIncome

union

--Part G Section 2: Veteran's Benefits

--Valid values
--Student level: 1=Undergraduate, 2=Graduate, 3=Total (Total will be generated)
--Number of students: 0 to 999999, -2 or blank = not-applicable
--Total amount of aid: 0 to 999999999999, -2 or blank = not-applicable

select 'G' PART,
       max(FIELD2_1) FIELD2_1,
       max(FIELD3_1) FIELD3_1,
       round(max(FIELD4_1),0) FIELD4_1,
       max(FIELD5_1) FIELD5_1,
       round(max(FIELD6_1),0) FIELD6_1,
       null FIELD7_1,
       null FIELD8_1,
       null FIELD9_1,
       null FIELD10_1,
       null FIELD11_1,
       null FIELD12_1,
       null FIELD13_1,
       null FIELD14_1,
       null FIELD15_1,
       null FIELD16_1
from (
--if institution offers undergraduate level, count military benefits; if none, output 0
    select 1 FIELD2_1, --Student Level 1=Undergraduate, 2=Graduate
       coalesce(mililevl.giCount, 0) FIELD3_1, --Post-9/11 GI Bill Benefits - Number of students receiving benefits/assistance
       coalesce(mililevl.giBillAmt, 0) FIELD4_1, --Post-9/11 GI Bill Benefits - Total dollar amount of benefits/assistance disbursed through the institution
       coalesce(mililevl.dodCount, 0) FIELD5_1, --Department of Defense Tuition Assistance Program - Number of students receiving benefits/assistance
       coalesce(mililevl.dodAmt, 0) FIELD6_1 --Department of Defense Tuition Assistance Program - Total dollar amount of benefits/assistance disbursed through the institution
    from MilitaryStuLevel mililevl
    where mililevl.studentLevel = 'Undergrad'
        and (select first(icOfferUndergradAwardLevel)
                    from ClientConfigMCR) = 'Y'
        
    union

--if institution doesn't offer undergraduate level or no records exist in MilitaryBenefitMCR, output null values
    select 1,
        null,
        null,
        null,
        null
    )
    
union

select 'G' PART,
       max(FIELD2_1) FIELD2_1,
       max(FIELD3_1) FIELD3_1,
       round(max(FIELD4_1),0) FIELD4_1,
       max(FIELD5_1) FIELD5_1,
       round(max(FIELD6_1),0) FIELD6_1,
       null FIELD7_1,
       null FIELD8_1,
       null FIELD9_1,
       null FIELD10_1,
       null FIELD11_1,
       null FIELD12_1,
       null FIELD13_1,
       null FIELD14_1,
       null FIELD15_1,
       null FIELD16_1
from (
--if institution offers graduate level, count military benefits; if none, output 0 
    select 2 FIELD2_1, --Student Level 1=Undergraduate, 2=Graduate
       coalesce(mililevl.giCount, 0) FIELD3_1, --Post-9/11 GI Bill Benefits - Number of students receiving benefits/assistance
       coalesce(mililevl.giBillAmt, 0) FIELD4_1, --Post-9/11 GI Bill Benefits - Total dollar amount of benefits/assistance disbursed through the institution
       coalesce(mililevl.dodCount, 0) FIELD5_1, --Department of Defense Tuition Assistance Program - Number of students receiving benefits/assistance
       coalesce(mililevl.dodAmt, 0) FIELD6_1 --Department of Defense Tuition Assistance Program - Total dollar amount of benefits/assistance disbursed through the institution
    from MilitaryStuLevel mililevl
    where mililevl.studentLevel = 'Graduate'
        and (select first(icOfferGraduateAwardLevel)
                    from ClientConfigMCR) = 'Y'
        
    union

--if institution doesn't offer graduate level or no records exist in MilitaryBenefitMCR, output null values
    select 2,
        null,
        null,
        null,
        null
    ) 
