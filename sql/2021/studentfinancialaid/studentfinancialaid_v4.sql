/********************

EVI PRODUCT:    DORIS 2021-22 IPEDS Survey Winter Collection
FILE NAME:      Student Financial Aid v4 (SFA)
FILE DESC:      Student Financial Aid for institutions reporting on a full-year cohort (private program reporters)
AUTHOR:         Ahmed Khasawneh
CREATED:        20210907

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Cohort Creation
Formatting Views
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)   Author             	Tag             	Comments
----------- 	--------------------	-------------   	-------------------------------------------------
20210907    	akhasawneh 									Initial version test 24m 11s, prod 24m 42s

Enrollment cohort is prior Fall term

Snapshot tag requirements:

Multiple snapshots - one for each term/part of term combination:
Fall Census - enrollment cohort
Pre-Fall Summer Census - check for studentType of 'First Time' or 'Transfer' if Fall studentType is 'Continuing'

*If client is reporting the prior year and/or second prior year numbers, there could be up to 3 years of Fall and 
Summer census snapshots.

One snapshot for each year (current, prior 1, prior 2):
Financial Aid Year End

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
 full or partial term that starts within the academic year 7/1/2020 thru 6/30/2021
  ------------------------------------------------------------------
 Each client will need to determine how to identify and/or pull the 
 terms for their institution based on how they track the terms.  
 For some schools, it could be based on dates or academic year and for others,
 it may be by listing specific terms. 
 *******************************************************************/ 

--prod default block

select '2122' surveyYear, 
	'SFA' surveyId,
	'Fall Census' repPeriodTag1,
	'Financial Aid Year End' repPeriodTag2,
	'June End' repPeriodTag3, --'October End' repPeriodTag3,
	'GI Bill' repPeriodTag4,
	'Department of Defense' repPeriodTag5,
	'Pre-Fall Summer Census' repPeriodTag6,
	'Dept of Defense' repPeriodTag7, --temporary: old enum value
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2020-07-01' AS DATE) reportingDateStart,
	CAST('2021-06-30' AS DATE) reportingDateEnd,
	'202110' termCode, --Fall 2020
	'1' partOfTermCode, 
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
	CAST('2020-07-01' as DATE) giBillStartDate,
    CAST('2021-06-30' as DATE) giBillEndDate,
    CAST('2020-10-01' as DATE) dodStartDate,
    CAST('2021-09-30' as DATE) dodEndDate,
    CAST('2021-06-30' as DATE) financialAidEndDate,
    '' sfaLargestProgCIPC, --'CIPC (no dashes, just numeric characters); Default value (if no record): null'
    -- 'N' sfaHybridReporter, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportPriorYear, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportSecondPriorYear --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'

/*
--testing default block
select '1415' surveyYear,  
	'SFA' surveyId, 
	'Fall Census' repPeriodTag1,
	'Financial Aid Year End' repPeriodTag2,
	'June End' repPeriodTag3, --'October End' repPeriodTag3,
	'GI Bill' repPeriodTag4,
	'Department of Defense' repPeriodTag5,
	'Pre-Fall Summer Census' repPeriodTag6,
	'Dept of Defense' repPeriodTag7, --temporary: old enum value
	CAST('9999-09-09' as DATE) snapshotDate,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
	'201410' termCode,
	'1' partOfTermCode,
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    CAST('2013-07-01' as DATE) giBillStartDate,
    CAST('2014-06-30' as DATE) giBillEndDate,
    CAST('2013-10-01' as DATE) dodStartDate,
    CAST('2014-09-30' as DATE) dodEndDate,
    CAST('2014-06-30' as DATE) financialAidEndDate, 
    '' sfaLargestProgCIPC, --'CIPC (no dashes, just numeric characters); Default value (if no record): null'
    -- 'N' sfaHybridReporter, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportPriorYear, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
    'N' sfaReportSecondPriorYear --'Valid values: Y = Yes, N = No; Default value (if no record or null value): N'
*/
),

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 

--  1st union 1st order - pull snapshot for defvalues.repPeriodTag1 or defvalues.repPeriodTag3
--  1st union 2nd order - pull snapshot for defvalues.repPeriodTag2
--  1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--  2nd union - pull default values if no record in IPEDSReportingPeriod

select distinct RepDates.surveyYear	surveyYear,
    RepDates.source source,
    coalesce(upper(RepDates.surveySection), 'COHORT') surveySection,
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
    upper(RepDates.termCode) termCode,	
	upper(RepDates.partOfTermCode) partOfTermCode
from (
    select repperiodENT.surveyCollectionYear surveyYear,
	    'IPEDSReportingPeriod' source,
		repperiodENT.snapshotDate snapshotDate,
		repPeriodENT.surveyId surveyId,
		repPeriodENT.surveySection surveySection,
		repperiodENT.termCode termCode,
		coalesce(repperiodENT.partOfTermCode, 1) partOfTermCode,
		row_number() over (	
			partition by 
				repPeriodENT.surveyCollectionYear,
                repPeriodENT.surveyId,
                repPeriodENT.surveySection, 
				repperiodENT.termCode,
				repperiodENT.partOfTermCode	
			order by 
			    (case when array_contains(repperiodENT.tags, defvalues.repPeriodTag1) then 1 --academic reporters Fall Census
			         when array_contains(repperiodENT.tags, defvalues.repPeriodTag3) then 1 --hybrid reporters October End
                     when array_contains(repperiodENT.tags, defvalues.repPeriodTag2) then 2 --Financial Aid Year End
			         else 3 end) asc,
			     repperiodENT.snapshotDate desc,
                repperiodENT.recordActivityDate desc	
		) reportPeriodRn	
		from IPEDSReportingPeriod repperiodENT
		    cross join DefaultValues defvalues
		where repperiodENT.surveyId = defvalues.surveyId
	    and repperiodENT.surveyCollectionYear = defvalues.surveyYear
	    and repperiodENT.termCode is not null
	
    union 
 
	select defvalues.surveyYear surveyYear,
	    'DefaultValues' source,
		CAST('9999-09-09' as DATE) snapshotDate,
		defvalues.surveyId surveyId, 
		null surveySection,
		defvalues.termCode termCode,
		defvalues.partOfTermCode partOfTermCode,
		1
	from DefaultValues defvalues
    where defvalues.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = defvalues.surveyYear
											and upper(repperiodENT.surveyId) = defvalues.surveyId 
											and repperiodENT.termCode is not null) 
    ) RepDates
    where RepDates.reportPeriodRn = 1
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 
--          1st union 1st order - pull snapshot if the same as ReportingPeriodMCR snapshot 
--          1st union 2nd order - pull snapshot > ReportingPeriodMCR snapshot 
--          1st union 3rd order - pull snapshot < ReportingPeriodMCR snapshot 
--          2nd union - pull default values if no record in IPEDSClientConfig

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.source source,
    to_date(ConfigLatest.snapshotDate,'YYYY-MM-DD') snapshotDate,
    ConfigLatest.repperiodSnapshotDate repperiodSnapshotDate,
    upper(ConfigLatest.instructionalActivityType) instructionalActivityType,
    upper(ConfigLatest.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
    upper(ConfigLatest.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
	ConfigLatest.repPeriodTag2 repPeriodTag2,
    ConfigLatest.repPeriodTag3 repPeriodTag3,
	ConfigLatest.repPeriodTag4 repPeriodTag4,
    ConfigLatest.repPeriodTag5 repPeriodTag5,
	ConfigLatest.repPeriodTag6 repPeriodTag6,
	ConfigLatest.repPeriodTag7 repPeriodTag7,
    ConfigLatest.giBillStartDate giBillStartDate,
    ConfigLatest.giBillEndDate giBillEndDate,
    ConfigLatest.dodStartDate dodStartDate,
    ConfigLatest.dodEndDate dodEndDate,
    ConfigLatest.financialAidEndDate financialAidEndDate,
    upper(ConfigLatest.sfaLargestProgCIPC) sfaLargestProgCIPC,
    upper(ConfigLatest.sfaReportPriorYear) sfaReportPriorYear, 
    --'N' sfaReportPriorYear, 
	upper(ConfigLatest.sfaReportSecondPriorYear) sfaReportSecondPriorYear,
    --'N' sfaReportSecondPriorYear,
    -- upper(ConfigLatest.sfaHybridReporter) sfaHybridReporter,
    upper(ConfigLatest.eviReserved1) caresAct1,
    upper(ConfigLatest.eviReserved2) caresAct2
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'IPEDSClientConfig' source,
		clientConfigENT.snapshotDate snapshotDate, 
		repperiod.snapshotDate repperiodSnapshotDate,
        coalesce(clientConfigENT.instructionalActivityType, defvalues.instructionalActivityType) instructionalActivityType,
        coalesce(clientConfigENT.icOfferUndergradAwardLevel, defvalues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
		coalesce(clientConfigENT.icOfferGraduateAwardLevel, defvalues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
		defvalues.repPeriodTag3 repPeriodTag3,
	    defvalues.repPeriodTag4 repPeriodTag4,
		defvalues.repPeriodTag5 repPeriodTag5,
	    defvalues.repPeriodTag6 repPeriodTag6,
	    defvalues.repPeriodTag7 repPeriodTag7,
		defvalues.giBillStartDate giBillStartDate,
		defvalues.giBillEndDate giBillEndDate,
        defvalues.dodStartDate dodStartDate,
        defvalues.dodEndDate dodEndDate,
        defvalues.financialAidEndDate financialAidEndDate,
        coalesce(clientConfigENT.sfaLargestProgCIPC, defvalues.sfaLargestProgCIPC) sfaLargestProgCIPC,
    -- coalesce(clientConfigENT.sfaHybridReporter) sfaHybridReporter,
        coalesce(clientConfigENT.sfaReportPriorYear, defvalues.sfaReportPriorYear) sfaReportPriorYear,
        coalesce(clientConfigENT.sfaReportSecondPriorYear, defvalues.sfaReportSecondPriorYear) sfaReportSecondPriorYear,
        clientConfigENT.eviReserved1 eviReserved1,
        clientConfigENT.eviReserved2 eviReserved2,
		row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
			    (case when to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				clientConfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientConfigENT
		inner join ReportingPeriodMCR repperiod on clientConfigENT.surveyCollectionYear = repperiod.surveyYear
	    cross join DefaultValues defvalues 
	where clientConfigENT.surveyCollectionYear = defvalues.surveyYear

    union

	select defvalues.surveyYear surveyYear,
	    'DefaultValues' source,
	    CAST('9999-09-09' as DATE) snapshotDate,
		null repperiodSnapshotDate,
        defvalues.instructionalActivityType instructionalActivityType,
        defvalues.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		defvalues.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
		defvalues.repPeriodTag3 repPeriodTag3,
	    defvalues.repPeriodTag4 repPeriodTag4,
		defvalues.repPeriodTag5 repPeriodTag5,
	    defvalues.repPeriodTag6 repPeriodTag6,
	    defvalues.repPeriodTag7 repPeriodTag7,
		defvalues.giBillStartDate giBillStartDate,
		defvalues.giBillEndDate giBillEndDate,
        defvalues.dodStartDate dodStartDate,
        defvalues.dodEndDate dodEndDate,
        defvalues.financialAidEndDate financialAidEndDate,
        defvalues.sfaLargestProgCIPC sfaLargestProgCIPC,
        defvalues.sfaReportPriorYear sfaReportPriorYear,
        defvalues.sfaReportSecondPriorYear sfaReportSecondPriorYear,
        null eviReserved1,
        null eviReserved2,
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
    select distinct upper(acadtermENT.termCode) termCode, 
        row_number() over (
            partition by 
                acadTermENT.snapshotDate,
                acadTermENT.termCode,
                acadTermENT.partOfTermCode
            order by
               coalesce(acadTermENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
        ) acadTermRn,
        acadTermENT.snapshotDate,
        acadTermENT.tags,
		coalesce(upper(acadtermENT.partOfTermCode), 1) partOfTermCode, 
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
		coalesce(acadtermENT.requiredFTCreditHoursGR, 9) requiredFTCreditHoursGR,
	    coalesce(acadtermENT.requiredFTCreditHoursUG, 12) requiredFTCreditHoursUG,
	    coalesce(acadtermENT.requiredFTClockHoursUG, 24) requiredFTClockHoursUG
	from AcademicTerm acadtermENT 
	where coalesce(acadtermENT.isIPEDSReportable, true) = true
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

select coalesce(repPerTerms.yearType, 'CY') yearType,
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
		repPerTerms.instructionalActivityType,
	    repPerTerms.equivCRHRFactor equivCRHRFactor,
	    repPerTerms.repPeriodTag1 repPeriodTag1, --'Fall Census'
	    repPerTerms.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
        repPerTerms.repPeriodTag3 repPeriodTag3, --'October End'
	    repPerTerms.repPeriodTag4 repPeriodTag4, --'GI Bill'
        repPerTerms.repPeriodTag5 repPeriodTag5, --'Department of Defense'
	    repPerTerms.repPeriodTag6 repPeriodTag6, --'Pre-Fall Summer Census'
	    repPerTerms.repPeriodTag7 repPeriodTag7, --'Dept of Defense'
	    repPerTerms.financialAidEndDate,
        repPerTerms.sfaReportPriorYear sfaReportPriorYear,
        repPerTerms.sfaReportSecondPriorYear sfaReportSecondPriorYear,
        repPerTerms.sfaLargestProgCIPC sfaLargestProgCIPC,
		repPerTerms.giBillStartDate giBillStartDate,
		repPerTerms.giBillEndDate giBillEndDate,
        repPerTerms.dodStartDate dodStartDate,
        repPerTerms.dodEndDate dodEndDate,
	    repPerTerms.caresAct1 caresAct1,
        repPerTerms.caresAct2 caresAct2,
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
        acadterm.tags tags,
        (case when repperiod.surveySection in ('COHORT', 'PRIOR SUMMER') then 'CY'
              when repperiod.surveySection in ('PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER') then 'PY1'
              when repperiod.surveySection in ('PRIOR YEAR 2 COHORT', 'PRIOR YEAR 2 PRIOR SUMMER') then 'PY2'
              end) yearType,
        acadterm.censusDate censusDate,
        termorder.termOrder termOrder,
        termorder.maxCensus maxCensus,
        acadterm.termClassification termClassification,
        acadterm.termType termType,
        acadterm.startDate startDate,
        acadterm.endDate endDate,
        acadterm.requiredFTCreditHoursGR,
	    acadterm.requiredFTCreditHoursUG,
	    acadterm.requiredFTClockHoursUG,
		clientconfig.instructionalActivityType,
	    coalesce(acadterm.requiredFTCreditHoursUG/
		    coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
        clientconfig.caresAct1 caresAct1,
        clientconfig.caresAct2 caresAct2,
        clientconfig.repPeriodTag1 repPeriodTag1, --'Fall Census'
	    clientconfig.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
        clientconfig.repPeriodTag3 repPeriodTag3, --'October End'
	    clientconfig.repPeriodTag4 repPeriodTag4, --'GI Bill'
        clientconfig.repPeriodTag5 repPeriodTag5, --'Department of Defense'
	    clientconfig.repPeriodTag6 repPeriodTag6, --'Pre-Fall Summer Census'
	    clientconfig.repPeriodTag7 repPeriodTag7, --'Dept of Defense'
	    clientconfig.financialAidEndDate,
        clientconfig.sfaReportPriorYear sfaReportPriorYear,
        clientconfig.sfaReportSecondPriorYear sfaReportSecondPriorYear,
        clientconfig.sfaLargestProgCIPC sfaLargestProgCIPC,
		clientconfig.giBillStartDate giBillStartDate,
		clientconfig.giBillEndDate giBillEndDate,
        clientconfig.dodStartDate dodStartDate,
        clientconfig.dodEndDate dodEndDate,
		row_number() over (
            partition by 
                repperiod.termCode,
                repperiod.partOfTermCode
            order by
                (case when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') 
                            and (((array_contains(acadterm.tags, clientconfig.repPeriodTag1) or array_contains(acadterm.tags, clientconfig.repPeriodTag3)) and repperiod.surveySection in ('COHORT', 'PRIOR YEAR 1 COHORT', 'PRIOR YEAR 2 COHORT')) --and acadterm.termType = 'Fall'
                                or (array_contains(acadterm.tags, clientconfig.repPeriodTag6) and repperiod.surveySection in ('PRIOR SUMMER', 'PRIOR YEAR 1 PRIOR SUMMER', 'PRIOR YEAR 2 PRIOR SUMMER'))) then 1 --and acadterm.termType = 'Summer'
                      when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') then 2
                     else 3 end) asc,
                (case when acadterm.snapshotDate > acadterm.censusDate then acadterm.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when acadterm.snapshotDate < acadterm.censusDate then acadterm.snapshotDate else CAST('1900-09-09' as DATE) end) desc
            ) acadTermRnReg
    from ReportingPeriodMCR repperiod 
        left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
	            and repperiod.partOfTermCode = acadterm.partOfTermCode
		left join AcademicTermOrder termorder on termOrder.termCode = repperiod.termCode
		inner join ClientConfigMCR clientconfig on repperiod.surveyYear = clientconfig.surveyYear
--remove 'FALL'
where ((clientconfig.sfaReportPriorYear = 'N' and clientconfig.sfaReportSecondPriorYear = 'N'
			            and upper(repperiod.surveySection) in ('FALL', 'PRIOR SUMMER', 'COHORT'))
		    or (clientconfig.sfaReportPriorYear = 'Y' and clientconfig.sfaReportSecondPriorYear = 'N'
			            and upper(repperiod.surveySection) in ('PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER', 'FALL', 'PRIOR SUMMER', 'COHORT'))
			or (clientconfig.sfaReportPriorYear = 'N' and clientconfig.sfaReportSecondPriorYear = 'Y'
			            and upper(repperiod.surveySection) in ('PRIOR YEAR 2 COHORT', 'PRIOR YEAR 2 PRIOR SUMMER', 'FALL', 'PRIOR SUMMER', 'COHORT'))
			or (clientconfig.sfaReportPriorYear = 'Y' and clientconfig.sfaReportSecondPriorYear = 'Y'
			            and upper(repperiod.surveySection) in ('PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER', 'PRIOR YEAR 2 COHORT', 'PRIOR YEAR 2 PRIOR SUMMER', 'FALL', 'PRIOR SUMMER', 'COHORT')))
		) repPerTerms
where repPerTerms.acadTermRnReg = 1 
),

AcademicTermReportingRefactor as (
--Returns all records from AcademicTermReporting, converts Summer terms to Pre-Fall or Post-Spring and creates reportingDateStart/End

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

select campus,
	isInternational,
	snapshotDate
from ( 
    select upper(campusENT.campus) campus,
		campusENT.campusDescription,
		coalesce(campusENT.isInternational, false) isInternational,
		to_date(campusENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
		row_number() over (
			partition by
			    campusENT.snapshotDate, 
				campusENT.campus
			order by
				campusENT.recordActivityDate desc
		) campusRn
	from Campus campusENT 
    where coalesce(campusENT.isIpedsReportable, true) = true
		and ((coalesce(to_date(campusENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
			and to_date(campusENT.recordActivityDate,'YYYY-MM-DD') <= to_date(campusENT.snapshotDate,'YYYY-MM-DD'))
				or coalesce(to_date(campusENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
	)
where campusRn = 1
),

RegistrationMCR as ( 
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 

select regData.yearType, 
        regData.surveySection,
        regData.snapshotDate,
        regData.termCode,
        regData.partOfTermCode,
        regData.personId personId,
        regData.regENTSSD regENTSSD,
        regData.repSSD repSSD,
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
		regData.instructionalActivityType,
        regData.equivCRHRFactor,
	    regData.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
	    regData.financialAidEndDate,
        regData.sfaReportPriorYear sfaReportPriorYear,
        regData.sfaReportSecondPriorYear sfaReportSecondPriorYear,
        regData.sfaLargestProgCIPC sfaLargestProgCIPC,
        regData.caresAct1,
        regData.caresAct2,   
        regData.courseSectionNumber,
        regData.courseSectionCampusOverride,
        regData.isAudited, 
		regData.courseSectionLevelOverride,
        regData.enrollmentHoursOverride
from ( 
    select regENT.personId personId,
        repperiod.snapshotDate snapshotDate,
        to_date(regENT.snapshotDate, 'YYYY-MM-DD') regENTSSD,
        repperiod.snapshotDate repSSD,
        upper(regENT.termCode) termCode,
        coalesce(upper(regENT.partOfTermCode), 1) partOfTermCode, 
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
        repperiod.instructionalActivityType,
        repperiod.equivCRHRFactor,
        repperiod.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
        repperiod.financialAidEndDate,
        repperiod.sfaReportPriorYear sfaReportPriorYear,
        repperiod.sfaReportSecondPriorYear sfaReportSecondPriorYear,
        repperiod.sfaLargestProgCIPC sfaLargestProgCIPC,
        repperiod.caresAct1 caresAct1,
        repperiod.caresAct2 caresAct2,
        upper(regENT.courseSectionCampusOverride) courseSectionCampusOverride,
        coalesce(regENT.isAudited, false) isAudited,
        coalesce(regENT.isEnrolled, true) isEnrolled,
        regENT.courseSectionLevelOverride courseSectionLevelOverride,  
        regENT.enrollmentHoursOverride enrollmentHoursOverride,
        upper(regENT.courseSectionNumber) courseSectionNumber,
        row_number() over (
            partition by
                repperiod.yearType,
                repperiod.surveySection,
                regENT.termCode,
                regENT.partOfTermCode,
                regENT.personId,
                regENT.courseSectionNumber,
                regENT.courseSectionLevelOverride
            order by 
                (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
                (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                regENT.recordActivityDate desc,
                regENT.registrationStatusActionDate desc
        ) regRn
    from AcademicTermReportingRefactor repperiod   
        inner join Registration regENT on upper(regENT.termCode) = repperiod.termCode
            and coalesce(upper(regENT.partOfTermCode), 1) = repperiod.partOfTermCode
            and ((coalesce(to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' AS DATE)
                        and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
                    or (coalesce(to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' AS DATE)
                        and ((coalesce(to_date(regENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                                and to_date(regENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                            or coalesce(to_date(regENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))))
            and coalesce(regENT.isIpedsReportable, true) = true
    ) regData
where regData.regRn = 1
    and regData.isEnrolled = true
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  

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
					when stuData.studentType = 'Visiting' then true
					when stuData.studentType = 'Unknown' then true
                    when stuData.studentLevel = 'Continuing Education' then true
                    when stuData.studentLevel = 'Other' then true
					when studata.studyAbroadStatus = 'Study Abroad - Host Institution' then true
                  else stuData.isNonDegreeSeeking end), false) isNonDegreeSeeking,
        stuData.studentLevel,
        stuData.studentType,
        stuData.residency,
        stuData.homeCampus,
		stuData.studyAbroadStatus,
		stuData.fullTimePartTimeStatus
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
            coalesce(studentENT.isNonDegreeSeeking, false) isNonDegreeSeeking,
            studentENT.studentLevel studentLevel,
            studentENT.studentType studentType,
            studentENT.residency residency,
            upper(studentENT.homeCampus) homeCampus,
			studentENT.fullTimePartTimeStatus,
			studentENT.studyAbroadStatus,
            row_number() over (
                partition by
                    reg.yearType,
                    reg.surveySection,
                    studentENT.personId,                    
                    studentENT.termCode
                order by
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') = reg.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    studentENT.recordActivityDate desc
            ) studRn
	from RegistrationMCR reg
		inner join Student studentENT on reg.personId = studentENT.personId 
			and reg.termCode = upper(studentENT.termCode)
			and ((coalesce(to_date(studentENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)  
				and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= reg.censusDate)
					or coalesce(to_date(studentENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  = CAST('9999-09-09' AS DATE))
			and coalesce(studentENT.isIpedsReportable, true) = true
	) stuData
where stuData.studRn = 1 
),

StudentRefactor as ( 
--Determine student info based on full term and degree-seeking status

--studentType logic: if studentType = 'Continuing' in fall term, assign prior summer studentType if exists; 
--   if studentType = 'Unknown' in fall or prior summer term and studentLevel equates to undergraduate, assign studentType of 'First Time';
--   if studentType in fall term is null, assign prior summer studentType

--Fall term enrollment mod: Drop surveySection from select fields and use yearType only going forward - Prior Summer sections only used to determine student type
--SFA mod: added outer filter of FallStu.studentLevelUGGR = 'UG' for all undergraduate requirement for Group 1

select FallStu.personId personId,
        SumStu.studentType innerType,
        SumStu.surveySection innerSection,
        FallStu.termCode firstFullTerm,
        FallStu.termCode termCode,
        FallStu.yearType yearType,
        FallStu.studentLevel studentLevel,
        FallStu.studentLevelUGGR,
        (case when FallStu.studentType = 'Continuing' and SumStu.personId is not null then SumStu.studentType 
              when coalesce(FallStu.studentType, SumStu.studentType) = 'Unknown' and FallStu.studentLevelUGGR = 'UG' then 'First Time' 
              else coalesce(FallStu.studentType, SumStu.studentType) 
        end) studentType, 
        FallStu.isNonDegreeSeeking isNonDegreeSeeking,
        FallStu.snapshotDate,
        FallStu.censusDate censusDate,
        FallStu.maxCensus maxCensus,
        FallStu.financialAidYear,
        FallStu.termOrder,
        FallStu.termType,
        FallStu.residency,
        FallStu.studyAbroadStatus,
        FallStu.fullTimePartTimeStatus
    from (
            select stu.yearType,
                    stu.surveySection,
                    stu.snapshotDate,
                    stu.termCode, 
                    stu.termOrder,
                    stu.financialAidYear,
                    stu.termType,
                    stu.startDate,
                    stu.censusDate,
                    stu.maxCensus,
                    stu.fullTermOrder,
                    stu.personId,
                    stu.isNonDegreeSeeking,
                    stu.homeCampus,
                    stu.studentType,
                    stu.studentLevel,
                    (case when stu.studentLevel in ('Undergraduate', 'Continuing Education', 'Other') then 'UG' else 'GR' end) studentLevelUGGR,
                    stu.residency,
					stu.studyAbroadStatus,
					stu.fullTimePartTimeStatus
            from StudentMCR stu
            where stu.surveySection like '%COHORT%'
        ) FallStu
        left join (select stu2.personId personId,
                          stu2.studentType studentType,
                          stu2.yearType yearType,
                          stu2.surveySection surveySection
                    from StudentMCR stu2
                    where stu2.surveySection like '%SUMMER%') SumStu on FallStu.personId = SumStu.personId
                        and FallStu.yearType = SumStu.yearType
    where FallStu.studentLevelUGGR = 'UG'
),

CourseSectionMCR as (
--Included to get enrollment hours of a courseSectionNumber

select *
from (
    select stu.yearType,
        reg.snapshotDate snapshotDate,
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
		stu.studyAbroadStatus,
		stu.fullTimePartTimeStatus,
--***** SFA doesn't report ethnicity and gender
	    null ipedsGender,
	    null ipedsEthnicity,
        reg.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
        reg.financialAidEndDate,
        reg.sfaReportPriorYear sfaReportPriorYear,
        reg.sfaReportSecondPriorYear sfaReportSecondPriorYear,
        reg.sfaLargestProgCIPC sfaLargestProgCIPC,
        reg.caresAct1 caresAct1,
        reg.caresAct2 caresAct2,
        coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        reg.courseSectionNumber,
        reg.isAudited,
        coalesce(reg.courseSectionLevelOverride, coursesectENT.courseSectionLevel) courseLevel, --reg level prioritized over courseSection level 
        coursesectENT.courseSectionLevel,
        upper(coursesectENT.subject) subject,
        upper(coursesectENT.courseNumber) courseNumber,
        upper(coursesectENT.section) section,
		upper(coursesectENT.customDataValue) customDataValue,
        coursesectENT.courseSectionStatus,
		coalesce(coursesectENT.isESL, false) isESL, 
		coalesce(coursesectENT.isRemedial, false) isRemedial,
		upper(coursesectENT.college) college,
		upper(coursesectENT.division) division,
		upper(coursesectENT.department) department,
        coalesce(reg.enrollmentHoursOverride, coursesectENT.enrollmentHours) enrollmentHours, --reg enr hours prioritized over courseSection enr hours
        reg.equivCRHRFactor,
        coalesce(coursesectENT.isClockHours, false) isClockHours,
		reg.courseSectionCampusOverride,
        reg.enrollmentHoursOverride,
        reg.courseSectionLevelOverride,
        coalesce(row_number() over (
                partition by
                    reg.yearType,
                    reg.termCode,
                    reg.partOfTermCode,
                    reg.personId,
                    reg.courseSectionNumber,
                    coursesectENT.courseSectionNumber
                order by
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') = reg.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    coursesectENT.recordActivityDate desc
            ), 1) courseRn
    from RegistrationMCR reg 
        inner join StudentRefactor stu on stu.personId = reg.personId
            and stu.firstFullTerm = reg.termCode
            and reg.yearType = stu.yearType
        left join CourseSection coursesectENT on reg.termCode = upper(coursesectENT.termCode)
            and reg.partOfTermCode = coalesce(upper(coursesectENT.partOfTermCode), 1)
            and reg.courseSectionNumber = upper(coursesectENT.courseSectionNumber)
            and coalesce(coursesectENT.isIpedsReportable, true) = true
            and ((coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                    and to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') <= reg.censusDate)
                or coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))  
    )
where courseRn = 1
--    and (recordActivityDate = CAST('9999-09-09' AS DATE)
--        or (recordActivityDate != CAST('9999-09-09' AS DATE)
--                and courseSectionStatus = 'Active'))
),

CourseSectionScheduleMCR as (
--Returns course scheduling related info for the registration courseSectionNumber. 

select *
from (
	select CourseData.*,
		coalesce(campus.isInternational, false) isInternational,
		coalesce(row_number() over (
				partition by
					CourseData.yearType,
					CourseData.termCode,
					CourseData.partOfTermCode,
					CourseData.personId,
					CourseData.courseSectionNumber,
					CourseData.courseSectionLevel
				order by 
					(case when campus.snapshotDate = CourseData.snapshotDate then 1 else 2 end) asc,
					(case when campus.snapshotDate > CourseData.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
					(case when campus.snapshotDate < CourseData.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc
			), 1) regCampRn
	from (
		select coursesect.yearType yearType,
			coursesect.snapshotDate snapshotDate,
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
            coursesect.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
            coursesect.financialAidEndDate,
            coursesect.sfaReportPriorYear sfaReportPriorYear,
            coursesect.sfaReportSecondPriorYear sfaReportSecondPriorYear,
            coursesect.sfaLargestProgCIPC sfaLargestProgCIPC,
            coursesect.caresAct1 caresAct1,
            coursesect.caresAct2 caresAct2,
			coursesect.residency,
			coursesect.courseSectionNumber courseSectionNumber,
			coursesect.subject subject,
			coursesect.courseNumber courseNumber,
			coursesect.section section,
			coursesect.customDataValue,
			coursesect.isESL, 
			coursesect.isRemedial,
			coursesect.isAudited,
			coursesect.courseSectionCampusOverride,
			coursesect.college,
			coursesect.division,
			coursesect.department,
			coursesect.studyAbroadStatus,
		    coursesect.fullTimePartTimeStatus,
			coursesect.courseSectionLevel courseSectionLevel,
			coursesect.enrollmentHours enrollmentHours,
			coursesect.equivCRHRFactor equivCRHRFactor,
			coursesect.isClockHours isClockHours,
			coalesce(to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
			coalesce(coursesect.courseSectionCampusOverride, upper(coursesectschedENT.campus)) campus, --reg campus prioritized over courseSection campus 
			coursesectschedENT.instructionType,
			coursesectschedENT.locationType,
			coursesectschedENT.distanceEducationType,
			coursesectschedENT.onlineInstructionType,
			coursesectschedENT.maxSeats,
			coalesce(row_number() over (
				partition by
					coursesect.yearType,
					coursesect.termCode, 
					coursesect.partOfTermCode,
					coursesect.personId,
					coursesect.courseSectionNumber,
					coursesectschedENT.courseSectionNumber,
					coursesect.courseSectionLevel,
					coursesect.subject,
					coursesect.courseNumber
				order by
					(case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') = coursesect.snapshotDate then 1 else 2 end) asc,
					(case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') > coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
					(case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') < coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
					coursesectschedENT.recordActivityDate desc
			), 1) courseSectSchedRn
		from CourseSectionMCR coursesect
			left join CourseSectionSchedule coursesectschedENT on coursesect.termCode = upper(coursesectschedENT.termCode) 
                    and coursesect.partOfTermCode = coalesce(upper(coursesectschedENT.partOfTermCode), 1)
                    and coursesect.courseSectionNumber = upper(coursesectschedENT.courseSectionNumber)
                    and coalesce(coursesectschedENT.isIpedsReportable, true) = true 
                    and ((coalesce(to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                        and to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') <= coursesect.censusDate)
                            or coalesce(to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))  
		) CourseData
	    left join CampusMCR campus on campus.campus = CourseData.campus
	where CourseData.courseSectSchedRn = 1
	)
where regCampRn = 1
),

CourseMCR as (
--Included to get course type information

select *
from (
	select coursesectsched.yearType yearType,
	     coursesectsched.snapshotDate snapshotDate,
	    coursesectsched.termCode termCode,
		coursesectsched.partOfTermCode partOfTermCode,
		coursesectsched.financialAidYear,
	    termorder.termOrder courseTermOrder,
	    coursesectsched.termOrder termOrder,
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
        coursesectsched.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
        coursesectsched.financialAidEndDate,
        coursesectsched.sfaReportPriorYear sfaReportPriorYear,
        coursesectsched.sfaReportSecondPriorYear sfaReportSecondPriorYear,
        coursesectsched.sfaLargestProgCIPC sfaLargestProgCIPC,
        coursesectsched.caresAct1 caresAct1,
        coursesectsched.caresAct2 caresAct2,
	    coursesectsched.residency,
		coursesectsched.studyAbroadStatus,
		coursesectsched.fullTimePartTimeStatus,
	    coursesectsched.courseSectionNumber courseSectionNumber,
		coursesectsched.section section,
		coursesectsched.subject subject,
		coursesectsched.courseNumber courseNumber,
		coalesce(coursesectsched.courseSectionLevel, courseENT.courseLevel) courseLevel,
		coursesectsched.isRemedial isRemedial,
		coursesectsched.isESL isESL,
		coursesectsched.isAudited isAudited,
		coursesectsched.customDataValue,
		coalesce(coursesectsched.college, upper(courseENT.courseCollege)) college,
		coalesce(coursesectsched.division, upper(courseENT.courseDivision)) division,
		coalesce(coursesectsched.department, upper(courseENT.courseDepartment)) department,
		coursesectsched.equivCRHRFactor equivCRHRFactor,
		coursesectsched.isInternational isInternational,
		coursesectsched.isClockHours isClockHours,
		(case when coursesectsched.instructionalActivityType = 'CR' then coursesectsched.enrollmentHours
		      when coursesectsched.isClockHours = false then coursesectsched.enrollmentHours
              when coursesectsched.isClockHours = true and coursesectsched.instructionalActivityType = 'B' then coursesectsched.equivCRHRFactor * coursesectsched.enrollmentHours
              else coursesectsched.enrollmentHours end) enrollmentHours,
        coursesectsched.campus,
		coursesectsched.instructionType,
		coursesectsched.locationType,
		coursesectsched.distanceEducationType,
		coursesectsched.onlineInstructionType,
		coursesectsched.maxSeats,
	    coalesce(to_date(courseENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) recordActivityDate,
        courseENT.courseStatus courseStatus,
	    coalesce(row_number() over (
			partition by
			    coursesectsched.yearType,
                coursesectsched.termCode, 
				coursesectsched.partOfTermCode,
                coursesectsched.personId,
			    coursesectsched.courseSectionNumber,
			    coursesectsched.courseSectionLevel,
			    coursesectsched.subject,
                courseENT.subject,
                coursesectsched.courseNumber,
                courseENT.courseNumber
			order by
			    (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') = coursesectsched.snapshotDate then 1 else 2 end) asc,
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') > coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') < coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
			    termorder.termOrder desc,
			    courseENT.recordActivityDate desc
		), 1) courseRn
	from CourseSectionScheduleMCR coursesectsched
	    left join Course courseENT on coursesectsched.subject = upper(courseENT.subject) 
			        and coursesectsched.courseNumber = upper(courseENT.courseNumber)
			        and coalesce(courseENT.isIpedsReportable, true) = true
			        and ((coalesce(to_date(courseENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
				        and to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') <= coursesectsched.censusDate) 
					        or coalesce(to_date(courseENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
		left join AcademicTermOrder termorder on termorder.termCode = upper(courseENT.termCodeEffective)
            and termorder.termOrder <= coursesectsched.termOrder
	)
where courseRn = 1
--    and (recordActivityDate = CAST('9999-09-09' AS DATE)
--        or (recordActivityDate != CAST('9999-09-09' AS DATE)
--                and courseStatus = 'Active'))
),

CourseTypeCountsSTU as (
-- View used to break down course category type counts for student
-- In order to calculate credits and filters per term, do not include censusDate or snapshotDate in any inner view

----SFA mod: added timeStatus filter for only first time, degree-seeking students per Group 1 requirement; no graduate timeStatus calculation

select *,
        (select first(maxCensus) 
                from AcademicTermReportingRefactor acadRep
                where acadRep.termcode = termcode
                and acadRep.partOfTermCode = acadRep.maxPOT) censusDate,
        (select first(snapshotDate)
                from AcademicTermReportingRefactor acadRep
                where acadRep.termcode = termcode
                and acadRep.partOfTermCode = acadRep.maxPOT) snapshotDate,
        (case when timeStatus = 'FT' and isNonDegreeSeeking = false and studentType = 'First Time' then 1 else 0 end) isGroup2Ind
from (
    select *,
            (case when studentType = 'First Time' and isNonDegreeSeeking = false then
                    (case when instructionalActivityType in ('CR', 'B') then 
                                (case when totalCreditHrs >= requiredFTCreditHoursUG then 'FT' else 'PT' end)
                          when instructionalActivityType = 'CL' then 
                                (case when totalClockHrs >= requiredFTClockHoursUG then 'FT' else 'PT' end) 
                          else null end)
                else null end) timeStatus
    from (
        select personId,
                yearType,
                (case when studyAbroadStatus != 'Study Abroad - Home Institution' then isNonDegreeSeeking
                      when totalSAHomeCourses > 0 or totalCreditHrs > 0 or totalClockHrs > 0 then false 
                      else isNonDegreeSeeking 
                  end) isNonDegreeSeeking,
                (case when totalCECourses = totalCourses then 0 --exclude students enrolled only in continuing ed courses
                    when totalIntlCourses = totalCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
                    when totalAuditCourses = totalCourses then 0 --exclude students exclusively auditing classes
                    when totalProfResidencyCourses > 0 then 0 --exclude PHD residents or interns
                    when totalThesisCourses > 0 then 0 --exclude PHD residents or interns
                    when totalRemCourses = totalCourses and isNonDegreeSeeking = false then 1 --include students taking remedial courses if degree-seeking
                    when totalESLCourses = totalCourses and isNonDegreeSeeking = false then 1 --exclude students enrolled only in ESL courses/programs
                    when totalSAHomeCourses > 0 then 1 --include study abroad student where home institution provides resources, even if credit hours = 0
                    when totalCreditHrs > 0 then 1
                    when totalClockHrs > 0 then 1
                    else 0
                 end) ipedsInclude,
                termCode,
                termOrder,
                financialAidYear,
                instructionalActivityType,
                requiredFTCreditHoursUG,
                requiredFTClockHoursUG,
                studentLevel,
                studentType,
                residency,
                ipedsGender,
                ipedsEthnicity,
                repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
                financialAidEndDate,
                sfaReportPriorYear sfaReportPriorYear,
                sfaReportSecondPriorYear sfaReportSecondPriorYear,
                sfaLargestProgCIPC sfaLargestProgCIPC,
                caresAct1 caresAct1,
                caresAct2 caresAct2,
                totalClockHrs,
                totalCreditHrs,
                fullTimePartTimeStatus
        from (
             select course.yearType,
                    course.termCode,
                    course.termOrder,
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
                    course.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
                    course.financialAidEndDate,
                    course.sfaReportPriorYear sfaReportPriorYear,
                    course.sfaReportSecondPriorYear sfaReportSecondPriorYear,
                    course.sfaLargestProgCIPC sfaLargestProgCIPC,
                    course.caresAct1 caresAct1,
                    course.caresAct2 caresAct2,
                    course.studyAbroadStatus,
		            course.fullTimePartTimeStatus,
		            coalesce(count(course.courseSectionNumber), 0) totalCourses,
                    coalesce(sum((case when course.enrollmentHours >= 0 then 1 else 0 end)), 0) totalCreditCourses,
                    coalesce(sum((case when course.isClockHours = false then course.enrollmentHours else 0 end)), 0) totalCreditHrs,
                    coalesce(sum((case when course.isClockHours = true and course.courseLevel = 'Undergraduate' then course.enrollmentHours else 0 end)), 0) totalClockHrs,
                    coalesce(sum((case when course.courseLevel = 'Continuing Education' then 1 else 0 end)), 0) totalCECourses,
                    coalesce(sum((case when course.locationType = 'Foreign Country' then 1 else 0 end)), 0) totalSAHomeCourses, 
                    coalesce(sum((case when course.isESL = true then 1 else 0 end)), 0) totalESLCourses,
                    coalesce(sum((case when course.isRemedial = true then 1 else 0 end)), 0) totalRemCourses,
                    coalesce(sum((case when course.isInternational = true then 1 else 0 end)), 0) totalIntlCourses,
                    coalesce(sum((case when course.isAudited = true then 1 else 0 end)), 0) totalAuditCourses,
                    coalesce(sum((case when course.instructionType = 'Thesis/Capstone' then 1 else 0 end)), 0) totalThesisCourses,
                    coalesce(sum((case when course.instructionType in ('Residency', 'Internship', 'Practicum') and course.studentLevel = 'Professional Practice Doctorate' then 1 else 0 end)), 0) totalProfResidencyCourses
            from CourseMCR course
            group by course.yearType,
                    course.termCode,
                    course.termOrder,
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
                    course.repPeriodTag2, 
                    course.financialAidEndDate,
                    course.sfaReportPriorYear,
                    course.sfaReportSecondPriorYear,
                    course.sfaLargestProgCIPC,
                    course.caresAct1,
                    course.caresAct2,
                    course.studyAbroadStatus,
		            course.fullTimePartTimeStatus
            )
        )
    where ipedsInclude = 1
    )
),

AcademicTrackMCR as (
--Returns most up to date student academic track information for the enrollment term. 

select *
from (
    select distinct person2.personId,
            person2.yearType,
            acadtrackENT.personId atPersonId,
            coalesce(row_number() over (
                partition by
                    person2.personId,
                    acadtrackENT.personId,
                    person2.yearType
                order by
                   (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') = person2.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') > person2.snapshotDate then to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') < person2.snapshotDate then to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    coalesce(acadtrackENT.fieldOfStudyPriority, 1) asc,
                    termorder.termOrder desc,
                    acadtrackENT.recordActivityDate desc,
                    acadtrackENT.fieldOfStudyActionDate desc,
                    (case when acadtrackENT.academicTrackStatus = 'In Progress' then 1 else 2 end) asc
            ), 1) acadTrackRn,
            person2.snapshotDate,
            person2.financialAidYear,
            person2.isGroup2Ind,
            person2.residency,
            person2.termCode,
            person2.termOrder,
            person2.censusDate,
            person2.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
            person2.financialAidEndDate,
            person2.sfaReportPriorYear sfaReportPriorYear,
            person2.sfaReportSecondPriorYear sfaReportSecondPriorYear,
            person2.sfaLargestProgCIPC sfaLargestProgCIPC,
            person2.caresAct1 caresAct1,
            person2.caresAct2 caresAct2,
            upper(acadtrackENT.degreeProgram) degreeProgram,
            upper(acadtrackENT.fieldOfStudy) fieldOfStudy,
            acadtrackENT.academicTrackStatus academicTrackStatus,
            coalesce(to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) fieldOfStudyActionDate,
            coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
            coalesce(acadtrackENT.fieldOfStudyPriority, 1) fieldOfStudyPriority,
            coalesce(acadtrackENT.isCurrentFieldOfStudy, true) isCurrentFieldOfStudy
    from CourseTypeCountsSTU person2
        left join AcademicTrack acadtrackENT on person2.personId = acadTrackENT.personId
            and ((coalesce(to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                        and to_date(acadtrackENT.fieldOfStudyActionDate,'YYYY-MM-DD') <= person2.censusDate)
                    or (coalesce(to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                        and ((coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' as DATE)
                                and to_date(acadtrackENT.recordActivityDate,'YYYY-MM-DD') <= person2.censusDate)
                            or coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' as DATE)))) 
                    and acadTrackENT.fieldOfStudyType = 'Major' 
                    and coalesce(acadTrackENT.isIpedsReportable, true) = true
                    --and coalesce(acadTrackENT.isCurrentFieldOfStudy, true) = true
               left join AcademicTermOrder termorder on termorder.termCode = upper(acadtrackENT.termCodeEffective)
                                    and termorder.termOrder <= person2.termOrder
        )
where acadTrackRn = 1
), 

DegreeProgramMCR as (
--Returns most up to date student academic track information as of their award date and term. 

select *
from (
    select distinct person2.personId,
            person2.yearType,
            coalesce(degProgENT.isESL, false) isESL,
            person2.degreeProgram,
            person2.fieldOfStudy major,
            upper(degprogENT.degreeProgram) DPdegreeProgram,
            upper(degprogENT.major) DPmajor,
            coalesce(row_number() over (
                partition by
                    person2.personId,
                    person2.yearType,
                    person2.degreeProgram,
                    degprogENT.degreeProgram
                order by
                    (case when to_date(degprogENT.snapshotDate, 'YYYY-MM-DD') = person2.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(degprogENT.snapshotDate, 'YYYY-MM-DD') > person2.snapshotDate then to_date(degprogENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(degprogENT.snapshotDate, 'YYYY-MM-DD') < person2.snapshotDate then to_date(degprogENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    termorder.termOrder desc,
                    degprogENT.startDate desc,
                    degprogENT.recordActivityDate desc
            ), 1) degProgRn,
            person2.snapshotDate,
            person2.financialAidYear,
            person2.isGroup2Ind,
            person2.residency,
            person2.termCode,
            person2.termOrder,
            person2.censusDate,
            person2.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
            person2.financialAidEndDate,
            person2.sfaReportPriorYear sfaReportPriorYear,
            person2.sfaReportSecondPriorYear sfaReportSecondPriorYear,
            person2.sfaLargestProgCIPC sfaLargestProgCIPC,
            person2.caresAct1 caresAct1,
            person2.caresAct2 caresAct2
    from AcademicTrackMCR person2
        left join DegreeProgram degprogENT on person2.degreeProgram = upper(degprogENT.degreeProgram)
                   and person2.fieldOfStudy = upper(degprogENT.major)
                   and ((coalesce(to_date(degprogENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                               and to_date(degprogENT.recordActivityDate,'YYYY-MM-DD') <= person2.censusDate)
                         or coalesce(to_date(degprogENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
                                and (degprogENT.startDate is null 
                                    or to_date(degprogENT.startDate, 'YYYY-MM-DD') <= person2.censusDate) 
                  and coalesce(degprogENT.isIpedsReportable, true) = true
         left join AcademicTermOrder termorder on termorder.termCode = upper(degprogENT.termCodeEffective)
                                and termorder.termOrder <= person2.termOrder
        )
where degProgRn = 1
    and isESL = false
),

--*** mod from v1
FieldOfStudyMCR as (

select *
from (
    select person2.personId,
            person2.snapshotDate,
            person2.yearType,
            person2.financialAidYear,
            person2.isGroup2Ind,
            person2.residency,
            person2.termCode,
            person2.termOrder,
            person2.censusDate,
            person2.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
            person2.financialAidEndDate,
            person2.sfaReportPriorYear sfaReportPriorYear,
            person2.sfaReportSecondPriorYear sfaReportSecondPriorYear,
            person2.caresAct1 caresAct1,
            person2.caresAct2 caresAct2,
            (case when fosENT.cipCode = person2.sfaLargestProgCIPC then 1 else 0 end) inLgCipCode,
            coalesce(row_number() over (
                partition by
                    person2.personId,
                    person2.yearType,
                    person2.degreeProgram,
                    person2.DPdegreeProgram
                order by
                    (case when to_date(fosENT.snapshotDate, 'YYYY-MM-DD') = person2.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(fosENT.snapshotDate, 'YYYY-MM-DD') > person2.snapshotDate then to_date(fosENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(fosENT.snapshotDate, 'YYYY-MM-DD') < person2.snapshotDate then to_date(fosENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc
            ), 1) fosRn
    from DegreeProgramMCR person2
            left join FieldOfStudy fosENT on person2.major = upper(fosENT.fieldOfStudy)
                and fosENT.fieldOfStudyType = 'Major'
                and ((coalesce(to_date(fosENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                            and to_date(fosENT.recordActivityDate,'YYYY-MM-DD') <= person2.censusDate)
                        or coalesce(to_date(fosENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
                and coalesce(fosENT.isIpedsReportable, true) = true
    )
where fosRn = 1
),

FinancialAidMCR as (
-- included to get student Financial aid information paid any time during the academic year.
-- Report grant or scholarship aid that was awarded to students. 
-- Report loans that were awarded to and accepted by the student.
-- For public institutions, include only those students paying the in-state or in-district tuition rate. For program reporters, include only those students enrolled in the institution's largest program.

select * ,
    isGroup2Ind isGroup2,
    (case when isGroup2Ind = 1 and group2aTotal > 0 then 1 else 0 end) isGroup2a,
    (case when isGroup2Ind = 1 and group2bTotal > 0 then 1 else 0 end) isGroup2b,
    (case when isGroup2Ind = 1 and group3Total > 0 then 1 else 0 end) isGroup3,
    (case when isGroup2Ind = 1 and group4Total > 0 then 1 else 0 end) isGroup4,
    (case when totalAid > 0 then 1 else 0 end) isAwardedAid, --Count of Group 1 with awarded aid or work study from any source
    (case when pellGrant > 0  then 1 else 0 end) isPellGrant, --Count of Group 1 with awarded PELL grants
    (case when federalLoan > 0  then 1 else 0 end) isFedLoan --Count of Group 1 with awarded and accepted federal loans
from ( 
    select course2.personId personId,
            course2.yearType yearType,
            course2.financialAidYear financialAidYear,
            course2.isGroup2Ind isGroup2Ind,
            first(finaid.livingArrangement) livingArrangement,
            course2.residency residency,
            course2.sfaReportPriorYear sfaReportPriorYear,
            course2.sfaReportSecondPriorYear sfaReportSecondPriorYear,
--*** mod from v1
            course2.inLgCipCode inLgCipCode,
            round(sum(coalesce(case when finaid.fundType = 'Loan' and finaid.fundSource = 'Federal' then finaid.IPEDSFinancialAidAmount end, 0)), 0) federalLoan,
            round(sum(coalesce(case when finaid.fundType in ('Grant', 'Scholarship') and finaid.fundSource = 'Federal' then finaid.IPEDSFinancialAidAmount end, 0)), 0) federalGrantSchol,
            round(sum(coalesce(case when finaid.fundType = 'Grant' and finaid.fundSource = 'Federal' and finaid.fundCode in (course2.caresAct1, course2.caresAct2) then finaid.IPEDSFinancialAidAmount end, 0)), 0) caresFederalGrant,
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
            round(sum(coalesce(case when finaid.isPellGrant = true then finaid.IPEDSFinancialAidAmount end, 0)), 0) pellGrant, 
            round(sum(coalesce(case when finaid.isTitleIV = true then finaid.IPEDSFinancialAidAmount end, 0)), 0) titleIV, 
            round(sum(coalesce(case when finaid.fundType in ('Grant', 'Scholarship') then finaid.IPEDSFinancialAidAmount end, 0)), 0) allGrantSchol, 
            round(sum(coalesce(case when finaid.fundType = 'Loan' then finaid.IPEDSFinancialAidAmount end, 0)), 0) allLoan, 
            round(sum(coalesce(case when finaid.fundType in ('Grant', 'Scholarship') and finaid.fundSource = 'Federal' and finaid.isPellGrant != true then finaid.IPEDSFinancialAidAmount end, 0)), 0) nonPellFederalGrantSchol, 
            round(sum(finaid.IPEDSFinancialAidAmount), 0) totalAid,
            round(sum(case when course2.isGroup2Ind = 1 then finaid.IPEDSFinancialAidAmount end)) group2aTotal,
            round(sum(case when course2.isGroup2Ind = 1 and finaid.fundType in ('Loan', 'Grant', 'Scholarship') and finaid.fundSource in ('Federal', 'State', 'Local', 'Institution') then finaid.IPEDSFinancialAidAmount end)) group2bTotal,
            --round(sum(case when course2.isGroup2Ind = 1 and finaid.fundType in ('Grant', 'Scholarship') and finaid.fundSource in ('Federal', 'State', 'Local', 'Institution') then finaid.IPEDSFinancialAidAmount end)) group3Total, --Pre CARES Act (Revert after CARES Act phases out)
            round(sum(case when course2.isGroup2Ind = 1 and finaid.fundType in ('Grant', 'Scholarship') and finaid.fundSource in ('Federal', 'State', 'Local', 'Institution') and finaid.fundCode not in (course2.caresAct1, course2.caresAct2) then finaid.IPEDSFinancialAidAmount end)) group3Total,
            --round(sum(case when course2.isGroup2Ind = 1 and finaid.isTitleIV = true then finaid.IPEDSFinancialAidAmount end)) group4Total, --Pre CARES Act (Revert after CARES Act phases out)
            round(sum(case when course2.isGroup2Ind = 1 and finaid.isTitleIV = true and finaid.fundCode not in (course2.caresAct1, course2.caresAct2) then finaid.IPEDSFinancialAidAmount end)) group4Total,
            (case when course2.isGroup2Ind = 1 then (case when first(finaid.familyIncome) <= 30000 then 1
                                                        when first(finaid.familyIncome) between 30001 and 48000 then 2
                                                        when first(finaid.familyIncome) between 48001 and 75000 then 3
                                                        when first(finaid.familyIncome) between 75001 and 110000 then 4
                                                        when first(finaid.familyIncome) > 110000 then 5
                                                        else 1 end)
               end) familyIncome
--*** mod from v1
    from FieldOfStudyMCR course2
        left join ( 
            select * 
            from (
                select DISTINCT
                        course.personId personId,
                        course.yearType yearType,
                        course.financialAidYear financialAidYear,
                        FinancialAidENT.fundType fundType,
                        upper(FinancialAidENT.fundCode) fundCode,
                        FinancialAidENT.fundSource fundSource,
                        coalesce(to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivity,
                        case when coalesce(to_date(FinancialAidENT.awardStatusActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE) then coalesce(to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) 
                        else coalesce(to_date(FinancialAidENT.awardStatusActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) end recordDate,
                        coalesce(to_date(FinancialAidENT.awardStatusActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) awardStatusActionDate,
                        upper(FinancialAidENT.termCode) termCode,
                        FinancialAidENT.awardStatus awardStatus,
                        coalesce(FinancialAidENT.isPellGrant, false) isPellGrant,
                        coalesce(FinancialAidENT.isTitleIV, false) isTitleIV,
                        coalesce(FinancialAidENT.isSubsidizedDirectLoan, false) isSubsidizedDirectLoan,
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
                        row_number() over (
                            partition by
                                course.yearType,
                                FinancialAidENT.financialAidYear,
                                FinancialAidENT.termCode,
                                course.personId,
                                FinancialAidENT.fundCode,
                                FinancialAidENT.fundType,
                                FinancialAidENT.fundSource
                            order by 	
                                (case when array_contains(FinancialAidENT.tags, course.repPeriodTag2) then 1 
                                    when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') between date_sub(course.financialAidEndDate, 30) and date_add(course.financialAidEndDate, 10) then 2 else 3 end) asc,	    
                                (case when to_date(FinancialAidENT.snapshotDate,'YYYY-MM-DD') > course.financialAidEndDate then to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                                (case when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') < course.financialAidEndDate then to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                                FinancialAidENT.recordActivityDate desc,
                                FinancialAidENT.awardStatusActionDate desc
                        ) finAidRn 
--*** mod from v1
                    from FieldOfStudyMCR course
                        inner join FinancialAid FinancialAidENT on course.personId = FinancialAidENT.personId
                            and course.financialAidYear = FinancialAidENT.financialAidYear
                            and coalesce(FinancialAidENT.isIpedsReportable, true) = true
                            and ((coalesce(to_date(FinancialAidENT.awardStatusActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                                    and to_date(FinancialAidENT.awardStatusActionDate, 'YYYY-MM-DD') <= course.financialAidEndDate)
                                or (coalesce(to_date(FinancialAidENT.awardStatusActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                                        and ((coalesce(to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  != CAST('9999-09-09' AS DATE)
                                    and to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD') <= course.financialAidEndDate)
                                        or coalesce(to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  = CAST('9999-09-09' AS DATE))))
                    )
                where finAidRn = 1
           ) finaid on course2.personId = finaid.personId
                and course2.yearType = finaid.yearType
                and course2.financialAidYear = finaid.financialAidYear
                 and (finaid.recordDate = CAST('9999-09-09' AS DATE)
                        or (finaid.recordDate != CAST('9999-09-09' AS DATE) and finaid.awardStatus not in ('Source Denied', 'Cancelled')))
    group by course2.personId, 
            course2.yearType, 
            course2.financialAidYear,
            course2.isGroup2Ind,
            course2.residency,
            course2.sfaReportPriorYear,
            course2.sfaReportSecondPriorYear,
--*** mod from v1
            course2.inLgCipCode
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
        startDate, 
        endDate,
        repPeriodTag4, --'GI Bill'
        repPeriodTag5, --'Department of Defense'
        repPeriodTag7 --'Dept of Defense'
from ( 
    select distinct MilitaryBenefitENT.personID personID,
        upper(MilitaryBenefitENT.termCode) termCode,
        MilitaryBenefitENT.benefitType benefitType,
        abs(MilitaryBenefitENT.benefitAmount) benefitAmount,
		to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		to_date(MilitaryBenefitENT.transactionDate, 'YYYY-MM-DD') transactionDate,
        MilitaryBenefitENT.tags tags,
        config.giBillStartDate startDate,
        config.giBillEndDate endDate,
        coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        config.repPeriodTag4 repPeriodTag4, --'GI Bill'
        config.repPeriodTag5 repPeriodTag5, --'Department of Defense'
        config.repPeriodTag7 repPeriodTag7, --'Dept of Defense'
        row_number() over (
            partition by
                MilitaryBenefitENT.personId,
			    MilitaryBenefitENT.benefitType,
			    MilitaryBenefitENT.termCode,
			    MilitaryBenefitENT.transactionDate,
			    MilitaryBenefitENT.benefitAmount
		    order by
				(case when array_contains(MilitaryBenefitENT.tags, config.repPeriodTag4) and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.giBillStartDate, 1) and date_add(config.giBillEndDate, 3) then 1 else 2 end) asc,
				(case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.giBillStartDate, 1) and date_add(config.giBillEndDate, 3) then 3 
				    else 4 end) asc,
                (case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') > config.giBillEndDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') < config.giBillStartDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                MilitaryBenefitENT.recordActivityDate desc
	    ) militarybenefitRn
    from MilitaryBenefit MilitaryBenefitENT
        cross join ClientConfigMCR config
    where coalesce(MilitaryBenefitENT.isIpedsReportable, true) = true 
        and MilitaryBenefitENT.benefitType = config.repPeriodTag4
        and ((coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                and to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') between config.giBillStartDate and config.giBillEndDate
                and MilitaryBenefitENT.transactionDate between config.giBillStartDate and config.giBillEndDate)
            or (coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                and MilitaryBenefitENT.transactionDate between config.giBillStartDate and config.giBillEndDate))

    union

--Dept of Defense
    select distinct MilitaryBenefitENT.personID personID,
        upper(MilitaryBenefitENT.termCode) termCode,
        MilitaryBenefitENT.benefitType benefitType,
        abs(MilitaryBenefitENT.benefitAmount) benefitAmount,
		to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		to_date(MilitaryBenefitENT.transactionDate, 'YYYY-MM-DD') transactionDate,
        MilitaryBenefitENT.tags tags,
        config.dodStartDate startDate,
        config.dodEndDate endDate,
        coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        config.repPeriodTag4 repPeriodTag4, --'GI Bill'
        config.repPeriodTag5 repPeriodTag5, --'Department of Defense'
        config.repPeriodTag7 repPeriodTag7, --'Dept of Defense'
        row_number() over (
            partition by
                MilitaryBenefitENT.personId,
			    MilitaryBenefitENT.benefitType,
			    MilitaryBenefitENT.termCode,
			    MilitaryBenefitENT.transactionDate,
			    MilitaryBenefitENT.benefitAmount
		    order by
				(case when array_contains(MilitaryBenefitENT.tags, config.repPeriodTag5)
				            and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.dodStartDate, 1) and date_add(config.dodEndDate, 3) then 1
				      else 2 end) asc,
				(case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.dodStartDate, 1) and date_add(config.dodEndDate, 3) then 3 
				    else 4 end) asc,
                (case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') > config.dodEndDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') < config.dodStartDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                MilitaryBenefitENT.recordActivityDate desc
	    ) militarybenefitRn
    from MilitaryBenefit MilitaryBenefitENT
        cross join DefaultValues config
    where coalesce(MilitaryBenefitENT.isIpedsReportable, true) = true 
        and MilitaryBenefitENT.benefitType in (config.repPeriodTag5, config.repPeriodTag7)
        and ((coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                and to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') between config.dodStartDate and config.dodEndDate
                and MilitaryBenefitENT.transactionDate between config.dodStartDate and config.dodEndDate)
            or (coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                and MilitaryBenefitENT.transactionDate between config.dodStartDate and config.dodEndDate))
    )   
where militarybenefitRn = 1
group by personId, benefitType, termCode, snapshotDate, startDate, endDate, repPeriodTag4, repPeriodTag5, repPeriodTag7
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
            sum((case when stu.benefitType = stu.repPeriodTag4 then stu.benefitAmount else 0 end)) giBillAmt,
            sum((case when stu.benefitType in (stu.repPeriodTag5, stu.repPeriodTag7) then stu.benefitAmount else 0 end)) dodAmt,
            stu.studentLevel studentLevel,
            (case when stu.benefitType = stu.repPeriodTag4 then 1 else 0 end) giCount,
            (case when stu.benefitType in (stu.repPeriodTag5, stu.repPeriodTag7) then 1 else 0 end) dodCount
    from (
        select distinct miliben.personId personId,
                miliben.termCode termCode,
                (case when studentENT.studentLevel not in ('Undergraduate', 'Continuing Education', 'Other') then 2 else 1 end) studentLevel,
                miliben.benefitType,
                miliben.benefitAmount benefitAmount,
                miliben.repPeriodTag4,
                miliben.repPeriodTag5,
                miliben.repPeriodTag7,
                coalesce(row_number() over (
                    partition by
                        miliben.personId,
                        miliben.termCode,
                        miliben.benefitType,
                        miliben.benefitAmount
                    order by
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') = miliben.snapshotDate then 1 else 2 end) asc,
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') > miliben.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else miliben.snapshotDate end) asc,
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') < miliben.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                        studentENT.recordActivityDate desc
                ), 1) studRn
        from MilitaryBenefitMCR miliben
            left join Student studentENT on miliben.personId = studentENT.personId
                and miliben.termCode = upper(studentENT.termCode)
                and ((coalesce(to_date(studentENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)  
                    and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= miliben.endDate)
                        or coalesce(to_date(studentENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
                and coalesce(studentENT.isIpedsReportable, true) = true
        ) stu
    where stu.studRn = 1 
    group by stu.personId, stu.studentLevel, stu.benefitType, stu.repPeriodTag4, stu.repPeriodTag5, stu.repPeriodTag7
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
),

--Part G	
FormatPartGLevel as (
select *
from (
	VALUES
		(1), -- Undergraduate
		(2) -- Graduate
	) as studentLevel(ipedsLevel)
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

--*** mod from v1
select PART,
        null FIELD2_1, --Academic reporters only
        coalesce(sum(FIELD3_1), 0) FIELD3_1, --Program reporters only
        coalesce(sum(FIELD4_1), 0) FIELD4_1,
        coalesce(sum(FIELD5_1), 0) FIELD5_1,
        coalesce(sum(FIELD6_1), 0) FIELD6_1,
        ROUND(coalesce(sum(FIELD7_1), 0)) FIELD7_1,
        ROUND(coalesce(sum(FIELD8_1), 0)) FIELD8_1,
        ROUND(coalesce(sum(FIELD9_1), 0)) FIELD9_1,
        null FIELD10_1,
        null FIELD11_1,
        null FIELD12_1,
        null FIELD13_1,
        null FIELD14_1,
        null FIELD15_1,
        null FIELD16_1
from (
    select 'A' PART,
           null FIELD2_1, --Public and private academic reporters - Count of Group 1
           count(*) FIELD3_1, --Program reporters - Count of unduplicated Group 1
           SUM(isAwardedAid) FIELD4_1, --Count of Group 1 with awarded aid or work study from any source
           SUM(isPellGrant) FIELD5_1, --Count of Group 1 with awarded PELL grants
           SUM(isFedLoan) FIELD6_1, --Count of Group 1 with awarded and accepted federal loans
           SUM(allGrantSchol) FIELD7_1, --Total grant aid amount awarded to Group 1 from all sources
           SUM(pellGrant) FIELD8_1, --Total PELL grant amount awarded to Group 1
           SUM(federalLoan) FIELD9_1 --Total federal loan amount awarded and accepted by Group 1
    from FinancialAidMCR
    where yearType = 'CY'
    
    union
    
    select 'A', 
            null, --Academic reporters only
            0, --Program reporters only
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

--*** mod from v1
select PART,
        null FIELD2_1, --Academic reporters
        null FIELD3_1, --Public reporters
        null FIELD4_1, --Public reporters
        null FIELD5_1, --Public reporters
        coalesce(sum(FIELD6_1), 0) FIELD6_1, --Program reporters
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
    select 'B' PART, 
            null FIELD2_1, --Public and private academic reporters - Count of Group 2 --SUM(isGroup2)
            null FIELD3_1, --Public reporters
            null FIELD4_1, --Public reporters
            null FIELD5_1, --Public reporters
            sum(cohortstu.isGroup2) FIELD6_1, --Program reporters - Count of unduplicated Group 2, -2 or blank = not-applicable
            SUM(cohortstu.isGroup2a) FIELD7_1, --Count of Group 2a, -2 or blank = not-applicable ***have not figured out a scenario where this would be null/not applicable
            SUM(cohortstu.isGroup2b) FIELD8_1 --Count of Group 2b, -2 or blank = not-applicable ***have not figured out a scenario where this would be null/not applicable
    from FinancialAidMCR cohortstu
    where cohortstu.yearType = 'CY'
        and cohortstu.isGroup2 = 1
            
    union
    
    select 'B',
            null, --Academic reporters
            null, --Public reporters
            null, --Public reporters
            null, --Public reporters
            0, --Program reporters
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
--       For program reporters, include only those students enrolled in the institution's largest program.
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
            (case when cohortstu.sfaReportPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY1' then isGroup3 end), 0) else null end) FIELD3_1, --Count of Group 3 prior year, 0-999999
            (case when cohortstu.sfaReportSecondPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY2' then isGroup3 end), 0) else null end) FIELD4_1, --Count of Group 3 prior2 year, 0-999999
            coalesce(SUM(case when cohortstu.yearType = 'CY' and cohortstu.livingArrangement = 'On Campus' then isGroup3 end), 0) FIELD5_1, --Count of Group 3 current year living on campus, 0-999999
            (case when cohortstu.sfaReportPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY1' and cohortstu.livingArrangement = 'On Campus' then isGroup3 end), 0) else null end) FIELD6_1, --Count of Group 3 prior year living on campus, 0-999999
            (case when cohortstu.sfaReportSecondPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY2' and cohortstu.livingArrangement = 'On Campus' then isGroup3 end), 0) else null end) FIELD7_1, --Count of Group 3 prior2 year living on campus, 0-999999
            coalesce(SUM(case when cohortstu.yearType = 'CY' and cohortstu.livingArrangement = 'Off Campus with Family' then isGroup3 end), 0) FIELD8_1, --Count of Group 3 current year living off campus with family, 0-999999
            (case when cohortstu.sfaReportPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY1' and cohortstu.livingArrangement = 'Off Campus with Family' then isGroup3 end), 0) else null end) FIELD9_1, --Count of Group 3 prior year living off campus with family, 0-999999
            (case when cohortstu.sfaReportSecondPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY2' and cohortstu.livingArrangement = 'Off Campus with Family' then isGroup3 end), 0) else null end) FIELD10_1, --Count of Group 3 prior2 year living off campus with family, 0-999999
            coalesce(SUM(case when cohortstu.yearType = 'CY' and cohortstu.livingArrangement = 'Off Campus' then isGroup3 end), 0) FIELD11_1, --Count of Group 3 current year living off campus not with family, 0-999999
            (case when cohortstu.sfaReportPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY1' and cohortstu.livingArrangement = 'Off Campus' then isGroup3 end), 0) else null end) FIELD12_1, --Count of Group 3 prior year living off campus not with family, 0-999999
            (case when cohortstu.sfaReportSecondPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY2' and cohortstu.livingArrangement = 'Off Campus' then isGroup3 end), 0) else null end) FIELD13_1, --Count of Group 3 prior2 year living off campus not with family, 0-999999
            ROUND(coalesce(SUM(case when cohortstu.yearType = 'CY' then cohortstu.group3Total end), 0)) FIELD14_1, --Total aid for Group 3 current year, 0-999999999999
            ROUND((case when cohortstu.sfaReportPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY1' then cohortstu.group3Total end), 0) else null end)) FIELD15_1, --Total aid for Group 3 prior year, 0-999999999999
            ROUND((case when cohortstu.sfaReportSecondPriorYear = 'Y' then coalesce(SUM(case when cohortstu.yearType = 'PY2' then cohortstu.group3Total end), 0) else null end)) FIELD16_1  --Total aid for Group 3 prior2 year, 0-999999999999
    from FinancialAidMCR cohortstu
    where cohortstu.isGroup3 = 1
--*** mod from v1
        and cohortstu.inLgCipCode = 1 --program reporters - include only those students enrolled in the institution's largest program.
    group by cohortstu.sfaReportPriorYear, cohortstu.sfaReportSecondPriorYear
    
    union
    
    select 'D',
            0 FIELD2_1,
            (case when sfaReportPriorYear = 'Y' then 0 else null end) FIELD3_1,
            (case when sfaReportSecondPriorYear = 'Y' then 0 else null end) FIELD4_1,
            0 FIELD5_1,
            (case when sfaReportPriorYear = 'Y' then 0 else null end) FIELD6_1,
            (case when sfaReportSecondPriorYear = 'Y' then 0 else null end) FIELD7_1,
            0 FIELD8_1,
            (case when sfaReportPriorYear = 'Y' then 0 else null end) FIELD9_1,
            (case when sfaReportSecondPriorYear = 'Y' then 0 else null end) FIELD10_1, 
           0 FIELD11_1,
           (case when sfaReportPriorYear = 'Y' then 0 else null end) FIELD12_1,
           (case when sfaReportSecondPriorYear = 'Y' then 0 else null end) FIELD13_1,
           0 FIELD14_1,
           (case when sfaReportPriorYear = 'Y' then 0 else null end) FIELD15_1,
           (case when sfaReportSecondPriorYear = 'Y' then 0 else null end) FIELD16_1
    from clientConfigMCR
    
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
--Academic Years: 1=academic year 2021-20, 2=academic year 2018-19, 3=academic year 2017-18. If you have previously reported prior year values to IPEDS, report only YEAR=1.
--Number of students: 0-999999
--       For public institutions, include only those students paying the in-state or in-district tuition rate.
--       For program reporters, include only those students enrolled in the institution's largest program.

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
                  when cohortstu.yearType = 'PY1' and cohortstu.sfaReportPriorYear = 'Y' then 2
                  when cohortstu.yearType = 'PY2' and cohortstu.sfaReportSecondPriorYear = 'Y' then 3 
            end) yearType
    from FinancialAidMCR cohortstu
            
    where cohortstu.isGroup4 = 1
        and cohortstu.TitleIV > 0
--*** mod from v1
        and cohortstu.inLgCipCode = 1 --program reporters - include only those students enrolled in the institution's largest program.
    
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
              when yearType = 'PY1' and sfaReportPriorYear = 'Y' then 2
              when yearType = 'PY2' and sfaReportSecondPriorYear = 'Y' then 3 
        end) FIELD2_1, --Acad Year, prior
       familyIncome FIELD3_1, --Income range of Group 4 students (values 1 - 5)
       coalesce(sum(isGroup4), 0) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
-- Emergency grants funded through the CARES Act should be NOT included for Group 4 in Part E under ???grant or scholarship aid from the following sources: 
--     the federal government, state/local government, or the institution,???as inclusion of these grants would skew net price calculations.
       coalesce(SUM(case when (allGrantSchol - caresFederalGrant) > 0 then isGroup4 else 0 end), 0) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid 
       ROUND(coalesce(SUM(allGrantSchol) - SUM(caresFederalGrant), 0)) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
            cohortstu.caresFederalGrant caresFederalGrant,
            cohortstu.yearType yearType,
            cohortstu.sfaReportPriorYear sfaReportPriorYear,
            cohortstu.sfaReportSecondPriorYear sfaReportSecondPriorYear
    from FinancialAidMCR cohortstu
    where cohortstu.isGroup4 = 1
        and cohortstu.TitleIV > 0
--*** mod from v1
        and cohortstu.inLgCipCode = 1 --program reporters - include only those students enrolled in the institution's largest program.

    union

    select null, -- personId
            0, --isGroup4
            studentIncome.ipedsIncome, --familyIncome
            0, --allGrantSchol
            0, --caresFederalGrant
            'CY', --yearType
            null,
            null
    from FormatPartFStudentIncome studentIncome
    
    union

    select null, -- personId
            0, --isGroup4
            studentIncome.ipedsIncome, --familyIncome
            0, --allGrantSchol
            0, --caresFederalGrant
            'PY1', --yearType
            'Y',
            null
    from FormatPartFStudentIncome studentIncome
    where (select sfaReportPriorYear
                from clientConfigMCR) = 'Y'
                
    union

    select null, -- personId
            0, --isGroup4
            studentIncome.ipedsIncome, --familyIncome
            0, --allGrantSchol
            0, --caresFederalGrant
            'PY2', --yearType
            null,
            'Y'
    from FormatPartFStudentIncome studentIncome
    where (select sfaReportSecondPriorYear
                from clientConfigMCR) = 'Y'
    )
group by yearType, familyIncome, sfaReportPriorYear, sfaReportSecondPriorYear


union

--Part G Section 2: Veteran's Benefits 

--Valid values
--Student level: 1=Undergraduate, 2=Graduate, 3=Total (Total will be generated)
--Number of students: 0 to 999999, -2 or blank = not-applicable
--Total amount of aid: 0 to 999999999999, -2 or blank = not-applicable

select 'G' PART,
       FIELD2_1 FIELD2_1,
       sum(FIELD3_1) FIELD3_1,
       sum(FIELD4_1) FIELD4_1,
       sum(FIELD5_1) FIELD5_1,
       sum(FIELD6_1) FIELD6_1,
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
--if institution offers each level, count military benefits; if none, output 0
    select mililevl.studentLevel FIELD2_1, --Student Level 1=Undergraduate, 2=Graduate
           coalesce(mililevl.giCount) FIELD3_1, --Post-9/11 GI Bill Benefits - Number of students receiving benefits/assistance
           (case when mililevl.giCount > 0 then ROUND(coalesce(mililevl.giBillAmt, 0)) else null end) FIELD4_1, --Post-9/11 GI Bill Benefits - Total dollar amount of benefits/assistance disbursed through the institution
           coalesce(mililevl.dodCount) FIELD5_1, --Department of Defense Tuition Assistance Program - Number of students receiving benefits/assistance
           (case when mililevl.dodCount > 0 then ROUND(coalesce(mililevl.dodAmt, 0)) else null end) FIELD6_1 --Department of Defense Tuition Assistance Program - Total dollar amount of benefits/assistance disbursed through the institution
    from MilitaryStuLevel mililevl
    where ((mililevl.studentLevel = 1
            and (select icOfferUndergradAwardLevel
                    from ClientConfigMCR) = 'Y')
        or (mililevl.studentLevel = 2
            and (select icOfferGraduateAwardLevel
                    from ClientConfigMCR) = 'Y'))
        
 union

    select studentLevel.ipedsLevel,
        (case when studentLevel.ipedsLevel = 1 and icOfferUndergradAwardLevel = 'Y' then 0
              when studentLevel.ipedsLevel = 2 and icOfferGraduateAwardLevel = 'Y' then 0 
         else null end),
        (case when studentLevel.ipedsLevel = 1 and icOfferUndergradAwardLevel = 'Y' then 0
              when studentLevel.ipedsLevel = 2 and icOfferGraduateAwardLevel = 'Y' then 0 
         else null end),
        (case when studentLevel.ipedsLevel = 1 and icOfferUndergradAwardLevel = 'Y' then 0
              when studentLevel.ipedsLevel = 2 and icOfferGraduateAwardLevel = 'Y' then 0 
         else null end),
        (case when studentLevel.ipedsLevel = 1 and icOfferUndergradAwardLevel = 'Y' then 0
              when studentLevel.ipedsLevel = 2 and icOfferGraduateAwardLevel = 'Y' then 0 
         else null end)
    from FormatPartGLevel studentLevel
        cross join ClientConfigMCR
    
)

group by FIELD2_1
