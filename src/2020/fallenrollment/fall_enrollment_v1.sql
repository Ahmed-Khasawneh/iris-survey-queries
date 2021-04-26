/********************

EVI PRODUCT:    DORIS 2020-21 IPEDS Survey Spring Collection
FILE NAME:      Fall Enrollment v1 (EF1)
FILE DESC:      Fall Enrollment for 4-year, degree-granting institutions
AUTHOR:         Ahmed Khasawneh
CREATED:        20210420

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
20210420    	akhasawneh 									Initial version 
	
********************/

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.

--Production Default (Begin)
select '2021' surveyYear, 
	'EF1' surveyId,
	'Fall Census' repPeriodTag1, --Academic Reporters
	'Pre-Fall Summer Census' repPeriodTag2, --Academic Reporters, if applicable
	'HR Reporting End' repPeriodTag3, --Student to Faculty Ratio, use 20-21 HR
	--'October End' repPeriodTag4, --Program Reporters
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2019-08-01' AS DATE) reportingDateStart, --Program Reporters
	CAST('2020-10-31' AS DATE) reportingDateEnd, --Program Reporters
	'202010' termCode, --Fall 2020
	'1' partOfTermCode,
	'M' genderForUnknown,   --‘Valid values: M = Male, F = Female; Default value (if no record or null value): M’
	'F' genderForNonBinary, --‘Valid values: M = Male, F = Female; Default value (if no record or null value): F’
    'CR' instructionalActivityType, --‘Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR’
	CAST('2020-11-01' AS DATE) asOfDateHR,
	'Y' feincludeoptsurveydata,
    'Cohort' surveySection

union

select '2021' surveyYear, 
	'EF1' surveyId,
	'Fall Census' repPeriodTag1, --Academic Reporters
	'Pre-Fall Summer Census' repPeriodTag2, --Academic Reporters, if applicable
	'HR Reporting End' repPeriodTag3, --Student to Faculty Ratio, use 20-21 HR
	--'October End' repPeriodTag4, --Program Reporters
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2019-08-01' AS DATE) reportingDateStart, --Program Reporters
	CAST('2020-10-31' AS DATE) reportingDateEnd, --Program Reporters
	'201930' termCode, --Fall 2020
	'1' partOfTermCode,
	'M' genderForUnknown,   --‘Valid values: M = Male, F = Female; Default value (if no record or null value): M’
	'F' genderForNonBinary, --‘Valid values: M = Male, F = Female; Default value (if no record or null value): F’
    'CR' instructionalActivityType, --‘Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR’
	CAST('2020-11-01' AS DATE) asOfDateHR,
	'Y' feincludeoptsurveydata,
    'Prior Summer' surveySection
    
union

select '2021' surveyYear, 
	'EF1' surveyId,
	'Fall Census' repPeriodTag1, --Academic Reporters
	'Pre-Fall Summer Census' repPeriodTag2, --Academic Reporters, if applicable
	'HR Reporting End' repPeriodTag3, --Student to Faculty Ratio, use 20-21 HR
	--'October End' repPeriodTag4, --Program Reporters
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2018-08-01' AS DATE) reportingDateStart, --Program Reporters
	CAST('2019-10-31' AS DATE) reportingDateEnd, --Program Reporters
	'201910' termCode, --Fall 2020
	'1' partOfTermCode,
	'M' genderForUnknown,   --‘Valid values: M = Male, F = Female; Default value (if no record or null value): M’
	'F' genderForNonBinary, --‘Valid values: M = Male, F = Female; Default value (if no record or null value): F’
    'CR' instructionalActivityType, --‘Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR’
	CAST('2019-11-01' AS DATE) asOfDateHR,
	'Y' feincludeoptsurveydata,
    'Prior Year 1 Cohort' surveySection

union

select '2021' surveyYear, 
	'EF1' surveyId,
	'Fall Census' repPeriodTag1, --Academic Reporters
	'Pre-Fall Summer Census' repPeriodTag2, --Academic Reporters, if applicable
	'HR Reporting End' repPeriodTag3, --Student to Faculty Ratio, use 20-21 HR
	--'October End' repPeriodTag4, --Program Reporters
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2018-08-01' AS DATE) reportingDateStart, --Program Reporters
	CAST('2019-10-31' AS DATE) reportingDateEnd, --Program Reporters
	'201830' termCode, --Fall 2020
	'1' partOfTermCode,
	'M' genderForUnknown,   --‘Valid values: M = Male, F = Female; Default value (if no record or null value): M’
	'F' genderForNonBinary, --‘Valid values: M = Male, F = Female; Default value (if no record or null value): F’
    'CR' instructionalActivityType, --‘Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR’
	CAST('2019-11-01' AS DATE) asOfDateHR,
	'Y' feincludeoptsurveydata,
    'Prior Year 1 Prior Summer' surveySection
--Production Default (End)

/*
--Test Default (Begin)
select '1415' surveyYear, 
	'EF1' surveyId,
	'Fall Census' repPeriodTag1, --Academic Reporters
	'Pre-Fall Summer Census' repPeriodTag2, --Academic Reporters, if applicable
	'HR Reporting End' repPeriodTag3, --Student to Faculty Ratio, use 20-21 HR
	--'October End' repPeriodTag4, --Program Reporters
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2013-08-01' AS DATE) reportingDateStart, --Program Reporters
	CAST('2014-10-31' AS DATE) reportingDateEnd, --Program Reporters
	'201510' termCode, --Fall 2014
	'1' partOfTermCode,
	'M' genderForUnknown,   --‘Valid values: M = Male, F = Female; Default value (if no record or null value): M’
	'F' genderForNonBinary, --‘Valid values: M = Male, F = Female; Default value (if no record or null value): F’
    'CR' instructionalActivityType, --‘Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR’
	CAST('2014-11-01' AS DATE) asOfDateHR,
	'Y' feincludeoptsurveydata,
    'Cohort' surveySection

union

select '1415' surveyYear, 
	'EF1' surveyId,
	'Fall Census' repPeriodTag1, --Academic Reporters
	'Pre-Fall Summer Census' repPeriodTag2, --Academic Reporters, if applicable
	'HR Reporting End' repPeriodTag3, --Student to Faculty Ratio, use 20-21 HR
	--'October End' repPeriodTag4, --Program Reporters
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2013-08-01' AS DATE) reportingDateStart, --Program Reporters
	CAST('2014-10-31' AS DATE) reportingDateEnd, --Program Reporters
	'201430' termCode, --Fall 2014
	'1' partOfTermCode,
	'M' genderForUnknown,   --‘Valid values: M = Male, F = Female; Default value (if no record or null value): M’
	'F' genderForNonBinary, --‘Valid values: M = Male, F = Female; Default value (if no record or null value): F’
    'CR' instructionalActivityType, --‘Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR’
	CAST('2014-11-01' AS DATE) asOfDateHR,
	'Y' feincludeoptsurveydata,
    'Prior Summer' surveySection

union

select '1415' surveyYear, 
	'EF1' surveyId,
	'Fall Census' repPeriodTag1, --Academic Reporters
	'Pre-Fall Summer Census' repPeriodTag2, --Academic Reporters, if applicable
	'HR Reporting End' repPeriodTag3, --Student to Faculty Ratio, use 20-21 HR
	--'October End' repPeriodTag4, --Program Reporters
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2013-08-01' AS DATE) reportingDateStart, --Program Reporters
	CAST('2014-10-31' AS DATE) reportingDateEnd, --Program Reporters
	'201410' termCode, --Fall 2014
	'1' partOfTermCode,
	'M' genderForUnknown,   --‘Valid values: M = Male, F = Female; Default value (if no record or null value): M’
	'F' genderForNonBinary, --‘Valid values: M = Male, F = Female; Default value (if no record or null value): F’
    'CR' instructionalActivityType, --‘Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR’
	CAST('2014-11-01' AS DATE) asOfDateHR,
	'Y' feincludeoptsurveydata,
    'Prior Year 1 Cohort' surveySection

union

select '1415' surveyYear, 
	'EF1' surveyId,
	'Fall Census' repPeriodTag1, --Academic Reporters
	'Pre-Fall Summer Census' repPeriodTag2, --Academic Reporters, if applicable
	'HR Reporting End' repPeriodTag3, --Student to Faculty Ratio, use 20-21 HR
	--'October End' repPeriodTag4, --Program Reporters
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2013-08-01' AS DATE) reportingDateStart, --Program Reporters
	CAST('2014-10-31' AS DATE) reportingDateEnd, --Program Reporters
	'201330' termCode, --Fall 2014
	'1' partOfTermCode,
	'M' genderForUnknown,   --‘Valid values: M = Male, F = Female; Default value (if no record or null value): M’
	'F' genderForNonBinary, --‘Valid values: M = Male, F = Female; Default value (if no record or null value): F’
    'CR' instructionalActivityType, --‘Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR’
	CAST('2014-11-01' AS DATE) asOfDateHR,
	'Y' feincludeoptsurveydata,
    'Prior Year 1 Prior Summer' surveySection
--Test Default (End)
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
    to_date(RepDates.snapshotDate, 'YYYY-MM-DD') snapshotDate,
    RepDates.termCode termCode,	
	coalesce(RepDates.partOfTermCode, 1) partOfTermCode,
	to_date(RepDates.reportingDateStart, 'YYYY-MM-DD') reportingDateStart,
    to_date(RepDates.reportingDateEnd, 'YYYY-MM-DD') reportingDateEnd,
    RepDates.repPeriodTag1 repPeriodTag1,
	RepDates.repPeriodTag2 repPeriodTag2
from (
    select repperiodENT.surveyCollectionYear surveyYear,
	    'IPEDSReportingPeriod' source,
		repperiodENT.snapshotDate snapshotDate,
		defvalues.surveyId surveyId,
		repPeriodENT.surveySection surveySection,
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		coalesce(repperiodENT.termCode, defvalues.termCode) termCode,
		coalesce(repperiodENT.partOfTermCode, defvalues.partOfTermCode) partOfTermCode,
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
		defvalues.surveySection surveySection,
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		defvalues.termCode termCode,
		defvalues.partOfTermCode partOfTermCode, 
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
		1 reportPeriodRn
	from DefaultValues defvalues
    where defvalues.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = defvalues.surveyYear
											and upper(repperiodENT.surveyId) = defvalues.surveyId 
											and repperiodENT.termCode is not null
										) 
    ) RepDates
where RepDates.reportPeriodRn = 1
),

ClientConfigMCR as ( 
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 
--          1st union 1st order - pull snapshot for 'June End' 
--          1st union 2nd order - pull snapshot for 'August End'
--          1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--          2nd union - pull default values if no record in IPEDSClientConfig

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.source source,
    to_date(ConfigLatest.snapshotDate, 'YYYY-MM-DD') snapshotDate,
    ConfigLatest.repperiodSnapshotDate repperiodSnapshotDate,
	upper(ConfigLatest.genderForUnknown) genderForUnknown,
	upper(ConfigLatest.genderForNonBinary) genderForNonBinary,
    upper(ConfigLatest.instructionalActivityType) instructionalActivityType,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
	ConfigLatest.repPeriodTag2 repPeriodTag2,
    ConfigLatest.repPeriodTag3 repPeriodTag3,
	case when mod(ConfigLatest.surveyYear,2) = 0 then 'Y' --then odd first year and age is required
		else upper(ConfigLatest.feIncludeOptSurveyData)
	end reportAge,
	case when mod(ConfigLatest.surveyYear,2) != 0 then 'Y' --then even first year and resid is required
		else upper(ConfigLatest.feIncludeOptSurveyData)
	end reportResidency,
	ConfigLatest.asOfDateHR asOfDateHR,
    upper(ConfigLatest.feincludeoptsurveydata) feincludeoptsurveydata,
    ConfigLatest.surveyId surveyId
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'configFullYearTag' source,
		clientConfigENT.snapshotDate snapshotDate, 
		repperiod.snapshotDate repperiodSnapshotDate,
		coalesce(clientConfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		coalesce(clientConfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
        coalesce(clientConfigENT.instructionalActivityType, defvalues.instructionalActivityType) instructionalActivityType,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    defvalues.repPeriodTag3 repPeriodTag3,
		defvalues.asOfDateHR asOfDateHR,
	    defvalues.feincludeoptsurveydata feincludeoptsurveydata,
	    defvalues.surveyId surveyId,
		row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
			    (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
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
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    defvalues.repPeriodTag3 repPeriodTag3,
		defvalues.asOfDateHR asOfDateHR,
	    defvalues.feincludeoptsurveydata feincludeoptsurveydata,
	    defvalues.surveyId surveyId, 
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
               coalesce(acadTermENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
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
	where coalesce(acadtermENT.isIPEDSReportable, true) = true
	)
where acadTermRn = 1
),

AcademicTermOrder as ( 
-- Orders term codes based on date span and keeps the numeric value of the greatest term/part of term record. 

select termCode termCode, 
    max(termOrder) termOrder,
    to_date(max(censusDate),'YYYY-MM-DD') maxCensus,
    to_date(min(startDate),'YYYY-MM-DD') minStart,
    to_date(max(endDate),'YYYY-MM-DD') maxEnd,
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
		repPerTerms.instructionalActivityType,
	    repPerTerms.equivCRHRFactor equivCRHRFactor,
	    repPerTerms.genderForUnknown genderForUnknown,
	    repPerTerms.genderForNonBinary genderForNonBinary,
        (case when repPerTerms.termClassification = 'Standard Length' then 1
             when repPerTerms.termClassification is null then (case when repPerTerms.termType in ('Fall', 'Spring') then 1 else 2 end)
             else 2
        end) fullTermOrder
from (
select distinct repperiod.surveySection surveySection,
        repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        acadterm.snapshotDate acadTermSSDate,
        repperiod.snapshotDate repPeriodSSDate,
        repperiod.reportingDateStart reportingDateStart,
        repperiod.reportingDateEnd reportingDateEnd,
        acadterm.tags tags,
        (case when repperiod.surveySection in ('COHORT', 'PRIOR SUMMER') then 'CY'
            when repperiod.surveySection in ('PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER') then 'PY' end) yearType,
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
		clientconfig.genderForUnknown genderForUnknown,
	    clientconfig.genderForNonBinary genderForNonBinary,
		row_number() over (
            partition by 
                repperiod.termCode,
                repperiod.partOfTermCode
            order by
                (case when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') 
                            
                            and ((array_contains(acadterm.tags, 'Fall Census') and acadterm.termType = 'Fall' and repperiod.surveySection in ('COHORT', 'PRIOR YEAR 1 COHORT'))
                                or (array_contains(acadterm.tags, 'Pre-Fall Summer Census') and acadterm.termType = 'Summer' and repperiod.surveySection in ('PRIOR SUMMER','PRIOR YEAR 1 PRIOR SUMMER'))) then 1
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
    ) repPerTerms
where repPerTerms.acadTermRnReg = 1 
),

AcademicTermReportingRefactor as ( 
--Returns all records from AcademicTermReporting, converts Summer terms to Pre-Fall or Post-Spring and creates reportingDateStart/End

select rep.*,
        --(select first(financialAidYear) from AcademicTermReporting where termType = 'Fall') financialAidYearFall,
        (select first(termCode) from AcademicTermReporting where termType = 'Fall') termCodeFall,
        (select first(termOrder) from AcademicTermReporting where termType = 'Fall') termCodeFallOrder,
        (select first(startDate) from AcademicTermReporting where termType = 'Fall' and partOfTermCode = '1') termStartDateFall,
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

/* 
NO LONGER NEED? asOfDateHR IS AN IPEDS DEFINED VALUE WE ARE PULLING THIS DIRECLY FOR THE DEFAULT VALUES INTO ClientConfigMCR
ReportingPeriodMCR_HR as (
--Returns client specified reporting period for HR reporting and defaults to IPEDS specified date range if not otherwise defined 
),
*/

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusMCR as ( --11s
-- Returns most recent campus record for all snapshots in the ReportingPeriod

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

RegistrationMCR as ( --1m 14s
--COHORT-538, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-688, PRIOR YEAR 1 PRIOR SUMMER-114
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 
--Returns valid 'Enrolled' registration records for all reportable terms from current and prior year cohort.

select *
from (
    select regData.yearType, 
        regData.surveySection,
        regData.snapshotDate,
        regData.termCode,
        regData.partOfTermCode,
        regData.regENTSSD regENTSSD,
        regData.repSSD repSSD,
        --campus.snapshotDate campusSSD,
        regData.termorder,
        regData.genderForUnknown,
        regData.genderForNonBinary,
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
		regData.courseSectionNumber,
		regData.enrollmentHoursOverride,
        regData.termStartDateFall termStartDateFall,
        regData.courseSectionCampusOverride,
        regData.isAudited, 
		regData.courseSectionLevelOverride
    from ( 
        select regENT.personId personId,
            repperiod.snapshotDate snapshotDate,
            to_date(regENT.snapshotDate,'YYYY-MM-DD') regENTSSD,
            repperiod.snapshotDate repSSD,
			upper(regENT.termCode) termCode,
			coalesce(upper(regENT.partOfTermCode), 1) partOfTermCode, 
            repperiod.surveySection surveySection,
            repperiod.termorder termorder,
            repperiod.genderForUnknown,
            repperiod.genderForNonBinary,
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
            repperiod.termStartDateFall termStartDateFall,
            upper(regENT.courseSectionCampusOverride) courseSectionCampusOverride,
			coalesce(regENT.isAudited, false) isAudited,
			coalesce(regENT.isEnrolled, true) isEnrolled,
			regENT.courseSectionLevelOverride courseSectionLevelOverride,
            upper(regENT.courseSectionNumber) courseSectionNumber,
            regENT.enrollmentHoursOverride enrollmentHoursOverride,
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
    )
),

StudentMCR as ( --1m 20s
--COHORT-538, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-688, PRIOR YEAR 1 PRIOR SUMMER-114
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  
--Includes student data for both current and prior year cohort.

select stuData.yearType,
        stuData.surveySection,
        stuData.snapshotDate,
        stuData.termCode, 
        stuData.termOrder,
        stuData.maxCensus,
        stuData.termType,
        stuData.startDate,
        stuData.censusDate,
        stuData.genderForUnknown,
        stuData.genderForNonBinary,
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
		studata.studyAbroadStatus
from ( 
	 select reg.yearType yearType,
            reg.snapshotDate snapshotDate,
            to_date(studentENT.snapshotDate,'YYYY-MM-DD') stuSSD,
            reg.surveySection surveySection,
            reg.termCode termCode, 
            reg.termOrder termOrder,
            reg.censusDate censusDate,
            reg.genderForUnknown,
            reg.genderForNonBinary,
            reg.maxCensus maxCensus,
            reg.termType termType,
            reg.startDate startDate,
            reg.fullTermOrder fullTermOrder, --1 for 'full' (standard), 2 for non-standard
            studentENT.personId personId,
            studentENT.isNonDegreeSeeking isNonDegreeSeeking,
            studentENT.studentLevel studentLevel,
            studentENT.studentType studentType,
            studentENT.residency residency,
            studentENT.homeCampus homeCampus,
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

StudentRefactor as ( --1m 35s
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114 **Lost records here due to "isInternational = false" filter.
--Determine student info based on full term and degree-seeking status
--Drop surveySection from select fields and use yearType only going forward - Prior Summer sections only used to determine student type

select *
from ( 
    select FallStu.personId personId,
            FallStu.surveySection surveySection,
            FallStu.termCode termCode,
            FallStu.yearType yearType,
            (case when FallStu.studentType = 'Returning' and SumStu.personId is not null then 'First Time' else FallStu.studentType end) studentType,
            FallStu.isNonDegreeSeeking isNonDegreeSeeking,
            FallStu.snapshotDate snapshotDate,
            FallStu.censusDate censusDate,
            FallStu.maxCensus maxCensus,
            FallStu.termOrder termOrder,
            FallStu. termType,
--            (case when FallStu.studentLevelUGGR = 'UG' then 
  --              (case when FallStu.studentTypeTermType = 'Fall' and FallStu.studentType = 'Returning' and FallStu.preFallStudType is not null then FallStu.preFallStudType
    --                  else FallStu.studentType 
      --          end)
        --        else FallStu.studentType 
          --  end) studentType,
            FallStu.studentLevel studentLevel,
            FallStu.studentLevelUGGR studentLevelUGGR,
            FallStu.genderForUnknown,
            FallStu.genderForNonBinary,
            FallStu.residency,
            FallStu.studyAbroadStatus
    from (
        select *
         from (
            select stu.yearType,
                    stu.surveySection,
                    stu.snapshotDate,
                    stu.termCode, 
                    stu.termOrder,
                    stu.maxCensus,
                    stu.termType,
                    stu.startDate,
                    stu.censusDate,
                    stu.maxCensus,
                    stu.fullTermOrder,
                    stu.startDate,
                    stu.personId,
                    stu.isNonDegreeSeeking,
                    stu.homeCampus,
                    stu.studentType,
                    stu.studentLevel studentLevel,
                    (case when stu.studentLevel = 'Graduate' then 'GR'
                        else 'UG' 
                    end) studentLevelUGGR,
                    stu.genderForUnknown,
                    stu.genderForNonBinary,
                    stu.residency,
                    stu.studyAbroadStatus,
                    --stu.highSchoolGradDate
                    coalesce(campus.isInternational, false) isInternational,        
                    row_number() over (
                            partition by
                                stu.yearType,
                                stu.surveySection,
                                stu.personId 
                            order by 
                                (case when campus.snapshotDate = stu.snapshotDate then 1 else 2 end) asc,
                                (case when campus.snapshotDate > stu.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                                (case when campus.snapshotDate < stu.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc
                        ) regCampRn
            from StudentMCR stu
                   left join CampusMCR campus on stu.homeCampus = campus.campus
--            where stu.termType = 'Fall'
            ) 
        where regCampRn = 1 
            and isInternational = false
        ) FallStu
        left join (select stu2.personId personId,
                          stu2.studentType studentType
                    from StudentMCR stu2
                    where stu2.termType != 'Fall'
                        and stu2.studentType = 'First Time') SumStu on FallStu.personId = SumStu.personId
    )
),

CourseSectionMCR as ( --2m 14s
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114
--Included to get enrollment hours of a courseSectionNumber
--Maintains cohort records for current and prior year.

select *
from (
    select stu.yearType,
        reg.surveySection,
        reg.snapshotDate snapshotDate,
        stu.snapshotDate stuSSD,
        to_date(coursesectENT.snapshotDate,'YYYY-MM-DD') courseSectionSSD,
        reg.termCode,
        reg.partOfTermCode,
        reg.censusDate,
        reg.termType,
        reg.termOrder,
        reg.requiredFTCreditHoursUG,
	    reg.requiredFTClockHoursUG,
	    reg.requiredFTCreditHoursGR, 
	    reg.instructionalActivityType,
	    stu.personId personId,
        stu.studentLevel,
		stu.studentLevelUGGR,
	    stu.studentType,
	    stu.isNonDegreeSeeking,
	    stu.residency,
		stu.studyAbroadStatus,
        stu.genderForUnknown,
        stu.genderForNonBinary,
        coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        reg.courseSectionNumber,
        reg.isAudited,
        reg.enrollmentHoursOverride,
        coalesce(reg.courseSectionLevelOverride, coursesectENT.courseSectionLevel) courseSectionLevel, --reg level prioritized over courseSection level 
        upper(coursesectENT.subject) subject,
        upper(coursesectENT.courseNumber) courseNumber,
        upper(coursesectENT.section) section,
		upper(coursesectENT.customDataValue) customDataValue,
		coalesce(coursesectENT.isESL, false) isESL, 
		coalesce(coursesectENT.isRemedial, false) isRemedial,
		upper(coursesectENT.college) college,
		upper(coursesectENT.division) division,
		upper(coursesectENT.department) department,
        coalesce(reg.enrollmentHoursOverride, coursesectENT.enrollmentHours) enrollmentHours, --reg enr hours prioritized over courseSection enr hours
        reg.equivCRHRFactor,
        coalesce(coursesectENT.isClockHours, false) isClockHours,
		reg.courseSectionCampusOverride,
        row_number() over (
                partition by
                    reg.yearType,
                    reg.surveySection,
                    reg.termCode,
                    reg.partOfTermCode,
                    reg.personId,
                    reg.courseSectionNumber,
                    coursesectENT.courseSectionNumber,
                    coursesectENT.courseSectionLevel,
                    coursesectENT.subject,
                    coursesectENT.courseNumber
                order by
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') = reg.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    coursesectENT.recordActivityDate desc
            ) courseRn
    from RegistrationMCR reg  
        inner join StudentRefactor stu on stu.personId = reg.personId
            and stu.termCode = reg.termCode
            and stu.yearType = reg.yearType
            and stu.surveySection = reg.surveySection
		left join CourseSection coursesectENT on reg.termCode = upper(coursesectENT.termCode)
            and reg.partOfTermCode = coalesce(upper(coursesectENT.partOfTermCode), 1)
            and reg.courseSectionNumber = upper(coursesectENT.courseSectionNumber)
            and coalesce(coursesectENT.isIpedsReportable, true) = true
            and ((coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                    and to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') <= reg.censusDate)
                or coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))   
--AK mod add courseSectionStatus inclusion condition --and courseSectionStatus in ('Active', 'Pending', 'Renumbered')
    )
where courseRn = 1
),

CourseSectionScheduleMCR as ( --2m 14s
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114
--Returns course scheduling related info for the registration courseSectionNumber. 
--Maintains cohort records for current and prior year.

select *
from (
	select CourseData.*,
		coalesce(campus.isInternational, false) isInternational,
		row_number() over (
				partition by
					CourseData.yearType,
					--CourseData.surveySection,
					CourseData.termCode,
					CourseData.partOfTermCode,
					CourseData.personId,
					CourseData.courseSectionNumber,
					CourseData.courseSectionLevel
				order by 
					(case when campus.snapshotDate = CourseData.snapshotDate then 1 else 2 end) asc,
					(case when campus.snapshotDate > CourseData.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
					(case when campus.snapshotDate < CourseData.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc
			) regCampRn
	from (
		select coursesect.yearType yearType,
		    coursesect.surveySection surveySection,
			coursesect.snapshotDate snapshotDate, 
			coursesect.courseSectionSSD courseSectionSSD,
			to_date(coursesectschedENT.snapshotDate,'YYYY-MM-DD') courseSectSchedSSD,
			coursesect.termCode termCode,
			coursesect.partOfTermCode partOfTermCode,
			coursesect.censusDate censusDate,
			coursesect.termType termType,
			coursesect.termOrder termOrder, 
			coursesect.requiredFTCreditHoursUG,
			coursesect.requiredFTClockHoursUG,
			coursesect.requiredFTCreditHoursGR,
			coursesect.instructionalActivityType,
			coursesect.personId personId,
			coursesect.studentLevel,
			coursesect.studentLevelUGGR,
			coursesect.studentType,
			coursesect.isNonDegreeSeeking,
            coursesect.genderForNonBinary,
            coursesect.genderForUnknown,
			coursesect.residency,
			coalesce(to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
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
			--coursesect.highSchoolGradDate,
			coursesect.courseSectionLevel courseSectionLevel,
			coursesect.enrollmentHours enrollmentHours,
			coursesect.equivCRHRFactor equivCRHRFactor,
			coursesect.isClockHours isClockHours,
			coalesce(coursesect.courseSectionCampusOverride, upper(coursesectschedENT.campus)) campus, --reg campus prioritized over courseSection campus 
			coursesectschedENT.instructionType,
			coursesectschedENT.locationType,
			coursesectschedENT.distanceEducationType,
			coursesectschedENT.onlineInstructionType,
			coursesectschedENT.maxSeats,
			row_number() over (
				partition by
					coursesect.yearType,
					coursesect.termCode, 
					coursesect.partOfTermCode,
					coursesect.personId,
					coursesect.courseSectionNumber,
					coursesect.courseSectionLevel,
					coursesect.subject,
					coursesect.courseNumber
				order by
					(case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') = coursesect.snapshotDate then 1 else 2 end) asc,
					(case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') > coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
					(case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') < coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
					coursesectschedENT.recordActivityDate desc
			) courseSectSchedRn
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

CourseMCR as ( --2m 38s
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114
--Included to get course type information
--Maintains cohort records for current and prior year.

select *
from (
	select coursesectsched.yearType yearType,
	    coursesectsched.surveySection surveySection,
	    coursesectsched.snapshotDate snapshotDate,
	    coursesectsched.courseSectionSSD courseSectionSSD,
	    coursesectsched.courseSectSchedSSD courseSectSchedSSD,
	    to_date(courseENT.snapshotDate,'YYYY-MM-DD') courseSSD,
	    coursesectsched.termCode termCode,
		coursesectsched.partOfTermCode partOfTermCode,
		coursesectsched.termOrder termOrder,
	    termorder.termOrder courseTermOrder,
	    coursesectsched.termOrder courseSectTermOrder,
	    coursesectsched.censusDate censusDate,
	    coursesectsched.termType termType,
	    coursesectsched.requiredFTCreditHoursUG,
	    coursesectsched.requiredFTCreditHoursGR,
	    coursesectsched.requiredFTClockHoursUG,
	    coursesectsched.instructionalActivityType,
        coursesectsched.personId personId,
	    coursesectsched.studentType,
	    coursesectsched.studentLevel,
		coursesectsched.studentLevelUGGR,
	    coursesectsched.isNonDegreeSeeking,
        coursesectsched.genderForNonBinary,
        coursesectsched.genderForUnknown,
	    coursesectsched.residency,
		coursesectsched.studyAbroadStatus,
	    coursesectsched.courseSectionNumber courseSectionNumber,
		coursesectsched.section section,
		coursesectsched.subject subject,
		coursesectsched.courseNumber courseNumber,
		coalesce(coursesectsched.courseSectionLevel, courseENT.courseLevel) courseLevel,
		coalesce(coursesectsched.isRemedial, false) isRemedial,
		coalesce(coursesectsched.isESL, false) isESL,
		coalesce(coursesectsched.isAudited, false) isAudited,
		coursesectsched.customDataValue,
		coalesce(coursesectsched.college, courseENT.courseCollege) college,
		coalesce(coursesectsched.division, courseENT.courseDivision) division,
		coalesce(coursesectsched.department, courseENT.courseDepartment) department,
		--coursesectsched.enrollmentHours enrollmentHours,
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
	    row_number() over (
			partition by
			    coursesectsched.yearType,
                coursesectsched.termCode, 
				coursesectsched.partOfTermCode,
                coursesectsched.personId,
			    coursesectsched.courseSectionNumber,
			    coursesectsched.courseSectionLevel,
			    coursesectsched.subject,
                coursesectsched.courseNumber
			order by
			    (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') = coursesectsched.snapshotDate then 1 else 2 end) asc,
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') > coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') < coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
			    termorder.termOrder desc,
			    courseENT.recordActivityDate desc
		) courseRn
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
),

CourseTypeCountsSTU as ( --2m 25s
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114
-- View used to break down course category type counts for student
--Maintains cohort records for current and prior year.

select *,
        (select first(maxCensus) 
                from AcademicTermReportingRefactor acadRep
                where acadRep.termcode = termcode
                and acadRep.partOfTermCode = acadRep.maxPOT) censusDate,
        (select first(snapshotDate)
                from AcademicTermReportingRefactor acadRep
                where acadRep.termcode = termcode
                and acadRep.partOfTermCode = acadRep.maxPOT) snapshotDate
from (
    select *,
            (case when studentType = 'First Time' and isNonDegreeSeeking = 0 then
                    (case when instructionalActivityType in ('CR', 'B') then 
                                (case when totalCreditHrs >= requiredFTCreditHoursUG then 'FT' else 'PT' end)
                          when instructionalActivityType = 'CL' then 
                                (case when totalClockHrs >= requiredFTClockHoursUG then 'FT' else 'PT' end) 
                          else null end)
                else null end) timeStatus
	from (
		select yearType,
		    surveySection,
			instructionalActivityType,
			requiredFTCreditHoursUG,
			requiredFTClockHoursUG,
			requiredFTCreditHoursGR,
			personId,
			studentLevel,
			studentType,
			residency,
			genderForNonBinary,
			genderForUnknown,
			totalClockHrs,
			totalCreditHrs,
			totalCourses,
			totalDECourses,
			termCode, 
			termOrder, 
			termType,
			requiredFTCreditHoursGR, 
			studentLevelUGGR, 
			studyAbroadStatus, 
			(case when studyAbroadStatus != 'Study Abroad - Home Institution' then isNonDegreeSeeking
				  when totalSAHomeCourses > 0 or totalCreditHrs > 0 or totalClockHrs > 0 then false 
				  else isNonDegreeSeeking 
			  end) isNonDegreeSeeking,
			(case when totalCECourses = totalCourses then 0 --exclude students enrolled only in continuing ed courses
				when totalIntlCourses = totalCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
				when totalAuditCourses = totalCourses then 0 --exclude students exclusively auditing classes
				when totalProfResidencyCourses > 0 then 0 --exclude PHD residents or interns
				when totalThesisCourses > 0 then 0 --exclude PHD residents or interns
				when totalRemCourses = totalCourses and isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
				when totalESLCourses = totalCourses and isNonDegreeSeeking = 0 then 1 --exclude students enrolled only in ESL courses/programs
				when totalSAHomeCourses > 0 then 1 --include study abroad student where home institution provides resources, even if credit hours = 0
				when totalCreditHrs > 0 then 1
				when totalClockHrs > 0 then 1
				else 0
			 end) ipedsInclude
        from (
			select course.yearType,
			    course.surveySection,
				course.instructionalActivityType,
				course.requiredFTCreditHoursUG,
				course.requiredFTClockHoursUG,
				course.requiredFTCreditHoursGR,
				course.personId,
				course.studentLevel,
				course.studentLevelUGGR,
				course.studentType,
				course.isNonDegreeSeeking,
				course.residency,
				course.genderForNonBinary,
				course.genderForUnknown,
				course.studyAbroadStatus,
				course.termCode, -- FE
				course.termOrder, --FE
				course.termType,
				coalesce(count(course.courseSectionNumber), 0) totalCourses,
				coalesce(sum((case when course.enrollmentHours >= 0 then 1 else 0 end)), 0) totalCreditCourses,
				coalesce(sum((case when coalesce(course.isClockHours, false) = false then course.enrollmentHours else 0 end)), 0) totalCreditHrs,
				coalesce(sum((case when coalesce(course.isClockHours, false) = true and course.courseLevel = 'Undergraduate' then course.enrollmentHours else 0 end)), 0) totalClockHrs,
				coalesce(sum((case when course.courseLevel = 'Continuing Education' then 1 else 0 end)), 0) totalCECourses,
				coalesce(sum((case when course.locationType = 'Foreign Country' then 1 else 0 end)), 0) totalSAHomeCourses, 
				coalesce(sum((case when coalesce(course.isESL, false) = true then 1 else 0 end)), 0) totalESLCourses,
				coalesce(sum((case when coalesce(course.isRemedial, false) = true then 1 else 0 end)), 0) totalRemCourses,
				coalesce(sum((case when coalesce(course.isInternational, false) = true then 1 else 0 end)), 0) totalIntlCourses,
				coalesce(sum((case when coalesce(course.isAudited, false) = true then 1 else 0 end)), 0) totalAuditCourses,
				coalesce(sum((case when course.instructionType = 'Thesis/Capstone' then 1 else 0 end)), 0) totalThesisCourses,
				coalesce(sum((case when course.instructionType in ('Residency', 'Internship', 'Practicum') and course.studentLevel = 'Professional Practice Doctorate' then 1 else 0 end)), 0) totalProfResidencyCourses,
				coalesce(sum((case when course.distanceEducationType != 'Not distance education' then 1 else 0 end)), 0) totalDECourses
			from CourseMCR course
		group by course.yearType,
		        course.surveySection,
				course.instructionalActivityType,
				course.requiredFTCreditHoursUG,
				course.requiredFTClockHoursUG,
				course.requiredFTCreditHoursGR,
				course.personId,
				course.termCode,
				course.termOrder,
				course.termType,
				course.studentLevel,
				course.studentLevelUGGR,
				course.studentType,
				course.isNonDegreeSeeking,
				course.residency,
				course.genderForNonBinary,
				course.genderForUnknown,
				course.studyAbroadStatus
			)
		)
	--ipedsInclude filter had to be moved to make this view reusable for study abroad inclusions to the retention cohort
	--where ipedsInclude = 1
	)
--where timeStatus = 'FT'
),

PersonMCR as ( --3m
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 
--Maintains cohort records for current and prior year.

select --distinct 
        personId personId,
        yearType yearType,
        surveySection surveySection,
        snapshotDate snapshotDate,
        termCode termCode,
        censusDate censusDate,
        termOrder termOrder,
        timeStatus,
        studentLevel,
        studentLevelUGGR, 
        studentType,
        isNonDegreeSeeking,
        totalCourses,
        totalDECourses,
        (case when gender = 'Male' then 'M'
            when gender = 'Female' then 'F' 
            when gender = 'Non-Binary' then genderForNonBinary
            else genderForUnknown
        end) ipedsGender,
        FLOOR(DATEDIFF(to_date(censusDate, 'YYYY-MM-DD'), to_date(birthDate, 'YYYY-MM-DD')) / 365) asOfAge,
        residency,
        state,
        nation,
        studyAbroadStatus,
        ipedsInclude,
        (case when isUSCitizen = true or ((isInUSOnVisa = true or censusDate between visaStartDate and visaEndDate)
                            and visaType in ('Employee Resident', 'Other Resident')) then 
            (case when isHispanic = true then '2' 
                when isMultipleRaces = true then '8' 
                when ethnicity != 'Unknown' and ethnicity is not null then
                    (case when ethnicity = 'Hispanic or Latino' then '2'
                        when ethnicity = 'American Indian or Alaskan Native' then '3'
                        when ethnicity = 'Asian' then '4'
                        when ethnicity = 'Black or African American' then '5'
                        when ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
                        when ethnicity = 'Caucasian' then '7'
                        else '9' 
                    end) 
                else '9' end) -- 'race and ethnicity unknown'
            when ((isInUSOnVisa = true or censusDate between visaStartDate and visaEndDate)
                and visaType in ('Student Non-resident', 'Employee Non-resident', 'Other Non-resident')) then '1' -- 'nonresident alien'
            else '9' -- 'race and ethnicity unknown'
        end) ipedsEthnicity
from ( 
    select distinct stu.personId personId,
            stu.yearType yearType,
            stu.surveySection surveySection,
            stu.snapshotDate snapshotDate,
            stu.termCode termCode,
            stu.censusDate censusDate,
            stu.termOrder termOrder,
            stu.genderForNonBinary,
            stu.genderForUnknown,
            stu.residency,
            personENT.ethnicity ethnicity,
            coalesce(personENT.isHispanic, false) isHispanic,
            coalesce(personENT.isMultipleRaces, false) isMultipleRaces,
            coalesce(personENT.isInUSOnVisa, false) isInUSOnVisa,
            to_date(personENT.visaStartDate, 'YYYY-MM-DD') visaStartDate,
            to_date(personENT.visaEndDate, 'YYYY-MM-DD') visaEndDate,
            personENT.visaType visaType,
            coalesce(personENT.isUSCitizen, true) isUSCitizen,
            personENT.gender gender,
            to_date(personENT.birthDate, 'YYYY-MM-DD') birthDate,
            upper(personENT.nation) nation,
            upper(personENT.state) state,
            stu.studyAbroadStatus,
            stu.ipedsInclude,
            stu.timeStatus,
            stu.studentLevel,
            stu.studentLevelUGGR,
            stu.studentType,
            stu.isNonDegreeSeeking,
            stu.totalCourses,
            stu.totalDECourses,
            row_number() over (
                partition by
                    stu.yearType,
                    stu.surveySection,
                    stu.personId,
                    personENT.personId
                order by
                    (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') = stu.snapshotDate then 1 else 2 end) asc,
			        (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') > stu.snapshotDate then to_date(personENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') < stu.snapshotDate then to_date(personENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    personENT.recordActivityDate desc
            ) personRn
    from CourseTypeCountsSTU stu 
        left join Person personENT on stu.personId = personENT.personId
            and coalesce(personENT.isIpedsReportable = true, true)
			and ((coalesce(to_date(personENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
				and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= stu.censusDate)
				or coalesce(to_date(personENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
    )
where personRn <= 1
),

AcademicTrackMCR as ( --4m 5s
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114
--Returns most current data based on initial academic track of student
--Maintains cohort records for current and prior year.

select * 
from (
	select personId personId,
		yearType yearType,
		surveySection surveySection,
		snapshotDate snapshotDate,
		termCode termCode,
		censusDate censusDate,
		termOrder termOrder,
		residency residency,
		state state,
		nation nation,
		studyAbroadStatus studyAbroadStatus,
		ipedsInclude ipedsInclude,
		timeStatus timeStatus,
		studentLevel studentLevel,
		studentLevelUGGR studentLevelUGGR,
		studentType studentType,
		isNonDegreeSeeking isNonDegreeSeeking,
		totalCourses,
		totalDECourses,
		ipedsGender ipedsGender,
		asOfAge asOfAge,
		ipedsEthnicity ipedsEthnicity,
		acadTrackTermOrder acadTrackTermOrder,
		departmentOverride departmentOverride,
		divisionOverride divisionOverride,
		collegeOverride collegeOverride,
		campusOverride campusOverride,
		degreeProgram degreeProgram,
		fieldOfStudy fieldOfStudy,
		coalesce(row_number() over (
			        partition by
				        personId,
				        yearType,
				        surveySection
			        order by
				        coalesce(fieldOfStudyPriority, 1) asc
		        ), 1) fosRn
    from (
		select person.personId personId,
		    person.yearType yearType,
			person.surveySection surveySection,
            person.snapshotDate snapshotDate,
            person.termCode termCode,
			row_number() over (
				partition by					
					person.yearType,
					person.surveySection,
					person.personId,
					acadtrackENT.degreeProgram,
					acadtrackENT.fieldOfStudyPriority
				order by
					(case when to_date(acadtrackENT.snapshotDate,'YYYY-MM-DD') = person.snapshotDate then 1 else 2 end) asc,
					(case when to_date(acadtrackENT.snapshotDate,'YYYY-MM-DD') > person.snapshotDate then to_date(acadtrackENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
					(case when to_date(acadtrackENT.snapshotDate,'YYYY-MM-DD') < person.snapshotDate then to_date(acadtrackENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
					termorder.termOrder desc,
					acadtrackENT.fieldOfStudyActionDate desc, 
					acadtrackENT.recordActivityDate desc,
					(case when acadtrackENT.academicTrackStatus = 'In Progress' then 1 else 2 end) asc
            ) acadtrackRn,
            person.censusDate censusDate,
            person.termOrder termOrder,
            person.residency residency,
            person.state state,
            person.nation nation,
            person.studyAbroadStatus studyAbroadStatus,
            person.ipedsInclude ipedsInclude,
            person.timeStatus timeStatus,
            person.studentLevel studentLevel,
            person.studentLevelUGGR studentLevelUGGR,
            person.studentType studentType,
            person.isNonDegreeSeeking isNonDegreeSeeking,
            person.totalCourses,
            person.totalDECourses,
            person.ipedsGender ipedsGender,
            person.asOfAge asOfAge,
            person.ipedsEthnicity ipedsEthnicity,
            upper(acadtrackENT.collegeOverride) collegeOverride,
            upper(acadtrackENT.campusOverride) campusOverride,
            upper(acadtrackENT.departmentOverride) departmentOverride,
            upper(acadtrackENT.divisionOverride) divisionOverride,
            acadtrackENT.termCodeEffective termCodeEffective,
            upper(acadtrackENT.degreeProgram) degreeProgram,
            acadtrackENT.fieldOfStudy fieldOfStudy,
            acadtrackENT.fieldOfStudyPriority fieldOfStudyPriority,
            acadtrackENT.fieldOfStudyType fieldOfStudyType,
            acadtrackENT.academicTrackStatus academicTrackStatus,
            to_date(acadtrackENT.fieldOfStudyActionDate,'YYYY-MM-DD') fieldOfStudyActionDate,
            coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
            termorder.termOrder acadTrackTermOrder
		from PersonMCR person
            left join AcademicTrack acadtrackENT on acadtrackENT.personId = person.personId
				and ((coalesce(to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                        and to_date(acadtrackENT.fieldOfStudyActionDate,'YYYY-MM-DD') <= person.censusDate)
                    or (coalesce(to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                        and ((coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' as DATE)
                                and to_date(acadtrackENT.recordActivityDate,'YYYY-MM-DD') <= person.censusDate)
                            or coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' as DATE)))) 
				and acadTrackENT.fieldOfStudyType = 'Major' 
				and coalesce(acadTrackENT.isIpedsReportable, true) = true
				and coalesce(acadTrackENT.isCurrentFieldOfStudy, true) = true
				and acadTrackENT.isCurrentFieldOfStudy = true
			left join AcademicTermOrder termorder on termorder.termCode = acadtrackENT.termCodeEffective
				and termorder.termOrder <= person.termOrder
				
			)
		where acadtrackRn <= 1
    )
where fosRn = 1
),

DegreeProgramMCR as ( --4m 11s
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114
-- Returns all degreeProgram values at the end of the reporting period
--Maintains cohort records for current and prior year.

select *
from (
    select DISTINCT
        acadtrack.personId personId,
        acadtrack.yearType yearType,
        acadtrack.surveySection surveySection,
        acadtrack.snapshotDate snapshotDate,
        acadtrack.termCode termCode,
        acadtrack.censusDate censusDate,
        acadtrack.termOrder termOrder,
        acadtrack.residency residency,
        acadtrack.state state,
        acadtrack.nation nation,
        acadtrack.studyAbroadStatus studyAbroadStatus,
        acadtrack.ipedsInclude ipedsInclude,
        acadtrack.timeStatus timeStatus,
        acadtrack.studentLevel studentLevel,
        acadtrack.studentLevelUGGR studentLevelUGGR,
        acadtrack.studentType studentType,
        acadtrack.isNonDegreeSeeking isNonDegreeSeeking,
        acadtrack.totalCourses totalCourses,
        acadtrack.totalDECourses totalDECourses,
        acadtrack.ipedsGender ipedsGender,
        acadtrack.asOfAge asOfAge,
        acadtrack.ipedsEthnicity ipedsEthnicity,
        acadtrack.acadTrackTermOrder acadTrackTermOrder,
        acadtrack.fieldOfStudy fieldOfStudy,
		upper(programENT.degreeProgram) degreeProgram, 
		to_date(programENT.snapshotDate, 'YYYY-MM-DD') progSnapshotDate,
		upper(programENT.degree) degree,
		upper(programENT.major) major, 
		coalesce(acadtrack.collegeOverride, upper(programENT.college)) college, 
		coalesce(acadtrack.campusOverride, upper(programENT.campus)) campus, 
		coalesce(acadtrack.departmentOverride, upper(programENT.department)) department,
		coalesce(acadtrack.divisionOverride, upper(programENT.division)) division,
		to_date(programENT.startDate, 'YYYY-MM-DD') startDate,
		programENT.termCodeEffective,
		termorder.termOrder DegreeProgramOrder,
		programENT.distanceEducationType,
		coalesce(programENT.isESL, false) isESL,
		coalesce(to_date(programENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
		row_number() over (
			partition by
			    acadtrack.personId,
			    acadtrack.yearType,
			    acadtrack.surveySection,
				programENT.snapshotDate,
				programENT.degreeProgram,
				programENT.degree,
				programENT.major,
				programENT.campus, 
				programENT.division
			order by
                (case when to_date(programENT.snapshotDate, 'YYYY-MM-DD') = acadtrack.snapshotDate then 1 else 2 end) asc,
                (case when to_date(programENT.snapshotDate, 'YYYY-MM-DD') > acadtrack.snapshotDate then to_date(programENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                (case when to_date(programENT.snapshotDate, 'YYYY-MM-DD') < acadtrack.snapshotDate then to_date(programENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
			    (case when termorder.termOrder is not null then termorder.termOrder else 1 end) desc, --DegreeProgramOrder; return max term order less than acadTrackTermOrder
				programENT.recordActivityDate desc
		) programRN
	from AcademicTrackMCR acadtrack 
	    left join DegreeProgram programENT on upper(programENT.degreeProgram) = acadtrack.degreeProgram 
	        and programENT.lengthInMonths is not null
            and coalesce(programENT.isIpedsReportable, true) = true
		    and programENT.isForStudentAcadTrack = true
		    and programENT.isForDegreeAcadHistory = true
		    and ((to_date(programENT.startDate, 'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
				    and to_date(programENT.startDate, 'YYYY-MM-DD') <= acadtrack.censusDate)
					    or to_date(programENT.startDate, 'YYYY-MM-DD') = CAST('9999-09-09' as DATE)) 
			and ((coalesce(to_date(programENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
			   and to_date(programENT.recordActivityDate,'YYYY-MM-DD') <= acadtrack.censusDate)
				or coalesce(to_date(programENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
	    left join AcademicTermOrder termorder on programENT.termCodeEffective = termorder.termCode
	where ((termorder.termOrder is not null and acadtrack.acadTrackTermOrder >= termOrder.termOrder) --DegreeProgramOrder
	    or termorder.termOrder is null)
    )
where programRN <= 1
    and isESL != 1 --EXCLUDE Students enrolled only in ESL programs (programs comprised exclusively of ESL courses)
),

FieldOfStudyMCR as ( --4m 34s
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114
--Returns most up to 'major' information as of the reporting term codes and part of term census periods.
--This information is only required for even-numbered IPEDS reporting years. 
--Maintains cohort records for current and prior year.

select *
from (
	select program.personId personId,
        program.yearType yearType,
        program.surveySection surveySection,
        program.snapshotDate snapshotDate,
        program.termCode termCode,
        program.censusDate censusDate,
        program.termOrder termOrder,
        program.residency residency,
        program.state state,
        program.nation nation,
        program.studyAbroadStatus studyAbroadStatus,
        program.ipedsInclude ipedsInclude,
        program.timeStatus timeStatus,
        program.studentLevel studentLevel,
        program.studentLevelUGGR studentLevelUGGR,
        program.studentType studentType,
        program.isNonDegreeSeeking isNonDegreeSeeking,
        program.totalCourses totalCourses,
        program.totalDECourses totalDECourses,
        program.ipedsGender ipedsGender,
        program.asOfAge asOfAge,
        program.ipedsEthnicity ipedsEthnicity,
        program.acadTrackTermOrder acadTrackTermOrder,
        program.fieldOfStudy fieldOfStudy,
		program.degreeProgram degreeProgram, 
		program.progSnapshotDate progSnapshotDate,
		program.degree degree,
		program.major major, 
		program.college college, 
		program.campus campus, 
		program.department department,
		program.division division,
		program.startDate startDate,
		program.termCodeEffective,
		program.termOrder DegreeProgramOrder,
		program.distanceEducationType,
		(case when substr(fieldofstudyENT.cipCode, 0, 2) = '13' then '13.0000'
		    when substr(fieldofstudyENT.cipCode, 0, 2) = '14' then '14.0000'
		    when substr(fieldofstudyENT.cipCode, 0, 2) = '26' then '26.0000'
		    when substr(fieldofstudyENT.cipCode, 0, 2) = '27' then '27.0000'
		    when substr(fieldofstudyENT.cipCode, 0, 2) = '40' then '40.0000'
		    when substr(fieldofstudyENT.cipCode, 0, 2) = '52' then '52.0000'
		    when translate(fieldofstudyENT.cipCode, '.', '') = '220101' then '22.0101'
		    when translate(fieldofstudyENT.cipCode, '.', '') = '510401' then '51.0401'
		    when translate(fieldofstudyENT.cipCode, '.', '') = '511201' then '51.1201'
        else null end) partACipCode,
		row_number() over (
			partition by
			    program.personId,
			    program.yearType,
			    program.surveySection,
				fieldofstudyENT.fieldOfStudy
			order by
                (case when to_date(fieldofstudyENT.snapshotDate, 'YYYY-MM-DD') = program.snapshotDate then 1 else 2 end) asc,
                (case when to_date(fieldofstudyENT.snapshotDate, 'YYYY-MM-DD') > program.snapshotDate then to_date(fieldofstudyENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                (case when to_date(fieldofstudyENT.snapshotDate, 'YYYY-MM-DD') < program.snapshotDate then to_date(fieldofstudyENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
				fieldofstudyENT.recordActivityDate desc
		) majorRn
	from DegreeProgramMCR program
		left join fieldOfStudy fieldofstudyENT ON fieldofstudyENT.fieldOfStudy = program.fieldOfStudy
            and coalesce(fieldofstudyENT.isIpedsReportable, true) = true
			and ((coalesce(to_date(fieldofstudyENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
			   and to_date(fieldofstudyENT.recordActivityDate,'YYYY-MM-DD') <= program.censusDate)
				or coalesce(to_date(fieldofstudyENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
	)
where majorRn <= 1
),

DegreeMCR as (
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114
--Returns most up to 'degree' information as of the reporting term codes and part of term census periods.
--This information is used for Retention cohort filtering on awardLevel
--Maintains cohort records for current and prior year.

select *
from (
	select fieldofstudy.personId personId,
        fieldofstudy.yearType yearType,
        fieldofstudy.surveySection surveySection,
        fieldofstudy.snapshotDate snapshotDate,
        fieldofstudy.termCode termCode,
        fieldofstudy.censusDate censusDate,
        fieldofstudy.termOrder termOrder,
        fieldofstudy.residency residency,
        fieldofstudy.state state,
        fieldofstudy.nation nation,
        fieldofstudy.studyAbroadStatus studyAbroadStatus,
        fieldofstudy.ipedsInclude ipedsInclude,
        fieldofstudy.timeStatus timeStatus,
        fieldofstudy.studentLevel studentLevel,
        fieldofstudy.studentLevelUGGR studentLevelUGGR,
        fieldofstudy.studentType studentType,
        fieldofstudy.isNonDegreeSeeking isNonDegreeSeeking,
        fieldofstudy.totalCourses totalCourses,
        fieldofstudy.totalDECourses totalDECourses,
        fieldofstudy.ipedsGender ipedsGender,
        fieldofstudy.asOfAge asOfAge,
        fieldofstudy.ipedsEthnicity ipedsEthnicity,
        fieldofstudy.acadTrackTermOrder acadTrackTermOrder,
        fieldofstudy.fieldOfStudy fieldOfStudy,
		fieldofstudy.degreeProgram degreeProgram, 
		fieldofstudy.progSnapshotDate progSnapshotDate,
		fieldofstudy.degree degree,
		fieldofstudy.major major, 
		fieldofstudy.college college, 
		fieldofstudy.campus campus, 
		fieldofstudy.department department,
		fieldofstudy.division division,
		fieldofstudy.startDate startDate,
		fieldofstudy.termCodeEffective,
		fieldofstudy.termOrder DegreeProgramOrder,
		fieldofstudy.distanceEducationType,
		fieldofstudy.partACipCode,
		degreeENT.degreeLevel,
		degreeENT.awardLevel,
		coalesce(degreeENT.isNonDegreeSeeking, false) isNonDegreeSeekingDegree,
		row_number() over (
			partition by
			    fieldofstudy.personId,
			    fieldofstudy.yearType,
			    fieldofstudy.surveySection,
			    degreeENT.snapshotDate,
			    degreeENT.degreeLevel,
			    degreeENT.awardLevel,
			    degreeENT.degree
			order by
                (case when to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') = fieldofstudy.snapshotDate then 1 else 2 end) asc,
                (case when to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') > fieldofstudy.snapshotDate then to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                (case when to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') < fieldofstudy.snapshotDate then to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
				degreeENT.recordActivityDate desc
		) degreeRn
	from FieldOfStudyMCR fieldofstudy
		--inner join ClientConfigMCR clientconfig ON acadTrack.cohortInd = clientconfig.sectionRetFall
		left join Degree degreeENT ON degreeENT.degree = fieldofstudy.degree
			and coalesce(degreeENT.isIpedsReportable, true) = true
			and ((coalesce(to_date(degreeENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
			   and to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') <= fieldofstudy.censusDate)
				or coalesce(to_date(degreeENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
					
	)
where degreeRn = 1
--**    and ((degreeLevel is not null and degreeLevel != 'Professional Practice Doctorate') --EXCLUDE Residents or interns in doctor's - professional practice programs, since they have already received their doctor's degree
--**        or degreeLevel is null)
),

AdmissionMCR as (
--COHORT-530, PRIOR SUMMER-64, PRIOR YEAR 1 COHORT-681, PRIOR YEAR 1 PRIOR SUMMER-114
--Only need to report high school related data on first-time degree/certificate-seeking undergraduate students

select * 
from (
select degree.*,
    admENT.secondarySchoolCompleteDate highSchoolGradDate,
    row_number() over (
        partition by
            degree.yearType,
            degree.surveySection,
            admENT.termCodeApplied,
            admENT.termCodeAdmitted,
            degree.personId
        order by                    
            (case when to_date(admENT.snapshotDate, 'YYYY-MM-DD') = degree.snapshotDate then 1 else 2 end) asc,
            (case when to_date(admENT.snapshotDate, 'YYYY-MM-DD') > degree.snapshotDate then to_date(admENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
            (case when to_date(admENT.snapshotDate, 'YYYY-MM-DD') < degree.snapshotDate then to_date(admENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
            admENT.applicationNumber desc,
            admENT.applicationStatusActionDate desc,
            admENT.recordActivityDate desc,
            (case when admENT.applicationStatus in ('Complete', 'Decision Made') then 1 else 2 end) asc
    ) appRn 
from DegreeMCR degree
    left join Admission admENT on admENT.personId = degree.personId
            and ((coalesce(to_date(admENT.applicationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' AS DATE)
                        and to_date(admENT.applicationStatusActionDate,'YYYY-MM-DD') <= degree.censusDate)
                    or (coalesce(to_date(admENT.applicationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' AS DATE)
                        and ((coalesce(to_date(admENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                                and to_date(admENT.recordActivityDate,'YYYY-MM-DD') <= degree.censusDate)
                            or coalesce(to_date(admENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))))
			and admENT.studentLevel = 'Undergraduate'
			-- and admENT.admissionType = 'New Applicant'
			-- and admENT.studentType = 'First Time'
			and admENT.admissionDecision is not null
			and admENT.applicationStatus is not null
			and coalesce(admENT.isIpedsReportable, true) = true
)
where appRn <= 1
),

CohortSTU as ( --4m 33s
--COHORT-514, PRIOR SUMMER-63 (CohortSTU) - Lost records based on admission.yearType = 'CY' and admission.ipedsInclude = 1
-- This view is used to refactor the 'StudentCohort' view records in terms of IPEDS reportable values
-- Only includes current year cohort records that are IPEDS acceptable for reporting. 

		select admission.personId personId,
            admission.yearType yearType,
            admission.surveySection surveySection,
			----admission.cohortInd cohortInd,
			admission.censusDate censusDate,
			config.reportResidency reportResidency,
			config.reportAge reportAge,
			admission.studentType studentType,
			admission.timeStatus timeStatus,
			admission.ipedsInclude ipedsInclude,
			admission.isNonDegreeSeeking isNonDegreeSeeking,
            admission.totalCourses totalCourses,
            admission.totalDECourses totalDECourses,
			admission.studentLevel studentLevel,
			admission.studentLevelUGGR studentLevelUGGR,
			admission.asOfAge asOfAge,
			admission.ipedsEthnicity ipedsEthnicity,
			admission.nation nation,
			admission.residency residency,
            admission.state state,
            admission.studyAbroadStatus studyAbroadStatus,
            admission.highSchoolGradDate highSchoolGradDate,
            admission.ipedsInclude ipedsInclude,
            admission.partACipCode partACipCode,
			--jh 20200423 Broke up ipedsLevel into PartG, PartB and PartA
			-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'
			(case when admission.studentLevelUGGR = 'GR' then 3
				when admission.isNonDegreeSeeking = true then 2
				else 1
			end) ipedsPartGStudentLevel,
			(case when admission.studentLevelUGGR = 'GR' then 3
				else 1
			end) ipedsPartBStudentLevel,
			----admission.timeStatus timeStatus,
			case when admission.timeStatus = 'FT'
						and admission.studentType = 'First Time'
						and admission.isNonDegreeSeeking = false
						and admission.studentLevelUGGR = 'UG'  
				then 1 -- 1 - Full-time, first-time degree/certificate-seeking undergraduate
				when admission.timeStatus = 'FT'
						and admission.studentType = 'Transfer'
						and admission.isNonDegreeSeeking = false
						and admission.studentLevelUGGR = 'UG'
				then 2 -- 2 - Full-time, transfer-IN degree/certificate-seeking undergraduate
				when admission.timeStatus = 'FT'
						and admission.studentType = 'Continuing'
						and admission.isNonDegreeSeeking = false
						and admission.studentLevelUGGR = 'UG'
				then 3 -- 3 - Full-time, continuing degree/certificate-seeking undergraduate
				when admission.timeStatus = 'FT'
						and admission.isNonDegreeSeeking = true
						and admission.studentLevelUGGR = 'UG'
				then 7 -- 7 - Full-time, non-degree/certificate-seeking undergraduate
				when admission.timeStatus = 'FT'
						and admission.studentLevelUGGR = 'GR'
				then 11 -- 11 - Full-time graduate
				when admission.timeStatus = 'PT'
						and admission.studentType = 'First Time'
						and admission.isNonDegreeSeeking = false
						and admission.studentLevelUGGR = 'UG'
				then 15 -- 15 - Part-time, first-time degree/certificate-seeking undergraduate
				when admission.timeStatus = 'PT'
						and admission.studentType = 'Transfer'
						and admission.isNonDegreeSeeking = false
						and admission.studentLevelUGGR = 'UG'
				then 16 -- 16 - Part-time, transfer-IN degree/certificate-seeking undergraduate
				when admission.timeStatus = 'PT'
						and admission.studentType = 'Continuing'
						and admission.isNonDegreeSeeking = false
						and admission.studentLevelUGGR = 'UG'
				then 17 -- 17 - Part-time, continuing degree/certificate-seeking undergraduate
				when admission.timeStatus = 'PT'
						and admission.isNonDegreeSeeking = true
						and admission.studentLevelUGGR = 'UG'
				then 21 -- 21 - Part-time, non-degree/certificate-seeking undergraduate
				when admission.timeStatus = 'PT'
						and admission.studentLevelUGGR = 'GR'
				then 25 -- 25 - Part-time graduate
			end as ipedsPartAStudentLevel,
					-- ak 20200330 (PF-1253) Including client preferences for non-binary and unknown gender assignments
		admission.ipedsGender ipedsGender,
			case when admission.timeStatus = 'FT' 
				then (
					case when admission.asOfAge < 18 then 1 --1 - Full-time, under 18
						when admission.asOfAge >= 18 and admission.asOfAge <= 19 then 2 --2 - Full-time, 18-19
						when admission.asOfAge >= 20 and admission.asOfAge <= 21 then 3 --3 - Full-time, 20-21
						when admission.asOfAge >= 22 and admission.asOfAge <= 24 then 4 --4 - Full-time, 22-24
						when admission.asOfAge >= 25 and admission.asOfAge <= 29 then 5 --5 - Full-time, 25-29
						when admission.asOfAge >= 30 and admission.asOfAge <= 34 then 6 --6 - Full-time, 30-34
						when admission.asOfAge >= 35 and admission.asOfAge <= 39 then 7 --7 - Full-time, 35-39
						when admission.asOfAge >= 40 and admission.asOfAge <= 49 then 8 --8 - Full-time, 40-49
						when admission.asOfAge >= 50 and admission.asOfAge <= 64 then 9 --9 - Full-time, 50-64
						when admission.asOfAge >= 65 then 10 --10 - Full-time, 65 and over
						end )
				else (
					case when admission.asOfAge < 18 then 13 --13 - Part-time, under 18
						when admission.asOfAge >= 18 and admission.asOfAge <= 19 then 14 --14 - Part-time, 18-19
						when admission.asOfAge >= 20 and admission.asOfAge <= 21 then 15 --15 - Part-time, 20-21
						when admission.asOfAge >= 22 and admission.asOfAge <= 24 then 16 --16 - Part-time, 22-24
						when admission.asOfAge >= 25 and admission.asOfAge <= 29 then 17 --17 - Part-time, 25-29
						when admission.asOfAge >= 30 and admission.asOfAge <= 34 then 18 --18 - Part-time, 30-34
						when admission.asOfAge >= 35 and admission.asOfAge <= 39 then 19 --19 - Part-time, 35-39
						when admission.asOfAge >= 40 and admission.asOfAge <= 49 then 20 --20 - Part-time, 40-49
						when admission.asOfAge >= 50 and admission.asOfAge <= 64 then 21 --21 - Part-time, 50-64
						when admission.asOfAge >= 65 then 22 --22 - Part-time, 65 and over
						end )
			end ipedsAgeGroup,
			case when admission.nation in ('US', 'UNITED STATES', 'UNITED STATES OF AMERICA') then 1 else 0 end isInAmerica,
			case when admission.residency = 'In District' then 'IN'
		        when admission.residency = 'In State' then 'IN'
		        when admission.residency = 'Out of State' then 'OUT'
--jh 20200423 added value of Out of US
		        when admission.residency = 'Out of US' then 'OUT'
		        else 'UNKNOWN'
	        end residentStatus,
--check 'IPEDSClientConfig' entity to confirm that optional data is reported
		case when config.reportResidency = 'Y' 
			then (case when admission.state IS NULL then 57 --57 - State unknown
				when admission.state = 'AL' then 01 --01 - Alabama
				when admission.state = 'AK' then 02 --02 - Alaska
				when admission.state = 'AZ' then 04 --04 - Arizona
				when admission.state = 'AR' then 05 --05 - Arkansas
				when admission.state = 'CA' then 06 --06 - California
				when admission.state = 'CO' then 08 --08 - Colorado
				when admission.state = 'CT' then 09 --09 - CONnecticut
				when admission.state = 'DE' then 10 --10 - Delaware
				when admission.state = 'DC' then 11 --11 - District of Columbia
				when admission.state = 'FL' then 12 --12 - Florida
				when admission.state = 'GA' then 13 --13 - Georgia
				when admission.state = 'HI' then 15 --15 - Hawaii
				when admission.state = 'ID' then 16 --16 - Idaho
				when admission.state = 'IL' then 17 --17 - Illinois
				when admission.state = 'IN' then 18 --18 - Indiana
				when admission.state = 'IA' then 19 --19 - Iowa
				when admission.state = 'KS' then 20 --20 - Kansas
				when admission.state = 'KY' then 21 --21 - Kentucky
				when admission.state = 'LA' then 22 --22 - Louisiana
				when admission.state = 'ME' then 23 --23 - Maine
				when admission.state = 'MD' then 24 --24 - Maryland
				when admission.state = 'MA' then 25 --25 - Massachusetts
				when admission.state = 'MI' then 26 --26 - Michigan
				when admission.state = 'MS' then 27 --27 - Minnesota
				when admission.state = 'MO' then 28 --28 - Mississippi
				when admission.state = 'MN' then 29 --29 - Missouri
				when admission.state = 'MT' then 30 --30 - Montana
				when admission.state = 'NE' then 31 --31 - Nebraska
				when admission.state = 'NV' then 32 --32 - Nevada
				when admission.state = 'NH' then 33 --33 - New Hampshire
				when admission.state = 'NJ' then 34 --34 - New Jersey
				when admission.state = 'NM' then 35 --35 - New Mexico
				when admission.state = 'NY' then 36 --36 - New York
				when admission.state = 'NC' then 37 --37 - North Carolina
				when admission.state = 'ND' then 38 --38 - North Dakota
				when admission.state = 'OH' then 39 --39 - Ohio	
				when admission.state = 'OK' then 40 --40 - Oklahoma
				when admission.state = 'OR' then 41 --41 - Oregon
				when admission.state = 'PA' then 42 --42 - Pennsylvania
				when admission.state = 'RI' then 44 --44 - Rhode Island
				when admission.state = 'SC' then 45 --45 - South Carolina
				when admission.state = 'SD' then 46 --46 - South Dakota
				when admission.state = 'TN' then 47 --47 - Tennessee
				when admission.state = 'TX' then 48 --48 - Texas
				when admission.state = 'UT' then 49 --49 - Utah
				when admission.state = 'VT' then 50 --50 - Vermont
				when admission.state = 'VA' then 51 --51 - Virginia
				when admission.state = 'WA' then 53 --53 - Washington
				when admission.state = 'WV' then 54 --54 - West Virginia
				when admission.state = 'WI' then 55 --55 - Wisconsin
				when admission.state = 'WY' then 56 --56 - Wyoming
				when admission.state = 'UNK' then 57 --57 - State unknown
				when admission.state = 'American Samoa' then 60
				when admission.state = 'FM' then 64 --64 - Federated States of Micronesia
				when admission.state = 'GU' then 66 --66 - Guam
				when admission.state = 'Marshall Islands' then 68
				when admission.state = 'Northern Marianas' then 69
				when admission.state = 'Palau' then 70
				when admission.state = 'PUE' then 72 --72 - Puerto Rico
				when admission.state = 'VI' then 78 --78 - Virgin Islands
				else 90 --90 - Foreign countries
			end)
		else NULL
	end ipedsStateCode,
    admission.totalcourses totalCourses,
    admission.totalDECourses,
    case when admission.totalDECourses > 0 and admission.totalDECourses < admission.totalCourses then 'Some DE'
		when admission.totalDECourses = admission.totalCourses then 'Exclusive DE'
	end distanceEdInd
from AdmissionMCR admission
    cross join (select first(reportResidency) reportResidency,
                    first(reportAge) reportAge
                from ClientConfigMCR) config
where admission.yearType = 'CY'
    and admission.ipedsInclude = 1
-- NO LONGER NEEDED?			and admission.studentLevel in ('Undergrad', 'Graduate')
),

/*****
BEGIN SECTION - Retention Cohort Creation
This set of views is used to look at prior year data and return retention cohort information of the group as of the current reporting year.
*****/

CohortSTU_RET as (
--View used to build the reportable retention student cohort. (CohortInd = 'Retention Fall')
--Full-time, first-time Fall retention bachelor's cohort

select * 
from (
select degree.*,
    (case when currenroll.personId is not null then 1 end) isCurEnroll,
    (case when degree.studyAbroadStatus = 'Study Abroad - Home Institution' --??and degree.isNonDegreeSeeking = true 
            and currenroll.studyAbroadStatus != 'Study Abroad - Home Institution' and currenroll.isNonDegreeSeeking != true
        then 1
    end) isInclusion
from DegreeMCR degree
left join (
--Students from prior year cohort still enrolled as of Fall of current year
    select DISTINCT course.personId,
        course.studyAbroadStatus,
        course.isNonDegreeSeeking
    from CourseTypeCountsSTU course
    where course.yearType = 'CY'
        and course.termType = 'Fall'
    ) currenroll on currenroll.personId = degree.personId
--Include only full-time, first-time bachelor's students in this cohort.
where degree.yearType = 'PY'
    and degree.studentLevelUGGR = 'UG'
    and degree.studentType = 'First Time'
    --??Part E asks for both full time and part time students
    --and degree.timeStatus = 'FT'
    and degree.ipedsInclude = 1
    --??
    and degree.awardLevel = 'Bachelors Degree'
)
where ipedsInclude = 1
    --?? specs don't state that the retention cohort is degree seeking only. Rather, that only degree seeking inclusions are allowed. This filter is an assumption
    and isNonDegreeSeeking = false 
),

CohortExclusionMCR_RET as ( -- 6m 47s
--View used to build the retention student cohort. (CohortInd = 'Retention Fall')

select *
from (
	select cohort.*,
	    (case when exclusionENT.personId is not null then 1 else 0 end) isExclusion,
		exclusionENT.termCodeEffective,
		row_number() over (
			partition by
				cohort.personId,
				exclusionENT.termCodeEffective
			order by
                (case when to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') = cohort.snapshotDate then 1 else 2 end) asc,
                (case when to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') > cohort.snapshotDate then to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                (case when to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') < cohort.snapshotDate then to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
				exclusionENT.recordActivityDate desc
		) exclusionRn
	from CohortSTU_RET cohort
		left join cohortExclusion exclusionENT on exclusionENT.personId = cohort.personId
		    and exclusionENT.exclusionReason in ('Died', 'Medical Leave', 'Military Leave', 'Foreign Aid Service', 'Religious Leave')
		    and coalesce(exclusionENT.isIPEDSReportable, true) = true
		left join AcademicTermOrder termorder
			on termorder.termCode = exclusionENT.termCodeEffective
    where ((termorder.termOrder is not null
        and termOrder.termOrder <= (select max(termOrder1.termOrder)
                                    from ReportingPeriodMCR repper
                                    inner join AcademicTermOrder termOrder1 on termOrder1.termCode = repper.termCode
                                    where repper.surveySection = 'COHORT'))
            or termorder.termOrder is null)
	)
where exclusionRn <= 1
),

/*****
BEGIN SECTION - Student-to-Faculty Ratio Calculation
The views below pull the Instuctor count and calculates the Student-to-Faculty Ratio 
*****/

EmployeeMCR as (
--returns all employees based on HR asOfDate

select *
from (
	select --empENT.*,
	    empENT.personId,
	    empENT.primaryFunction,
	    coalesce(empENT.isIpedsMedicalOrDental, false) isIpedsMedicalOrDental,
	    to_date(empENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		config.asOfDateHR asOfDate,
		--config.termCodeFall termCodeFall,
		row_number() over (
			partition by
				empENT.personId
			order by
                (case when to_date(empENT.snapshotDate, 'YYYY-MM-DD') = config.snapshotDate then 1 else 2 end) asc,
                (case when to_date(empENT.snapshotDate, 'YYYY-MM-DD') > config.snapshotDate then to_date(empENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                (case when to_date(empENT.snapshotDate, 'YYYY-MM-DD') < config.snapshotDate then to_date(empENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
				empENT.recordActivityDate desc
		) employeeRn
	from Employee empENT
		cross join ClientConfigMCR config
	where ((empENT.terminationDate IS NULL)
		or (to_date(empENT.terminationDate, 'YYYY-MM-DD') > config.asOfDateHR
		and to_date(empENT.hireDate, 'YYYY-MM-DD') <= config.asOfDateHR))
		and coalesce(empENT.isIpedsReportable, true) = true
		and empENT.employeeStatus = 'Active'
		and ((coalesce(to_date(empENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
			and to_date(empENT.recordActivityDate, 'YYYY-MM-DD') <=  config.asOfDateHR)
				or coalesce(to_date(empENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))  
		
	)
where employeeRn = 1
),

EmployeeAssignmentMCR as (
--returns all employee assignments for employees applicable for IPEDS requirements

select *
from (
	select --empassignENT.*,
	    empassignENT.personId,
	    empassignENT.position,
	    empassignENT.fullOrPartTimeStatus,
	    emp.primaryFunction,
	    emp.isIpedsMedicalOrDental,
		emp.asOfDate asOfDate,
		emp.snapshotDate snapshotDate,
		row_number() over (
			partition by
				empassignENT.personId,
				empassignENT.position,
				empassignENT.suffix
			order by
                (case when to_date(empassignENT.snapshotDate, 'YYYY-MM-DD') = emp.snapshotDate then 1 else 2 end) asc,
                (case when to_date(empassignENT.snapshotDate, 'YYYY-MM-DD') > emp.snapshotDate then to_date(empassignENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                (case when to_date(empassignENT.snapshotDate, 'YYYY-MM-DD') < emp.snapshotDate then to_date(empassignENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
				empassignENT.recordActivityDate desc
		) jobRn
	from EmployeeMCR emp
		inner join EmployeeAssignment empassignENT on empassignENT.personId = emp.personId
			and to_date(empassignENT.assignmentStartDate, 'YYYY-MM-DD') <= emp.asOfDate
			and (to_date(empassignENT.assignmentEndDate, 'YYYY-MM-DD') IS NULL
			or to_date(empassignENT.assignmentEndDate, 'YYYY-MM-DD') >= emp.asOfDate)
			and coalesce(empassignENT.isUndergradStudent, false) = false
			and coalesce(empassignENT.isWorkStudy, false) = false
			and coalesce(empassignENT.isTempOrSeasonal, false) = false
			and coalesce(empassignENT.isIpedsReportable, true) = true
			and empassignENT.assignmentType = 'Primary'
			and empassignENT.assignmentStatus = 'Active'
			and ((coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
				and to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD') <=  emp.asOfDate)
					or coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
				
	)
where jobRn = 1
),

EmployeePositionMCR as (
--returns ESOC (standardOccupationalCategory) for all employee assignments

select *
from (
	select --empposENT.*,
	    empposENT.position,
	    empposENT.standardOccupationalCategory,
	    empassign.personId,
	    empassign.fullOrPartTimeStatus,
	    empassign.primaryFunction,
	    empassign.isIpedsMedicalOrDental,
		empassign.asOfDate asOfDate,
		empassign.snapshotDate,
		row_number() over (
			partition by
				empposENT.position
			order by
                (case when to_date(empposENT.snapshotDate, 'YYYY-MM-DD') = empassign.snapshotDate then 1 else 2 end) asc,
                (case when to_date(empposENT.snapshotDate, 'YYYY-MM-DD') > empassign.snapshotDate then to_date(empposENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                (case when to_date(empposENT.snapshotDate, 'YYYY-MM-DD') < empassign.snapshotDate then to_date(empposENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
				--empposENT.startDate desc,
				empposENT.recordActivityDate desc
		) positionRn
	from EmployeeAssignmentMCR empassign
		inner join EmployeePosition empposENT on empposENT.position = empassign.position
			and to_date(empposENT.startDate, 'YYYY-MM-DD') <= empassign.asOfDate
			and (empposENT.endDate IS NULL
			or to_date(empposENT.endDate, 'YYYY-MM-DD') >= empassign.asOfDate)
			and coalesce(empposENT.isIpedsReportable, true) = true
			and empposENT.positionStatus != 'Cancelled'
			and ((coalesce(to_date(empposENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
				and to_date(empposENT.recordActivityDate, 'YYYY-MM-DD') <=  empassign.asOfDate)
					or coalesce(to_date(empposENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))
	)
where positionRn = 1
),

InstructionalAssignmentMCR as (
--returns all instructional assignments for all employees

select *
from (
	select --instructassignENT.*,
	    instructassignENT.personId,
	    instructassignENT.termCode, 
	    instructassignENT.partOfTermCode,
	    instructassignENT.courseSectionNumber,
		emp.asOfDate asOfDate,
		emp.snapshotDate snapshotDate,
		emp.asOfDate asOfDate,
		row_number() over (
			partition by
				instructassignENT.personId,
				instructassignENT.termCode,
				instructassignENT.partOfTermCode,
				instructassignENT.courseSectionNumber,
				instructassignENT.section
			order by
                (case when to_date(instructassignENT.snapshotDate, 'YYYY-MM-DD') = emp.snapshotDate then 1 else 2 end) asc,
                (case when to_date(instructassignENT.snapshotDate, 'YYYY-MM-DD') > emp.snapshotDate then to_date(instructassignENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                (case when to_date(instructassignENT.snapshotDate, 'YYYY-MM-DD') < emp.snapshotDate then to_date(instructassignENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
				instructassignENT.recordActivityDate desc
		) jobRn
    from AcademicTermReportingRefactor rep
        inner join InstructionalAssignment instructassignENT on instructassignENT.termCode = rep.termCode
			and instructassignENT.partOfTermCode = rep.partOfTermCode
            and coalesce(instructassignENT.isIpedsReportable, true) = true
        inner join EmployeeMCR emp on emp.personId = instructassignENT.personId
    where rep.surveySection = 'COHORT'
        and ((coalesce(to_date(instructassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
            and to_date(instructassignENT.recordActivityDate, 'YYYY-MM-DD') <=  emp.asOfDate)
                or coalesce(to_date(instructassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))
	)
where jobRn = 1
),

CourseTypeCountsEMP as (
--view used to break down course category type counts by type

select instructassign.personId personId,
	SUM(case when course.courseSectionNumber is not null then 1 else 0 end) totalCourses,
	SUM(case when course.enrollmentHours = 0 then 1 else 0 end) totalNonCredCourses,
	SUM(case when course.enrollmentHours > 0 then 1 else 0 end) totalCredCourses,
	SUM(case when course.courseLevel = 'Continuing Education' then 1 else 0 end) totalCECourses
from instructionalAssignmentMCR instructassign
	left join CourseMCR course on instructassign.termCode = course.termCode
		and instructassign.partOfTermCode = course.partOfTermCode
		and instructassign.courseSectionNumber = course.courseSectionNumber
group by instructassign.personId
),

CohortFTE_EMP as ( --4m 11s
--view used to build the employee cohort to calculate instructional full time equivalency

select ROUND(FTInst + ((PTInst + PTNonInst) * 1/3), 2) FTE
from(
	select SUM(case when fullOrPartTimeStatus = 'FT'
					and isInstructional = 1 --instructional staff
					and ((totalNonCredCourses < totalCourses) --not exclusively non credit
					or (totalCECourses < totalCourses) --not exclusively CE (non credit)
					or (totalCourses IS NULL)) then 1 
					else 0 
		end) FTInst, --FT instructional staff not teaching exclusively non-credit courses
		SUM(case when fullOrPartTimeStatus = 'PT'
			and isInstructional = 1 --instructional staff
			and ((totalNonCredCourses < totalCourses) --not exclusively non credit
			or (totalCECourses < totalCourses) --not exclusively CE (non credit)
			or (totalCourses IS NULL)) then 1 
		else 0 
		end) PTInst, --PT instructional staff not teaching exclusively non-credit courses
		SUM(case when isInstructional = 0
			and NVL(totalCredCourses, 0) > 0 then 1 
		else 0 
		end) PTNonInst
	from (
		select emp.personId personId,
			NVL(empassign.fullOrPartTimeStatus, 'FT') fullOrPartTimeStatus,
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
		where emp.isIpedsMedicalOrDental = false
		)
	)
),

FTE_STU as (
--student full time equivalency calculation
--**add exclusions for teaching exclusively in stand-alone graduate or Occupational/Professional programs

select ROUND(FTStud + (PTStud * 1/3), 2) FTE
from (
	select SUM(case when cohortstu.timeStatus = 'FT' then 1 else 0 end) FTStud,
		SUM(case when cohortstu.timeStatus = 'PT' then 1 else 0 end) PTStud
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
		(1),
		--Full-time, first-time degree/certificate-seeking undergraduate
		(2),
		--Full-time, transfer-in degree/certificate-seeking undergraduate
		(3),
		--Full-time, continuing degree/certificate-seeking undergraduate
		(7),
		--Full-time, non-degree/certificate-seeking undergraduate
		(11),
		--Full-time graduate
		(15),
		--Part-time, first-time degree/certificate-seeking undergraduate
		(16),
		--Part-time, transfer-in degree/certificate-seeking undergraduate
		(17),
		--Part-time, continuing degree/certificate-seeking undergraduate
		(21),
		--Part-time, non-degree/certificate-seeking undergraduate
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
		(1),
		--Degree/Certificate seeking undergraduate students
		(2),
		--Non-Degree/Certificate seeking undergraduate Students
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
		(1),
		--Undergraduate students
		(3)  --Graduate students
	) as StudentLevel(ipedsLevel)
),

--Part B
-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'					  
FormatPartBAgeGroup as (
select *
from (
	VALUES
		(1),
		--1 - Full-time, under 18
		(2),
		--2 - Full-time, 18-19
		(3),
		--3 - Full-time, 20-21
		(4),
		--4 - Full-time, 22-24
		(5),
		--5 - Full-time, 25-29
		(6),
		--6 - Full-time, 30-34
		(7),
		--7 - Full-time, 35-39
		(8),
		--8 - Full-time, 40-49
		(9),
		--9 - Full-time, 50-64
		(10),
		--10 - Full-time, 65 and over
		(13),
		--13 - Part-time, under 18
		(14),
		--14 - Part-time, 18-19
		(15),
		--15 - Part-time, 20-21
		(16),
		--16 - Part-time, 22-24
		(17),
		--17 - Part-time, 25-29
		(18),
		--18 - Part-time, 30-34
		(19),
		--19 - Part-time, 35-39
		(20),
		--20 - Part-time, 40-49
		(21),
		--21 - Part-time, 50-64
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
-- 5m 32s

select 'A' part,
	1 sortorder,
	'99.0000' field1, --Classification of instructional program (CIP) code default (99.0000) for all institutions. 'field1' will be utilized for client CIPCodes every other reporting year
	ipedsLevel field2, -- Student level, 1,2,3,7,11,15,16,17,21, and 25 (refer to student level table (Part A) in appendix)(6,8,14,20,22, 28, 29 and 99 are for export only.)
	coalesce(SUM(case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end), 0) field3, -- Nonresident alien - Men (1), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end), 0) field4, -- Nonresident alien - Women (2), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end), 0) field5, -- Hispanic/Latino - Men (25), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end), 0) field6, -- Hispanic/Latino - Women (26), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end), 0) field7, -- American Indian or Alaska Native - Men (27), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end), 0) field8, -- American Indian or Alaska Native - Women (28), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end), 0) field9, -- Asian - Men (29), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end), 0) field10, -- Asian - Women (30), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end), 0) field11, -- Black or African American - Men (31), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end), 0) field12, -- Black or African American - Women (32), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end), 0) field13, -- Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end), 0) field14, -- Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end), 0) field15, -- White - Men (35), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end), 0) field16, -- White - Women (36), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end), 0) field17, -- Two or more races - Men (37), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end), 0) field18, -- Two or more races - Women (38), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end), 0) field19, -- Race and ethnicity unknown - Men (13), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end), 0) field20
-- Race and ethnicity unknown - Women (14), 0 to 999999
from (
	select cohort.personId personId,
		cohort.ipedsPartAStudentLevel ipedsLevel,
		cohort.ipedsGender ipedsGender,
		cohort.ipedsEthnicity ipedsEthnicity
	from CohortSTU cohort
	where cohort.ipedsPartAStudentLevel is not null
	--EF2 mod testing - no Graduate level
	--		and cohort.studentLevel = 'Undergraduate' 

	union

	select NULL, --personId,
		StudentLevel.ipedsLevel,
		NULL, -- ipedsGender,
		NULL
	-- ipedsEthnicity
	from FormatPartAStudentLevel StudentLevel
--EF2 mod testing - no Graduate level values 11 and 25
--	where StudentLevel.ipedsLevel != 11 
--		and StudentLevel.ipedsLevel != 25
	)
group by ipedsLevel

union

-- Part A: Fall Enrollment by Student Level, Attendance Status, Race/Ethnicity, and Gender
--undergraduate and graduate
-- Only included for 4 year schools on odd number years where mod(config.surveyYear,2) != 0
-- 9m 4s 

select 'A' part,
	1.5 sortorder,
	partACipCode field1, --Classification of instructional program (CIP) code default (99.0000) for all institutions. 'field1' will be utilized for client CIPCodes every other reporting year
	ipedsLevel field2, -- Student level, 1,2,3,7,11,15,16,17,21, and 25 (refer to student level table (Part A) in appendix)(6,8,14,20,22, 28, 29 and 99 are for export only.)
	coalesce(SUM(case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end), 0) field3, -- Nonresident alien - Men (1), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end), 0) field4, -- Nonresident alien - Women (2), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end), 0) field5, -- Hispanic/Latino - Men (25), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end), 0) field6, -- Hispanic/Latino - Women (26), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end), 0) field7, -- American Indian or Alaska Native - Men (27), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end), 0) field8, -- American Indian or Alaska Native - Women (28), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end), 0) field9, -- Asian - Men (29), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end), 0) field10, -- Asian - Women (30), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end), 0) field11, -- Black or African American - Men (31), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end), 0) field12, -- Black or African American - Women (32), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end), 0) field13, -- Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end), 0) field14, -- Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end), 0) field15, -- White - Men (35), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end), 0) field16, -- White - Women (36), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end), 0) field17, -- Two or more races - Men (37), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end), 0) field18, -- Two or more races - Women (38), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end), 0) field19, -- Race and ethnicity unknown - Men (13), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end), 0) field20
-- Race and ethnicity unknown - Women (14), 0 to 999999
from (
	select cohort.personId personId,
		cohort.ipedsPartAStudentLevel ipedsLevel,
		cohort.partACipCode partACipCode,
		cohort.ipedsGender ipedsGender,
		cohort.ipedsEthnicity ipedsEthnicity
	from CohortSTU cohort
	where cohort.ipedsPartAStudentLevel is not null
	    and cohort.partACipCode is not null
	--EF2 mod testing - no Graduate level
	--		and cohort.studentLevel = 'Undergraduate' 

--??Note: If there is zero enrollment for any category (e.g. student level, age, state), the line does not have to be included in the import file.
	union

	select NULL, --personId,
		StudentLevel.ipedsLevel, -- ipedsLevel,
		cipGroup.ipedsCipCodeGroup, -- partACipCode,
		NULL, -- ipedsGender,
		NULL
	-- ipedsEthnicity
	from FormatPartAStudentLevel StudentLevel
	    cross join FormatFallEnrlCipCodeGroup cipGroup
--EF2 mod testing - no Graduate level values 11 and 25
--	where StudentLevel.ipedsLevel != 11 
--		and StudentLevel.ipedsLevel != 25
	)
    --cross join ClientConfigMCR config
where (select first(surveyId) from ClientConfigMCR) = 'EF1'
    and mod((select first(surveyYear) from ClientConfigMCR),2) != 0
    and ((ipedsLevel in ('1', '2', '3', '7', '15', '16', '17') and partACipCode in ('13.0000', '14.0000', '26.0000', '27.0000', '40.0000', '52.0000'))
        or (ipedsLevel in ('11', '25') and partACipCode in ('22.0101', '51.0401', '51.1201')))
group by ipedsLevel,
    partACipCode
--order by field2

union

--Part G: Distance Education Status
--undergraduate and graduate
-- 11m 53s

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
	NULL
-- field20
from (

-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'	
	select cohort.personId personId,
		cohort.ipedsPartGStudentLevel ipedsLevel,
		cohort.distanceEdInd distanceEdInd,
		cohort.residentStatus residentStatus,
		cohort.isInAmerica
	from CohortSTU cohort
	--EF2 mod testing - no Graduate level
	--where cohort.studentLevel = 'Undergraduate' 

	union

	-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'	
	select NULL, --personId,
		StudentLevel.ipedsLevel,
		0, --distanceEdInd,
		NULL, -- residentStatus,
		0
	--isInAmerica
	from FormatPartGStudentLevel StudentLevel
	--EF2 mod testing - no Graduate level value 3
	--	where StudentLevel.ipedsLevel != 3 
)
group by ipedsLevel

union

--Part B - Fall Enrollment by Age and Gender 
--**(Part B is mandatory in this collection)**
--undergraduate and graduate
-- 15m 21s

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
	NULL
-- field20
from (

-- ak 20200413 (PF-1371) Mod to move all formatting to the 'Formatting Views'
	select cohort.personId personId,
		cohort.ipedsPartBStudentLevel ipedsLevel,
		cohort.ipedsAgeGroup ipedsAgeGroup,
		cohort.ipedsGender
	from CohortSTU cohort
--EF2 mod testing - no Graduate level
--	where cohort.studentLevel = 'Undergrad' 

	union

	select NULL, -- personId,
		StudentLevel.ipedsLevel,
		AgeGroup.ipedsAgeGroup,
		NULL
		-- ipedsGender
	from FormatPartBStudentLevel StudentLevel
		cross join FormatPartBAgeGroup AgeGroup
--EF2 mod testing - no Graduate level
	--	where StudentLevel.ipedsLevel != 3 
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
-- 19m 8s

--jh 20200422 Removed extra select statement
select 'C', -- part,
	4, -- sortorder,
	NULL, -- field1,
	cohort.ipedsStateCode, -- field2, --State of residence, 1, 2, 4–6, 8–13, 15–42, 44–51, 53–57, 60, 64, 66, 68–70, 72, 78, 90 (valid FIPS codes, refer to state table in appendix) (98 and 99 are for export only)
	case when count(cohort.personId) > 0 then count(cohort.personId) else 1 end, -- field3, --Total first-time degree/certificate-seeking undergraduates, 1 to 999999 ***error in spec - not allowing 0
	NVL(SUM(case when date_add(cohort.highSchoolGradDate, 365) >= cohort.censusDate then 1 else 0 end), 0), -- field4, --Total first-time degree/certificate-seeking undergraduates who enrolled within 12 months of graduating high school 0 to 999999 
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
	NULL
-- field20
from CohortSTU cohort
where cohort.studentType = 'First Time'
	and cohort.studentLevelUGGR = 'UG' --all versions
	--use client config value to apply optional/required survey section logic.
	and cohort.reportResidency = 'Y'
	and cohort.isNonDegreeSeeking != 1
group by cohort.ipedsStateCode

union

--Part D: Total Undergraduate Entering Class
--**This section is only applicable to degree-granting, academic year reporters (calendar system = semester, quarter, trimester, 
--**or 4-1-4) that offer undergraduate level programs and reported full-time, first-time degree/certificate seeking students in Part A.
--undergraduate ONLY
--acad reporters ONLY
-- 20m 19s

select 'D', -- part,
	5, -- sortorder,
	NULL, -- field1,
	case when COUNT(cohort.personId) = 0 then 1 else COUNT(cohort.personId) end, -- field2, --Total number of non-degree/certificate seeking undergraduates that are new to the institution in the Fall 1 to 999999 ***error in spec - not allowing 0
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
	NULL
-- field20
from CohortSTU cohort
where cohort.studentType in ('First Time', 'Transfer')
	and cohort.studentLevelUGGR = 'UG' --all versions
	
union

--jh 20200422 removed extra select and modified exclusionInd, inclusionInd and enrolledInd code
-- 25m 3s

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
	NULL
-- field20
from (
	select NVL(SUM(case when cohortret.timeStatus = 'FT'
				and cohortret.isExclusion is null
				and cohortret.isInclusion is null 
						then 1 
						else 0 
		end), 0) Ftft, --Full-time, first-time bachelor's cohort, 1 to 999999
			NVL(SUM(case when cohortret.timeStatus = 'FT' and cohortret.isExclusion = 1 then 1 end), 0) FtftEx, --Full-time, first-time bachelor's cohort exclusions, 1 to 999999
			NVL(SUM(case when cohortret.timeStatus = 'FT' and cohortret.isInclusion = 1 then 1 end), 0) FtftIn, --Full-time, first-time bachelor's cohort inclusions, 1 to 999999
			NVL(SUM(case when cohortret.timeStatus = 'FT' and cohortret.isCurEnroll = 1 then 1 end), 0) FtftEn, --Full-time, first-time bachelor's cohort students still enrolled in current fall term, 1 to 999999
			NVL(SUM(case when cohortret.timeStatus = 'PT'
				and cohortret.isExclusion is null
				and cohortret.isInclusion is null 
					then 1 
					else 0 
		end), 0) Ptft, --Part-time, first-time bachelor's cohort, 1 to 999999
			NVL(SUM(case when cohortret.timeStatus = 'PT' and cohortret.isExclusion = 1 then 1 end), 0) PtftEx, --Part-time, first-time bachelor's cohort exclusions, 1 to 999999
			NVL(SUM(case when cohortret.timeStatus = 'PT' and cohortret.isInclusion = 1 then 1 end), 0) PtftIn, --Part-time, first-time bachelor's cohort inclusions, 1 to 999999
			NVL(SUM(case when cohortret.timeStatus = 'PT' and cohortret.isCurEnroll = 1 then 1 end), 0) PtftEn
		from CohortExclusionMCR_RET cohortret
	
	)
	
union

--jh 20200422 Moved inline views to from section instead of in select field

--Part F: Student-to-Faculty Ratio
-- 32m 48s

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
	NULL
-- field20
from FTE_STU ftestu
cross join CohortFTE_EMP fteemp
--order by 1,2,3,4
