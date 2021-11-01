/********************

EVI PRODUCT:	DORIS 2021-22 IPEDS Survey  
FILE NAME: 	    200% Graduation Rates v1 (G21)
FILE DESC:      4-Year Institutions
AUTHOR:         akhasawneh
CREATED:        20211021

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Student Counts
Course Counts
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)  	Author             	    Tag             	Comments
----------- 		--------------------	-------------   	------------------------------------------------- 
20211021	        akhasawneh				                    Initial version (runtime: 5m, 53s(test data) 5m, 46s (prod)



- COHORT - Only cares about Bachelor's degree seeking/full time/first time in the cohort.
    - Academic Reporters - as of October 15, 2012 (or the institution's official fall reporting date) 
    - Program Reporters - between September 1, 2012 and August 31, 2013
- REPORTING PERIOD - July 01, 2012 through August 31, 2020.
--of the cohort group it wants:
    - exclusions between 151% and 200% completion time.
        * use the same logic for 150% dates. (September 1, 2018 through August 31, 2020 as hard end points)
    - completed a bachelor’s degree between 151% and 200% completion time.
        * ONLY BACHELOR'S AWARDS!! use the same logic for 150% dates. (September 1, 2018 through August 31, 2020 as hard end points)
    - Still enrolled as of 200% of normal time to completion

********************/ 

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.

--Production Default (Begin)
select '2122' surveyYear, 
	'G21' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2013-07-01' AS DATE) reportingDateStart,
	CAST('2021-08-31' AS DATE) reportingDateEnd,
	'201410' termCode, --Fall 2014
	'1' partOfTermCode, 
	CAST('2013-09-10' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    CAST('2013-08-31' as DATE) earliestStatusDate,
    CAST('2019-08-31' as DATE) midStatusDate, --(same as Grad Rates) "150% of normal time to completion or August 31, 2018 (whichever was earlier)"
    CAST('2021-08-31' as DATE) latestStatusDate, --151% - 200% (September 1, 2018 through August 31, 2020)
--***** start survey-specific mods
	CAST('2013-10-15' AS DATE) cohortDateEnd, --as of October 15, 2013 (or the institution's official fall reporting date) 
    'Fall' surveySection
--***** end survey-specific mods

union

select '2122' surveyYear, 
	'G21' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2013-07-01' AS DATE) reportingDateStart,
	CAST('2021-08-31' AS DATE) reportingDateEnd,
	'201230' termCode, --Fall 2014
	'1' partOfTermCode, 
	CAST('2013-08-01' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    CAST('2013-08-31' as DATE) earliestStatusDate,
    CAST('2019-08-31' as DATE) midStatusDate, --(same as Grad Rates) "150% of normal time to completion or August 31, 2018 (whichever was earlier)"
    CAST('2021-08-31' as DATE) latestStatusDate, --151% - 200% (September 1, 2018 through August 31, 2020)
--***** start survey-specific mods
	CAST('2013-10-15' AS DATE) cohortDateEnd, --as of October 15, 2012 (or the institution's official fall reporting date) 
    'Fall' surveySection
--***** end survey-specific mods

/*
--Test Default (Begin)
select '1415' surveyYear, 
	'G21' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2006-07-01' AS DATE) reportingDateStart,
	CAST('2014-08-31' AS DATE) reportingDateEnd,
	'200710' termCode, --Fall 2006
	'1' partOfTermCode, 
	CAST('2006-08-01' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    CAST('2006-08-31' as DATE) earliestStatusDate, --BS4YrDate
    CAST('2012-08-31' as DATE) midStatusDate, --(same as Grad Rates) "150% of normal time to completion or August 31, 2018 (whichever was earlier)"
    CAST('2014-08-31' as DATE) latestStatusDate,  --151% - 200% (September 1, 2018 through August 31, 2020)
--***** start survey-specific mods
	CAST('2007-10-15' AS DATE) cohortDateEnd, --as of October 15, 2012 (or the institution's official fall reporting date) 
    'Fall' surveySection
--***** end survey-specific mods

union

select '1415' surveyYear, 
	'G21' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2006-07-01' AS DATE) reportingDateStart,
	CAST('2014-08-31' AS DATE) reportingDateEnd,
	'200630' termCode, --Fall 2006
	'1' partOfTermCode, 
	CAST('2006-06-15' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    CAST('2006-08-31' as DATE) earliestStatusDate, --BS4YrDate
    CAST('2012-08-31' as DATE) midStatusDate, --(same as Grad Rates) "150% of normal time to completion or August 31, 2018 (whichever was earlier)"
    CAST('2014-08-31' as DATE) latestStatusDate,  --151% - 200% (September 1, 2018 through August 31, 2020)
--***** start survey-specific mods
	CAST('2007-10-15' AS DATE) cohortDateEnd, --as of October 15, 2012 (or the institution's official fall reporting date) 
    'Prior Summer' surveySection
--***** end survey-specific mods
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
    coalesce(upper(RepDates.surveySection), 'FALL') surveySection,
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
    RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,
    to_date(RepDates.censusDate,'YYYY-MM-DD') censusDate,
	to_date(RepDates.reportingDateStart,'YYYY-MM-DD') reportingDateStart,
    to_date(RepDates.reportingDateEnd,'YYYY-MM-DD') reportingDateEnd,
    RepDates.repPeriodTag1 repPeriodTag1,
	RepDates.repPeriodTag2 repPeriodTag2,
	RepDates.repPeriodTag3 repPeriodTag3,
    RepDates.earliestStatusDate earliestStatusDate,
    RepDates.midStatusDate midStatusDate,
    RepDates.latestStatusDate latestStatusDate
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
		defvalues.repPeriodTag3 repPeriodTag3,
        defvalues.earliestStatusDate earliestStatusDate,
        defvalues.midStatusDate midStatusDate,
        defvalues.latestStatusDate latestStatusDate,
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
		defvalues.censusDate censusDate,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    defvalues.repPeriodTag3 repPeriodTag3,
        defvalues.earliestStatusDate earliestStatusDate,
        defvalues.midStatusDate midStatusDate,
        defvalues.latestStatusDate latestStatusDate,
		1 reportPeriodRn
	from DefaultValues defvalues
    where defvalues.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = defvalues.surveyYear
											and upper(repperiodENT.surveyId) = defvalues.surveyId 
											and repperiodENT.termCode is not null
											and repperiodENT.partOfTermCode is not null
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
    to_date(ConfigLatest.snapshotDate,'YYYY-MM-DD') snapshotDate,
    ConfigLatest.repperiodSnapshotDate repperiodSnapshotDate,
	upper(ConfigLatest.genderForUnknown) genderForUnknown,
	upper(ConfigLatest.genderForNonBinary) genderForNonBinary,
    upper(ConfigLatest.instructionalActivityType) instructionalActivityType,
    upper(ConfigLatest.acadOrProgReporter) acadOrProgReporter,
    upper(ConfigLatest.publicOrPrivateInstitution) publicOrPrivateInstitution,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
	ConfigLatest.repPeriodTag2 repPeriodTag2,
	ConfigLatest.repPeriodTag3 repPeriodTag3,
    ConfigLatest.earliestStatusDate earliestStatusDate,
    ConfigLatest.midStatusDate midStatusDate,
    ConfigLatest.latestStatusDate latestStatusDate,
--***** start survey-specific mods
    ConfigLatest.cohortDateEnd cohortDateEnd,
	ConfigLatest.surveySection surveySection,
    ConfigLatest.reportingDateStart reportingDateStart
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
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    defvalues.repPeriodTag3 repPeriodTag3,
        defvalues.earliestStatusDate earliestStatusDate,
        defvalues.midStatusDate midStatusDate,
        defvalues.latestStatusDate latestStatusDate,
--***** start survey-specific mods
        defvalues.cohortDateEnd cohortDateEnd,
		defvalues.surveySection surveySection,
        repperiod.reportingDateStart reportingDateStart,
--***** end survey-specific mods
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
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    defvalues.repPeriodTag3 repPeriodTag3,
        defvalues.earliestStatusDate earliestStatusDate,
        defvalues.midStatusDate midStatusDate,
        defvalues.latestStatusDate latestStatusDate,	    
--***** start survey-specific mods
        defvalues.cohortDateEnd cohortDateEnd,
		defvalues.surveySection surveySection,
        defvalues.reportingDateStart reportingDateStart,
--***** end survey-specific mods
		1 configRn
    from DefaultValues defvalues
    where defvalues.surveyYear not in (select max(configENT.surveyCollectionYear)
										from IPEDSClientConfig configENT
										where configENT.surveyCollectionYear = defvalues.surveyYear)
	) ConfigLatest
where ConfigLatest.configRn = 1	
),

AcademicTermMCR as ( --1 sec
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

AcademicTermOrder as ( --1 sec
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
        'CY' yearType,
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
                            and ((array_contains(acadterm.tags, 'Fall Census') and acadterm.termType = 'Fall' and repperiod.surveySection in ('FALL', 'COHORT'))
                                or (array_contains(acadterm.tags, 'Pre-Fall Summer Census') and acadterm.termType = 'Summer' and repperiod.surveySection = 'PRIOR SUMMER')) then 1
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
        potMax.partOfTermCode maxPOT,
--***** start survey-specific mods
        (select max(termorder.termorder)
         from AcademicTermMCR term 
            inner join AcademicTermOrder termorder on termorder.termCode = term.termCode
         where term.termType = 'Fall'
            and term.startdate <= (select first(midStatusDate) from ClientConfigMCR)) fallTermOrder150, 
        (select max(termorder.termorder)
         from AcademicTermMCR term 
            inner join AcademicTermOrder termorder on termorder.termCode = term.termCode
         where term.termType = 'Fall'
            and term.startdate <= (select first(latestStatusDate) from ClientConfigMCR)) fallTermOrder200
--***** end survey-specific mods
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

CampusMCR as ( --3 sec
-- Returns most recent campus record for all snapshots in the ReportingPeriod

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

DegreeMCR as ( --10 sec
-- Returns all degree values at the end of the reporting period

select *
from (
    select upper(degreeENT.degree) degree,
                degreeENT.degreeLevel degreeLevel,
                degreeENT.awardLevel awardLevel,
                to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
                (case  
                    when degreeENT.awardLevel in ('Associates Degree', 'Postsecondary (>1800 clock/>60 semester credit/>90 quarter credit hours)') then 2                   
                    when degreeENT.awardLevel = 'Bachelors Degree' then 3 
                    when degreeENT.awardLevel in ('Postsecondary (<300 clock/<9 semester credit/<13 quarter credit hours)', 
                                                'Postsecondary (300-899 clock/9-29 semester credit/13-44 quarter credit hours)', 
                                                'Postsecondary (900-1800 clock/30-60 semester credit/45-90 quarter credit hours)') then 1
                else 0 end) awardLevelNo,
                coalesce(row_number() over (
                    partition by
                        degreeENT.snapshotDate,
                        degreeENT.degreeLevel,
                        degreeENT.awardLevel,
                        degreeENT.degree
                    order by
                        degreeENT.recordActivityDate desc
                ), 1) degreeRn		
        from Degree degreeENT 
        where degreeENT.isIpedsReportable = 1
                and degreeENT.isNonDegreeSeeking = 0
                and degreeENT.awardLevel is not null
                and ((to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                    and to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') <= to_date(degreeENT.snapshotDate, 'YYYY-MM-DD'))
                        or to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
   ) 
where degreeRn = 1
),

DegreeProgramMCR as ( --8 sec
-- Returns all degreeProgram values at the end of the reporting period

select *
from (
    select DISTINCT
		upper(programENT.degreeProgram) degreeProgram, 
		to_date(programENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		upper(programENT.degree) degree,
		upper(programENT.major) major, 
		upper(programENT.college) college, 
		upper(programENT.campus) campus, 
		upper(programENT.department) department,
		to_date(programENT.startDate, 'YYYY-MM-DD') startDate,
		programENT.termCodeEffective,
		termorder.termOrder,
		programENT.lengthInMonths,
		programENT.lengthInCreditOrClockHours,
		programENT.isClockHours,
		to_date(programENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
		coalesce(row_number() over (
			partition by
				programENT.snapshotDate,
				programENT.degreeProgram,
				programENT.degree,
				programENT.major,
				programENT.campus
			order by
			    (case when to_date(programENT.startDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE) then to_date(programENT.startDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
			    (case when termorder.termOrder is not null then termorder.termOrder else 1 end) desc,
				programENT.recordActivityDate desc
		), 1) programRN
	from DegreeProgram programENT 
	    left join AcademicTermOrder termorder on programENT.termCodeEffective = termorder.termCode
	where programENT.lengthInMonths is not null
	    and programENT.isIPEDSReportable = 1
		and programENT.isForStudentAcadTrack = 1
		and programENT.isForDegreeAcadHistory = 1
		and ((to_date(programENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
				and to_date(programENT.recordActivityDate,'YYYY-MM-DD') <= to_date(programENT.snapshotDate, 'YYYY-MM-DD'))
					or to_date(programENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
		and ((to_date(programENT.startDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
				and to_date(programENT.startDate,'YYYY-MM-DD') <= to_date(programENT.snapshotDate, 'YYYY-MM-DD'))
					or to_date(programENT.startDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
    )
where programRN = 1
),

ProgramAwardLevel as ( --8 sec
-- Returns all degreeProgram and the degree award level values at the end of the reporting period

select *
from (
    select DegProg.*,
            deg.awardLevel,
            deg.awardLevelNo,
            deg.degreeLevel,
            coalesce(row_number() over (
                partition by
                    DegProg.snapshotDate,
                    DegProg.degreeProgram,
                    DegProg.degree,
                    deg.degreeLevel,
                    DegProg.major,
			        DegProg.campus
                order by
                    (case when deg.snapshotDate = DegProg.snapshotDate then 1 else 2 end) asc,
                    (case when deg.snapshotDate < DegProg.snapshotDate then deg.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                    (case when deg.snapshotDate > DegProg.snapshotDate then deg.snapshotDate else CAST('9999-09-09' as DATE) end) asc
        ), 1) progdegRn
    from DegreeProgramMCR DegProg
        left join DegreeMCR deg on DegProg.degree = deg.degree
													 
)
where progdegRn = 1
),

RegStatusMCR as (

select personId
from (
    select distinct regENT.personId personId, 
            coalesce(row_number() over (
                partition by
                    regENT.personId
                order by 
                    (case when array_contains(regENT.tags, config.repPeriodTag1) then 1 else 2 end) asc,
                    regENT.snapshotDate desc
            ), 1) regRn
        from Registration regENT
            cross join (select first(latestStatusDate) latestStatusDate,
                                    first(repPeriodTag1) repPeriodTag1
                            from ReportingPeriodMCR) config
            inner join AcademicTermMCR repperiod on repperiod.startDate between date_sub(config.latestStatusDate, 90) and date_add(config.latestStatusDate, 60)
                and repperiod.termCode = regENT.termCode
                and repperiod.partOfTermCode = regENT.partOfTermCode
        where ((to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                            and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= config.latestStatusDate)
                        or (to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                            and ((to_date(regENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                                    and to_date(regENT.recordActivityDate,'YYYY-MM-DD') <= config.latestStatusDate)
                                or to_date(regENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE)))) 
                and regENT.isEnrolled = 1
                and regENT.isIpedsReportable = 1
        )
where regRn = 1 
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
	    regData.termStartDateFall termStartDateFall,
        regData.fallTermOrder150, 
        regData.fallTermOrder200,
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
        repperiod.termStartDateFall termStartDateFall,
        repperiod.fallTermOrder150, 
        repperiod.fallTermOrder200,
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
            where stu.surveySection in ('COHORT', 'FALL')
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
        reg.censusDate,
        reg.termType,
        reg.termOrder,
        reg.fallTermOrder150, 
        reg.fallTermOrder200,
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
        --reg.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
        reg.termStartDateFall termStart,
        --reg.sfaReportPriorYear sfaReportPriorYear,
        --reg.sfaReportSecondPriorYear sfaReportSecondPriorYear,
        --reg.caresAct1 caresAct1,
        --reg.caresAct2 caresAct2,
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
			coursesect.censusDate censusDate,
			coursesect.termType termType,
			coursesect.termOrder termOrder, 
            coursesect.fallTermOrder150, 
            coursesect.fallTermOrder200,
			coursesect.requiredFTCreditHoursUG,
			coursesect.requiredFTClockHoursUG,
			coursesect.instructionalActivityType,
			coursesect.personId personId,
			coursesect.studentLevel,
			coursesect.studentType,
			coursesect.isNonDegreeSeeking,
			coursesect.ipedsGender,
			coursesect.ipedsEthnicity,
            --coursesect.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
            coursesect.termStart termStart,
            --coursesect.sfaReportPriorYear sfaReportPriorYear,
            --coursesect.sfaReportSecondPriorYear sfaReportSecondPriorYear,
            --coursesect.caresAct1 caresAct1,
            --coursesect.caresAct2 caresAct2,
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
	    termorder.termOrder courseTermOrder,
	    coursesectsched.termOrder termOrder,
        coursesectsched.fallTermOrder150, 
        coursesectsched.fallTermOrder200,
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
        --coursesectsched.repPeriodTag2 repPeriodTag2, --'Financial Aid Year End'
        coursesectsched.termStart termStart,
        --coursesectsched.sfaReportPriorYear sfaReportPriorYear,
        --coursesectsched.sfaReportSecondPriorYear sfaReportSecondPriorYear,
        --coursesectsched.caresAct1 caresAct1,
        --coursesectsched.caresAct2 caresAct2,
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


    select cohort.*,
        acadRep.censusDate, 
        acadRep.snapshotDate,
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
                termStart,
                termOrder,
                fallTermOrder150, 
                fallTermOrder200,
                instructionalActivityType,
                requiredFTCreditHoursUG,
                requiredFTClockHoursUG,
                studentLevel,
                studentType,
                residency,
                ipedsGender,
                ipedsEthnicity,
                totalClockHrs,
                totalCreditHrs,
                fullTimePartTimeStatus
        from (
             select course.yearType,
                    course.termCode,
                    course.termStart,
                    course.termOrder,
                    course.fallTermOrder150, 
                    course.fallTermOrder200,                    
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
                    course.termStart,
                    course.termOrder,
                    course.fallTermOrder150, 
                    course.fallTermOrder200,
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
                    course.studyAbroadStatus,
		            course.fullTimePartTimeStatus
            )
        ) cohort
    inner join (select first(ref.maxCensus) censusDate,
                    first(ref.snapshotDate) snapshotDate,
                    ref.termcode termcode
                    from AcademicTermReportingRefactor ref
                    where ref.partOfTermCode = ref.maxPOT
                    group by termcode) acadRep on acadRep.termcode = cohort.termcode
    where ipedsInclude = 1
    ),
    
AcademicTrackMCR as ( 
--Returns most current data based on initial academic track of student

select * 
from (
	select coh2.personId personId,
			coh2.yearType yearType,
            coh2.snapshotDate snapshotDate,
			coh2.termCode termCode,
			coh2.censusDate censusDate,
			coh2.termOrder termOrder,
            coh2.termStart,
            coh2.fallTermOrder150, 
            coh2.fallTermOrder200,
            config.repPeriodTag1,
            config.earliestStatusDate,
            config.midStatusDate,
            config.latestStatusDate,
            acadtrack.departmentCOH departmentCOH,
			acadtrack.degreeCOH degreeCOH,           
            acadtrack.collegeCOH collegeCOH,
            acadtrack.campusCOH campusCOH,
            acadtrack.degreeProgramCOH degreeProgramCOH,
            acadtrack.academicTrackLevelCOH academicTrackLevelCOH,
            acadtrack.fieldOfStudyCOH fieldOfStudyCOH,
            acadtrack.fieldOfStudyPriorityCOH fieldOfStudyPriorityCOH,
--internal testing
            --coalesce(acadtrack.degreeCOH, 'BA') degreeCOH,
            --coalesce(acadtrack.collegeCOH, 'AS') collegeCOH,
            --coalesce(acadtrack.campusCOH, 'BA') campusCOH,
            --coalesce(acadtrack.degreeProgramCOH, 'BA-ENGL') degreeProgramCOH,
            --coalesce(acadtrack.academicTrackLevelCOH, 'Undergrad') academicTrackLevelCOH,
            --coalesce(acadtrack.fieldOfStudyCOH, 'ENGL') fieldOfStudyCOH,
            --coalesce(acadtrack.fieldOfStudyPriorityCOH, 1) fieldOfStudyPriorityCOH,
--internal testing end
            coalesce(row_number() over (
                partition by
                    coh2.personId
                order by
                    coalesce(acadtrack.fieldOfStudyPriorityCOH, 1) asc
            ), 1) fosRn
    from CourseTypeCountsSTU coh2
        left join (
                select coh.personId personId,
                        coalesce(row_number() over (
                            partition by					
                                coh.yearType,
                                acadTrackENT.personId,
                                acadtrackENT.degree,
                                acadtrackENT.degreeProgram,
                                acadtrackENT.fieldOfStudyPriority
                            order by
                                (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') = coh.snapshotDate then 1 else 2 end) asc,
                                (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') > coh.snapshotDate then to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                                (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') < coh.snapshotDate then to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
                                termorder.termOrder desc,
                                acadtrackENT.fieldOfStudyActionDate desc, 
                                acadtrackENT.recordActivityDate desc,
                                (case when acadtrackENT.academicTrackStatus = 'In Progress' then 1 else 2 end) asc
                        ), 1) acadtrackRn,
                        coh.censusDate censusDate,
                        coh.termOrder termOrder,
                        upper(acadtrackENT.degree) degreeCOH,
                        upper(acadtrackENT.college) collegeCOH,
                        upper(acadtrackENT.campus) campusCOH,
                        upper(acadtrackENT.department) departmentCOH,
                        acadtrackENT.termCodeEffective termCodeEffective,
                        upper(acadtrackENT.degreeProgram) degreeProgramCOH,
                        acadtrackENT.academicTrackLevel academicTrackLevelCOH,
                        acadtrackENT.fieldOfStudy fieldOfStudyCOH,
                        acadtrackENT.fieldOfStudyPriority fieldOfStudyPriorityCOH,
                        acadtrackENT.academicTrackStatus academicTrackStatusCOH,
                        to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') fieldOfStudyActionDateCOH,
                        to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDateCOH,
                        termorder.termOrder acadTrackTermOrder
                from CourseTypeCountsSTU coh
                    inner join AcademicTrack acadtrackENT on acadtrackENT.personId = coh.personId
                        and acadTrackENT.fieldOfStudyType = 'Major'
                        and acadTrackENT.fieldOfStudy is not null
                        and acadTrackENT.isIpedsReportable = 1
                        and acadTrackENT.isCurrentFieldOfStudy = 1
                        and ((to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                                and to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') <= coh.censusDate)
                            or (to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)					
                                and ((to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                                        and to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') <= coh.censusDate) 
                                    or to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)))) 
                    inner join AcademicTermOrder termorder on termorder.termCode = acadtrackENT.termCodeEffective
                        and termorder.termOrder <= coh.termOrder
            ) acadtrack on coh2.personId = acadtrack.personId
                    and acadtrack.acadtrackRn = 1
        cross join (select 
                        first(repPeriodTag1) repPeriodTag1,
                        first(repPeriodTag2) repPeriodTag2,
                        first(earliestStatusDate) earliestStatusDate,
                        first(midStatusDate) midStatusDate,
                        first(latestStatusDate) latestStatusDate,
                        first(cohortDateEnd) cohortDateEnd
                    from ClientConfigMCR) config    
    )
where fosRn = 1
),

Cohort as ( --4 min 39 sec
--returns degree program data for academic track of student

select *,
	least(to_date(add_months(termStart, (lengthInMonths * 1.5)),'YYYY-MM-DD'), midStatusDate) gradDate150,
	least(to_date(add_months(termStart, (lengthInMonths * 2)),'YYYY-MM-DD'), latestStatusDate) gradDate200
from (
    select acadtrack.personId personId,
			(case when deg.awardLevelNo = 3 then 2 else 3 end) subCohort, --2 bach-seeking, 3 other-seeking
			acadtrack.yearType yearType,
            acadtrack.snapshotDate snapshotDate,
			acadtrack.termCode termCode,
			acadtrack.censusDate censusDate,
			acadtrack.termOrder termOrder,
            acadtrack.fallTermOrder150, 
            acadtrack.fallTermOrder200,
            acadtrack.repPeriodTag1,
            acadtrack.earliestStatusDate,
            acadtrack.midStatusDate,
            acadtrack.latestStatusDate,
			acadtrack.degreeCOH degreeCOH,
            acadtrack.collegeCOH collegeCOH,
            acadtrack.campusCOH campusCOH,
            acadtrack.departmentCOH departmentCOH,
            acadtrack.degreeProgramCOH degreeProgramCOH,
            acadtrack.academicTrackLevelCOH academicTrackLevelCOH,
            acadtrack.fieldOfStudyCOH fieldOfStudyCOH,
            deg.awardLevel awardLevel,
			deg.awardLevelNo awardLevelNo,
			acadtrack.termStart termStart,
			deg.lengthInMonths lengthInMonths,
			coalesce(row_number() over (
                partition by					
                    acadtrack.yearType,
                    acadtrack.personId,
                    acadtrack.degreeCOH,
                    acadtrack.degreeProgramCOH
                order by
                    (case when deg.snapshotDate = acadtrack.snapshotDate then 1 else 2 end) asc,
                    (case when deg.snapshotDate > acadtrack.snapshotDate then deg.snapshotDate else CAST('9999-09-09' as DATE) end) desc,
                    (case when deg.snapshotDate < acadtrack.snapshotDate then deg.snapshotDate else CAST('1900-09-09' as DATE) end) asc
            ), 1) degRn
      from AcademicTrackMCR acadtrack
            left join ProgramAwardLevel deg on acadtrack.degreeProgramCOH = deg.degreeProgram
                and acadtrack.degreeCOH = deg.degree
        )
where degRn = 1
    or degRn is null
),

AwardMCR as ( --4 min 45 sec
--Pulls all distinct student awards obtained as-of reporting end (200%) date

select *
from (
    select cohort2.personId personId,
			cohort2.subCohort subCohort,
            (case when awardENT.awardedDate is not null then 1 else 0 end) awardInd,
            cohort2.yearType yearType,
            cohort2.snapshotDate cohortSnapshotDate,
			cohort2.termCode termCode,           
			cohort2.termStart termStart,
			cohort2.censusDate censusDate,
			cohort2.termOrder termOrder,
            cohort2.fallTermOrder150 fallTermOrder150, 
            cohort2.fallTermOrder200 fallTermOrder200,
			cohort2.degreeCOH degreeCOH,
            cohort2.collegeCOH collegeCOH,
            cohort2.campusCOH campusCOH,
            cohort2.departmentCOH departmentCOH,
            cohort2.degreeProgramCOH degreeProgramCOH,
            cohort2.academicTrackLevelCOH academicTrackLevelCOH,
            cohort2.fieldOfStudyCOH fieldOfStudyCOH,
            cohort2.awardLevel awardLevelCOH,
			cohort2.awardLevelNo awardLevelNoCOH,
			cohort2.lengthInMonths lengthInMonthsCOH,
			cohort2.earliestStatusDate earliestStatusDate,
			cohort2.midStatusDate midStatusDate,
			cohort2.latestStatusDate latestStatusDate,
            cohort2.repPeriodTag1 repPeriodTag1,
            cohort2.gradDate150 gradDate150COH,
            cohort2.gradDate200 gradDate200COH,
            awardENT.snapshotDate snapshotDate,
            awardENT.awardedDate awardedDate,
            awardENT.degreeProgram awardedDegreeProgram,
            awardENT.degree awardedDegree,
            awardENT.degreeLevel awardedDegreeLevel,
                    coalesce(row_number() over (
                        partition by
                            cohort2.personId,
                            awardENT.personId,
                            awardENT.awardedDate,
                            awardENT.degreeProgram,
                            awardENT.degreeLevel,
                            awardENT.degree
                        order by
                            (case when array_contains(awardENT.tags, cohort2.repPeriodTag1) then 1 else 2 end) asc,
                            awardENT.snapshotDate desc,
                            awardENT.recordActivityDate desc
                    ), 1) awardLatestRn
    from Cohort cohort2
        left join Award awardENT on cohort2.personId = awardENT.personId
            and awardENT.isIpedsReportable = 1
            and awardENT.awardStatus = 'Awarded'
            and awardENT.degreeLevel is not null
            and awardENT.awardedDate is not null
            and awardENT.degreeLevel != 'Continuing Ed'
            and to_date(awardENT.awardedDate,'YYYY-MM-DD') between cohort2.termStart and cohort2.latestStatusDate
            and ((to_date(awardENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                and to_date(awardENT.recordActivityDate,'YYYY-MM-DD') <= cohort2.latestStatusDate)
                    or to_date(awardENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
        left join AcademicTermOrder termorder on termorder.termCode = awardENT.awardedTermCode
        left join CampusMCR campus on upper(awardENT.campus) = campus.campus
            and campus.isInternational = false
    )
    where (awardLatestRn = 1 or awardLatestRn is null)
),

CohortStatus as ( --5 min 18 sec
-- Pulls degree information as of the reporting period and assigns status based on priority (in order): completers, transfer, exclusion, still enrolled

select *
from ( 
select *,
--if awarded degree program is same length as initial degree program, grad date 150% is same, else re-calculate based on awarded degree program
			(case when awardedLengthInMonths = lengthInMonthsCOH then gradDate150COH
                    else least(to_date(add_months(termStart, (awardedLengthInMonths * 1.5)),'YYYY-MM-DD'), midStatusDate) end) gradDate150AW,
--if awarded degree program is same length as initial degree program, grad date 200% is same, else re-calculate based on awarded degree program
			(case when awardedLengthInMonths = lengthInMonthsCOH then gradDate200COH
                    else least(to_date(add_months(termStart, (awardedLengthInMonths * 2)),'YYYY-MM-DD'), latestStatusDate) end) gradDate200AW,
			row_number() over (
				partition by
					personId
				order by
					awardedAwardLevelNo desc,
					awardedLengthInMonths asc
			) awardRn
	from (
		select award.personId personId,
				award.subCohort subCohort,
				award.awardInd awardInd,
				award.termOrder termOrder,
                award.fallTermOrder150 fallTermOrder150, 
                award.fallTermOrder200 fallTermOrder200,
				award.termStart termStart,
				--award.ipedsGender ipedsGender,
				--award.ipedsEthnicity ipedsEthnicity,
				award.midStatusDate midStatusDate,
				award.earliestStatusDate earliestStatusDate,
				award.latestStatusDate latestStatusDate,
				award.snapshotDate snapshotDate,
				award.gradDate150COH gradDate150COH,
				award.gradDate200COH gradDate200COH,
				award.awardedDate awardedDate,
				award.awardedDegreeProgram awardedDegreeProgram,
				award.awardedDegree awardedDegree,
				award.awardedDegreeLevel awardedDegreeLevel,
				award.lengthInMonthsCOH lengthInMonthsCOH,
				deg.awardLevel awardedAwardLevel,
				deg.awardLevelNo awardedAwardLevelNo,
				deg.lengthInMonths awardedLengthInMonths,
				reg.personId regPersonId,
				row_number() over (
					partition by
						award.personId,
						award.awardedDate,
						award.awardedDegree,
						award.awardedDegreeProgram,
						award.awardedDegreeLevel
					order by
						(case when deg.snapshotDate = award.snapshotDate then 1 else 2 end) asc,
						(case when deg.snapshotDate > award.snapshotDate then deg.snapshotDate else CAST('9999-09-09' as DATE) end) desc,
						(case when deg.snapshotDate < award.snapshotDate then deg.snapshotDate else CAST('1900-09-09' as DATE) end) asc
				) as degreeRn
		from AwardMCR award
			left join ProgramAwardLevel deg on award.awardedDegreeProgram = deg.degreeProgram
				and award.awardedDegree = deg.degree
				and award.awardedDegreeLevel = deg.degreeLevel
			left join RegStatusMCR reg on award.personId = reg.personId
		)        
	where degreeRn <= 1
 )                   
where awardRn <= 1
--***** start survey-specific mods
    and awardedAwardLevel = 'Bachelors Degree'
--***** end survey-specific mods
),

CohortExclusionMCR as ( --5 min 5 sec
--Pulls students who left the institution between 151% - 200% date and can be removed from the cohort for one of the IPEDS allowable reasons

select *
from (
    select coh.*,
        exclusionENT.personId exclPersonId,
        row_number() over (
            partition by
                coh.personId
            order by
                (case when array_contains(exclusionENT.tags, config.repPeriodTag1) then 1 else 2 end) asc,
                exclusionENT.snapshotDate desc,
                termorder.termOrder asc,
                exclusionENT.recordActivityDate desc
            ) exclRn
        from CohortStatus coh
            cross join (select first(repPeriodTag1) repPeriodTag1
                            from ClientConfigMCR) config
            left join CohortExclusion exclusionENT on coh.personId = exclusionENT.personId
                and exclusionENT.exclusionReason in ('Died', 'Medical Leave', 'Military Leave', 'Foreign Aid Service', 'Religious Leave')
                and exclusionENT.isIPEDSReportable = 1
            and ((to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                    and (to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') <= coh.gradDate200COH
                        and to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') > coh.gradDate150COH))
                or to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
            left join AcademicTermOrder termorder on exclusionENT.termCodeEffective = termorder.termCode
        where (termorder.termOrder is null 
            or (coh.fallTermOrder150 <= termorder.termOrder
                and coh.fallTermOrder200 >= termorder.termOrder))
    )
where exclRn <= 1
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

select 'A' part,
    sum(isExclusion) field1, --Additional Exclusions
    sum(completer200) field2, --Completers in years 7 and years 8 (completed a bachelor's degree between 151% and 200% of normal time to completion)
    sum(isEnrolled200) field3 --Still enrolled as of 200% of normal time
from (
    select personId,
        (case when awardInd = 0 and exclPersonId is not null then 1 else 0 end) isExclusion,
        (case when awardInd = 1  
            and awardedDate > gradDate150AW
            and awardedDate <= gradDate200AW then 1 else 0 end) completer200,
        (case when awardInd = 0 and regPersonId is not null then 1 else 0 end) isEnrolled200
    from CohortExclusionMCR
    
    union
    
    select null,
        0,
        0,
        0
    )
group by part
