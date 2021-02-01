/********************

EVI PRODUCT:	DORIS 2020-21 IPEDS Survey  
FILE NAME: 		200% Graduation Rates v1 (G21)
FILE DESC:      4-Year Institutions
AUTHOR:         akhasawneh
CREATED:        20200129

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Student Counts
Course Counts
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)  	Author             	    Tag             	Comments
----------- 		--------------------	-------------   	------------------------------------------------- 
20200129	        akhasawneh				                    Initial version (runtime: 5m, 53s(test data) 5m, 46s (prod)



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
select '2021' surveyYear, 
	'G21' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2012-07-01' AS DATE) reportingDateStart,
	CAST('2020-08-31' AS DATE) reportingDateEnd,
	'201310' termCode, --Fall 2014
	'1' partOfTermCode, 
	CAST('2012-09-10' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    CAST('2012-08-31' as DATE) earliestStatusDate,
    CAST('2018-08-31' as DATE) midStatusDate, --(same as Grad Rates) "150% of normal time to completion or August 31, 2018 (whichever was earlier)"
    CAST('2020-08-31' as DATE) latestStatusDate, --151% - 200% (September 1, 2018 through August 31, 2020)
--***** start survey-specific mods
	CAST('2012-10-15' AS DATE) cohortDateEnd, --as of October 15, 2012 (or the institution's official fall reporting date) 
    'Fall' surveySection
--***** end survey-specific mods

union

select '2021' surveyYear, 
	'G21' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2012-07-01' AS DATE) reportingDateStart,
	CAST('2020-08-31' AS DATE) reportingDateEnd,
	'201230' termCode, --Fall 2014
	'1' partOfTermCode, 
	CAST('2012-08-01' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    CAST('2012-08-31' as DATE) earliestStatusDate,
    CAST('2018-08-31' as DATE) midStatusDate, --(same as Grad Rates) "150% of normal time to completion or August 31, 2018 (whichever was earlier)"
    CAST('2020-08-31' as DATE) latestStatusDate, --151% - 200% (September 1, 2018 through August 31, 2020)
--***** start survey-specific mods
	CAST('2012-10-15' AS DATE) cohortDateEnd, --as of October 15, 2012 (or the institution's official fall reporting date) 
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

ReportingPeriodMCR as ( --6 sec
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

ClientConfigMCR as ( --8 sec
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 
--          1st union 1st order - pull snapshot for 'June End' 
--          1st union 2nd order - pull snapshot for 'August End'
--          1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--          2nd union - pull default values if no record in IPEDSClientConfig

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.source source,
    to_date(ConfigLatest.snapshotDate,'YYYY-MM-DD') snapshotDate,
    ConfigLatest.repperiodSnapshotDate repperiodSnapshotDate,
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
--where ConfigLatest.configRn = 1	
),

AcademicTermMCR as ( --1 sec
--Returns most recent (recordActivityDate) term code record for all term codes and parts of term code for all snapshots. 

select termCode, 
	partOfTermCode, 
	--financialAidYear,
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
		--acadtermENT.financialAidYear,
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
        --repPerTerms.financialAidYear financialAidYear,
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
        --acadterm.financialAidYear financialAidYear,
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
                            and ((array_contains(acadterm.tags, 'Fall Census') and acadterm.termType = 'Fall' and repperiod.surveySection = 'FALL')
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
                row_number() over (
                    partition by
                        degreeENT.snapshotDate,
                        degreeENT.degreeLevel,
                        degreeENT.awardLevel,
                        degreeENT.degree
                    order by
                        degreeENT.recordActivityDate desc
                ) degreeRn		
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
		programENT.degreeLevel, 
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
		row_number() over (
			partition by
				programENT.snapshotDate,
				programENT.degreeProgram,
				programENT.degreeLevel,
				programENT.degree,
				programENT.major,
				programENT.campus
			order by
			    (case when to_date(programENT.startDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE) then to_date(programENT.startDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
			    (case when termorder.termOrder is not null then termorder.termOrder else 1 end) desc,
				programENT.recordActivityDate desc
		) programRN
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
            row_number() over (
                partition by
                    DegProg.snapshotDate,
                    DegProg.degreeProgram,
                    DegProg.degreeLevel,
                    DegProg.degree,
                    DegProg.major,
			        DegProg.campus
                order by
                    (case when deg.snapshotDate = DegProg.snapshotDate then 1 else 2 end) asc,
                    (case when deg.snapshotDate < DegProg.snapshotDate then deg.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                    (case when deg.snapshotDate > DegProg.snapshotDate then deg.snapshotDate else CAST('9999-09-09' as DATE) end) asc
        ) progdegRn
    from DegreeProgramMCR DegProg
        left join DegreeMCR deg on DegProg.degree = deg.degree
            and DegProg.degreeLevel = deg.degreeLevel
)
where progdegRn = 1
),

RegistrationMCR as ( --1 min 15 sec
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
        --regData.financialAidYear,
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
        regData.personId,                    
        regData.crn,
        regData.crnLevel,    
        regData.crnGradingMode,
        regData.termStartDateFall termStartDateFall,
        regData.fallTermOrder150, 
        regData.fallTermOrder200,
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
            --repperiod.financialAidYearFall financialAidYear,
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
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
                    regENT.recordActivityDate desc
            ) regRn
        from AcademicTermReportingRefactor repperiod   
            inner join Registration regENT on regENT.termCode = repperiod.termCode
                and regENT.partOfTermCode = repperiod.partOfTermCode
                and ((to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                            and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
                        or (to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                            and ((to_date(regENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                                    and to_date(regENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                                or to_date(regENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))))
                and regENT.isEnrolled = 1
                and regENT.isIpedsReportable = 1 
                ) regData
            left join CampusMCR campus on regData.campus = campus.campus
        where regData.regRn = 1
    )
where regCampRn = 1
),

RegStatusMCR as ( --1 min 1 sec

select personId
from (
    select distinct regENT.personId personId, 
            row_number() over (
                partition by
                    regENT.personId
                order by 
                    (case when array_contains(regENT.tags, config.repPeriodTag1) then 1 else 2 end) asc,
                    regENT.snapshotDate desc
            ) regRn
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

StudentMCR as ( --1 min 24 sec
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  

select stuData.yearType,
        stuData.surveySection,
        stuData.snapshotDate,
        stuData.termCode, 
        stuData.termOrder,
        --stuData.financialAidYear,
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
                    when stuData.studentLevel = 'Other' then true
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
            --reg.financialAidYear financialAidYear,
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
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
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
	where yearType = 'CY'
	) stuData
where stuData.studRn = 1 
),

StudentRefactor as ( --1 min 39 sec
--Determine student info based on full term and degree-seeking status
--Drop surveySection from select fields and use yearType only going forward - Prior Summer sections only used to determine student type

select *
from ( 
    select FallStu.personId personId,
            FallStu.termCode termCode,
            FallStu.yearType yearType,
            FallStu.studentLevel studentLevel,
            (case when FallStu.studentType = 'Returning' and SumStu.personId is not null then 'First Time' else FallStu.studentType end) studentType,
            FallStu.isNonDegreeSeeking isNonDegreeSeeking,
            FallStu.snapshotDate
    from (
        select *
         from (
            select stu.yearType,
                    stu.surveySection,
                    stu.snapshotDate,
                    stu.termCode, 
                    stu.termOrder,
                    --stu.financialAidYear,
                    stu.maxCensus,
                    stu.termType,
                    stu.startDate,
                    stu.censusDate,
                    stu.maxCensus,
                    stu.fullTermOrder,
                    stu.startDate,
                    stu.personId,
                    stu.isNonDegreeSeeking,
                    stu.campus,
                    stu.studentType,
                    stu.studentLevel studentLevelORIG,
                    (case when stu.studentLevel = 'Graduate' then 'GR'
                        else 'UG' 
                    end) studentLevel,
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
                   left join CampusMCR campus on stu.campus = campus.campus
            where stu.termType = 'Fall'
            ) 
        where regCampRn = 1 
            and isInternational = false
        ) FallStu
        left join (select stu2.personId personId,
                          stu2.studentType studentType
                    from StudentMCR stu2
                    where stu2.termType != 'Fall'
                        and stu2.studentType = 'First Time') SumStu on FallStu.personId = SumStu.personId
    where FallStu.studentLevel = 'UG'
        and FallStu.isNonDegreeSeeking != 1
    )
--***** start survey-specific mods
where studentType = 'First Time'
--***** end survey-specific mods
),

CourseSectionMCR as ( 
--Included to get enrollment hours of a CRN
    
select *
from (
    select stu.yearType,
        reg.snapshotDate snapshotDate,
        stu.snapshotDate stuSSD,
        to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') courseSectionSSD,
        reg.termCode,
        reg.partOfTermCode,
        --reg.financialAidYear,
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
        reg.termStartDateFall termStart,
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
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    coursesectENT.recordActivityDate desc
            ) courseRn
    from RegistrationMCR reg  
        inner join StudentRefactor stu on stu.personId = reg.personId 
            and stu.termCode = reg.termCode 
            and stu.yearType = reg.yearType
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

select *
from (
	select coursesect.yearType yearType,
	    coursesect.snapshotDate snapshotDate, 
	    coursesect.courseSectionSSD courseSectionSSD,
	    to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') courseSectSchedSSD,
	    coursesect.termCode termCode,
	    coursesect.partOfTermCode partOfTermCode,
	    --coursesect.financialAidYear,
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
        coursesect.termStart termStart,
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
                (case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') > coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') < coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
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
	     coursesectsched.snapshotDate snapshotDate,
	    coursesectsched.courseSectionSSD courseSectionSSD,
	    coursesectsched.courseSectSchedSSD courseSectSchedSSD,
	    to_date(courseENT.snapshotDate, 'YYYY-MM-DD') courseSSD,
	    coursesectsched.termCode termCode,
		coursesectsched.partOfTermCode partOfTermCode,
		--coursesectsched.financialAidYear,
	    coursesectsched.termOrder termOrder,
        coursesectsched.fallTermOrder150, 
        coursesectsched.fallTermOrder200,
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
        coursesectsched.termStart termStart,
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
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') > coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') < coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
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

CourseTypeCountsSTU as ( --2 min 34 sec
-- View used to break down course category type counts for student

select yearType,
        snapshotDate,
        censusDate,
        --financialAidYear,
        personId,
        termCode,
        termStart,
        termOrder,
        fallTermOrder150, 
        fallTermOrder200,
        --config.financialAidEndDate,
        config.repPeriodTag1,
        config.repPeriodTag2,
        config.earliestStatusDate,
        config.midStatusDate,
        config.latestStatusDate,
        --config.genderForNonBinary,
        --config.genderForUnknown,
        config.cohortDateEnd
from (
    select yearType,
            snapshotDate,
            censusDate,
            --financialAidYear,
            personId,
            termCode,
            termStart,
            termOrder, 
            fallTermOrder150, 
            fallTermOrder200,
            --fullTermOrder,
          --  (case when studentType = 'First Time' and isNonDegreeSeeking = 0 then
                    (case when instructionalActivityType in ('CR', 'B') then 
                                (case when totalCreditHrsCalc >= requiredFTCreditHoursUG then 'FT' else 'PT' end)
                          when instructionalActivityType = 'CL' then 
                                (case when totalClockHrs >= requiredFTClockHoursUG then 'FT' else 'PT' end) 
                          else 'UG null' end) 
            --    else null end) 
            timeStatus,
            (case when totalCredCourses > 0 --exclude students not enrolled for credit
                            then (case when totalESLCourses = totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
                                       when totalCECourses = totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
                                       when totalIntlCourses = totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
                                       when totalAuditCourses = totalCredCourses then 0 --exclude students exclusively auditing classes
                                       when totalOccCourses = totalCredCourses then 0 --exclude students exclusively taking occupational courses
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
                --course.financialAidYear,
                course.instructionalActivityType,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId,
                course.termCode,
                course.termStart,
                course.termOrder,
                course.fallTermOrder150, 
                course.fallTermOrder200,
                course.studentLevel,
                course.studentType,
                course.isNonDegreeSeeking,
                sum((case when course.enrollmentHours >= 0 then 1 else 0 end)) totalCourses,
                sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 then course.enrollmentHours else 0 end)) totalCreditHrs,
                sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then coalesce(course.enrollmentHours, 0) else 0 end)) totalCreditUGHrs,
                --sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel = 'Graduate' then coalesce(course.enrollmentHours, 0) else 0 end)) totalCreditGRHrs,
                sum((case when course.isClockHours = 1 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then course.enrollmentHours else 0 end)) totalClockHrs,
                --sum((case when course.enrollmentHours = 0 then 1 else 0 end)) totalNonCredCourses,
                sum((case when course.enrollmentHours > 0 then 1 else 0 end)) totalCredCourses,
                --sum((case when course.meetingType = 'Online/Distance Learning' then 1 else 0 end)) totalDECourses,
                --sum((case when course.courseLevel = 'Undergrad' then 1 else 0 end)) totalUGCourses,
                --sum((case when course.courseLevel = 'Graduate' then 1 else 0 end)) totalGRCourses,
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
                --course.financialAidYear,
                course.instructionalActivityType,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId,
                course.termCode,
                course.termStart,
                course.termOrder,
                course.fallTermOrder150, 
                course.fallTermOrder200,
                course.studentLevel,
                course.studentType,
                course.isNonDegreeSeeking
        )
    )    
    cross join (select --first(financialAidEndDate) financialAidEndDate,
                        first(repPeriodTag1) repPeriodTag1,
                        first(repPeriodTag2) repPeriodTag2,
                        first(earliestStatusDate) earliestStatusDate,
                        first(midStatusDate) midStatusDate,
                        first(latestStatusDate) latestStatusDate,
                        --first(genderForNonBinary) genderForNonBinary,
                        --first(genderForUnknown) genderForUnknown,
                        first(cohortDateEnd) cohortDateEnd
                    from ClientConfigMCR) config
where ipedsInclude = 1
   and timeStatus = 'FT'
),

AcademicTrackMCR as ( --4 min 36 sec
--Returns most current data based on initial academic track of student

select * 
from (
	select course2.personId personId,
			course2.yearType yearType,
            course2.snapshotDate snapshotDate,
			course2.termCode termCode,
			course2.censusDate censusDate,
			course2.termOrder termOrder,
            course2.fallTermOrder150, 
            course2.fallTermOrder200,
            course2.termStart,
            course2.repPeriodTag1,
            course2.earliestStatusDate,
            course2.midStatusDate,
            course2.latestStatusDate,
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
            row_number() over (
                partition by
                    course2.personId
                order by
                    coalesce(acadtrack.fieldOfStudyPriorityCOH, 1) asc
            ) fosRn
    from CourseTypeCountsSTU course2
        left join (
                select course.personId personId,
                        row_number() over (
                            partition by					
                                course.yearType,
                                acadTrackENT.personId,
                                acadtrackENT.degree,
                                acadtrackENT.degreeProgram,
                                acadtrackENT.fieldOfStudyPriority
                            order by
                                (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') = course.snapshotDate then 1 else 2 end) asc,
                                (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') > course.snapshotDate then to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                                (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') < course.snapshotDate then to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
                                termorder.termOrder desc,
                                acadtrackENT.fieldOfStudyActionDate desc, 
                                acadtrackENT.recordActivityDate desc,
                                (case when acadtrackENT.academicTrackStatus = 'In Progress' then 1 else 2 end) asc
                        ) acadtrackRn,
                        course.censusDate censusDate,
                        course.termOrder termOrder,
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
                from CourseTypeCountsSTU course
                    inner join AcademicTrack acadtrackENT on acadtrackENT.personId = course.personId
                        and acadTrackENT.fieldOfStudyType = 'Major'
                        and acadTrackENT.fieldOfStudy is not null
                        and acadTrackENT.isIpedsReportable = 1
                        and acadTrackENT.isCurrentFieldOfStudy = 1
                        and ((to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                                and to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') <= course.censusDate)
                            or (to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)					
                                and ((to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                                        and to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') <= course.censusDate) 
                                    or to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)))) 
                    inner join AcademicTermOrder termorder on termorder.termCode = acadtrackENT.termCodeEffective
                        and termorder.termOrder <= course.termOrder
            ) acadtrack on course2.personId = acadtrack.personId
                    and acadtrack.acadtrackRn = 1
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
			row_number() over (
                partition by					
                    acadtrack.yearType,
                    acadtrack.personId,
                    acadtrack.degreeCOH,
                    acadtrack.degreeProgramCOH
                order by
                    (case when deg.snapshotDate = acadtrack.snapshotDate then 1 else 2 end) asc,
                    (case when deg.snapshotDate > acadtrack.snapshotDate then deg.snapshotDate else CAST('9999-09-09' as DATE) end) desc,
                    (case when deg.snapshotDate < acadtrack.snapshotDate then deg.snapshotDate else CAST('1900-09-09' as DATE) end) asc
            ) degRn
      from AcademicTrackMCR acadtrack
            left join ProgramAwardLevel deg on acadtrack.degreeProgramCOH = deg.degreeProgram
                and acadtrack.degreeCOH = deg.degree
                and acadtrack.academicTrackLevelCOH = deg.degreeLevel
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
                    row_number() over (
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
                    ) awardLatestRn
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

