/********************

EVI PRODUCT:	DORIS 2020-21 IPEDS Survey  
FILE NAME: 		Graduation Rates v1 (GR1)
FILE DESC:      Graduation Rates for 4-Year Institutions
AUTHOR:         akhasawneh
CREATED:        20210126

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Student Counts
Course Counts
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)  	Author             	    Tag             	Comments
----------- 		--------------------	-------------   	------------------------------------------------- 
20210126	        akhasawneh				                    Initial version (runtime: prod 19m 21s, test 18m 58s)

Testing notes - test data with one full snapshot and one for transfer
ReportingPeriodMCR as ( --6 sec
ClientConfigMCR as ( --8 sec
AcademicTermMCR as ( --1 sec
AcademicTermOrder as ( --1 sec
AcademicTermReporting as ( --16 sec
AcademicTermReportingRefactor as ( --1 min 3 sec
CampusMCR as ( --3 sec
DegreeMCR as ( --10 sec
DegreeProgramMCR as ( --8 sec
ProgramAwardLevel as ( --8 sec    
RegistrationMCR as ( --1 min 15 sec
StudentMCR as ( --1 min 24 sec
StudentRefactor as ( --1 min 39 sec
CourseTypeCountsSTU as ( --2 min 34 sec
FinancialAidMCR as ( --3 min 53 sec
AcademicTrackMCR as ( --6 min 33 sec
Cohort as ( --6 min 34 sec
AwardMCR as ( --7 min 37 sec
CohortStatus as ( --12 min 26 sec (new awardMCR)

********************/ 

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.

--Production Default (Begin)
select '2021' surveyYear, 
	'GR1' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2014-07-01' AS DATE) reportingDateStart,
	CAST('2020-08-31' AS DATE) reportingDateEnd,
	'201510' termCode, --Fall 2014
	'1' partOfTermCode, 
	CAST('2014-09-10' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    CAST('2015-05-30' as DATE) financialAidEndDate, --For GR report: only aid for first year of enrollment(July 1 - June 30)
    CAST('2018-08-31' as DATE) earliestStatusDate, --BS4YrDate
    CAST('2019-08-31' as DATE) midStatusDate, --BS5YrDate
    CAST('2020-08-31' as DATE) latestStatusDate, --BS6YrDate
--***** start survey-specific mods
	CAST('2015-08-31' AS DATE) cohortDateEnd,
    'Fall' surveySection,
	'000000' ncSchoolCode,
    '00' ncBranchCode,
    CAST('2021-02-10' as DATE) surveyCloseDate
--***** end survey-specific mods

union

select '2021' surveyYear, 
	'GR1' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2014-07-01' AS DATE) reportingDateStart,
	CAST('2020-08-31' AS DATE) reportingDateEnd,
	'201430' termCode, --Fall 2014
	'1' partOfTermCode, 
	CAST('2014-06-01' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    CAST('2015-05-30' as DATE) financialAidEndDate, --For GR report: only aid for first year of enrollment(July 1 - June 30)
    CAST('2018-08-31' as DATE) earliestStatusDate, --BS4YrDate
    CAST('2019-08-31' as DATE) midStatusDate, --BS5YrDate
    CAST('2020-08-31' as DATE) latestStatusDate, --BS6YrDate
--***** start survey-specific mods
	CAST('2015-08-31' AS DATE) cohortDateEnd,
    'Fall' surveySection,
	'000000' ncSchoolCode,
    '00' ncBranchCode,
    CAST('2021-02-10' as DATE) surveyCloseDate
--***** end survey-specific mods

/*
--Test Default (Begin)
select '1415' surveyYear, 
	'GR1' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2008-07-01' AS DATE) reportingDateStart, 
	CAST('2014-08-31' AS DATE) reportingDateEnd,
	'200910' termCode, --Fall 2008
	'1' partOfTermCode, 
	CAST('2008-09-15' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    CAST('2009-05-30' as DATE) financialAidEndDate, --For GR report: only aid for first year of enrollment(July 1 - June 30)
    CAST('2012-08-31' as DATE) earliestStatusDate, --BS4YrDate
    CAST('2013-08-31' as DATE) midStatusDate, --BS5YrDate
    CAST('2014-08-31' as DATE) latestStatusDate, --BS6YrDate
--***** start survey-specific mods
	CAST('2009-08-31' AS DATE) cohortDateEnd,
    'Fall' surveySection,
	'000000' ncSchoolCode,
    '00' ncBranchCode,
    CAST('2021-02-10' as DATE) surveyCloseDate
--***** end survey-specific mods

union

select '1415' surveyYear, 
	'GR1' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2008-07-01' AS DATE) reportingDateStart,
	CAST('2014-08-31' AS DATE) reportingDateEnd, --'2009-08-31'
	'200830' termCode, --Summer 2008
	'1' partOfTermCode, 
	CAST('2008-06-15' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    CAST('2009-05-30' as DATE) financialAidEndDate, --For GR report: only aid for first year of enrollment(July 1 - June 30)
    CAST('2012-08-31' as DATE) earliestStatusDate, --BS4YrDate
    CAST('2013-08-31' as DATE) midStatusDate, --BS5YrDate
    CAST('2014-08-31' as DATE) latestStatusDate, --BS6YrDate
--***** start survey-specific mods
	CAST('2009-08-31' AS DATE) cohortDateEnd,
    'Prior Summer' surveySection,
	'000000' ncSchoolCode,
    '00' ncBranchCode,
    CAST('2021-02-10' as DATE) surveyCloseDate
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
    to_date(RepDates.financialAidEndDate,'YYYY-MM-DD') financialAidEndDate,
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
		defvalues.financialAidEndDate financialAidEndDate,
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
		defvalues.financialAidEndDate financialAidEndDate,
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
	ConfigLatest.financialAidEndDate financialAidEndDate,
    ConfigLatest.earliestStatusDate earliestStatusDate,
    ConfigLatest.midStatusDate midStatusDate,
    ConfigLatest.latestStatusDate latestStatusDate,
--***** start survey-specific mods
    ConfigLatest.cohortDateEnd cohortDateEnd,
	ConfigLatest.surveySection surveySection,
	ConfigLatest.ncSchoolCode ncSchoolCode,
    ConfigLatest.ncBranchCode ncBranchCode,
    ConfigLatest.reportingDateStart reportingDateStart,
    ConfigLatest.surveyCloseDate surveyCloseDate
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
        defvalues.financialAidEndDate financialAidEndDate,
        defvalues.earliestStatusDate earliestStatusDate,
        defvalues.midStatusDate midStatusDate,
        defvalues.latestStatusDate latestStatusDate,
--***** start survey-specific mods
        defvalues.cohortDateEnd cohortDateEnd,
		defvalues.surveySection surveySection,
		coalesce(clientConfigENT.ncSchoolCode, defvalues.ncSchoolCode) ncSchoolCode,
        coalesce(clientConfigENT.ncBranchCode, defvalues.ncBranchCode) ncBranchCode,
        repperiod.reportingDateStart reportingDateStart,
        defvalues.surveyCloseDate surveyCloseDate,
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
        defvalues.financialAidEndDate financialAidEndDate,
        defvalues.earliestStatusDate earliestStatusDate,
        defvalues.midStatusDate midStatusDate,
        defvalues.latestStatusDate latestStatusDate,	    
--***** start survey-specific mods
        defvalues.cohortDateEnd cohortDateEnd,
		defvalues.surveySection surveySection,
		defvalues.ncSchoolCode ncSchoolCode,
        defvalues.ncBranchCode ncBranchCode,
        defvalues.reportingDateStart reportingDateStart,
        defvalues.surveyCloseDate surveyCloseDate,
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
        acadterm.financialAidYear financialAidYear,
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
        (select first(financialAidYear) from AcademicTermReporting where termType = 'Fall') financialAidYearFall,
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

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusMCR as (
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

DegreeMCR as ( 
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

DegreeProgramMCR as (
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

TransferMCR as (
--Pulls students who left the institution and can be removed from the cohort for one of the IPEDS allowable reasons

select personId
from (
    select transferENT.personId personId,
        row_number() over (
            partition by
                transferENT.personId
            order by
                (case when array_contains(transferENT.tags, config.repPeriodTag3) then 1 else 2 end) asc,
                transferENT.snapshotDate desc,
                transferENT.recordActivityDate desc
            ) transRn
        from Transfer transferENT
            cross join (select first(latestStatusDate) latestStatusDate,
                                    first(repPeriodTag3) repPeriodTag3,
                                    first(reportingDateStart) reportingDateStart,
                                    first(ncBranchCode) ncBranchCode,
                                    first(ncSchoolCode) ncSchoolCode,
                                    first(surveyCloseDate) surveyCloseDate
                            from ClientConfigMCR) config
        where to_date(transferENT.startDate,'YYYY-MM-DD') >= config.reportingDateStart
                and (transferENT.collegeCodeBranch != concat(config.ncSchoolCode, '-', config.ncBranchCode)
                    and transferENT.collegeCodeBranch != concat(config.ncSchoolCode, config.ncBranchCode))
                and ((to_date(transferENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                           and to_date(transferENT.recordActivityDate,'YYYY-MM-DD') <= config.surveyCloseDate)
                        or to_date(transferENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
    )
where transRn = 1
),

CohortExclusionMCR as (
--Pulls students who left the institution and can be removed from the cohort for one of the IPEDS allowable reasons

select personId
from (
    select exclusionENT.personId personId,
        row_number() over (
            partition by
                exclusionENT.personId
            order by
                (case when array_contains(exclusionENT.tags, config.repPeriodTag1) then 1 else 2 end) asc,
                exclusionENT.snapshotDate desc,
                termorder.termOrder asc,
                exclusionENT.recordActivityDate desc
            ) exclRn
        from CohortExclusion exclusionENT
            inner join AcademicTermOrder termorder on exclusionENT.termCodeEffective = termorder.termCode
            cross join (select first(latestStatusDate) latestStatusDate,
                                    first(repPeriodTag1) repPeriodTag1
                            from ReportingPeriodMCR) config
        where exclusionENT.exclusionReason in ('Died', 'Medical Leave', 'Military Leave', 'Foreign Aid Service', 'Religious Leave')
            and exclusionENT.isIPEDSReportable = 1
            and termorder.termOrder >= (select first(termCodeFallOrder)
                                                    from AcademicTermReportingRefactor)
            and ((to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                    and to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') <= config.latestStatusDate)
                or to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
    )
where exclRn = 1
),

RegStatusMCR as (

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
		regData.instructionalActivityType,
        regData.equivCRHRFactor,
        regData.personId,                    
        regData.crn,
        regData.crnLevel,    
        regData.crnGradingMode,
        regData.termStartDateFall termStartDateFall,
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
            repperiod.financialAidYearFall financialAidYear,
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

StudentRefactor as ( 
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
                    stu.financialAidYear,
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
		coursesectsched.financialAidYear,
	    coursesectsched.termOrder termOrder,
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
		coalesce(courseENT.isRemedial, 0) isRemedial,
		coalesce(courseENT.isESL, 0) isESL,
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

CourseTypeCountsSTU as ( 
-- View used to break down course category type counts for student

select yearType,
        snapshotDate,
        censusDate,
        financialAidYear,
        personId,
        termCode,
        termStart,
        termOrder,
        config.financialAidEndDate,
        config.repPeriodTag1,
        config.repPeriodTag2,
        config.earliestStatusDate,
        config.midStatusDate,
        config.latestStatusDate,
        config.genderForNonBinary,
        config.genderForUnknown,
        config.cohortDateEnd
from (
    select yearType,
            snapshotDate,
            censusDate,
            financialAidYear,
            personId,
            termCode,
            termStart,
            termOrder, 
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
                course.financialAidYear,
                course.instructionalActivityType,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId,
                course.termCode,
                course.termStart,
                course.termOrder,
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
                sum((case when course.isESL = 1 then 1 else 0 end)) totalESLCourses,
                sum((case when course.isRemedial = 1 then 1 else 0 end)) totalRemCourses,
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
                course.termCode,
                course.termStart,
                course.termOrder,
                course.studentLevel,
                course.studentType,
                course.isNonDegreeSeeking
        )
    )    
    cross join (select first(financialAidEndDate) financialAidEndDate,
                        first(repPeriodTag1) repPeriodTag1,
                        first(repPeriodTag2) repPeriodTag2,
                        first(earliestStatusDate) earliestStatusDate,
                        first(midStatusDate) midStatusDate,
                        first(latestStatusDate) latestStatusDate,
                        first(genderForNonBinary) genderForNonBinary,
                        first(genderForUnknown) genderForUnknown,
                        first(cohortDateEnd) cohortDateEnd
                    from ClientConfigMCR) config
where ipedsInclude = 1
   and timeStatus = 'FT'
),

PersonMCR as ( 
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

select pers.*,
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
    select distinct stu.personId personId,
            stu.yearType yearType,
            stu.snapshotDate snapshotDate,
            stu.financialAidYear financialAidYear,
            stu.termCode termCode,
            stu.censusDate censusDate,
            stu.termStart,
            stu.termOrder termOrder,
            stu.financialAidEndDate,
            stu.repPeriodTag1,
            stu.repPeriodTag2,
            stu.earliestStatusDate,
            stu.midStatusDate,
            stu.latestStatusDate,
            stu.genderForNonBinary,
            stu.genderForUnknown,
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
                    stu.personId,
                    personENT.personId
                order by
                    (case when to_date(personENT.snapshotDate,'YYYY-MM-DD') = stu.snapshotDate then 1 else 2 end) asc,
			        (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') > stu.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') < stu.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    personENT.recordActivityDate desc
            ) personRn
    from CourseTypeCountsSTU stu 
        inner join Person personENT on stu.personId = personENT.personId
            and personENT.isIpedsReportable = 1
            and ((to_date(personENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
               and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= stu.cohortDateEnd) 
                or to_date(personENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
    ) pers
where pers.personRn = 1
),

FinancialAidMCR as ( 
--Included to determine Pell and subsidized loan recipients within the first year of entering institution. 
--IPEDS requirement: recipient receives and uses the award within their first year at the institution (July 1 - June 30)

select pers2.personId personId,
        pers2.yearType yearType,
        pers2.snapshotDate snapshotDate,
        pers2.financialAidYear financialAidYear,
        pers2.termCode termCode,
        pers2.censusDate censusDate,
        pers2.termStart,
        pers2.termOrder termOrder,
        pers2.financialAidEndDate,
        pers2.repPeriodTag1,
        pers2.repPeriodTag2,
        pers2.earliestStatusDate,
        pers2.midStatusDate,
        pers2.latestStatusDate,
        pers2.ipedsGender,
        pers2.ipedsEthnicity,
        (case when finaid.isPellRec > 0 then 1 else 0 end) isPellRec,
        (case when finaid.isPellRec > 0 then 0
              when finaid.isSubLoanRec > 0 then 1 
              else 0 
        end) isSubLoanRec
from PersonMCR pers2 
    left join (  
        select personId personId,
                sum((case when isPellGrant = 'Y' then IPEDSOutcomeMeasuresAmount else 0 end)) isPellRec,
                sum((case when isSubsidizedDirectLoan = 'Y' then IPEDSOutcomeMeasuresAmount else 0 end)) isSubLoanRec
        from ( 
            select DISTINCT pers.yearType yearType,
                    to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') snapshotDateFA,
                    pers.financialAidYear financialAidYear,
                    pers.personId personId,
                    FinancialAidENT.isPellGrant isPellGrant,
                    FinancialAidENT.isSubsidizedDirectLoan isSubsidizedDirectLoan,
                    (case when FinancialAidENT.IPEDSOutcomeMeasuresAmount is not null and FinancialAidENT.IPEDSOutcomeMeasuresAmount > 0 then FinancialAidENT.IPEDSOutcomeMeasuresAmount
                         else FinancialAidENT.paidAmount
                    end) IPEDSOutcomeMeasuresAmount,
                    row_number() over (
                        partition by
                             pers.yearType,
                             FinancialAidENT.termCode,
                             pers.financialAidYear,
                             pers.personId
                        order by 	
                            (case when array_contains(FinancialAidENT.tags, rep.repPeriodTag2) then 1 else 2 end) asc,
                            (case when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') between date_sub(rep.financialAidEndDate, 30) and date_add(rep.financialAidEndDate, 10) then 1 else 2 end) asc,	    
                            (case when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') > rep.financialAidEndDate then to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                            (case when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') < rep.financialAidEndDate then to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                            FinancialAidENT.recordActivityDate desc,
                            (case when FinancialAidENT.awardStatus in ('Source Offered', 'Student Accepted') then 1 else 2 end) asc
                    ) finAidRn
            from PersonMCR pers  
                cross join (select first(financialAidEndDate) financialAidEndDate,
                                    first(repPeriodTag2) repPeriodTag2
                            from ReportingPeriodMCR) rep
                inner join FinancialAid FinancialAidENT on pers.personId = FinancialAidENT.personId
                    and pers.financialAidYear = FinancialAidENT.financialAidYear
                    and (FinancialAidENT.isPellGrant = 1
                        or FinancialAidENT.isSubsidizedDirectLoan = 1)
                    and (FinancialAidENT.paidAmount > 0 
                        or FinancialAidENT.IPEDSOutcomeMeasuresAmount > 0)
                    and FinancialAidENT.awardStatus not in ('Source Denied', 'Cancelled')
                    and FinancialAidENT.isIpedsReportable = 1
                    and ((to_date(FinancialAidENT.awardStatusActionDate , 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                            and to_date(FinancialAidENT.awardStatusActionDate , 'YYYY-MM-DD') <= rep.financialAidEndDate)
                        or (to_date(FinancialAidENT.awardStatusActionDate , 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                            and ((to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                                    and to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD') <= rep.financialAidEndDate)
                                or to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))))
                )
            where finAidRn = 1
            group by personId 
        ) finaid on pers2.personId = finaid.personId
),

AcademicTrackMCR as ( 
--Returns most current data based on initial academic track of student

select * 
from (
	select finaid2.personId personId,
			finaid2.yearType yearType,
            finaid2.snapshotDate snapshotDate,
			finaid2.termCode termCode,
			finaid2.censusDate censusDate,
			finaid2.termOrder termOrder,
            finaid2.termStart,
            finaid2.repPeriodTag1,
            finaid2.earliestStatusDate,
            finaid2.midStatusDate,
            finaid2.latestStatusDate,
			finaid2.ipedsGender ipedsGender,
			finaid2.ipedsEthnicity ipedsEthnicity,
			finaid2.isPellRec isPellRec,
			finaid2.isSubLoanRec isSubLoanRec,
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
                    finaid2.personId
                order by
                    coalesce(acadtrack.fieldOfStudyPriorityCOH, 1) asc
            ) fosRn
    from FinancialAidMCR finaid2
        left join (
                select finaid.personId personId,
                        row_number() over (
                            partition by					
                                finaid.yearType,
                                acadTrackENT.personId,
                                acadtrackENT.degree,
                                acadtrackENT.degreeProgram,
                                acadtrackENT.fieldOfStudyPriority
                            order by
                                (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') = finaid.snapshotDate then 1 else 2 end) asc,
                                (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') > finaid.snapshotDate then to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) desc,
                                (case when to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') < finaid.snapshotDate then to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) asc,
                                termorder.termOrder desc,
                                acadtrackENT.fieldOfStudyActionDate desc, 
                                acadtrackENT.recordActivityDate desc,
                                (case when acadtrackENT.academicTrackStatus = 'In Progress' then 1 else 2 end) asc
                        ) acadtrackRn,
                        finaid.censusDate censusDate,
                        finaid.termOrder termOrder,
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
                from FinancialAidMCR finaid
                    inner join AcademicTrack acadtrackENT on acadtrackENT.personId = finaid.personId
                        and acadTrackENT.fieldOfStudyType = 'Major'
                        and acadTrackENT.fieldOfStudy is not null
                        and acadTrackENT.isIpedsReportable = 1
                        and acadTrackENT.isCurrentFieldOfStudy = 1
                        and ((to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                                and to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') <= finaid.censusDate)
                            or (to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)					
                                and ((to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                                        and to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') <= finaid.censusDate) 
                                    or to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)))) 
                    inner join AcademicTermOrder termorder on termorder.termCode = acadtrackENT.termCodeEffective
                        and termorder.termOrder <= finaid.termOrder
            ) acadtrack on finaid2.personId = acadtrack.personId
                    and acadtrack.acadtrackRn = 1
    )
where fosRn = 1
),

Cohort as ( 
--returns degree program data for academic track of student

select *,
	least(to_date(add_months(termStart, (lengthInMonths * 1.5)),'YYYY-MM-DD'), latestStatusDate) gradDate150
from (
    select acadtrack.personId personId,
			(case when deg.awardLevelNo = 3 then 2 else 3 end) subCohort, --2 bach-seeking, 3 other-seeking
			acadtrack.yearType yearType,
            acadtrack.snapshotDate snapshotDate,
			acadtrack.termCode termCode,
			acadtrack.censusDate censusDate,
			acadtrack.termOrder termOrder,
            acadtrack.repPeriodTag1,
            acadtrack.earliestStatusDate,
            acadtrack.midStatusDate,
            acadtrack.latestStatusDate,
			acadtrack.ipedsGender ipedsGender,
			acadtrack.ipedsEthnicity ipedsEthnicity,
			acadtrack.isPellRec isPellRec,
			acadtrack.isSubLoanRec isSubLoanRec,
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

AwardMCR as (
--Pulls all distinct student awards obtained as-of four year status date '2020-08-31'

select *
from (
    select cohort2.personId personId,
			cohort2.subCohort subCohort,
			cohort2.isPellRec isPellRec,
			cohort2.isSubLoanRec isSubLoanRec,
            (case when awardENT.awardedDate is not null then 1 else 0 end) awardInd,
			cohort2.ipedsGender ipedsGender,
			cohort2.ipedsEthnicity ipedsEthnicity,
            cohort2.yearType yearType,
            cohort2.snapshotDate cohortSnapshotDate,
			cohort2.termCode termCode,           
			cohort2.termStart termStart,
			cohort2.censusDate censusDate,
			cohort2.termOrder termOrder,
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

CohortStatus as (
-- Pulls degree information as of the reporting period and assigns status based on priority (in order): completers, transfer, exclusion, still enrolled

select personId personId,
        subCohort subCohort, --2 bach-seeking, 3 other-seeking
		isPellRec isPellRec,
		isSubLoanRec isSubLoanRec,
        ipedsGender ipedsGender,
		ipedsEthnicity ipedsEthnicity,
        (case when ipedsEthnicity ='1' and ipedsGender = 'M' then 1 else 0 end) field1, --Nonresident Alien
        (case when ipedsEthnicity ='1' and ipedsGender = 'F' then 1 else 0 end) field2,
        (case when ipedsEthnicity ='2' and ipedsGender = 'M' then 1 else 0 end) field3, -- Hispanic/Latino
        (case when ipedsEthnicity ='2' and ipedsGender = 'F' then 1 else 0 end) field4,
        (case when ipedsEthnicity ='3' and ipedsGender = 'M' then 1 else 0 end) field5, -- American Indian or Alaska Native
        (case when ipedsEthnicity ='3' and ipedsGender = 'F' then 1 else 0 end) field6,
        (case when ipedsEthnicity ='4' and ipedsGender = 'M' then 1 else 0 end) field7, -- Asian
        (case when ipedsEthnicity ='4' and ipedsGender = 'F' then 1 else 0 end) field8,
        (case when ipedsEthnicity ='5' and ipedsGender = 'M' then 1 else 0 end) field9, -- Black or African American
        (case when ipedsEthnicity ='5' and ipedsGender = 'F' then 1 else 0 end) field10,
        (case when ipedsEthnicity ='6' and ipedsGender = 'M' then 1 else 0 end) field11, -- Native Hawaiian or Other Pacific Islander
        (case when ipedsEthnicity ='6' and ipedsGender = 'F' then 1 else 0 end) field12,
        (case when ipedsEthnicity ='7' and ipedsGender = 'M' then 1 else 0 end) field13, -- White
        (case when ipedsEthnicity ='7' and ipedsGender = 'F' then 1 else 0 end) field14,
        (case when ipedsEthnicity ='8' and ipedsGender = 'M' then 1 else 0 end) field15, -- Two or more races
        (case when ipedsEthnicity ='8' and ipedsGender = 'F' then 1 else 0 end) field16,
        (case when ipedsEthnicity ='9' and ipedsGender = 'M' then 1 else 0 end) field17, -- Race and ethnicity unknown
        (case when ipedsEthnicity ='9' and ipedsGender = 'F' then 1 else 0 end) field18,
--if awarded degree program is less than 2 years and within the awarded grad date 150%
        (case when awardedAwardLevelNo = 1 and awardedDate <= gradDate150AW then 1 else 0 end) stat11,
--if awarded degree program is at least 2 years and less than 4 years and within the awarded grad date 150%
        (case when awardedAwardLevelNo = 2 and awardedDate <= gradDate150AW then 1 else 0 end) stat12,
--if awarded degree program is bachelor level and within the awarded grad date 150% 
        (case when awardedAwardLevelNo = 3 and awardedDate <= gradDate150AW then 1 else 0 end) stat18,
--if awarded degree program is bachelor and initial degree program is bachelor and within the awarded grad date of 100% (4 years)
        (case when gradDate100AW is not null and awardedDate <= gradDate100AW then 1 else 0 end) stat19,
--if awarded degree program is bachelor and initial degree program is bachelor and within the awarded grad date of 125% (5 years)
        (case when gradDate125AW is not null and awardedDate between gradDate100AW and gradDate125AW then 1 else 0 end) stat20,
--if awarded degree program within the awarded grad date 150%
        (case when awardedDate <= gradDate150AW then 1 else 0 end) stat29,
--if no award and transfered out (awardInd filter used in join to TransferMCR)
        (case when awardInd is null and transPersonId is not null then 1 else 0 end) stat30,
--if no award, not transfered out and an exclusion (awardInd filter used in join to CohortExclusionMCR)
        (case when awardInd is null and transPersonId is null and exclPersonId is not null then 1 else 0 end) stat45,
--if no award, not transfered out, not an exclusion and is still registered (awardInd filter used in join to RegStatusMCR) 
        (case when awardInd is null and transPersonId is null and exclPersonId is null and regPersonId is not null then 1 else 0 end) stat51
from ( 
select *,
--if awarded degree program is same length as initial degree program, grad date 150% is same, else re-calculate based on awarded degree program
			(case when awardedLengthInMonths = lengthInMonthsCOH then gradDate150COH
                    else least(to_date(add_months(termStart, (awardedLengthInMonths * 1.5)),'YYYY-MM-DD'), latestStatusDate) end) gradDate150AW,
--if initial degree program is bachelor level and awarded degree program is bachelor level, use awarded degree program to calculate grad date 125%
            (case when subCohort = 2 and awardedAwardLevelNo = 3 then least(to_date(add_months(termStart, (awardedLengthInMonths * 1.25)),'YYYY-MM-DD'), midStatusDate) end) gradDate125AW,
--if initial degree program is bachelor level and awarded degree program is bachelor level, use awarded degree program to calculate grad date 100%
             (case when subCohort = 2 and awardedAwardLevelNo = 3 then least(to_date(add_months(termStart, awardedLengthInMonths),'YYYY-MM-DD'), earliestStatusDate) end) gradDate100AW,
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
				award.isPellRec isPellRec,
				award.isSubLoanRec isSubLoanRec,
				award.awardInd awardInd,
				award.termOrder termOrder,
				award.termStart termStart,
				award.ipedsGender ipedsGender,
				award.ipedsEthnicity ipedsEthnicity,
				award.midStatusDate midStatusDate,
				award.earliestStatusDate earliestStatusDate,
				award.latestStatusDate latestStatusDate,
				award.snapshotDate snapshotDate,
				award.gradDate150COH gradDate150COH,
				award.awardedDate awardedDate,
				award.awardedDegreeProgram awardedDegreeProgram,
				award.awardedDegree awardedDegree,
				award.awardedDegreeLevel awardedDegreeLevel,
				award.lengthInMonthsCOH lengthInMonthsCOH,
				deg.awardLevel awardedAwardLevel,
				deg.awardLevelNo awardedAwardLevelNo,
				deg.lengthInMonths awardedLengthInMonths,
				exclusion.personId exclPersonId,
				transfer.personId transPersonId,
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
			left join TransferMCR transfer on award.personId = transfer.personId
			left join CohortExclusionMCR exclusion on award.personId = exclusion.personId
			left join RegStatusMCR reg on award.personId = reg.personId
		)        
	where degreeRn <= 1
 )                   
where awardRn <= 1
),

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/

estabSubCohortPartB1 as (
select *
from (
    VALUES
--Establishing cohort table (section 1)
        (1), -- Revised cohort of all full-time, first-time degree or certificate seeking undergraduates, cohort year 2014
        (2) -- Subcohort of full-time, first-time students seeking a bachelor's or equivalent degree, cohort year 2014
        --(3)  -- Subcohort of full-time, first-time students seeking other than a bachelor's degree, cohort year 2014. Do not include in import file, will be generated.
    ) as PartBSection1 (subcoh)
),

estabSubCohort as (
select *
from (
    VALUES
--Establishing cohort table (section 1)
        --(1), -- Revised cohort of all full-time, first-time degree or certificate seeking undergraduates, cohort year 2014
        (2), -- Subcohort of full-time, first-time students seeking a bachelor's or equivalent degree, cohort year 2014
        (3)  -- Subcohort of full-time, first-time students seeking other than a bachelor's degree, cohort year 2014. Do not include in import file, will be generated.
    ) as estSubCohort (subcoh)
),

FormatPartB as (
select *
from (
	VALUES
		(11), -- 11 - Completers of programs of less than 2 years within 150% of normal time --all
		(12), -- 12 - Completers of programs of at least 2 but less than 4 years within 150% of normal time --all
		(18), -- 18 - Completers of bachelor's or equivalent degree programs within 150% of normal time --all
		(19), -- 19 - Completers of bachelor's or equivalent degree programs in 4 years or less --subcohort 2
		(20), -- 20 - Completers of bachelor's or equivalent degree programs in 5 years --subcohort 2
		(30), -- 30 - Total transfer-out students (non-completers)
		(45), -- 45 - Total exclusions 
		(51)  -- 51 - Still enrolled 
	) as PartB (cohortStatus)
),

FormatPartC as (
select *
from (
	VALUES
		(10), -- 10 - Number of students in cohort
		(18), -- 18 - Completed bachelor's degree or equivalent within 150%
		(29), -- 29 - Total completers within 150%
		(45)  -- 45 - Total exclusions
	) as PartC (cohortStatus)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

select 'B' part,
        1 section, 
        cast(subCohortPB1 as string) line, --subcohort
        cast(coalesce((case when subCohortPB1 = 1 then field1
              else field1_2 end), 0) as string) field1,
        cast(coalesce((case when subCohortPB1 = 1 then field2
              else field2_2 end), 0) as string) field2,
        cast(coalesce((case when subCohortPB1 = 1 then field3
              else field3_2 end), 0) as string) field3,
        cast(coalesce((case when subCohortPB1 = 1 then field4
              else field4_2 end), 0) as string) field4,
        cast(coalesce((case when subCohortPB1 = 1 then field5
              else field5_2 end), 0) as string) field5,
        cast(coalesce((case when subCohortPB1 = 1 then field6
              else field6_2 end), 0) as string) field6,
        cast(coalesce((case when subCohortPB1 = 1 then field7
              else field7_2 end), 0) as string) field7,
        cast(coalesce((case when subCohortPB1 = 1 then field8
              else field8_2 end), 0) as string) field8,
        cast(coalesce((case when subCohortPB1 = 1 then field9
              else field9_2 end), 0) as string) field9,
        cast(coalesce((case when subCohortPB1 = 1 then field10
              else field10_2 end), 0) as string) field10,
        cast(coalesce((case when subCohortPB1 = 1 then field11
              else field11_2 end), 0) as string) field11,
        cast(coalesce((case when subCohortPB1 = 1 then field12
              else field12_2 end), 0) as string) field12,
        cast(coalesce((case when subCohortPB1 = 1 then field13
              else field13_2 end), 0) as string) field13,
        cast(coalesce((case when subCohortPB1 = 1 then field14
              else field14_2 end), 0) as string) field14,
        cast(coalesce((case when subCohortPB1 = 1 then field15
              else field15_2 end), 0) as string) field15,
        cast(coalesce((case when subCohortPB1 = 1 then field16
              else field16_2 end), 0) as string) field16,
        cast(coalesce((case when subCohortPB1 = 1 then field17
              else field17_2 end), 0) as string) field17,
        cast(coalesce((case when subCohortPB1 = 1 then field18
              else field18_2 end), 0) as string) field18
from ( 
    select pb1.subcoh subCohortPB1,
            sum(field1) field1, 
            sum(field1_2) field1_2,
            sum(field2) field2, 
            sum(field2_2) field2_2,
            sum(field3) field3, 
            sum(field3_2) field3_2,
            sum(field4) field4, 
            sum(field4_2) field4_2,
            sum(field5) field5,
            sum(field5_2) field5_2,
            sum(field6) field6, 
            sum(field6_2) field6_2,
            sum(field7) field7, 
            sum(field7_2) field7_2,
            sum(field8) field8,
            sum(field8_2) field8_2,
            sum(field9) field9,
            sum(field9_2) field9_2,
            sum(field10) field10, 
            sum(field10_2) field10_2,
            sum(field11) field11, 
            sum(field11_2) field11_2,
            sum(field12) field12, 
            sum(field12_2) field12_2,
            sum(field13) field13,
            sum(field13_2) field13_2,
            sum(field14) field14,
            sum(field14_2) field14_2,
            sum(field15) field15, 
            sum(field15_2) field15_2,
            sum(field16) field16, 
            sum(field16_2) field16_2,
            sum(field17) field17, 
            sum(field17_2) field17_2,
            sum(field18) field18,
            sum(field18_2) field18_2
    from estabSubCohortPartB1 pb1
        left join (
        select personId personId,
                (case when subCohort = 2 then 2 else 1 end) subcoh,         
                field1 field1,
                (case when subCohort = 2 then field1 else 0 end) field1_2,
                field2 field2,
                (case when subCohort = 2 then field2 else 0 end) field2_2,
                field3 field3,
                (case when subCohort = 2 then field3 else 0 end) field3_2,
                field4 field4,
                (case when subCohort = 2 then field4 else 0 end) field4_2,
                field5 field5,
                (case when subCohort = 2 then field5 else 0 end) field5_2,
                field6 field6,
                (case when subCohort = 2 then field6 else 0 end) field6_2,
                field7 field7,
                (case when subCohort = 2 then field7 else 0 end) field7_2,
                field8 field8, 
                (case when subCohort = 2 then field8 else 0 end) field8_2,
                field9 field9,
                (case when subCohort = 2 then field9 else 0 end) field9_2,
                field10 field10,
                (case when subCohort = 2 then field10 else 0 end) field10_2,
                field11 field11,
                (case when subCohort = 2 then field11 else 0 end) field11_2,
                field12 field12,
                (case when subCohort = 2 then field12 else 0 end) field12_2,
                field13 field13,
                (case when subCohort = 2 then field13 else 0 end) field13_2,
                field14 field14,
                (case when subCohort = 2 then field14 else 0 end) field14_2,
                field15 field15,
                (case when subCohort = 2 then field15 else 0 end) field15_2,
                field16 field16, 
                (case when subCohort = 2 then field16 else 0 end) field16_2,
                field17 field17, 
                (case when subCohort = 2 then field17 else 0 end) field17_2,
                field18 field18, 
                (case when subCohort = 2 then field18 else 0 end) field18_2
            from CohortStatus
        ) innerData on pb1.subcoh = innerData.subcoh
    group by pb1.subcoh
)

union

select 'B' part,
         cast(subCohortall as string) section, --subcohort
        cast(cohortStatusall as string) line, --cohort status
        cast(coalesce((case when cohortStatusall = 11 then field1_11
              when cohortStatusall = 12 then field1_12
              when cohortStatusall = 18 then field1_18
              when cohortStatusall = 19 then field1_19
              when cohortStatusall = 20 then field1_20
              when cohortStatusall = 30 then field1_30
              when cohortStatusall = 45 then field1_45
              when cohortStatusall = 51 then field1_51
            else 0 end), 0) as string) field1,
        cast(coalesce((case when cohortStatusall = 11 then field2_11
              when cohortStatusall = 12 then field2_12
              when cohortStatusall = 18 then field2_18
              when cohortStatusall = 19 then field2_19
              when cohortStatusall = 20 then field2_20
              when cohortStatusall = 30 then field2_30
              when cohortStatusall = 45 then field2_45
              when cohortStatusall = 51 then field2_51
            else 0 end), 0) as string) field2,
        cast(coalesce((case when cohortStatusall = 11 then field3_11
              when cohortStatusall = 12 then field3_12
              when cohortStatusall = 18 then field3_18
              when cohortStatusall = 19 then field3_19
              when cohortStatusall = 20 then field3_20
              when cohortStatusall = 30 then field3_30
              when cohortStatusall = 45 then field3_45
              when cohortStatusall = 51 then field3_51
            else 0 end), 0) as string) field3,
        cast(coalesce((case when cohortStatusall = 11 then field4_11
              when cohortStatusall = 12 then field4_12
              when cohortStatusall = 18 then field4_18
              when cohortStatusall = 19 then field4_19
              when cohortStatusall = 20 then field4_20
              when cohortStatusall = 30 then field4_30
              when cohortStatusall = 45 then field4_45
              when cohortStatusall = 51 then field4_51
            else 0 end), 0) as string) field4,
        cast(coalesce((case when cohortStatusall = 11 then field5_11
              when cohortStatusall = 12 then field5_12
              when cohortStatusall = 18 then field5_18
              when cohortStatusall = 19 then field5_19
              when cohortStatusall = 20 then field5_20
              when cohortStatusall = 30 then field5_30
              when cohortStatusall = 45 then field5_45
              when cohortStatusall = 51 then field5_51
            else 0 end), 0) as string) field5,
        cast(coalesce((case when cohortStatusall = 11 then field6_11
              when cohortStatusall = 12 then field6_12
              when cohortStatusall = 18 then field6_18
              when cohortStatusall = 19 then field6_19
              when cohortStatusall = 20 then field6_20
              when cohortStatusall = 30 then field6_30
              when cohortStatusall = 45 then field6_45
              when cohortStatusall = 51 then field6_51
            else 0 end), 0) as string) field6,
        cast(coalesce((case when cohortStatusall = 11 then field7_11
              when cohortStatusall = 12 then field7_12
              when cohortStatusall = 18 then field7_18
              when cohortStatusall = 19 then field7_19
              when cohortStatusall = 20 then field7_20
              when cohortStatusall = 30 then field7_30
              when cohortStatusall = 45 then field7_45
              when cohortStatusall = 51 then field7_51
            else 0 end), 0) as string) field7,
        cast(coalesce((case when cohortStatusall = 11 then field8_11
              when cohortStatusall = 12 then field8_12
              when cohortStatusall = 18 then field8_18
              when cohortStatusall = 19 then field8_19
              when cohortStatusall = 20 then field8_20
              when cohortStatusall = 30 then field8_30
              when cohortStatusall = 45 then field8_45
              when cohortStatusall = 51 then field8_51
            else 0 end), 0) as string) field8,
        cast(coalesce((case when cohortStatusall = 11 then field9_11
              when cohortStatusall = 12 then field9_12
              when cohortStatusall = 18 then field9_18
              when cohortStatusall = 19 then field9_19
              when cohortStatusall = 20 then field9_20
              when cohortStatusall = 30 then field9_30
              when cohortStatusall = 45 then field9_45
              when cohortStatusall = 51 then field9_51
            else 0 end), 0) as string) field9,
        cast(coalesce((case when cohortStatusall = 11 then field10_11
              when cohortStatusall = 12 then field10_12
              when cohortStatusall = 18 then field10_18
              when cohortStatusall = 19 then field10_19
              when cohortStatusall = 20 then field10_20
              when cohortStatusall = 30 then field10_30
              when cohortStatusall = 45 then field10_45
              when cohortStatusall = 51 then field10_51
            else 0 end), 0) as string) field10,
        cast(coalesce((case when cohortStatusall = 11 then field11_11
              when cohortStatusall = 12 then field11_12
              when cohortStatusall = 18 then field11_18
              when cohortStatusall = 19 then field11_19
              when cohortStatusall = 20 then field11_20
              when cohortStatusall = 30 then field11_30
              when cohortStatusall = 45 then field11_45
              when cohortStatusall = 51 then field11_51
            else 0 end), 0) as string) field11,
        cast(coalesce((case when cohortStatusall = 11 then field12_11
              when cohortStatusall = 12 then field12_12
              when cohortStatusall = 18 then field12_18
              when cohortStatusall = 19 then field12_19
              when cohortStatusall = 20 then field12_20
              when cohortStatusall = 30 then field12_30
              when cohortStatusall = 45 then field12_45
              when cohortStatusall = 51 then field12_51
            else 0 end), 0) as string) field12,
        cast(coalesce((case when cohortStatusall = 11 then field13_11
              when cohortStatusall = 12 then field13_12
              when cohortStatusall = 18 then field13_18
              when cohortStatusall = 19 then field13_19
              when cohortStatusall = 20 then field13_20
              when cohortStatusall = 30 then field13_30
              when cohortStatusall = 45 then field13_45
              when cohortStatusall = 51 then field13_51
            else 0 end), 0) as string) field13,
        cast(coalesce((case when cohortStatusall = 11 then field14_11
              when cohortStatusall = 12 then field14_12
              when cohortStatusall = 18 then field14_18
              when cohortStatusall = 19 then field14_19
              when cohortStatusall = 20 then field14_20
              when cohortStatusall = 30 then field14_30
              when cohortStatusall = 45 then field14_45
              when cohortStatusall = 51 then field14_51
            else 0 end), 0) as string) field14,
        cast(coalesce((case when cohortStatusall = 11 then field15_11
              when cohortStatusall = 12 then field15_12
              when cohortStatusall = 18 then field15_18
              when cohortStatusall = 19 then field15_19
              when cohortStatusall = 20 then field15_20
              when cohortStatusall = 30 then field15_30
              when cohortStatusall = 45 then field15_45
              when cohortStatusall = 51 then field15_51
            else 0 end), 0) as string) field15,
        cast(coalesce((case when cohortStatusall = 11 then field16_11
              when cohortStatusall = 12 then field16_12
              when cohortStatusall = 18 then field16_18
              when cohortStatusall = 19 then field16_19
              when cohortStatusall = 20 then field16_20
              when cohortStatusall = 30 then field16_30
              when cohortStatusall = 45 then field16_45
              when cohortStatusall = 51 then field16_51
            else 0 end), 0) as string) field16,
        cast(coalesce((case when cohortStatusall = 11 then field17_11
              when cohortStatusall = 12 then field17_12
              when cohortStatusall = 18 then field17_18
              when cohortStatusall = 19 then field17_19
              when cohortStatusall = 20 then field17_20
              when cohortStatusall = 30 then field17_30
              when cohortStatusall = 45 then field17_45
              when cohortStatusall = 51 then field17_51
            else 0 end), 0) as string) field17,
        cast(coalesce((case when cohortStatusall = 11 then field18_11
              when cohortStatusall = 12 then field18_12
              when cohortStatusall = 18 then field18_18
              when cohortStatusall = 19 then field18_19
              when cohortStatusall = 20 then field18_20
              when cohortStatusall = 30 then field18_30
              when cohortStatusall = 45 then field18_45
              when cohortStatusall = 51 then field18_51
            else 0 end), 0) as string) field18
from (
    select subCoh.subcoh subCohortall,
        cohSt.cohortStatus cohortStatusall,
        sum(cohortStatus11) cohortStatus11,
        sum(cohortStatus12) cohortStatus12,
        sum(cohortStatus18) cohortStatus18,
        sum(cohortStatus19) cohortStatus19,
        sum(cohortStatus20) cohortStatus20,
        sum(cohortStatus30) cohortStatus30,
        sum(cohortStatus45) cohortStatus45,
        sum(cohortStatus51) cohortStatus51,
        sum(field1_11) field1_11, 
        sum(field1_12) field1_12,
        sum(field1_18) field1_18,
        sum(field1_19) field1_19,
        sum(field1_20) field1_20,
        sum(field1_30) field1_30,
        sum(field1_45) field1_45,
        sum(field1_51) field1_51,
        sum(field2_11) field2_11, 
        sum(field2_12) field2_12,
        sum(field2_18) field2_18,
        sum(field2_19) field2_19,
        sum(field2_20) field2_20,
        sum(field2_30) field2_30,
        sum(field2_45) field2_45,
        sum(field2_51) field2_51,
        sum(field3_11) field3_11, 
        sum(field3_12) field3_12,
        sum(field3_18) field3_18,
        sum(field3_19) field3_19,
        sum(field3_20) field3_20,
        sum(field3_30) field3_30,
        sum(field3_45) field3_45,
        sum(field3_51) field3_51,
        sum(field4_11) field4_11, 
        sum(field4_12) field4_12,
        sum(field4_18) field4_18,
        sum(field4_19) field4_19,
        sum(field4_20) field4_20,
        sum(field4_30) field4_30,
        sum(field4_45) field4_45,
        sum(field4_51) field4_51,
        sum(field5_11) field5_11, 
        sum(field5_12) field5_12,
        sum(field5_18) field5_18,
        sum(field5_19) field5_19,
        sum(field5_20) field5_20,
        sum(field5_30) field5_30,
        sum(field5_45) field5_45,
        sum(field5_51) field5_51,
        sum(field6_11) field6_11, 
        sum(field6_12) field6_12,
        sum(field6_18) field6_18,
        sum(field6_19) field6_19,
        sum(field6_20) field6_20,
        sum(field6_30) field6_30,
        sum(field6_45) field6_45,
        sum(field6_51) field6_51,
        sum(field7_11) field7_11, 
        sum(field7_12) field7_12,
        sum(field7_18) field7_18,
        sum(field7_19) field7_19,
        sum(field7_20) field7_20,
        sum(field7_30) field7_30,
        sum(field7_45) field7_45,
        sum(field7_51) field7_51,
        sum(field8_11) field8_11, 
        sum(field8_12) field8_12,
        sum(field8_18) field8_18,
        sum(field8_19) field8_19,
        sum(field8_20) field8_20,
        sum(field8_30) field8_30,
        sum(field8_45) field8_45,
        sum(field8_51) field8_51,
        sum(field9_11) field9_11, 
        sum(field9_12) field9_12,
        sum(field9_18) field9_18,
        sum(field9_19) field9_19,
        sum(field9_20) field9_20,
        sum(field9_30) field9_30,
        sum(field9_45) field9_45,
        sum(field9_51) field9_51,
        sum(field10_11) field10_11, 
        sum(field10_12) field10_12,
        sum(field10_18) field10_18,
        sum(field10_19) field10_19,
        sum(field10_20) field10_20,
        sum(field10_30) field10_30,
        sum(field10_45) field10_45,
        sum(field10_51) field10_51,
        sum(field11_11) field11_11, 
        sum(field11_12) field11_12,
        sum(field11_18) field11_18,
        sum(field11_19) field11_19,
        sum(field11_20) field11_20,
        sum(field11_30) field11_30,
        sum(field11_45) field11_45,
        sum(field11_51) field11_51,
        sum(field12_11) field12_11, 
        sum(field12_12) field12_12,
        sum(field12_18) field12_18,
        sum(field12_19) field12_19,
        sum(field12_20) field12_20,
        sum(field12_30) field12_30,
        sum(field12_45) field12_45,
        sum(field12_51) field12_51,
        sum(field13_11) field13_11, 
        sum(field13_12) field13_12,
        sum(field13_18) field13_18,
        sum(field13_19) field13_19,
        sum(field13_20) field13_20,
        sum(field13_30) field13_30,
        sum(field13_45) field13_45,
        sum(field13_51) field13_51,
        sum(field14_11) field14_11, 
        sum(field14_12) field14_12,
        sum(field14_18) field14_18,
        sum(field14_19) field14_19,
        sum(field14_20) field14_20,
        sum(field14_30) field14_30,
        sum(field14_45) field14_45,
        sum(field14_51) field14_51,
        sum(field15_11) field15_11, 
        sum(field15_12) field15_12,
        sum(field15_18) field15_18,
        sum(field15_19) field15_19,
        sum(field15_20) field15_20,
        sum(field15_30) field15_30,
        sum(field15_45) field15_45,
        sum(field15_51) field15_51,
        sum(field16_11) field16_11, 
        sum(field16_12) field16_12,
        sum(field16_18) field16_18,
        sum(field16_19) field16_19,
        sum(field16_20) field16_20,
        sum(field16_30) field16_30,
        sum(field16_45) field16_45,
        sum(field16_51) field16_51,
        sum(field17_11) field17_11, 
        sum(field17_12) field17_12,
        sum(field17_18) field17_18,
        sum(field17_19) field17_19,
        sum(field17_20) field17_20,
        sum(field17_30) field17_30,
        sum(field17_45) field17_45,
        sum(field17_51) field17_51,
        sum(field18_11) field18_11, 
        sum(field18_12) field18_12,
        sum(field18_18) field18_18,
        sum(field18_19) field18_19,
        sum(field18_20) field18_20,
        sum(field18_30) field18_30,
        sum(field18_45) field18_45,
        sum(field18_51) field18_51
    from estabSubCohort subCoh
        cross join FormatPartB cohSt
        left join (
            select personId personId,
                    subCohort subCohort,
                    stat11 cohortStatus11,
                    stat12 cohortStatus12,
                    stat18 cohortStatus18,
                    stat19 cohortStatus19,
                    stat20 cohortStatus20, 
                    stat30 cohortStatus30,
                    stat45 cohortStatus45,
                    stat51 cohortStatus51,
                    (case when stat11 = 1 then field1 else 0 end) field1_11, 
                    (case when stat12 = 1 then field1 else 0 end) field1_12,
                    (case when stat18 = 1 then field1 else 0 end) field1_18,
                    (case when stat19 = 1 then field1 else 0 end) field1_19,
                    (case when stat20 = 1 then field1 else 0 end) field1_20,
                    (case when stat30 = 1 then field1 else 0 end) field1_30,
                    (case when stat45 = 1 then field1 else 0 end) field1_45,
                    (case when stat51 = 1 then field1 else 0 end) field1_51,
                    (case when stat11 = 1 then field2 else 0 end) field2_11, 
                    (case when stat12 = 1 then field2 else 0 end) field2_12,
                    (case when stat18 = 1 then field2 else 0 end) field2_18,
                    (case when stat19 = 1 then field2 else 0 end) field2_19,
                    (case when stat20 = 1 then field2 else 0 end) field2_20,
                    (case when stat30 = 1 then field2 else 0 end) field2_30,
                    (case when stat45 = 1 then field2 else 0 end) field2_45,
                    (case when stat51 = 1 then field2 else 0 end) field2_51,
                    (case when stat11 = 1 then field3 else 0 end) field3_11, 
                    (case when stat12 = 1 then field3 else 0 end) field3_12,
                    (case when stat18 = 1 then field3 else 0 end) field3_18,
                    (case when stat19 = 1 then field3 else 0 end) field3_19,
                    (case when stat20 = 1 then field3 else 0 end) field3_20,
                    (case when stat30 = 1 then field3 else 0 end) field3_30,
                    (case when stat45 = 1 then field3 else 0 end) field3_45,
                    (case when stat51 = 1 then field3 else 0 end) field3_51,
                    (case when stat11 = 1 then field4 else 0 end) field4_11, 
                    (case when stat12 = 1 then field4 else 0 end) field4_12,
                    (case when stat18 = 1 then field4 else 0 end) field4_18,
                    (case when stat19 = 1 then field4 else 0 end) field4_19,
                    (case when stat20 = 1 then field4 else 0 end) field4_20,
                    (case when stat30 = 1 then field4 else 0 end) field4_30,
                    (case when stat45 = 1 then field4 else 0 end) field4_45,
                    (case when stat51 = 1 then field4 else 0 end) field4_51,
                    (case when stat11 = 1 then field5 else 0 end) field5_11, 
                    (case when stat12 = 1 then field5 else 0 end) field5_12,
                    (case when stat18 = 1 then field5 else 0 end) field5_18,
                    (case when stat19 = 1 then field5 else 0 end) field5_19,
                    (case when stat20 = 1 then field5 else 0 end) field5_20,
                    (case when stat30 = 1 then field5 else 0 end) field5_30,
                    (case when stat45 = 1 then field5 else 0 end) field5_45,
                    (case when stat51 = 1 then field5 else 0 end) field5_51,
                    (case when stat11 = 1 then field6 else 0 end) field6_11, 
                    (case when stat12 = 1 then field6 else 0 end) field6_12,
                    (case when stat18 = 1 then field6 else 0 end) field6_18,
                    (case when stat19 = 1 then field6 else 0 end) field6_19,
                    (case when stat20 = 1 then field6 else 0 end) field6_20,
                    (case when stat30 = 1 then field6 else 0 end) field6_30,
                    (case when stat45 = 1 then field6 else 0 end) field6_45,
                    (case when stat51 = 1 then field6 else 0 end) field6_51,
                    (case when stat11 = 1 then field7 else 0 end) field7_11, 
                    (case when stat12 = 1 then field7 else 0 end) field7_12,
                    (case when stat18 = 1 then field7 else 0 end) field7_18,
                    (case when stat19 = 1 then field7 else 0 end) field7_19,
                    (case when stat20 = 1 then field7 else 0 end) field7_20,
                    (case when stat30 = 1 then field7 else 0 end) field7_30,
                    (case when stat45 = 1 then field7 else 0 end) field7_45,
                    (case when stat51 = 1 then field7 else 0 end) field7_51,
                    (case when stat11 = 1 then field8 else 0 end) field8_11, 
                    (case when stat12 = 1 then field8 else 0 end) field8_12,
                    (case when stat18 = 1 then field8 else 0 end) field8_18,
                    (case when stat19 = 1 then field8 else 0 end) field8_19,
                    (case when stat20 = 1 then field8 else 0 end) field8_20,
                    (case when stat30 = 1 then field8 else 0 end) field8_30,
                    (case when stat45 = 1 then field8 else 0 end) field8_45,
                    (case when stat51 = 1 then field8 else 0 end) field8_51,
                    (case when stat11 = 1 then field9 else 0 end) field9_11, 
                    (case when stat12 = 1 then field9 else 0 end) field9_12,
                    (case when stat18 = 1 then field9 else 0 end) field9_18,
                    (case when stat19 = 1 then field9 else 0 end) field9_19,
                    (case when stat20 = 1 then field9 else 0 end) field9_20,
                    (case when stat30 = 1 then field9 else 0 end) field9_30,
                    (case when stat45 = 1 then field9 else 0 end) field9_45,
                    (case when stat51 = 1 then field9 else 0 end) field9_51,
                    (case when stat11 = 1 then field10 else 0 end) field10_11, 
                    (case when stat12 = 1 then field10 else 0 end) field10_12,
                    (case when stat18 = 1 then field10 else 0 end) field10_18,
                    (case when stat19 = 1 then field10 else 0 end) field10_19,
                    (case when stat20 = 1 then field10 else 0 end) field10_20,
                    (case when stat30 = 1 then field10 else 0 end) field10_30,
                    (case when stat45 = 1 then field10 else 0 end) field10_45,
                    (case when stat51 = 1 then field10 else 0 end) field10_51,
                    (case when stat11 = 1 then field11 else 0 end) field11_11, 
                    (case when stat12 = 1 then field11 else 0 end) field11_12,
                    (case when stat18 = 1 then field11 else 0 end) field11_18,
                    (case when stat19 = 1 then field11 else 0 end) field11_19,
                    (case when stat20 = 1 then field11 else 0 end) field11_20,
                    (case when stat30 = 1 then field11 else 0 end) field11_30,
                    (case when stat45 = 1 then field11 else 0 end) field11_45,
                    (case when stat51 = 1 then field11 else 0 end) field11_51,
                    (case when stat11 = 1 then field12 else 0 end) field12_11, 
                    (case when stat12 = 1 then field12 else 0 end) field12_12,
                    (case when stat18 = 1 then field12 else 0 end) field12_18,
                    (case when stat19 = 1 then field12 else 0 end) field12_19,
                    (case when stat20 = 1 then field12 else 0 end) field12_20,
                    (case when stat30 = 1 then field12 else 0 end) field12_30,
                    (case when stat45 = 1 then field12 else 0 end) field12_45,
                    (case when stat51 = 1 then field12 else 0 end) field12_51,
                    (case when stat11 = 1 then field13 else 0 end) field13_11, 
                    (case when stat12 = 1 then field13 else 0 end) field13_12,
                    (case when stat18 = 1 then field13 else 0 end) field13_18,
                    (case when stat19 = 1 then field13 else 0 end) field13_19,
                    (case when stat20 = 1 then field13 else 0 end) field13_20,
                    (case when stat30 = 1 then field13 else 0 end) field13_30,
                    (case when stat45 = 1 then field13 else 0 end) field13_45,
                    (case when stat51 = 1 then field13 else 0 end) field13_51,
                    (case when stat11 = 1 then field14 else 0 end) field14_11, 
                    (case when stat12 = 1 then field14 else 0 end) field14_12,
                    (case when stat18 = 1 then field14 else 0 end) field14_18,
                    (case when stat19 = 1 then field14 else 0 end) field14_19,
                    (case when stat20 = 1 then field14 else 0 end) field14_20,
                    (case when stat30 = 1 then field14 else 0 end) field14_30,
                    (case when stat45 = 1 then field14 else 0 end) field14_45,
                    (case when stat51 = 1 then field14 else 0 end) field14_51,
                    (case when stat11 = 1 then field15 else 0 end) field15_11, 
                    (case when stat12 = 1 then field15 else 0 end) field15_12,
                    (case when stat18 = 1 then field15 else 0 end) field15_18,
                    (case when stat19 = 1 then field15 else 0 end) field15_19,
                    (case when stat20 = 1 then field15 else 0 end) field15_20,
                    (case when stat30 = 1 then field15 else 0 end) field15_30,
                    (case when stat45 = 1 then field15 else 0 end) field15_45,
                    (case when stat51 = 1 then field15 else 0 end) field15_51,
                    (case when stat11 = 1 then field16 else 0 end) field16_11, 
                    (case when stat12 = 1 then field16 else 0 end) field16_12,
                    (case when stat18 = 1 then field16 else 0 end) field16_18,
                    (case when stat19 = 1 then field16 else 0 end) field16_19,
                    (case when stat20 = 1 then field16 else 0 end) field16_20,
                    (case when stat30 = 1 then field16 else 0 end) field16_30,
                    (case when stat45 = 1 then field16 else 0 end) field16_45,
                    (case when stat51 = 1 then field16 else 0 end) field16_51,
                    (case when stat11 = 1 then field17 else 0 end) field17_11, 
                    (case when stat12 = 1 then field17 else 0 end) field17_12,
                    (case when stat18 = 1 then field17 else 0 end) field17_18,
                    (case when stat19 = 1 then field17 else 0 end) field17_19,
                    (case when stat20 = 1 then field17 else 0 end) field17_20,
                    (case when stat30 = 1 then field17 else 0 end) field17_30,
                    (case when stat45 = 1 then field17 else 0 end) field17_45,
                    (case when stat51 = 1 then field17 else 0 end) field17_51,
                    (case when stat11 = 1 then field18 else 0 end) field18_11, 
                    (case when stat12 = 1 then field18 else 0 end) field18_12,
                    (case when stat18 = 1 then field18 else 0 end) field18_18,
                    (case when stat19 = 1 then field18 else 0 end) field18_19,
                    (case when stat20 = 1 then field18 else 0 end) field18_20,
                    (case when stat30 = 1 then field18 else 0 end) field18_30,
                    (case when stat45 = 1 then field18 else 0 end) field18_45,
                    (case when stat51 = 1 then field18 else 0 end) field18_51 
            from CohortStatus
            ) innerData on subCoh.subcoh = innerData.subCohort
        group by subCoh.subcoh, cohSt.cohortStatus
)
where not (subCohortall = 3
    and cohortStatusall = 19)
and not (subCohortall = 3
    and cohortStatusall = 20)
    
union

select 'C' part,
        cast(subCohortall as string) section, --subcohort
        cast(cohortStatusall as string) line, --cohort status
        cast(coalesce((case when cohortStatusall = 10 then isPellRec 
              when cohortStatusall = 18 then isPellRec18
              when cohortStatusall = 29 then isPellRec29
            else isPellRec45 end), 0) as string) field1, --isPell,
        cast(coalesce((case when cohortStatusall = 10 then isSubLoanRec 
              when cohortStatusall = 18 then isSubLoanRec18
              when cohortStatusall = 29 then isSubLoanRec29
            else isSubLoanRec45 end), 0) as string) field2, --isSubLoan,
	null field3,
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
	null field18
from (
    select  subCoh.subcoh subCohortall,
            cohSt.cohortStatus cohortStatusall,
            sum(isPellRec) isPellRec,
                sum(isPellRec18) isPellRec18,
                sum(isPellRec29) isPellRec29,
                sum(isPellRec45) isPellRec45,
                sum(isSubLoanRec) isSubLoanRec,
                sum(isSubLoanRec18) isSubLoanRec18,
                sum(isSubLoanRec29) isSubLoanRec29,
                sum(isSubLoanRec45) isSubLoanRec45,
                sum(stat18) stat18,
                sum(stat29) stat29,
                sum(stat45) stat45 
    from estabSubCohort subCoh 
        cross join FormatPartC cohSt
        left join (
            select personId personId,
                    subCohort subCohort,
                    (case when stat18 > 0 then 1 end) cohortStatus18,
                    (case when stat29 > 0 then 1 end) cohortStatus29,
                    (case when stat45 > 0 then 1 end) cohortStatus45,
                    isPellRec isPellRec,
                    isSubLoanRec isSubLoanRec,
                    stat18 stat18,
                    stat29 stat29,
                    stat45 stat45,
                    (case when stat18 = 1 then isPellRec else 0 end) isPellRec18,
                    (case when stat29 = 1 then isPellRec else 0 end) isPellRec29,
                    (case when stat45 = 1 then isPellRec else 0 end) isPellRec45,
                    (case when stat18 = 1 then isSubLoanRec else 0 end) isSubLoanRec18,
                    (case when stat29 = 1 then isSubLoanRec else 0 end) isSubLoanRec29,
                    (case when stat45 = 1 then isSubLoanRec else 0 end) isSubLoanRec45
            from CohortStatus
        ) innerData on subCoh.subcoh = innerData.subCohort
    group by subCoh.subcoh,
            cohSt.cohortStatus
    )
