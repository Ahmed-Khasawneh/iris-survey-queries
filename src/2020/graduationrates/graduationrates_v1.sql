/********************

EVI PRODUCT:	DORIS 2020-21 IPEDS Survey  
FILE NAME: 		Graduation Rates v1 (GR1)
FILE DESC:      Graduation Rates Full Instructions - 4-Year Institutions
AUTHOR:         akhasawneh
CREATED:        20201117

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Student Counts
Course Counts
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)  	Author             	    Tag             	Comments
----------- 		--------------------	-------------   	------------------------------------------------- 
20201117	        akhasawneh				                    Initial version (runtime: m, s)

********************/ 

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.
/*
--Production Default (Begin)
select '2021' surveyYear, 
	'GR1' surveyId,
	'Fall Census' repPeriodTag1,
	'Fall Census' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2014-07-01' AS DATE) reportingDateStart,
	CAST('2020-06-30' AS DATE) reportingDateEnd,
	'201410' termCode, --Fall 2014
	'1' partOfTermCode, 
	CAST('2014-10-15' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
	CAST('2015-08-31' AS DATE) cohortDateEnd,
    'D' compGradDateOrTerm,
    'Fall' surveySection,
	'000000' ncSchoolCode
--***** end survey-specific mods

union

select '2021' surveyYear, 
	'GR1' surveyId,
	'Fall Census' repPeriodTag1,
	'Fall Census' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2014-07-01' AS DATE) reportingDateStart,
	CAST('2020-06-30' AS DATE) reportingDateEnd,
	'201430' termCode, --Fall 2014
	'1' partOfTermCode, 
	CAST('2014-10-15' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
	CAST('2015-08-31' AS DATE) cohortDateEnd,
    'D' compGradDateOrTerm,
    'Prior Summer' surveySection,
	'000000' ncSchoolCode
--***** end survey-specific mods
*/

--Test Default (Begin)
select '1415' surveyYear, 
	'GR1' surveyId,
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2008-07-01' AS DATE) reportingDateStart, --(July 1 - June 30)
	CAST('2014-08-31' AS DATE) reportingDateEnd,
	'200810' termCode, --Fall 2008
	'1' partOfTermCode, 
	CAST('2008-09-10' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    CAST('2009-05-30' as DATE) financialAidEndDate, --For GR report: only aid for first year of enrollment(July 1 - June 30)
    CAST('2012-08-31' as DATE) earliestStatusDate, --BS4YrDate
    CAST('2013-08-31' as DATE) midStatusDate, --BS5YrDate
    CAST('2014-08-31' as DATE) latestStatusDate, --BS6YrDate
--***** start survey-specific mods
	CAST('2009-08-31' AS DATE) cohortDateEnd,
    'D' compGradDateOrTerm,
    'Fall' surveySection,
	'000000' ncSchoolCode,
    '00' ncBranchCode
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
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    CAST('2009-05-30' as DATE) financialAidEndDate, --For GR report: only aid for first year of enrollment(July 1 - June 30)
    CAST('2012-08-31' as DATE) earliestStatusDate, --BS4YrDate
    CAST('2013-08-31' as DATE) midStatusDate, --BS5YrDate
    CAST('2014-08-31' as DATE) latestStatusDate, --BS6YrDate
--***** start survey-specific mods
	CAST('2009-08-31' AS DATE) cohortDateEnd,
    'D' compGradDateOrTerm,
    'Prior Summer' surveySection,
	'000000' ncSchoolCode,
    '00' ncBranchCode
--***** end survey-specific mods
--Test Default (End)
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
	RepDates.repPeriodTag3 repPeriodTag3
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
	    --where repperiodENT.termCode is not null
		--and repperiodENT.partOfTermCode is not null
	
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
		1 reportPeriodRn
	from DefaultValues defvalues
    where defvalues.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = defvalues.surveyYear
											and upper(repperiodENT.surveyId) = defvalues.surveyId 
											--and repperiodENT.termCode is not null
											--and repperiodENT.partOfTermCode is not null
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
    upper(ConfigLatest.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
    upper(ConfigLatest.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
    upper(ConfigLatest.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
	ConfigLatest.repPeriodTag2 repPeriodTag2,
	ConfigLatest.repPeriodTag3 repPeriodTag3,
	ConfigLatest.financialAidEndDate financialAidEndDate,
    ConfigLatest.earliestStatusDate earliestStatusDate,
    ConfigLatest.midStatusDate midStatusDate,
    ConfigLatest.latestStatusDate latestStatusDate,
--***** start survey-specific mods
    ConfigLatest.cohortDateEnd cohortDateEnd,
    ConfigLatest.compGradDateOrTerm compGradDateOrTerm,
	ConfigLatest.surveySection surveySection,
	ConfigLatest.ncSchoolCode ncSchoolCode,
    ConfigLatest.ncBranchCode ncBranchCode,
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
        coalesce(clientConfigENT.icOfferUndergradAwardLevel, defvalues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
		coalesce(clientConfigENT.icOfferGraduateAwardLevel, defvalues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
        coalesce(clientConfigENT.icOfferDoctorAwardLevel, defvalues.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,		
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    defvalues.repPeriodTag3 repPeriodTag3,
        defvalues.financialAidEndDate financialAidEndDate,
        defvalues.earliestStatusDate earliestStatusDate,
        defvalues.midStatusDate midStatusDate,
        defvalues.latestStatusDate latestStatusDate,
--***** start survey-specific mods
        defvalues.cohortDateEnd cohortDateEnd,
        coalesce(clientConfigENT.compGradDateOrTerm, defvalues.compGradDateOrTerm) compGradDateOrTerm,
		defvalues.surveySection surveySection,
		coalesce(clientConfigENT.ncSchoolCode, defvalues.ncSchoolCode) ncSchoolCode,
        coalesce(clientConfigENT.ncBranchCode, defvalues.ncBranchCode) ncBranchCode,
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
        defvalues.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		defvalues.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        defvalues.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    defvalues.repPeriodTag3 repPeriodTag3,
        defvalues.financialAidEndDate financialAidEndDate,
        defvalues.earliestStatusDate earliestStatusDate,
        defvalues.midStatusDate midStatusDate,
        defvalues.latestStatusDate latestStatusDate,	    
--***** start survey-specific mods
        defvalues.cohortDateEnd cohortDateEnd,
        defvalues.compGradDateOrTerm compGradDateOrTerm,
		defvalues.surveySection surveySection,
		defvalues.ncSchoolCode ncSchoolCode,
        defvalues.ncBranchCode ncBranchCode,
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
	    repPerTerms.genderForUnknown,
		repPerTerms.genderForNonBinary,
		repPerTerms.instructionalActivityType,
	    repPerTerms.equivCRHRFactor equivCRHRFactor,
        (case when repPerTerms.termClassification = 'Standard Length' then 1
             when repPerTerms.termClassification is null then (case when repPerTerms.termType in ('Fall', 'Spring') then 1 else 2 end)
             else 2
        end) fullTermOrder,
--***** start survey-specific mods
        repPerTerms.cohortDateEnd cohortDateEnd,
        repPerTerms.surveyYear surveyYear,
        repPerTerms.compGradDateOrTerm compGradDateOrTerm,
		repPerTerms.ncSchoolCode ncSchoolCode
--***** end survey-specific mods
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
                            and ((array_contains(acadterm.tags, 'Fall Census') and acadterm.termType = 'Fall' and repperiod.surveySection = 'FALL')
                                or (array_contains(acadterm.tags, 'Pre-Fall Summer Census') and acadterm.termType = 'Summer' and repperiod.surveySection = 'PRIOR SUMMER')) then 1
                      when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') then 2
                     else 3 end) asc,
                (case when acadterm.snapshotDate > acadterm.censusDate then acadterm.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when acadterm.snapshotDate < acadterm.censusDate then acadterm.snapshotDate else CAST('1900-09-09' as DATE) end) desc
            ) acadTermRnReg,
--***** start survey-specific mods
        clientconfig.cohortDateEnd cohortDateEnd,
        repperiod.surveyYear surveyYear,
        clientconfig.compGradDateOrTerm compGradDateOrTerm,
		clientconfig.ncSchoolCode ncSchoolCode
--***** end survey-specific mods
    from ReportingPeriodMCR repperiod 
        left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
            and repperiod.partOfTermCode = acadterm.partOfTermCode
        left join AcademicTermOrder termorder on termOrder.termCode = repperiod.termCode
        inner join ClientConfigMCR clientconfig on repperiod.surveyYear = clientconfig.surveyYear
    --where upper(repperiod.surveySection) in ('FALL', 'PRIOR SUMMER', 'BS FOUR YEAR', 'BS FIVE YEAR' 'BS SIX YEAR') -- Oddly, this filter causes 4 and 5 year sections to not print even though the strings match.
    ) repPerTerms
where repPerTerms.acadTermRnReg = 1 
),

/*
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
        repPerTerms.snapshotDate snapshotDate,
        repPerTerms.reportingDateStart reportingDateStart,
        --repPerTerms.reportingDateEnd reportingDateEnd, 
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
        end) fullTermOrder,
--***** start survey-specific mods
        repPerTerms.cohortDateEnd cohortDateEnd,
        repPerTerms.surveyYear surveyYear,
        repPerTerms.compGradDateOrTerm compGradDateOrTerm,
		repPerTerms.ncSchoolCode ncSchoolCode
--***** end survey-specific mods
from (
    select *
    from (
        select distinct 'CY' yearType,
                repperiod.surveySection surveySection,
                repperiod.termCode termCode,
                repperiod.partOfTermCode partOfTermCode,
                acadterm.financialAidYear financialAidYear,
                acadterm.snapshotDate snapshotDate,
                repperiod.reportingDateStart reportingDateStart,
                --repperiod.reportingDateEnd reportingDateEnd,
                acadterm.tags tags,
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
                                    and ((array_contains(acadterm.tags, 'Fall Census') and acadterm.termType = 'Fall' and repperiod.surveySection = 'FALL')
                                        or (array_contains(acadterm.tags, 'Pre-Fall Summer Census') and acadterm.termType = 'Summer' and repperiod.surveySection = 'PRIOR SUMMER')) then 1
                              when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                                    and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') then 2
                             else 3 end) asc,
                        (case when acadterm.snapshotDate > acadterm.censusDate then acadterm.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                        (case when acadterm.snapshotDate < acadterm.censusDate then acadterm.snapshotDate else CAST('1900-09-09' as DATE) end) desc
                    ) acadTermRnReg,
        --***** start survey-specific mods
                clientconfig.cohortDateEnd cohortDateEnd,
                repperiod.surveyYear surveyYear,
                clientconfig.compGradDateOrTerm compGradDateOrTerm,
                clientconfig.ncSchoolCode ncSchoolCode
        --***** end survey-specific mods
            from ReportingPeriodMCR repperiod 
                left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
                    and repperiod.partOfTermCode = acadterm.partOfTermCode
                left join AcademicTermOrder termorder on termOrder.termCode = repperiod.termCode
                inner join ClientConfigMCR clientconfig on repperiod.surveyYear = clientconfig.surveyYear
            )
        where acadTermRnReg = 1 
        
        union
        
        select *
        from (
            select distinct 'SY' yearType,
                    config.surveySection surveySection,
                    acadterm.termCode termCode,
                    acadterm.partOfTermCode partOfTermCode,
                    acadterm.financialAidYear financialAidYear,
                    acadterm.snapshotDate snapshotDate,
                    config.reportingDateStart reportingDateStart,
                    --repperiod.reportingDateEnd reportingDateEnd,
                    acadterm.tags tags,
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
                    config.genderForUnknown,
                    config.genderForNonBinary,
                    config.instructionalActivityType,
                    coalesce(acadterm.requiredFTCreditHoursUG/
                        coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
                    row_number() over (
                        partition by 
                            acadterm.termCode,
                            acadterm.partOfTermCode
                        order by
                            (case when array_contains(acadterm.tags, config.repPeriodTag1) then 1 else 2 end) asc,
                            acadterm.snapshotDate desc
                        ) acadTermRnReg,
            --***** start survey-specific mods
                    config.cohortDateEnd cohortDateEnd,
                    config.surveyYear surveyYear,
                    config.compGradDateOrTerm compGradDateOrTerm,
                    config.ncSchoolCode ncSchoolCode
            --***** end survey-specific mods
            from AcademicTermMCR acadterm 
            left join AcademicTermOrder termorder on termOrder.termCode = acadterm.termCode
            cross join (select first(latestStatusDate) latestStatusDate,
                                first(repPeriodTag1) repPeriodTag1,
                                first(genderForUnknown) genderForUnknown,
                                first(genderForNonBinary) genderForNonBinary,
                                first(instructionalActivityType) instructionalActivityType,
                                first(cohortDateEnd) cohortDateEnd,
                                first(surveyYear) surveyYear,
                                first(compGradDateOrTerm) compGradDateOrTerm,
                                first(reportingDateStart) reportingDateStart,
                                first(surveySection) surveySection,
                                first(ncSchoolCode) ncSchoolCode
                        from ClientConfigMCR) config 
            where acadterm.startDate between date_sub(config.latestStatusDate, 90) and date_add(config.latestStatusDate, 60)
            )
        where acadTermRnReg = 1
    ) repPerTerms
),
*/

AcademicTermReportingRefactor as (
--Returns all records from AcademicTermReporting, converts Summer terms to Pre-Fall or Post-Spring and creates reportingDateStart/End

select rep.*,
        (select first(financialAidYear) from AcademicTermReporting where termType = 'Fall') financialAidYearFall,
        (select first(termCode) from AcademicTermReporting where termType = 'Fall') termCodeFall,
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

DegreeMCR as (

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

ProgramAwardLevel as (
-- Returns all degreeProgram values at the end of the reporting period

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
                                    first(ncSchoolCode) ncSchoolCode
                            from ClientConfigMCR) config
        where to_date(transferENT.startDate,'YYYY-MM-DD') >= config.reportingDateStart
                and (transferENT.collegeCodeBranch != concat(config.ncSchoolCode, '-', config.ncBranchCode)
                    and transferENT.collegeCodeBranch != concat(config.ncSchoolCode, config.ncBranchCode))
                and ((to_date(transferENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                           and to_date(transferENT.recordActivityDate,'YYYY-MM-DD') <= config.latestStatusDate)
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
                            from ClientConfigMCR) config
        where exclusionENT.exclusionReason in ('Died', 'Medical Leave', 'Military Leave', 'Foreign Aid Service', 'Religious Leave')
            and exclusionENT.isIPEDSReportable = 1
            and exclusionENT.termCodeEffective >= (select first(termCodeFall)
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
                            from ClientConfigMCR) config
            inner join AcademicTermMCR repperiod on repperiod.startDate between date_sub(config.latestStatusDate, 90) and date_add(config.latestStatusDate, 60)
                and repperiod.termCode = regENT.termCode
                and repperiod.partOfTermCode = regENT.partOfTermCode
        where regENT.registrationStatus is not null
                and ((to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
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
        null reportingDateEnd, 
        regData.reportingDateStart,
        regData.cohortDateEnd,
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
        regData.compGradDateOrTerm,
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
            ) regCampRn,
--***** start survey-specific mods
        regData.surveyYear surveyYear,
        regData.ncSchoolCode ncSchoolCode
--***** end survey-specific mods
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
            --repperiod.reportingDateEnd, 
            repperiod.reportingDateStart,
            repperiod.cohortDateEnd,
            repperiod.requiredFTCreditHoursGR,
            repperiod.requiredFTCreditHoursUG,
            repperiod.requiredFTClockHoursUG,
            repperiod.genderForUnknown,
		    repperiod.genderForNonBinary,
		    repperiod.instructionalActivityType,
            repperiod.equivCRHRFactor,
            repperiod.compGradDateOrTerm,
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
            ) regRn,
--***** start survey-specific mods
            repperiod.surveyYear surveyYear,
            repperiod.ncSchoolCode ncSchoolCode
--***** end survey-specific mods
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
        stuData.reportingDateEnd, 
        stuData.reportingDateStart,
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
            reg.reportingDateEnd, 
            reg.reportingDateStart,
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
                    stu.reportingDateEnd, 
                    stu.reportingDateStart,
                    stu.maxCensus,
                    stu.fullTermOrder,
                    stu.startDate,
                    stu.personId,
                    stu.isNonDegreeSeeking,
                    stu.campus,
                    stu.isNonDegreeSeeking,
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
                        ) regCampRn--,
            --***** start survey-specific mods
                    --stu.surveyYear surveyYear,
                   -- stu.compGradDateOrTerm compGradDateOrTerm,
                    --stu.ncSchoolCode ncSchoolCode
            --***** end survey-specific mods
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
        reg.reportingDateEnd, 
        reg.reportingDateStart,
        reg.cohortDateEnd,
        reg.termType,
        reg.termOrder,
        reg.requiredFTCreditHoursUG,
	    reg.requiredFTClockHoursUG,
	    reg.instructionalActivityType,
	    stu.personId personId,
        stu.studentLevel,
	    stu.studentType,
	    stu.isNonDegreeSeeking,
	    --stu.residency,
	    reg.genderForUnknown,
		reg.genderForNonBinary,
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
            ) courseRn,
--***** start survey-specific mods
        reg.compGradDateOrTerm,
        reg.ncSchoolCode
--***** end survey-specific mods
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
        coursesect.reportingDateEnd reportingDateEnd, 
        coursesect.reportingDateStart reportingDateStart,
        coursesect.cohortDateEnd cohortDateEnd,
		coursesect.termType termType,
		coursesect.termOrder termOrder, 
		coursesect.requiredFTCreditHoursUG,
	    coursesect.requiredFTClockHoursUG,
	    coursesect.instructionalActivityType,
        coursesect.personId personId,
	    coursesect.studentLevel,
	    coursesect.studentType,
	    coursesect.isNonDegreeSeeking,
	    coursesect.genderForUnknown,
		coursesect.genderForNonBinary,
	    --coursesect.residency,
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
		) courseSectSchedRn,
--***** start survey-specific mods
        coursesect.compGradDateOrTerm,
        coursesect.ncSchoolCode
--***** end survey-specific mods
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
	    coursesectsched.reportingDateEnd reportingDateEnd, 
        coursesectsched.reportingDateStart reportingDateStart,
        coursesectsched.cohortDateEnd cohortDateEnd,
	    coursesectsched.termType termType,
	    coursesectsched.requiredFTCreditHoursUG,
	    coursesectsched.requiredFTClockHoursUG,
	    coursesectsched.instructionalActivityType,
        coursesectsched.personId personId,
	    coursesectsched.studentType,
	    coursesectsched.studentLevel,
	    coursesectsched.isNonDegreeSeeking,
	    coursesectsched.genderForUnknown,
		coursesectsched.genderForNonBinary,
	    --coursesectsched.residency,
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
		) courseRn,
--***** start survey-specific mods
        coursesectsched.compGradDateOrTerm,
        coursesectsched.ncSchoolCode
--***** end survey-specific mods
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

select *
from (
    select yearType,
            snapshotDate,
            censusDate,
            reportingDateEnd, 
            reportingDateStart,
            cohortDateEnd,
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
            genderForUnknown,
		    genderForNonBinary,
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
             end) ipedsInclude,
--***** start survey-specific mods
            compGradDateOrTerm,
            ncSchoolCode
--***** end survey-specific mods
    from (
         select course.yearType,
                course.snapshotDate,
                course.censusDate,
                course.reportingDateEnd, 
                course.reportingDateStart,
                course.cohortDateEnd,
                course.financialAidYear,
                course.instructionalActivityType,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId,
                course.termCode,
                course.termStart,
                --course.fullTermOrder,
                course.termOrder,
                course.studentLevel,
                course.studentType,
                course.isNonDegreeSeeking,
                --course.residency,
                course.genderForUnknown,
		        course.genderForNonBinary,
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
                    else 0 end)) totalCreditHrsCalc,
--***** start survey-specific mods
                course.compGradDateOrTerm,
                course.ncSchoolCode
--***** end survey-specific mods
        from CourseMCR course
        group by course.yearType,
                course.snapshotDate,
                course.censusDate,
                course.reportingDateEnd, 
                course.reportingDateStart,
                course.cohortDateEnd,
                course.financialAidYear,
                course.instructionalActivityType,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId,
                course.termCode,
                course.termStart,
                --course.fullTermOrder,
                course.termOrder,
                course.studentLevel,
                course.studentType,
                course.isNonDegreeSeeking,
                --course.residency,
                course.genderForUnknown,
		        course.genderForNonBinary,
                course.compGradDateOrTerm,
                course.ncSchoolCode
        )
    )
where ipedsInclude = 1
   and timeStatus = 'FT'
),

PersonMCR as ( --259 students
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

select pers.personId personId,
        pers.yearType yearType,
        --pers.surveySection surveySection,
        pers.snapshotDate snapshotDate,
        pers.financialAidYear financialAidYear,
        pers.termCode termCode,
        pers.censusDate censusDate,
        pers.reportingDateEnd reportingDateEnd, 
        pers.reportingDateStart reportingDateStart,
        pers.cohortDateEnd cohortDateEnd,
        pers.termStart,
        --pers.maxCensus maxCensus,
        pers.termOrder termOrder,
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
        end) ipedsEthnicity,
--***** start survey-specific mods
        pers.compGradDateOrTerm,
        pers.ncSchoolCode
--***** end survey-specific mods
from (
    select distinct 
            stu.yearType yearType,
            --stu.surveySection surveySection,
            to_date(stu.snapshotDate,'YYYY-MM-DD') snapshotDate,
            stu.termCode termCode,
            stu.censusDate censusDate,
            stu.reportingDateEnd reportingDateEnd, 
            stu.reportingDateStart reportingDateStart,
            stu.cohortDateEnd cohortDateEnd,
            --stu.maxCensus maxCensus,
            stu.financialAidYear,
            stu.termOrder termOrder,
            stu.personId personId,
	        stu.genderForUnknown,
		    stu.genderForNonBinary,
            stu.termStart,
            --to_date(personENT.birthDate,'YYYY-MM-DD') birthDate,
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
                    --stu.surveySection,
                    stu.personId,
                    personENT.personId
                order by
                    (case when to_date(personENT.snapshotDate,'YYYY-MM-DD') = stu.snapshotDate then 1 else 2 end) asc,
			        (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') > stu.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') < stu.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    personENT.recordActivityDate desc
            ) personRn,
--***** start survey-specific mods
            stu.compGradDateOrTerm,
            stu.ncSchoolCode
--***** end survey-specific mods
    from CourseTypeCountsSTU stu 
        left join Person personENT on stu.personId = personENT.personId
            and personENT.isIpedsReportable = 1
            and ((to_date(personENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
               and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= stu.cohortDateEnd) 
                or to_date(personENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
    ) pers
where pers.personRn = 1
),

FinancialAidMCR as (
--Included to determine Pell and subsidized loan recipients within the first year of entering institution. 
--"...a recipient of a Pell Grant or Subsidized Direct Loan is a student who receives and uses that award 
--(i.e., award is partially or fully disbursed) within their first year at the institution (July 1 - June 30)..."

select pers2.yearType,
        pers2.snapshotDate,
        pers2.censusDate,
        pers2.reportingDateEnd, 
        pers2.reportingDateStart,
        pers2.cohortDateEnd,
        pers2.personId,
        pers2.termCode, --student firstFullTerm
        pers2.termOrder,
        pers2.termStart,
        pers2.ipedsGender,
        pers2.ipedsEthnicity,
--***** start survey-specific mods
        pers2.compGradDateOrTerm,
        pers2.ncSchoolCode,
--***** end survey-specific mods
        (case when finaid.isPellRec > 0 then 1 else 0 end) isPellRec,
        (case when finaid.isPellRec > 0 then 0
              when finaid.isSubLoanRec > 0 then 1 
              else 0 
        end) isSubLoanRec
from PersonMCR pers2 --CourseTypeCountsSTU course2
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
--Returns most up to date student academic track information as of their award date and term. 
--"If a student has switched programs, you will calculate 150% of normal time to completion based on his or her most recent program for reporting outcomes. 
-- However, the student's "clock" does not reset once they switch programs."
--! 186 count at this point. 

select * 
from (
	select finaid2.personId personId,
			finaid2.yearType yearType,
            finaid2.snapshotDate snapshotDate,
			finaid2.termCode termCode,
			finaid2.censusDate censusDate,
			finaid2.reportingDateEnd reportingDateEnd, 
            finaid2.reportingDateStart reportingDateStart,
            finaid2.cohortDateEnd cohortDateEnd,
			finaid2.termOrder termOrder,
            finaid2.termStart,
			finaid2.ipedsGender ipedsGender,
			finaid2.ipedsEthnicity ipedsEthnicity,
			finaid2.isPellRec isPellRec,
			finaid2.isSubLoanRec isSubLoanRec,
			finaid2.compGradDateOrTerm compGradDateOrTerm,
			finaid2.ncSchoolCode ncSchoolCode,
			--acadtrack.degree degreeCOH,
            coalesce(acadtrack.degreeCOH, 'BA') degreeCOH,
            --acadtrack.collegeCOH collegeCOH,
            coalesce(acadtrack.collegeCOH, 'AS') collegeCOH,
            coalesce(acadtrack.campusCOH, 'BA') campusCOH,
            acadtrack.departmentCOH departmentCOH,
            --acadtrackENT.termCodeEffective termCodeEffective,
            coalesce(acadtrack.degreeProgramCOH, 'BA-ENGL') degreeProgramCOH,
            --acadtrack.academicTrackLevelCOH academicTrackLevelCOH,
            coalesce(acadtrack.academicTrackLevelCOH, 'Undergrad') academicTrackLevelCOH,
            --acadtrack.fieldOfStudyCOH fieldOfStudyCOH,
            coalesce(acadtrack.fieldOfStudyCOH, 'ENGL') fieldOfStudyCOH,
            --acadtrack.fieldOfStudyPriorityCOH fieldOfStudyPriorityCOH,
            coalesce(acadtrack.fieldOfStudyPriorityCOH, 1) fieldOfStudyPriorityCOH,
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
                                acadTrackENT.personId, --coalesce(acadTrackENT.personId, finaid.personId),
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

select *,
	least(to_date(add_months(termStart, (lengthInMonths * 1.5)),'YYYY-MM-DD'), latestStatusDate) gradDate150
from (
    select acadtrack.personId personId,
			acadtrack.yearType yearType,
            acadtrack.snapshotDate snapshotDate,
			acadtrack.termCode termCode,
			acadtrack.censusDate censusDate,
			acadtrack.reportingDateEnd reportingDateEnd, 
            acadtrack.reportingDateStart reportingDateStart,
            acadtrack.cohortDateEnd cohortDateEnd,
			acadtrack.termOrder termOrder,
			acadtrack.ipedsGender ipedsGender,
			acadtrack.ipedsEthnicity ipedsEthnicity,
			acadtrack.isPellRec isPellRec,
			acadtrack.isSubLoanRec isSubLoanRec,
			acadtrack.compGradDateOrTerm compGradDateOrTerm,
			acadtrack.ncSchoolCode ncSchoolCode,
			acadtrack.degreeCOH degreeCOH,
            acadtrack.collegeCOH collegeCOH,
            acadtrack.campusCOH campusCOH,
            acadtrack.departmentCOH departmentCOH,
            acadtrack.degreeProgramCOH degreeProgramCOH,
            acadtrack.academicTrackLevelCOH academicTrackLevelCOH,
            acadtrack.fieldOfStudyCOH fieldOfStudyCOH,
            deg.awardLevel awardLevel,
			deg.awardLevelNo awardLevelNo,
--Section table (defines cohort)
--1 - Establishing cohorts, cohort year 2013
--2 - Subcohort of full-time, first-time students seeking a bachelor's or equivalent degree, cohort year 2013
--3 - Subcohort of full-time, first-time students seeking other than a bachelor's degree, cohort year 2013
			(case when deg.awardLevelNo = 3 then 2 else 3 end) subCohort,
			acadtrack.termStart termStart,
			deg.lengthInMonths lengthInMonths,
			defval.earliestStatusDate earliestStatusDate,
			defval.midStatusDate midStatusDate,
			defval.latestStatusDate latestStatusDate,
            defval.repPeriodTag1 repPeriodTag1,
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
            cross join (select first(earliestStatusDate) earliestStatusDate,
                                first(midStatusDate) midStatusDate,
                                first(latestStatusDate) latestStatusDate,
                                first(repPeriodTag1) repPeriodTag1
                        from DefaultValues) defval
        )
where degRn = 1
or degRn is null
),

AwardMCR as (
--Pulls all distinct student awards obtained as-of four year status date '2010-08-31'

select cohort2.personId personId,
			cohort2.subCohort subCohort,
			cohort2.isPellRec isPellRec,
			cohort2.isSubLoanRec isSubLoanRec,
            (case when awardData.awardedDate is not null then 1 else 0 end) awardInd,
            cohort2.yearType yearType,
            cohort2.snapshotDate cohortSnapshotDate,
			cohort2.termCode termCode,
			cohort2.censusDate censusDate,
			cohort2.reportingDateEnd reportingDateEnd, 
            cohort2.reportingDateStart reportingDateStart,
            cohort2.cohortDateEnd cohortDateEnd,
			cohort2.termOrder termOrder,
			cohort2.ipedsGender ipedsGender,
			cohort2.ipedsEthnicity ipedsEthnicity,
			cohort2.compGradDateOrTerm compGradDateOrTerm,
			cohort2.ncSchoolCode ncSchoolCode,
			cohort2.degreeCOH degreeCOH,
            cohort2.collegeCOH collegeCOH,
            cohort2.campusCOH campusCOH,
            cohort2.departmentCOH departmentCOH,
            cohort2.degreeProgramCOH degreeProgramCOH,
            cohort2.academicTrackLevelCOH academicTrackLevelCOH,
            cohort2.fieldOfStudyCOH fieldOfStudyCOH,
            cohort2.awardLevel awardLevelCOH,
			cohort2.awardLevelNo awardLevelNoCOH,
			cohort2.termStart termStart,
			cohort2.lengthInMonths lengthInMonthsCOH,
			cohort2.earliestStatusDate earliestStatusDate,
			cohort2.midStatusDate midStatusDate,
			cohort2.latestStatusDate latestStatusDate,
            cohort2.repPeriodTag1 repPeriodTag1,
            cohort2.gradDate150 gradDate150COH,
            awardData.snapshotDate snapshotDate,
            awardData.awardedDate awardedDate,
            awardData.degreeProgram awardedDegreeProgram,
            awardData.degree awardedDegree,
            awardData.degreeLevel awardedDegreeLevel
    from Cohort cohort2
        left join
            (select personId,
                    snapshotDate,
                    awardedDate,
                    degreeProgram,
                    degree,
                    degreeLevel
            from ( 
                select distinct awardENT.personId personId,
                    row_number() over (
                        partition by
                            awardENT.personId,
                            awardENT.awardedDate,
                            awardENT.degreeProgram,
                            awardENT.degreeLevel,
                            awardENT.degree
                        order by
                            (case when array_contains(awardENT.tags, cohort.repPeriodTag1) then 1 else 2 end) asc,
                            awardENT.snapshotDate desc,
                            awardENT.recordActivityDate desc
                    ) awardLatestRn,
                    to_date(awardENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
                    to_date(awardENT.awardedDate, 'YYYY-MM-DD') awardedDate,
                    to_date(awardENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
                    upper(awardENT.degreeProgram) degreeProgram,
                    upper(awardENT.degree) degree,
                    awardENT.degreeLevel degreeLevel,
                    coalesce(campus.isInternational, false) isInternational
                from Cohort cohort
                    inner join Award awardENT on cohort.personId = awardENT.personId
                        and awardENT.isIpedsReportable = 1
                        and awardENT.awardStatus = 'Awarded'
                        and awardENT.degreeLevel is not null
                        and awardENT.awardedDate is not null
                        and awardENT.degreeLevel != 'Continuing Ed'
                        and to_date(awardENT.awardedDate,'YYYY-MM-DD') between cohort.termStart and cohort.latestStatusDate
                        and ((to_date(awardENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                                and to_date(awardENT.recordActivityDate,'YYYY-MM-DD') <= cohort.latestStatusDate)
                            or to_date(awardENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
                    inner join AcademicTermOrder termorder on termorder.termCode = awardENT.awardedTermCode
                    left join CampusMCR campus on upper(awardENT.campus) = campus.campus
                    )
               where isInternational = false
                    and awardLatestRn = 1
		) awardData on cohort2.personId = awardData.personId
) ,
    
CohortStatus as (
-- Pulls degree information as of the reporting period
--Priority of reporting status: completers, transfer, exclusion, still enrolled

select personId personId,
        subCohort subCohort, --2 bach-seeking, 3 other-seeking
		isPellRec isPellRec,
		isSubLoanRec isSubLoanRec,
        ipedsGender ipedsGender,
		ipedsEthnicity ipedsEthnicity,
        latestStatusDate latestStatusDate,
        snapshotDate snapshotDate,
        termOrder termOrder,
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
        (case when awardedDate <= gradDate150AW and awardedAwardLevelNo = 1 then 11
              when awardedDate <= gradDate150AW and awardedAwardLevelNo = 2 then 12
              when gradDate100AW is not null and awardedDate <= gradDate100AW then 19
              when gradDate125AW is not null and awardedDate between gradDate100AW and gradDate125AW then 20
              when awardedDate <= gradDate150AW and awardedAwardLevelNo = 3 then 18
              when transPersonId is not null then 30
              when exclPersonId is not null then 45  
              when regPersonId is not null then 51
              else null end) cohortStatus,
        (case when awardedDate <= gradDate150AW and awardedAwardLevelNo = 3 then 1 else 0 end) stat18, --Completed bachelor's degree or equivalent within 150%
        (case when awardedDate <= gradDate150AW then 1 else 0 end) stat29, --Total completers within 150%
        (case when transPersonId is null and exclPersonId is not null then 1 else 0 end) stat45 --Total exclusions
from (
    select award2.personId personId,
            award2.subCohort subCohort,
            award2.isPellRec isPellRec,
            award2.isSubLoanRec isSubLoanRec,
            award2.awardInd awardInd,
            award2.termOrder termOrder,
            award2.ipedsGender ipedsGender,
            award2.ipedsEthnicity ipedsEthnicity,
            award2.latestStatusDate latestStatusDate,
            award2.snapshotDate snapshotDate,
            award2.gradDate150COH gradDate150COH,
            award2.awardedDate awardedDate,
            award2.awardedDegreeProgram awardedDegreeProgram,
            award2.awardedDegree awardedDegree,
            award2.awardedDegreeLevel awardedDegreeLevel,
            AwardDegData.awardedAwardLevel awardedAwardLevel,
            AwardDegData.awardedAwardLevelNo awardedAwardLevelNo,
            AwardDegData.awardedLengthInMonths awardedLengthInMonths,
            (case when AwardDegData.awardedLengthInMonths = award2.lengthInMonthsCOH then gradDate150COH
                    else least(to_date(add_months(award2.termStart, (AwardDegData.awardedLengthInMonths * 1.5)),'YYYY-MM-DD'), award2.latestStatusDate) end) gradDate150AW,
            (case when award2.subCohort = 2 and AwardDegData.awardedAwardLevelNo = 3 then
                        least(to_date(add_months(award2.termStart, (AwardDegData.awardedLengthInMonths * 1.25)),'YYYY-MM-DD'), award2.midStatusDate) end) gradDate125AW,
             (case when award2.subCohort = 2 and AwardDegData.awardedAwardLevelNo = 3 then
                        least(to_date(add_months(award2.termStart, AwardDegData.awardedLengthInMonths),'YYYY-MM-DD'), award2.earliestStatusDate) end) gradDate100AW,
            exclusion.personId exclPersonId,
            transfer.personId transPersonId,
            reg.personId regPersonId
    from AwardMCR award2
        left join
            (select *,
                    row_number() over (
                        partition by
                            personId
                        order by
                            awardedAwardLevelNo desc,
                            awardedLengthInMonths asc
                    ) awardRn
              from
                    (select award.personId personId,
                            award.awardedDate awardedDate,
                            award.awardedDegreeProgram awardedDegreeProgram,
                            award.awardedDegree awardedDegree,
                            award.awardedDegreeLevel awardedDegreeLevel,
                            deg.awardLevel awardedAwardLevel,
                            deg.awardLevelNo awardedAwardLevelNo,
                            deg.lengthInMonths awardedLengthInMonths,
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
                        inner join ProgramAwardLevel deg on award.awardedDegreeProgram = deg.degreeProgram
                            and award.awardedDegree = deg.degree
                            and award.awardedDegreeLevel = deg.degreeLevel
                    where award.awardInd = 1
                    )
            where degreeRn = 1
            ) AwardDegData on award2.personId = AwardDegData.personId
                and award2.awardedDate = AwardDegData.awardedDate
                and award2.awardedDegreeProgram = AwardDegData.awardedDegreeProgram
                and award2.awardedDegree = AwardDegData.awardedDegree
                and award2.awardedDegreeLevel = AwardDegData.awardedDegreeLevel
                and AwardDegData.awardRn = 1
        left join CohortExclusionMCR exclusion on award2.personId = exclusion.personId
        left join TransferMCR transfer on award2.personId = transfer.personId
        left join RegStatusMCR reg on award2.personId = reg.personId
    )
) --,

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/

FormatPartBSection2And3 as (
select *
from (
	VALUES
		--(10), -- 10 - Number of students in cohort
		(11), -- 11 - Completers of programs of less than 2 years within 150% of normal time --all
		(12), -- 12 - Completers of programs of at least 2 but less than 4 years within 150% of normal time --all
		(18), -- 18 - Completers of bachelor's or equivalent degree programs within 150% of normal time --all
		(19), -- 19 - Completers of bachelor's or equivalent degree programs in 4 years or less --subcohort 2
		(20), -- 20 - Completers of bachelor's or equivalent degree programs in 5 years --subcohort 2
		--(29), -- 29 - Total completers within 150%
		(30), -- 30 - Total transfer-out students (non-completers)
		(45), -- 45 - Total exclusions 
		(51)  -- 51 - Still enrolled 
	) as section2and3(cohort)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

--Section table (defines cohort)
--1 - Establishing cohorts, cohort year 2014
--2 - Subcohort of full-time, first-time students seeking a bachelor's or equivalent degree, cohort year 2014
--3 - Subcohort of full-time, first-time students seeking other than a bachelor's degree, cohort year 2014

--Establishing cohort table (section 1)
--1 - Revised cohort of all full-time, first-time degree or certificate seeking undergraduates, cohort year 2014
--2 - Subcohort of full-time, first-time students seeking a bachelor's or equivalent degree, cohort year 2014
--3 - Subcohort of full-time, first-time students seeking other than a bachelor's degree, cohort year 2014. Do not include in import file, will be generated.

--GR1 & 2
--Part B: Section I, Establishing Cohorts

SELECT 'B' part,
	'1' section, --Sub survey section - identifies cohort: 1 - Establishing cohorts, cohort year 2014
	1 line, --Status of students in cohort: 1 - Revised cohort of all full-time, first-time degree or certificate seeking undergraduates, cohort year 2014
	cast(sum(field1) as string) field1, --Nonresident Alien
	cast(sum(field2) as string) field2,
	cast(sum(field3) as string) field3, -- Hispanic/Latino
	cast(sum(field4) as string) field4,
	cast(sum(field5) as string) field5, -- American Indian or Alaska Native
	cast(sum(field6) as string) field6,
	cast(sum(field7) as string) field7, -- Asian
	cast(sum(field8) as string) field8,
	cast(sum(field9) as string) field9, -- Black or African American
	cast(sum(field10) as string) field10,
	cast(sum(field11) as string) field11, -- Native Hawaiian or Other Pacific Islander
	cast(sum(field12) as string) field12,
	cast(sum(field13) as string) field13, -- White
	cast(sum(field14) as string) field14,
	cast(sum(field15) as string) field15, -- Two or more races
	cast(sum(field16) as string) field16,
	cast(sum(field17) as string) field17, -- Race and ethnicity unknown
	cast(sum(field18) as string) field18
FROM CohortStatus

union

--Part B: Section I, Establishing Cohorts

SELECT 'B' part,
	'1' section, --Sub survey section - identifies cohort: 1 - Establishing cohorts, cohort year 2014
	2 line, -- Status of students in cohort: 2 - Subcohort of full-time, first-time students seeking a bachelor's or equivalent degree, cohort year 2014
	cast(sum(field1) as string) field1, --Nonresident Alien
	cast(sum(field2) as string) field2,
	cast(sum(field3) as string) field3, -- Hispanic/Latino
	cast(sum(field4) as string) field4,
	cast(sum(field5) as string) field5, -- American Indian or Alaska Native
	cast(sum(field6) as string) field6,
	cast(sum(field7) as string) field7, -- Asian
	cast(sum(field8) as string) field8,
	cast(sum(field9) as string) field9, -- Black or African American
	cast(sum(field10) as string) field10,
	cast(sum(field11) as string) field11, -- Native Hawaiian or Other Pacific Islander
	cast(sum(field12) as string) field12,
	cast(sum(field13) as string) field13, -- White
	cast(sum(field14) as string) field14,
	cast(sum(field15) as string) field15, -- Two or more races
	cast(sum(field16) as string) field16,
	cast(sum(field17) as string) field17, -- Race and ethnicity unknown
	cast(sum(field18) as string) field18
FROM CohortStatus
where subCohort = 2

union

--Part B: Section II, Cohort of Full-time, First-time Students Seeking a Bachelor's or Equivalent Degree Cohort Year 2014

SELECT 'B' part,
	'2' section, --Sub survey section - identifies cohort: 2 - Subcohort of full-time, first-time students seeking a bachelor's or equivalent degree, cohort year 2014
	cohortStatus line, --Status of students in cohort: valid values 11,12,18,19,20,30,45,51
	cast(sum(field1) as string) field1, --Nonresident Alien
	cast(sum(field2) as string) field2,
	cast(sum(field3) as string) field3, -- Hispanic/Latino
	cast(sum(field4) as string) field4,
	cast(sum(field5) as string) field5, -- American Indian or Alaska Native
	cast(sum(field6) as string) field6,
	cast(sum(field7) as string) field7, -- Asian
	cast(sum(field8) as string) field8,
	cast(sum(field9) as string) field9, -- Black or African American
	cast(sum(field10) as string) field10,
	cast(sum(field11) as string) field11, -- Native Hawaiian or Other Pacific Islander
	cast(sum(field12) as string) field12,
	cast(sum(field13) as string) field13, -- White
	cast(sum(field14) as string) field14,
	cast(sum(field15) as string) field15, -- Two or more races
	cast(sum(field16) as string) field16,
	cast(sum(field17) as string) field17, -- Race and ethnicity unknown
	cast(sum(field18) as string) field18
FROM (
    select personId personId,
            cohortStatus cohortStatus,
            field1 field1, --Nonresident Alien
            field2 field2,
            field3 field3, -- Hispanic/Latino
            field4 field4,
            field5 field5, -- American Indian or Alaska Native
            field6 field6,
            field7 field7, -- Asian
            field8 field8,
            field9 field9, -- Black or African American
            field10 field10,
            field11 field11, -- Native Hawaiian or Other Pacific Islander
            field12 field12,
            field13 field13, -- White
            field14 field14,
            field15 field15, -- Two or more races
            field16 field16,
            field17 field17, -- Race and ethnicity unknown
            field18 field18
	from CohortStatus
    where subCohort = 2 
        and cohortStatus is not null
        
	union
	
	select null,
            formatsect.cohort,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0
	from FormatPartBSection2And3 formatsect
	)
group by cohortStatus

union

----Part C: Section 2 - Bachelor’s or equivalent degree-seeking cohort - Pell recipients and recipients of a subsidized Direct Loan who did not receive a Pell Grant

--Pell recipients and recipients of a Direct Subsidized Loan who did not receive a Pell Grant table (sections 2 and 3)
--10. Number of students in cohort
--18. Completed bachelor's degree or equivalent within 150%
--29. Total completers within 150%
--45. Total exclusions

-- Group 10 - Number of students in cohort
select 'C' part, 
	'2' section,
	10 line,--cohort line, 
	cast(sum(isPellRec) as string) field1, --Recipients of a Pell Grant (within entering year)
	cast(sum(isSubLoanRec) as string) field2, --Recipients of a Direct Subsidized Loan (within entering year) that did not receive a Pell Grant
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
from CohortStatus
where subCohort = 2
	
union

-- Group 18 - Completers of bachelor's or equivalent degree programs within 150% of normal time
select 'C' part, 
	'2' section,
	18 line, --cohort line, 
	cast(sum(isPellRec) as string) field1, --Recipients of a Pell Grant (within entering year)
	cast(sum(isSubLoanRec) as string) field2, --Recipients of a Direct Subsidized Loan (within entering year) that did not receive a Pell Grant
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
from CohortStatus
where subCohort = 2
    and stat18 = 1
    
union

-- Group 29 - Total completers within 150%
select 'C' part, 
	'2' section,
	29 line,--cohort line, 
	cast(sum(isPellRec) as string) field1, --Recipients of a Pell Grant (within entering year)
	cast(sum(isSubLoanRec) as string) field2, --Recipients of a Direct Subsidized Loan (within entering year) that did not receive a Pell Grant
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
from CohortStatus
where subCohort = 2
    and stat29 = 1

union

-- Group 45 - Total exclusions 
select 'C' part, 
	'2' section,
	45 line,--cohort line, 
	cast(sum(isPellRec) as string) field1, --Recipients of a Pell Grant (within entering year)
	cast(sum(isSubLoanRec) as string) field2, --Recipients of a Direct Subsidized Loan (within entering year) that did not receive a Pell Grant
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
from CohortStatus
where subCohort = 2
    and stat45 = 1
    
union

--Part B: Section 3 - Cohort of Full-time, First-time Students Seeking Other than a Bachelor's Degree Cohort Year 2014

SELECT 'B' part,
	'3' section, --Sub survey section - identifies cohort:3 - Subcohort of full-time, first-time students seeking other than a bachelor's degree, cohort year 2014
	cohortStatus line, --Status of students in cohort: valid values 11,12,18,30,45,51
	cast(sum(field1) as string) field1, --Nonresident Alien
	cast(sum(field2) as string) field2,
	cast(sum(field3) as string) field3, -- Hispanic/Latino
	cast(sum(field4) as string) field4,
	cast(sum(field5) as string) field5, -- American Indian or Alaska Native
	cast(sum(field6) as string) field6,
	cast(sum(field7) as string) field7, -- Asian
	cast(sum(field8) as string) field8,
	cast(sum(field9) as string) field9, -- Black or African American
	cast(sum(field10) as string) field10,
	cast(sum(field11) as string) field11, -- Native Hawaiian or Other Pacific Islander
	cast(sum(field12) as string) field12,
	cast(sum(field13) as string) field13, -- White
	cast(sum(field14) as string) field14,
	cast(sum(field15) as string) field15, -- Two or more races
	cast(sum(field16) as string) field16,
	cast(sum(field17) as string) field17, -- Race and ethnicity unknown
	cast(sum(field18) as string) field18
FROM (
    select personId personId,
            cohortStatus cohortStatus,
            field1 field1, --Nonresident Alien
            field2 field2,
            field3 field3, -- Hispanic/Latino
            field4 field4,
            field5 field5, -- American Indian or Alaska Native
            field6 field6,
            field7 field7, -- Asian
            field8 field8,
            field9 field9, -- Black or African American
            field10 field10,
            field11 field11, -- Native Hawaiian or Other Pacific Islander
            field12 field12,
            field13 field13, -- White
            field14 field14,
            field15 field15, -- Two or more races
            field16 field16,
            field17 field17, -- Race and ethnicity unknown
            field18 field18
	from CohortStatus
    where subCohort = 3 
        --and cast(cohortStatus as string) in ('11', '12', '18', '30', '45', '51')
        
	union
	
	select null,
            formatsect.cohort,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0
	from FormatPartBSection2And3 formatsect
    --where cast(formatsect.cohort as string) in ('11', '12', '18', '30', '45', '51')
	)
where cast(cohortStatus as string) in ('11', '12', '18', '30', '45', '51')
group by cohortStatus
    

union

----Part C: Section 3 - Other degree/certificate-seeking subcohort - Pell recipients and recipients of a subsidized Direct Loan who did not receive a Pell Grant

--Pell recipients and recipients of a Direct Subsidized Loan who did not receive a Pell Grant table (sections 2 and 3)
--10. Number of students in cohort
--18. Completed bachelor's degree or equivalent within 150%
--29. Total completers within 150%
--45. Total exclusions

-- Group 10 - Number of students in cohort
select 'C' part, 
	'3' section,
	10 line,--cohort line, 
	cast(sum(isPellRec) as string) field1, --Recipients of a Pell Grant (within entering year)
	cast(sum(isSubLoanRec) as string) field2, --Recipients of a Direct Subsidized Loan (within entering year) that did not receive a Pell Grant
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
from CohortStatus
where subCohort = 3
	
union

-- Group 18 - Completers of bachelor's or equivalent degree programs within 150% of normal time
select 'C' part, 
	'3' section,
	18 line, --cohort line, 
	cast(sum(isPellRec) as string) field1, --Recipients of a Pell Grant (within entering year)
	cast(sum(isSubLoanRec) as string) field2, --Recipients of a Direct Subsidized Loan (within entering year) that did not receive a Pell Grant
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
from CohortStatus
where subCohort = 3
    and stat18 = 1
    
union

-- Group 29 - Total completers within 150%
select 'C' part, 
	'3' section,
	29 line,--cohort line, 
	cast(sum(isPellRec) as string) field1, --Recipients of a Pell Grant (within entering year)
	cast(sum(isSubLoanRec) as string) field2, --Recipients of a Direct Subsidized Loan (within entering year) that did not receive a Pell Grant
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
from CohortStatus
where subCohort = 3
    and stat29 = 1

union

-- Group 45 - Total exclusions 
select 'C' part, 
	'3' section,
	45 line,--cohort line, 
	cast(sum(isPellRec) as string) field1, --Recipients of a Pell Grant (within entering year)
	cast(sum(isSubLoanRec) as string) field2, --Recipients of a Direct Subsidized Loan (within entering year) that did not receive a Pell Grant
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
from CohortStatus
where subCohort = 3
    and stat45 = 1

--select * from --RegStatusMCR
--StudentMCR
--CohortStatus --27m 27s 28m 50s
