/********************

EVI PRODUCT:    DORIS 2020-21 IPEDS Survey Winter Collection
FILE NAME:      Admissions v1 (ADM)
FILE DESC:      Admissions for all institutions
AUTHOR:         jhanicak/akhasawneh
CREATED:        20201215

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Student Counts
Test Score MCR and Counts
Survey Formatting

SUMMARY OF CHANGES

Date(yyyymmdd)   Author             	Tag             	Comments
----------- 	--------------------	-------------   	-------------------------------------------------
20201215    	jhanicak/akhasawneh 						Initial version (runtime test data 45m 7s prod data 42m 41s)
	
********************/

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.

--Production Default (Begin)
select '1920' surveyYear, 
	'ADM' surveyId,
	'Fall Census' repPeriodTag1,
	'Fall Census' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2020-08-01' AS DATE) reportingDateStart, --term start date
	CAST('2020-12-30' AS DATE) reportingDateEnd, --term end date
	'202110' termCode, --Fall 2020
	'1' partOfTermCode, 
	CAST('2020-10-15' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
    'R' admSecSchoolGPA, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admSecSchoolRank, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admSecSchoolRecord, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admCollegePrepProgram, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admRecommendation, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admDemoOfCompetency, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admAdmissionTestScores, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admOtherTestScores, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admTOEFL, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'B' admUseTestScores, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    'B' admUseForBothSubmitted, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    'H' admUseForMultiOfSame, --Valid values: H = Highest, A = Average; Default value (if no record or null value): H
    12 requiredFTCreditHoursUG, --IPEDS-defined credit hours for full-time undergrad students
	24 requiredFTClockHoursUG, --IPEDS-defined clock hours for full-time undergrad students
    9 requiredFTCreditHoursGR --IPEDS-defined credit hours for full-time graduate students
--***** end survey-specific mods

union

select '1920' surveyYear, 
	'ADM' surveyId,
	'Fall Census' repPeriodTag1,
	'Fall Census' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2020-05-01' AS DATE) reportingDateStart, --term start date
	CAST('2020-07-30' AS DATE) reportingDateEnd, --term end date
	'202030' termCode, --Summer 2020
	'1' partOfTermCode, 
	CAST('2020-06-15' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
    'R' admSecSchoolGPA, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admSecSchoolRank, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admSecSchoolRecord, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admCollegePrepProgram, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admRecommendation, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admDemoOfCompetency, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admAdmissionTestScores, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admOtherTestScores, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admTOEFL, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'B' admUseTestScores, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    'B' admUseForBothSubmitted, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    'H' admUseForMultiOfSame, --Valid values: H = Highest, A = Average; Default value (if no record or null value): H
    12 requiredFTCreditHoursUG, --IPEDS-defined credit hours for full-time undergrad students
	24 requiredFTClockHoursUG, --IPEDS-defined clock hours for full-time undergrad students
    9 requiredFTCreditHoursGR --IPEDS-defined credit hours for full-time graduate students
--***** end survey-specific mods
--Production Default (End)

/*
--Test Default (Begin)
select '1415' surveyYear, 
	'ADM' surveyId,
	'Fall Census' repPeriodTag1,
	'Fall Census' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2014-09-03' AS DATE) reportingDateStart, --term start date
	CAST('2014-12-20' AS DATE) reportingDateEnd, --term end date
	'201510' termCode, --Fall 2014
	'1' partOfTermCode, 
	CAST('2014-09-10' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
    'R' admSecSchoolGPA, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admSecSchoolRank, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admSecSchoolRecord, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admCollegePrepProgram, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admRecommendation, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admDemoOfCompetency, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admAdmissionTestScores, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admOtherTestScores, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admTOEFL, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'B' admUseTestScores, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    'B' admUseForBothSubmitted, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    'H' admUseForMultiOfSame, --Valid values: H = Highest, A = Average; Default value (if no record or null value): H
    12 requiredFTCreditHoursUG, --IPEDS-defined credit hours for full-time undergrad students
	24 requiredFTClockHoursUG, --IPEDS-defined clock hours for full-time undergrad students
    9 requiredFTCreditHoursGR --IPEDS-defined credit hours for full-time graduate students
--***** end survey-specific mods

union

select '1415' surveyYear, 
	'ADM' surveyId,
	'Fall Census' repPeriodTag1,
	'Fall Census' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2014-05-24' AS DATE) reportingDateStart, --term start date
	CAST('2014-08-30' AS DATE) reportingDateEnd, --term end date
	'201430' termCode, --Summer 2014
	'1' partOfTermCode, 
	CAST('2014-06-01' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
    'R' admSecSchoolGPA, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admSecSchoolRank, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admSecSchoolRecord, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admCollegePrepProgram, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admRecommendation, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admDemoOfCompetency, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admAdmissionTestScores, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admOtherTestScores, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'R' admTOEFL, --Valid values: R = Required, C = Considered but not required, M = Recommended, N = Neither required nor recommended; Default value (if no record or null value): R
    'B' admUseTestScores, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    'B' admUseForBothSubmitted, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    'H' admUseForMultiOfSame, --Valid values: H = Highest, A = Average; Default value (if no record or null value): H
    12 requiredFTCreditHoursUG, --IPEDS-defined credit hours for full-time undergrad students
	24 requiredFTClockHoursUG, --IPEDS-defined clock hours for full-time undergrad students
    9 requiredFTCreditHoursGR --IPEDS-defined credit hours for full-time graduate students
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
--Returns client customizations for this survey submission year. 

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
	ConfigLatest.admSecSchoolGPA admSecSchoolGPA,
	ConfigLatest.admSecSchoolRank admSecSchoolRank,
	ConfigLatest.admSecSchoolRecord admSecSchoolRecord,
	ConfigLatest.admCollegePrepProgram admCollegePrepProgram,
	ConfigLatest.admRecommendation admRecommendation,
	ConfigLatest.admDemoOfCompetency admDemoOfCompetency,
	ConfigLatest.admAdmissionTestScores admAdmissionTestScores,
	ConfigLatest.admOtherTestScores admOtherTestScores,
	ConfigLatest.admTOEFL admTOEFL,       
	ConfigLatest.admUseTestScores admUseTestScores,
	ConfigLatest.admUseForBothSubmitted admUseForBothSubmitted,
	ConfigLatest.admUseForMultiOfSame admUseForMultiOfSame
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
		coalesce(clientconfigENT.admSecSchoolGPA, defvalues.admSecSchoolGPA) admSecSchoolGPA,
		coalesce(clientconfigENT.admSecSchoolRank, defvalues.admSecSchoolRank) admSecSchoolRank,
		coalesce(clientconfigENT.admSecSchoolRecord, defvalues.admSecSchoolRecord) admSecSchoolRecord,
		coalesce(clientconfigENT.admCollegePrepProgram, defvalues.admCollegePrepProgram) admCollegePrepProgram,
		coalesce(clientconfigENT.admRecommendation, defvalues.admRecommendation) admRecommendation,
		coalesce(clientconfigENT.admDemoOfCompetency, defvalues.admDemoOfCompetency) admDemoOfCompetency,
		coalesce(clientconfigENT.admAdmissionTestScores, defvalues.admAdmissionTestScores) admAdmissionTestScores,
		coalesce(clientconfigENT.admOtherTestScores, defvalues.admOtherTestScores) admOtherTestScores,
		coalesce(clientconfigENT.admTOEFL, defvalues.admTOEFL) admTOEFL,       
		coalesce(clientconfigENT.admUseTestScores, defvalues.admUseTestScores) admUseTestScores,
		coalesce(clientconfigENT.admUseForBothSubmitted, defvalues.admUseForBothSubmitted) admUseForBothSubmitted,
		coalesce(clientconfigENT.admUseForMultiOfSame, defvalues.admUseForMultiOfSame) admUseForMultiOfSame,
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
		defvalues.admSecSchoolGPA admSecSchoolGPA,
		defvalues.admSecSchoolRank admSecSchoolRank,
		defvalues.admSecSchoolRecord admSecSchoolRecord,
		defvalues.admCollegePrepProgram admCollegePrepProgram,
		defvalues.admRecommendation admRecommendation,
		defvalues.admDemoOfCompetency admDemoOfCompetency,
		defvalues.admAdmissionTestScores admAdmissionTestScores,
		defvalues.admOtherTestScores admOtherTestScores,
		defvalues.admTOEFL admTOEFL,       
		defvalues.admUseTestScores admUseTestScores,
		defvalues.admUseForBothSubmitted admUseForBothSubmitted,
		defvalues.admUseForMultiOfSame admUseForMultiOfSame,
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
        repPerTerms.surveyYear surveyYear,
        repPerTerms.termCode termCode,
        repPerTerms.partOfTermCode partOfTermCode,
        repPerTerms.financialAidYear financialAidYear,
        repPerTerms.termOrder termOrder,
        repPerTerms.maxCensus maxCensus,
        coalesce(repPerTerms.acadTermSSDate, repPerTerms.repPeriodSSDate) snapshotDate,
        repPerTerms.reportingDateStart reportingDateStart,
        repPerTerms.reportingDateEnd reportingDateEnd,
        repPerTerms.isTagPFS isTagPFS,
        repPerTerms.isTagF isTagF,
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
select distinct repperiod.surveySection surveySection,
        repperiod.surveyYear surveyYear,
        repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        acadterm.financialAidYear financialAidYear,
        acadterm.snapshotDate acadTermSSDate,
        repperiod.snapshotDate repPeriodSSDate,
        repperiod.reportingDateStart reportingDateStart,
        repperiod.reportingDateEnd reportingDateEnd,
        acadterm.tags tags,
        (case when array_contains(acadterm.tags, 'Pre-Fall Summer Census') then 1 else 0 end) isTagPFS,
        (case when array_contains(acadterm.tags, 'Fall Census') then 1 else 0 end) isTagF,
        null yearType,
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
		clientconfig.acadOrProgReporter,
	    coalesce(acadterm.requiredFTCreditHoursUG/
		    coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
		row_number() over (
            partition by 
                repperiod.termCode,
                repperiod.partOfTermCode
            order by
                (case when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') 
                            and ((array_contains(acadterm.tags, 'Fall Census') and acadterm.termType = 'Fall' and repperiod.surveySection = 'COHORT')
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
where  upper(repperiod.surveySection) in ('COHORT', 'PRIOR SUMMER')
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
-- We only use campus for international status, but for different campus values, including course/crn, student, degree, etc.
-- We are maintaining the ability to look at a campus at different points in time through relevant snapshots. 

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
        inner join AcademicTermReportingRefactor acadterm on acadterm.snapshotDate = to_date(campusENT.snapshotDate,'YYYY-MM-DD')
	where campusENT.isIpedsReportable = 1 
		and ((to_date(campusENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
			and to_date(campusENT.recordActivityDate,'YYYY-MM-DD') <= to_date(campusENT.snapshotDate,'YYYY-MM-DD'))
				or coalesce(to_date(campusENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
	)
where campusRn = 1
),

AdmissionMCR as ( 
-- Pull records of applicants in Fall and Prior Summer terms

--Applicant requirements satisfied:
--First-time, undergraduate students
--Applicants who have been notified of one of the following actions: admission, nonadmission, placement on waiting list, or application withdrawn by applicant or institution.
--Admitted applicants (admissions) should include wait-listed students who were subsequently offered admission.

--Admitted requirements satisfied:
--Include all students who were offered admission to your institution. This would include:
--early decision students who were notified of an admissions decision prior to the regular notification date and who agreed to accept **not sure about this one
--early action students who were notified of an admission decision prior to the regular notification date with no commitment to accept **not sure about this one
--admitted students who began studies during the summer prior to Fall 2020.

select *
from (
    select yearType,
           surveySection,
            surveyYear,
            snapshotDate,
            snapshotDate_adm,
            personId,
            termCodeApplied,
            termCodeAdmitted,
            maxPartOfTermCode,
            admTermOrder termOrder,
            censusDate,
            genderForUnknown,
		    genderForNonBinary,
            admissionDecision,
            (case when admissionDecision in ('Accepted', 'Admitted', 'Admitted, Waitlisted', 'Student Accepted', 'Student Accepted, Deferred') then 1 else 0 end) isAdmitted,
            row_number() over (
                    partition by
                        yearType,
                        surveySection,
                        termCodeApplied,
                        personId
                    order by 
                        admTermOrder desc,
                        admissionDecisionActionDate desc,
                        recordActivityDate desc,
                        (case when admissionDecision in ('Accepted', 'Admitted', 'Student Accepted') then 1
                              when admissionDecision in ('Admitted, Waitlisted', 'Student Accepted, Deferred') then 2
                              else 3 end) asc
            ) admRn
    from ( 
        select repperiod.yearType yearType,
                repperiod.surveySection surveySection,
                repperiod.surveyYear surveyYear,
                repperiod.snapshotDate snapshotDate,
                to_date(admENT.snapshotDate, 'YYYY-MM-DD') snapshotDate_adm, 
                admENT.personId personId,
                admENT.termCodeApplied termCodeApplied,
                repperiod.maxPOT maxPartOfTermCode,
                termOrder.termOrder admTermOrder,
                repperiod.censusDate censusDate,
                repperiod.genderForUnknown,
		        repperiod.genderForNonBinary,
                admENT.applicationNumber applicationNumber,
                admENT.applicationStatus applicationStatus,
                admENT.applicationStatusActionDate,
                to_date(admENT.admissionDecisionActionDate, 'YYYY-MM-DD') admissionDecisionActionDate,
                admENT.admissionDecision admissionDecision,
                admENT.termCodeAdmitted termCodeAdmitted,
                to_date(admENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
                row_number() over (
                    partition by
                        repperiod.yearType,
                        repperiod.surveySection,
                        admENT.termCodeApplied,
                        admENT.termCodeAdmitted,
                        admENT.personId,
                        admENT.admissionDecision,
                        admENT.admissionDecisionActionDate
                    order by                    
                        (case when to_date(admENT.snapshotDate, 'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
                        (case when to_date(admENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(admENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                        (case when to_date(admENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(admENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                        admENT.applicationNumber desc,
                        admENT.applicationStatusActionDate desc,
                        admENT.recordActivityDate desc,
                        (case when admENT.applicationStatus in ('Complete', 'Decision Made') then 1 else 2 end) asc
                ) appRn 
        from AcademicTermReportingRefactor repperiod
            inner join Admission admENT on repperiod.termCode = admENT.termCodeApplied
                 and ((to_date(admENT.applicationStatusActionDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                            and to_date(admENT.applicationStatusActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
                        or (to_date(admENT.applicationStatusActionDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                            and ((to_date(admENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                                    and to_date(admENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                                or to_date(admENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))))
                and to_date(admENT.applicationDate,'YYYY-MM-DD') <= repperiod.censusDate 
                and admENT.admissionType = 'New Applicant'
                and admENT.studentLevel = 'Undergrad'
                and admENT.studentType = 'First Time'
                and admENT.admissionDecision is not null
                and admENT.applicationStatus is not null
                and admENT.isIpedsReportable = 1
            left join AcademicTermOrder termOrder on admENT.termCodeAdmitted = termOrder.termCode
        where repperiod.partOfTermCode = repperiod.maxPOT
            and repperiod.termTypeNew in ('Fall', 'Pre-Fall Summer')
            and repperiod.surveySection in ('COHORT', 'PRIOR SUMMER')
        )
    where appRn = 1
        and ((admissionDecisionActionDate != CAST('9999-09-09' AS DATE)
                    and admissionDecisionActionDate <= censusDate)
                or (admissionDecisionActionDate = CAST('9999-09-09' AS DATE)
                            and ((recordActivityDate != CAST('9999-09-09' as DATE)
                                    and recordActivityDate <= censusDate)
                                or recordActivityDate = CAST('9999-09-09' as DATE))))
        and (admTermOrder is null
            or admTermOrder between (select min(termorder) from AcademicTermReporting) and (select max(termOrder) from AcademicTermReporting))
    )
where admRn = 1 
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

--Applicant requirements satisfied:
--Gender for all applicants

select pers.yearType yearType,
        pers.surveySection surveySection,
        pers.surveyYear surveyYear,
        pers.snapshotDate snapshotDate,
        pers.censusDate censusDate,
        pers.termOrder termOrder,
        pers.personId personId,
        pers.isAdmitted isAdmitted,
        pers.termCodeApplied termCodeApplied,
        pers.termCodeAdmitted termCodeAdmitted,
        pers.maxPartOfTermCode maxPartOfTermCode,
        (case when pers.gender = 'Male' then 'M'
            when pers.gender = 'Female' then 'F' 
            when pers.gender = 'Non-Binary' then pers.genderForNonBinary
            else pers.genderForUnknown
        end) ipedsGender,
        null ipedsEthnicity 
from (
    select distinct 
            adm.yearType yearType,
            adm.surveySection surveySection,
            adm.surveyYear surveyYear,
            to_date(adm.snapshotDate,'YYYY-MM-DD') snapshotDate,
            adm.censusDate censusDate,
            adm.genderForUnknown,
		    adm.genderForNonBinary,
            adm.termOrder termOrder,
            adm.personId personId,
            adm.isAdmitted isAdmitted,
            adm.termCodeApplied termCodeApplied,
            adm.termCodeAdmitted termCodeAdmitted,
            adm.maxPartOfTermCode maxPartOfTermCode,
            personENT.gender gender,
            row_number() over (
                partition by
                    adm.yearType,
                    adm.surveySection,
                    adm.personId,
                    personENT.personId
                order by
                    (case when to_date(personENT.snapshotDate,'YYYY-MM-DD') = adm.snapshotDate then 1 else 2 end) asc,
			        (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') > adm.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc, 
                    (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') < adm.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    personENT.recordActivityDate desc
            ) personRn
    from AdmissionMCR adm 
        left join Person personENT on adm.personId = personENT.personId
            and personENT.isIpedsReportable = 1
            and ((to_date(personENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
               and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= adm.censusDate) 
                or to_date(personENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
    ) pers
where pers.personRn = 1
),

RegistrationMCR as (
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 

--Enrolled requirements satisfied:
--Include students enrolled in the fall term who attended college for the first time in the prior summer term. 
--Also include students who entered with advanced standing (college credits earned before graduation from high school).

select *
from (
    select regData.yearType, 
        regData.surveySection,
        regData.surveyYear,
        regData.snapshotDate,
        regData.regENTSSD regENTSSD,
        campus.snapshotDate campusSSD,
        regData.termCode,
        regData.partOfTermCode,
        regData.maxPOT,
        regData.financialAidYear,
        regData.termOrder,
        regData.maxCensus,
        regData.fullTermOrder,
        regData.termType,
        regData.startDate,
        regData.censusDate,
        regData.requiredFTCreditHoursUG,
        regData.requiredFTClockHoursUG,
		regData.instructionalActivityType,
        regData.equivCRHRFactor,
		regData.personId,
--Report admitted students who enrolled in the summer ONLY IF they remained enrolled into the fall.
	    (case when regData.termType = 'Fall' then 1 else 0 end) isRegisteredFall,
	    regData.registrationStatus,
        regData.registrationStatusActionDate,
        regData.recordActivityDate,
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
                    (case when campus.snapshotDate > regData.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                    (case when campus.snapshotDate < regData.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc
            ) regCampRn
    from ( 
        select repperiod.yearType yearType,
            repperiod.surveySection surveySection, 
			repperiod.surveyYear surveyYear,
			repperiod.snapshotDate snapshotDate,
            to_date(regENT.snapshotDate, 'YYYY-MM-DD') regENTSSD,
            repperiod.termCode termCode,
            repperiod.partOfTermCode partOfTermCode,
            repperiod.maxPOT maxPOT,
            repperiod.financialAidYear financialAidYear,
            repperiod.termOrder termOrder,
            repperiod.maxCensus maxCensus,
            repperiod.fullTermOrder fullTermOrder,
            repperiod.termTypeNew termType,
            repperiod.startDate startDate,
			repperiod.censusDate censusDate,
			repperiod.requiredFTCreditHoursUG,
            repperiod.requiredFTClockHoursUG,
		    repperiod.instructionalActivityType,
            repperiod.equivCRHRFactor,
            regENT.personId personId,
            regENT.registrationStatus,
            to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') registrationStatusActionDate,
            to_date(regENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
            upper(regENT.campus) campus,
            coalesce(regENT.crnGradingMode, 'Standard') crnGradingMode,                    
            upper(regENT.crn) crn,
            regENT.crnLevel crnLevel,
            row_number() over (
                partition by
                    repperiod.yearType,
                    repperiod.surveySection,
                    repperiod.termCode,
                    repperiod.partOfTermCode,
                    regENT.personId,
                    regENT.crn,
                    regENT.crnLevel
                order by 
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    regENT.recordActivityDate desc
            ) regRn
        from AcademicTermReportingRefactor repperiod
            inner join Registration regENT on repperiod.termCode = regENT.termCode
                and repperiod.partOfTermCode = regENT.partOfTermCode
                and regENT.registrationStatus is not null
                and ((to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                            and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
                        or (to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                            and ((to_date(regENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                                    and to_date(regENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                                or to_date(regENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE)))) 
                and regENT.isEnrolled = 1
                and regENT.isIpedsReportable = 1
            inner join (select personId personId
                        from PersonMCR
                        where isAdmitted = 1) pers on pers.personId = regENT.personId
                ) regData
            left join CampusMCR campus on regData.campus = campus.campus
        where regData.regRn = 1
    )
where regCampRn = 1
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  

--Enrolled requirements satisfied:
--degree/certificate-seeking undergraduate students 
--Report admitted students who enrolled in the summer ONLY IF they remained enrolled into the fall.
--  Pulling all admission records for fall and prior summer, but only checking enrollment for fall. If student not enrolled
--  for fall, they won't be counted as enrolled

select *
from (
    select stuData.yearType,
            stuData.surveySection,
            stuData.surveyYear,
            stuData.snapshotDate,
            stuData.termCode,
            stuData.personId,
            stuData.firstTermEnrolled,
            stuData.studentLevel,
            stuData.studentType,
            stuData.campus,
            coalesce(campus.isInternational, false) isInternational,        
            row_number() over (
                    partition by
                        stuData.yearType,
                        stuData.surveySection,
                        stuData.termCode,
                        stuData.personId
                    order by 
                        (case when campus.snapshotDate = stuData.snapshotDate then 1 else 2 end) asc,
                        (case when campus.snapshotDate > stuData.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) asc,
                        (case when campus.snapshotDate < stuData.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) desc
                ) regCampRn
    from ( 
         select reg.yearType yearType,
                reg.surveySection surveySection,
                reg.surveyYear surveyYear,
                reg.snapshotDate snapshotDate,
                to_date(studentENT.snapshotDate,'YYYY-MM-DD') stuSSD,
                reg.termCode termCode,
                reg.personId personId,
                studentENT.isNonDegreeSeeking isNonDegreeSeeking,
                studentENT.studentLevel studentLevel,
                studentENT.studentType studentType,
                studentENT.firstTermEnrolled firstTermEnrolled,
                studentENT.campus campus,
                row_number() over (
                    partition by
                        reg.yearType,
                        reg.surveySection,
                        reg.termCode,
                        reg.personId                    
                    order by
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') = reg.snapshotDate then 1 else 2 end) asc,
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                        studentENT.recordActivityDate desc
                ) studRn
        from RegistrationMCR reg
            inner join Student studentENT on reg.personId = studentENT.personId 
                and reg.termCode = studentENT.termCode
                and reg.partOfTermCode = reg.maxPOT
                and ((to_date(studentENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)  
                    and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= reg.censusDate
                    and studentENT.studentStatus = 'Active') --do not report Study Abroad students
                        or to_date(studentENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
                and studentENT.studentLevel = 'Undergrad'
                and studentENT.isNonDegreeSeeking = false
                and studentENT.isIpedsReportable = 1
--Only report enrolled students if they were enrolled in the Fall term
         where reg.termType = 'Fall'
            and (select sum(reg2.isRegisteredFall)
                 from RegistrationMCR reg2
                 where reg2.personId = reg.personId) > 0
        ) stuData
        left join CampusMCR campus on stuData.campus = campus.campus
    where stuData.studRn = 1 
    )
where regCampRn = 1
and isInternational = false
),

CourseSectionMCR as (
--Included to get enrollment hours of a CRN

--Enrolled requirements satisfied:
--Get course info in order to determine time status for enrolled students

select *
from (
    select stu.yearType,
        reg.surveySection surveySection,
        reg.surveyYear surveyYear,
        reg.snapshotDate snapshotDate,
        reg.termCode termCode,
        reg.partOfTermCode partOfTermCode,
        reg.censusDate,
        reg.termType,
        reg.termOrder,
        reg.requiredFTCreditHoursUG,
	    reg.requiredFTClockHoursUG,
	    reg.instructionalActivityType,
	    stu.personId personId,
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
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc, 
                    coursesectENT.recordActivityDate desc
            ) courseRn
    from RegistrationMCR reg   
        inner join StudentMCR stu on stu.personId = reg.personId
            and stu.termCode = reg.termCode
            and stu.yearType = reg.yearType
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

--Enrolled requirements satisfied:
--Get course info in order to determine time status for enrolled students

select *
from (
	select coursesect.yearType yearType,
	    coursesect.surveySection surveySection,
        coursesect.surveyYear surveyYear,
	    coursesect.snapshotDate snapshotDate,
	    coursesect.termCode termCode,
	    coursesect.partOfTermCode partOfTermCode,
		coursesect.censusDate censusDate,
		coursesect.termType termType,
		coursesect.termOrder termOrder, 
		coursesect.requiredFTCreditHoursUG,
	    coursesect.requiredFTClockHoursUG,
	    coursesect.instructionalActivityType,
        coursesect.personId personId,
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
                (case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') > coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') < coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
			    coursesectschedENT.recordActivityDate desc
		) courseSectSchedRn
	from CourseSectionMCR coursesect
	    left join CourseSectionSchedule coursesectschedENT on coursesect.termCode = coursesectschedENT.termCode 
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

--Enrolled requirements satisfied:
--Get course info in order to determine time status for enrolled students

select *
from (
	select coursesectsched.yearType yearType,
	    coursesectsched.surveySection surveySection,
        coursesectsched.surveyYear surveyYear,
	    coursesectsched.snapshotDate snapshotDate,
	    coursesectsched.termCode termCode,
		coursesectsched.partOfTermCode partOfTermCode,
	    termorder.termOrder courseTermOrder,
	    coursesectsched.termOrder termOrder,
	    coursesectsched.censusDate censusDate,
	    coursesectsched.termType termType,
	    coursesectsched.requiredFTCreditHoursUG,
	    coursesectsched.requiredFTClockHoursUG,
	    coursesectsched.instructionalActivityType,
        coursesectsched.personId personId,
	    coursesectsched.crn crn,
		coursesectsched.section section,
		coursesectsched.schedSection schedSection,
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
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') > coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') < coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
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
This set of views is used to transform and aggregate records from MCR views above for applied, admitted and enrolled counts
*****/

CourseTypeCountsSTU as (
-- View used to break down course category type counts for student

--Enrolled requirements satisfied:
--Use course and term data in order to determine time status for enrolled students

select yearType,
    surveySection,
    surveyYear,
    termCode,
    personId,    
    timeStatus,
    ipedsInclude
from ( 
    select yearType,
            surveySection,
            surveyYear,
            termCode,
            personId,
            (case when instructionalActivityType in ('CR', 'B') then 
                                (case when totalCreditHrsCalc >= requiredFTCreditHoursUG then 'FT' else 'PT' end)
                          when instructionalActivityType = 'CL' then 
                                (case when totalClockHrs >= requiredFTClockHoursUG then 'FT' else 'PT' end) 
             end) timeStatus,
            (case when totalCredCourses > 0 --exclude students not enrolled for credit
                            then (case when totalESLCourses = totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
                                       when totalCECourses = totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
                                       when totalIntlCourses = totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
                                       when totalAuditCourses = totalCredCourses then 0 --exclude students exclusively auditing classes
                                       -- when... then 0 --exclude PHD residents or interns
                                       -- when... then 0 --exclude students in experimental Pell programs
                                       else 1
                                  end)
                  when totalRemCourses = totalCourses -- and isNonDegreeSeeking = 0 
                    then 1 --include students taking remedial courses if degree-seeking
                  else 0 
             end) ipedsInclude
    from ( 
         select course.yearType yearType,
                course.surveySection surveySection,
                course.surveyYear surveyYear,
                course.termCode termCode,
                course.instructionalActivityType,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId personId,
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
                 course.surveySection,
                 course.surveyYear,  
                 course.termCode, 
                 course.instructionalActivityType,
                 course.requiredFTCreditHoursUG,
                 course.requiredFTClockHoursUG,
                 course.personId
        )
    )
),

AdmissionsCount as (
--Aggregates all applicant, admitted and enrolled data

--Survey requirements satisfied:
--Total counts of applied (all), admitted (isAdmitted) and enrolled (isEnrolled)

select person.yearType,
       person.surveySection,
       person.surveyYear,
       person.snapshotDate,
       person.personId,
       person.ipedsGender,
       person.isAdmitted,
       coalesce(enr.ipedsInclude, 0) isEnrolled,
       enr.timeStatus
from PersonMCR person
    left join CourseTypeCountsSTU enr on person.personId = enr.personId
        and person.yearType = enr.yearType
        and person.surveySection = enr.surveySection
        and person.surveyYear = enr.surveyYear
 ),

/*****
BEGIN SECTION - Test Score MCR and Counts
This set of views is used to pull most current records and transform and aggregate records for test score totals
*****/

TestScoreMCR as (
--Pulls test score data for enrolled students

select personId,
    testScoreType,
    testScore,
	row_number() over (
		partition by
			testScoreType
		order by
			testScore asc
	) testRank
from (
    select test.personId,
        test.testScoreType,
        (case when (select first(config.admUseForMultiOfSame) from ClientConfigMCR config) = 'A' then avg(test.testScore) else max(test.testScore) end) testScore
    from ( 
        select cenDate.surveyYear surveyYear,
            adm.personID personID,
            testscoreENT.testScoreType testScoreType, --('SAT Evidence-Based Reading and Writing', 'SAT Math', 'ACT Composite', 'ACT English', 'ACT Math')
            testscoreENT.testScore testScore,
            testscoreENT.testDate testDate,
            row_number() over (
                partition by
                    adm.personID,
                    testscoreENT.personID,
                    testscoreENT.testScoreType,
                    testscoreENT.testScore,
                    testscoreENT.testDate
                order by
                    (case when to_date(testscoreENT.snapshotDate, 'YYYY-MM-DD') = cenDate.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(testscoreENT.snapshotDate, 'YYYY-MM-DD') > cenDate.snapshotDate then to_date(testscoreENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(testscoreENT.snapshotDate, 'YYYY-MM-DD') < cenDate.snapshotDate then to_date(testscoreENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    testscoreENT.recordActivityDate desc
            ) tstRn
        from AdmissionsCount adm
            inner join (select acadterm.yearType yearType, acadterm.surveyYear surveyYear, acadterm.censusDate censusDate, acadterm.snapshotDate snapshotDate
                          from AcademicTermReportingRefactor acadterm
                          where acadterm.termType = 'Fall'
                          and acadterm.partOfTermCode = acadterm.maxPOT) cenDate on adm.yearType = cenDate.yearType
                            and adm.surveyYear = cenDate.surveyYear
            left join TestScore testscoreENT on adm.personId = testscoreENT.personId
                and testscoreENT.testDate <= cenDate.censusDate
                and testscoreENT.isIPEDSReportable = 1
                and ((to_date(testscoreENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                                and to_date(testscoreENT.recordActivityDate, 'YYYY-MM-DD') <= cenDate.censusDate)
                                    or to_date(testscoreENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
        where (select first(config.admAdmissionTestScores) from ClientConfigMCR config) in ('R', 'C')
            and (((select first(config.admUseTestScores) from ClientConfigMCR config) = 'B' and testscoreENT.testScoreType in ('SAT Evidence-Based Reading and Writing', 'SAT Math', 'ACT Composite', 'ACT English', 'ACT Math'))
                or ((select first(config.admUseTestScores) from ClientConfigMCR config) = 'A' and testscoreENT.testScoreType in ('ACT Composite', 'ACT English', 'ACT Math'))
                or ((select first(config.admUseTestScores) from ClientConfigMCR config) = 'S' and testscoreENT.testScoreType in ('SAT Evidence-Based Reading and Writing', 'SAT Math')))
            and adm.isEnrolled = 1
        ) test    
    where test.tstRn = 1
    group by test.personId, test.testScoreType
    )
),

TestTypeCounts as (
--View to calculate unduplicated count of students submitting SAT and ACT scores

select (select count(distinct personId) stuCount
        from TestScoreMCR
        where testScoreType in ('SAT Evidence-Based Reading and Writing', 'SAT Math')) SATcount,
        (select count(distinct personId) stuCount
        from TestScoreMCR
        where testScoreType in ('ACT Composite', 'ACT English', 'ACT Math')) ACTcount
),

TestScoreCounts as (
--View to calculate total number of tests and 25th percentile rank number and 75th percentile rank number by testScoreType

select testScoreType,
        numTests,
        (case when per25 = 0 then 1 else per25 end) per25,
        (case when per75 = 0 then 1 else per75 end) per75
from (
    select testScoreType, 
            max(testRank) numTests,
            round(max(testRank) * .25) per25, --11
            round(max(testRank) * .75) per75 --34
    from TestScoreMCR
    group by testScoreType
    )
),

TestScorePerc as (
--View to find the 25th and 75th percentile score per test type

select max(SATReadPerc25) SATReadPerc25,
        max(SATReadPerc75) SATReadPerc75,
        max(SATMathPerc25) SATMathPerc25,
        max(SATMathPerc75) SATMathPerc75,
        max(ACTCompPerc25) ACTCompPerc25,
        max(ACTCompPerc75) ACTCompPerc75,
        max(ACTEngPerc25) ACTEngPerc25,
        max(ACTEngPerc75) ACTEngPerc75,
        max(ACTMathPerc25) ACTMathPerc25,
        max(ACTMathPerc75) ACTMathPerc75
from (		
	select (case when test.testScoreType = 'SAT Evidence-Based Reading and Writing' then max(test.score25) end) SATReadPerc25,
            (case when test.testScoreType = 'SAT Evidence-Based Reading and Writing' then max(test.score75) end) SATReadPerc75,
            (case when test.testScoreType = 'SAT Math' then max(test.score25) end) SATMathPerc25,
            (case when test.testScoreType = 'SAT Math' then max(test.score75) end) SATMathPerc75,
            (case when test.testScoreType = 'ACT Composite' then max(test.score25) end) ACTCompPerc25,
            (case when test.testScoreType = 'ACT Composite' then max(test.score75) end) ACTCompPerc75,
            (case when test.testScoreType = 'ACT English' then max(test.score25) end) ACTEngPerc25,
            (case when test.testScoreType = 'ACT English' then max(test.score75) end) ACTEngPerc75,
            (case when test.testScoreType = 'ACT Math' then max(test.score25) end) ACTMathPerc25,
            (case when test.testScoreType = 'ACT Math' then max(test.score75) end) ACTMathPerc75 
    from (
    select testA.testScoreType,
            max(testA.score25) score25,
            max(testA.score75) score75
    from (
        select tests.testScoreType, 
               (case when tests.testRank = counts.per25 then tests.testScore end) score25,
               (case when tests.testRank = counts.per75 then tests.testScore end) score75
        from TestScoreMCR tests
            inner join TestScoreCounts counts on tests.testScoreType = counts.testScoreType
                and (tests.testRank = counts.per25
                    or tests.testRank = counts.per75)
            ) testA
        group by testA.testScoreType
        ) test
    group by test.testScoreType
    )
) 

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
fields are used as follows...
PART                      - Should reflect the appropriate survey part
Field1 - Field18          - Used for numeric values. Specifically, counts of gender/ethnicity categories described by the IPEDS index of values.
****/

-- Part A: Admission considerations
--Report the options that best describe how an institution uses any of the following data in its undergraduate selection process.
-- Valid values: 1=Required, 2=Recommended, 3=Neither Required nor Recommended, 5=Considered but not required

select 'A' part,
--Secondary school GPA
    (case when clientconfig.admSecSchoolGPA = 'R' then '1'
        when clientconfig.admSecSchoolGPA = 'M' then '2'
        when clientconfig.admSecSchoolGPA = 'N' then '3'
        when clientconfig.admSecSchoolGPA = 'C' then '5'
    end ) field1,
-- Secondary school rank
    (case when clientconfig.admSecSchoolRank = 'R' then '1'
        when clientconfig.admSecSchoolRank = 'M' then '2'
        when clientconfig.admSecSchoolRank = 'N' then '3'
        when clientconfig.admSecSchoolRank = 'C' then '5'
    end ) field2, 
-- Secondary school record
    (case when clientconfig.admSecSchoolRecord = 'R' then '1'
        when clientconfig.admSecSchoolRecord = 'M' then '2'
        when clientconfig.admSecSchoolRecord = 'N' then '3'
        when clientconfig.admSecSchoolRecord = 'C' then '5'
    end ) field3, 
-- Completion of college-preparatory program
    (case when clientconfig.admCollegePrepProgram = 'R' then '1'
        when clientconfig.admCollegePrepProgram = 'M' then '2'
        when clientconfig.admCollegePrepProgram = 'N' then '3'
        when clientconfig.admCollegePrepProgram = 'C' then '5'
    end ) field4, 
-- Recommendations
    (case when clientconfig.admRecommendation = 'R' then '1'
        when clientconfig.admRecommendation = 'M' then '2'
        when clientconfig.admRecommendation = 'N' then '3'
        when clientconfig.admRecommendation = 'C' then '5'
    end ) field5, 
-- Formal demonstration of competencies (e.g., portfolios, certificates of mastery, assessment instruments)
    (case when clientconfig.admDemoOfCompetency = 'R' then '1'
        when clientconfig.admDemoOfCompetency = 'M' then '2'
        when clientconfig.admDemoOfCompetency = 'N' then '3'
        when clientconfig.admDemoOfCompetency = 'C' then '5'
    end ) field6, 
-- Admission test scores - SAT/ACT
    (case when clientconfig.admAdmissionTestScores = 'R' then '1'
        when clientconfig.admAdmissionTestScores = 'M' then '2'
        when clientconfig.admAdmissionTestScores = 'N' then '3'
        when clientconfig.admAdmissionTestScores = 'C' then '5'
    end ) field7, -- If "1" or "5" is entered, Part C is required.
-- 	Admission test scores - TOEFL (Test of English as a Foreign Language)
    (case when clientconfig.admTOEFL = 'R' then '1'
        when clientconfig.admTOEFL = 'M' then '2'
        when clientconfig.admTOEFL = 'N' then '3'
        when clientconfig.admTOEFL = 'C' then '5'
    end ) field8,
-- Admission test scores - Other Test (ABT, Wonderlic, WISC-III, etc.)
    (case when clientconfig.admOtherTestScores = 'R' then '1'
        when clientconfig.admOtherTestScores = 'M' then '2'
        when clientconfig.admOtherTestScores = 'N' then '3'
        when clientconfig.admOtherTestScores = 'C' then '5'
    end ) field9,
    null field10, 
    null field11,
    null field12,
    null field13,
    null field14
from ClientConfigMCR clientconfig 

union 

-- Part B: Selection Process - A/A/E
--Provide the number of first-time, degree/certificate-seeking undergraduate students who applied, who were admitted, and who enrolled (either full- or part-time) for Fall 2020. Include early decision, early action, and students 
--who began studies during the summer prior to Fall 2020.
-- Valid values: - 0 to 999999, -2 or blank = not-applicable

select 'B', -- part
-- Number of applicants (Men)
    sum(case when ipedsGender = 'M' then 1 else 0 end), --field1 
-- Number of applicants (Women)
	sum(case when ipedsGender = 'F' then 1 else 0 end), --field2 
 -- Number of applicants (total)	
	sum(1), --field3 
-- Number of admissions	(Men)
	sum(case when ipedsGender = 'M' then isAdmitted end), --field4
-- Number of admissions (Women)	
	sum(case when ipedsGender = 'F' then isAdmitted end), --field5 
-- Number of admissions (Total)
	sum(isAdmitted), --field6 
-- Number (of admitted) that enrolled full-time (Men)
	sum(case when timeStatus = 'FT' and ipedsGender = 'M' then isEnrolled end), --field7 
-- Number (of admitted) that enrolled full-time (Women)
	sum(case when timeStatus = 'FT' and ipedsGender = 'F' then isEnrolled end), --field8 
-- Number (of admitted) that enrolled full-time (Total)
	sum(case when timeStatus = 'FT' then isEnrolled end), --field9 
--Number (of admitted) that enrolled part-time (Male)
	sum(case when timeStatus = 'PT' and ipedsGender = 'M' then isEnrolled end), --field10 
--Number (of admitted) that enrolled part-time (Female)
	sum(case when timeStatus = 'PT' and ipedsGender = 'F' then isEnrolled end), --field11 
--Number (of admitted) that enrolled part-time (Total)
	sum(case when timeStatus = 'PT' then isEnrolled end), --field12 
	null, --field13
	null --field14
from AdmissionsCount

union 

-- Part C: Selection Process - Test Scores
--Provide the number of first-time, degree/certificate-seeking undergraduate students who applied, who were admitted, and who enrolled (either full- or part-time) for Fall 2020. Include early decision, early action, and students 
--who began studies during the summer prior to Fall 2020.
 
select 'C', -- part
--Number of enrolled students that submitted SAT scores
	(case when config.admUseTestScores in ('S', 'B') then coalesce(testref.SATcount, 0) 
	    else null end), --field1 - 0 to 999999, -2 or blank = not-applicable
--Percent of enrolled students that submitted SAT scores
    (case when config.admUseTestScores in ('S', 'B') then 
    (case when coalesce(admenr.enrollCount, 0) > 0 then round(testref.SATcount/admenr.enrollCount*100) else '0' end) 
		else null end), -- field2 - 0 to 100, -2 or blank = not-applicable
--Number of enrolled students that submitted ACT scores
    (case when config.admUseTestScores in ('A', 'B') then coalesce(testref.ACTcount, 0)
		else null end), -- field3 - 0 to 999999, -2 or blank = not-applicable
--Percent of enrolled students that submitted ACT scores
    (case when config.admUseTestScores in ('A', 'B') then 
    (case when coalesce(admenr.enrollCount, 0) > 0 then round(testref.ACTcount/admenr.enrollCount*100) else '0' end) 
		else null end), -- field4 = 0 to 100, -2 or blank = not-applicable
--SAT Evidence-Based Reading and Writing - 25th Percentile
	(case when config.admUseTestScores in ('S', 'B') then coalesce(testperc.SATReadPerc25, 200) 
		else null end), -- field5 - 200 to 800, -2 or blank = not-applicable
--SAT Evidence-Based Reading and Writing - 75th Percentile
	(case when config.admUseTestScores in ('S', 'B') then coalesce(testperc.SATReadPerc75, 200) 
		else null end), -- field6 - 200 to 800, -2 or blank = not-applicable
--SAT Math - 25th Percentile
	(case when config.admUseTestScores in ('S', 'B') then coalesce(testperc.SATMathPerc25, 200)
		else null end), -- field7 - 200 to 800, -2 or blank = not-applicable
--SAT Math - 75th Percentile
	(case when config.admUseTestScores in ('S', 'B') then coalesce(testperc.SATMathPerc75, 200)
		else null end), -- field8 - 200 to 800, -2 or blank = not-applicable
--ACT Composite - 25th Percentile
	(case when config.admUseTestScores in ('A', 'B') then coalesce(testperc.ACTCompPerc25, 1)
		else null end), -- field9 - 1 to 36, -2 or blank = not-applicable
--ACT Composite - 75th Percentile
	(case when config.admUseTestScores in ('A', 'B') then coalesce(testperc.ACTCompPerc75, 1)
		else null end), -- field10 - 1 to 36, -2 or blank = not-applicable
--ACT English - 25th Percentile
	(case when config.admUseTestScores in ('A', 'B') then coalesce(testperc.ACTEngPerc25, 1)
		else null end), -- field11 - 1 to 36, -2 or blank = not-applicable
--ACT English - 75th Percentile
	(case when config.admUseTestScores in ('A', 'B') then coalesce(testperc.ACTEngPerc75, 1)
		else null end), -- field12 - 1 to 36, -2 or blank = not-applicable
--ACT Math - 25th Percentile
	(case when config.admUseTestScores in ('A', 'B') then coalesce(testperc.ACTMathPerc25, 1)
		else null end), -- field13 - 1 to 36, -2 or blank = not-applicable
--ACT Math - 75th Percentile
	(case when config.admUseTestScores in ('A', 'B') then coalesce(testperc.ACTMathPerc75, 1)
		else null end)-- field14 - 1 to 36, -2 or blank = not-applicable
from TestTypeCounts testref
	cross join TestScorePerc testperc
	cross join ClientConfigMCR config
	cross join (select sum(isEnrolled) enrollCount from AdmissionsCount) admenr
where config.admAdmissionTestScores in ('R', 'C')

union 

-- Part C: Selection Process - Test Scores
--Provide the number of first-time, degree/certificate-seeking undergraduate students who applied, who were admitted, and who enrolled (either full- or part-time) for Fall 2020. Include early decision, early action, and students 
--who began studies during the summer prior to Fall 2020.
 
select 'C', -- part
--Number of enrolled students that submitted SAT scores
	null, --field1 - 0 to 999999, -2 or blank = not-applicable
--Percent of enrolled students that submitted SAT scores
    null, -- field2 - 0 to 100, -2 or blank = not-applicable
--Number of enrolled students that submitted ACT scores
    null, -- field3 - 0 to 999999, -2 or blank = not-applicable
--Percent of enrolled students that submitted ACT scores
    null, -- field4 = 0 to 100, -2 or blank = not-applicable
--SAT Evidence-Based Reading and Writing - 25th Percentile
	null, -- field5 - 200 to 800, -2 or blank = not-applicable
--SAT Evidence-Based Reading and Writing - 75th Percentile
	null, -- field6 - 200 to 800, -2 or blank = not-applicable
--SAT Math - 25th Percentile
	null, -- field7 - 200 to 800, -2 or blank = not-applicable
--SAT Math - 75th Percentile
	null, -- field8 - 200 to 800, -2 or blank = not-applicable
--ACT Composite - 25th Percentile
	null, -- field9 - 1 to 36, -2 or blank = not-applicable
--ACT Composite - 75th Percentile
	null, -- field10 - 1 to 36, -2 or blank = not-applicable
--ACT English - 25th Percentile
	null, -- field11 - 1 to 36, -2 or blank = not-applicable
--ACT English - 75th Percentile
	null, -- field12 - 1 to 36, -2 or blank = not-applicable
--ACT Math - 25th Percentile
	null, -- field13 - 1 to 36, -2 or blank = not-applicable
--ACT Math - 75th Percentile
	null -- field14 - 1 to 36, -2 or blank = not-applicable
from ClientConfigMCR config
where config.admAdmissionTestScores in ('M', 'N')
