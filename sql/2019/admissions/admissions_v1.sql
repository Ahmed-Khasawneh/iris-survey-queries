/********************
EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Winter Collection
FILE NAME:      Admissions v1 (ADM)
FILE DESC:      Admissions for all institutions
AUTHOR:         Janet Hanicak
CREATED:        202000708
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
20210513    	akhasawneh 									Query refactor, data type casting and null value handling. PF-2184 (runtime test data 21m 13s prod data 20m 15s)
20200727    	jhanicak 									Initial version 
	
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
	'Pre-Fall Summer Census' repPeriodTag2,
    'October End' repPeriodTag3,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2019-08-01' AS DATE) reportingDateStart, --term start date
	CAST('2019-10-31' AS DATE) reportingDateEnd, --term end date
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
--Production Default (End)

/*
--Test Default (Begin)
select '1415' surveyYear, 
	'ADM' surveyId,
	'Fall Census' repPeriodTag1,
	'Pre-Fall Summer Census' repPeriodTag2,
    'October End' repPeriodTag3,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2014-08-01' AS DATE) reportingDateStart, --term start date
	CAST('2014-10-31' AS DATE) reportingDateEnd, --term end date
	'201410' termCode, --Fall 2014
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
--Test Default (End)
--Test values to return workable TestScore data
--Test Default (Begin)
select '1314' surveyYear, 
	'ADM' surveyId,
	'Fall Census' repPeriodTag1,
	'Pre-Fall Summer Census' repPeriodTag2,
    'October End' repPeriodTag3,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2013-08-01' AS DATE) reportingDateStart, --term start date
	CAST('2013-10-31' AS DATE) reportingDateEnd, --term end date
	'201410' termCode, --Fall 2014
	'1' partOfTermCode, 
	CAST('2013-09-10' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
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
--Test Default (End)
*/
),

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 

--  1st union 1st order - pull snapshot for defvalues.repPeriodTag1 
--  1st union 2nd order - pull snapshot for defvalues.repPeriodTag3
--  1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--  2nd union - pull default values if no record in IPEDSReportingPeriod

select distinct RepDates.surveyYear	surveyYear,
    RepDates.source source,
    RepDates.surveySection surveySection,
    RepDates.snapshotDate snapshotDate,
    RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,
	RepDates.reportingDateStart reportingDateStart,
    RepDates.reportingDateEnd reportingDateEnd,
    RepDates.repPeriodTag1 repPeriodTag1,
	RepDates.repPeriodTag2 repPeriodTag2,
	RepDates.repPeriodTag3 repPeriodTag3
from (
    select repperiodENT.surveyCollectionYear surveyYear,
	    'IPEDSReportingPeriod' source,
		to_date(repperiodENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		repPeriodENT.surveyId surveyId,
		coalesce(upper(repPeriodENT.surveySection), 'COHORT') surveySection,
		to_date(defvalues.reportingDateStart, 'YYYY-MM-DD') reportingDateStart,
		to_date(defvalues.reportingDateEnd, 'YYYY-MM-DD') reportingDateEnd,
		upper(repperiodENT.termCode) termCode,
		coalesce(upper(repperiodENT.partOfTermCode), 1) partOfTermCode,
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
                     when array_contains(repperiodENT.tags, defvalues.repPeriodTag3) then 2
                     when array_contains(repperiodENT.tags, defvalues.repPeriodTag2) then 3
			         else 4 end) asc,
			    repperiodENT.snapshotDate desc,
                coalesce(repperiodENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
		) reportPeriodRn	
		from IPEDSReportingPeriod repperiodENT
		    cross join DefaultValues defvalues
	    where repperiodENT.surveyId = defvalues.surveyId
	        and repperiodENT.surveyCollectionYear = defvalues.surveyYear
	        and repperiodENT.termCode is not null
		    --and repperiodENT.partOfTermCode is not null
	
    union 
 
	select defvalues.surveyYear surveyYear,
	    'DefaultValues' source,
		CAST('9999-09-09' as DATE) snapshotDate,
		defvalues.surveyId surveyId, 
		'COHORT' surveySection,
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		defvalues.termCode termCode,
		defvalues.partOfTermCode partOfTermCode, 
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
        defvalues.repPeriodTag3 repPeriodTag3,
		1
	from DefaultValues defvalues
    where defvalues.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = defvalues.surveyYear
											and upper(repperiodENT.surveyId) = defvalues.surveyId 
											and repperiodENT.termCode is not null
											--and repperiodENT.partOfTermCode is not null
											) 
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
    ConfigLatest.snapshotDate snapshotDate,
    ConfigLatest.repperiodSnapshotDate repperiodSnapshotDate,
	ConfigLatest.genderForUnknown genderForUnknown,
	ConfigLatest.genderForNonBinary genderForNonBinary,
    ConfigLatest.instructionalActivityType instructionalActivityType,
    ConfigLatest.acadOrProgReporter acadOrProgReporter,
    ConfigLatest.publicOrPrivateInstitution publicOrPrivateInstitution,
    ConfigLatest.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
    ConfigLatest.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
    ConfigLatest.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
	ConfigLatest.repPeriodTag2 repPeriodTag2,
	ConfigLatest.repPeriodTag3 repPeriodTag3,
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
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'configFullYearTag' source,
		to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') snapshotDate, 
		repperiod.snapshotDate repperiodSnapshotDate,
		coalesce(upper(clientConfigENT.genderForUnknown), defvalues.genderForUnknown) genderForUnknown,
		coalesce(upper(clientConfigENT.genderForNonBinary), defvalues.genderForNonBinary) genderForNonBinary,
        coalesce(upper(clientConfigENT.instructionalActivityType), defvalues.instructionalActivityType) instructionalActivityType,
        coalesce(upper(clientconfigENT.acadOrProgReporter), defvalues.acadOrProgReporter) acadOrProgReporter,
        coalesce(upper(clientconfigENT.publicOrPrivateInstitution), defvalues.publicOrPrivateInstitution) publicOrPrivateInstitution,
        coalesce(upper(clientConfigENT.icOfferUndergradAwardLevel), defvalues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
		coalesce(upper(clientConfigENT.icOfferGraduateAwardLevel), defvalues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
        coalesce(upper(clientConfigENT.icOfferDoctorAwardLevel), defvalues.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,		
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    defvalues.repPeriodTag3 repPeriodTag3,
		coalesce(upper(clientconfigENT.admSecSchoolGPA), defvalues.admSecSchoolGPA) admSecSchoolGPA,
		coalesce(upper(clientconfigENT.admSecSchoolRank), defvalues.admSecSchoolRank) admSecSchoolRank,
		coalesce(upper(clientconfigENT.admSecSchoolRecord), defvalues.admSecSchoolRecord) admSecSchoolRecord,
		coalesce(upper(clientconfigENT.admCollegePrepProgram), defvalues.admCollegePrepProgram) admCollegePrepProgram,
		coalesce(upper(clientconfigENT.admRecommendation), defvalues.admRecommendation) admRecommendation,
		coalesce(upper(clientconfigENT.admDemoOfCompetency), defvalues.admDemoOfCompetency) admDemoOfCompetency,
		coalesce(upper(clientconfigENT.admAdmissionTestScores), defvalues.admAdmissionTestScores) admAdmissionTestScores,
		coalesce(upper(clientconfigENT.admOtherTestScores), defvalues.admOtherTestScores) admOtherTestScores,
		coalesce(upper(clientconfigENT.admTOEFL), defvalues.admTOEFL) admTOEFL,       
		coalesce(upper(clientconfigENT.admUseTestScores), defvalues.admUseTestScores) admUseTestScores,
		coalesce(upper(clientconfigENT.admUseForBothSubmitted), defvalues.admUseForBothSubmitted) admUseForBothSubmitted,
		coalesce(upper(clientconfigENT.admUseForMultiOfSame), defvalues.admUseForMultiOfSame) admUseForMultiOfSame,
		coalesce(row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
			    (case when to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				coalesce(clientConfigENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
		), 1) configRn
	from IPEDSClientConfig clientConfigENT
		inner join ReportingPeriodMCR repperiod on clientConfigENT.surveyCollectionYear = repperiod.surveyYear
	    cross join DefaultValues defvalues 
	where clientConfigENT.surveyCollectionYear = defvalues.surveyYear

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

select *
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
        to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
        acadTermENT.tags,
		coalesce(upper(acadtermENT.partOfTermCode), 1) partOfTermCode, 
		coalesce(to_date(acadtermENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) recordActivityDate, 
		acadtermENT.termCodeDescription,       
		acadtermENT.partOfTermCodeDescription, 
		to_date(acadtermENT.startDate, 'YYYY-MM-DD') startDate,
		to_date(acadtermENT.endDate, 'YYYY-MM-DD') endDate,
		acadtermENT.academicYear,
		to_date(acadtermENT.censusDate, 'YYYY-MM-DD') censusDate,
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
		coalesce(row_number() over (
			order by  
				acadterm.censusDate asc
        ), 0) termOrder
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
		repPerTerms.admSecSchoolGPA admSecSchoolGPA,
		repPerTerms.admSecSchoolRank admSecSchoolRank,
		repPerTerms.admSecSchoolRecord admSecSchoolRecord,
		repPerTerms.admCollegePrepProgram admCollegePrepProgram,
		repPerTerms.admRecommendation admRecommendation,
		repPerTerms.admDemoOfCompetency admDemoOfCompetency,
		repPerTerms.admAdmissionTestScores admAdmissionTestScores,
		repPerTerms.admOtherTestScores admOtherTestScores,
		repPerTerms.admTOEFL admTOEFL,       
		repPerTerms.admUseTestScores admUseTestScores,
		repPerTerms.admUseForBothSubmitted admUseForBothSubmitted,
		repPerTerms.admUseForMultiOfSame admUseForMultiOfSame,
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
        acadterm.snapshotDate acadTermSSDate,
        repperiod.snapshotDate repPeriodSSDate,
        repperiod.reportingDateStart reportingDateStart,
        repperiod.reportingDateEnd reportingDateEnd,
        acadterm.tags tags,
        (case when array_contains(acadterm.tags, 'Pre-Fall Summer Census') then 1 else 0 end) isTagPFS,
        (case when array_contains(acadterm.tags, 'Fall Census') then 1 else 0 end) isTagF,
        null yearType,
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
	    clientconfig.genderForUnknown,
		clientconfig.genderForNonBinary,
		clientconfig.instructionalActivityType,
		clientconfig.acadOrProgReporter,
		clientconfig.admSecSchoolGPA admSecSchoolGPA,
		clientconfig.admSecSchoolRank admSecSchoolRank,
		clientconfig.admSecSchoolRecord admSecSchoolRecord,
		clientconfig.admCollegePrepProgram admCollegePrepProgram,
		clientconfig.admRecommendation admRecommendation,
		clientconfig.admDemoOfCompetency admDemoOfCompetency,
		clientconfig.admAdmissionTestScores admAdmissionTestScores,
		clientconfig.admOtherTestScores admOtherTestScores,
		clientconfig.admTOEFL admTOEFL,       
		clientconfig.admUseTestScores admUseTestScores,
		clientconfig.admUseForBothSubmitted admUseForBothSubmitted,
		clientconfig.admUseForMultiOfSame admUseForMultiOfSame,
	    coalesce(acadterm.requiredFTCreditHoursUG/
		    coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
		coalesce(row_number() over (
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
                
            ), 1) acadTermRnReg
    from ReportingPeriodMCR repperiod 
        left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
	            and repperiod.partOfTermCode = acadterm.partOfTermCode
		left join AcademicTermOrder termorder on termOrder.termCode = repperiod.termCode
		inner join ClientConfigMCR clientconfig on repperiod.surveyYear = clientconfig.surveyYear
where  upper(repperiod.surveySection) in ('COHORT', 'FALL', 'PRIOR SUMMER')
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
		coalesce(row_number() over (
			partition by
			    campusENT.snapshotDate, 
				campusENT.campus
			order by
				coalesce(campusENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
		), 1) campusRn
	from Campus campusENT 
    where coalesce(campusENT.isIpedsReportable, true) = true
		and ((coalesce(to_date(campusENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
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
--admitted students who began studies during the summer prior to Fall 2019.

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
            studentType, 
            studentLevel,
            maxPartOfTermCode,
            admTermOrder termOrder,
            censusDate,
            genderForUnknown,
		    genderForNonBinary,
            admissionDecision,
            (case when admissionDecision in ('Accepted', 'Admitted', 'Admitted, Waitlisted', 'Student Accepted', 'Student Accepted, Deferred') then 1 else 0 end) isAdmitted,
            coalesce(row_number() over (
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
            ), 1) admRn
    from ( 
        select repperiod.yearType yearType,
                repperiod.surveySection surveySection,
                repperiod.surveyYear surveyYear,
                repperiod.snapshotDate snapshotDate,
                to_date(admENT.snapshotDate, 'YYYY-MM-DD') snapshotDate_adm, 
                admENT.personId personId,
                upper(admENT.termCodeApplied) termCodeApplied,
                repperiod.maxPOT maxPartOfTermCode,
                termOrder.termOrder admTermOrder,
                repperiod.censusDate censusDate,
                repperiod.genderForUnknown,
		        repperiod.genderForNonBinary,
                admENT.applicationNumber applicationNumber,
                admENT.applicationStatus applicationStatus,
                to_date(admENT.applicationStatusActionDate, 'YYYY-MM-DD') applicationStatusActionDate,
                admENT.studentType,
                admENT.studentLevel,
                to_date(admENT.admissionDecisionActionDate, 'YYYY-MM-DD') admissionDecisionActionDate,
                admENT.admissionDecision admissionDecision,
                upper(admENT.termCodeAdmitted) termCodeAdmitted,
                coalesce(to_date(admENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) recordActivityDate,
                coalesce(row_number() over (
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
                        coalesce(admENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc,
                        (case when admENT.applicationStatus in ('Complete', 'Decision Made') then 1 else 2 end) asc
                ), 1) appRn 
        from AcademicTermReportingRefactor repperiod
            inner join Admission admENT on repperiod.termCode = upper(admENT.termCodeApplied)
                and ((coalesce(to_date(admENT.admissionDecisionActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' AS DATE)
                        and to_date(admENT.admissionDecisionActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
                    or (coalesce(to_date(admENT.admissionDecisionActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' AS DATE)
                        and ((coalesce(to_date(admENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                                and to_date(admENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                            or coalesce(to_date(admENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))))
                and to_date(admENT.applicationDate,'YYYY-MM-DD') <= repperiod.censusDate 
                and admENT.admissionType = 'New Applicant'
                -- If the Admission record is missing the type/level for a student we will keep them and try their Student entity level
				and (admENT.studentLevel = 'Undergraduate'
					or admENT.studentLevel is null)
                and (admENT.studentType = 'First Time'
					or admENT.studentType is null)
				and admENT.admissionDecision is not null
                and admENT.applicationStatus is not null
                and coalesce(admENT.isIpedsReportable, true) = true
            left join AcademicTermOrder termOrder on upper(admENT.termCodeAdmitted) = termOrder.termCode
        where repperiod.partOfTermCode = repperiod.maxPOT
--            and repperiod.termTypeNew in ('Fall', 'Pre-Fall Summer')
            and repperiod.surveySection in ('COHORT', 'PRIOR SUMMER')
        )
    where appRn = 1
        and (admTermOrder is null
            or admTermOrder between (select min(termorder) from AcademicTermReporting) and (select max(termOrder) from AcademicTermReporting))
    )
where admRn = 1 
),

RegistrationMCR as (
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 

--Enrolled requirements satisfied:
--Include students enrolled in the fall term who attended college for the first time in the prior summer term. 
--Also include students who entered with advanced standing (college credits earned before graduation from high school).

select regData.yearType, 
    regData.surveySection,
    regData.surveyYear,
    regData.snapshotDate,
    regData.regENTSSD,
    regData.termCode,
    regData.partOfTermCode,
    regData.maxPOT,
    regData.termOrder,
    regData.maxCensus,
    regData.fullTermOrder,
    regData.termType,
    regData.startDate,
    regData.censusDate,
    regData.requiredFTCreditHoursUG,
    regData.requiredFTClockHoursUG,
    regData.genderForUnknown,
    regData.genderForNonBinary,
    regData.instructionalActivityType,
    regData.equivCRHRFactor,
    adm.personId,
    regData.registrationStatus,
    regData.registrationStatusActionDate,
    regData.recordActivityDate,
    regData.courseSectionCampusOverride,
    regData.isAudited isAudited,
    regData.courseSectionLevelOverride,  
    regData.enrollmentHoursOverride,
    regData.courseSectionNumber,
    adm.isAdmitted,
    adm.studentType, 
    adm.studentLevel,
    (case when regData.personId is not null then 'Y' end) isEnrolled
from AdmissionMCR adm
    left join (
        select * 
        from (
        select repperiod.yearType yearType,
            repperiod.surveySection surveySection, 
            repperiod.surveyYear surveyYear,
            repperiod.snapshotDate snapshotDate,
            to_date(regENT.snapshotDate, 'YYYY-MM-DD') regENTSSD,
            upper(regENT.termCode) termCode,
            coalesce(upper(regENT.partOfTermCode), 1) partOfTermCode, 
            repperiod.maxPOT maxPOT,
            repperiod.termOrder termOrder,
            repperiod.maxCensus maxCensus,
            repperiod.fullTermOrder fullTermOrder,
            repperiod.termTypeNew termType,
            repperiod.startDate startDate,
            repperiod.censusDate censusDate,
            repperiod.requiredFTCreditHoursUG,
            repperiod.requiredFTClockHoursUG,
            repperiod.genderForUnknown,
            repperiod.genderForNonBinary,
            repperiod.instructionalActivityType,
            repperiod.equivCRHRFactor,
            regENT.personId personId,
            regENT.registrationStatus,
            to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') registrationStatusActionDate,
            coalesce(to_date(regENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) recordActivityDate,
            upper(regENT.courseSectionCampusOverride) courseSectionCampusOverride,
            coalesce(regENT.isAudited, false) isAudited,
            coalesce(regENT.isEnrolled, true) isEnrolled,
            regENT.courseSectionLevelOverride courseSectionLevelOverride,  
            regENT.enrollmentHoursOverride enrollmentHoursOverride,
            upper(regENT.courseSectionNumber) courseSectionNumber,
            coalesce(row_number() over (
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
                    coalesce(regENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc,
                    regENT.registrationStatusActionDate desc
            ), 1) regRn
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
        ) 
    where regRn = 1
        and isEnrolled = true
    ) regData on adm.personId = regData.personId
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  
--!! Records may be lost here but the survey expects only first-time, degree/certificate-seeking undergraduate students. If the admissions group doesn't have a student record then we have no way
--!! of knowing if they should be included. 

--Enrolled requirements satisfied:
--degree/certificate-seeking undergraduate students 
--Report admitted students who enrolled in the summer ONLY IF they remained enrolled into the fall.
--  Pulling all admission records for fall and prior summer, but only checking enrollment for fall. If student not enrolled
--  for fall, they won't be counted as enrolled

select stuData.yearType yearType,
        stuData.surveySection surveySection,
        stuData.surveyYear surveyYear,
        stuData.snapshotDate snapshotDate,
        stuData.stuSSD stuSSD,
        stuData.termCode termCode,
        stuData.personId personId,
        stuData.termOrder termOrder,
        coalesce((case when stuData.studentType = 'High School' then true
                    when stuData.studentType = 'Visiting' then true
                    when stuData.studentType = 'Unknown' then true
                    when stuData.studentLevel = 'Continuing Education' then true
                    when stuData.studentLevel = 'Other' then true
                    when studata.studyAbroadStatus = 'Study Abroad - Host Institution' then true
                    else stuData.isNonDegreeSeeking end), false) isNonDegreeSeeking,
        stuData.studentLevel,
        stuData.studentType,
        stuData.homeCampus,
    stuData.studyAbroadStatus,
    stuData.fullTimePartTimeStatus
from ( 
      select reg.yearType yearType,
            reg.surveySection surveySection,
            reg.surveyYear surveyYear,
            reg.snapshotDate snapshotDate,
            to_date(studentENT.snapshotDate,'YYYY-MM-DD') stuSSD,
            reg.termCode termCode,
            reg.personId personId,
            reg.termOrder termOrder,
            coalesce(studentENT.isNonDegreeSeeking, false) isNonDegreeSeeking,
			coalesce(reg.studentLevel, studentENT.studentLevel) studentLevel,
            coalesce(reg.studentType, studentENT.studentType) studentType,
            studentENT.firstTermEnrolled firstTermEnrolled,
            upper(studentENT.homeCampus) homeCampus,
            studentENT.fullTimePartTimeStatus,
            studentENT.studyAbroadStatus,
            coalesce(row_number() over (
                partition by
                    reg.yearType,
                    reg.surveySection,
                    studentENT.personId,                    
                    studentENT.termCode
                order by
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') = reg.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    coalesce(studentENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
            ), 1) studRn
    from RegistrationMCR reg
        inner join Student studentENT on reg.personId = studentENT.personId 
            and reg.termCode = upper(studentENT.termCode)
            and ((coalesce(to_date(studentENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)  
                and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= reg.censusDate)
                    or coalesce(to_date(studentENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  = CAST('9999-09-09' AS DATE))
            and coalesce(studentENT.isIpedsReportable, true) = true
    ) stuData
where stuData.studRn = 1 
	and stuData.studentLevel = 'Undergraduate'
	and stuData.studentType = 'First Time'
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
        --FallStu.censusDate censusDate,
        --FallStu.maxCensus maxCensus,
        FallStu.termOrder,
        --FallStu.termType,
        FallStu.studyAbroadStatus,
        FallStu.fullTimePartTimeStatus
    from (
            select stu.yearType,
                    stu.surveySection,
                    stu.snapshotDate,
                    stu.termCode, 
                    stu.termOrder,
                    --stu.termType,
                    --stu.startDate,
                    --stu.censusDate,
                    --stu.maxCensus,
                    --stu.fullTermOrder,
                    stu.personId,
                    stu.isNonDegreeSeeking,
                    stu.homeCampus,
                    stu.studentType,
                    stu.studentLevel,
                    (case when stu.studentLevel in ('Undergraduate', 'Continuing Education', 'Other') then 'UG' else 'GR' end) studentLevelUGGR,
					stu.studyAbroadStatus,
					stu.fullTimePartTimeStatus
            from StudentMCR stu
            where (stu.surveySection like '%COHORT%'
                or stu.surveySection like '%FALL%')
        ) FallStu
        left join (select stu2.personId personId,
                          stu2.studentType studentType,
                          stu2.yearType yearType,
                          stu2.surveySection surveySection
                    from StudentMCR stu2
                    where stu2.surveySection = '%SUMMER%') SumStu on FallStu.personId = SumStu.personId
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
        reg.requiredFTCreditHoursUG,
	    reg.requiredFTClockHoursUG,
        reg.genderForUnknown,
        reg.genderForNonBinary,
	    reg.instructionalActivityType,
	    reg.personId personId,
        stu.studentLevel,
	    stu.studentType,
	    stu.isNonDegreeSeeking,
		stu.studyAbroadStatus,
		stu.fullTimePartTimeStatus,
        coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        reg.courseSectionNumber,
        reg.isAudited,
        reg.isAdmitted,
        reg.isEnrolled,
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
                    coalesce(coursesectENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
            ), 1) courseRn
    from RegistrationMCR reg 
        left join StudentRefactor stu on stu.personId = reg.personId
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
			coursesect.requiredFTCreditHoursUG,
			coursesect.requiredFTClockHoursUG,
            coursesect.genderForUnknown,
            coursesect.genderForNonBinary,
			coursesect.instructionalActivityType,
			coursesect.personId personId,
			coursesect.studentLevel,
			coursesect.studentType,
			coursesect.isNonDegreeSeeking,
			coursesect.courseSectionNumber courseSectionNumber,
			coursesect.subject subject,
			coursesect.courseNumber courseNumber,
			coursesect.section section,
			coursesect.customDataValue,
			coursesect.isESL, 
			coursesect.isRemedial,
			coursesect.isAudited,
			coursesect.isAdmitted,
			coursesect.isEnrolled,
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
					coalesce(coursesectschedENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
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
	    coursesectsched.censusDate censusDate,
	    coursesectsched.termType termType,
	    coursesectsched.requiredFTCreditHoursUG,
	    coursesectsched.requiredFTClockHoursUG,
        coursesectsched.genderForUnknown,
        coursesectsched.genderForNonBinary,
	    coursesectsched.instructionalActivityType,
        coursesectsched.personId personId,
	    coursesectsched.studentType,
	    coursesectsched.studentLevel,
	    coursesectsched.isNonDegreeSeeking,
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
		coursesectsched.isAdmitted isAdmitted,
		coursesectsched.isEnrolled isEnrolled,
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
			    coalesce(courseENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
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
),

CourseTypeCountsSTU as (
-- View used to break down course category type counts for student
-- In order to calculate credits and filters per term, do not include censusDate or snapshotDate in any inner view

select *,
        (select first(maxCensus) 
                from AcademicTermReportingRefactor acadRep
                where acadRep.termcode = termcode
                and acadRep.partOfTermCode = acadRep.maxPOT) censusDate
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
                snapshotDate,
                instructionalActivityType,
                requiredFTCreditHoursUG,
                requiredFTClockHoursUG,
                genderForUnknown,
                genderForNonBinary,
                studentLevel,
                studentType,
                isAdmitted,
                isEnrolled,
                totalClockHrs,
                totalCreditHrs,
                fullTimePartTimeStatus
        from (
             select course.yearType,
                    course.termCode,
                    course.termOrder,
                    course.snapshotDate,
                    course.instructionalActivityType,
                    course.requiredFTCreditHoursUG,
                    course.requiredFTClockHoursUG,
                    course.genderForUnknown,
                    course.genderForNonBinary,
                    course.personId,
                    course.studentLevel,
                    course.studentType,
                    course.isAdmitted,
                    course.isEnrolled,
                    course.isNonDegreeSeeking,
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
                    course.snapshotDate,
                    course.instructionalActivityType,
                    course.requiredFTCreditHoursUG,
                    course.requiredFTClockHoursUG,
                    course.genderForUnknown,
                    course.genderForNonBinary,
                    course.personId,
                    course.studentLevel,
                    course.studentType,
                    course.isAdmitted,
                    course.isEnrolled,
                    course.isNonDegreeSeeking,
                    course.studyAbroadStatus,
		            course.fullTimePartTimeStatus
            )
        )
    --where ipedsInclude = 1
    )
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

--Applicant requirements satisfied:
--Gender for all applicants

select pers.yearType yearType,
        --pers.surveySection surveySection,
        --pers.surveyYear surveyYear,
        --to_date(pers.snapshotDate,'YYYY-MM-DD') snapshotDate,
        pers.censusDate censusDate,
        pers.termOrder termOrder,
        pers.personId personId,
        pers.isAdmitted isAdmitted,
        pers.isEnrolled isEnrolled,
        --pers.maxPartOfTermCode maxPartOfTermCode,
        pers.ipedsInclude ipedsInclude, 
        pers.termCode termCode, 
        pers.studentLevel studentLevel, 
        pers.studentType studentType, 
        pers.isNonDegreeSeeking isNonDegreeSeeking, 
        pers.snapshotDate snapshotDate, 
        pers.timeStatus timeStatus,
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
            crsecnt.yearType yearType,
            --crsecnt.surveySection surveySection,
            --crsecnt.surveyYear surveyYear,
            --to_date(crsecnt.snapshotDate,'YYYY-MM-DD') snapshotDate,
            crsecnt.censusDate censusDate,
            crsecnt.termOrder termOrder,
            crsecnt.personId personId,
            crsecnt.isAdmitted isAdmitted,
            crsecnt.isEnrolled isEnrolled,
            --crsecnt.maxPartOfTermCode maxPartOfTermCode,
            crsecnt.ipedsInclude ipedsInclude, 
            crsecnt.termCode termCode, 
            crsecnt.studentLevel studentLevel, 
            crsecnt.studentType studentType, 
            crsecnt.isNonDegreeSeeking isNonDegreeSeeking, 
            crsecnt.snapshotDate snapshotDate, 
            crsecnt.timeStatus timeStatus,
            crsecnt.genderForUnknown,
            crsecnt.genderForNonBinary,
            personENT.ethnicity ethnicity,
            coalesce(personENT.isHispanic, false) isHispanic,
            coalesce(personENT.isMultipleRaces, false) isMultipleRaces,
            coalesce(personENT.isInUSOnVisa, false) isInUSOnVisa,
            to_date(personENT.visaStartDate,'YYYY-MM-DD') visaStartDate,
            to_date(personENT.visaEndDate,'YYYY-MM-DD') visaEndDate,
            personENT.visaType visaType,
            coalesce(personENT.isUSCitizen, true) isUSCitizen,
            personENT.gender gender,
            coalesce(row_number() over (
                partition by
                    crsecnt.yearType,
                    --crsecnt.surveySection,
                    crsecnt.personId,
                    personENT.personId
                order by
                    (case when to_date(personENT.snapshotDate,'YYYY-MM-DD') = crsecnt.snapshotDate then 1 else 2 end) asc,
			        (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') > crsecnt.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc, 
                    (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') < crsecnt.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    coalesce(personENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
            ), 1) personRn
    from CourseTypeCountsSTU crsecnt 
        left join Person personENT on crsecnt.personId = personENT.personId
			and ((coalesce(to_date(personENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)  
				and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= crsecnt.censusDate)
					or coalesce(to_date(personENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  = CAST('9999-09-09' AS DATE))
			and coalesce(personENT.isIpedsReportable, true) = true
    ) pers
where pers.personRn = 1
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
	coalesce(row_number() over (
		partition by
			testScoreType
		order by
			testScore asc
	), 1) testRank
from (
    select personId,
        testScoreType,
        (case when admUseForMultiOfSame = 'A' then avg(testScore) else max(testScore) end) testScore
    from ( 
        select --cenDate.surveyYear surveyYear,
            pers.personID personID,
            testscoreENT.testScoreType testScoreType, --('SAT Evidence-Based Reading and Writing', 'SAT Math', 'ACT Composite', 'ACT English', 'ACT Math')
            (case when upper(config.admAdmissionTestScores) in ('R', 'C') then
                (case when upper(config.admUseTestScores) = 'B' and testscoreENT.testScoreType in ('SAT Evidence-Based Reading and Writing', 'SAT Math', 'ACT Composite', 'ACT English', 'ACT Math') then testScore
                    when upper(config.admUseTestScores) = 'A' and testscoreENT.testScoreType in ('ACT Composite', 'ACT English', 'ACT Math') then testScore
                    when upper(config.admUseTestScores) = 'S' and testscoreENT.testScoreType in ('SAT Evidence-Based Reading and Writing', 'SAT Math') then testScore
                end)
            end) testScore,
            upper(config.admUseForMultiOfSame) admUseForMultiOfSame,
            --testscoreENT.testScore testScore,
            testscoreENT.testDate testDate,
            coalesce(row_number() over (
                partition by
                    pers.personID,
                    testscoreENT.personID,
                    testscoreENT.testScoreType,
                    testscoreENT.testScore,
                    testscoreENT.testDate
                order by
                    (case when to_date(testscoreENT.snapshotDate, 'YYYY-MM-DD') = pers.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(testscoreENT.snapshotDate, 'YYYY-MM-DD') > pers.snapshotDate then to_date(testscoreENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(testscoreENT.snapshotDate, 'YYYY-MM-DD') < pers.snapshotDate then to_date(testscoreENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    coalesce(testscoreENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
            ), 1) tstRn
        from PersonMCR pers
            left join TestScore testscoreENT on pers.personId = testscoreENT.personId
                and to_date(testscoreENT.testDate, 'YYYY-MM-DD') <= pers.censusDate
                and coalesce(testscoreENT.isIPEDSReportable, true) = true
                and ((to_date(testscoreENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                                and to_date(testscoreENT.recordActivityDate, 'YYYY-MM-DD') <= pers.censusDate)
                                    or to_date(testscoreENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
            cross join (select first(admUseForMultiOfSame) admUseForMultiOfSame,
                            first(admAdmissionTestScores) admAdmissionTestScores,
                            first(admUseTestScores) admUseTestScores
                        from ClientConfigMCR) config
        where pers.studentType = 'First Time'
            and pers.isNonDegreeSeeking = false
            and pers.ipedsInclude = 1
            )
            --and adm.isEnrolled = 1
            
    where tstRn = 1
    group by personId, 
        testScoreType, 
        admUseForMultiOfSame
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
--Provide the number of first-time, degree/certificate-seeking undergraduate students who applied, who were admitted, and who enrolled (either full- or part-time) for Fall 2019. Include early decision, early action, and students 
--who began studies during the summer prior to Fall 2019.
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
	sum(case when timeStatus = 'FT' and ipedsGender = 'M' and isEnrolled = true and ipedsInclude = 1 then 1 end), --field7 
-- Number (of admitted) that enrolled full-time (Women)
	sum(case when timeStatus = 'FT' and ipedsGender = 'F' and isEnrolled = true and ipedsInclude = 1 then 1 end), --field8 
-- Number (of admitted) that enrolled full-time (Total)
	sum(case when timeStatus = 'FT' and isEnrolled = true and ipedsInclude = 1 then 1 end), --field9 
--Number (of admitted) that enrolled part-time (Male)
	sum(case when timeStatus = 'PT' and ipedsGender = 'M' and isEnrolled = true and ipedsInclude = 1 then 1 end), --field10 
--Number (of admitted) that enrolled part-time (Female)
	sum(case when timeStatus = 'PT' and ipedsGender = 'F' and isEnrolled = true and ipedsInclude = 1 then 1 end), --field11 
--Number (of admitted) that enrolled part-time (Total)
	sum(case when timeStatus = 'PT' and isEnrolled = true and ipedsInclude = 1 then 1 end), --field12 
	null, --field13
	null --field14
from PersonMCR 
where studentType = 'First Time'
    and isNonDegreeSeeking = false

union 

-- Part C: Selection Process - Test Scores
--Provide the number of first-time, degree/certificate-seeking undergraduate students who applied, who were admitted, and who enrolled (either full- or part-time) for Fall 2019. Include early decision, early action, and students 
--who began studies during the summer prior to Fall 2019.
 
select 'C', -- part
--Number of enrolled students that submitted SAT scores
	(case when config.admUseTestScores in ('S', 'B') then coalesce(testref.SATcount, 0) 
	    else null end), --field1 - 0 to 999999, -2 or blank = not-applicable
--Percent of enrolled students that submitted SAT scores
    (case when config.admUseTestScores in ('S', 'B') then 
    (case when coalesce(admenr.enrollCount, 0) > 0 then round(testref.SATcount/admenr.enrollCount*100) end) 
		else null end), -- field2 - 0 to 100, -2 or blank = not-applicable
		
--Number of enrolled students that submitted ACT scores
    (case when config.admUseTestScores in ('A', 'B') then coalesce(testref.ACTcount, 0)
		else null end), -- field3 - 0 to 999999, -2 or blank = not-applicable
--Percent of enrolled students that submitted ACT scores
    (case when config.admUseTestScores in ('A', 'B') then 
    (case when coalesce(admenr.enrollCount, 0) > 0 then round(testref.ACTcount/admenr.enrollCount*100) end) 
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
	cross join (select count(personId) enrollCount from PersonMCR where isEnrolled = true and ipedsInclude = 1) admenr
where config.admAdmissionTestScores in ('R', 'C')

union 

-- Part C: Selection Process - Test Scores
--Provide the number of first-time, degree/certificate-seeking undergraduate students who applied, who were admitted, and who enrolled (either full- or part-time) for Fall 2019. Include early decision, early action, and students 
--who began studies during the summer prior to Fall 2019.
 
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
