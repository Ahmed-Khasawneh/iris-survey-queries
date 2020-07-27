/********************

EVI PRODUCT:    DORIS 2020-21 IPEDS Survey Winter Collection
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
20200727    	jhanicak 									Initial version 
	
********************/

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.
/*
select '1920' surveyYear, 
	'ADM' surveyId,
	'202010' termCode, --Fall 2019
	'1' partOfTermCode, 
    'Fall' surveySection, 
	CAST('2019-10-15' AS TIMESTAMP) censusDate,
    'Y' includeNonDegreeAsUG,   --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
	'M' genderForUnknown,       --Valid values: M = Male, F = Female; Default value (if no record or null value): M
	'F' genderForNonBinary,      --Valid values: M = Male, F = Female; Default value (if no record or null value): F
    'CR' instructionalActivityType, --Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR
    1 icOfferUndergradAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
    1 icOfferGraduateAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
    1 icOfferDoctorAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
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
    'B' admUseForMultiOfSame, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    12 requiredFTCreditHoursUG, --IPEDS-defined credit hours for full-time undergrad students
	24 requiredFTClockHoursUG, --IPEDS-defined clock hours for full-time undergrad students
    9 requiredFTCreditHoursGR --IPEDS-defined credit hours for full-time graduate students

union

select '1920' surveyYear, 
	'ADM' surveyId, 
	'201930' termCode, --Prior Summer 2019
	'1' partOfTermCode, 
    'Prior Summer' surveySection,
	CAST('2019-07-15' AS TIMESTAMP) censusDate,
	'Y' includeNonDegreeAsUG,   --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
	'M' genderForUnknown,       --Valid values: M = Male, F = Female; Default value (if no record or null value): M
	'F' genderForNonBinary,      --Valid values: M = Male, F = Female; Default value (if no record or null value): F
    'CR' instructionalActivityType, --Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR
    1 icOfferUndergradAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
    1 icOfferGraduateAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
    1 icOfferDoctorAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
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
    'B' admUseForMultiOfSame, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    12 requiredFTCreditHoursUG, --IPEDS-defined credit hours for full-time undergrad students
	24 requiredFTClockHoursUG, --IPEDS-defined clock hours for full-time undergrad students
    9 requiredFTCreditHoursGR --IPEDS-defined credit hours for full-time graduate students
*/

select '1314' surveyYear, 
	'ADM' surveyId,
	'201410' termCode, --Fall 2013
	'1' partOfTermCode, 
    'Fall' surveySection, 
	CAST('2013-09-13' AS TIMESTAMP) censusDate,
    'Y' includeNonDegreeAsUG,   --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
	'M' genderForUnknown,       --Valid values: M = Male, F = Female; Default value (if no record or null value): M
	'F' genderForNonBinary,      --Valid values: M = Male, F = Female; Default value (if no record or null value): F
    'CR' instructionalActivityType, --Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR
    1 icOfferUndergradAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
    1 icOfferGraduateAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
    1 icOfferDoctorAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
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
    'B' admUseForMultiOfSame, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    12 requiredFTCreditHoursUG, --IPEDS-defined credit hours for full-time undergrad students
	24 requiredFTClockHoursUG, --IPEDS-defined clock hours for full-time undergrad students
    9 requiredFTCreditHoursGR --IPEDS-defined credit hours for full-time graduate students
/*
union

select '1314' surveyYear, 
	'ADM' surveyId, 
	'201330' termCode, --Prior Summer 2013
	'1' partOfTermCode, 
    'Prior Summer' surveySection,
	CAST('2013-07-15' AS TIMESTAMP) censusDate,
	'Y' includeNonDegreeAsUG,   --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
	'M' genderForUnknown,       --Valid values: M = Male, F = Female; Default value (if no record or null value): M
	'F' genderForNonBinary,      --Valid values: M = Male, F = Female; Default value (if no record or null value): F
    'CR' instructionalActivityType, --Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR
    1 icOfferUndergradAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
    1 icOfferGraduateAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
    1 icOfferDoctorAwardLevel, --Valid values: Y = Yes, N = No; Default value (if no record or null value): Y
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
    'B' admUseForMultiOfSame, --Valid values: A = ACT, S = SAT, B = Both; Default value (if no record or null value): B
    12 requiredFTCreditHoursUG, --IPEDS-defined credit hours for full-time undergrad students
	24 requiredFTClockHoursUG, --IPEDS-defined clock hours for full-time undergrad students
    9 requiredFTCreditHoursGR --IPEDS-defined credit hours for full-time graduate students
*/
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

select ConfigLatest.surveyYear surveyYear, 
        ConfigLatest.surveyId surveyId, 
        ConfigLatest.surveySection surveySection,
        ConfigLatest.termCode termCode,
        ConfigLatest.partOfTermCode partOfTermCode, 
        ConfigLatest.censusDate censusDate,
		ConfigLatest.includeNonDegreeAsUG includeNonDegreeAsUG,
		ConfigLatest.genderForUnknown genderForUnknown,
		ConfigLatest.genderForNonBinary genderForNonBinary,
        ConfigLatest.instructionalActivityType instructionalActivityType,
        ConfigLatest.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
        ConfigLatest.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        ConfigLatest.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
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
		ConfigLatest.admUseForMultiOfSame admUseForMultiOfSame,
        ConfigLatest.requiredFTCreditHoursUG requiredFTCreditHoursUG,
        ConfigLatest.requiredFTClockHoursUG requiredFTClockHoursUG, 
        ConfigLatest.requiredFTCreditHoursGR requiredFTCreditHoursGR 	    
from (
	select defvalues.surveyYear surveyYear, 
            defvalues.surveyId surveyId,
            defvalues.surveySection surveySection,
            defvalues.termCode termCode, 
            defvalues.partOfTermCode partOfTermCode,
            defvalues.censusDate censusDate,
            NVL(clientconfigENT.includeNonDegreeAsUG, defvalues.includeNonDegreeAsUG) includeNonDegreeAsUG,
            NVL(clientconfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
            NVL(clientconfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
            NVL(clientconfigENT.instructionalActivityType, defvalues.instructionalActivityType) instructionalActivityType,
            NVL(clientconfigENT.icOfferUndergradAwardLevel, defvalues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
            NVL(clientconfigENT.icOfferGraduateAwardLevel, defvalues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
            NVL(clientconfigENT.icOfferDoctorAwardLevel, defvalues.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,
            NVL(clientconfigENT.admSecSchoolGPA, defvalues.admSecSchoolGPA) admSecSchoolGPA,
            NVL(clientconfigENT.admSecSchoolRank, defvalues.admSecSchoolRank) admSecSchoolRank,
            NVL(clientconfigENT.admSecSchoolRecord, defvalues.admSecSchoolRecord) admSecSchoolRecord,
            NVL(clientconfigENT.admCollegePrepProgram, defvalues.admCollegePrepProgram) admCollegePrepProgram,
            NVL(clientconfigENT.admRecommendation, defvalues.admRecommendation) admRecommendation,
            NVL(clientconfigENT.admDemoOfCompetency, defvalues.admDemoOfCompetency) admDemoOfCompetency,
            NVL(clientconfigENT.admAdmissionTestScores, defvalues.admAdmissionTestScores) admAdmissionTestScores,
            NVL(clientconfigENT.admOtherTestScores, defvalues.admOtherTestScores) admOtherTestScores,
            NVL(clientconfigENT.admTOEFL, defvalues.admTOEFL) admTOEFL,       
            NVL(clientconfigENT.admUseTestScores, defvalues.admUseTestScores) admUseTestScores,
            NVL(clientconfigENT.admUseForBothSubmitted, defvalues.admUseForBothSubmitted) admUseForBothSubmitted,
            NVL(clientconfigENT.admUseForMultiOfSame, defvalues.admUseForMultiOfSame) admUseForMultiOfSame,
            defvalues.requiredFTCreditHoursUG requiredFTCreditHoursUG,
            defvalues.requiredFTClockHoursUG requiredFTClockHoursUG, 
            defvalues.requiredFTCreditHoursGR requiredFTCreditHoursGR, 
            row_number() over (
                partition by
                    clientconfigENT.surveyCollectionYear
                order by
                    clientconfigENT.recordActivityDate desc
            ) configRn
	from IPEDSClientConfig clientconfigENT
		cross join DefaultValues defvalues
	where clientconfigENT.surveyCollectionYear = defvalues.surveyYear
		
	union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select defvalues.surveyYear surveyYear, 
            defvalues.surveyId surveyId, 
            defvalues.surveySection surveySection, 
            defvalues.termCode termCode, 
            defvalues.partOfTermCode partOfTermCode,
            defvalues.censusDate censusDate,
            defvalues.includeNonDegreeAsUG includeNonDegreeAsUG,
            defvalues.genderForUnknown genderForUnknown,
            defvalues.genderForNonBinary genderForNonBinary,
            defvalues.instructionalActivityType instructionalActivityType,
            defvalues.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
            defvalues.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
            defvalues.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
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
            defvalues.requiredFTCreditHoursUG requiredFTCreditHoursUG,
            defvalues.requiredFTClockHoursUG requiredFTClockHoursUG, 
            defvalues.requiredFTCreditHoursGR requiredFTCreditHoursGR,
            1 configRn
	from DefaultValues defvalues
	where defvalues.surveyYear not in (select clientconfigENT.surveyCollectionYear
										from IPEDSClientConfig clientconfigENT
										where clientconfigENT.surveyCollectionYear = defvalues.surveyYear)
	) ConfigLatest
where ConfigLatest.configRn = 1
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for each term and part of term code. 
--PartOfTerm code defines a subcategory of a termCode that may have different start, end and census dates. 

select *
from ( 
	select acadTermENT.*,
		row_number() over (
			partition by 
				acadTermENT.termCode,
				acadTermENT.partOfTermCode
			order by  
				acadTermENT.recordActivityDate desc
		) acadTermRn
	from AcademicTerm acadTermENT
	where acadTermENT.isIPEDSReportable = 1 
	)
where acadTermRn = 1
),

AcadTermOrder as (
-- Orders term codes based on date span and keeps the numeric value of the greatest term/part of term record. 

select termCode termCode, 
    max(termOrder) termOrder
from (
	select acadterm.termCode termCode,
	   row_number() over (
			order by  
				acadterm.startDate asc,
				acadterm.endDate asc
        ) termOrder    
	from AcademicTermMCR acadterm
	)
group by termCode
),

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 
--Need current survey terms ('Fall', 'Summer') and prior year retention ('RetFall', 'RetSummer')
--Introduces 'cohortInd' which is used through out the query to associate records with the appropriate cohort. 

select RepDates.surveyYear surveyYear,
	RepDates.surveySection cohortInd,
	RepDates.termCode termCode,	
	termorder.termOrder termOrder,
	RepDates.partOfTermCode partOfTermCode,	
	to_date(coalesce(acadterm.censusDate, DATE_ADD(acadterm.startDate, 15), RepDates.censusDate),'yyyy-MM-dd') censusDate,
	RepDates.includeNonDegreeAsUG includeNonDegreeAsUG, --cohort
	RepDates.genderForUnknown genderForUnknown,
	RepDates.genderForNonBinary genderForNonBinary,
    RepDates.instructionalActivityType instructionalActivityType,
    RepDates.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
    RepDates.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
    RepDates.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
    RepDates.admSecSchoolGPA admSecSchoolGPA,
    RepDates.admSecSchoolRank admSecSchoolRank,
    RepDates.admSecSchoolRecord admSecSchoolRecord,
    RepDates.admCollegePrepProgram admCollegePrepProgram,
    RepDates.admRecommendation admRecommendation,
    RepDates.admDemoOfCompetency admDemoOfCompetency,
    RepDates.admAdmissionTestScores admAdmissionTestScores,
    RepDates.admOtherTestScores admOtherTestScores,
    RepDates.admTOEFL admTOEFL,       
    RepDates.admUseTestScores admUseTestScores,
    RepDates.admUseForBothSubmitted admUseForBothSubmitted,
    RepDates.admUseForMultiOfSame admUseForMultiOfSame,
	coalesce(acadterm.requiredFTCreditHoursUG, RepDates.requiredFTCreditHoursUG) requiredFTCreditHoursUG,
    coalesce(acadterm.requiredFTClockHoursUG, RepDates.requiredFTClockHoursUG) requiredFTClockHoursUG, 
    coalesce(acadterm.requiredFTCreditHoursGR, RepDates.requiredFTCreditHoursGR) requiredFTCreditHoursGR
from (
    select repperiodENT.surveyCollectionYear surveyYear,
			repperiodENT.surveySection surveySection,
			repperiodENT.termCode termCode,
			repperiodENT.partOfTermCode partOfTermCode,
            clientconfig.censusDate censusDate,
			clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
			clientconfig.genderForUnknown genderForUnknown,
			clientconfig.genderForNonBinary genderForNonBinary,
            clientconfig.instructionalActivityType instructionalActivityType,
            clientconfig.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
            clientconfig.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
            clientconfig.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
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
            clientconfig.requiredFTCreditHoursUG requiredFTCreditHoursUG,
            clientconfig.requiredFTClockHoursUG requiredFTClockHoursUG, 
            clientconfig.requiredFTCreditHoursGR requiredFTCreditHoursGR,
			row_number() over (	
				partition by 
					repperiodENT.surveyCollectionYear,
					repperiodENT.surveyId,
					repperiodENT.surveySection,
					repperiodENT.termCode,
					repperiodENT.partOfTermCode	
				order by 
                    repperiodENT.recordActivityDate desc
			) reportPeriodRn	
		from IPEDSReportingPeriod repperiodENT
			cross join ClientConfigMCR clientconfig
		where repperiodENT.surveyCollectionYear = clientconfig.surveyYear
			and repperiodENT.surveyId = clientconfig.surveyId
			and repperiodENT.surveySection = clientconfig.surveySection
			and repperiodENT.termCode is not null
			and repperiodENT.partOfTermCode is not null
				
	union
	
	--Pulls default values for Fall when IPEDSReportingPeriod record doesn't exist
	select clientconfig.surveyYear surveyYear,
			clientconfig.surveySection surveySection,
			clientconfig.termCode termCode,
			clientconfig.partOfTermCode partOfTermCode,
            clientconfig.censusDate censusDate,
			clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
			clientconfig.genderForUnknown genderForUnknown,
			clientconfig.genderForNonBinary genderForNonBinary,
            clientconfig.instructionalActivityType instructionalActivityType,
            clientconfig.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
            clientconfig.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
            clientconfig.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
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
            clientconfig.requiredFTCreditHoursUG requiredFTCreditHoursUG,
            clientconfig.requiredFTClockHoursUG requiredFTClockHoursUG, 
            clientconfig.requiredFTCreditHoursGR requiredFTCreditHoursGR,
            1 reportPeriodRn
	from ClientConfigMCR clientconfig
--check this
	where clientconfig.surveyYear not in (select repperiodENT1.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT1
										  where repperiodENT1.surveyCollectionYear = clientconfig.surveyYear
											and repperiodENT1.surveyId = clientconfig.surveyId
											and repperiodENT1.surveySection = clientconfig.surveySection
											and repperiodENT1.termCode is not null
											and repperiodENT1.partOfTermCode is not null)
	)  RepDates
	inner join AcadTermOrder termorder on RepDates.termCode = termorder.termCode
        left join AcademicTermMCR acadterm on RepDates.termCode = acadterm.termCode	
                and RepDates.partOfTermCode = acadterm.partOfTermCode
    where RepDates.reportPeriodRn = 1
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusMCR as ( 
-- Returns most recent campus record for each campus available per the reporting terms and part of terms (ReportingPeriod).

select *
from (
	select campusENT.*,
		row_number() over (
			partition by
				campusENT.campus
			order by
				campusENT.recordActivityDate desc
		   ) campusRn
	from ReportingPeriodMCR repperiod
		cross join campus campusENT 
	where campusENT.isIpedsReportable = 1
--ak 20200406 Including dummy date changes. (PF-1368)
		and ((to_date(campusENT.recordActivityDate,'yyyy-MM-dd') != CAST('9999-09-09' AS TIMESTAMP)
			and to_date(campusENT.recordActivityDate,'yyyy-MM-dd') <= repperiod.censusDate)
				or to_date(campusENT.recordActivityDate,'yyyy-MM-dd') = CAST('9999-09-09' AS TIMESTAMP))
	)
where campusRn = 1
),

ApplicantMCR as ( 
/* Number of applicants
Applicants are individuals who have fulfilled the institution's requirements to be considered for admission (including payment or waiving of 
the application fee, if any) and who has been notified of one of the following actions: admission, nonadmission, placement on 
waiting list, or application withdrawn by applicant or institution.
*/

select cohortInd,
	personId,
	termCode,
	termOrder,
	censusDate,
    applicationNumber,
    applicationStatus,
    applicationStatusActionDate,
    admissionDecision
from ( 
	select repperiod.cohortInd cohortInd,
		admENT.personId personId,
		admENT.termCodeApplied termCode,
		repperiod.termOrder termOrder,
		repperiod.censusDate censusDate,
        admENT.applicationNumber applicationNumber,
        admENT.applicationStatus applicationStatus,
        admENT.applicationStatusActionDate applicationStatusActionDate,
        admENT.admissionDecision admissionDecision,
		row_number() over (
			partition by
				admENT.personId,
				admENT.termCodeApplied
			order by
			    admENT.applicationNumber desc,
                admENT.applicationStatusActionDate desc,
				admENT.recordActivityDate desc
		) admRn
	from ReportingPeriodMCR repperiod
		inner join Admission admENT on repperiod.termCode = admENT.termCodeApplied
			and ((to_date(admENT.applicationStatusActionDate,'yyyy-MM-dd') != CAST('9999-09-09' AS TIMESTAMP)
				and to_date(admENT.applicationStatusActionDate,'yyyy-MM-dd') <= repperiod.censusDate)
					or to_date(admENT.applicationStatusActionDate,'yyyy-MM-dd') = CAST('9999-09-09' AS TIMESTAMP))
			and admENT.applyDate <= repperiod.censusDate
			and admENT.admissionType = 'New Applicant'
            and admENT.studentLevel = 'Undergrad'
            and admENT.studentType = 'First Time'
            and ((admENT.applicationStatus != 'Incomplete')
                or (admENT.applicationStatus = 'Incomplete'
                        and admENT.admissionDecision is not null))
			and admENT.isIpedsReportable = 1
	)
where admRn = 1
),
--(to_date(degreeENT.recordActivityDate,'yyyy-MM-dd')

AdmissionMCR as ( 
/* Number of admissions
Applicants that have been granted an official offer to enroll in a postsecondary institution.
include all students who were offered admission to your institution:
early decision students who were notified of an admission decision prior to the regular notification date and who agreed to accept
early action students who were notified of an admission decision prior to the regular notification date with no commitment to accept
the admitted students who began studies during the summer prior to the fall reporting period
*/

select cohortInd,
	personId,
	termCode,
	termOrder,
    applicationNumber,
    admissionDecision
from ( 
	select app2.cohortInd cohortInd,
		app2.personId personId,
		app2.termCode termCode,
		app2.termOrder termOrder,
        app2.applicationNumber applicationNumber,
        case when AdmissRec.personId is not null then 1 else 0 end admissionDecision,
        --to_date(admENT.admissionDecisionActionDate,'yyyy-MM-dd') admissionDecisionActionDate,
        --admENT.admissionType admissionType,
		row_number() over (
			partition by
				app2.personId--,
				--admENT.termCodeApplied
			order by
			    AdmissRec.admissionDecisionActionDate desc,
				AdmissRec.recordActivityDate desc
		) admRn
	from ApplicantMCR app2
	    left join (
	            select admENT.personId personId,
		            admENT.termCodeApplied termCode,
		            admENT.applicationNumber applicationNumber,
                    --admENT.admissionDecision admissionDecision,
                    to_date(admENT.admissionDecisionActionDate,'yyyy-MM-dd') admissionDecisionActionDate,
                    --admENT.admissionType admissionType,
                    admENT.recordActivityDate recordActivityDate
	            from ApplicantMCR app
		            inner join Admission admENT on app.personId = admENT.personId
		                and app.termCode = admENT.termCodeApplied
		                and app.applicationNumber = admENT.applicationNumber
		                and admENT.admissionType = 'New Applicant'
		                and admENT.admissionDecision = 'Accepted'
		                and admENT.isIpedsReportable = 1
		          ) AdmissRec on app2.personId = AdmissRec.personId
		                and app2.termCode = AdmissRec.termCode
		                and app2.applicationNumber = AdmissRec.applicationNumber 
			and ((AdmissRec.admissionDecisionActionDate != CAST('9999-09-09' AS TIMESTAMP)
				and AdmissRec.admissionDecisionActionDate <= app2.censusDate)
					or AdmissRec.admissionDecisionActionDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where admRn = 1
)

select count(*) 
from ApplicantMCR --281
--AdmissionMCR --281
--where admissionDecision = 0 --24
--admissionDecision = 1 --257 ReportingPeriodMCR 
--ApplicantMCR --285
--order by 2, 5
