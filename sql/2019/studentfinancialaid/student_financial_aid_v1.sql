/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Winter Collection
FILE NAME:      Student Financial Aid v1 (SFA)
FILE DESC:      Student Financial Aid for private institutions reporting on a fall cohort (academic reporters)
AUTHOR:         Ahmed Khasawneh
CREATED:        20200714

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Cohort Creation
Formatting Views
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)   Author             	Tag             	Comments
----------- 	--------------------	-------------   	-------------------------------------------------
20200714    	akhasawneh 									Initial version 
	
	
DELETE AFTER INITIAL DEV 
IMPLEMENTATION NOTES:
     Cohort - prior Fall (academic reporters) or full prior academic year (program reporters)
     Aid period - Any time during the prior academic year (academic reporters) or aid year period from July 1 through June 30 (program reporters)
     
********************/

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.
/*
select '1920' surveyYear, 
	CAST('2019-08-01' AS TIMESTAMP) startDateProg,
	CAST('2019-10-31' AS TIMESTAMP) endDateProg, 
	'SFA' surveyId,
	'Fall' sectionFall,
	'Summer' sectionSummer,
	'202010' termCodeFall, 
	'201930' termCodeSummer, 
	CAST('2019-10-15' AS TIMESTAMP) censusDateFall,
	CAST('2019-07-31' AS TIMESTAMP) censusDateSummer, 
	CAST('2018-07-01' AS TIMESTAMP) censusStartPost911, --Post-9/11 GI Bill Benefits: July 1, 2018 - June 30, 2019 
	CAST('2019-06-30' AS TIMESTAMP) censusEndPost911,
	CAST('2018-10-01' AS TIMESTAMP) censusStartDoD, --Report for Department of Defense Tuition Assistance Program: October 1, 2018 - September 30, 2019
	CAST('2019-09-30' AS TIMESTAMP) censusEndDoD,
	'1' partOfTermCode,
	'A' acadOrProgReporter, --A = Academic, P = Program
	'Y' includeNonDegreeAsUG, --Y = Yes, N = No
	'M' genderForUnknown, --M = Male, F = Female
	'F' genderForNonBinary,  --M = Male, F = Female
-- ak 20200707 Adding additional client config values for level offereings and instructor activity type (PF-1552)
	'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    '' sfaLargestProgramCIPC,
    '' sfaGradStudentsOnly
*/
--Use for testing internally only

select '1516' surveyYear, --testing '1516'
	CAST('2015-08-01' AS TIMESTAMP) startDateProg,
	CAST('2015-10-31' AS TIMESTAMP) endDateProg, 
	'SFA' surveyId,
	'Fall' sectionFall,
	'Summer' sectionSummer,
	'201610' termCodeFall,
	'201530' termCodeSummer,
	CAST('2015-10-15' AS TIMESTAMP) censusDateFall,
	CAST('2015-07-31' AS TIMESTAMP) censusDateSummer, 
	CAST('2015-07-01' AS TIMESTAMP) censusStartPost911, --Post-9/11 GI Bill Benefits: July 1, 2018 - June 30, 2019 
	CAST('2015-06-30' AS TIMESTAMP) censusEndPost911,
	CAST('2015-10-01' AS TIMESTAMP) censusStartDoD, --Report for Department of Defense Tuition Assistance Program: October 1, 2018 - September 30, 2019
	CAST('2015-09-30' AS TIMESTAMP) censusEndDoD,
	'1' partOfTermCode,
	'A' acadOrProgReporter, --A = Academic, P = Program
	'Y' includeNonDegreeAsUG, --Y = Yes, N = No
	'M' genderForUnknown, --M = Male, F = Female
	'F' genderForNonBinary,  --M = Male, F = Female
	'CR' instructionalActivityType, --CR = Credit, CL = Clock, B = Both
    'Y' icOfferUndergradAwardLevel, --Y = Yes, N = No
    'Y' icOfferGraduateAwardLevel, --Y = Yes, N = No
    '' sfaLargestProgramCIPC,
    '' sfaGradStudentsOnly
    
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

select ConfigLatest.surveyYear surveyYear,
	ConfigLatest.acadOrProgReporter acadOrProgReporter,
	ConfigLatest.includeNonDegreeAsUG includeNonDegreeAsUG,
	ConfigLatest.sfaLargestProgCIPC sfaLargestProgCIPC,
	ConfigLatest.genderForUnknown genderForUnknown,
	ConfigLatest.genderForNonBinary genderForNonBinary,
	ConfigLatest.surveyId surveyId,
	ConfigLatest.sectionFall sectionFall,
	ConfigLatest.sectionSummer sectionSummer,
	ConfigLatest.termCodeFall termCodeFall, 
	ConfigLatest.termCodeSummer termCodeSummer, 
	ConfigLatest.censusDateFall censusDateFall,
	ConfigLatest.censusDateSummer censusDateSummer, 
	ConfigLatest.partOfTermCode partOfTermCode,
	ConfigLatest.instructionalActivityType instructionalActivityType,
	ConfigLatest.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
	ConfigLatest.icOfferGraduateAwardLevel icOfferGraduateAwardLevel
from (
	select clientconfigENT.surveyCollectionYear surveyYear,
		NVL(clientconfigENT.acadOrProgReporter, defvalues.acadOrProgReporter) acadOrProgReporter,
		NVL(clientconfigENT.includeNonDegreeAsUG, defvalues.includeNonDegreeAsUG) includeNonDegreeAsUG,
		case when NVL(clientconfigENT.acadOrProgReporter, defvalues.acadOrProgReporter) = 'P' then clientconfigENT.sfaLargestProgCIPC else NULL end sfaLargestProgCIPC,
		NVL(clientconfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		NVL(clientconfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
		NVL(clientconfigENT.icOfferUndergradAwardLevel, defvalues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
		NVL(clientconfigENT.icOfferGraduateAwardLevel, defvalues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
		NVL(clientconfigENT.instructionalActivityType, defvalues.instructionalActivityType) instructionalActivityType,
		defvalues.surveyId surveyId,
		defvalues.sectionFall sectionFall,
		defvalues.sectionSummer sectionSummer,
		defvalues.termCodeFall termCodeFall, 
		defvalues.termCodeSummer termCodeSummer, 
		defvalues.censusDateFall censusDateFall,
		defvalues.censusDateSummer censusDateSummer, 
		defvalues.partOfTermCode partOfTermCode,
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
		defvalues.acadOrProgReporter acadOrProgReporter,
		defvalues.includeNonDegreeAsUG includeNonDegreeAsUG,
		null sfaLargestProgCIPC,
		defvalues.genderForUnknown genderForUnknown,
		defvalues.genderForNonBinary genderForNonBinary,
		defvalues.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		defvalues.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
		defvalues.instructionalActivityType instructionalActivityType,
		defvalues.surveyId surveyId,
		defvalues.sectionFall sectionFall,
		defvalues.sectionSummer sectionSummer,
		defvalues.termCodeFall termCodeFall, 
		defvalues.termCodeSummer termCodeSummer, 
		defvalues.censusDateFall censusDateFall,
		defvalues.censusDateSummer censusDateSummer, 
		defvalues.partOfTermCode partOfTermCode,
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
--Need current survey terms ('Fall', 'Summer')
--Introduces 'cohortInd' which is used through out the query to associate records with the appropriate cohort. 

select RepDates.surveyYear surveyYear,
	RepDates.surveySection cohortInd,
	RepDates.termCode termCode,	
	termorder.termOrder termOrder,
	RepDates.partOfTermCode partOfTermCode,
	acadterm.financialAidYear financialAidYear,
	NVL(NVL(acadterm.censusDate, DATE_ADD(acadterm.startDate, 15)), RepDates.censusDate) censusDate,
	RepDates.acadOrProgReporter acadOrProgReporter,
	RepDates.includeNonDegreeAsUG includeNonDegreeAsUG,
	RepDates.genderForUnknown genderForUnknown,
	RepDates.genderForNonBinary genderForNonBinary,
    acadterm.requiredFTCreditHoursUG requiredFTCreditHoursUG,
    acadterm.requiredFTClockHoursUG requiredFTClockHoursUG,
    acadterm.requiredFTCreditHoursGR requiredFTCreditHoursGR,
	NVL(acadterm.requiredFTCreditHoursUG/
		NVL(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
	RepDates.instructionalActivityType instructionalActivityType,
	RepDates.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
	RepDates.icOfferGraduateAwardLevel icOfferGraduateAwardLevel
from (

--Pulls reporting period and default configuration values for Fall, Summer from IPEDSReportingPeriod entity
	select surveyYear surveyYear,
		surveySection surveySection,
		termCode termCode,
		censusDate censusDate,
		partOfTermCode partOfTermCode,
		acadOrProgReporter acadOrProgReporter,
		includeNonDegreeAsUG includeNonDegreeAsUG,
		genderForUnknown genderForUnknown,
		genderForNonBinary genderForNonBinary,
        instructionalActivityType instructionalActivityType,
        icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		icOfferGraduateAwardLevel icOfferGraduateAwardLevel
	from (
		select repperiodENT.surveyCollectionYear surveyYear,
			repperiodENT.surveySection surveySection,
			repperiodENT.termCode termCode,
			case when repperiodENT.surveySection = clientconfig.sectionFall then clientconfig.censusDateFall
				when repperiodENT.surveySection = clientconfig.sectionSummer then clientconfig.censusDateSummer
			end censusDate,
			repperiodENT.partOfTermCode partOfTermCode,
			clientconfig.acadOrProgReporter acadOrProgReporter,
			clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
			clientconfig.genderForUnknown genderForUnknown,
			clientconfig.genderForNonBinary genderForNonBinary,
			clientconfig.instructionalActivityType instructionalActivityType,
			clientconfig.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
			clientconfig.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
			row_number() over (	
				partition by 
					repperiodENT.surveyCollectionYear,
					repperiodENT.surveyId,
					repperiodENT.surveySection,
					repperiodENT.termCode,
					repperiodENT.partOfTermCode	
				order by repperiodENT.recordActivityDate desc
			) reportPeriodRn	
		from IPEDSReportingPeriod repperiodENT
			cross join ClientConfigMCR clientconfig
		where repperiodENT.surveyCollectionYear = clientconfig.surveyYear
			and repperiodENT.surveyId = clientconfig.surveyId
			and repperiodENT.termCode is not null
			and repperiodENT.partOfTermCode is not null
		)
	where reportPeriodRn = 1
	
	union
	
--Pulls default values for Fall when IPEDSReportingPeriod record doesn't exist
	select clientconfig.surveyYear surveyYear,
		clientconfig.sectionFall surveySection,
		clientconfig.termCodeFall termCode,
		clientconfig.censusDateFall censusDate,
		clientconfig.partOfTermCode partOfTermCode,
		clientconfig.acadOrProgReporter acadOrProgReporter,
		clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary,
        clientconfig.instructionalActivityType instructionalActivityType,
        clientconfig.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		clientconfig.icOfferGraduateAwardLevel icOfferGraduateAwardLevel
	from ClientConfigMCR clientconfig
	where clientconfig.surveyYear not in (select repperiodENT1.surveyCollectionYear
										  from ClientConfigMCR clientconfig1
											cross join IPEDSReportingPeriod repperiodENT1
										  where repperiodENT1.surveyCollectionYear = clientconfig1.surveyYear
											and repperiodENT1.surveyId = clientconfig1.surveyId
											and repperiodENT1.surveySection = clientconfig1.sectionFall
											and repperiodENT1.termCode is not null
											and repperiodENT1.partOfTermCode is not null)
	
	union
	
--Pulls default values for Summer when IPEDSReportingPeriod record doesn't exist
	select clientconfig.surveyYear surveyYear,
		clientconfig.sectionSummer surveySection,
		clientconfig.termCodeSummer termCode,
		clientconfig.censusDateSummer censusDate,
		clientconfig.partOfTermCode partOfTermCode,
		clientconfig.acadOrProgReporter acadOrProgReporter,
		clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary,
        clientconfig.instructionalActivityType instructionalActivityType,
        clientconfig.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		clientconfig.icOfferGraduateAwardLevel icOfferGraduateAwardLevel
	from ClientConfigMCR clientconfig
	where clientconfig.surveyYear not in (select repperiodENT1.surveyCollectionYear
										  from ClientConfigMCR clientconfig1
											cross join IPEDSReportingPeriod repperiodENT1
										  where repperiodENT1.surveyCollectionYear = clientconfig1.surveyYear
											and repperiodENT1.surveyId = clientconfig1.surveyId
											and repperiodENT1.surveySection = clientconfig1.sectionSummer
											and repperiodENT1.termCode is not null
											and repperiodENT1.partOfTermCode is not null)										
	
	)  RepDates
	inner join AcadTermOrder termorder on RepDates.termCode = termorder.termCode
	left join AcademicTermMCR acadterm on RepDates.termCode = acadterm.termCode	
		and RepDates.partOfTermCode = acadterm.partOfTermCode
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
		and ((campusENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
			and campusENT.recordActivityDate <= repperiod.censusDate)
				or campusENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where campusRn = 1
),

RegistrationMCR as ( 
--Returns all student enrollment records as of the ReportingPeriod and where course is viable

select cohortInd cohortInd,
	personId personId,
	regTermCode termCode,
	termOrder termOrder,
	partOfTermCode partOfTermCode,
	financialAidYear financialAidYear,
	censusDate censusDate,
	crn crn,
	crnLevel crnLevel,
	isInternational isInternational,
	crnGradingMode crnGradingMode,
	equivCRHRFactor equivCRHRFactor,
	instructionalActivityType instructionalActivityType,
	requiredFTClockHoursUG requiredFTClockHoursUG,
	requiredFTCreditHoursUG requiredFTCreditHoursUG,
	requiredFTCreditHoursGR requiredFTCreditHoursGR,
	acadOrProgReporter acadOrProgReporter
from (
	select repperiod.cohortInd cohortInd,
		regENT.personId personId,
		repperiod.censusDate censusDate,
		repperiod.termCode regTermCode,
		repperiod.financialAidYear financialAidYear,
		termorder.termOrder termOrder,
		repperiod.partOfTermCode partOfTermCode,
		campus.isInternational isInternational,
		regENT.crnGradingMode crnGradingMode,
		repperiod.equivCRHRFactor equivCRHRFactor,
		repperiod.requiredFTClockHoursUG requiredFTClockHoursUG,
		repperiod.requiredFTCreditHoursUG requiredFTCreditHoursUG,
		repperiod.requiredFTCreditHoursGR requiredFTCreditHoursGR,
		regENT.crn crn,
		regENT.crnLevel crnLevel,
		repperiod.instructionalActivityType instructionalActivityType,
		repperiod.acadOrProgReporter acadOrProgReporter,
		row_number() over (
			partition by
				regENT.personId,
				regENT.termCode,
				regENT.partOfTermCode,
				regENT.crn,
				regENT.crnLevel
			order by
				regENT.recordActivityDate desc
		) regRn
	from ReportingPeriodMCR repperiod
		inner join AcadTermOrder termorder on repperiod.termOrder = termorder.termOrder
		inner join Registration regENT on repperiod.termCode = regENT.termCode 
			and regENT.partOfTermCode = repperiod.partOfTermCode
			--and regENT.registrationStatus = 'Enrolled'
			and regENT.isEnrolled = 1
			and regENT.isIpedsReportable = 1 
			and ((regENT.registrationStatusActionDate  != CAST('9999-09-09' AS TIMESTAMP)
				and regENT.registrationStatusActionDate  <= repperiod.censusDate)
					or regENT.registrationStatusActionDate  = CAST('9999-09-09' AS TIMESTAMP))
			--and regENT.recordActivityDate <= repperiod.censusDate
		left join CampusMCR campus on regENT.campus = campus.campus
	)
where regRn = 1
    and cohortInd in ('Fall', 'Summer')
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods. 

select *
from (
	select distinct reg.cohortInd cohortInd,
		reg.crn,
		studentENT.*,
		row_number() over (
			partition by
				studentENT.personId,
				studentENT.termCode
			order by
				studentENT.recordActivityDate desc
		) studRn
	from RegistrationMCR reg
		inner join Student studentENT ON reg.personId = studentENT.personId
			and reg.termCode = studentENT.termCode
			and studentENT.isIpedsReportable = 1
			and studentENT.studentStatus = 'Active'
			and ((studentENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) --77
				and studentENT.recordActivityDate <= reg.censusDate)
					or studentENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			--and studentENT.studentLevel in ('Undergrad', 'Continuing Ed') -- DoD and Post 9/11 aid applies to both UG & GR so we can't exclude here.
	)
where studRn = 1
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 
	
select *
from (
	select reg.cohortInd cohortInd,
		personENT.*,
		row_number() over (
			partition by
				personENT.personId
			order by
				personENT.recordActivityDate desc
			) personRn
	from RegistrationMCR reg
		inner join Person personENT ON reg.personId = personENT.personId
			and personENT.isIpedsReportable = 1
--ak 20200406 Including dummy date changes. (PF-1368)
			and ((personENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and personENT.recordActivityDate <= reg.censusDate)
					or personENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 

	)
where personRn = 1
),

AcademicTrackMCR as (
--Returns most up to date student academic track information as of their award date and term. 

select *
from (
	select reg.personId personId,
--		award.degree degree,
--		award.awardedDate awardedDate,
--		award.awardedTermCode awardedTermCode,
		acadTrackENT.termCodeEffective termCodeEffective,
		acadTrackENT.curriculumCode curriculumCode,
--		award.degreeLevel degreeLevel,
--		award.college awardCollege,
		acadTrackENT.college acadCollege,
		acadTrackENT.fieldOfStudyPriority FOSPriority,
		acadTrackENT.curriculumCode major,
		row_number() over (
			partition by
				acadTrackENT.personId,
				acadTrackENT.academicTrackLevel,
				acadTrackENT.degree,
				acadTrackENT.fieldOfStudyPriority
			order by
				acadterm.termOrder desc,
				acadTrackENT.recordActivityDate desc
		) as acadTrackRn
    from RegistrationMCR reg
		inner join AcademicTrack acadTrackENT 
			on award.personId = acadTrackENT.personId
			and award.degree = acadTrackENT.degree
			and award.college = acadTrackENT.college
			and award.degreeLevel = acadTrackENT.academicTrackLevel
			and acadTrackENT.fieldOfStudyType = 'Major'
			and acadTrackENT.curriculumStatus = 'Completed'
			and acadTrackENT.isIpedsReportable = 1
-- Remove for testing...
			and ((acadTrackENT.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
				and acadTrackENT.recordActivityDate <= award.awardedDate)
				or acadTrackENT.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
		inner join AcademicTermMCR acadterm
			on acadterm.termCode = acadTrackENT.termCodeEffective
	where award.termOrder >= acadterm.termOrder
	)
where acadTrackRn = 1
),

MajorMCR as (
-- Pulls most relevant major information as of the date the reported student received their award.

select * 
from (
    select acadtrack.personId personId,
        majorENT.*, 
        row_number() over (
			partition by
				acadtrack.personId,
				majorENT.major,
				majorENT.cipCode
			order by
				majorENT.recordActivityDate desc
		) as majorRn
    from Major majorENT
		inner join AcademicTrackMCR acadtrack
			on acadtrack.major = majorENT.major
			and majorENT.isIpedsReportable = 1 --true
-- Remove for testing...
			and ((majorENT.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
				and majorENT.recordActivityDate <= acadtrack.awardedDate)
					or majorENT.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
    )
where majorRn = 1
),

CourseSectionMCR as (
--Included to get enrollment hours of a CRN

select distinct 
	reg2.cohortInd cohortInd,
	CourseSect.crn crn,
	CourseSect.section section,
	CourseSect.subject subject,
	CourseSect.courseNumber courseNumber,
	reg2.termOrder termOrder,
	CourseSect.termCode termCode,
	CourseSect.partOfTermCode partOfTermCode,
	reg2.crnLevel crnLevel,
	reg2.censusDate censusDate,
	reg2.equivCRHRFactor equivCRHRFactor,
	CourseSect.recordActivityDate recordActivityDate,
	CAST(CourseSect.enrollmentHours as DECIMAL(2,0)) enrollmentHours,
	CourseSect.isClockHours isClockHours, 
	reg2.instructionalActivityType instructionalActivityType
from RegistrationMCR reg2 
left join (
	select distinct coursesectENT.termCode termCode,
		coursesectENT.partOfTermCode partOfTermCode,
		coursesectENT.crn crn,
		coursesectENT.section section,
		coursesectENT.subject subject,
		coursesectENT.courseNumber courseNumber,
		coursesectENT.recordActivityDate recordActivityDate,
		CAST(coursesectENT.enrollmentHours as decimal(2,0)) enrollmentHours,
		coursesectENT.isClockHours isClockHours,
		row_number() over (
			partition by
				coursesectENT.subject,
				coursesectENT.courseNumber,
				coursesectENT.crn,
				coursesectENT.section,
				coursesectENT.termCode,
				coursesectENT.partOfTermCode
			order by
				coursesectENT.recordActivityDate desc
		) courseRn
	from RegistrationMCR reg 
		inner join CourseSection coursesectENT on coursesectENT.termCode = reg.termCode
			and coursesectENT.partOfTermCode = reg.partOfTermCode
			and coursesectENT.crn = reg.crn
			and coursesectENT.sectionStatus = 'Active'
			and coursesectENT.isIpedsReportable = 1 
			and ((coursesectENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and coursesectENT.recordActivityDate <= reg.censusDate)
					or coursesectENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
) CourseSect 
	on reg2.termCode = CourseSect.termCode
	and reg2.partOfTermCode = CourseSect.partOfTermCode
    and reg2.crn = CourseSect.crn 
where CourseSect.courseRn = 1
), 

CourseSectionScheduleMCR as (
--Returns course scheduling related info for the registration CRN.
--AcademicTerm.partOfTermCode, CourseSectionSchedule.partOfTermCode & AcademicTerm.censusDate together are used to define the period 
--of valid course registration attempts. 

	select coursesect2.cohortInd cohortInd,
		coursesect2.censusDate censusDate,
		CourseSched.crn crn,
		CourseSched.section section,
		CourseSched.termCode termCode,
		coursesect2.termOrder termOrder, 
		CourseSched.partOfTermCode partOfTermCode,
		coursesect2.equivCRHRFactor equivCRHRFactor,
		coursesect2.subject subject,
		coursesect2.courseNumber courseNumber,
		coursesect2.crnLevel crnLevel,
		NVL(CourseSched.meetingType, 'Standard') meetingType,
		coursesect2.enrollmentHours enrollmentHours,
		coursesect2.isClockHours isClockHours,
		coursesect2.instructionalActivityType instructionalActivityType
	from CourseSectionMCR coursesect2
		left join (
			select courseSectSchedENT.termCode termCode,
				courseSectSchedENT.crn crn,
				courseSectSchedENT.section section,
				courseSectSchedENT.partOfTermCode partOfTermCode,
				nvl(courseSectSchedENT.meetingType, 'Standard') meetingType,
				row_number() over (
					partition by
						courseSectSchedENT.crn,
						courseSectSchedENT.section,
						courseSectSchedENT.termCode,
						courseSectSchedENT.partOfTermCode
					order by
						courseSectSchedENT.recordActivityDate desc
				) courseSectSchedRn 
			from CourseSectionMCR coursesect
				inner join CourseSectionSchedule courseSectSchedENT on courseSectSchedENT.termCode = coursesect.termCode
					and courseSectSchedENT.partOfTermCode = coursesect.partOfTermCode
					and courseSectSchedENT.crn = coursesect.crn
					and (courseSectSchedENT.section = coursesect.section or coursesect.section is null)
					and courseSectSchedENT.isIpedsReportable = 1  
					and ((courseSectSchedENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
						and courseSectSchedENT.recordActivityDate <= coursesect.censusDate)
							or courseSectSchedENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
			) CourseSched 
			on CourseSched.termCode = coursesect2.termCode
			and CourseSched.partOfTermCode = coursesect2.partOfTermCode
			and CourseSched.crn = coursesect2.crn
			and (courseSched.section = coursesect2.section or coursesect2.section is null)
			and CourseSched.courseSectSchedRn = 1		 
), 

CourseMCR as (
--Included to get course type information
-- ak 20200707 Bug fixes in CourseMCR, CourseTypeCountsSTU, PersonMCR and mods to support changes to these views

select *
from (
	select distinct coursesectsched.cohortInd cohortInd,
--		coursesectsched.crn crn,
--		coursesectsched.section section,
		courseENT.subject subject,
		courseENT.courseNumber courseNumber,
--		coursesectsched.termOrder,
--		coursesectsched.termCode termCode,
--		coursesectsched.partOfTermCode partOfTermCode,
--		coursesectsched.censusDate censusDate,
		courseENT.courseLevel courseLevel,
		courseENT.isRemedial isRemedial,
		courseENT.isESL isESL,
--		coursesectsched.meetingType meetingType,
--		NVL(coursesectsched.enrollmentHours, 0) enrollmentHours,
--		coursesectsched.isClockHours isClockHours,
--        coursesectsched.equivCRHRFactor equivCRHRFactor,
--        coursesectsched.instructionalActivityType instructionalActivityType,
		row_number() over (
			partition by
				courseENT.subject,
				courseENT.courseNumber,
				courseENT.courseLevel
			order by
				courseENT.termCodeEffective desc,
				courseENT.recordActivityDate desc
		) courseRn
	from CourseSectionScheduleMCR coursesectsched
		left join Course courseENT ON courseENT.subject = coursesectsched.subject
			and courseENT.courseNumber = coursesectsched.courseNumber
			and courseENT.courseLevel = coursesectsched.crnLevel
		inner join AcadTermOrder termorder on termorder.termCode = courseENT.termCodeEffective
			and courseENT.isIpedsReportable = 1 --true
			and courseENT.courseStatus = 'Active'
			and ((courseENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and courseENT.recordActivityDate <= coursesectsched.censusDate)
					or courseENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	where termorder.termOrder <= coursesectsched.termOrder
	)
where courseRn = 1
),

FinancialAidMCR as (
-- included to get student Financial aid information paid any time during the academic year.

select *
from (
    select FinancialAidENT.*,
        row_number() over (
            partition by
                FinancialAidENT.personId,
                FinancialAidENT.termCode,
			    FinancialAidENT.fundCode
		    order by
			    FinancialAidENT.recordActivityDate desc
	    ) financialaidRn
    from RegistrationMCR reg 
	    inner join FinancialAid FinancialAidENT ON FinancialAidENT.personId = reg.personId
	        and FinancialAidENT.financialAidYear = reg.financialAidYear
            and NVL(FinancialAidENT.IPEDSFinancialAidAmount, 0) > 0
		    and FinancialAidENT.isIpedsReportable = 1
		    and ((FinancialAidENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
			    and FinancialAidENT.recordActivityDate <= reg.censusDate)
				    or FinancialAidENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where financialaidRn = 1
),

/*****
BEGIN SECTION - Transformations
This set of views is used to transform, aggregate, etc. records from MCR views above.
*****/

CourseTypeCountsSTU as (
-- View used to break down course category type counts by type

select cohortInd cohortInd,
    personId personId,
    termCode termCode,
    partOfTermCode partOfTermCode,
	SUM(totalCourses) totalCourses,
	SUM(totalCreditHrs) totalCreditHrs,
	SUM(totalClockHrs) totalClockHrs,
	SUM(totalNonCredCourses) totalNonCredCourses,
	SUM(totalCredCourses) totalCredCourses,
	SUM(totalDECourses) totalDECourses,
	SUM(totalUGCourses) totalUGCourses,
	SUM(totalGRCourses) totalGRCourses,
	SUM(totalPostGRCourses) totalPostGRCourses,
	SUM(totalCECourses) totalCECourses,
	SUM(totalESLCourses) totalESLCourses,
	SUM(totalRemCourses) totalRemCourses,
	SUM(totalIntlCourses) totalIntlCourses,
	SUM(totalAuditCourses) totalAuditCourses
from (
	select reg.cohortInd cohortInd,
		reg.personId personId,
		reg.termCode termCode,
		reg.partOfTermCode partOfTermCode,
		reg.crn crn,
		case when coursesectsched.enrollmentHours >= 0 then 1 else 0 end totalCourses,
		case when reg.instructionalActivityType = 'CR' and coursesectsched.enrollmentHours >= 0 then coursesectsched.enrollmentHours 
			when reg.instructionalActivityType = 'B' and coursesectsched.isClockHours = 0 then coursesectsched.enrollmentHours 
            when reg.instructionalActivityType = 'B' and coursesectsched.isClockHours = 1 
                then coursesectsched.equivCRHRFactor * coursesectsched.enrollmentHours
            else 0 
        end totalCreditHrs,
		case when reg.instructionalActivityType = ('CL') and coursesectsched.enrollmentHours >= 0 then coursesectsched.enrollmentHours 
					else 0 end totalClockHrs,
		case when coursesectsched.enrollmentHours = 0 then 1 else 0 end totalNonCredCourses,
		case when coursesectsched.enrollmentHours > 0 then 1 else 0 end totalCredCourses,
		case when coursesectsched.meetingType = 'Online/Distance Learning' then 1 else 0 end totalDECourses,
		case when course.courseLevel = 'Undergrad' then 1 else 0 end totalUGCourses,
		case when course.courseLevel in ('Graduate', 'Professional') then 1 else 0 end totalGRCourses,
		case when course.courseLevel = 'Postgraduate' then 1 else 0 end totalPostGRCourses,
		case when course.courseLevel = 'Continuing Ed' then 1 else 0 end totalCECourses,
		case when course.isESL = 'Y' then 1 else 0 end totalESLCourses,
		case when course.isRemedial = 'Y' then 1 else 0 end totalRemCourses,
		case when reg.isInternational = 1 then 1 else 0 end totalIntlCourses,
		case when reg.crnGradingMode = 'Audit' then 1 else 0 end totalAuditCourses
	from RegistrationMCR reg
		left join CourseSectionScheduleMCR coursesectsched on reg.termCode = coursesectsched.termCode
			and reg.partOfTermCode = coursesectsched.partOfTermCode
			and reg.crn = coursesectsched.crn 
			and reg.crnLevel = coursesectsched.crnLevel
		left join CourseMCR course on coursesectsched.subject = course.subject
			and coursesectsched.courseNumber = course.courseNumber
			and coursesectsched.crnLevel = course.courseLevel
	)
group by cohortInd,
	personId,
	termCode,
	partOfTermCode
--order by reg.cohortInd,
--		personId,
--		termCode
),

FinancialAidCountsSTU as (

select personId personId,

    ROUND(SUM(federalLoan), 0) federalLoan,
    ROUND(SUM(federalGrantSchol), 0) federalGrantSchol,
    ROUND(SUM(federalWorkStudy), 0) federalWorkStudy,
    
    ROUND(SUM(stateLocalLoan), 0) stateLocalLoan,
    ROUND(SUM(stateLocalGrantSchol), 0) stateLocalGrantSchol,
    ROUND(SUM(stateLocalWorkStudy), 0) stateLocalWorkStudy,

    ROUND(SUM(institutionLoan), 0) institutionLoan,
    ROUND(SUM(institutionGrantSchol), 0) institutionGrantSchol,
    ROUND(SUM(institutionalWorkStudy), 0) institutionalWorkStudy,

    ROUND(SUM(otherLoan), 0) otherLoan,
    ROUND(SUM(otherGrantSchol), 0) otherGrantSchol,
    ROUND(SUM(otherWorkStudy), 0) otherWorkStudy,
    
    ROUND(SUM(pellGrant), 0) pellGrant,
    ROUND(SUM(titleIV), 0) titleIV,
    ROUND(SUM(allGrantSchol), 0) allGrantSchol,
    ROUND(SUM(IPEDSFinancialAidAmount), 0) totalAid
    
from (
    select financialaid.personId personId,
        case when financialaid.fundType = 'Loan' and financialaid.fundSource = 'Federal' then financialaid.IPEDSFinancialAidAmount else 0 end federalLoan,
        case when financialaid.fundType in ('Grant', 'Scholarship') and financialaid.fundSource = 'Federal' then financialaid.IPEDSFinancialAidAmount else 0 end federalGrantSchol,
        case when financialaid.fundType = 'Work Study' and financialaid.fundSource = 'Federal' then financialaid.IPEDSFinancialAidAmount else 0 end federalWorkStudy,
    
        case when financialaid.fundType = 'Loan' and financialaid.fundSource in ('State', 'Local') then financialaid.IPEDSFinancialAidAmount else 0 end stateLocalLoan,
        case when financialaid.fundType in ('Grant', 'Scholarship') and financialaid.fundSource in ('State', 'Local') then financialaid.IPEDSFinancialAidAmount else 0 end stateLocalGrantSchol,
        case when financialaid.fundType = 'Work Study' and financialaid.fundSource in ('State', 'Local') then financialaid.IPEDSFinancialAidAmount else 0 end stateLocalWorkStudy,

        case when financialaid.fundType = 'Loan' and financialaid.fundSource = 'Institution' then financialaid.IPEDSFinancialAidAmount else 0 end institutionLoan,
        case when financialaid.fundType in ('Grant', 'Scholarship') and financialaid.fundSource = 'Institution' then financialaid.IPEDSFinancialAidAmount else 0 end institutionGrantSchol,
        case when financialaid.fundType = 'Work Study' and financialaid.fundSource = 'Institution' then financialaid.IPEDSFinancialAidAmount else 0 end institutionalWorkStudy,

        case when financialaid.fundType = 'Loan' and financialaid.fundSource = 'Other' then financialaid.IPEDSFinancialAidAmount else 0 end otherLoan,
        case when financialaid.fundType in ('Grant', 'Scholarship') and financialaid.fundSource = 'Other' then financialaid.IPEDSFinancialAidAmount else 0 end otherGrantSchol,
        case when financialaid.fundType = 'Work Study' and financialaid.fundSource = 'Other' then financialaid.IPEDSFinancialAidAmount else 0 end otherWorkStudy,
        
        case when financialaid.isPellGrant = 1 then financialaid.IPEDSFinancialAidAmount else 0 end pellGrant,
        case when financialaid.isTitleIV = 1 then financialaid.IPEDSFinancialAidAmount else 0 end titleIV,
        case when financialaid.fundType in ('Grant', 'Scholarship') then financialaid.IPEDSFinancialAidAmount else 0 end allGrantSchol,
        financialaid.IPEDSFinancialAidAmount IPEDSFinancialAidAmount
    
    from FinancialAidMCR financialaid
    )
group by personId
),

StudentTypeRefactor as (
--View used to determine reporting year IPEDS defined student type using prior summer considerations
--Student is considered 'First Time' if they enrolled for the first time in the reporting fall term or 
--if they enrolled for the first time in the prior summer and continued on to also take fall courses. 

select student.personId personId
from ClientConfigMCR clientconfig
	cross join StudentMCR student 
where student.cohortInd = clientconfig.sectionSummer 
	and student.studentType = 'First Time'
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

CohortSTU as (
--View used to build the reportable 'Fall' student cohort. (CohortInd = 'Fall)
--An indicator 'IPEDSinclude' is built out in this view to indicate if a student cohort record meets IPEDS reporting 
--specs to be included. 

select distinct CohView.*,
	case when CohView.studentLevel = 'Undergraduate' then 
-- ak 20200707 Adding additional client config values for level offereings and instructor activity type (PF-1552)
		(case when CohView.instructionalActivityType = 'CL' 
		            and CohView.totalClockHrs >= CohView.requiredFTClockHoursUG then 'Full' 
			 when CohView.totalCreditHrs >= CohView.requiredFTCreditHoursUG then 'Full' 
			 else 'Part' end)
		when CohView.studentLevel = 'Graduate' then 
			(case when CohView.totalCreditHrs >= CohView.requiredFTCreditHoursGR then 'Full' else 'Part' end)
		else 'Part' 
	end timeStatus
from (	
	select distinct reg.cohortInd cohortInd,
		reg.personId personId,
		reg.termCode termCode,
		reg.censusDate censusDate,
		reg.instructionalActivityType instructionalActivityType, 
		clientconfig.acadOrProgReporter acadOrProgReporter,
		--clientconfig.reportResidency reportResidency,
		--clientconfig.reportAge reportAge,
		clientconfig.genderForNonBinary genderForNonBinary,
		clientconfig.genderForUnknown genderForUnknown,
		--major.cipCode cipCode,
		--major.major major,
		--AcademicTrackCurrent.curriculumCode degree,
		student.isNonDegreeSeeking isNonDegreeSeeking,
		student.residency residency,
		--student.highSchoolGradDate hsGradDate,
		NVL(acadterm.requiredFTCreditHoursUG, 12) requiredFTCreditHoursUG,
		NVL(acadterm.requiredFTClockHoursUG, 24) requiredFTClockHoursUG,
		NVL(acadterm.requiredFTCreditHoursGR, 9) requiredFTCreditHoursGR,
		person.gender gender,
		person.isInUSOnVisa isInUSOnVisa,
		person.visaStartDate visaStartDate,
		person.visaEndDate visaEndDate,
		person.isUSCitizen isUSCitizen,
		person.isHispanic isHispanic,
		person.isMultipleRaces isMultipleRaces,
		person.ethnicity ethnicity,
		--FLOOR(DATEDIFF(TO_DATE(reg.censusDate), person.birthDate) / 365) asOfAge,
		person.nation nation,
		person.state state,
		--courseSch.meetingType meetingType,
		coursecnt.totalCreditHrs totalCreditHrs,
		coursecnt.totalClockHrs totalClockHrs,
		coursecnt.totalCourses totalcourses,
		coursecnt.totalDEcourses totalDEcourses,
		coursecnt.totalUGCourses totalUGCourses,
		coursecnt.totalGRCourses totalGRCourses,
		coursecnt.totalPostGRCourses totalPostGRCourses,
		coursecnt.totalNonCredCourses totalNonCredCourses,
		coursecnt.totalESLCourses totalESLCourses,
		coursecnt.totalRemCourses totalRemCourses,
		case when student.studentType = 'First Time' then 'First Time'
			when student.personId = studtype.personId then 'First Time'
			else student.studentType
		end studentType,
		case when student.studentLevel = 'Continuing Ed' and clientconfig.includeNonDegreeAsUG = 'Y' then 'UnderGrad' 
			when student.studentLevel in ('Professional', 'Postgraduate') then 'Graduate' 
			else student.studentLevel 
		end studentLevel,
		case 
			when coursecnt.totalRemCourses = coursecnt.totalCourses and student.isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
			when coursecnt.totalCredCourses > 0 --exclude students not enrolled for credit
			then (case when coursecnt.totalESLCourses = coursecnt.totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
						when coursecnt.totalCECourses = coursecnt.totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
						when coursecnt.totalIntlCourses = coursecnt.totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
						when coursecnt.totalAuditCourses = coursecnt.totalCredCourses then 0 --exclude students exclusively auditing classes
						-- when... then 0 --exclude PHD residents or interns
						-- when... then 0 --students studying abroad if enrollment at home institution is an admin only record
						-- when... then 0 --exclude students in experimental Pell programs
						else 1
					end)
			else 0 
		end ipedsInclude,
		financialaid.fundType fundType,
		--financialaid.fundCode fundCode,
		financialaid.fundSource fundsource,
		financialaid.isPellGrant isPellGrant,
		financialaid.isTitleIV isTitleIV,
		
        finaidcounts.federalLoan federalLoan,
        finaidcounts.federalGrantSchol federalGrantSchol,
        finaidcounts.federalWorkStudy federalWorkStudy,
        finaidcounts.stateLocalLoan stateLocalLoan,
        finaidcounts.stateLocalGrantSchol stateLocalGrantSchol,
        finaidcounts.stateLocalWorkStudy stateLocalWorkStudy,
        finaidcounts.institutionLoan institutionLoan,
        finaidcounts.institutionGrantSchol institutionGrantSchol,
        finaidcounts.institutionalWorkStudy institutionalWorkStudy,
        finaidcounts.otherLoan otherLoan,
        finaidcounts.otherGrantSchol otherGrantSchol,
        finaidcounts.otherWorkStudy otherWorkStudy,
        finaidcounts.pellGrant pellGrant,
        finaidcounts.titleIV titleIV,
        finaidcounts.allGrantSchol allGrantSchol,
        finaidcounts.totalAid totalAid,
        
		financialaid.isSubsidizedDirectLoan isSubsidizedDirectLoan,
		financialaid.IPEDSFinancialAidAmount IPEDSFinancialAidAmount,
		financialaid.familyIncome familyIncome,
		financialaid.livingArrangement livingArrangement 
		
		from ClientConfigMCR clientconfig
			inner join RegistrationMCR reg on reg.cohortInd = clientconfig.sectionFall 
			inner join PersonMCR person on reg.personId = person.personId
				and person.cohortInd = reg.cohortInd
			inner join StudentMCR student on reg.personId = student.personId
				and student.cohortInd = reg.cohortInd
			inner join AcademicTermMCR acadterm on acadterm.termCode = reg.termCode
			left join CourseTypeCountsSTU coursecnt on reg.personId = coursecnt.personId --5299
				and coursecnt.cohortInd = reg.cohortInd
			left join StudentTypeRefactor studtype on studtype.personId = reg.personId
			left join FinancialAidMCR financialaid on financialaid.personId = reg.personId
			left join FinancialAidCountsSTU finaidcounts on finaidcounts.personId = reg.personId
		where student.studentStatus = 'Active'
	) as CohView
),

CohortRefactorSTU as (
-- This view is used to refactor the 'StudentCohort' view records in terms of IPEDS reportable values

select distinct cohortstu.personId personId,
	cohortstu.cohortInd cohortInd,
--	cohortstu.censusDate censusDate,
	cohortstu.acadOrProgReporter acadOrProgReporter,
--	cohortstu.studentType studentType,
--	cohortstu.ipedsInclude ipedsInclude,
	cohortstu.isNonDegreeSeeking isNonDegreeSeeking,
	cohortstu.studentLevel studentLevel,
    cohortstu.federalLoan federalLoan,
    cohortstu.federalGrantSchol federalGrantSchol,
    cohortstu.federalWorkStudy federalWorkStudy,
    cohortstu.stateLocalLoan stateLocalLoan,
    cohortstu.stateLocalGrantSchol stateLocalGrantSchol,
    cohortstu.stateLocalWorkStudy stateLocalWorkStudy,
    cohortstu.institutionLoan institutionLoan,
    cohortstu.institutionGrantSchol institutionGrantSchol,
    cohortstu.institutionalWorkStudy institutionalWorkStudy,
    cohortstu.otherLoan otherLoan,
    cohortstu.otherGrantSchol otherGrantSchol,
    cohortstu.otherWorkStudy otherWorkStudy,
    cohortstu.pellGrant pellGrant,
    cohortstu.titleIV titleIV,
    cohortstu.allGrantSchol allGrantSchol,
    cohortstu.totalAid totalAid,
    
	case when cohortstu.studentLevel = 'Undergrad' then 1 end group1, --All undergrad
	case when cohortstu.studentLevel = 'Undergrad' 
	        and cohortstu.timeStatus = 'Full'
	        and cohortstu.studentType = 'First Time'
	        and cohortstu.isNonDegreeSeeking is null then 1 end group2, -- Group 1,  full-time, first-time degree/certificate-seeking students (FTFT)
	case when cohortstu.studentLevel = 'Undergrad' 
	        and cohortstu.timeStatus = 'Full'
	        and cohortstu.studentType = 'First Time'
	        and cohortstu.isNonDegreeSeeking is null 
            and (cohortstu.federalLoan > 0
                or cohortstu.federalGrantSchol > 0
                or cohortstu.federalWorkStudy > 0
                or cohortstu.stateLocalLoan > 0
                or cohortstu.stateLocalGrantSchol > 0
                or cohortstu.stateLocalWorkStudy > 0
                or cohortstu.institutionLoan > 0
                or cohortstu.institutionGrantSchol > 0
                or cohortstu.institutionalWorkStudy > 0
                or cohortstu.otherLoan > 0
                or cohortstu.otherGrantSchol > 0
                or cohortstu.otherWorkStudy > 0) 
        then 1 end group2a, -- Group 2, awarded Work Study, loans, grant/scholarship aid from the federal government, state/local government, the institution, or other sources.
	case when cohortstu.studentLevel = 'Undergrad' 
	        and cohortstu.timeStatus = 'Full'
	        and cohortstu.studentType = 'First Time'
	        and cohortstu.isNonDegreeSeeking is null 
            and (cohortstu.federalLoan > 0
                or cohortstu.federalGrantSchol > 0
                or cohortstu.stateLocalLoan > 0
                or cohortstu.stateLocalGrantSchol > 0
                or cohortstu.institutionLoan > 0
                or cohortstu.institutionGrantSchol > 0)
        then 1 end group2b, -- Group 2, awarded loans, grant/scholarship aid from the federal government, state/local government or the institution.
	case when cohortstu.studentLevel = 'Undergrad' 
	        and cohortstu.timeStatus = 'Full'
	        and cohortstu.studentType = 'First Time'
	        and cohortstu.isNonDegreeSeeking is null 
	        and cohortstu.residency in ('In District', 'In State')  --In district/In state tuition
            and (cohortstu.federalLoan > 0
                or cohortstu.federalGrantSchol > 0
                or cohortstu.stateLocalGrantSchol > 0
                or cohortstu.institutionGrantSchol > 0)
        then 1 end group3, -- Group 2, paying the in-state/in-district tuition rate and awarded federal government, state/local government, or the institution grant/scholarship aid.
	case when cohortstu.studentLevel = 'Undergrad' 
	        and cohortstu.timeStatus = 'Full'
	        and cohortstu.studentType = 'First Time'
	        and cohortstu.isNonDegreeSeeking is null 
	        and cohortstu.residency in ('In District', 'In State')  --In district/In state tuition
            and cohortstu.titleIV > 0
        then 1 end group4, --Group 2, paying the in-state/in-district tuition rate and awarded federal Title IV.
	
	case when cohortstu.gender = 'Male' then 'M'
		when cohortstu.gender = 'Female' then 'F'
		when cohortstu.gender = 'Non-Binary' then cohortstu.genderForNonBinary
		else cohortstu.genderForUnknown
	end ipedsGender,
	case when cohortstu.isUSCitizen = 1 then 
		(case when cohortstu.isHispanic = true then 2 -- 'hispanic/latino'
			when cohortstu.isMultipleRaces = true then 8 -- 'two or more races'
			when cohortstu.ethnicity != 'Unknown' and cohortstu.ethnicity is not null
			then (case when cohortstu.ethnicity = 'Hispanic or Latino' then 2
					when cohortstu.ethnicity = 'American Indian or Alaskan Native' then 3
					when cohortstu.ethnicity = 'Asian' then 4
					when cohortstu.ethnicity = 'Black or African American' then 5
					when cohortstu.ethnicity = 'Native Hawaiian or Other Pacific Islander' then 6
					when cohortstu.ethnicity = 'Caucasian' then 7
					else 9 end) 
			    else 9 end) -- 'race and ethnicity unknown'
            else (case when cohortstu.isInUSOnVisa = 1 
						and cohortstu.censusDate between cohortstu.visaStartDate and cohortstu.visaEndDate then '1' -- 'nonresident alien'
		else 9 end) -- 'race and ethnicity unknown'
	end ipedsEthnicity,
	cohortstu.nation,
	case when cohortstu.nation = 'US' then 1 else 0 end isInAmerica,
	cohortstu.totalcourses totalCourses,
	cohortstu.totalDECourses,
	case when cohortstu.totalDECourses > 0 and cohortstu.totalDECourses < cohortstu.totalCourses then 'Some DE'
		when cohortstu.totalDECourses = cohortstu.totalCourses then 'Exclusive DE'
	end distanceEdInd
from CohortSTU cohortstu
where cohortstu.ipedsInclude = 1
	and cohortstu.studentLevel in ('Undergrad', 'Graduate') --filters out CE when client does not want to count CE taking a credit course as Undergrad
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
/*
select 'A' PART,
       SUM(case when cohortrefactor.acadOrProgReporter = 'A' then 1 end) FIELD2_1, --Public and private academic reporters - Count of Group 1
       SUM(case when cohortrefactor.acadOrProgReporter = 'P' then 1 end) FIELD3_1, --Program reporters - Count of unduplicated Group 1
       SUM(case when cohortrefactor.totalAid > 0 then 1 else 0 end) FIELD4_1, --Count of Group 1 with awarded aid or work study from any source
       SUM(case when cohortrefactor.pellGrant > 0  then 1 else 0 end) FIELD5_1, --Count of Group 1 with awarded PELL grants
       SUM(case when cohortrefactor.federalLoan > 0  then 1 else 0 end) FIELD6_1, --Count of Group 1 with awarded and accepted federal loans
       ROUND(SUM(case when cohortrefactor.allGrantSchol > 0  then cohortrefactor.allGrantSchol else 0 end)) FIELD7_1, --Total grant aid amount awarded to Group 1 from all sources
       ROUND(SUM(case when cohortrefactor.pellGrant > 0  then cohortrefactor.pellGrant else 0 end)) FIELD8_1, --Total PELL grant amount awarded to Group 1
       ROUND(SUM(case when cohortrefactor.federalLoan > 0  then cohortrefactor.federalLoan else 0 end)) FIELD9_1, --Total federal loan amount awarded and accepted by Group 1
       null FIELD10_1,
       null FIELD11_1,
       null FIELD12_1,
       null FIELD13_1,
       null FIELD14_1,
       null FIELD15_1,
       null FIELD16_1
from CohortRefactorSTU cohortrefactor
where cohortrefactor.group1 = 1

union

--Part B Full-time, first-time undergraduate counts
--Group 2 -> full-time, first-time degree/certificate-seeking
--Group 2a -> Work study, grant or scholarship aid from the federal, state/local govt or institution, loan from all sources;
--            Do not include grant or scholarship aid from private or other sources
--Group 2b -> awarded any loans or grants or scholarship from federal, state, local or institution

--Valid values
--Number of students: 0 to 999999, -2 or blank = not-applicable

select 'B' PART,
       SUM(case when :Configuration_CB_Rep_Acad = 1 then 1 end) FIELD2_1, --Public and private academic reporters - Count of Group 2
       SUM(case when :Configuration_CB_Rep_Public = 1 then case when AllEnrolled.Residency = 'INDIS' then 1 else 0 end end) FIELD3_1, --Public reporters - Count of Group 2 paying in-district tuition rates
       SUM(case when :Configuration_CB_Rep_Public = 1 then case when AllEnrolled.Residency = 'INST' then 1 else 0 end end) FIELD4_1, --Public reporters - Count of Group 2 paying in-state tuition rates
       SUM(case when :Configuration_CB_Rep_Public = 1 then case when AllEnrolled.Residency = 'OUTST' then 1 else 0 end end) FIELD5_1, --Public reporters - Count of Group 2 paying out-of-state tuition rates
       SUM(case when :Configuration_CB_Rep_Prog = 1 then 1 end) FIELD6_1, --Program reporters - Count of unduplicated Group 2
       SUM(case when (AllEnrolled.WorkStudy + AllEnrolled.AllLoan + AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL + AllEnrolled.StateGrant + AllEnrolled.InstGrant + AllEnrolled.FedSchol + AllEnrolled.StateSchol + AllEnrolled.InstSchol) > 0 then 1 else 0 end) FIELD7_1, --Count of Group 2a
       SUM(case when (AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL + AllEnrolled.StateGrant + AllEnrolled.InstGrant + AllEnrolled.FedSchol + AllEnrolled.StateSchol + AllEnrolled.InstSchol + AllEnrolled.FedLoan + AllEnrolled.StateLoan + AllEnrolled.InstLoan) > 0 then 1 else 0 end) FIELD8_1, --Count of Group 2b
       null FIELD9_1,
       null FIELD10_1,
       null FIELD11_1,
       null FIELD12_1,
       null FIELD13_1,
       null FIELD14_1,
       null FIELD15_1,
       null FIELD16_1
from AllEnrolled AllEnrolled
where AllEnrolled.FTPT = 'Full'
and AllEnrolled.FirstTime = 'FT'

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
       SUM(case when (AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL + AllEnrolled.StateGrant + AllEnrolled.InstGrant + AllEnrolled.FedSchol + AllEnrolled.StateSchol + AllEnrolled.InstSchol) > 0 then 1 else 0 end) FIELD2_1, --Count of Group 2 awarded grant or schol aid from fed, state/local govt or institution
       SUM(case when (AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL) > 0 then 1 end) FIELD3_1, --Count of Group 2 awarded federal grants
       SUM(case when AllEnrolled.PELL > 0 then 1 else 0 end) FIELD4_1, --Count of Group 2 awarded PELL grants
       SUM(case when AllEnrolled.FedGrantNoPELL > 0  then 1 else 0 end) FIELD5_1, --Count of Group 2 awarded other federal grants
       SUM(case when AllEnrolled.StateGrant > 0  then 1 else 0 end) FIELD6_1, --Count of Group 2 awarded state/local govt grants
       SUM(case when AllEnrolled.InstGrant > 0  then 1 else 0 end) FIELD7_1, --Count of Group 2 awarded institutional grants
       SUM(case when AllEnrolled.AllLoan > 0  then 1 else 0 end) FIELD8_1, --Count of Group 2 awarded and accepted loans from all sources
       SUM(case when AllEnrolled.FedLoan > 0  then 1 else 0 end) FIELD9_1, --Count of Group 2 awarded and accepted federal loans
       SUM(case when (AllEnrolled.AllLoan - AllEnrolled.FedLoan) > 0 then 1 else 0 end) FIELD10_1, --Count of Group 2 awarded and accepted other loans to students (including private loans)
       ROUND(SUM(case when AllEnrolled.PELL > 0 then AllEnrolled.PELL end)) FIELD11_1, --Total PELL grant amount awarded to Group 2
       ROUND(SUM(case when AllEnrolled.FedGrantNoPELL > 0 then AllEnrolled.FedGrantNoPELL end)) FIELD12_1, --Total other federal grant amount awarded to Group 2
       ROUND(SUM(case when AllEnrolled.StateGrant > 0 then AllEnrolled.StateGrant end)) FIELD13_1, --Total state/local govt grant amount awarded to Group 2
       ROUND(SUM(case when AllEnrolled.InstGrant > 0 then AllEnrolled.InstGrant end)) FIELD14_1, --Total inst grant amount awarded to Group 2
       ROUND(SUM(case when AllEnrolled.FedLoan > 0 then AllEnrolled.FedLoan end)) FIELD15_1, --Total federal loan amount awarded and accepted by Group 2
       ROUND(SUM(case when (AllEnrolled.AllLoan - AllEnrolled.FedLoan) > 0 then (AllEnrolled.AllLoan - AllEnrolled.FedLoan) end)) FIELD16_1  --Total other loan amount awarded and accepted by Group 2
from AllEnrolled AllEnrolled
where AllEnrolled.FTPT = 'Full'
and AllEnrolled.FirstTime = 'FT'

union

--Part D Full-time, first-time who were awarded aid
--Group 3 -> Awarded grant or scholarship aid from federal, state, local govt or the institution;
--           enrolled in the largest program for program reporters

--Valid values
--Number of students: 0-999999
--Total amount of aid: 0-999999999999
--       For public institutions, include only those students paying the in-state or in-district tuition rate.
--       For program reporters, include only those students enrolled in the institution’s largest program.

select 'D' PART,
       SUM(case when (AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL + AllEnrolled.StateGrant + AllEnrolled.InstGrant + AllEnrolled.FedSchol + AllEnrolled.StateSchol + AllEnrolled.InstSchol) > 0 then 1 else 0 end) FIELD2_1, --Count of Group 3 current year
       SUM(case when :Parameters_CB_PriorYear = 1 then :Global_SQL_PriorYearTotals.D3 end) FIELD3_1, --Count of Group 3 prior year
       SUM(case when :Parameters_CB_Prior2Year = 1 then :Global_SQL_Prior2YearTotals.D4 end) FIELD4_1, --Count of Group 3 prior2 year
       SUM(case when AllEnrolled.LivArrgmt = 'ONCAMP' then 1 else 0 end) FIELD5_1, --Count of Group 3 current year living on campus
       SUM(case when :Parameters_CB_PriorYear = 1 then :Global_SQL_PriorYearTotals.D6 end) FIELD6_1, --Count of Group 3 prior year living on campus
       SUM(case when :Parameters_CB_Prior2Year = 1 then :Global_SQL_Prior2YearTotals.D7 end) FIELD7_1, --Count of Group 3 prior2 year living on campus
       SUM(case when AllEnrolled.LivArrgmt = 'OFF_CAMP_FAM' then 1 else 0 end) FIELD8_1, --Count of Group 3 current year living off campus with family
       SUM(case when :Parameters_CB_PriorYear = 1 then :Global_SQL_PriorYearTotals.D9 end) FIELD9_1, --Count of Group 3 prior year living off campus with family
       SUM(case when :Parameters_CB_Prior2Year = 1 then :Global_SQL_Prior2YearTotals.D10 end) FIELD10_1, --Count of Group 3 prior2 year living off campus with family
       SUM(case when AllEnrolled.LivArrgmt = 'OFFCAMP' then 1 else 0 end) FIELD11_1, --Count of Group 3 current year living off campus not with family
       SUM(case when :Parameters_CB_PriorYear = 1 then :Global_SQL_PriorYearTotals.D12 end) FIELD12_1, --Count of Group 3 prior year living off campus not with family
       SUM(case when :Parameters_CB_Prior2Year = 1 then :Global_SQL_Prior2YearTotals.D13 end) FIELD13_1, --Count of Group 3 prior2 year living off campus not with family
       ROUND(SUM(case when (AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL + AllEnrolled.StateGrant + AllEnrolled.InstGrant + AllEnrolled.FedSchol + AllEnrolled.StateSchol + AllEnrolled.InstSchol) > 0
            then (AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL + AllEnrolled.StateGrant + AllEnrolled.InstGrant + AllEnrolled.FedSchol + AllEnrolled.StateSchol + AllEnrolled.InstSchol) end)) FIELD14_1, --Total aid for Group 3 current year
       ROUND(SUM(case when :Parameters_CB_PriorYear = 1 then :Global_SQL_PriorYearTotals.D15 end)) FIELD15_1, --Total aid for Group 3 prior year
       ROUND(SUM(case when :Parameters_CB_Prior2Year = 1 then :Global_SQL_Prior2YearTotals.D16 end)) FIELD16_1  --Total aid for Group 3 prior2 year
from AllEnrolled AllEnrolled
where AllEnrolled.FTPT = 'Full'
and AllEnrolled.FirstTime = 'FT'
and (AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL + AllEnrolled.StateGrant + AllEnrolled.InstGrant + AllEnrolled.FedSchol + AllEnrolled.StateSchol + AllEnrolled.InstSchol) > 0
and AllEnrolled.LgProg = 'Y'
and ((:Configuration_CB_Rep_Public = 1 and (AllEnrolled.Residency = 'INDIS' or AllEnrolled.Residency = 'INST'))
      or :Configuration_CB_Rep_Private = 1)

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

select 'E' PART,
       1 FIELD2_1, --Acad Year, current
       COUNT(AllEnrolled.PIDM) FIELD3_1, --Count of Group 4 students
       SUM(case when AllEnrolled.LivArrgmt = 'ONCAMP' then 1 else 0 end) FIELD4_1, --Count of Group 4 living on campus
       SUM(case when AllEnrolled.LivArrgmt = 'OFF_CAMP_FAM' then 1 else 0 end) FIELD5_1, --Count of Group 4 living off campus with family
       SUM(case when AllEnrolled.LivArrgmt = 'OFFCAMP' then 1 else 0 end) FIELD6_1, --Count of Group 4 living off campus not with family
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
from AllEnrolled AllEnrolled
where AllEnrolled.FTPT = 'Full'
and AllEnrolled.FirstTime = 'FT'
and AllEnrolled.TitleIV > 0
and AllEnrolled.LgProg = 'Y'
and ((:Configuration_CB_Rep_Public = 1 and (AllEnrolled.Residency = 'INDIS' or AllEnrolled.Residency = 'INST'))
      or :Configuration_CB_Rep_Private = 1)

union

select 'E' PART,
       2 FIELD2_1, --Acad Year, prior
       ROUND(:Global_SQL_PriorYearTotals.E3) FIELD3_1, --Count of Group 4 students
       ROUND(:Global_SQL_PriorYearTotals.E4) FIELD4_1, --Count of Group 4 living on campus
       ROUND(:Global_SQL_PriorYearTotals.E5) FIELD5_1, --Count of Group 4 living off campus with family
       ROUND(:Global_SQL_PriorYearTotals.E6) FIELD6_1, --Count of Group 4 living off campus not with family
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
from dual
where :Parameters_CB_PriorYear = 1

union

select 'E' PART,
       3 FIELD2_1, --Acad Year, prior2
       ROUND(:Global_SQL_PriorYearTotals.E3) FIELD3_1, --Count of Group 4 students
       ROUND(:Global_SQL_PriorYearTotals.E4) FIELD4_1, --Count of Group 4 living on campus
       ROUND(:Global_SQL_PriorYearTotals.E5) FIELD5_1, --Count of Group 4 living off campus with family
       ROUND(:Global_SQL_PriorYearTotals.E6) FIELD6_1, --Count of Group 4 living off campus not with family
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
from dual
where :Parameters_CB_Prior2Year = 1

union

--Part F Title IV aid awarded to full-time, first-time students by income
--Group 4 -> Awarded any Title IV aid
--Federal Pell Grant, Federal Supplemental Educational Opportunity Grant (FSEOG), Academic Competitiveness Grant (ACG),
--National Science and Mathematics Access to Retain Talent Grant (National SMART Grant), Teacher Education Assistance for College and Higher Education (TEACH) Grant
--Federal Work Study
--Federal Perkins Loan, Subsidized Direct or FFEL Stafford Loan, and Unsubsidized Direct or FFEL Stafford Loan

--Valid values
--Academic Years: 1=academic year 2017-18, 2=academic year 2016-17, 3=academic year 2015-16. If you have previously reported prior year values to IPEDS, report only YEAR=1.
--Income Range: 1-5. 1=$0-30,000  2=$30,001-48,000  3=$48,001-75,000  4=$75,001-110,000  5=$110,001 and more
--Number of students: 0-999999
--Total amount of aid: 0-999999999999
--       For public institutions, include only those students paying the in-state or in-district tuition rate.
--       For program reporters, include only those students enrolled in the institution’s largest program.

select 'F' PART,
       1 FIELD2_1, --Acad Year, current
       IncomeLevl FIELD3_1, --Income range of Group 4 students (values 1 - 5)
       nvl(SUM(FIELD4), 0) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       nvl(SUM(FIELD5), 0) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       nvl(ROUND(SUM(FIELD6)), 0) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from
(select AllEnrolled.Income IncomeLevl, --Income range of Group 4 students (values 1 - 5)
       COUNT(AllEnrolled.PIDM) FIELD4, --Count of Group 4 students who were awarded any Title IV aid
       SUM(case when (AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL + AllEnrolled.StateGrant + AllEnrolled.InstGrant + AllEnrolled.FedSchol + AllEnrolled.StateSchol + AllEnrolled.InstSchol) > 0 then 1 else 0 end) FIELD5, --Count of Group 4 students who were awarded any grant or scholarship aid
       SUM(case when (AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL + AllEnrolled.StateGrant + AllEnrolled.InstGrant + AllEnrolled.FedSchol + AllEnrolled.StateSchol + AllEnrolled.InstSchol) > 0 then (AllEnrolled.PELL + AllEnrolled.FedGrantNoPELL + AllEnrolled.StateGrant + AllEnrolled.InstGrant + AllEnrolled.FedSchol + AllEnrolled.StateSchol + AllEnrolled.InstSchol) end) FIELD6 --Total amount of grant or scholarship aid awarded to Group 4 students
from AllEnrolled AllEnrolled
where AllEnrolled.FTPT = 'Full'
and AllEnrolled.FirstTime = 'FT'
and AllEnrolled.TitleIV > 0
and AllEnrolled.LgProg = 'Y'
and ((:Configuration_CB_Rep_Public = 1 and (AllEnrolled.Residency = 'INDIS' or AllEnrolled.Residency = 'INST'))
      or :Configuration_CB_Rep_Private = 1)
group by AllEnrolled.Income

union

select
Val1,
0,
0,
0
from
(select T.DummyCount Val1
from
(select rownum DummyCount, spriden.spriden_pidm PIDM
from saturn.spriden spriden
where rownum < 6) T)
)
group by IncomeLevl

union

select 'F' PART,
       2 FIELD2_1, --Acad Year, prior
       1 FIELD3_1, --Income range 1
       ROUND(:Global_SQL_PriorYearTotals.F4_1) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       ROUND(:Global_SQL_PriorYearTotals.F5_1) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       ROUND(:Global_SQL_PriorYearTotals.F6_1) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from dual
where :Parameters_CB_PriorYear = 1

union

select 'F' PART,
       2 FIELD2_1, --Acad Year, prior
       2 FIELD3_1, --Income range 2
       ROUND(:Global_SQL_PriorYearTotals.F4_2) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       ROUND(:Global_SQL_PriorYearTotals.F5_2) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       ROUND(:Global_SQL_PriorYearTotals.F6_2) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from dual
where :Parameters_CB_PriorYear = 1

union

select 'F' PART,
       2 FIELD2_1, --Acad Year, prior
       3 FIELD3_1, --Income range 3
       ROUND(:Global_SQL_PriorYearTotals.F4_3) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       ROUND(:Global_SQL_PriorYearTotals.F5_3) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       ROUND(:Global_SQL_PriorYearTotals.F6_3) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from dual
where :Parameters_CB_PriorYear = 1

union

select 'F' PART,
       2 FIELD2_1, --Acad Year, prior
       4 FIELD3_1, --Income range 4
       ROUND(:Global_SQL_PriorYearTotals.F4_4) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       ROUND(:Global_SQL_PriorYearTotals.F5_4) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       ROUND(:Global_SQL_PriorYearTotals.F6_4) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from dual
where :Parameters_CB_PriorYear = 1

union

select 'F' PART,
       2 FIELD2_1, --Acad Year, prior
       5 FIELD3_1, --Income range 5
       ROUND(:Global_SQL_PriorYearTotals.F4_5) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       ROUND(:Global_SQL_PriorYearTotals.F5_5) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       ROUND(:Global_SQL_PriorYearTotals.F6_5) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from dual
where :Parameters_CB_PriorYear = 1

union

select 'F' PART,
       3 FIELD2_1, --Acad Year, prior2
       1 FIELD3_1, --Income range 1
       ROUND(:Global_SQL_Prior2YearTotals.F4_1) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       ROUND(:Global_SQL_Prior2YearTotals.F5_1) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       ROUND(:Global_SQL_Prior2YearTotals.F6_1) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from dual
where :Parameters_CB_Prior2Year = 1

union

select 'F' PART,
       3 FIELD2_1, --Acad Year, prior2
       2 FIELD3_1, --Income range 2
       ROUND(:Global_SQL_Prior2YearTotals.F4_2) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       ROUND(:Global_SQL_Prior2YearTotals.F5_2) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       ROUND(:Global_SQL_Prior2YearTotals.F6_2) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from dual
where :Parameters_CB_Prior2Year = 1

union

select 'F' PART,
       3 FIELD2_1, --Acad Year, prior2
       3 FIELD3_1, --Income range 3
       ROUND(:Global_SQL_Prior2YearTotals.F4_3) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       ROUND(:Global_SQL_Prior2YearTotals.F5_3) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       ROUND(:Global_SQL_Prior2YearTotals.F6_3) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from dual
where :Parameters_CB_Prior2Year = 1

union

select 'F' PART,
       3 FIELD2_1, --Acad Year, prior2
       4 FIELD3_1, --Income range 4
       ROUND(:Global_SQL_Prior2YearTotals.F4_4) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       ROUND(:Global_SQL_Prior2YearTotals.F5_4) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       ROUND(:Global_SQL_Prior2YearTotals.F6_4) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from dual
where :Parameters_CB_Prior2Year = 1

union

select 'F' PART,
       3 FIELD2_1, --Acad Year, prior2
       5 FIELD3_1, --Income range 5
       ROUND(:Global_SQL_Prior2YearTotals.F4_5) FIELD4_1, --Count of Group 4 students who were awarded any Title IV aid
       ROUND(:Global_SQL_Prior2YearTotals.F5_5) FIELD5_1, --Count of Group 4 students who were awarded any grant or scholarship aid
       ROUND(:Global_SQL_Prior2YearTotals.F6_5) FIELD6_1, --Total amount of grant or scholarship aid awarded to Group 4 students
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
from dual
where :Parameters_CB_Prior2Year = 1

union

--Part G Section 2: Veteran's Benefits

--Valid values
--Student level: 1=Undergraduate, 2=Graduate, 3=Total (Total will be generated)
--Number of students: 0 to 999999, -2 or blank = not-applicable
--Total amount of aid: 0 to 999999999999, -2 or blank = not-applicable

select 'G' PART,
       1 FIELD2_1, --Student Level 1=Undergraduate, 2=Graduate
       ROUND(:Global_SQL_GIBill_UG.STUDCOUNT) FIELD3_1, --Post-9/11 GI Bill Benefits - Number of students receiving benefits/assistance
       ROUND(:Global_SQL_GIBill_UG.BENEFITAMT) FIELD4_1, --Post-9/11 GI Bill Benefits - Total dollar amount of benefits/assistance disbursed through the institution
       ROUND(:Global_SQL_DDBill_UG.STUDCOUNT) FIELD5_1, --Department of Defense Tuition Assistance Program - Number of students receiving benefits/assistance
       ROUND(:Global_SQL_DDBill_UG.BENEFITAMT) FIELD6_1, --Department of Defense Tuition Assistance Program - Total dollar amount of benefits/assistance disbursed through the institution
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
from dual

union

select 'G' PART,
       2 FIELD2_1, --Student Level 1=Undergraduate, 2=Graduate
       ROUND(:Global_SQL_GIBill_GR.STUDCOUNT) FIELD3_1, --Post-9/11 GI Bill Benefits - Number of students receiving benefits/assistance
       ROUND(:Global_SQL_GIBill_GR.BENEFITAMT) FIELD4_1, --Post-9/11 GI Bill Benefits - Total dollar amount of benefits/assistance disbursed through the institution
       ROUND(:Global_SQL_DDBill_GR.STUDCOUNT) FIELD5_1, --Department of Defense Tuition Assistance Program - Number of students receiving benefits/assistance
       ROUND(:Global_SQL_DDBill_GR.BENEFITAMT) FIELD6_1, --Department of Defense Tuition Assistance Program - Total dollar amount of benefits/assistance disbursed through the institution
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
from dual

--Sort Order: UNITID, SURVSECT, PART
order by 1, 2
*/
