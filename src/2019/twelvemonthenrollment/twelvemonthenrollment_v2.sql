/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey  
FILE NAME:      12 Month Enrollment v2 (E12)
FILE DESC:      12 Month Enrollment for less than 4-year institutions
AUTHOR:         jhanicak
CREATED:        20200609

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Cohort Creation
Formatting Views
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)      Author             	    Tag             	Comments
-----------         --------------------	-------------   	-------------------------------------------------
20200618			akhasawneh				ak 20200618			Modify 12 MO report query with standardized view naming/aliasing convention (PF-1535) (runtime: 1m, 33s)
20200616	       	akhasawneh              ak 20200616         Modified to not reference term code as a numeric indicator of term ordering (PF-1494) (runtime: 1m, 38s)
20200609	        jhanicak				                    Initial version PF-1410 (runtime: 1m, 29s)

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
 full or partial term that starts within the academic year 7/1/2018 thru 6/30/2019
  ------------------------------------------------------------------
 Each client will need to determine how to identify and/or pull the 
 terms for their institution based on how they track the terms.  
 For some schools, it could be based on dates or academic year and for others,
 it may be by listing specific terms. 
 *******************************************************************/ 
select '1920' surveyYear, 
	'E1D' surveyId,  
	CAST('2018-07-01' AS TIMESTAMP) startDateProg,
	CAST('2019-06-30' AS TIMESTAMP) endDateProg,
	'202010' termCode,
	'1' partOfTermCode, 
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary      --M = Male, F = Female 

/*
--Use for testing internally only
--union 

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201330' termCode,
	'B' partOfTermCode, 
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary      --M = Male, F = Female 

union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201410' termCode,
	'1' partOfTermCode, 
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary      --M = Male, F = Female 

union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201410' termCode,
	'A' partOfTermCode, 
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary      --M = Male, F = Female 

union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201410' termCode,
	'B' partOfTermCode, 
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary   
union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201420' termCode,
	'1' partOfTermCode, 
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary      --M = Male, F = Female 
union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201420' termCode,
	'A' partOfTermCode, 
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary      --M = Male, F = Female 	
union

select '1314' surveyYear,  
	'E1D' surveyId,   
	CAST('2013-07-01' AS TIMESTAMP) startDateProg,
	CAST('2014-06-30' AS TIMESTAMP) endDateProg, 
	'201430' termCode,
	--CAST('2013-10-15' AS TIMESTAMP) censusDate, 
	'1' partOfTermCode, 
	'Y' includeNonDegreeAsUG,   --Y = Yes, N = No
	'M' genderForUnknown,       --M = Male, F = Female
	'F' genderForNonBinary      --M = Male, F = Female 
*/
), 

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

select surveyYear,
	surveyId, 
	startDateProg,
	endDateProg,
	termCode, 
	partOfTermCode,   
	includeNonDegreeAsUG,
	genderForUnknown,
	genderForNonBinary
from (
    select configENT.surveyCollectionYear surveyYear,
		defvalues.surveyId surveyId, 
		defvalues.startDateProg	startDateProg,
		defvalues.endDateProg endDateProg,
		defvalues.termCode termCode, 
		defvalues.partOfTermCode partOfTermCode,   
		nvl(configENT.includeNonDegreeAsUG, defvalues.includeNonDegreeAsUG) includeNonDegreeAsUG,
		nvl(configENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		nvl(configENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
		--annualDPPCreditHoursFTE
		--useClockHours
		row_number() over (
			partition by
				configENT.surveyCollectionYear
			order by
				configENT.recordActivityDate desc
		) configRn
		from IPEDSClientConfig configENT
			cross join DefaultValues defvalues
		where configENT.surveyCollectionYear = defvalues.surveyYear 

    union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select  defvalues.surveyYear surveyYear,
		defvalues.surveyId surveyId, 
		defvalues.startDateProg	startDateProg,
		defvalues.endDateProg endDateProg,
		defvalues.termCode termCode, 
		defvalues.partOfTermCode partOfTermCode, 
		defvalues.includeNonDegreeAsUG includeNonDegreeAsUG,  
		defvalues.genderForUnknown genderForUnknown,
		defvalues.genderForNonBinary genderForNonBinary,
		1 configRn
    from DefaultValues defvalues
    where defvalues.surveyYear not in (select configENT.surveyCollectionYear
										from IPEDSClientConfig configENT
										where configENT.surveyCollectionYear = defvalues.surveyYear)
    	)
where configRn = 1	
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for each term and part of term code. 
--PartOfTerm code defines a subcategory of a termCode that may have different start, end and census dates. 
-- 1 record per term/partOfTerm

select termCode, 
	partOfTermCode, 
	startDate,
	endDate,
	academicYear,
	censusDate				   
from ( 
    select distinct acadtermENT.termCode, 
		acadtermENT.partOfTermCode, 
		acadtermENT.recordActivityDate, 
		acadtermENT.termCodeDescription,       
		acadtermENT.partOfTermCodeDescription, 
		acadtermENT.startDate,
		acadtermENT.endDate,
		acadtermENT.academicYear,
		acadtermENT.censusDate,
		acadtermENT.isIPEDSReportable,
		row_number() over (
			partition by 
				acadtermENT.termCode,
				acadtermENT.partOfTermCode
			order by  
				acadtermENT.recordActivityDate desc
		) acadTermRn
		from AcademicTerm acadtermENT 
		where acadtermENT.isIPEDSReportable = 1  
	)
where acadTermRn = 1
),

--ak 20200616 Adding term order indicator (PF-1494)
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
-- ReportingPeriodMCR combines the dates from the IPEDSReportingPeriod with the 
-- AcademicTermMCR subquery, If no records comes IPEDSReportingPeriod
-- it uses the default dates & default term information in the ConfigPerAsOfDate
-- to insure that there are some records to keep IRIS from failing .  --JDH

select distinct RepDates.surveyYear	surveyYear,
	RepDates.surveyId surveyId,
	RepDates.startDatePeriod startDatePeriod,
	RepDates.endDatePeriod endDatePeriod,
	RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,	
	nvl(acadtermENT.censusDate, DATE_ADD(acadtermENT.startDate, 15)) censusDate, 
	acadtermENT.startDate termStartDate,
	acadtermENT.endDate termEndDate, 
	RepDates.includeNonDegreeAsUG includeNonDegreeAsUG,
	RepDates.genderForUnknown genderForUnknown,
	RepDates.genderForNonBinary genderForNonBinary 
from (
    select repperiodENT.surveyCollectionYear surveyYear,
		clientconfig.surveyId surveyId, 
		nvl(repperiodENT.reportingDateStart, clientconfig.startDateProg) startDatePeriod,
		nvl(repperiodENT.reportingDateEnd, clientconfig.endDateProg) endDatePeriod,
		nvl(repperiodENT.termCode, clientconfig.termCode) termCode,
		nvl(repperiodENT.partOfTermCode, clientconfig.partOfTermCode) partOfTermCode, 
		clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary,
		row_number() over (	
			partition by 
				repperiodENT.surveyCollectionYear,
				repperiodENT.surveyId, 
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
	
-- Added union to use default data when IPEDSReportingPeriod record doesn't exist so valid output file will always be generated
union 
 
	select clientconfig.surveyYear surveyYear, 
		clientconfig.surveyId surveyId, 
		clientconfig.startDateProg startDatePeriod,
		clientconfig.endDateProg endDatePeriod,
		clientconfig.termCode termCode,
		clientconfig.partOfTermCode partOfTermCode, 
		clientconfig.includeNonDegreeAsUG includeNonDegreeAsUG,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary,
		1
	from ClientConfigMCR clientconfig
    where clientconfig.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = clientconfig.surveyYear
											and repperiodENT.surveyId = clientconfig.surveyId 
											and repperiodENT.termCode is not null
											and repperiodENT.partOfTermCode is not null)
    ) RepDates
	inner join AcademicTermMCR acadtermENT on RepDates.termCode = acadtermENT.termCode	
		and RepDates.partOfTermCode = acadtermENT.partOfTermCode
where RepDates.reportPeriodRn = 1
), 

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusMCR as ( 
-- Returns most recent campus record for the ReportingPeriod. 
select campus,
	isInternational
from ( 
	select campusENT.campus,
		campusENT.campusDescription,
		campusENT.isInternational,
		row_number() over (
			partition by
				campusENT.campus
			order by
				campusENT.recordActivityDate desc
		) campusRn
	from ReportingPeriodMCR repperiod
		cross join Campus campusENT 
	where campusENT.isIpedsReportable = 1 
		and ((campusENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
			and campusENT.recordActivityDate <= repperiod.endDatePeriod)
                 or campusENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where campusRn = 1
), 
 
RegistrationMCR as ( 
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 

select personId,
	termCode,
	partOfTermCode, 
	censusDate,
	isInternational,    
	crnGradingMode,                    
	crn,
	crnLevel, 
	includeNonDegreeAsUG,
	genderForUnknown,
	genderForNonBinary
from ( 
    select  regENT.personId personId,
		regENT.termCode termCode,
		regENT.partOfTermCode partOfTermCode, 
		repperiod.censusDate censusDate,
		nvl(campus.isInternational, false) isInternational,    
		regENT.crnGradingMode crnGradingMode,                    
		regENT.crn crn,
		regENT.crnLevel crnLevel, 
		repperiod.includeNonDegreeAsUG includeNonDegreeAsUG,
		repperiod.genderForUnknown genderForUnknown,
		repperiod.genderForNonBinary genderForNonBinary,
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
		inner join Registration regENT on regENT.termCode = repperiod.termCode  
			and repperiod.partOfTermCode = regENT.partOfTermCode 
			and ((regENT.registrationStatusDate != CAST('9999-09-09' AS TIMESTAMP)
				and regENT.registrationStatusDate <= repperiod.censusDate)
					or regENT.registrationStatusDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and regENT.isEnrolled = 1
			and regENT.isIpedsReportable = 1 
		left join CampusMCR campus on regENT.campus = campus.campus  
	)
where regRn = 1
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  

select personId,
	termCode,
	isNonDegreeSeeking,
	studentLevel
from ( 
    select distinct studentENT.personId personId,
		studentENT.termCode termCode, 
		studentENT.isNonDegreeSeeking isNonDegreeSeeking,
		studentENT.studentLevel studentLevel,
		row_number() over (
			partition by
				studentENT.personId,
				studentENT.termCode
			order by
				studentENT.recordActivityDate desc
		) studRn
	from RegistrationMCR reg
		inner join Student studentENT on reg.personId = studentENT.personId 
			and reg.termCode = studentENT.termCode
			and ((studentENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)  
				and studentENT.recordActivityDate <= reg.censusDate)
					or studentENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and studentENT.studentStatus = 'Active' --do not report Study Abroad students
			and (studentENT.studentLevel = 'UnderGrad'
				or (studentENT.studentLevel = 'Continuing Ed' and reg.includeNonDegreeAsUG = 'Y')) 
			and studentENT.isIpedsReportable = 1
	)
where studRn = 1
),

CourseSectionMCR as (
--Included to get enrollment hours of a CRN

select termCode,
	partOfTermCode,
	crn,
	section,
	subject,
	courseNumber,
	crnLevel,
	censusDate,
	enrollmentHours,
	isClockHours
from ( 
    select distinct coursesectENT.termCode termCode,
		coursesectENT.partOfTermCode partOfTermCode,
		coursesectENT.crn crn,
		coursesectENT.section section,
		coursesectENT.subject subject,
		coursesectENT.courseNumber courseNumber,
		reg.crnLevel crnLevel,
		reg.censusDate censusDate,
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
            and coursesectENT.isIpedsReportable = 1 
            and ((coursesectENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
                and coursesectENT.recordActivityDate <= reg.censusDate)
                	or coursesectENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where courseRn = 1
), 

CourseSectionScheduleMCR  as (
--Returns course scheduling related info for the registration CRN.
--AcademicTerm.partOfTermCode, CourseSectionSchedule.partOfTermCode & AcademicTerm.censusDate together 
--are used to define the period of valid course registration attempts. 

select coursesect.termCode termCode,
--ak 20200616 Adding term order indicator (PF-1494)
	termOrder.termOrder termOrder,
	coursesect.censusDate censusDate,
	coursesect.crn crn,
	coursesect.section section,
	coursesect.partOfTermCode partOfTermCode,
	coursesect.enrollmentHours enrollmentHours,
	coursesect.isClockHours isClockHours,
	coursesect.subject subject,
	coursesect.courseNumber courseNumber,
	coursesect.crnLevel crnLevel,
	nvl(CourseSched.meetingType, 'Standard') meetingType
from CourseSectionMCR coursesect
--ak 20200616 Adding term order indicator (PF-1494)
	inner join AcadTermOrder termOrder 
		on termOrder.TermCode = coursesect.termCode
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
				and courseSectSchedENT.section = coursesect.section
				and courseSectSchedENT.isIpedsReportable = 1  
				and ((courseSectSchedENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
					and courseSectSchedENT.recordActivityDate <= coursesect.censusDate)
						or courseSectSchedENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	) CourseSched 
		on CourseSched.termCode = coursesect.termCode
		and CourseSched.partOfTermCode = coursesect.partOfTermCode
		and CourseSched.crn = coursesect.crn
		and CourseSched.section = coursesect.section
		and CourseSched.courseSectSchedRn = 1		 
),

CourseMCR as (
-- Included to get course type information and it has included the most recent version
-- prior to the census date. 

select termCode,
	crn,
	section,
	subject,
	courseNumber,
	partOfTermCode,
	censusDate,
	courseLevel,
	isRemedial,
	isESL,
	meetingType,
	enrollmentHours,
	isClockHours
from ( 
    select distinct 
		coursesectsched.termCode termCode,
        coursesectsched.crn crn,
        coursesectsched.section section,
        courseENT.subject subject,
        courseENT.courseNumber courseNumber,
        coursesectsched.partOfTermCode partOfTermCode,
        coursesectsched.censusDate censusDate,
        courseENT.courseLevel courseLevel,
        courseENT.isRemedial isRemedial,
        courseENT.isESL isESL,
        coursesectsched.meetingType meetingType,
        coursesectsched.enrollmentHours enrollmentHours,
        coursesectsched.isClockHours isClockHours,
        row_number() over (
            partition by
                courseENT.subject,
                courseENT.courseNumber,
                courseENT.termCodeEffective,
                courseENT.courseLevel
            order by
                courseENT.recordActivityDate desc
        ) courseRn
    from CourseSectionScheduleMCR  coursesectsched
        inner join Course courseENT ON courseENT.subject = coursesectsched.subject
            and courseENT.courseNumber = coursesectsched.courseNumber
            and courseENT.courseLevel = coursesectsched.crnLevel
            and courseENT.isIpedsReportable = 1  
            and ((courseENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
                and courseENT.recordActivityDate <= coursesectsched.censusDate)
                    or courseENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
--ak 20200616 Adding term order indicator (PF-1494)
		inner join AcadTermOrder termOrder
			on termOrder.termCode = courseENT.TermCodeEffective
	where termOrder.TermOrder <= coursesectsched.termOrder
	)
where courseRn = 1
), 

CourseTypeCountsSTU as (
-- View used to break down course category type totals by type

select reg.personId personId,  
	sum(case when nvl(course.enrollmentHours, 0) is not null then 1 else 0 end) totalCourses,
	sum(case when course.isClockHours = 0 then nvl(course.enrollmentHours, 0) else 0 end) totalCreditHrs,
	sum(case when course.isClockHours = 1 then nvl(course.enrollmentHours, 0) else 0 end) totalClockHrs,
	sum(case when nvl(course.enrollmentHours, 0) = 0 then 1 else 0 end) totalNonCredCourses,
	sum(case when nvl(course.enrollmentHours, 0) != 0 then 1 else 0 end) totalCredCourses,
	sum(case when course.meetingType = 'Online/Distance Learning' then 1 else 0 end) totalDECourses,
	sum(case when course.courseLevel = 'Continuing Ed' then 1 else 0 end) totalCECourses,
	sum(case when course.isESL = 'Y' then 1 else 0 end) totalESLCourses,
	sum(case when course.isRemedial = 'Y' then 1 else 0 end) totalRemCourses,
	sum(case when reg.isInternational = 1 then 1 else 0 end) totalIntlCourses, 
	sum(case when reg.crnGradingMode = 'Audit' then 1 else 0 end) totalAuditCourses
from RegistrationMCR reg
	inner join CourseMCR course on course.termCode = reg.termCode
	    and course.partOfTermCode = reg.partOfTermCode
	    and course.crn = reg.crn 
		and course.courseLevel = reg.crnLevel 
group by reg.termCode,
	reg.personId,
	reg.partOfTermCode 
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting period and term census periods.
--I used the RegistrationMCR to limit list to only students in the reportable terms, but limit the
--Person Record to the latest record. 
	
select termCode, 
	partOfTermCode, 
	censusDate, 
	personId,
	birthDate,
	ethnicity,
	isHispanic,
	isMultipleRaces,
	isInUSOnVisa,
	isUSCitizen,
	gender
from (  
    select reg.termCode termCode, 
		reg.partOfTermCode partOfTermCode, 
		reg.censusDate censusDate, 
		personENT.personId personId,
		personENT.birthDate birthDate,
		personENT.ethnicity ethnicity,
		personENT.isHispanic isHispanic,
		personENT.isMultipleRaces isMultipleRaces,
		personENT.isInUSOnVisa isInUSOnVisa,
		personENT.isUSCitizen isUSCitizen,
		personENT.gender gender,
		row_number() over (
			partition by
				personENT.personId
			order by
				personENT.recordActivityDate desc
		) personRn
	from RegistrationMCR reg
		inner join Person personENT on reg.personId = personENT.personId 
			and ((personENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and personENT.recordActivityDate <= reg.censusDate) 
					or personENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
			and personENT.isIpedsReportable = 1
	)
where personRn = 1
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

CohortSTU as (
--View used to get one record per student and studentLevel

select personID,  
	ipedsGender,
	ipedsEthnicity,  
    totalCreditHrs,
    totalClockHrs 
from (
     select distinct reg.personID personID,
		case when person.gender = 'Male' then 'M'
             when person.gender = 'Female' then 'F' 
             when person.gender = 'Non-Binary' then reg.genderForNonBinary
			else reg.genderForUnknown
		end ipedsGender,
		case when person.isUSCitizen = 1 then 
			  (case when person.isHispanic = true then '2' -- 'hispanic/latino'
				    when person.isMultipleRaces = true then '8' -- 'two or more races'
				    when person.ethnicity != 'Unknown' and person.ethnicity is not null then
				           (case when person.ethnicity = 'Hispanic or Latino' then '2'
				                when person.ethnicity = 'American Indian or Alaskan Native' then '3'
				                when person.ethnicity = 'Asian' then '4'
				                when person.ethnicity = 'Black or African American' then '5'
				                when person.ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
				                when person.ethnicity = 'Caucasian' then '7'
		                        else '9' 
			                end) 
			         else '9' end) -- 'race and ethnicity unknown'
		        when person.isInUSOnVisa = 1 then '1' -- 'nonresident alien'
		        else '9' -- 'race and ethnicity unknown'
		end ipedsEthnicity,  
        coalesce(coursecnt.totalCreditHrs, 0) totalCreditHrs,
        coalesce(coursecnt.totalClockHrs, 0) totalClockHrs, 
-- Exclude Flag for those students whose total course counts equal excluded groups		
        case when coursecnt.totalRemCourses = coursecnt.totalCourses and student.isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
			 when coursecnt.totalCredCourses > 0 --exclude students not enrolled for credit
                        then (case when coursecnt.totalESLCourses = coursecnt.totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
                                    when coursecnt.totalCECourses = coursecnt.totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
                                    when coursecnt.totalIntlCourses = coursecnt.totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
                                    when coursecnt.totalAuditCourses = coursecnt.totalCredCourses then 0 --exclude students exclusively auditing classes
                                        -- when... then 0 --exclude PHD residents or interns 
                                        -- when... then 0 --exclude students in experimental Pell programs
                                    else 1
                            end)
            else 0 
        end ipedsInclude
    from RegistrationMCR reg  
        inner join PersonMCR person on reg.personId = person.personId 
        inner join CourseTypeCountsSTU coursecnt on reg.personId = coursecnt.personId  
        inner join StudentMCR student on student.personId = reg.personId
    )
where ipedsInclude = 1
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

/* Part A: Unduplicated Count by Student Level, Gender, and Race/Ethnicity
Report all students enrolled for credit at any time during the July 1, 2018 - June 30, 2019 reporting period. 
Students are reported by gender, race/ethnicity, and their level of standing with the institution.
*/

select 'A'                                                                                      part,
       1                                                                                        field1,  -- SLEVEL   - valid value is 1 for undergraduate 
       coalesce(sum(case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end),
                0)                                                                              field2,  -- FYRACE01 - Nonresident alien - Men (1), 0 to 999999                           
       coalesce(sum(case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end),
                0)                                                                              field3,  -- FYRACE02 - Nonresident alien - Women (2), 0 to 999999                         
       coalesce(sum(case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end),
                0)                                                                              field4,  -- FYRACE25 - Hispanic/Latino - Men (25), 0 to 999999                            
       coalesce(sum(case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end),
                0)                                                                              field5,  -- FYRACE26 - Hispanic/Latino - Women (26), 0 to 999999                          
       coalesce(sum(case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end),
                0)                                                                              field6,  -- FYRACE27 - American Indian or Alaska Native - Men (27), 0 to 999999           
       coalesce(sum(case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end),
                0)                                                                              field7,  -- FYRACE28 - American Indian or Alaska Native - Women (28), 0 to 999999         
       coalesce(sum(case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end),
                0)                                                                              field8,  -- FYRACE29 - Asian - Men (29), 0 to 999999                                      
       coalesce(sum(case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end),
                0)                                                                              field9,  -- FYRACE30 - Asian - Women (30), 0 to 999999                                    
       coalesce(sum(case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end),
                0)                                                                              field10, -- FYRACE31 - Black or African American - Men (31), 0 to 999999                  
       coalesce(sum(case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end),
                0)                                                                              field11, -- FYRACE32 - Black or African American - Women (32), 0 to 999999                
       coalesce(sum(case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end),
                0)                                                                              field12, -- FYRACE33 - Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999  
       coalesce(sum(case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end),
                0)                                                                              field13, -- FYRACE34 - Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
       coalesce(sum(case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end),
                0)                                                                              field14, -- FYRACE35 - White - Men (35), 0 to 999999                                      
       coalesce(sum(case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end),
                0)                                                                              field15, -- FYRACE36 - White - Women (36), 0 to 999999                                    
       coalesce(sum(case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end),
                0)                                                                              field16, -- FYRACE37 - Two or more races - Men (37), 0 to 999999                          
       coalesce(sum(case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end),
                0)                                                                              field17, -- FYRACE38 - Two or more races - Women (38), 0 to 999999                        
       coalesce(sum(case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end),
                0)                                                                              field18, -- FYRACE13 - Race and ethnicity unknown - Men (13), 0 to 999999                 
       coalesce(sum(case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end),
                0)                                                                              field19  -- FYRACE14 - Race and ethnicity unknown - Women (14), 0 to 999999               
from CohortSTU cohortstu


union

/* Part B: Instructional Activity and Full-Time Equivalent Enrollment
Report the total clock hour and/or credit hour activity attempted during the 12-month period of July 1, 2018 - June 30, 2019. 
The instructional activity data reported will be used to calculate full-time equivalent (FTE) student enrollment at the institution.
*/

select 'B'                                     part,
       null                                    field1,
       coalesce(sum(cohortstu.totalCreditHrs), 0) field2, -- CREDHRSU - credit hour instructional activity at the undergraduate level
       sum(cohortstu.totalClockHrs)               field3, -- CONTHRS  - clock hour instructional activity at the undergraduate level
       null                                    field4,
       null                                    field5,
       null                                    field6,
       null                                    field7,
       null                                    field8,
       null                                    field9,
       null                                    field10,
       null                                    field11,
       null                                    field12,
       null                                    field13,
       null                                    field14,
       null                                    field15,
       null                                    field16,
       null                                    field17,
       null                                    field18,
       null                                    field19
from CohortSTU cohortstu
