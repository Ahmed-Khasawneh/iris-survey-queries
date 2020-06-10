/********************

EVI PRODUCT:	DORIS 2019-20 IPEDS Survey  
FILE NAME: 		12 Month Enrollment v1 (E1D)
FILE DESC:      12 Month Enrollment for 4-year institutions
AUTHOR:         jhanicak/JD Hysler
CREATED:        20200609

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Cohort Creation
Formatting Views
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)  	Author             	    Tag             	Comments
----------- 		--------------------	-------------   	-------------------------------------------------
20200609	        jhanicak				                    Initial version PF-1409 (runtime: 1m, 40s)

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

ConfigPerAsOfDate as (
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
    select config.surveyCollectionYear surveyYear,
			defaultValues.surveyId surveyId, 
			defaultValues.startDateProg	startDateProg,
			defaultValues.endDateProg endDateProg,
			defaultValues.termCode termCode, 
			defaultValues.partOfTermCode partOfTermCode,   
			nvl(config.includeNonDegreeAsUG, defaultValues.includeNonDegreeAsUG) includeNonDegreeAsUG,
			nvl(config.genderForUnknown, defaultValues.genderForUnknown) genderForUnknown,
			nvl(config.genderForNonBinary, defaultValues.genderForNonBinary) genderForNonBinary,
			--annualDPPCreditHoursFTE
			--useClockHours
            row_number() over (
                partition by
                    config.surveyCollectionYear
                order by
                    config.recordActivityDate desc
                ) configRn
		from IPEDSClientConfig config
			cross join DefaultValues defaultValues
		where config.surveyCollectionYear = defaultValues.surveyYear 

    union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select  defaultValues.surveyYear surveyYear,
			defaultValues.surveyId surveyId, 
			defaultValues.startDateProg	startDateProg,
			defaultValues.endDateProg endDateProg,
			defaultValues.termCode termCode, 
			defaultValues.partOfTermCode partOfTermCode, 
			defaultValues.includeNonDegreeAsUG includeNonDegreeAsUG,  
			defaultValues.genderForUnknown genderForUnknown,
			defaultValues.genderForNonBinary genderForNonBinary,
			1 configRn
    from DefaultValues defaultValues
    where defaultValues.surveyYear not in (select config.surveyCollectionYear
                                            from IPEDSClientConfig config
                                            where config.surveyCollectionYear = defaultValues.surveyYear)
    	)
where configRn = 1	
),

AcadTermLatestRecord as (
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
    select distinct acadTerm.termCode, 
                    acadTerm.partOfTermCode, 
                    acadTerm.recordActivityDate, 
                    acadTerm.termCodeDescription,       
                    acadTerm.partOfTermCodeDescription, 
                    acadTerm.startDate,
                    acadTerm.endDate,
                    acadTerm.academicYear,
                    acadTerm.censusDate,
                    acadTerm.isIPEDSReportable,
                    row_number() over (
                        partition by 
                            acadTerm.termCode,
                            acadTerm.partOfTermCode
                        order by  
                            acadTerm.recordActivityDate desc
                        ) acadTermRn
		from AcademicTerm acadTerm 
		where acadTerm.isIPEDSReportable = 1  
	)
where acadTermRn = 1
),

ReportingDates as (
-- ReportingDates combines the dates from the IPEDSReportingPeriod with the 
-- AcadTermLatestRecord subquery, If no records comes IPEDSReportingPeriod
-- it uses the default dates & default term information in the ConfigPerAsOfDate
-- to insure that there are some records to keep IRIS from failing .  --JDH

select distinct RepDates.surveyYear	surveyYear,
                RepDates.surveyId surveyId,
                RepDates.startDatePeriod startDatePeriod,
                RepDates.endDatePeriod endDatePeriod,
                RepDates.termCode termCode,	
                RepDates.partOfTermCode partOfTermCode,	
                nvl(AcademicTerm.censusDate, DATE_ADD(AcademicTerm.startDate, 15)) censusDate, 
                AcademicTerm.startDate termStartDate,
                AcademicTerm.endDate termEndDate, 
                RepDates.includeNonDegreeAsUG includeNonDegreeAsUG,
                RepDates.genderForUnknown genderForUnknown,
                RepDates.genderForNonBinary genderForNonBinary 
from (
    select  ReportPeriod.surveyCollectionYear surveyYear,
            defaultValues.surveyId surveyId, 
            nvl(ReportPeriod.reportingDateStart, defaultValues.startDateProg) startDatePeriod,
            nvl(ReportPeriod.reportingDateEnd, defaultValues.endDateProg) endDatePeriod,
            nvl(ReportPeriod.termCode, defaultValues.termCode) termCode,
            nvl(ReportPeriod.partOfTermCode, defaultValues.partOfTermCode) partOfTermCode, 
            defaultValues.includeNonDegreeAsUG includeNonDegreeAsUG,
            defaultValues.genderForUnknown genderForUnknown,
            defaultValues.genderForNonBinary genderForNonBinary,
            row_number() over (	
                partition by 
                    ReportPeriod.surveyCollectionYear,
                    ReportPeriod.surveyId, 
                    ReportPeriod.termCode,
                    ReportPeriod.partOfTermCode	
                order by ReportPeriod.recordActivityDate desc	
            ) reportPeriodRn	
		from IPEDSReportingPeriod ReportPeriod
			cross join ConfigPerAsOfDate defaultValues
		where ReportPeriod.surveyCollectionYear = defaultValues.surveyYear
			and ReportPeriod.surveyId = defaultValues.surveyId
			and ReportPeriod.termCode is not null
			and ReportPeriod.partOfTermCode is not null
	
-- Added union to use default data when IPEDSReportingPeriod record doesn't exist so valid output file will always be generated
union 
 
	select  defValues.surveyYear surveyYear, 
			defValues.surveyId surveyId, 
            defValues.startDateProg startDatePeriod,
            defValues.endDateProg endDatePeriod,
			defValues.termCode termCode,
			defValues.partOfTermCode partOfTermCode, 
			defValues.includeNonDegreeAsUG includeNonDegreeAsUG,
			defValues.genderForUnknown genderForUnknown,
			defValues.genderForNonBinary genderForNonBinary,
			1
	from ConfigPerAsOfDate defValues
    where defValues.surveyYear not in (select ReportPeriod.surveyCollectionYear
										from IPEDSReportingPeriod ReportPeriod
										where ReportPeriod.surveyCollectionYear = defValues.surveyYear
											and ReportPeriod.surveyId = defValues.surveyId 
											and ReportPeriod.termCode is not null
											and ReportPeriod.partOfTermCode is not null)
    ) RepDates
        inner join AcadTermLatestRecord AcademicTerm on RepDates.termCode = AcademicTerm.termCode	
            and RepDates.partOfTermCode = AcademicTerm.partOfTermCode
    where RepDates.reportPeriodRn = 1
), 

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusPerAsOfCensus as ( 
-- Returns most recent campus record for the ReportingPeriod. 
select campus,
       isInternational
from ( 
    select campus.campus,
           campus.campusDescription,
           campus.isInternational,
			row_number() over (
					partition by
						campus.campus
					order by
						campus.recordActivityDate desc
				) campusRn
		from ReportingDates ReportingDates
            cross join Campus campus 
		where campus.isIpedsReportable = 1 
            and ((campus.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
                and campus.recordActivityDate <= ReportingDates.endDatePeriod)
                 or campus.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where campusRn = 1
), 
 
RegPerTermAsOfCensus as ( 
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
    select  reg.personId personId,
            reg.termCode termCode,
            reg.partOfTermCode partOfTermCode, 
            ReportingDates.censusDate censusDate,
            nvl(campus.isInternational, false) isInternational,    
            reg.crnGradingMode crnGradingMode,                    
            reg.crn crn,
            reg.crnLevel crnLevel, 
            ReportingDates.includeNonDegreeAsUG includeNonDegreeAsUG,
            ReportingDates.genderForUnknown genderForUnknown,
            ReportingDates.genderForNonBinary genderForNonBinary,
            row_number() over (
                partition by
                    reg.personId,
                    reg.termCode,
                    reg.partOfTermCode,
                    reg.crn,
                    reg.crnLevel
                order by
                    reg.recordActivityDate desc
            ) regRn
		from ReportingDates ReportingDates   
			inner join Registration reg on reg.termCode = ReportingDates.termCode  
				and ReportingDates.partOfTermCode = reg.partOfTermCode 
                and ((reg.registrationStatusDate != CAST('9999-09-09' AS TIMESTAMP)
				    and reg.registrationStatusDate <= ReportingDates.censusDate)
			            or reg.registrationStatusDate = CAST('9999-09-09' AS TIMESTAMP)) 
				and reg.isEnrolled = 1
				and reg.isIpedsReportable = 1 
			left join CampusPerAsOfCensus campus on reg.campus = campus.campus  
	)
where regRn = 1
),

StudentPerTermReg as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  

select personId,
       termCode, 
       stuLevelCalc,
		isNonDegreeSeeking,
		studentLevel
from ( 
    select distinct student.personId personId,
                student.termCode termCode, 
                case when student.studentLevel = 'Continuing Ed' and reg.includeNonDegreeAsUG = 'Y' then 1 
                     when student.studentLevel = 'Undergrad' then 1
                     when student.studentLevel in ('Professional', 'Postgraduate','Graduate') then 3 
					 else 0 
				end stuLevelCalc, -- translating to numeric for MaxStuLevel later on 
				student.isNonDegreeSeeking isNonDegreeSeeking,
				student.studentLevel studentLevel,
				row_number() over (
					partition by
						student.personId,
						student.termCode
					order by
						student.recordActivityDate desc
                    ) studRn
		from RegPerTermAsOfCensus reg
		    inner join Student student on reg.personId = student.personId 
		        	and reg.termCode = student.termCode
                		and ((student.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)  
				    and student.recordActivityDate <= reg.censusDate)
			            or student.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and student.studentStatus = 'Active' --do not report Study Abroad students	    		and student.isIpedsReportable = 1
	)
where studRn = 1
),

StuMaxLevelPerPeriod as (
-- Included to get the Max Student level for the Reporting Period as a whole. 

select personId,  
       max(stuLevelCalc) maxStuLevelCalcA
from StudentPerTermReg
group by personId 
), 

CourseSectionPerTerm as (
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
    select distinct courseSect.termCode termCode,
                    courseSect.partOfTermCode partOfTermCode,
                    courseSect.crn crn,
                    courseSect.section section,
                    courseSect.subject subject,
                    courseSect.courseNumber courseNumber,
                    reg.crnLevel crnLevel,
                    reg.censusDate censusDate,
                    courseSect.recordActivityDate recordActivityDate,
                    CAST(courseSect.enrollmentHours as decimal(2,0)) enrollmentHours,
                    courseSect.isClockHours isClockHours,
                    row_number() over (
                        partition by
                            courseSect.subject,
                            courseSect.courseNumber,
                            courseSect.crn,
                            courseSect.section,
                            courseSect.termCode,
                            courseSect.partOfTermCode
                        order by
                            courseSect.recordActivityDate desc
                    ) courseRn
    from RegPerTermAsOfCensus reg 
        inner join CourseSection courseSect on courseSect.termCode = reg.termCode
            and courseSect.partOfTermCode = reg.partOfTermCode
            and courseSect.crn = reg.crn
            and courseSect.isIpedsReportable = 1 
            and ((courseSect.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
                and courseSect.recordActivityDate <= reg.censusDate)
                    or courseSect.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where courseRn = 1
), 

CourseSectionSchedulePerTerm as (
--Returns course scheduling related info for the registration CRN.
--AcademicTerm.partOfTermCode, CourseSectionSchedule.partOfTermCode & AcademicTerm.censusDate together 
--are used to define the period of valid course registration attempts. 

select courseSect.termCode termCode,
        courseSect.censusDate censusDate,
        courseSect.crn crn,
        courseSect.section section,
        courseSect.partOfTermCode partOfTermCode,
        courseSect.enrollmentHours enrollmentHours,
        courseSect.isClockHours isClockHours,
        courseSect.subject subject,
        courseSect.courseNumber courseNumber,
        courseSect.crnLevel crnLevel,
        nvl(CourseSched.meetingType, 'Standard') meetingType
from CourseSectionPerTerm courseSect
		left join (
            select courseSectSched.termCode termCode,
				courseSectSched.crn crn,
				courseSectSched.section section,
				courseSectSched.partOfTermCode partOfTermCode,
				nvl(courseSectSched.meetingType, 'Standard') meetingType,
				row_number() over (
					partition by
						courseSectSched.crn,
						courseSectSched.section,
						courseSectSched.termCode,
						courseSectSched.partOfTermCode
					order by
						courseSectSched.recordActivityDate desc
                    ) courseSectSchedRn 
                from CourseSectionPerTerm courseSection
                    inner join CourseSectionSchedule courseSectSched on courseSectSched.termCode = courseSection.termCode
                        and courseSectSched.partOfTermCode = courseSection.partOfTermCode
                        and courseSectSched.crn = courseSection.crn
                        and courseSectSched.section = courseSection.section
                        and courseSectSched.isIpedsReportable = 1  
                        and ((courseSectSched.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
                            and courseSectSched.recordActivityDate <= courseSection.censusDate)
                                or courseSectSched.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
                ) CourseSched on CourseSched.termCode = courseSect.termCode
                    and CourseSched.partOfTermCode = courseSect.partOfTermCode
                    and CourseSched.crn = courseSect.crn
                    and CourseSched.section = courseSect.section
                    and CourseSched.courseSectSchedRn = 1		 
), 

CoursePerTerm as (
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
        courseSect.termCode termCode,
        courseSect.crn crn,
        courseSect.section section,
        course.subject subject,
        course.courseNumber courseNumber,
        courseSect.partOfTermCode partOfTermCode,
        courseSect.censusDate censusDate,
        course.courseLevel courseLevel,
        course.isRemedial isRemedial,
        course.isESL isESL,
        courseSect.meetingType meetingType,
        courseSect.enrollmentHours enrollmentHours,
        courseSect.isClockHours isClockHours,
        row_number() over (
            partition by
                course.subject,
                course.courseNumber,
                course.termCodeEffective,
                course.courseLevel
            order by
                course.recordActivityDate desc
        ) courseRn
    from CourseSectionSchedulePerTerm courseSect
        inner join Course course ON course.subject = courseSect.subject
            and course.courseNumber = courseSect.courseNumber
            and course.courseLevel = courseSect.crnLevel
            and course.termCodeEffective <= courseSect.termCode
            and course.isIpedsReportable = 1  
            and ((course.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
                and course.recordActivityDate <= courseSect.censusDate)
                    or course.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
	)
where courseRn = 1
), 

CourseCountsPerStudent as (
-- View used to break down course category type totals by type

select reg.personId personId,  
        sum(case when nvl(course.enrollmentHours, 0) is not null then 1 else 0 end) totalCourses,
		sum(case when course.isClockHours = 0 then nvl(course.enrollmentHours, 0) else 0 end) totalCreditHrs,
		sum(case when course.isClockHours = 0 and course.courseLevel = 'Undergrad' then nvl(course.enrollmentHours, 0) else 0 end) totalCreditUGHrs,
		sum(case when course.isClockHours = 0 and course.courseLevel in ('Graduate', 'Professional') then nvl(course.enrollmentHours, 0) else 0 end) totalCreditGRHrs,
		sum(case when course.isClockHours = 0 and course.courseLevel = 'Postgraduate' then nvl(course.enrollmentHours, 0) else 0 end) totalCreditPostGRHrs,
		sum(case when course.isClockHours = 1 then nvl(course.enrollmentHours, 0) else 0 end) totalClockHrs,
        sum(case when nvl(course.enrollmentHours, 0) = 0 then 1 else 0 end) totalNonCredCourses,
        sum(case when nvl(course.enrollmentHours, 0) != 0 then 1 else 0 end) totalCredCourses,
        sum(case when course.meetingType = 'Online/Distance Learning' then 1 else 0 end) totalDECourses,
        sum(case when course.courseLevel = 'Undergrad' then 1 else 0 end) totalUGCourses,
        sum(case when course.courseLevel in ('Graduate', 'Professional') then 1 else 0 end) totalGRCourses,
        sum(case when course.courseLevel = 'Postgraduate' then 1 else 0 end) totalPostGRCourses,
		sum(case when course.courseLevel = 'Continuing Ed' then 1 else 0 end) totalCECourses,
        sum(case when course.isESL = 'Y' then 1 else 0 end) totalESLCourses,
        sum(case when course.isRemedial = 'Y' then 1 else 0 end) totalRemCourses,
        sum(case when reg.isInternational = 1 then 1 else 0 end) totalIntlCourses, 
        sum(case when reg.crnGradingMode = 'Audit' then 1 else 0 end) totalAuditCourses
from RegPerTermAsOfCensus reg
	inner join CoursePerTerm course on course.termCode = reg.termCode
	    and course.partOfTermCode = reg.partOfTermCode
	    and course.crn = reg.crn 
		and course.courseLevel = reg.crnLevel 
group by reg.termCode,
		reg.personId,
		reg.partOfTermCode 
),

PersonPerPeriod as (
--Returns most up to date student personal information as of the reporting period and term census periods.
--I used the RegPerTermAsOfCensus to limit list to only students in the reportable terms, but limit the
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
           person.personId personId,
           person.birthDate birthDate,
           person.ethnicity ethnicity,
           person.isHispanic isHispanic,
           person.isMultipleRaces isMultipleRaces,
           person.isInUSOnVisa isInUSOnVisa,
           person.isUSCitizen isUSCitizen,
           person.gender gender,
           row_number() over (
                partition by
                    person.personId
                order by
                     person.recordActivityDate desc
                ) personRn
        from RegPerTermAsOfCensus reg
            inner join Person person on reg.personId = person.personId 
                and ((person.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
                    and person.recordActivityDate <= reg.censusDate) 
                        or person.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
                and person.isIpedsReportable = 1
	)
where personRn = 1
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

StudentCohort as (
--View used to get one record per student and studentLevel

select personID,  
    ipedsPartAStudentLevel, 
    studentLevel,
    ipedsGender,
	ipedsEthnicity,  
    totalCreditHrs,
    totalCreditUGHrs,
    totalCreditGRHrs,
    totalCreditPostGRHrs,
    totalClockHrs 
from (
     select distinct reg.personID personID,  
        maxLevel.maxStuLevelCalcA ipedsPartAStudentLevel, 
        stuReg.studentLevel studentLevel,
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
        coalesce(courseCnt.totalCreditHrs, 0) totalCreditHrs,
        coalesce(courseCnt.totalCreditUGHrs, 0) totalCreditUGHrs,
        coalesce(courseCnt.totalCreditGRHrs, 0) totalCreditGRHrs,
        coalesce(courseCnt.totalCreditPostGRHrs, 0) totalCreditPostGRHrs,
        coalesce(courseCnt.totalClockHrs, 0) totalClockHrs, 
-- Exclude Flag for those students whose total course counts equal excluded groups		
        case when courseCnt.totalRemCourses = courseCnt.totalCourses and stuReg.isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
			 when courseCnt.totalCredCourses > 0 --exclude students not enrolled for credit
                    then (case when courseCnt.totalESLCourses = courseCnt.totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
                                when courseCnt.totalCECourses = courseCnt.totalCredCourses then 0 --exclude students enrolled only in continuing ed courses
                                when courseCnt.totalIntlCourses = courseCnt.totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
                                when courseCnt.totalAuditCourses = courseCnt.totalCredCourses then 0 --exclude students exclusively auditing classes
                                    -- when... then 0 --exclude PHD residents or interns 
                                    -- when... then 0 --exclude students in experimental Pell programs
                                else 1
                        end)
            else 0 
        end ipedsInclude
    from RegPerTermAsOfCensus reg  
        inner join PersonPerPeriod person on reg.personId = person.personId 
        inner join CourseCountsPerStudent courseCnt on reg.personId = courseCnt.personId  
        inner join StudentPerTermReg stuReg on stuReg.personId = reg.personId 
        inner join StuMaxLevelPerPeriod maxLevel on maxLevel.personId = reg.personId
    )
where ipedsInclude = 1
),

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
****/

FormatPartAStudentLevel as (
select *
from (
	VALUES 
		(1), --Undergraduate students
		(3)  --Graduate students
	) as StudentLevel(ipedsLevel)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

/* Part A: Unduplicated Count by Student Level, Gender, and Race/Ethnicity
Report all students enrolled for credit at any time during the July 1, 2018 - June 30, 2019 reporting period. 
Students are reported by gender, race/ethnicity, and their level of standing with the institution.
*/

select 'A' part,
		ipedsLevel field1, --valid values are 1 for undergraduate and 3 for graduate
		coalesce(sum(case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end), 0) field2, -- Nonresident alien - Men (1), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end), 0) field3, -- Nonresident alien - Women (2), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end), 0) field4, -- Hispanic/Latino - Men (25), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end), 0) field5, -- Hispanic/Latino - Women (26), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end), 0) field6, -- American Indian or Alaska Native - Men (27), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end), 0) field7, -- American Indian or Alaska Native - Women (28), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end), 0) field8,  -- Asian - Men (29), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end), 0) field9, -- Asian - Women (30), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end), 0) field10, -- Black or African American - Men (31), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end), 0) field11, -- Black or African American - Women (32), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end), 0) field12, -- Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end), 0) field13, -- Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end), 0) field14, -- White - Men (35), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end), 0) field15, -- White - Women (36), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end), 0) field16, -- Two or more races - Men (37), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end), 0) field17, -- Two or more races - Women (38), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end), 0) field18, -- Race and ethnicity unknown - Men (13), 0 to 999999
		coalesce(sum(case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end), 0) field19  -- Race and ethnicity unknown - Women (14), 0 to 999999
from (
	select cohort.personId personId,
		cohort.ipedsPartAStudentLevel ipedsLevel,
		cohort.ipedsGender ipedsGender,
		cohort.ipedsEthnicity ipedsEthnicity
	from StudentCohort cohort
	where cohort.ipedsPartAStudentLevel = 1
	or cohort.ipedsPartAStudentLevel = 3
	
	union
	
	select null, --personId,
		StudentLevel.ipedsLevel,
		null, -- ipedsGender,
		null -- ipedsEthnicity
	from FormatPartAStudentLevel StudentLevel
	)
group by ipedsLevel

union

/* Part B: Instructional Activity and Full-Time Equivalent Enrollment
Report the total clock hour and/or credit hour activity attempted during the 12-month period of July 1, 2018 - June 30, 2019. 
The instructional activity data reported will be used to calculate full-time equivalent (FTE) student enrollment at the institution.
*/

select 'B' part,
		null field1,
		--case when useClockHours = 'All' then null else...
		coalesce(sum(cohort.totalCreditUGHrs), 0) field2, -- credit hour instructional activity at the undergraduate level, 0 to 99999999, blank = not applicable, if no undergraduate level programs are measured in credit hours.
		--case when useClockHours = 'None' then null else ...
		sum(cohort.totalClockHrs) field3, -- clock hour instructional activity at the undergraduate level, 0 to 9999999, blank = not applicable, if no undergraduate programs are measured in clock hours.
		sum(cohort.totalCreditGRHrs) field4, -- credit hour instructional activity at the graduate level, 0 to 99999999, blank = not applicable
		case when sum(cohort.totalCreditPostGRHrs) > 0 then round((sum(cohort.totalCreditPostGRHrs))/18, 0) end field5, -- reported Doctor'92s degree-professional practice student FTE, 0 to 99999999, blank = not applicable
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
		null field18, 
		null field19
from StudentCohort cohort
