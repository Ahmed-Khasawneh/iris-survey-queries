/********************

EVI PRODUCT:    DORIS 2020-21 IPEDS Survey Spring Collection
FILE NAME:      Fall Enrollment v1 (EF1)
FILE DESC:      Fall Enrollment for 4-year, degree-granting institutions
AUTHOR:         Ahmed Khasawneh
CREATED:        20210420

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
20210524    	akhasawneh/jhanicak 						PF-1944 Initial version prod run: 30m 21s test run: 31s 28s
	
********************/

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.

--Production Default (Begin)
select '2021' surveyYear, 
	'EF1' surveyId,
	'Fall Census' repPeriodTag1, --Academic Reporters
	'Pre-Fall Summer Census' repPeriodTag2, --Academic Reporters, if applicable
	'HR Reporting End' repPeriodTag3, --Student to Faculty Ratio, use 20-21 HR
	--'October End' repPeriodTag4, --Program Reporters
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2020-08-01' AS DATE) reportingDateStart, --Program Reporters
	CAST('2020-10-31' AS DATE) reportingDateEnd, --Program Reporters
	'202010' termCode, --Fall 2020
	'1' partOfTermCode,
	'M' genderForUnknown,   --‘Valid values: M = Male, F = Female; Default value (if no record or null value): M’
	'F' genderForNonBinary, --‘Valid values: M = Male, F = Female; Default value (if no record or null value): F’
    'CR' instructionalActivityType, --‘Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR’
	CAST('2020-11-01' AS DATE) asOfDateHR,
	'Y' feIncludeOptSurveyData, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'COHORT' surveySection

/*
--Test Default (Begin)
select '1415' surveyYear, 
	'EF1' surveyId,
	'Fall Census' repPeriodTag1, --Academic Reporters
	'Pre-Fall Summer Census' repPeriodTag2, --Academic Reporters, if applicable
	'HR Reporting End' repPeriodTag3, --Student to Faculty Ratio, use 20-21 HR
	--'October End' repPeriodTag4, --Program Reporters
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2014-08-01' AS DATE) reportingDateStart, --Program Reporters
	CAST('2014-10-31' AS DATE) reportingDateEnd, --Program Reporters
	'201510' termCode, --Fall 2014
	'1' partOfTermCode,
	'M' genderForUnknown,   --‘Valid values: M = Male, F = Female; Default value (if no record or null value): M’
	'F' genderForNonBinary, --‘Valid values: M = Male, F = Female; Default value (if no record or null value): F’
    'CR' instructionalActivityType, --‘Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR’
	CAST('2014-11-01' AS DATE) asOfDateHR,
	'Y' feIncludeOptSurveyData, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'COHORT' surveySection
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
    RepDates.surveySection surveySection,
    RepDates.snapshotDate snapshotDate,
    RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,
	RepDates.reportingDateStart reportingDateStart,
    RepDates.reportingDateEnd reportingDateEnd
from (
    select repperiodENT.surveyCollectionYear surveyYear,
	    'IPEDSReportingPeriod' source,
		repperiodENT.snapshotDate snapshotDate,
		defvalues.surveyId surveyId,
		(case when upper(repPeriodENT.surveySection) in ('COHORT', 'FALL') then 'COHORT'
		        when upper(repPeriodENT.surveySection) = 'PRIOR SUMMER' then 'PRIOR SUMMER'
		        when upper(repPeriodENT.surveySection) in ('PRIOR YEAR 1 COHORT', 'RETENTION FALL') then 'PRIOR YEAR 1 COHORT'
		        when upper(repPeriodENT.surveySection) in ('PRIOR YEAR 1 PRIOR SUMMER', 'RETENTION PRIOR SUMMER') then 'PRIOR YEAR 1 PRIOR SUMMER'
		        else 'COHORT'
		  end) surveySection,
		to_date(defvalues.reportingDateStart, 'YYYY-MM-DD') reportingDateStart,
		to_date(defvalues.reportingDateEnd, 'YYYY-MM-DD') reportingDateEnd,
		upper(repperiodENT.termCode) termCode,
		coalesce(upper(repperiodENT.partOfTermCode), 1) partOfTermCode,
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
		    cross join DefaultValues defvalues 
	   where upper(repperiodENT.surveyId) = defvalues.surveyId
	        and repperiodENT.surveyCollectionYear = defvalues.surveyYear
			and repperiodENT.termCode is not null
	
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
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
		1 reportPeriodRn
	from DefaultValues defvalues
    where defvalues.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = defvalues.surveyYear
											and upper(repperiodENT.surveyId) = defvalues.surveyId 
											and repperiodENT.termCode is not null
										) 
    ) RepDates
where RepDates.reportPeriodRn = 1
),

ClientConfigMCR as ( 
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

--          1st union 1st order - pull snapshot if the same as ReportingPeriodMCR snapshot 
--          1st union 2nd order - pull snapshot > ReportingPeriodMCR snapshot 
--          1st union 3rd order - pull snapshot < ReportingPeriodMCR snapshot 
--          2nd union - pull default values if no record in IPEDSClientConfig

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.source source,
    ConfigLatest.snapshotDate snapshotDate,
    ConfigLatest.repperiodSnapshotDate repperiodSnapshotDate,
	ConfigLatest.genderForUnknown genderForUnknown,
	ConfigLatest.genderForNonBinary genderForNonBinary,
    ConfigLatest.instructionalActivityType instructionalActivityType,
    ConfigLatest.feIncludeOptSurveyData feIncludeOptSurveyData,
    ConfigLatest.repPeriodTag1 repPeriodTag1, --'Fall Census'
	ConfigLatest.repPeriodTag2 repPeriodTag2, --'Pre-Fall Summer Census' 
    ConfigLatest.repPeriodTag3 repPeriodTag3, --'HR Reporting End'
	(case when mod(ConfigLatest.surveyYear,2) = 0 then 'Y' --then odd first year and age is required
		else ConfigLatest.feIncludeOptSurveyData
	end) reportAge,
	(case when mod(ConfigLatest.surveyYear,2) != 0 then 'Y' --then even first year and resid is required
		else ConfigLatest.feIncludeOptSurveyData
	end) reportResidency,
    (case when mod(ConfigLatest.surveyYear,2) != 0 then 'Y' --then even first year and resid is required
		else null
	end) reportCIP,
	ConfigLatest.asOfDateHR asOfDateHR,
    ConfigLatest.surveyId surveyId
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'configFullYearTag' source,
		clientConfigENT.snapshotDate snapshotDate, 
		repperiod.snapshotDate repperiodSnapshotDate,
		coalesce(upper(clientConfigENT.genderForUnknown), defvalues.genderForUnknown) genderForUnknown,
		coalesce(upper(clientConfigENT.genderForNonBinary), defvalues.genderForNonBinary) genderForNonBinary,
        coalesce(upper(clientConfigENT.instructionalActivityType), defvalues.instructionalActivityType) instructionalActivityType,
	    coalesce(upper(clientConfigENT.feIncludeOptSurveyData), defvalues.feIncludeOptSurveyData) feIncludeOptSurveyData,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    defvalues.repPeriodTag3 repPeriodTag3,
		defvalues.asOfDateHR asOfDateHR,
	    defvalues.surveyId surveyId,
		row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
			    (case when clientConfigENT.snapshotDate = repperiod.snapshotDate then 1 else 2 end) asc,
			    (case when clientConfigENT.snapshotDate > repperiod.snapshotDate then clientConfigENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when clientConfigENT.snapshotDate < repperiod.snapshotDate then clientConfigENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
				clientConfigENT.recordActivityDate desc
		) configRn
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
	    defvalues.feIncludeOptSurveyData feIncludeOptSurveyData,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    defvalues.repPeriodTag3 repPeriodTag3,
		defvalues.asOfDateHR asOfDateHR,
	    defvalues.surveyId surveyId, 
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
	snapshotDate snapshotDate,
	startDate startDate,
	endDate endDate,
	academicYear,
	censusDate censusDate,
    termType,
    termClassification,
	requiredFTCreditHoursGR,
	requiredFTCreditHoursUG,
	requiredFTClockHoursUG,
    tags
from ( 
    select distinct upper(acadtermENT.termCode) termCode, 
        row_number() over (
            partition by 
                acadTermENT.snapshotDate,
                acadTermENT.termCode,
                acadTermENT.partOfTermCode
            order by
               acadTermENT.recordActivityDate desc
        ) acadTermRn,
        acadTermENT.snapshotDate snapshotDate,
        acadTermENT.tags,
		coalesce(upper(acadtermENT.partOfTermCode), 1) partOfTermCode, 
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
    max(censusDate) maxCensus,
    min(startDate) minStart,
    max(endDate) maxEnd,
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
        repPerTerms.reportAge,
        repPerTerms.reportResidency,
        repPerTerms.reportCIP,
	    repPerTerms.genderForUnknown genderForUnknown,
	    repPerTerms.genderForNonBinary genderForNonBinary,
	    repPerTerms.asOfDateHR,
	    repPerTerms.repPeriodTag3,
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
        (case when repperiod.surveySection in ('COHORT', 'PRIOR SUMMER') then 'CY'
            when repperiod.surveySection in ('PRIOR YEAR 1 COHORT', 'PRIOR YEAR 1 PRIOR SUMMER') then 'PY' end) yearType,
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
		clientconfig.instructionalActivityType,
	    coalesce(acadterm.requiredFTCreditHoursUG/
		    coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
        clientconfig.reportAge,
        clientconfig.reportResidency,
        clientconfig.reportCIP,
		clientconfig.genderForUnknown genderForUnknown,
	    clientconfig.genderForNonBinary genderForNonBinary,
	    clientconfig.asOfDateHR asOfDateHR,
	    clientconfig.repPeriodTag3 repPeriodTag3,
		row_number() over (
            partition by 
                repperiod.termCode,
                repperiod.partOfTermCode
            order by
                (case when to_date(acadterm.snapshotDate, 'YYYY-MM-DD') <= date_add(acadterm.censusdate, 3) 
                            and to_date(acadterm.snapshotDate, 'YYYY-MM-DD') >= date_sub(acadterm.censusDate, 1) 
                            and ((array_contains(acadterm.tags, clientconfig.repPeriodTag1) and acadterm.termType = 'Fall' and repperiod.surveySection in ('COHORT', 'PRIOR YEAR 1 COHORT'))
                                or (array_contains(acadterm.tags, clientconfig.repPeriodTag2) and acadterm.termType = 'Summer' and repperiod.surveySection in ('PRIOR SUMMER','PRIOR YEAR 1 PRIOR SUMMER'))) then 1
                      when to_date(acadterm.snapshotDate, 'YYYY-MM-DD') <= date_add(acadterm.censusdate, 3) 
                            and to_date(acadterm.snapshotDate, 'YYYY-MM-DD') >= date_sub(acadterm.censusDate, 1) then 2
                     else 3 end) asc,
                (case when to_date(acadterm.snapshotDate, 'YYYY-MM-DD') > acadterm.censusDate then acadterm.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(acadterm.snapshotDate, 'YYYY-MM-DD') < acadterm.censusDate then acadterm.snapshotDate else CAST('1900-09-09' as DATE) end) desc
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
        (case when rep.termType = 'Summer' and rep.termClassification != 'Standard Length' then 
                    case when (select max(rep2.termOrder)
                    from AcademicTermReporting rep2
                    where rep2.termType = 'Summer') < (select max(rep2.termOrder)
                                                        from AcademicTermReporting rep2
                                                        where rep2.termType = 'Fall') then 'Pre-Fall Summer'
                    else 'Post-Spring Summer' end
                else rep.termType 
        end) termTypeNew,
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

CampusMCR as ( --11s
-- Returns most recent campus record for all snapshots in the ReportingPeriod

select campus,
	isInternational,
	snapshotDate
from ( 
    select upper(campusENT.campus) campus,
		campusENT.campusDescription,
		coalesce(campusENT.isInternational, false) isInternational,
		campusENT.snapshotDate snapshotDate,
		row_number() over (
			partition by
			    campusENT.snapshotDate, 
				campusENT.campus
			order by
				campusENT.recordActivityDate desc
		) campusRn
	from Campus campusENT 
    where coalesce(campusENT.isIpedsReportable, true) = true
        and ((coalesce(to_date(campusENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                and to_date(campusENT.recordActivityDate, 'YYYY-MM-DD') <= to_date(campusENT.snapshotDate, 'YYYY-MM-DD'))
            or coalesce(to_date(campusENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
	)
where campusRn = 1
),

CourseSectionMCR as (
--Included to get enrollment hours of a courseSectionNumber

select yearType,
        surveySection,
        snapshotDate,
        termCode,
        partOfTermCode,
        censusDate,
        termType,
        termOrder,
        courseSectionNumber,
        courseSectionLevel,
        courseSectionStatus,
        equivCRHRFactor,
        enrollmentHours,
        instructionalActivityType,
        subject,
        courseNumber,
        isESL,
        isRemedial,
        isClockHours       
from (
    select repperiod.yearType,
        repperiod.surveySection,
        repperiod.snapshotDate,
        repperiod.termCode,
        repperiod.partOfTermCode,
        repperiod.censusDate,
        repperiod.termType,
        repperiod.termOrder,
        repperiod.equivCRHRFactor,
        repperiod.instructionalActivityType,
        upper(coursesectENT.courseSectionNumber) courseSectionNumber,
        --coalesce(reg.courseSectionLevelOverride, coursesectENT.courseSectionLevel) courseSectionLevel, --reg level prioritized over courseSection level 
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
        --coalesce(reg.enrollmentHoursOverride, coursesectENT.enrollmentHours) enrollmentHours, --reg enr hours prioritized over courseSection enr hours
        coursesectENT.enrollmentHours,
        coalesce(coursesectENT.isClockHours, false) isClockHours,
        coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        coalesce(row_number() over (
                partition by
                    repperiod.yearType,
                    repperiod.termCode,
                    repperiod.partOfTermCode,
                    coursesectENT.courseSectionNumber,
                    coursesectENT.courseSectionLevel
                order by
                    (case when coursesectENT.snapshotDate = repperiod.snapshotDate then 1 else 2 end) asc,
                    (case when coursesectENT.snapshotDate > repperiod.snapshotDate then coursesectENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                    (case when coursesectENT.snapshotDate < repperiod.snapshotDate then coursesectENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                    coursesectENT.recordActivityDate desc
            ), 1) courseRn
    from AcademicTermReportingRefactor repperiod   
        inner join CourseSection coursesectENT on repperiod.termCode = upper(coursesectENT.termCode)
            and repperiod.partOfTermCode = coalesce(upper(coursesectENT.partOfTermCode), 1)
            --and reg.courseSectionNumber = upper(coursesectENT.courseSectionNumber)
            and coalesce(coursesectENT.isIpedsReportable, true) = true
            and ((coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                    and to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') <= repperiod.censusDate)
                or coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))  
    )
where courseRn = 1
and ((recordActivityDate != CAST('9999-09-09' AS DATE)
        and courseSectionStatus = 'Active')
    or recordActivityDate = CAST('9999-09-09' AS DATE))
),

CourseSectionScheduleMCR as (
--Returns course scheduling related info for the registration courseSectionNumber. 
--Maintains cohort records for current and prior year.

select yearType,
        surveySection,
        snapshotDate,
        termCode,
        partOfTermCode,
        censusDate,
        termType,
        termOrder,
        courseSectionNumber,
        equivCRHRFactor,
        enrollmentHours,
        instructionalActivityType,
        courseSectionLevel,
        subject,
        courseNumber,
        isESL,
        isRemedial,
        isClockHours,
        campus,
        instructionType,
		locationType,
		distanceEducationType,
		onlineInstructionType
from (
	select coursesect.*,
			upper(coursesectschedENT.campus) campus,
			coursesectschedENT.instructionType,
			coursesectschedENT.locationType,
			coursesectschedENT.distanceEducationType,
			coursesectschedENT.onlineInstructionType,
			coalesce(row_number() over (
				partition by
					coursesect.yearType,
					coursesect.termCode, 
					coursesect.partOfTermCode,
					coursesect.courseSectionNumber,
					coursesect.courseSectionLevel
				order by
					(case when coursesectschedENT.snapshotDate = coursesect.snapshotDate then 1 else 2 end) asc,
					(case when coursesectschedENT.snapshotDate > coursesect.snapshotDate then coursesectschedENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
					(case when coursesectschedENT.snapshotDate < coursesect.snapshotDate then coursesectschedENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
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
	where CourseData.courseSectSchedRn = 1
),

CourseMCR as (
--Included to get course type information
--Maintains cohort records for current and prior year.

select yearType,
        surveySection,
        snapshotDate,
        termCode,
        partOfTermCode,
        censusDate,
        termType,
        termOrder,
        courseSectionNumber,
        equivCRHRFactor,
        enrollmentHours,       
        (case when instructionalActivityType = 'CR' then enrollmentHours
		      when isClockHours = false then enrollmentHours
              when isClockHours = true and instructionalActivityType = 'B' then equivCRHRFactor * enrollmentHours
        else enrollmentHours end) enrollmentHoursCalc,
        instructionalActivityType,
        courseSectionLevelCalc courseSectionLevel,
        subject,
        courseNumber,
        isESL,
        isRemedial,
        isClockHours,
        campus,
        instructionType,
		locationType,
		distanceEducationType,
		onlineInstructionType
from (
	select coursesectsched.*,
		coalesce(coursesectsched.courseSectionLevel, courseENT.courseLevel) courseSectionLevelCalc,
	    coalesce(row_number() over (
			partition by
			    coursesectsched.yearType,
                coursesectsched.termCode, 
				coursesectsched.partOfTermCode,
			    coursesectsched.courseSectionNumber,
			    coalesce(coursesectsched.courseSectionLevel, courseENT.courseLevel),
			    coursesectsched.subject,
                courseENT.subject,
                coursesectsched.courseNumber,
                courseENT.courseNumber
			order by
			    (case when courseENT.snapshotDate = coursesectsched.snapshotDate then 1 else 2 end) asc,
                (case when courseENT.snapshotDate > coursesectsched.snapshotDate then courseENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when courseENT.snapshotDate < coursesectsched.snapshotDate then courseENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
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
),

RegistrationMCR as ( 
--Returns all student enrollment records as of the term within period and where course is viable
--It also is pulling back the most recent version of registration data prior to the census date of that term. 
--Returns valid 'Enrolled' registration records for all reportable terms from current and prior year cohort.

select *
from (
    select regData.yearType,
            regData.termCode,
            regData.partOfTermCode,
            regData.censusDate,
            regData.termType,
            regData.termOrder,
            regData.personId,
            regData.courseSectionNumber,
            regData.isAudited,
            (case when regData.hoursOverride = 0 then regData.enrollmentHoursCalc
                  else (case when regData.instructionalActivityType = 'CR' then regData.enrollmentHours
                            when regData.isClockHours = false then regData.enrollmentHours
                            when regData.isClockHours = true and regData.instructionalActivityType = 'B' then regData.equivCRHRFactor * regData.enrollmentHours
                            else regData.enrollmentHours end)
            end) enrollmentHours,
            regData.instructionalActivityType,
            regData.courseSectionLevelCalc courseSectionLevel,
            regData.isESL,
            regData.isRemedial,
            regData.isClockHours,
            regData.instructionType,
            regData.locationType,
            regData.distanceEducationType,
            regData.onlineInstructionType,
            coalesce(campus.isInternational, false) isInternational,
            coalesce(row_number() over (
                    partition by
                        regData.yearType,
                        regData.termCode,
                        regData.partOfTermCode,
                        regData.personId,
                        regData.courseSectionNumber,
                        regData.courseSectionLevelCalc
                    order by 
                        (case when campus.snapshotDate = regData.snapshotDate then 1 else 2 end) asc,
                        (case when campus.snapshotDate > regData.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                        (case when campus.snapshotDate < regData.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc
                ), 1) regCampRn
    from (        
        select repperiod.yearType yearType, 
                repperiod.surveySection surveySection,
                repperiod.snapshotDate snapshotDate,
                upper(regENT.termCode) termCode,
                coalesce(upper(regENT.partOfTermCode), 1) partOfTermCode,
                repperiod.termorder termorder,
                repperiod.censusDate censusDate,
                repperiod.termTypeNew termType,
                regENT.personId personId,            
                repperiod.instructionalActivityType,
                repperiod.equivCRHRFactor,
                coalesce(upper(regENT.courseSectionCampusOverride), course.campus) campus,
                coalesce(regENT.isAudited, false) isAudited,
                coalesce(regENT.isEnrolled, true) isEnrolled,
                coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel) courseSectionLevelCalc,
                upper(regENT.courseSectionNumber) courseSectionNumber,
                coalesce(regENT.enrollmentHoursOverride, course.enrollmentHours) enrollmentHours,
                (case when regENT.enrollmentHoursOverride is not null then 1 else 0 end) hoursOverride,
                course.enrollmentHoursCalc,
                course.isESL,
                course.isRemedial,
                course.isClockHours,
                course.instructionType,
                course.locationType,
                course.distanceEducationType,
                course.onlineInstructionType,
                coalesce(row_number() over (
                    partition by
                        repperiod.yearType,
                        repperiod.surveySection,
                        regENT.termCode,
                        regENT.partOfTermCode,
                        regENT.personId,
                        regENT.courseSectionNumber,
                        coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel)
                    order by 
                        (case when regENT.snapshotDate = repperiod.snapshotDate then 1 else 2 end) asc,
                        (case when regENT.snapshotDate > repperiod.snapshotDate then regENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                        (case when regENT.snapshotDate < repperiod.snapshotDate then regENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                        regENT.recordActivityDate desc,
                        regENT.registrationStatusActionDate desc
                ), 1) regRn
            from AcademicTermReportingRefactor repperiod   
                inner join Registration regENT on upper(regENT.termCode) = repperiod.termCode
                    and coalesce(upper(regENT.partOfTermCode), 1) = repperiod.partOfTermCode 
                    and coalesce(regENT.isIpedsReportable, true) = true
                    and ((coalesce(to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' AS DATE)
                                and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
                            or (coalesce(to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' AS DATE)
                                and ((coalesce(to_date(regENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                                        and to_date(regENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                                    or coalesce(to_date(regENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))))                
             left join CourseMCR course on upper(regENT.courseSectionNumber) = course.courseSectionNumber
                    and upper(regENT.termCode) = course.termCode
                    and coalesce(upper(regENT.partOfTermCode), 1) = course.partOfTermCode
            ) regData
        left join CampusMCR campus on regData.campus = campus.campus
    where regData.regRn = 1
        and regData.isEnrolled = true
    )
where regCampRn = 1
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  
--Includes student data for both current and prior year cohort.

select *
from (
    select studData.*,
    reg.personId regPersonId,
    reg.termCode regTermCode
from (
    select stuData.yearType,
            stuData.surveySection,
            stuData.snapshotDate,
            stuData.termCode, 
            stuData.termOrder,
            stuData.termType,
            stuData.censusDate,
            stuData.requiredFTCreditHoursGR,
            stuData.requiredFTCreditHoursUG,
            stuData.requiredFTClockHoursUG,
            stuData.genderForUnknown,
            stuData.genderForNonBinary,			   
            stuData.instructionalActivityType,
            stuData.fullTermOrder,
            stuData.reportAge,
            stuData.reportResidency,
            stuData.reportCIP,
            stuData.personId,
            coalesce((case when stuData.studentType = 'High School' then true
                        when stuData.studentType = 'Visiting' then true
                        when stuData.studentType = 'Unknown' then true
                        when stuData.studentLevel = 'Continuing Education' then true
                        when stuData.studentLevel = 'Other' then true
                        when studata.studyAbroadStatus = 'Study Abroad - Host Institution' then true
                      else stuData.isNonDegreeSeeking end), false) isNonDegreeSeeking,
            stuData.fullTimePartTimeStatus,
            stuData.studentLevel,
            stuData.studentType,
            stuData.residency,
            stuData.homeCampus,
            studata.studyAbroadStatus
    from ( 
         select repperiod.yearType yearType,
                repperiod.snapshotDate snapshotDate,
                repperiod.surveySection surveySection,
                repperiod.termCode termCode, 
                repperiod.termOrder termOrder,
                repperiod.censusDate censusDate,
                repperiod.requiredFTCreditHoursGR,
                repperiod.requiredFTCreditHoursUG,
                repperiod.requiredFTClockHoursUG,
                repperiod.genderForUnknown,
                repperiod.genderForNonBinary,			   
                repperiod.instructionalActivityType,
                repperiod.termType termType,
                repperiod.fullTermOrder fullTermOrder, --1 for 'full' (standard), 2 for non-standard
                repperiod.reportAge,
                repperiod.reportResidency,
                repperiod.reportCIP,
                studentENT.personId personId,
                studentENT.isNonDegreeSeeking isNonDegreeSeeking,
                studentENT.studentLevel studentLevel,
    --TESTING only
                --(case when studentENT.personId in ('36401', '39724') and studentENT.termCode = '201410' then 'First Time' 
                --      when studentENT.personId in ('36314', '37244') and studentENT.termCode = '201330' then 'First Time' --38816
                --      when studentENT.personId in ('37445', '38804') and studentENT.termCode = '201410' then 'First Time' 
                --else studentENT.studentType end) studentType,
                studentENT.studentType studentType,
                studentENT.residency residency,
                upper(studentENT.homeCampus) homeCampus,
                studentENT.fullTimePartTimeStatus,
    --TESTING only
                --(case when studentENT.personId in ('36401', '39726') and studentENT.termCode = '201510' then 'Study Abroad - Home Institution' 
                --      when studentENT.personId in ('39724', '39394') and studentENT.termCode = '201410' then 'Study Abroad - Home Institution'
                --      else studentENT.studyAbroadStatus 
                -- end) studyAbroadStatus,
                studentENT.studyAbroadStatus studyAbroadStatus,
                coalesce(row_number() over (
                    partition by
                        repperiod.yearType,
                        repperiod.surveySection,
                        studentENT.personId,                    
                        studentENT.termCode
                    order by
                        (case when studentENT.snapshotDate = repperiod.snapshotDate then 1 else 2 end) asc,
                        (case when studentENT.snapshotDate > repperiod.snapshotDate then studentENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                        (case when studentENT.snapshotDate < repperiod.snapshotDate then studentENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                        studentENT.recordActivityDate desc
                ), 1) studRn
        from AcademicTermReportingRefactor repperiod   
                inner join Student studentENT on upper(studentENT.termCode) = repperiod.termCode
                    and coalesce(studentENT.isIpedsReportable, true) = true
                    and ((coalesce(to_date(studentENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)  
                    and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                        or coalesce(to_date(studentENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  = CAST('9999-09-09' AS DATE))
        ) stuData
    where stuData.studRn = 1 
    ) studData
    left join RegistrationMCR reg on studData.personId = reg.personId
        and studData.termCode = reg.termCode
   )
where studyAbroadStatus = 'Study Abroad - Home Institution'
 or regPersonId is not null
),

CourseTypeCountsSTU as (
-- View used to break down course category type counts for student
--Maintains cohort records for current and prior year.
-- In order to calculate credits and filters per term, do not include censusDate or snapshotDate in any inner view

select *,
            (case when studentLevelUGGR = 'UG' and totalCreditHrs is not null and totalClockHrs is not null then
                    (case when instructionalActivityType in ('CR', 'B') then 
                                (case when totalCreditHrs >= requiredFTCreditHoursUG then 'Full Time' else 'Part Time' end)
                          when instructionalActivityType = 'CL' then 
                                (case when totalClockHrs >= requiredFTClockHoursUG then 'Full Time' else 'Part Time' end) 
                          else null end)
                    when studentLevelUGGR = 'GR' and totalCreditHrs is not null then
                        (case when totalCreditHrs >= requiredFTCreditHoursGR then 'Full Time' else 'Part Time' end)
                else null end) timeStatus
	from (
		select stu.yearType,
            stu.surveySection,
            stu.snapshotDate,
            stu.personId,
            stu.termCode, 
            stu.termOrder,
            stu.termType,
            stu.censusDate,
            stu.fullTermOrder,
            stu.reportAge,
            stu.reportResidency,
            stu.reportCIP,
            stu.studentType,
            stu.studentLevel studentLevel,
            (case when stu.studentLevel in ('Masters', 'Doctorate', 'Professional Practice Doctorate') then 'GR'
                else 'UG' 
            end) studentLevelUGGR,
            stu.instructionalActivityType,
			stu.requiredFTCreditHoursUG,
			stu.requiredFTClockHoursUG,
			stu.requiredFTCreditHoursGR,
            stu.genderForUnknown,
            stu.genderForNonBinary,
            stu.residency,
            stu.studyAbroadStatus,
			(case when studyAbroadStatus != 'Study Abroad - Home Institution' then isNonDegreeSeeking
				  when totalSAHomeCourses > 0 or totalCreditHrs > 0 or totalClockHrs > 0 then false 
				  else isNonDegreeSeeking 
			  end) isNonDegreeSeeking,
			courseCnt.totalCreditHrs,
			courseCnt.totalClockHrs,
			courseCnt.totalCECourses,
			courseCnt.totalIntlCourses,
			courseCnt.totalAuditCourses,
			courseCnt.totalResidencyCourses,
			courseCnt.totalSAHomeCourses,
			(case when courseCnt.totalDECourses = courseCnt.totalCourses then 'Exclusive DE'
			     when courseCnt.totalDECourses > 0 then 'Some DE'
		         else 'None' 
	           end) distanceEdInd,
			(case when courseCnt.totalCECourses = totalCourses then 0 --exclude students enrolled only in continuing ed courses
				when courseCnt.totalIntlCourses = totalCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
				when courseCnt.totalAuditCourses = totalCourses then 0 --exclude students exclusively auditing classes
				when courseCnt.totalResidencyCourses > 0 and stu.studentLevel = 'Professional Practice Doctorate' then 0 --exclude PHD residents or interns
				--when courseCnt.totalThesisCourses > 0 then 0 --exclude PHD residents or interns
				--when courseCnt.totalRemCourses = courseCnt.totalCourses and courseCnt.isNonDegreeSeeking = false then 1 --include students taking remedial courses if degree-seeking
				--when courseCnt.totalESLCourses = courseCnt.totalCourses and courseCnt.isNonDegreeSeeking = false then 1 --exclude students enrolled only in ESL courses/programs
				when courseCnt.totalSAHomeCourses > 0 then 1 --include study abroad student where home institution provides resources, even if credit hours = 0
				when courseCnt.totalCreditHrs > 0 then 1
				when courseCnt.totalClockHrs > 0 then 1
				--when stu.studyAbroadStatus = 'Study Abroad - Home Institution' then 1 
				else 0
			 end) ipedsEnrolled
        from StudentMCR stu
            left join (                 
                select reg.personId,
                    reg.termCode, 
                    coalesce(count(reg.courseSectionNumber), 0) totalCourses,
                    coalesce(sum((case when reg.enrollmentHours >= 0 then 1 else 0 end)), 0) totalCreditCourses,
                    coalesce(sum((case when reg.isClockHours = false then reg.enrollmentHours else 0 end)), 0) totalCreditHrs,
                    coalesce(sum((case when reg.isClockHours = true and reg.courseSectionLevel = 'Undergraduate' then reg.enrollmentHours else 0 end)), 0) totalClockHrs,
                    coalesce(sum((case when reg.courseSectionLevel = 'Continuing Education' then 1 else 0 end)), 0) totalCECourses,
                    coalesce(sum((case when reg.locationType = 'Foreign Country' then 1 else 0 end)), 0) totalSAHomeCourses, 
                    coalesce(sum((case when reg.isESL = true then 1 else 0 end)), 0) totalESLCourses,
                    coalesce(sum((case when reg.isRemedial = true then 1 else 0 end)), 0) totalRemCourses,
                    coalesce(sum((case when reg.isInternational = true then 1 else 0 end)), 0) totalIntlCourses,
                    coalesce(sum((case when reg.isAudited = true then 1 else 0 end)), 0) totalAuditCourses,
                    coalesce(sum((case when reg.instructionType = 'Thesis/Capstone' then 1 else 0 end)), 0) totalThesisCourses,
                    coalesce(sum((case when reg.instructionType in ('Residency', 'Internship', 'Practicum') then 1 else 0 end)), 0) totalResidencyCourses,
                    coalesce(sum((case when reg.distanceEducationType != 'Not distance education' then 1 else 0 end)), 0) totalDECourses
                from RegistrationMCR reg
                group by reg.personId,
                    reg.termCode
            ) courseCnt on stu.personId = courseCnt.personId
                    and stu.termCode = courseCnt.termCode
		) 
),

StudentRefactor as (
--Determine student info based on full term and degree-seeking status
--Drop surveySection from select fields and use yearType only going forward - Prior Summer sections only used to determine student type

select *
from ( 
    select FallStu.personId personId,
            FallStu.surveySection surveySection,
            FallStu.termCode termCode,
            FallStu.yearType yearType,
            (case when FallStu.studentType = 'Continuing' and SumStu.personId is not null then SumStu.studentType else FallStu.studentType end) studentTypeCalc,
            FallStu.studentType,
            SumStu.studentType summStudType,
            FallStu.isNonDegreeSeeking isNonDegreeSeeking,
            FallStu.snapshotDate snapshotDate,
            FallStu.censusDate censusDate,
            FallStu.termOrder termOrder,
            FallStu.fullTermOrder,
            FallStu.reportAge,
            FallStu.reportResidency,
            FallStu.reportCIP,
            FallStu.termType,
            FallStu.studentLevel studentLevel,
            FallStu.studentLevelUGGR studentLevelUGGR,
            FallStu.genderForUnknown,
            FallStu.genderForNonBinary,
            FallStu.residency,
            FallStu.studyAbroadStatus,
            SumStu.studyAbroadStatus summSA,
            FallStu.timeStatus,
            SumStu.timeStatus summTimeStatus,
			FallStu.totalCreditHrs,
			FallStu.totalClockHrs,
			FallStu.totalCECourses,
			FallStu.totalIntlCourses,
			FallStu.totalAuditCourses,
			FallStu.totalResidencyCourses,
			FallStu.totalSAHomeCourses,
			FallStu.distanceEdInd,
            FallStu.ipedsEnrolled
    from (
        select *
         from ( 
            select stu.yearType,
                stu.surveySection,
                stu.snapshotDate,
                stu.personId,
                stu.termCode, 
                stu.termOrder,
                stu.termType,
                stu.censusDate,
                stu.fullTermOrder,
                stu.reportAge,
                stu.reportResidency,
                stu.reportCIP,
                stu.studentType,
                stu.isNonDegreeSeeking,
                stu.studentLevel,
                stu.studentLevelUGGR,
                stu.genderForUnknown,
                stu.genderForNonBinary,
                stu.residency,
                stu.studyAbroadStatus,
                stu.timeStatus,
                stu.totalCreditHrs,
                stu.totalClockHrs,
                stu.totalCECourses,
                stu.totalIntlCourses,
                stu.totalAuditCourses,
                stu.totalResidencyCourses,
                stu.totalSAHomeCourses,
                stu.distanceEdInd,
                stu.ipedsEnrolled
            from CourseTypeCountsSTU stu
            where stu.fullTermOrder = 1 --1 is full term, 2 is not full term
                and stu.termType = 'Fall'
                and (stu.ipedsEnrolled = 1
                    or stu.studyAbroadStatus = 'Study Abroad - Home Institution')
           )
        ) FallStu
        left join (
            select stu2.yearType yearType,
                  stu2.personId personId,
                  stu2.studentType studentType,
                  stu2.timeStatus timeStatus,
                  stu2.studyAbroadStatus studyAbroadStatus
            from CourseTypeCountsSTU stu2
            where stu2.fullTermOrder = 2
                    and stu2.termType != 'Fall'
                    and stu2.studentType in ('First Time', 'Transfer')
            ) SumStu on FallStu.personId = SumStu.personId 
                            and FallStu.yearType = SumStu.yearType
    )
where (yearType = 'CY'
or (yearType = 'PY'
    and studentTypeCalc = 'First Time'))
),

AcademicTrackMCR as (
--Returns most up to date student academic track information for the enrollment term. 

select *
from (
    select distinct person2.personId,
            person2.yearType,
            acadtrackENT.personId atPersonId,
            person2.snapshotDate,
            person2.termCode,
            person2.termOrder,
            person2.censusDate,
            person2.reportAge,
            person2.reportResidency,
            person2.reportCIP,
            person2.studentTypeCalc studentType,
            person2.isNonDegreeSeeking,
            person2.studentLevel,
            person2.studentLevelUGGR,
            person2.genderForUnknown,
            person2.genderForNonBinary,
            person2.residency,
            person2.studyAbroadStatus,
            person2.summSA,
            person2.timeStatus,
            person2.summTimeStatus,
            person2.distanceEdInd,
            person2.ipedsEnrolled,
            coalesce(row_number() over (
                partition by
                    person2.personId,
                    acadtrackENT.personId,
                    person2.yearType
                order by
                   (case when acadtrackENT.snapshotDate = person2.snapshotDate then 1 else 2 end) asc,
                    (case when acadtrackENT.snapshotDate > person2.snapshotDate then acadtrackENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                    (case when acadtrackENT.snapshotDate < person2.snapshotDate then acadtrackENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                    coalesce(acadtrackENT.fieldOfStudyPriority, 1) asc,
                    termorder.termOrder desc,
                    acadtrackENT.recordActivityDate desc,
                    acadtrackENT.fieldOfStudyActionDate desc,
                    (case when acadtrackENT.academicTrackStatus = 'In Progress' then 1 else 2 end) asc
            ), 1) acadTrackRn,
            upper(acadtrackENT.degreeProgram) degreeProgram
    from StudentRefactor person2
        left join AcademicTrack acadtrackENT on person2.personId = acadTrackENT.personId
            and ((coalesce(to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                        and to_date(acadtrackENT.fieldOfStudyActionDate,'YYYY-MM-DD') <= person2.censusDate)
                    or (coalesce(to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                        and ((coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' as DATE)
                                and to_date(acadtrackENT.recordActivityDate,'YYYY-MM-DD') <= person2.censusDate)
                            or coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' as DATE)))) 
                    and acadTrackENT.fieldOfStudyType = 'Major' 
                    and coalesce(acadTrackENT.isIpedsReportable, true) = true
                    --and coalesce(acadTrackENT.isCurrentFieldOfStudy, true) = true
        left join AcademicTermOrder termorder on termorder.termCode = upper(acadtrackENT.termCodeEffective)
                                    and termorder.termOrder <= person2.termOrder
        )
where acadTrackRn = 1
), 

DegreeProgramMCR as (
--Returns most up to date student academic track information as of their award date and term. 

select *
from (
    select distinct person2.personId,
            person2.yearType,
            person2.atPersonId,
            person2.snapshotDate,
            person2.termCode,
            person2.termOrder,
            person2.censusDate,
            person2.reportAge,
            person2.reportResidency,
            person2.reportCIP,
            person2.studentType,
            person2.isNonDegreeSeeking,
            person2.studentLevel,
            person2.studentLevelUGGR,
            person2.genderForUnknown,
            person2.genderForNonBinary,
            person2.residency,
            person2.studyAbroadStatus,
            person2.summSA,
            person2.timeStatus,
            person2.summTimeStatus,
            person2.distanceEdInd,
            person2.ipedsEnrolled,
            person2.degreeProgram,
            upper(degprogENT.degreeProgram) DPdegreeProgram,
            upper(degprogENT.degree) degree,
            upper(degprogENT.major) major,
            coalesce(degProgENT.isESL, false) isESL,
            coalesce(row_number() over (
                partition by
                    person2.personId,
                    person2.yearType,
                    person2.degreeProgram,
                    degprogENT.degreeProgram
                order by
                    (case when degprogENT.snapshotDate = person2.snapshotDate then 1 else 2 end) asc,
                    (case when degprogENT.snapshotDate > person2.snapshotDate then degprogENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                    (case when degprogENT.snapshotDate < person2.snapshotDate then degprogENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                    termorder.termOrder desc,
                    degprogENT.startDate desc,
                    degprogENT.recordActivityDate desc
            ), 1) degProgRn
    from AcademicTrackMCR person2
        left join DegreeProgram degprogENT on person2.degreeProgram = upper(degprogENT.degreeProgram)
                    and (degprogENT.startDate is null 
                            or to_date(degprogENT.startDate, 'YYYY-MM-DD') <= person2.censusDate) 
                   and coalesce(degprogENT.isIpedsReportable, true) = true
                   and ((coalesce(to_date(degprogENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                               and to_date(degprogENT.recordActivityDate,'YYYY-MM-DD') <= person2.censusDate)
                         or coalesce(to_date(degprogENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
         left join AcademicTermOrder termorder on termorder.termCode = upper(degprogENT.termCodeEffective)
                                and termorder.termOrder <= person2.termOrder
        )
where degProgRn = 1
    and isESL = false
),

FieldOfStudyMCR as ( 
--Returns most up to 'major' information as of the reporting term codes and part of term census periods.
--This information is only required for even-numbered IPEDS reporting years. 
--Maintains cohort records for current and prior year.

select *
from (
	select person2.personId,
            person2.yearType,
            person2.atPersonId,
            person2.snapshotDate,
            person2.termCode,
            person2.termOrder,
            person2.censusDate,
            person2.reportAge,
            person2.reportResidency,
            person2.reportCIP,
            person2.studentType,
            person2.isNonDegreeSeeking,
            person2.studentLevel,
            person2.studentLevelUGGR,
            person2.genderForUnknown,
            person2.genderForNonBinary,
            person2.residency,
            person2.studyAbroadStatus,
            person2.summSA,
            person2.timeStatus,
            person2.summTimeStatus,
            person2.distanceEdInd,
            person2.ipedsEnrolled,
            person2.degreeProgram,
            person2.degree,
            person2.major,
		(case when substr(fieldofstudyENT.cipCode, 0, 2) = '13' then '13.0000'
		    when substr(fieldofstudyENT.cipCode, 0, 2) = '14' then '14.0000'
		    when substr(fieldofstudyENT.cipCode, 0, 2) = '26' then '26.0000'
		    when substr(fieldofstudyENT.cipCode, 0, 2) = '27' then '27.0000'
		    when substr(fieldofstudyENT.cipCode, 0, 2) = '40' then '40.0000'
		    when substr(fieldofstudyENT.cipCode, 0, 2) = '52' then '52.0000'
		    when translate(fieldofstudyENT.cipCode, '.', '') = '220101' then '22.0101'
		    when translate(fieldofstudyENT.cipCode, '.', '') = '510401' then '51.0401'
		    when translate(fieldofstudyENT.cipCode, '.', '') = '511201' then '51.1201'
        else null end) partACipCode,
		coalesce(row_number() over (
			partition by
			    person2.personId,
			    person2.yearType,
                person2.degreeProgram,
                person2.degree,
			    person2.major,
				fieldofstudyENT.fieldOfStudy
			order by
                (case when fieldofstudyENT.snapshotDate = person2.snapshotDate then 1 else 2 end) asc,
                (case when fieldofstudyENT.snapshotDate > person2.snapshotDate then fieldofstudyENT.snapshotDate else CAST('9999-09-09' as DATE) end) desc,
                (case when fieldofstudyENT.snapshotDate < person2.snapshotDate then fieldofstudyENT.snapshotDate else CAST('1900-09-09' as DATE) end) asc,
				fieldofstudyENT.recordActivityDate desc
		), 1) majorRn
	from DegreeProgramMCR person2
		left join fieldOfStudy fieldofstudyENT on upper(fieldofstudyENT.fieldOfStudy) = person2.major
		    and fieldofstudyENT.fieldOfStudyType = 'Major'
            and person2.yearType = 'CY'
            and person2.reportCIP = 'Y'
            and coalesce(fieldofstudyENT.isIpedsReportable, true) = true
			and ((coalesce(to_date(fieldofstudyENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
			        and to_date(fieldofstudyENT.recordActivityDate,'YYYY-MM-DD') <= person2.censusDate)
				or coalesce(to_date(fieldofstudyENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
	)
where majorRn = 1
),

DegreeMCR as (
--Returns most up to 'degree' information as of the reporting term codes and part of term census periods.
--This information is used for Retention cohort filtering on awardLevel
--Maintains cohort records for current and prior year.

select *
from (
	select person2.personId,
            person2.yearType,
            person2.atPersonId,
            person2.snapshotDate,
            person2.termCode,
            person2.termOrder,
            person2.censusDate,
            person2.reportAge,
            person2.reportResidency,
            person2.reportCIP,
            person2.studentType,
            person2.isNonDegreeSeeking,
            person2.studentLevel,
            person2.studentLevelUGGR,
            person2.genderForUnknown,
            person2.genderForNonBinary,
            person2.residency,
            person2.studyAbroadStatus,
            person2.summSA,
            person2.timeStatus,
            person2.summTimeStatus,
            person2.distanceEdInd,
            person2.ipedsEnrolled,
            person2.degreeProgram,
            person2.degree,
            person2.major,
            person2.partACipCode,
		degreeENT.degreeLevel,
		degreeENT.awardLevel,
		coalesce(degreeENT.isNonDegreeSeeking, false) isNonDegreeSeekingDegree,
		coalesce(row_number() over (
			partition by
			    person2.personId,
			    person2.yearType,
			    degreeENT.degreeLevel,
			    degreeENT.awardLevel,
			    person2.degree,
			    degreeENT.degree
			order by
                (case when degreeENT.snapshotDate = person2.snapshotDate then 1 else 2 end) asc,
                (case when degreeENT.snapshotDate > person2.snapshotDate then degreeENT.snapshotDate else CAST('9999-09-09' as DATE) end) desc,
                (case when degreeENT.snapshotDate < person2.snapshotDate then degreeENT.snapshotDate else CAST('1900-09-09' as DATE) end) asc,
				degreeENT.recordActivityDate desc
		), 1) degreeRn
	from FieldOfStudyMCR person2
		left join Degree degreeENT ON upper(degreeENT.degree) = person2.degree
            and person2.yearType = 'PY'
			and coalesce(degreeENT.isIpedsReportable, true) = true
			and ((coalesce(to_date(degreeENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
			   and to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') <= person2.censusDate)
				or coalesce(to_date(degreeENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
	)
where degreeRn = 1
    and (yearType = 'CY'
        or (yearType = 'PY'  
            and awardLevel = 'Bachelors Degree'))
),

AdmissionMCR as (
--Only need to report high school related data on first-time degree/certificate-seeking undergraduate students

select * 
from (
    select person2.personId,
            person2.yearType,
            person2.atPersonId,
            person2.snapshotDate,
            person2.termCode,
            person2.termOrder,
            person2.censusDate,
            person2.reportAge,
            person2.reportResidency,
            person2.reportCIP,
            person2.studentType,
            person2.isNonDegreeSeeking,
            person2.studentLevel,
            person2.studentLevelUGGR,
            person2.genderForUnknown,
            person2.genderForNonBinary,
            person2.residency,
            person2.studyAbroadStatus,
            person2.summSA,
            person2.timeStatus,
            person2.summTimeStatus,
            person2.distanceEdInd,
            person2.ipedsEnrolled,
            person2.degreeProgram,
            person2.degree,
            person2.major,
            person2.partACipCode,
            person2.degreeLevel,
            person2.awardLevel,
            person2.isNonDegreeSeekingDegree,
            admENT.secondarySchoolCompleteDate highSchoolGradDate,
            coalesce(row_number() over (
                partition by
                    person2.yearType,
                    person2.personId
                order by                    
                    (case when admENT.snapshotDate = person2.snapshotDate then 1 else 2 end) asc,
                    (case when admENT.snapshotDate > person2.snapshotDate then admENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                    (case when admENT.snapshotDate < person2.snapshotDate then admENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                    termorder.termOrder desc,
                    termorder2.termOrder desc,
                    admENT.applicationNumber desc,
                    admENT.applicationStatusActionDate desc,
                    admENT.recordActivityDate desc,
                    (case when admENT.applicationStatus in ('Complete', 'Decision Made') then 1 else 2 end) asc
            ), 1) appRn 
    from DegreeMCR person2
        left join Admission admENT on admENT.personId = person2.personId
                and ((coalesce(to_date(admENT.applicationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' AS DATE)
                            and to_date(admENT.applicationStatusActionDate,'YYYY-MM-DD') <= person2.censusDate)
                        or (coalesce(to_date(admENT.applicationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' AS DATE)
                            and ((coalesce(to_date(admENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                                    and to_date(admENT.recordActivityDate,'YYYY-MM-DD') <= person2.censusDate)
                                or coalesce(to_date(admENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))))
                and person2.studentLevelUGGR = 'UG'
                and person2.studentType = 'First Time'
                and person2.isNonDegreeSeeking = false
                and person2.yearType = 'CY'
                and admENT.studentLevel not in ('Masters', 'Doctorate', 'Professional Practice Doctorate')
                -- and admENT.admissionType = 'New Applicant'
                and admENT.admissionDecision is not null
                and admENT.applicationStatus is not null
                and coalesce(admENT.isIpedsReportable, true) = true
        left join AcademicTermOrder termorder on termorder.termCode = upper(admENT.termCodeApplied)
                                    and termorder.termOrder <= person2.termOrder
        left join AcademicTermOrder termorder2 on termorder2.termCode = upper(admENT.termCodeAdmitted)
                                    and termorder2.termOrder <= person2.termOrder
    )
where appRn = 1
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 
--Maintains cohort records for current and prior year.

select personId,
        yearType,
        atPersonId,
        snapshotDate,
        termCode,
        termOrder,
        censusDate,
        reportAge,
        reportResidency,
        reportCIP,
        studentType,
        isNonDegreeSeeking,
        studentLevel,
        studentLevelUGGR,
        residency,
        studyAbroadStatus,
        summSA,
        timeStatus,
        summTimeStatus,
        distanceEdInd,
        ipedsEnrolled,
        degreeProgram,
        degree,
        major,
        partACipCode,
        degreeLevel,
        awardLevel,
        isNonDegreeSeekingDegree,
        highSchoolGradDate,
        state,
        nation,
        (case when gender = 'Male' then 'M'
            when gender = 'Female' then 'F' 
            when gender = 'Non-Binary' then genderForNonBinary
            else genderForUnknown
        end) ipedsGender,
        (case when reportAge = 'Y' then FLOOR(DATEDIFF(to_date(censusDate, 'YYYY-MM-DD'), to_date(birthDate, 'YYYY-MM-DD')) / 365) else null
            end) asOfAge,
        (case when isUSCitizen = true or ((isInUSOnVisa = true or censusDate between visaStartDate and visaEndDate)
                            and visaType in ('Employee Resident', 'Other Resident')) then 
            (case when isHispanic = true then '2' 
                when isMultipleRaces = true then '8' 
                when ethnicity != 'Unknown' and ethnicity is not null then
                    (case when ethnicity = 'Hispanic or Latino' then '2'
                        when ethnicity = 'American Indian or Alaskan Native' then '3'
                        when ethnicity = 'Asian' then '4'
                        when ethnicity = 'Black or African American' then '5'
                        when ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
                        when ethnicity = 'Caucasian' then '7'
                        else '9' 
                    end) 
                else '9' end) -- 'race and ethnicity unknown'
            when ((isInUSOnVisa = true or censusDate between visaStartDate and visaEndDate)
                and visaType in ('Student Non-resident', 'Employee Non-resident', 'Other Non-resident')) then '1' -- 'nonresident alien'
            else '9' -- 'race and ethnicity unknown'
        end) ipedsEthnicity
from ( 
    select distinct stu.*,
            personENT.ethnicity ethnicity,
            coalesce(personENT.isHispanic, false) isHispanic,
            coalesce(personENT.isMultipleRaces, false) isMultipleRaces,
            coalesce(personENT.isInUSOnVisa, false) isInUSOnVisa,
            to_date(personENT.visaStartDate, 'YYYY-MM-DD') visaStartDate,
            to_date(personENT.visaEndDate, 'YYYY-MM-DD') visaEndDate,
            personENT.visaType visaType,
            coalesce(personENT.isUSCitizen, true) isUSCitizen,
            personENT.gender gender,
            to_date(personENT.birthDate, 'YYYY-MM-DD') birthDate,
            upper(personENT.nation) nation,
            upper(personENT.state) state,
            coalesce(row_number() over (
                partition by
                    stu.yearType,
                    stu.personId,
                    personENT.personId
                order by
                    (case when personENT.snapshotDate = stu.snapshotDate then 1 else 2 end) asc,
			        (case when personENT.snapshotDate > stu.snapshotDate then personENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                    (case when personENT.snapshotDate < stu.snapshotDate then personENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                    personENT.recordActivityDate desc
            ), 1) personRn
    from AdmissionMCR stu 
        left join Person personENT on stu.personId = personENT.personId
            and stu.yearType = 'CY'
            and coalesce(personENT.isIpedsReportable = true, true)
			and ((coalesce(to_date(personENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
				and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= stu.censusDate)
				or coalesce(to_date(personENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
    )
where personRn = 1
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

CohortSTU as ( 
-- This view is used to refactor the 'StudentCohort' view records in terms of IPEDS reportable values
-- Only includes current year cohort records that are IPEDS acceptable for reporting. 

		select pers.personId personId,
			pers.reportResidency reportResidency,
			pers.reportAge reportAge,
            pers.reportCIP reportCIP,
			pers.ipedsEthnicity ipedsEthnicity,
            pers.ipedsGender ipedsGender,
            pers.studyAbroadStatus studyAbroadStatus,
            (case when pers.studentType = 'First Time' and pers.isNonDegreeSeeking = false and pers.studentLevelUGGR = 'UG' then
                        (case when date_add(pers.highSchoolGradDate, 365) >= pers.censusDate then 1 else 0 end)
                    else null end) highSchoolGradDate,
            pers.distanceEdInd,
            pers.ipedsEnrolled,
            pers.timeStatus,
            pers.isNonDegreeSeeking,
            pers.studentType,
            pers.studentLevelUGGR,
            pers.partACipCode partACipCode,
			(case when pers.studentLevelUGGR = 'GR' then '3'
				when pers.isNonDegreeSeeking = true then '2'
				else '1'
			end) ipedsPartGStudentLevel,
			(case when pers.studentLevelUGGR = 'GR' then '3'
				else '1'
			end) ipedsPartBStudentLevel,
			case when pers.timeStatus = 'Full Time'
						and pers.studentType = 'First Time'
						and pers.isNonDegreeSeeking = false
						and pers.studentLevelUGGR = 'UG'  
				then '1' -- 1 - Full-time, first-time degree/certificate-seeking undergraduate
				when pers.timeStatus = 'Full Time'
						and pers.studentType = 'Transfer'
						and pers.isNonDegreeSeeking = false
						and pers.studentLevelUGGR = 'UG'
				then '2' -- 2 - Full-time, transfer-IN degree/certificate-seeking undergraduate
				when pers.timeStatus = 'Full Time'
						and pers.studentType in ('Continuing', 'Re-admit')
						and pers.isNonDegreeSeeking = false
						and pers.studentLevelUGGR = 'UG'
				then '3' -- 3 - Full-time, continuing degree/certificate-seeking undergraduate
				when pers.timeStatus = 'Full Time'
						and pers.isNonDegreeSeeking = true
						and pers.studentLevelUGGR = 'UG'
				then '7' -- 7 - Full-time, non-degree/certificate-seeking undergraduate
				when pers.timeStatus = 'Full Time'
						and pers.studentLevelUGGR = 'GR'
				then '11' -- 11 - Full-time graduate
				when pers.timeStatus = 'Part Time'
						and pers.studentType = 'First Time'
						and pers.isNonDegreeSeeking = false
						and pers.studentLevelUGGR = 'UG'
				then '15' -- 15 - Part-time, first-time degree/certificate-seeking undergraduate
				when pers.timeStatus = 'Part Time'
						and pers.studentType = 'Transfer'
						and pers.isNonDegreeSeeking = false
						and pers.studentLevelUGGR = 'UG'
				then '16' -- 16 - Part-time, transfer-IN degree/certificate-seeking undergraduate
				when pers.timeStatus = 'Part Time'
						and pers.studentType in ('Continuing', 'Re-admit')
						and pers.isNonDegreeSeeking = false
						and pers.studentLevelUGGR = 'UG'
				then '17' -- 17 - Part-time, continuing degree/certificate-seeking undergraduate
				when pers.timeStatus = 'Part Time'
						and pers.isNonDegreeSeeking = true
						and pers.studentLevelUGGR = 'UG'
				then '21' -- 21 - Part-time, non-degree/certificate-seeking undergraduate
				when pers.timeStatus = 'Part Time'
						and pers.studentLevelUGGR = 'GR'
				then '25' -- 25 - Part-time graduate
			end as ipedsPartAStudentLevel,
			(case when pers.reportAge = 'Y' then (
                case when pers.timeStatus = 'Full Time' 
				then (
					case when pers.asOfAge < 18 then 1 --1 - Full-time, under 18
						when pers.asOfAge >= 18 and pers.asOfAge <= 19 then 2 --2 - Full-time, 18-19
						when pers.asOfAge >= 20 and pers.asOfAge <= 21 then 3 --3 - Full-time, 20-21
						when pers.asOfAge >= 22 and pers.asOfAge <= 24 then 4 --4 - Full-time, 22-24
						when pers.asOfAge >= 25 and pers.asOfAge <= 29 then 5 --5 - Full-time, 25-29
						when pers.asOfAge >= 30 and pers.asOfAge <= 34 then 6 --6 - Full-time, 30-34
						when pers.asOfAge >= 35 and pers.asOfAge <= 39 then 7 --7 - Full-time, 35-39
						when pers.asOfAge >= 40 and pers.asOfAge <= 49 then 8 --8 - Full-time, 40-49
						when pers.asOfAge >= 50 and pers.asOfAge <= 64 then 9 --9 - Full-time, 50-64
						when pers.asOfAge >= 65 then 10 --10 - Full-time, 65 and over
						end )
				else (
					case when pers.asOfAge < 18 then 13 --13 - Part-time, under 18
						when pers.asOfAge >= 18 and pers.asOfAge <= 19 then 14 --14 - Part-time, 18-19
						when pers.asOfAge >= 20 and pers.asOfAge <= 21 then 15 --15 - Part-time, 20-21
						when pers.asOfAge >= 22 and pers.asOfAge <= 24 then 16 --16 - Part-time, 22-24
						when pers.asOfAge >= 25 and pers.asOfAge <= 29 then 17 --17 - Part-time, 25-29
						when pers.asOfAge >= 30 and pers.asOfAge <= 34 then 18 --18 - Part-time, 30-34
						when pers.asOfAge >= 35 and pers.asOfAge <= 39 then 19 --19 - Part-time, 35-39
						when pers.asOfAge >= 40 and pers.asOfAge <= 49 then 20 --20 - Part-time, 40-49
						when pers.asOfAge >= 50 and pers.asOfAge <= 64 then 21 --21 - Part-time, 50-64
						when pers.asOfAge >= 65 then 22 --22 - Part-time, 65 and over
						end )
                end)
            end) ipedsAgeGroup,
			case when pers.nation in ('US', 'UNITED STATES', 'UNITED STATES OF AMERICA') then 1 else 0 end isInAmerica,
			case when pers.residency = 'In District' then 'IN'
		        when pers.residency = 'In State' then 'IN'
		        when pers.residency = 'Out of State' then 'OUT'
--jh 20200423 added value of Out of US
		        when pers.residency = 'Out of US' then 'OUT'
		        else 'UNKNOWN'
	        end residentStatus,
--check 'IPEDSClientConfig' entity to confirm that optional data is reported
		case when pers.reportResidency = 'Y' 
			then (case when pers.state IS NULL then '57' --57 - State unknown
				when pers.state = 'AL' then '01' --01 - Alabama
				when pers.state = 'AK' then '02' --02 - Alaska
				when pers.state = 'AZ' then '04' --04 - Arizona
				when pers.state = 'AR' then '05' --05 - Arkansas
				when pers.state = 'CA' then '06' --06 - California
				when pers.state = 'CO' then '08' --08 - Colorado
				when pers.state = 'CT' then '09' --09 - CONnecticut
				when pers.state = 'DE' then '10' --10 - Delaware
				when pers.state = 'DC' then '11' --11 - District of Columbia
				when pers.state = 'FL' then '12' --12 - Florida
				when pers.state = 'GA' then '13' --13 - Georgia
				when pers.state = 'HI' then '15' --15 - Hawaii
				when pers.state = 'ID' then '16' --16 - Idaho
				when pers.state = 'IL' then '17' --17 - Illinois
				when pers.state = 'IN' then '18' --18 - Indiana
				when pers.state = 'IA' then '19' --19 - Iowa
				when pers.state = 'KS' then '20' --20 - Kansas
				when pers.state = 'KY' then '21' --21 - Kentucky
				when pers.state = 'LA' then '22' --22 - Louisiana
				when pers.state = 'ME' then '23' --23 - Maine
				when pers.state = 'MD' then '24' --24 - Maryland
				when pers.state = 'MA' then '25' --25 - Massachusetts
				when pers.state = 'MI' then '26' --26 - Michigan
				when pers.state = 'MS' then '27' --27 - Minnesota
				when pers.state = 'MO' then '28' --28 - Mississippi
				when pers.state = 'MN' then '29' --29 - Missouri
				when pers.state = 'MT' then '30' --30 - Montana
				when pers.state = 'NE' then '31' --31 - Nebraska
				when pers.state = 'NV' then '32' --32 - Nevada
				when pers.state = 'NH' then '33' --33 - New Hampshire
				when pers.state = 'NJ' then '34' --34 - New Jersey
				when pers.state = 'NM' then '35' --35 - New Mexico
				when pers.state = 'NY' then '36' --36 - New York
				when pers.state = 'NC' then '37' --37 - North Carolina
				when pers.state = 'ND' then '38' --38 - North Dakota
				when pers.state = 'OH' then '39' --39 - Ohio	
				when pers.state = 'OK' then '40' --40 - Oklahoma
				when pers.state = 'OR' then '41' --41 - Oregon
				when pers.state = 'PA' then '42' --42 - Pennsylvania
				when pers.state = 'RI' then '44' --44 - Rhode Island
				when pers.state = 'SC' then '45' --45 - South Carolina
				when pers.state = 'SD' then '46' --46 - South Dakota
				when pers.state = 'TN' then '47' --47 - Tennessee
				when pers.state = 'TX' then '48' --48 - Texas
				when pers.state = 'UT' then '49' --49 - Utah
				when pers.state = 'VT' then '50' --50 - Vermont
				when pers.state = 'VA' then '51' --51 - Virginia
				when pers.state = 'WA' then '53' --53 - Washington
				when pers.state = 'WV' then '54' --54 - West Virginia
				when pers.state = 'WI' then '55' --55 - Wisconsin
				when pers.state = 'WY' then '56' --56 - Wyoming
				when pers.state = 'UNK' then '57' --57 - State unknown
				when pers.state = 'AS' or pers.state like '%SAMOA%' then '60' --American Samoa
				when pers.state = 'FM' or pers.state like '%MICRONESIA%' then '64' --64 - Federated States of Micronesia
				when pers.state = 'GU' or pers.state like '%GUAM%' then '66' --66 - Guam
				when pers.state = 'MH' or pers.state like '%MARSHALL%' then '68' --Marshall Islands
				when pers.state = 'MP' or pers.state like '%MARIANA%' then '69' --Northern Mariana Islands
				when pers.state = 'PW' or pers.state like '%PALAU%' then '70' --Palau
				when pers.state in ('PUE', 'PR') or pers.state like '%PUERTO RICO%' then '72' --72 - Puerto Rico
				when pers.state = 'VI' or pers.state like '%VIRGIN%' then '78' --78 - Virgin Islands
				else '90' --90 - Foreign countries
			end)
		else NULL
	end ipedsStateCode
from PersonMCR pers
where pers.yearType = 'CY'
),

/*****
BEGIN SECTION - Retention Cohort Creation
This set of views is used to look at prior year data and return retention cohort information of the group as of the current reporting year.
*****/

CohortExclusionMCR_RET as (
--View used to build the reportable retention student cohort. (CohortInd = 'Retention Fall')
--Full-time, first-time Fall retention bachelor's cohort

select *
from (
    select pers.personId,
            pers.ipedsEnrolled,
            (case when pers.ipedsEnrolled != 1 and pers.studyAbroadStatus = 'Study Abroad - Home Institution' then 1 else null end) inclusion,
            pers.timeStatus,
            (case when persCY.personId is not null then 1 else 0 end) stillEnrolled,
            (case when exclusionENT.personId is not null then 1 else 0 end ) exclusion,
            coalesce(row_number() over (
                partition by
                    pers.personId
                order by
                    (case when exclusionENT.snapshotDate = pers.snapshotDate then 1 else 2 end) asc,
                    (case when exclusionENT.snapshotDate > pers.snapshotDate then exclusionENT.snapshotDate else CAST('9999-09-09' as DATE) end) desc,
                    (case when exclusionENT.snapshotDate < pers.snapshotDate then exclusionENT.snapshotDate else CAST('1900-09-09' as DATE) end) asc,
                    exclusionENT.termCodeEffective desc,
                    exclusionENT.recordActivityDate desc
            ), 1) exclusionRn
    from PersonMCR pers
        left join PersonMCR persCY on pers.personId = persCY.personId
            and persCY.yearType = 'CY'
        left join CohortExclusion exclusionENT on exclusionENT.personId = pers.personId
                and exclusionENT.exclusionReason in ('Died', 'Medical Leave', 'Military Leave', 'Foreign Aid Service', 'Religious Leave')
                and coalesce(exclusionENT.isIPEDSReportable, true) = true
                and ((coalesce(to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                    and to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') <= pers.censusDate)
                    or coalesce(to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
            left join AcademicTermOrder termorder on termorder.termCode = exclusionENT.termCodeEffective
                and termorder.termOrder <= pers.termOrder
    where pers.yearType = 'PY'
    )
where exclusionRn = 1
),

/*****
BEGIN SECTION - Student-to-Faculty Ratio Calculation
The views below pull the Instuctor count and calculates the Student-to-Faculty Ratio 
*****/
/* FULL-AND PART-TIME INSTRUCTIONAL STAFF DATA:
Lines F9 and F12 should be reported based on data your institution is reporting in the IPEDS Human Resources (HR) survey component. 
Please work together with the appropriate staff at your institution to ensure that the data used on this worksheet and reported in the HR component are the same.

Line F9. The total number of full-time instructional staff (non-medical) as reported on the HR survey component.
Line F12. The total number of part-time instructional staff (non-medical) as reported on the HR survey component. NOTE: Graduate assistants are not included.
*/

InstructionalAssignmentMCR as (
--returns all instructional assignments for all employees

select instructor.personId personId,
    instructor.asOfDateHR asOfDateHR,
    instructor.snapshotDate snapshotDate,
    1 instructCourse,
	SUM(case when course.courseSectionNumber is not null then 1 else 0 end) totalCourses,
	SUM(case when course.enrollmentHoursCalc = 0 then 1 else 0 end) totalNonCredCourses,
	SUM(case when course.enrollmentHoursCalc > 0 then 1 else 0 end) totalCredCourses,
	--SUM(case when course.courseSectionLevel = 'Continuing Education' then 1 else 0 end) totalCECourses,
	SUM(case when course.courseSectionLevel = 'Professional Practice Doctorate' then 1 else 0 end) totalPRCourses
from (
    select *
    from (
        select instructassignENT.personId,
                upper(instructassignENT.termCode) termCode, 
                coalesce(upper(instructassignENT.partOfTermCode), 1) partOfTermCode,
                upper(instructassignENT.courseSectionNumber) courseSectionNumber,
                rep.asOfDateHR asOfDateHR,
                instructassignENT.snapshotDate snapshotDate,
                coalesce(row_number() over (
                    partition by
                        instructassignENT.personId,
                        instructassignENT.termCode,
                        instructassignENT.partOfTermCode,
                        instructassignENT.courseSectionNumber,
                        instructassignENT.section
                    order by
                        (case when array_contains(instructassignENT.tags, rep.repPeriodTag3) then 1
                                else 2 end) asc,
			            instructassignENT.snapshotDate desc,
                        instructassignENT.recordActivityDate desc
                ), 1) jobRn
        from AcademicTermReportingRefactor rep
            inner join InstructionalAssignment instructassignENT on upper(instructassignENT.termCode) = rep.termCode
                and coalesce(upper(instructassignENT.partOfTermCode), 1) = rep.partOfTermCode
                and coalesce(instructassignENT.isIpedsReportable, true) = true
        where rep.surveySection = 'COHORT'
            and ((coalesce(to_date(instructassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                    and to_date(instructassignENT.recordActivityDate, 'YYYY-MM-DD') <= rep.asOfDateHR)
                or coalesce(to_date(instructassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))
        )
    where jobRn = 1
    ) instructor
left join CourseMCR course on instructor.termCode = course.termCode
    and instructor.partOfTermCode = course.partOfTermCode
    and instructor.courseSectionNumber = course.courseSectionNumber
 group by instructor.personId,
        instructor.asOfDateHR,
        instructor.snapshotDate 
),

EmployeeMCR as (

select *
from (
    select empENT.personId personId,
	    coalesce(empENT.isIpedsMedicalOrDental, false) isIpedsMedicalOrDental,
	    empENT.primaryFunction primaryFunction,
	    (case when empENT.primaryFunction in (
					'Instruction with Research/Public Service',
					'Instruction - Credit',
					'Instruction - Non-credit',
					'Instruction - Combined Credit/Non-credit'
				) then 1 else 0 end) primFunctInstruct,
	    empENT.employeeGroup employeeGroup,
        empENT.snapshotDate snapshotDate,
        empENT.employeeStatus employeeStatus,
        coalesce(to_date(empENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        instr.totalCourses,
	    instr.totalNonCredCourses,
        instr.totalCredCourses,
	    --instr.totalCECourses,
	    instr.totalPRCourses,
	    coalesce(instr.instructCourse, 0) instructCourse,
        repperiod.asOfDateHR,
		coalesce(row_number() over (
			partition by
				empENT.personId
			order by 
			    (case when array_contains(empENT.tags, repperiod.repPeriodTag3) then 1
                     else 2 end) asc,
			     empENT.snapshotDate desc,
                 empENT.recordActivityDate desc
		), 1) empRn
    from (select asOfDateHR, repPeriodTag3
        from DefaultValues) repperiod
        cross join Employee empENT 
			on ((coalesce(to_date(empENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                        and to_date(empENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.asOfDateHR)
			        or coalesce(to_date(empENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
            and  ((empENT.terminationDate is null) -- all non-terminated employees
			        or (to_date(empENT.terminationDate,'YYYY-MM-DD') > repperiod.asOfDateHR
				and to_date(empENT.hireDate,'YYYY-MM-DD') <= repperiod.asOfDateHR)) -- employees terminated after the as-of date
			and coalesce(empENT.isIpedsReportable, true) = true
        left join InstructionalAssignmentMCR instr on empENT.personId = instr.personId
	)
where empRn = 1
and isIpedsMedicalOrDental = false
and ((recordActivityDate != CAST('9999-09-09' AS DATE) and employeeStatus in ('Active', 'Leave with Pay'))
    or recordActivityDate = CAST('9999-09-09' AS DATE))
),

EmployeeAssignmentMCR AS (
--Employee primary assignment

select *
from (
	select emp.personId personId, 
        emp.snapshotDate snapshotDate,
        emp.asOfDateHR asOfDateHR,
        emp.primaryFunction primaryFunction,
        coalesce(emp.totalCourses, 0) totalCourses,
	    coalesce(emp.totalNonCredCourses, 0) totalNonCredCourses,
	    coalesce(emp.totalCredCourses, 0) totalCredCourses,
	    --coalesce(emp.totalCECourses, 0) totalCECourses,
        coalesce(emp.totalPRCourses, 0) totalPRCourses,
	    emp.instructCourse,
        emp.primFunctInstruct,
        empassignENT.fullOrPartTimeStatus fullOrPartTimeStatus,
	    empassignENT.position position,
	    empassignENT.suffix suffix,
	    empassignENT.assignmentType assignmentType,
        coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        empassignENT.assignmentStatus assignmentStatus,
        coalesce(empassignENT.isUndergradStudent, false) isUndergradStudent,
		coalesce(empassignENT.isWorkStudy, false) isWorkStudy,
		coalesce(empassignENT.isTempOrSeasonal, false) isTempOrSeasonal,
        coalesce(ROW_NUMBER() OVER (
			PARTITION BY
				emp.personId,
				empassignENT.personId
			ORDER BY
			    (case when empassignENT.snapshotDate = emp.snapshotDate then 1 else 2 end) asc,
			    (case when empassignENT.snapshotDate > emp.snapshotDate then empassignENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when empassignENT.snapshotDate < emp.snapshotDate then empassignENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                (case when empassignENT.assignmentType = 'Primary' then 1 else 2 end) asc,
                (case when empassignENT.fullOrPartTimeStatus = 'Full Time' then 1 else 2 end) asc,
				empassignENT.assignmentStartDate desc,
                empassignENT.recordActivityDate desc
         ), 1) jobRn
    from EmployeeMCR emp
		left join EmployeeAssignment empassignENT on emp.personId = empassignENT.personId
			and ((coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                    and to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD') <= emp.asOfDateHR)
                or coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
			and to_date(empassignENT.assignmentStartDate, 'YYYY-MM-DD') <= emp.asOfDateHR
			and (empassignENT.assignmentendDate is null 
				or to_date(empassignENT.assignmentendDate, 'YYYY-MM-DD') >= emp.asOfDateHR)
			and coalesce(empassignENT.isIpedsReportable, true) = true
			--and empassignENT.assignmentType = 'Primary'
			and empassignENT.position is not null
	where (emp.primFunctInstruct = 1
	    or emp.instructCourse = 1)
     )
where jobRn = 1
    and ((recordActivityDate != CAST('9999-09-09' AS DATE) and assignmentStatus = 'Active')
                or recordActivityDate = CAST('9999-09-09' AS DATE))
    and isUndergradStudent = false
    and isWorkStudy = false
    and isTempOrSeasonal = false
),

FTE_EMP as (
--view used to build the employee cohort to calculate instructional full time equivalency

select ROUND(FTInst + ((PTInst + PTNonInst) * 1/3), 2) FTE
from (
	select SUM(case when fullOrPartTimeStatus = 'Full Time' and typeCourses in ('Credit', 'Combo') and allPR = 0 then 1 
					else 0 
		        end) FTInst, --FT instructional staff not teaching exclusively non-credit courses
		SUM(case when fullOrPartTimeStatus = 'Part Time' and typeCourses in ('Credit', 'Combo') and allPR = 0 then 1 
					else 0 
		        end) PTInst, --PT instructional staff not teaching exclusively non-credit courses
		SUM(case when typeCourses not in ('Credit', 'Combo') and allPR = 0 then 1 
		            else 0 
		    end) PTNonInst
	from ( 
        select personId,
	            coalesce(fullOrPartTimeStatus, 'Full Time') fullOrPartTimeStatus,
		        primFunctInstruct,
                instructCourse,
                (case when primaryFunction = 'Instruction - Credit' then 'Credit'
                        when primaryFunction = 'Instruction - Non-credit' then 'NonCredit'
			            when primaryFunction = 'Instruction - Combined Credit/Non-credit' then 'Combo'
			            when primaryFunction = 'Instruction with Research/Public Service' and totalCourses > 0 and totalCredCourses = totalCourses then 'Credit'
			            when primaryFunction = 'Instruction with Research/Public Service' and totalCourses > 0 and totalNonCredCourses = totalCourses then 'NonCredit'
			            when primaryFunction = 'Instruction with Research/Public Service' and totalCourses > 0 and totalNonCredCourses + totalCredCourses = totalCourses then 'Combo'
			            when primaryFunction = 'Instruction with Research/Public Service' then 'Combo'
			            when totalCourses > 0 and totalCredCourses = totalCourses then 'CreditExtra'
			            when totalCourses > 0 and totalNonCredCourses = totalCourses then 'NonCreditExtra'
			            when totalCourses > 0 and totalNonCredCourses + totalCredCourses = totalCourses then 'ComboExtra'
		            else 'None' end) typeCourses,
		        (case when totalPRCourses = totalCourses then 1 else 0 end) allPR
        from EmployeeAssignmentMCR
        )
    )
),

FTE_STU as (
--student full time equivalency calculation

select ROUND(FTStud + (PTStud * 1/3), 2) FTE
from (
	select SUM(case when cohortstu.timeStatus = 'Full Time' then 1 else 0 end) FTStud,
		SUM(case when cohortstu.timeStatus = 'Part Time' then 1 else 0 end) PTStud
	from CohortSTU cohortstu
    )
),

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/

FormatPartAStudentLevel as (
select *
from (
	VALUES
		('1'), --Full-time, first-time degree/certificate-seeking undergraduate
		('2'), --Full-time, transfer-in degree/certificate-seeking undergraduate
		('3'), --Full-time, continuing degree/certificate-seeking undergraduate
		('7'), --Full-time, non-degree/certificate-seeking undergraduate
		('11'), --Full-time graduate
		('15'), --Part-time, first-time degree/certificate-seeking undergraduate
		('16'), --Part-time, transfer-in degree/certificate-seeking undergraduate
		('17'), --Part-time, continuing degree/certificate-seeking undergraduate
		('21'), --Part-time, non-degree/certificate-seeking undergraduate
		('25')  --Part-time graduate
	) as StudentLevel(ipedsLevel)
),

FormatFallEnrlCipCodeGroup as (
select *
from (
	VALUES
		('13.0000'),
		('14.0000'),
		('26.0000'),
		('27.0000'),
		('40.0000'),
		('52.0000'),
		('22.0101'),
		('51.0401'),
		('51.1201')
	) as CipCodeGroup(ipedsCipCodeGroup)
),

FormatPartGStudentLevel as (
select *
from (
	VALUES
		('1'), --Degree/Certificate seeking undergraduate students
		('2'), --Non-Degree/Certificate seeking undergraduate Students
		('3')  --Graduate students
	) as StudentLevel(ipedsLevel)
),

FormatPartBStudentLevel as (
select *
from (
	VALUES
		('1'), --Undergraduate students
		('3')  --Graduate students
	) as StudentLevel(ipedsLevel)
),

FormatPartBAgeGroup as (
select *
from (
	VALUES
		(1), --1 - Full-time, under 18
		(2), --2 - Full-time, 18-19
		(3), --3 - Full-time, 20-21
		(4), --4 - Full-time, 22-24
		(5), --5 - Full-time, 25-29
		(6), --6 - Full-time, 30-34
		(7), --7 - Full-time, 35-39
		(8), --8 - Full-time, 40-49
		(9), --9 - Full-time, 50-64
		(10), --10 - Full-time, 65 and over
		(13), --13 - Part-time, under 18
		(14), --14 - Part-time, 18-19
		(15), --15 - Part-time, 20-21
		(16), --16 - Part-time, 22-24
		(17), --17 - Part-time, 25-29
		(18), --18 - Part-time, 30-34
		(19), --19 - Part-time, 35-39
		(20), --20 - Part-time, 40-49
		(21), --21 - Part-time, 50-64
		(22) --22 - Part-time, 65 and over
	) as AgeGroup(ipedsAgeGroup)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs


fields are used as follows...
Part                      - Should reflect the appropriate survey part
sortorder                 - Numeric field that is used to overide 'part' for ordering the output. IPEDS specs request ordering of parts in input file that are not alphabetic.
                            The fall enrollment survey is reported differently in odd years than in even. In the year to follow, additional union queries will need to be added in 'Part A'. 
                            These union queries should get be given VALUES in the 'sortorder' field that increment by tenths (1.1, 1.2 ...) to maintain ordering control. 
Field1                    - Used and reserved for CIPCodes. Part A needs to be ordered by CIPCodes so field 1 is reserved for ordering. The value of Field1 will be null where the value
                            should not impact ordering of the result set.
Field2 through Field20    - Fields2 through field 20 are used for numeric VALUES that will be impacted by the grouping/ordering of sortorder and/or Field1
*****/

-- Part A: Fall Enrollment by Student Level, Attendance Status, Race/Ethnicity, and Gender

select 'A' part,
	1 sortorder,
	'99.0000' field1, --Classification of instructional program (CIP) code default (99.0000) for all institutions. 'field1' will be utilized for client CIPCodes every other reporting year
	ipedsLevel field2, -- Student level, 1,2,3,7,11,15,16,17,21, and 25 (refer to student level table (Part A) in appendix)(6,8,14,20,22, 28, 29 and 99 are for export only.)
	coalesce(SUM(case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end), 0) field3, -- Nonresident alien - Men (1), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end), 0) field4, -- Nonresident alien - Women (2), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end), 0) field5, -- Hispanic/Latino - Men (25), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end), 0) field6, -- Hispanic/Latino - Women (26), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end), 0) field7, -- American Indian or Alaska Native - Men (27), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end), 0) field8, -- American Indian or Alaska Native - Women (28), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end), 0) field9, -- Asian - Men (29), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end), 0) field10, -- Asian - Women (30), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end), 0) field11, -- Black or African American - Men (31), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end), 0) field12, -- Black or African American - Women (32), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end), 0) field13, -- Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end), 0) field14, -- Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end), 0) field15, -- White - Men (35), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end), 0) field16, -- White - Women (36), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end), 0) field17, -- Two or more races - Men (37), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end), 0) field18, -- Two or more races - Women (38), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end), 0) field19, -- Race and ethnicity unknown - Men (13), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end), 0) field20 -- Race and ethnicity unknown - Women (14), 0 to 999999
from (
	select cohort.personId personId,
		cohort.ipedsPartAStudentLevel ipedsLevel,
		cohort.ipedsGender ipedsGender,
		cohort.ipedsEthnicity ipedsEthnicity
	from CohortSTU cohort
	where cohort.ipedsPartAStudentLevel is not null
	--EF2 mod testing - no Graduate level
	--		and cohort.studentLevel = 'Undergraduate' 

	union

	select NULL, --personId,
		StudentLevel.ipedsLevel,
		NULL, -- ipedsGender,
		NULL  -- ipedsEthnicity
	from FormatPartAStudentLevel StudentLevel
--EF2 mod testing - no Graduate level values 11 and 25
--	where StudentLevel.ipedsLevel != 11 
--		and StudentLevel.ipedsLevel != 25
	)
group by ipedsLevel

union

-- Part A: Fall Enrollment by Student Level, Attendance Status, Race/Ethnicity, and Gender
--undergraduate and graduate
-- Only included for 4 year schools on odd number years where mod(config.surveyYear,2) != 0

select 'A' part,
	1.5 sortorder,
	partACipCode field1, --Classification of instructional program (CIP) code default (99.0000) for all institutions. 'field1' will be utilized for client CIPCodes every other reporting year
	ipedsLevel field2, -- Student level, 1,2,3,7,11,15,16,17,21, and 25 (refer to student level table (Part A) in appendix)(6,8,14,20,22, 28, 29 and 99 are for export only.)
	coalesce(SUM(case when ipedsEthnicity = '1' and ipedsGender = 'M' then 1 else 0 end), 0) field3, -- Nonresident alien - Men (1), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '1' and ipedsGender = 'F' then 1 else 0 end), 0) field4, -- Nonresident alien - Women (2), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '2' and ipedsGender = 'M' then 1 else 0 end), 0) field5, -- Hispanic/Latino - Men (25), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '2' and ipedsGender = 'F' then 1 else 0 end), 0) field6, -- Hispanic/Latino - Women (26), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '3' and ipedsGender = 'M' then 1 else 0 end), 0) field7, -- American Indian or Alaska Native - Men (27), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '3' and ipedsGender = 'F' then 1 else 0 end), 0) field8, -- American Indian or Alaska Native - Women (28), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '4' and ipedsGender = 'M' then 1 else 0 end), 0) field9, -- Asian - Men (29), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '4' and ipedsGender = 'F' then 1 else 0 end), 0) field10, -- Asian - Women (30), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '5' and ipedsGender = 'M' then 1 else 0 end), 0) field11, -- Black or African American - Men (31), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '5' and ipedsGender = 'F' then 1 else 0 end), 0) field12, -- Black or African American - Women (32), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '6' and ipedsGender = 'M' then 1 else 0 end), 0) field13, -- Native Hawaiian or Other Pacific Islander - Men (33), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '6' and ipedsGender = 'F' then 1 else 0 end), 0) field14, -- Native Hawaiian or Other Pacific Islander - Women (34), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '7' and ipedsGender = 'M' then 1 else 0 end), 0) field15, -- White - Men (35), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '7' and ipedsGender = 'F' then 1 else 0 end), 0) field16, -- White - Women (36), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '8' and ipedsGender = 'M' then 1 else 0 end), 0) field17, -- Two or more races - Men (37), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '8' and ipedsGender = 'F' then 1 else 0 end), 0) field18, -- Two or more races - Women (38), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '9' and ipedsGender = 'M' then 1 else 0 end), 0) field19, -- Race and ethnicity unknown - Men (13), 0 to 999999
	coalesce(SUM(case when ipedsEthnicity = '9' and ipedsGender = 'F' then 1 else 0 end), 0) field20  -- Race and ethnicity unknown - Women (14), 0 to 999999
from (
	select cohort.personId personId,
		cohort.ipedsPartAStudentLevel ipedsLevel,
		cohort.partACipCode partACipCode,
		cohort.ipedsGender ipedsGender,
		cohort.ipedsEthnicity ipedsEthnicity
	from CohortSTU cohort
	where cohort.ipedsPartAStudentLevel is not null
	    and cohort.partACipCode is not null
	    and cohort.reportCIP = 'Y'

	union

	select NULL, --personId,
		StudentLevel.ipedsLevel, -- ipedsLevel,
		cipGroup.ipedsCipCodeGroup, -- partACipCode,
		NULL, -- ipedsGender,
		NULL -- ipedsEthnicity
	from FormatPartAStudentLevel StudentLevel
	    cross join FormatFallEnrlCipCodeGroup cipGroup
	)
where ((ipedsLevel in ('1', '2', '3', '7', '15', '16', '17') and partACipCode in ('13.0000', '14.0000', '26.0000', '27.0000', '40.0000', '52.0000'))
        or (ipedsLevel in ('11', '25') and partACipCode in ('22.0101', '51.0401', '51.1201')))
group by ipedsLevel,
    partACipCode

union

--Part G: Distance Education Status
--undergraduate and graduate

select 'G', -- part,
	'2', -- sortorder,
	NULL, -- field1,
	ipedsLevel, -- field2, --1,2 and 3 (refer to student level table (Part G) in appendix)
	SUM(case when distanceEdInd = 'Exclusive DE' then 1 else 0 end), -- field3,-- Enrolled exclusively in distance education courses 0 to 999999
	SUM(case when distanceEdInd = 'Some DE' then 1 else 0 end), -- field4,-- Enrolled in some but not all distance education courses 0 to 999999
	SUM(case when distanceEdInd = 'Exclusive DE' and residentStatus = 'IN' then 1 else 0 end), -- field5,-- Of those students exclusively enrolled in de courses - Located in the state/jurisdiction of institution 0 to 999999
	SUM(case when distanceEdInd = 'Exclusive DE' and isInAmerica = 1 and residentStatus = 'OUT' then 1 else 0 end), -- field6,-- Of those students exclusively enrolled in de courses - Located in the U.S. but not in state/jurisdiction of institution 0 to 999999
	SUM(case when distanceEdInd = 'Exclusive DE' and isInAmerica = 1 and residentStatus = 'UNKNOWN' then 1 else 0 end), -- field7,-- Of those students exclusively enrolled in de courses - Located in the U.S. but state/jurisdiction unknown 0 to 999999
	SUM(case when distanceEdInd = 'Exclusive DE' and isInAmerica = 0 then 1 else 0 end), -- field8,-- Of those students exclusively enrolled in de courses - Located outside the U.S. 0 to 999999
	NULL, -- field9,
	NULL, -- field10,
	NULL, -- field11,
	NULL, -- field12,
	NULL, -- field13,
	NULL, -- field14,
	NULL, -- field15,
	NULL, -- field16,
	NULL, -- field17,
	NULL, -- field18,
	NULL, -- field19,
	NULL -- field20
from (
	select cohort.personId personId,
		cohort.ipedsPartGStudentLevel ipedsLevel,
		cohort.distanceEdInd distanceEdInd,
		cohort.residentStatus residentStatus,
		cohort.isInAmerica
	from CohortSTU cohort 

	union
	
	select NULL, --personId,
		StudentLevel.ipedsLevel,
		0, --distanceEdInd,
		NULL, -- residentStatus,
		0	--isInAmerica
	from FormatPartGStudentLevel StudentLevel 
)
group by ipedsLevel

--10m

union

--Part B - Fall Enrollment by Age and Gender 
--**(Part B is mandatory in this collection)**
--undergraduate and graduate

select 'B', -- part,
	'3', -- sortorder,
	NULL, -- field1,
	ipedsLevel, -- field2, --1 and 3 (refer to student level table (Part B)) in appendix
	ipedsAgeGroup, -- field3, --1–24 (refer to age category table in appendix) (11, 12, 23, and 24 are for export only)
	SUM(case when ipedsGender = 'M' then 1 else 0 end), -- field4, --Men, 0 to 999999 
	SUM(case when ipedsGender = 'F' then 1 else 0 end), -- field5, --Female, 0 to 999999
	NULL, -- field6,
	NULL, -- field7,
	NULL, -- field8,
	NULL, -- field9,
	NULL, -- field10,
	NULL, -- field11,
	NULL, -- field12,
	NULL, -- field13,
	NULL, --  field14,
	NULL, --  field15,
	NULL, --  field16,
	NULL, --  field17,
	NULL, --  field18,
	NULL, --  field19,
	NULL -- field20
from (
	select cohort.personId personId,
		cohort.ipedsPartBStudentLevel ipedsLevel,
		cohort.ipedsAgeGroup ipedsAgeGroup,
		cohort.ipedsGender
	from CohortSTU cohort
    where cohort.reportAge = 'Y'
      and cohort.ipedsAgeGroup is not null 

	union

	select NULL, -- personId,
		StudentLevel.ipedsLevel,
		AgeGroup.ipedsAgeGroup,
		NULL  -- ipedsGender
	from FormatPartBStudentLevel StudentLevel
		cross join FormatPartBAgeGroup AgeGroup
    where (select clientconfig.reportAge
        from ClientConfigMCR clientconfig) = 'Y'
	)
group by ipedsLevel,
    ipedsAgeGroup

--12m 24 s

union

--Part C: Residence of First-Time Degree/Certificate-Seeking Undergraduate Students
--**(Part C is optional in this collection)**
--undergraduate ONLY

select 'C', -- part,
	4, -- sortorder,
	NULL, -- field1,
	cohort.ipedsStateCode, -- field2, --State of residence, 1, 2, 4–6, 8–13, 15–42, 44–51, 53–57, 60, 64, 66, 68–70, 72, 78, 90 (valid FIPS codes, refer to state table in appendix) (98 and 99 are for export only)
	case when count(cohort.personId) > 0 then count(cohort.personId) else 1 end, -- field3, --Total first-time degree/certificate-seeking undergraduates, 1 to 999999 ***error in spec - not allowing 0
	coalesce(SUM(cohort.highSchoolGradDate), 0), -- field4, --Total first-time degree/certificate-seeking undergraduates who enrolled within 12 months of graduating high school 0 to 999999 
	NULL, -- field5,
	NULL, -- field6,
	NULL, -- field7,
	NULL, -- field8,
	NULL, -- field9,
	NULL, -- field10,
	NULL, -- field11,
	NULL, -- field12,
	NULL, -- field13,
	NULL, -- field14,
	NULL, -- field15,
	NULL, -- field16,
	NULL, -- field17,
	NULL, -- field18,
	NULL, -- field19,
	NULL -- field20
from CohortSTU cohort
where cohort.reportResidency = 'Y'
and cohort.ipedsPartAStudentLevel in ('1', '15')
group by cohort.ipedsStateCode

union

--Part D: Total Undergraduate Entering Class
--**This section is only applicable to degree-granting, academic year reporters (calendar system = semester, quarter, trimester, 
--**or 4-1-4) that offer undergraduate level programs and reported full-time, first-time degree/certificate seeking students in Part A.
--undergraduate ONLY
--acad reporters ONLY

select 'D', -- part,
	5, -- sortorder,
	NULL, -- field1,
	case when COUNT(cohort.personId) = 0 then 1 else COUNT(cohort.personId) end, -- field2, --Total number of non-degree/certificate seeking undergraduates that are new to the institution in the Fall 1 to 999999 ***error in spec - not allowing 0
	NULL, -- field3,
	NULL, -- field4,
	NULL, -- field5,
	NULL, -- field6,
	NULL, -- field7,
	NULL, -- field8,
	NULL, -- field9,
	NULL, -- field10,
	NULL, -- field11,
	NULL, -- field12,
	NULL, -- field13,
	NULL, -- field14,
	NULL, -- field15,
	NULL, -- field16,
	NULL, -- field17,
	NULL, -- field18,
	NULL, -- field19,
	NULL -- field20
from CohortSTU cohort
where cohort.isNonDegreeSeeking = true
    and cohort.studentType in ('First Time', 'Transfer')
    and cohort.studentLevelUGGR = 'UG' --all versions
	
union

--Part E: First-time Bachelor's Cohort Retention Rates

select 'E', -- part,
	6, -- sortorder,
	NULL, -- field1,
	case when Ftft = 0 then 1 else Ftft end, -- field2, --Full-time, first-time bachelor's cohort, 1 to 999999 ***error in spec - not allowing 0
	case when FtftEx = 0 then 1 else FtftEx end, -- field3, --Full-time, first-time bachelor's cohort exclusions, 1 to 999999 ***error in spec - not allowing 0
	case when FtftIn = 0 then 1 else FtftIn end, -- field4, --Full-time, first-time bachelor's cohort inclusions, 1 to 999999 ***error in spec - not allowing 0
	case when FtftEn = 0 then 1 else FtftEn end, -- field5, --Full-time, first-time bachelor's cohort students still enrolled in current fall term, 1 to 999999 ***error in spec - not allowing 0
	case when Ptft = 0 then 1 else Ptft end, -- field6, --Part-time, first-time bachelor's cohort, 1 to 999999 ***error in spec - not allowing 0
	case when PtftEx = 0 then 1 else PtftEx end, -- field7, --Part-time, first-time bachelor's cohort exclusions, 1 to 999999 ***error in spec - not allowing 0
	case when PtftIn = 0 then 1 else PtftIn end, -- field8, --Part-time, first-time bachelor's cohort inclusions, 1 to 999999 ***error in spec - not allowing 0
	case when PtftEn = 0 then 1 else PtftEn end, -- field9, --Part-time, first-time bachelor's cohort students still enrolled in current fall term, 1 to 999999 ***error in spec - not allowing 0
	NULL, -- field10,
	NULL, -- field11,
	NULL, -- field12,
	NULL, -- field13,
	NULL, -- field14,
	NULL, -- field15,
	NULL, -- field16,
	NULL, -- field17,
	NULL, -- field18,
	NULL, -- field19,
	NULL -- field20
from (
	select coalesce(SUM(case when cohortret.timeStatus = 'Full Time' then cohortret.ipedsEnrolled end), 0) Ftft, --Full-time, first-time bachelor's cohort, 1 to 999999
			coalesce(SUM(case when cohortret.timeStatus = 'Full Time' then cohortret.exclusion end), 0) FtftEx, --Full-time, first-time bachelor's cohort exclusions, 1 to 999999
			coalesce(SUM(case when cohortret.timeStatus = 'Full Time' then cohortret.inclusion end), 0) FtftIn, --Full-time, first-time bachelor's cohort inclusions, 1 to 999999
			coalesce(SUM(case when cohortret.timeStatus = 'Full Time' then cohortret.inclusion end), 0) FtftEn, --Full-time, first-time bachelor's cohort students still enrolled in current fall term, 1 to 999999
			coalesce(SUM(case when cohortret.timeStatus = 'Part Time' then cohortret.ipedsEnrolled end), 0) Ptft, --Part-time, first-time bachelor's cohort, 1 to 999999
			coalesce(SUM(case when cohortret.timeStatus = 'Part Time' then cohortret.exclusion end), 0) PtftEx, --Part-time, first-time bachelor's cohort exclusions, 1 to 999999
			coalesce(SUM(case when cohortret.timeStatus = 'Part Time' then cohortret.inclusion end), 0) PtftIn, --Part-time, first-time bachelor's cohort inclusions, 1 to 999999
			coalesce(SUM(case when cohortret.timeStatus = 'Part Time' then cohortret.inclusion end), 0) PtftEn
    from CohortExclusionMCR_RET cohortret	
	)
	
union

--Part F: Student-to-Faculty Ratio

select 'F', -- part,
	7, -- sortorder,
	NULL, -- field1,
	CAST(ROUND(coalesce(ftestu.FTE, 0)/coalesce(fteemp.FTE, 1)) as int), -- field2, --Student-to-faculty ratio, 0 - 100
	NULL, -- field3,
	NULL, -- field4,
	NULL, -- field5,
	NULL, -- field6,
	NULL, -- field7,
	NULL, -- field8,
	NULL, -- field9,
	NULL, -- field10,
	NULL, -- field11,
	NULL, -- field12,
	NULL, -- field13,
	NULL, -- field14,
	NULL, -- field15,
	NULL, -- field16,
	NULL, -- field17,
	NULL, -- field18,
	NULL, -- field19,
	NULL -- field20
from FTE_STU ftestu
cross join FTE_EMP fteemp
