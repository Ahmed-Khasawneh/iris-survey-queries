/********************

EVI PRODUCT:    DORIS 2020-21 IPEDS Survey Spring Collection
FILE NAME:      Human Resources v1 (HR1)
FILE DESC:      Human Resources for degree-granting institutions and related administrative offices that have 15 or more full-time staff and a tenure system
AUTHOR:         akhasawneh
CREATED:        20210203

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Formatting Views
Cohort Refactoring
Survey Formatting

SUMMARY OF CHANGES

Date(yyyymmdd)      Author              Tag             Comments
-----------------   ----------------    -------------   ----------------------------------------------------------------------
20210428            jhanicak                            PF-2161 Add coalesce, upper and default values
20210408            jhanicak                            PF-2140 Modify boolean filters to use 1 or 0 (no null, false, true)
20210407            akhasawneh                 			PF-2137 Add OccCat5 is null to correct counts
20210330            jhanicak                            PF-2099 Add new fields, modify logic in views
20210203            akhasawneh                 			Initial version - 1m 58s (prod), 1m 49s (test)

********************/

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.

--Production Default (Begin)
select '2021' surveyYear, 
	'HR1' surveyId,
	'HR Reporting End' repPeriodTag1, --used for all status updates and IPEDS tables
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2019-11-01' AS DATE) reportingDateStart, --newHireStartDate
	CAST('2020-10-31' AS DATE) reportingDateEnd, --newHireendDate
	'M' genderForUnknown, --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    CAST('2020-11-01' AS DATE) asOfDate,
	'N' hrIncludeSecondarySalary --Y = Yes, N = No

/*
--Test Default (Begin)
select '1415' surveyYear, --'1415' surveyYear, 
	'HR1' surveyId,
	'HR Reporting End' repPeriodTag1, --used for all status updates and IPEDS tables
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2013-11-01' AS DATE) reportingDateStart, --newHireStartDate
	CAST('2014-10-31' AS DATE) reportingDateEnd, --newHireendDate
	'M' genderForUnknown, --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    CAST('2014-11-01' AS DATE) asOfDate,
	'N' hrIncludeSecondarySalary --Y = Yes, N = No
*/
),

/*
ReportingPeriodMCR as (
-- This view will no longer be needed for HR reporting. Fields being pulled in here under the 'HR1' surveyId are defined by IPEDS and can be assumed in the DefaultVales view. 
),
*/

ClientConfigMCR as ( 
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 
--          1st union 1st order - pull snapshot for 'June End' 
--          1st union 2nd order - pull snapshot for 'August End'
--          1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--          2nd union - pull default values if no record in IPEDSClientConfig

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.source source,
    to_date(ConfigLatest.snapshotDate,'YYYY-MM-DD') snapshotDate,
	upper(ConfigLatest.genderForUnknown) genderForUnknown,
	upper(ConfigLatest.genderForNonBinary) genderForNonBinary,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
    ConfigLatest.reportingDateStart reportingDateStart,
    ConfigLatest.reportingDateEnd reportingDateEnd,
    ConfigLatest.asOfDate asOfDate,
    upper(ConfigLatest.hrIncludeSecondarySalary) hrIncludeSecondarySalary
    --'N' hrIncludeSecondarySalary
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'configFullYearTag' source,
		clientConfigENT.snapshotDate snapshotDate, 
		coalesce(clientConfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		coalesce(clientConfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
		defvalues.repPeriodTag1 repPeriodTag1,
        defvalues.reportingDateStart reportingDateStart,
        defvalues.reportingDateEnd reportingDateEnd,
        defvalues.asOfDate asOfDate,
        coalesce(clientConfigENT.hrIncludeSecondarySalary, defvalues.hrIncludeSecondarySalary) hrIncludeSecondarySalary,
		row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
				(case when array_contains(clientConfigENT.tags, defvalues.repPeriodTag1) then 1 else 2 end) asc,
			    to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') desc,
				clientConfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientConfigENT
	    cross join DefaultValues defvalues on clientConfigENT.surveyCollectionYear = defvalues.surveyYear

    union

	select defvalues.surveyYear surveyYear,
	    'default' source,
	    CAST('9999-09-09' as DATE) snapshotDate,
		defvalues.genderForUnknown genderForUnknown,
		defvalues.genderForNonBinary genderForNonBinary,
		defvalues.repPeriodTag1 repPeriodTag1,
        defvalues.reportingDateStart reportingDateStart,
        defvalues.reportingDateEnd reportingDateEnd,
        defvalues.asOfDate asOfDate,
        defvalues.hrIncludeSecondarySalary hrIncludeSecondarySalary,
		1 configRn
    from DefaultValues defvalues
    where defvalues.surveyYear not in (select max(configENT.surveyCollectionYear)
										from IPEDSClientConfig configENT
										where configENT.surveyCollectionYear = defvalues.surveyYear)
	) ConfigLatest
where ConfigLatest.configRn = 1	
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

EmployeeMCR AS (

select *,
--New Hires between 11/01/2019 and 10/31/20
    case when (hireDate >= reportingDateStart and hireDate <= reportingDateEnd) then 1 
        else 0 
    end isNewHire,
--Current employees as-of 11/01/2020
    case when (((terminationDate is null) 
                or (terminationDate > asOfDate
                and hireDate <= asOfDate))
            and ((recordActivityDate != CAST('9999-09-09' AS DATE) and employeeStatus in ('Active', 'Leave with Pay'))
                or recordActivityDate = CAST('9999-09-09' AS DATE))) then 1 
         else 0 
    end isCurrentEmployee
from (
    select empENT.personId personId,
	    coalesce(empENT.isIpedsMedicalOrDental, false) isIpedsMedicalOrDental,
	    empENT.primaryFunction primaryFunction,
	    empENT.employeeGroup employeeGroup,
        to_date(empENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
        empENT.employeeStatus employeeStatus, 
        coalesce(to_date(empENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        to_date(empENT.hireDate,'YYYY-MM-DD') hireDate,
        to_date(empENT.terminationDate,'YYYY-MM-DD') terminationDate,
        to_date(empENT.leaveStartDate,'YYYY-MM-DD') leaveStartDate,
        to_date(empENT.leaveEndDate,'YYYY-MM-DD') leaveEndDate,
		coalesce(ROW_NUMBER() OVER (
			PARTITION BY
				empENT.personId
			ORDER BY
			    (case when to_date(empENT.snapshotDate,'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(empENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(empENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(empENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(empENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				empENT.recordActivityDate desc
		), 1) empRn,
        repperiod.asOfDate asOfDate,
        repperiod.reportingDateStart reportingDateStart,
		repperiod.reportingDateEnd reportingDateEnd,
		repperiod.hrIncludeSecondarySalary hrIncludeSecondarySalary,
		repperiod.genderForUnknown genderForUnknown,
		repperiod.genderForNonBinary genderForNonBinary
    from Employee empENT 
        cross join ClientConfigMCR repperiod
			on ((coalesce(to_date(empENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
				and to_date(empENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.asOfDate)
			        or coalesce(to_date(empENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
    where ((empENT.terminationDate is null) -- all non-terminated employees
			or (to_date(empENT.hireDate,'YYYY-MM-DD') BETWEEN to_date(repperiod.reportingDateStart,'YYYY-MM-DD') 
				and to_date(repperiod.reportingDateEnd,'YYYY-MM-DD')) -- new hires between Nov 1 and Oct 31
			or (to_date(empENT.terminationDate,'YYYY-MM-DD') > repperiod.asOfDate
				and to_date(empENT.hireDate,'YYYY-MM-DD') <= repperiod.asOfDate)) -- employees terminated after the as-of date
			and coalesce(empENT.isIpedsReportable, true) = true
	)
where empRn = 1
),

EmployeeAssignmentMCR AS (
--Employee primary assignment

select *
from (
	select emp.personId personId, 
        emp.snapshotDate snapshotDate,
        emp.isIpedsMedicalOrDental isIpedsMedicalOrDental,
        emp.primaryFunction primaryFunction,
        emp.employeeGroup employeeGroup,
        emp.isNewHire isNewHire,
        emp.isCurrentEmployee isCurrentEmployee,
        emp.asOfDate asOfDate,
        emp.reportingDateStart reportingDateStart,
        emp.reportingDateEnd reportingDateEnd,
        emp.hrIncludeSecondarySalary hrIncludeSecondarySalary,
		emp.genderForUnknown genderForUnknown,
		emp.genderForNonBinary genderForNonBinary,
        empassignENT.fullOrPartTimeStatus fullOrPartTimeStatus,
        coalesce(empassignENT.isFaculty, false) isFaculty,
	    empassignENT.position position,
	    empassignENT.assignmentDescription assignmentDescription,
        empassignENT.employeeClass employeeClass,
        empassignENT.annualSalary annualSalary,
        coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        empassignENT.assignmentStatus assignmentStatus,
        coalesce(empassignENT.isUndergradStudent, false) isUndergradStudent,
		coalesce(empassignENT.isWorkStudy, false) isWorkStudy,
		coalesce(empassignENT.isTempOrSeasonal, false) isTempOrSeasonal,
        coalesce(ROW_NUMBER() OVER (
			PARTITION BY
				emp.personId,
				empassignENT.personId,
				empassignENT.position,
				empassignENT.suffix
			ORDER BY
			    (case when to_date(empassignENT.snapshotDate,'YYYY-MM-DD') = emp.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(empassignENT.snapshotDate, 'YYYY-MM-DD') > emp.snapshotDate then to_date(empassignENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(empassignENT.snapshotDate, 'YYYY-MM-DD') < emp.snapshotDate then to_date(empassignENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				empassignENT.assignmentStartDate desc,
                empassignENT.recordActivityDate desc
         ), 1) jobRn
    from EmployeeMCR emp
		left join EmployeeAssignment empassignENT on emp.personId = empassignENT.personId
			and ((coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
				and to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD') <= emp.asOfDate)
			        or coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
			and to_date(empassignENT.assignmentStartDate, 'YYYY-MM-DD') <= emp.asOfDate
			and (empassignENT.assignmentendDate is null 
				or to_date(empassignENT.assignmentendDate, 'YYYY-MM-DD') >= emp.asOfDate)
			and coalesce(empassignENT.isIpedsReportable, true) = true
			and empassignENT.assignmentType = 'Primary'
			and empassignENT.position is not null
    where emp.isCurrentEmployee = 1
        or emp.isNewHire = 1 
     )
where jobRn = 1
    and ((recordActivityDate != CAST('9999-09-09' AS DATE) and assignmentStatus = 'Active')
                or recordActivityDate = CAST('9999-09-09' AS DATE))
    and isUndergradStudent = false
    and isWorkStudy = false
    and isTempOrSeasonal = false
),

EmployeeAssignmentMCR_SEC AS (
--Employee second assignment

select personId personId,
	SUM(annualSalary) annualSalary2
from (
	select emp.personId personId, 
        emp.snapshotDate snapshotDate,
        emp.isIpedsMedicalOrDental isIpedsMedicalOrDental,
        emp.primaryFunction primaryFunction,
        emp.employeeGroup employeeGroup,
        emp.isNewHire isNewHire,
        emp.isCurrentEmployee isCurrentEmployee,
        emp.asOfDate asOfDate,
        emp.reportingDateStart reportingDateStart,
        emp.reportingDateEnd reportingDateEnd,
        emp.hrIncludeSecondarySalary hrIncludeSecondarySalary,
		emp.genderForUnknown genderForUnknown,
		emp.genderForNonBinary genderForNonBinary,
        empassignENT.fullOrPartTimeStatus fullOrPartTimeStatus,
        coalesce(empassignENT.isFaculty, false) isFaculty,
	    upper(empassignENT.position) position,
	    empassignENT.assignmentDescription assignmentDescription,
        empassignENT.employeeClass employeeClass,
        empassignENT.annualSalary annualSalary,
        coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        empassignENT.assignmentStatus assignmentStatus,
        coalesce(empassignENT.isUndergradStudent, false) isUndergradStudent,
		coalesce(empassignENT.isWorkStudy, false) isWorkStudy,
		coalesce(empassignENT.isTempOrSeasonal, false) isTempOrSeasonal,
        coalesce(ROW_NUMBER() OVER (
			PARTITION BY
				emp.personId,
				empassignENT.personId,
				empassignENT.position,
				empassignENT.suffix
			ORDER BY
			    (case when to_date(empassignENT.snapshotDate,'YYYY-MM-DD') = emp.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(empassignENT.snapshotDate, 'YYYY-MM-DD') > emp.snapshotDate then to_date(empassignENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(empassignENT.snapshotDate, 'YYYY-MM-DD') < emp.snapshotDate then to_date(empassignENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				empassignENT.assignmentStartDate desc,
                empassignENT.recordActivityDate desc
         ), 1) jobRn
    from EmployeeMCR emp
		left join EmployeeAssignment empassignENT on emp.personId = empassignENT.personId
			and ((coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
				and to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD') <= emp.asOfDate)
			        or coalesce(to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
			and to_date(empassignENT.assignmentStartDate, 'YYYY-MM-DD') <= emp.asOfDate
			and (empassignENT.assignmentendDate is null 
				or to_date(empassignENT.assignmentendDate, 'YYYY-MM-DD') >= emp.asOfDate)
			and coalesce(empassignENT.isIpedsReportable, true) = true
			and empassignENT.assignmentType = 'Secondary'
			and empassignENT.position is not null
    where (emp.isCurrentEmployee = 1
            or emp.isNewHire = 1)
        and emp.hrIncludeSecondarySalary = 'Y'        
  )
where jobRn = 1
    and ((recordActivityDate != CAST('9999-09-09' AS DATE) and assignmentStatus = 'Active')
            or recordActivityDate = CAST('9999-09-09' AS DATE))
    and isUndergradStudent = false
    and isWorkStudy = false
    and isTempOrSeasonal = false
group by personId
),

EmployeePositionMCR AS (

select *
from (
	select empassign.personId personId, 
        empassign.snapshotDate snapshotDate,
        empassign.isIpedsMedicalOrDental isIpedsMedicalOrDental,
        empassign.primaryFunction primaryFunction,
        empassign.employeeGroup employeeGroup,
        empassign.isNewHire isNewHire,
        empassign.isCurrentEmployee isCurrentEmployee,
        empassign.fullOrPartTimeStatus fullOrPartTimeStatus,
        empassign.isFaculty isFaculty,
	    empassign.position position,
	    empassign.assignmentDescription assignmentDescription,
        empassign.employeeClass employeeClass,
        empassign.annualSalary annualSalary,
        empassign.asOfDate asOfDate,
        empassign.reportingDateStart reportingDateStart,
        empassign.reportingDateEnd reportingDateEnd,
		empassign.genderForUnknown genderForUnknown,
		empassign.genderForNonBinary genderForNonBinary,
        coalesce(to_date(empposENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
	    empposENT.positionDescription positionDescription,
        empposENT.positionStatus positionStatus,
	    empposENT.standardOccupationalCategory standardOccupationalCategory,
    	empposENT.skillClass skillClass,
	    empposENT.positionGroup positionGroup,
	    empposENT.positionClass positionClass,
	    empposENT.federalEmploymentCategory federalEmploymentCategory,
		coalesce(ROW_NUMBER() OVER (
			PARTITION BY
			    empassign.personId,
			    empassign.position,
				empposENT.position
			ORDER BY
                (case when to_date(empposENT.snapshotDate,'YYYY-MM-DD') = empassign.snapshotDate then 1 else 2 end) asc,
                (case when to_date(empposENT.snapshotDate, 'YYYY-MM-DD') > empassign.snapshotDate then to_date(empposENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(empposENT.snapshotDate, 'YYYY-MM-DD') < empassign.snapshotDate then to_date(empposENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				empposENT.startDate DESC,
				empposENT.recordActivityDate DESC
        ), 1) empposRn
    from EmployeeAssignmentMCR empassign 
		left join EmployeePosition empposENT on empassign.position = upper(empposENT.position)
			and ((coalesce(to_date(empposENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
				and to_date(empposENT.recordActivityDate, 'YYYY-MM-DD') <= empassign.asOfDate)
			        or coalesce(to_date(empposENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
		and to_date(empposENT.startDate, 'YYYY-MM-DD') <= empassign.asOfDate
			and (empposENT.endDate is null 
				or to_date(empposENT.endDate, 'YYYY-MM-DD') >= empassign.asOfDate)
			and coalesce(empposENT.isIpedsReportable, true) = true
	)
where empposRn = 1
    and ((recordActivityDate != CAST('9999-09-09' AS DATE) and positionStatus = 'Active')
            or recordActivityDate = CAST('9999-09-09' AS DATE))
),

FacultyMCR AS (

select *
from (
	select DISTINCT 
	    emppos.personId personId,
        emppos.snapshotDate snapshotDate,
        emppos.isIpedsMedicalOrDental isIpedsMedicalOrDental,
        emppos.primaryFunction primaryFunction,
        (case when emppos.primaryFunction != 'None' and emppos.primaryFunction is not null 
            then emppos.primaryFunction
		    else emppos.standardOccupationalCategory 
	    end) employeeFunction,
        emppos.employeeGroup employeeGroup,
        emppos.isNewHire isNewHire,
        emppos.isCurrentEmployee isCurrentEmployee,
        emppos.fullOrPartTimeStatus fullOrPartTimeStatus,
        emppos.isFaculty isFaculty,
	    emppos.position position,
	    emppos.assignmentDescription assignmentDescription,
        emppos.employeeClass employeeClass,
        emppos.annualSalary + coalesce(empassignsec.annualSalary2, 0) totalSalary,
	    emppos.positionDescription positionDescription,
	    emppos.standardOccupationalCategory ESOC,
    	emppos.skillClass skillClass,
	    emppos.positionGroup positionGroup,
	    emppos.positionClass positionClass,
	    emppos.federalEmploymentCategory ECIP,
        emppos.asOfDate asOfDate,
        emppos.reportingDateStart reportingDateStart,
        emppos.reportingDateEnd reportingDateEnd,
		emppos.genderForUnknown genderForUnknown,
		emppos.genderForNonBinary genderForNonBinary,
    	facultyENT.facultyRank facultyRank,
		coalesce(ROW_NUMBER() OVER (
			PARTITION BY
				emppos.personId
            ORDER BY
                (case when to_date(facultyENT.snapshotDate,'YYYY-MM-DD') = emppos.snapshotDate then 1 else 2 end) asc,
                (case when to_date(facultyENT.snapshotDate, 'YYYY-MM-DD') > emppos.snapshotDate then to_date(facultyENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(facultyENT.snapshotDate, 'YYYY-MM-DD') < emppos.snapshotDate then to_date(facultyENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				facultyENT.recordActivityDate desc,
				facultyENT.facultyRankActionDate desc
          ), 1) facultyRn
    from EmployeePositionMCR emppos
		left join Faculty facultyENT on emppos.personId = facultyENT.personId
            and ((coalesce(to_date(facultyENT.facultyRankActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                            and to_date(facultyENT.facultyRankActionDate,'YYYY-MM-DD') <= emppos.asOfDate)
                        or (coalesce(to_date(facultyENT.facultyRankActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                            and ((coalesce(to_date(facultyENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' as DATE)
                                    and to_date(facultyENT.recordActivityDate,'YYYY-MM-DD') <= emppos.asOfDate)
                                or coalesce(to_date(facultyENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' as DATE))))
			and coalesce(facultyENT.isIpedsReportable, true) = true
		left join EmployeeAssignmentMCR_SEC empassignsec on empassignsec.personId = emppos.personId
  )
where facultyRn = 1
),

FacultyAppointmentMCR as (

select *
from (
    select *,
		(case when tenureStatus = 'Tenured' and tenureEffectiveDate <= reportingDateEnd then 'Tenured'
				when tenureStatus = 'Tenured' and tenureEffectiveDate >= reportingDateEnd then 'On Tenure Track'
				when tenureStatus = 'On Tenure Track' and tenureTrackStartDate <= reportingDateEnd then 'On Tenure Track'
				else 'Not on Tenure Track'
			end) tenure
	from(
		select distinct
            fac.personId personId, 
    	    fac.facultyRank facultyRank,
            fac.snapshotDate snapshotDate,
            fac.isIpedsMedicalOrDental isIpedsMedicalOrDental,
            fac.primaryFunction primaryFunction,
            fac.employeeFunction employeeFunction,
            fac.employeeGroup employeeGroup,
            fac.isNewHire isNewHire,
            fac.isCurrentEmployee isCurrentEmployee,
            fac.fullOrPartTimeStatus fullOrPartTimeStatus,
            fac.isFaculty isFaculty,
	        fac.position position,
	        fac.assignmentDescription assignmentDescription,
            fac.employeeClass employeeClass,
            fac.totalSalary totalSalary,
	        fac.positionDescription positionDescription,
	        fac.ESOC ESOC,
    	    fac.skillClass skillClass,
	        fac.positionGroup positionGroup,
	        fac.positionClass positionClass,
	        fac.ECIP ECIP, 
            fac.asOfDate asOfDate,
            to_date(fac.reportingDateStart,'YYYY-MM-DD') reportingDateStart,
            to_date(fac.reportingDateEnd,'YYYY-MM-DD') reportingDateEnd,
            fac.genderForUnknown genderForUnknown,
            fac.genderForNonBinary genderForNonBinary,
            facappENT.appointmentDecision appointmentDecision,
			facappENT.appointmentPosition appointmentPosition,
			facappENT.tenureStatus tenureStatus,
			to_date(facappENT.tenureEffectiveDate,'YYYY-MM-DD') tenureEffectiveDate,
			to_date(facappENT.tenureTrackStartDate,'YYYY-MM-DD') tenureTrackStartDate,
			facappENT.nonTenureContractLength nonTenureContractLength,
			facappENT.snapshotDate snapshotDateFacapp,
			coalesce(to_date(facappENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
            coalesce(ROW_NUMBER() OVER (
                PARTITION BY
                    fac.personId
                ORDER BY
                    (case when to_date(facappENT.snapshotDate,'YYYY-MM-DD') = fac.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(facappENT.snapshotDate, 'YYYY-MM-DD') > fac.snapshotDate then to_date(facappENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(facappENT.snapshotDate, 'YYYY-MM-DD') < fac.snapshotDate then to_date(facappENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    facappENT.appointmentStartDate desc,
                    facappENT.recordActivityDate desc
          ), 1) facappRn
		from FacultyMCR fac
		    left join FacultyAppointment facappENT on facappENT.personId = fac.personId
                and ((coalesce(to_date(facappENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                        and to_date(facappENT.recordActivityDate, 'YYYY-MM-DD') <= fac.asOfDate)
			        or coalesce(to_date(facappENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
		        and (facappENT.appointmentPosition is null
		            or upper(facappENT.appointmentPosition) = fac.position)
    		    and to_date(facappENT.appointmentStartDate, 'YYYY-MM-DD') <= fac.asOfDate
			    and (facappENT.appointmentEndDate is null 
				    or to_date(facappENT.appointmentEndDate, 'YYYY-MM-DD') >= fac.asOfDate)			    
			    and coalesce(facappENT.isIpedsReportable, true) = true
	)
) 
where facappRn = 1
and ((recordActivityDate is null 
        or (recordActivityDate != CAST('9999-09-09' AS DATE) and appointmentDecision = 'Accepted')
            or recordActivityDate = CAST('9999-09-09' AS DATE)))
),

PersonMCR as ( 
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

select pers.personId personId, 
            pers.snapshotDate snapshotDate,
            pers.isNewHire isNewHire,
            pers.isCurrentEmployee isCurrentEmployee,
            pers.isIpedsMedicalOrDental isIpedsMedicalOrDental,
            pers.fullOrPartTimeStatus fullOrPartTimeStatus,
            pers.employeeFunction employeeFunction,
	        pers.ESOC ESOC,
            pers.isFaculty isFaculty,
			pers.tenureStatus tenureStatus,
			pers.nonTenureContractLength nonTenureContractLength,
            pers.facultyRank facultyRank,
            pers.totalSalary totalSalary,
	        pers.ECIP ECIP,
    (case when pers.gender = 'Male' then 'M'
            when pers.gender = 'Female' then 'F' 
            when pers.gender = 'Non-Binary' then pers.genderForNonBinary
            else pers.genderForUnknown
    end) ipedsGender,
    (case when pers.isUSCitizen = true or ((pers.isInUSOnVisa = true or pers.asOfDate between pers.visaStartDate and pers.visaEndDate)
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
                else '9' end) 
            else '9' end) -- 'race and ethnicity unknown'
        when ((pers.isInUSOnVisa = true or pers.asOfDate between pers.visaStartDate and pers.visaEndDate)
                and pers.visaType in ('Student Non-resident', 'Employee Non-resident', 'Other Non-resident')) then '1' -- 'nonresident alien'
        else '9' -- 'race and ethnicity unknown'
    end) ipedsEthnicity,
    pers.asOfDate asOfDate,
    pers.reportingDateStart reportingDateStart,
    pers.reportingDateEnd reportingDateEnd
from ( 
    select distinct fac.personId personId, 
            fac.snapshotDate snapshotDate,
            fac.isNewHire isNewHire,
            fac.isCurrentEmployee isCurrentEmployee,
            fac.isIpedsMedicalOrDental isIpedsMedicalOrDental,
            fac.fullOrPartTimeStatus fullOrPartTimeStatus,
            fac.employeeFunction employeeFunction,
	        fac.ESOC ESOC,
            fac.isFaculty isFaculty,
			fac.tenure tenureStatus,
			fac.nonTenureContractLength nonTenureContractLength,
            fac.facultyRank facultyRank,
            fac.totalSalary totalSalary,
	        fac.ECIP ECIP,
            fac.asOfDate asOfDate,
            fac.reportingDateStart reportingDateStart,
            fac.reportingDateEnd reportingDateEnd,
            fac.genderForNonBinary genderForNonBinary,
            fac.genderForUnknown genderForUnknown,
            personENT.ethnicity ethnicity,
            coalesce(personENT.isHispanic, false) isHispanic,
            coalesce(personENT.isMultipleRaces, false) isMultipleRaces,
            coalesce(personENT.isInUSOnVisa, false) isInUSOnVisa,
            to_date(personENT.visaStartDate,'YYYY-MM-DD') visaStartDate,
            to_date(personENT.visaEndDate,'YYYY-MM-DD') visaEndDate,
            personENT.visaType visaType,
            coalesce(personENT.isUSCitizen, true) isUSCitizen,
            personENT.gender gender,
            upper(personENT.nation) nation,
            upper(personENT.state) state,
            coalesce(row_number() over (
                partition by
                    fac.personId,
                    personENT.personId
                order by
                    (case when to_date(personENT.snapshotDate,'YYYY-MM-DD') = fac.snapshotDate then 1 else 2 end) asc,
			        (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') > fac.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') < fac.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    personENT.recordActivityDate desc
            ), 1) personRn
    from FacultyAppointmentMCR fac 
        left join Person personENT on fac.personId = personENT.personId
            and coalesce(personENT.isIpedsReportable, true) = true
            and ((coalesce(to_date(personENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
               and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= fac.asOfDate) 
                or coalesce(to_date(personENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))
    ) pers
where pers.personRn = 1
),

/*****
BEGIN SECTION - Cohort Refactoring
The view below reformats and configures the base cohort fields based on the IPEDS survey specs
*****/

CohortRefactorEMP AS (

select cohortemp.personId personId,
	cohortemp.isCurrentEmployee currentEmployee, --1, 0
	cohortemp.isNewHire newHire, --1, 0
	cohortemp.fullOrPartTimeStatus fullPartInd, --Full Time, Part Time, Other
	(case when cohortemp.isIpedsMedicalOrDental = true then 1 else 2 end) isMedical, --1, 0
	(case when cohortemp.ipedsGender = 'M' then cohortemp.ipedsEthnicity
	   when cohortemp.ipedsGender = 'F'
	        then (case when cohortemp.ipedsEthnicity = '1' then '10' -- 'nonresident alien'
	                when cohortemp.ipedsEthnicity = '2' then '11' -- 'hispanic/latino'
	                when cohortemp.ipedsEthnicity = '3' then '12' -- 'American Indian or Alaskan Native'
	                when cohortemp.ipedsEthnicity = '4' then '13' -- 'Asian'
	                when cohortemp.ipedsEthnicity = '5' then '14' -- 'Black or African American'
	                when cohortemp.ipedsEthnicity = '6' then '15' --'Native Hawaiian or Other Pacific Islander'
	                when cohortemp.ipedsEthnicity = '7' then '16' -- 'Caucasian'
	                when cohortemp.ipedsEthnicity = '8' then '17' -- 'two or more races'
	                when cohortemp.ipedsEthnicity = '9' then '18' -- 'race and ethnicity unknown'
	             else '18' end)
    end) reg,
	(case when cohortemp.ipedsGender = 'M' then 1 else 2 end) gender,
	cohortemp.employeeFunction employeeFunction,
	cohortemp.ESOC ESOC,
	(case when cohortemp.isFaculty = true then 1 else 0 end) isFaculty, --1, 0
	(case when cohortemp.employeeFunction in (
					'Instruction with Research/Public Service',
					'Instruction - Credit',
					'Instruction - Non-credit',
					'Instruction - Combined Credit/Non-credit'
				) then 1
		else null
	end) isInstructional,
	cohortemp.tenureStatus tenureStatus,
	cohortemp.nonTenureContractLength nonTenureContractLength,
	(case when cohortemp.tenureStatus = 'Tenured' then 1
		when cohortemp.tenureStatus = 'On Tenure Track' then 2
		when cohortemp.nonTenureContractLength = 'Multi-year' then 3
		when cohortemp.nonTenureContractLength = 'Less than Annual' then 4
		when cohortemp.nonTenureContractLength = 'Annual' then 5
		when cohortemp.isFaculty = false then 6
		when cohortemp.nonTenureContractLength = 'Indefinite' then 7
		else 7 
	end) tenure,
	(case when cohortemp.facultyRank = 'Professor' then 1
		when cohortemp.facultyRank = 'Associate Professor' then 2
		when cohortemp.facultyRank = 'Assistant Professor' then 3
		when cohortemp.facultyRank = 'Instructor' then 4
		when cohortemp.facultyRank = 'Lecturer' then 5
		when cohortemp.facultyRank = 'No Academic Rank' then 6
        when cohortemp.facultyRank is null then 6
		when cohortemp.isFaculty = false then 7
		else 6 
	end) rankCode,
	cohortemp.totalSalary totalSalary,
--Occupational Category 1 - All Staff
	(case when cohortemp.isCurrentEmployee = 1 then
		(case when cohortemp.employeeFunction in ('Instruction with Research/Public Service', 'Instruction - Credit', 'Instruction - Non-credit', 'Instruction - Combined Credit/Non-credit') then 1 -- Instruction
			when cohortemp.employeeFunction = 'Research' then 2 --Research
			when cohortemp.employeeFunction = 'Public Service' then 3 --Public Service
			when cohortemp.employeeFunction like '25-401%' then 4
			when cohortemp.employeeFunction like '25-402%' then 5
			when cohortemp.employeeFunction like '25-403%' then 6
			when cohortemp.employeeFunction like '25-2%' then 7
            when cohortemp.employeeFunction like '25-3%' then 7
            when cohortemp.employeeFunction like '25-9%' then 7
			when cohortemp.employeeFunction like '11-%' then 8
			when cohortemp.employeeFunction like '13-%' then 9
			when cohortemp.employeeFunction like '15-%' then 9
            when cohortemp.employeeFunction like '17-%' then 9
            when cohortemp.employeeFunction like '19-%' then 10
			when cohortemp.employeeFunction like '21-%' then 11
            when cohortemp.employeeFunction like '23-%' then 11
            when cohortemp.employeeFunction like '27-%' then 11
			when cohortemp.employeeFunction like '29-%' then 12
			when cohortemp.employeeFunction like '31-%' then 13
            when cohortemp.employeeFunction like '33-%' then 13 
            when cohortemp.employeeFunction like '35-%' then 13 
            when cohortemp.employeeFunction like '37-%' then 13 
            when cohortemp.employeeFunction like '39-%' then 13
			when cohortemp.employeeFunction like '41-%' then 14
			when cohortemp.employeeFunction like '43-%' then 15
			when cohortemp.employeeFunction like '45-%' then 16
            when cohortemp.employeeFunction like '47-%' then 16
            when cohortemp.employeeFunction like '49-%' then 16
			when cohortemp.employeeFunction like '51-%' then 17
            when cohortemp.employeeFunction like '53-%' then 17
			else null 
		end)
	end) occCat1,
--Occupational Category 2 - Full time salaries
	(case when cohortemp.fullOrPartTimeStatus = 'Full Time' and cohortemp.isCurrentEmployee = 1 then
		(case when cohortemp.employeeFunction = 'Research' then 1 --Research
            when cohortemp.employeeFunction = 'Public Service' then 2 --Public Service
            when cohortemp.employeeFunction like '25-401%' then 3
			when cohortemp.employeeFunction like '25-402%' then 3
			when cohortemp.employeeFunction like '25-403%' then 3
			when cohortemp.employeeFunction like '25-2%' then 3
            when cohortemp.employeeFunction like '25-3%' then 3
            when cohortemp.employeeFunction like '25-9%' then 3
            when cohortemp.employeeFunction like '11-%' then 4
			when cohortemp.employeeFunction like '13-%' then 5
			when cohortemp.employeeFunction like '15-%' then 6
            when cohortemp.employeeFunction like '17-%' then 6
            when cohortemp.employeeFunction like '19-%' then 6
			when cohortemp.employeeFunction like '21-%' then 7
            when cohortemp.employeeFunction like '23-%' then 7
            when cohortemp.employeeFunction like '27-%' then 7
            when cohortemp.employeeFunction like '29-%' then 8           
			when cohortemp.employeeFunction like '31-%' then 9
            when cohortemp.employeeFunction like '33-%' then 9 
            when cohortemp.employeeFunction like '35-%' then 9 
            when cohortemp.employeeFunction like '37-%' then 9 
            when cohortemp.employeeFunction like '39-%' then 9
            when cohortemp.employeeFunction like '41-%' then 10
			when cohortemp.employeeFunction like '43-%' then 11
			when cohortemp.employeeFunction like '45-%' then 12
            when cohortemp.employeeFunction like '47-%' then 12
            when cohortemp.employeeFunction like '49-%' then 12
			when cohortemp.employeeFunction like '51-%' then 13
            when cohortemp.employeeFunction like '53-%' then 13
            else null
        end) 
    else null end) occCat2,
--Occupational Category 3 - Part-time Staff
	(case when cohortemp.fullOrPartTimeStatus = 'Part Time' and cohortemp.isCurrentEmployee = 1 then
		(case when cohortemp.employeeFunction = 'Instruction - Credit' then 1 --'Credit'
			when cohortemp.employeeFunction = 'Instruction - Non-credit' then 2 --'NonCredit'
			when cohortemp.employeeFunction = 'Instruction - Combined Credit/Non-credit' then 3 --'CombCredNonCred'
			when cohortemp.employeeFunction = 'Instruction with Research/Public Service' then 4 --'IRP'
			when cohortemp.employeeFunction = 'Research' then 5 --'R'
			when cohortemp.employeeFunction = 'Public Service' then 6 --'PS'
            when cohortemp.employeeFunction like '25-401%' then 7
			when cohortemp.employeeFunction like '25-402%' then 8
			when cohortemp.employeeFunction like '25-403%' then 9            
			when cohortemp.employeeFunction like '25-2%' then 10
            when cohortemp.employeeFunction like '25-3%' then 10
            when cohortemp.employeeFunction like '25-9%' then 10
            when cohortemp.employeeFunction like '11-%' then 11
			when cohortemp.employeeFunction like '13-%' then 12
            when cohortemp.employeeFunction like '15-%' then 13
            when cohortemp.employeeFunction like '17-%' then 13
            when cohortemp.employeeFunction like '19-%' then 13
            when cohortemp.employeeFunction like '21-%' then 14
            when cohortemp.employeeFunction like '23-%' then 14
            when cohortemp.employeeFunction like '27-%' then 14
			when cohortemp.employeeFunction like '29-%' then 15
            when cohortemp.employeeFunction like '31-%' then 16
            when cohortemp.employeeFunction like '33-%' then 16 
            when cohortemp.employeeFunction like '35-%' then 16 
            when cohortemp.employeeFunction like '37-%' then 16 
            when cohortemp.employeeFunction like '39-%' then 16
			when cohortemp.employeeFunction like '41-%' then 17
			when cohortemp.employeeFunction like '43-%' then 18
            when cohortemp.employeeFunction like '45-%' then 19
			when cohortemp.employeeFunction like '47-%' then 19
            when cohortemp.employeeFunction like '49-%' then 19
            when cohortemp.employeeFunction like '51-%' then 20
            when cohortemp.employeeFunction like '53-%' then 20
			when cohortemp.employeeFunction = 'Graduate Assistant - Teaching' then 22 -- Teaching Assistants, Postsecondary (25-9044) , '25-9000'
			when cohortemp.employeeFunction = 'Graduate Assistant - Research' then 23 -- Graduate Assistant Research
			when cohortemp.employeeFunction = 'Graduate Assistant - Other' then 24
		end)
	else null end) occCat3,
--Occupational category(4) - graduate assistants
	(case 
		when cohortemp.isCurrentEmployee = 1
			then (case 
					when cohortemp.employeeFunction = 'Graduate Assistant - Teaching' then 1 -- Teaching Assistants, Postsecondary (25-9044) , '25-9000'
					when cohortemp.employeeFunction = 'Graduate Assistant - Research' then 2 -- Research
					when cohortemp.employeeFunction = 'Graduate Assistant - Other' then 3 -- Other
                    else null 
                end)
        else null
    end) occCat4,
--Occupational category(5) - new hires
	(case when cohortemp.fullOrPartTimeStatus = 'Full Time' and cohortemp.isNewHire = 1 then
		(case when cohortemp.employeeFunction = 'Research' then 2 --Research
            when cohortemp.employeeFunction = 'Public Service' then 3 --Public Service
            when cohortemp.employeeFunction like '25-401%' then 4
			when cohortemp.employeeFunction like '25-402%' then 4
			when cohortemp.employeeFunction like '25-403%' then 4            
			when cohortemp.employeeFunction like '25-2%' then 4
            when cohortemp.employeeFunction like '25-3%' then 4
            when cohortemp.employeeFunction like '25-9%' then 4
            when cohortemp.employeeFunction like '11-%' then 5
			when cohortemp.employeeFunction like '13-%' then 6
            when cohortemp.employeeFunction like '15-%' then 7
            when cohortemp.employeeFunction like '17-%' then 7
            when cohortemp.employeeFunction like '19-%' then 7
            when cohortemp.employeeFunction like '21-%' then 8
            when cohortemp.employeeFunction like '23-%' then 8
            when cohortemp.employeeFunction like '27-%' then 8
			when cohortemp.employeeFunction like '29-%' then 9
            when cohortemp.employeeFunction like '31-%' then 10
            when cohortemp.employeeFunction like '33-%' then 10 
            when cohortemp.employeeFunction like '35-%' then 10 
            when cohortemp.employeeFunction like '37-%' then 10 
            when cohortemp.employeeFunction like '39-%' then 10
			when cohortemp.employeeFunction like '41-%' then 11
			when cohortemp.employeeFunction like '43-%' then 12
            when cohortemp.employeeFunction like '45-%' then 13
			when cohortemp.employeeFunction like '47-%' then 13
            when cohortemp.employeeFunction like '49-%' then 13
            when cohortemp.employeeFunction like '51-%' then 14
            when cohortemp.employeeFunction like '53-%' then 14
			else null 
		end)
	else null end) occCat5,
	(case when cohortemp.ECIP = '12 Month Instructional' then 1 else 0 end) count12M,
	(case when cohortemp.ECIP = '11 Month Instructional' then 1 else 0 end) count11M,
	(case when cohortemp.ECIP = '10 Month Instructional' then 1 else 0 end) count10M,
	(case when cohortemp.ECIP = '9 Month Instructional' then 1 else 0 end) count9M,
	(case when cohortemp.ECIP = 'Other Full Time' then 1 else 0 end) countL9M,
	(case when cohortemp.ECIP = 'Unreported' then 1 else 0 end) countOther,
	(case when cohortemp.ECIP = '12 Month Instructional' then cohortemp.totalSalary else 0 end) sal12M,
	(case when cohortemp.ECIP = '11 Month Instructional' then cohortemp.totalSalary else 0 end) sal11M,
	(case when cohortemp.ECIP = '10 Month Instructional' then cohortemp.totalSalary else 0 end) sal10M,
	(case when cohortemp.ECIP = '9 Month Instructional' then cohortemp.totalSalary else 0 end) sal9M,
	(case when cohortemp.ECIP = 'Other Full Time' then cohortemp.totalSalary else 0 end) salL9M,
	(case when cohortemp.ECIP = 'Unreported' then cohortemp.totalSalary else 0 end) salOther
from PersonMCR cohortemp
),

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/

TenureFMT AS (
select * 
from (
    VALUES
		(1), --Tenured (required if the school has tenure system)
		(2), --On tenure track (required if the school has tenure system)
		(3), --Mutli-year contract or employment agreement. Not on Tenure Track or no tenure system (required from all DG institutions with 15 or more employees)
		(4), --Annual contract or employment agreement. Not on Tenure Track or no tenure system (required from all DG institutions with 15 or more employees)
		(5), --Less than annual contract or employment agreement.  Not on Tenure Track or no tenure system (required from all DG institutions with 15 or more employees)
		(6), --Without faculty status (required from all DG institutions with 15 or more employees)
		(7)  --Indefinite duration (continuing or at-will) contract or employment agreement. Not on Tenure Track or no tenure system (required from all DG institutions with 15 or more employees)
  ) tenure (ipedsTenure)
),

RaceEthnicityGenderFMT AS (
select *
from (
	VALUES 
		(1), -- Nonresident alien men
		(2), -- Hispanic or Latino men
		(3), -- American Indian or Alaska Native men
		(4), -- Asian men
		(5), -- Black or African American men
		(6), -- Native Hawaiian or Other Pacific Islander men
		(7), -- White men
		(8), -- Two or more races men
		(9), -- Race/ethnicity unknown men
		(10), -- Nonresident alien women
		(11), -- Hispanic or Latino women
		(12), -- American Indian or Alaska Native women
		(13), -- Asian women
		(14), -- Black or African American women
		(15), -- Native Hawaiian or Other Pacific Islander women
		(16), -- White women
		(17), -- Two or more races women
		(18) -- Race/ethnicity unknown women
	) reg (ipedsReg)
),

FacultyRankFMT AS (
select *
from (
	VALUES 
		(1), -- Professors
		(2), -- Associate professors
		(3), -- Assistant professors
		(4), -- Instructors
		(5), -- Lecturers
		(6), -- No academic rank
		(7) -- Total.  This will be generated. Do not include in import file
	) rank(ipedsRank)
),

MedicalIndFMT AS (
select * 
from (
    VALUES
		(1), --Medical
		(2)  --NonMedical
	) med (ipedsMed)
),

OccupationalCat1FMT AS (
select * 
from (
	VALUES
		(1), -- Instructional Staff
		(2), -- Research Staff
		(3), -- Public Service Staff
		(4), -- Archivists, Curators, and Museum Technicians (25-4010)
		(5), -- Librarians and Media Collections Specialists (25-4020)
		(6), -- Library Technicians (25-4030)
		(7), -- Student and Academic Affairs and Other Education Services Occupations (25-2000 + 25-3000 + 25-9000)
		(8), -- Management Occupations (11-0000)
		(9), -- Business and Financial Operations Occupations (13-0000)
		(10), -- Computer, Engineering, and Science Occupations (15-0000 + 17-0000 + 19-0000)
		(11), -- Community, Social Service, Legal, Arts, Design, Entertainment, Sports, and Media Occupations (21-0000 + 23-0000 + 27-0000) 
		(12), -- Healthcare Practitioners and Technical Occupations (29-0000)
		(13), -- Service Occupations (31-0000 + 33-0000 + 35-0000 + 37-0000 + 39-0000)
		(14), -- Sales and Related Occupations (41-0000)
		(15), -- Office and Administrative Support Occupations (43-0000)
		(16), --Natural Resources, Construction, and Maintenance Occupations (45-0000 + 47-0000 + 49-0000)
		(17) --Production, Transportation, and Material Moving Occupations (51-0000 + 53-0000)
	) occCat1 (ipedsOccCat1)
),

OccupationalCat2FMT AS (
select * 
from (
	VALUES
		(1), -- Research Staff
		(2), -- Public Service Staff
		(3), -- Library and Student and Academic Affairs and Other Education Services Occupations (25-4000 + 25-2000 + 25-3000 + 25-9000)
		(4), -- Management Occupations (11-0000)
		(5), -- Business and Financial Operations Occupations (13-0000)
		(6), -- Computer, Engineering, and Science Occupations (15-0000 + 17-0000 + 19-0000)
		(7), -- Community, Social Service, Legal, Arts, Design, Entertainment, Sports, and Media Occupations (21-0000 + 23-0000 + 27-0000)
		(8), -- Healthcare Practitioners and Technical Occupations (29-0000)
		(9), -- Service Occupations (31-0000 + 33-0000 + 35-0000 + 37-0000 + 39-0000)
		(10), -- Sales and Related Occupations (41-0000)
		(11), -- Office and Administrative Support Occupations (43-0000)
		(12), -- Natural Resources, Construction, and Maintenance Occupations (45-0000 + 47-0000 + 49-0000)
		(13) -- Production, Transportation, and Material Moving Occupations (51-0000 + 53-0000)
	) occCat2 (ipedsOccCat2)
),

OccupationalCat3FMT AS (
select * 
from (
    VALUES
		(1), --Primarily Instruction - exclusively credit
		(2), --Primarily Instruction - exclusively not-for-credit
		(3), --Primarily Instruction - Combined credit/not-for-credit
		(4), --Instruction/research/public service
		(5), --Research Staff
		(6), --Public Service Staff
		(7), --Archivists, Curators, and Museum Technicians (25-4010)
		(8), --Librarians and Media Collections Specialists (25-4020)
		(9), --Library Technicians (25-4030)
		(10), --tudent and Academic Affairs and Other Education Services Occupations (25-2000 + 25-3000 + 25-9000)
		(11), --Management Occupations (11-0000)
		(12), --Business and Financial Operations Occupations (13-0000)
		(13), --Computer, Engineering, and Science Occupations (15-0000 + 17-0000 + 19-0000)
		(14), --Community, Social Service, Legal, Arts, Design, Entertainment, Sports, and Media Occupations (21-0000 + 23-0000 + 27-0000) 
		(15), --Healthcare Practitioners and Technical Occupations (29-0000)
		(16), --Service Occupations (31-0000 + 33-0000 + 35-0000 + 37-0000 + 39-0000)
		(17), --Sales and Related Occupations (41-0000)
		(18), --Office and Administrative Support Occupations (43-0000)
		(19), --Natural Resources, Construction, and Maintenance Occupations (45-0000 + 47-0000 + 49-0000)
		(20), --Production, Transportation, and Material Moving Occupations (51-0000 + 53-0000)
		(22), --Teaching Assistants, Postsecondary (25-9044)
		(23), --Graduate Assistant Research
		(24) --Graduate Assistant Other
	) occCat3 (ipedsOccCat3)
),

OccupationalCat4FMT AS (
select * 
from (
    VALUES
		(1), -- Teaching Assistants, Postsecondary (25-9044)
		(2), -- Research
		(3)  -- Other
	) occCat4 (ipedsOccCat4)
),

OccupationalCat5FMT AS (
select * 
from (
    VALUES
		(2), -- Research Staff
		(3), -- Public Service Staff
		(4), -- Library and Student and Academic Affairs and Other Education Services Occupations (25-4000 + 25-2000 + 25-3000 + 25-9000)
		(5), -- Management Occupations (11-0000)
		(6), -- Business and Financial Operations Occupations (13-0000)
		(7), -- Computer, Engineering, and Science Occupations (15-0000 + 17-0000 + 19-0000)
		(8), -- Community, Social Service, Legal, Arts, Design, Entertainment, Sports, and Media Occupations (21-0000 + 23-0000 + 27-0000)
		(9), -- Healthcare Practitioners and Technical Occupations (29-0000)
		(10), -- Service Occupations (31-0000 + 33-0000 + 35-0000 + 37-0000 + 39-0000)
		(11), -- Sales and Related Occupations (41-0000)
		(12), -- Office and Administrative Support Occupations (43-0000)
		(13), -- Natural Resources, Construction, and Maintenance Occupations (45-0000 + 47-0000 + 49-0000)
		(14)  -- Production, Transportation, and Material Moving Occupations (51-0000 + 53-0000)

	) occCat5 (ipedsOccCat5)
),

InstructionFunctionFMT AS (
select * 
from (
	VALUES
		(1), --Exclusively credit
		(2), --Exclusively not-for-credit
		(3), --Combined credit/not-for-credit
		(5)  --Instruction/research/public service
	) instrFunc (ipedsInstrFunc)
),

GenderFMT AS (
select * 
from (
    VALUES
		(1), --Male
		(2)  --Female
	) gender (ipedsGender)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs

Add the Part and description per the spec requirements
Add any additional documentation after the Part description
*****/

--A1
--Full-time instructional staff by tenure status, academic rank and race/ethnicity/gender

select 'A1' part,
	tenure field1,
	rankCode field2,
	reg field3,
	SUM(totalCount) field4,
	null field5,
	null field6,
	null field7,
	null field8,
	null field9,
	null field10,
	null field11
from (
    select refactoremp.tenure tenure,
		refactoremp.rankCode rankCode,
		refactoremp.reg reg,
		COUNT(refactoremp.personId) totalCount
    from CohortRefactorEMP refactoremp
    where refactoremp.currentEmployee = 1
		and refactoremp.isMedical = 2 -- Med = 1, NonMed = 2
		and refactoremp.fullPartInd = 'Full Time'
		and refactoremp.isInstructional = 1 
    group by refactoremp.tenure,
        refactoremp.rankCode,
        refactoremp.reg
    
    union
    
    select tenure.ipedsTenure,
            facrank.ipedsRank,
            raceethngender.ipedsReg,
            0
    from TenureFMT tenure
		cross join FacultyRankFMT facrank
		cross join RaceEthnicityGenderFMT raceethngender
)
where (rankCode < 7
	or (rankCode = 7 
		and tenure = 6))
group by tenure,
	rankCode,
	reg

union all

--A2
--Full-time instructional staff by tenure, medical school, and function

select 'A2', --part
	tenure,-- field1
	case when isMedical = 2 then 0 else isMedical end, -- Med = 1, NonMed = 2
	instrucFunction,-- field3
	SUM(totalCount), -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11
from (
    select refactoremp.tenure tenure,
		refactoremp.isMedical isMedical,
		case when refactoremp.employeeFunction = 'Instruction - Credit' then 1 --'Credit'
			when refactoremp.employeeFunction = 'Instruction - Non-credit' then 2 --'NonCredit'
			when refactoremp.employeeFunction = 'Instruction - Combined Credit/Non-credit' then 3 --'CombCredNonCred'
			when refactoremp.employeeFunction = 'Instruction with Research/Public Service' then 5 --'IRP'
		end instrucFunction,
		COUNT(refactoremp.personId) totalCount
	from CohortRefactorEMP refactoremp
	where refactoremp.currentEmployee = 1
		and refactoremp.fullPartInd = 'Full Time'
		and refactoremp.isInstructional = 1
	group by refactoremp.tenure,
		refactoremp.isMedical,
		refactoremp.employeeFunction
    
	union
    
	select tenure.ipedsTenure,
		medind.ipedsMed,
		instrfunc.ipedsInstrFunc,
		0
	from TenureFMT tenure
		cross join MedicalIndFMT medind
		cross join InstructionFunctionFMT instrfunc
)
group by tenure,
	isMedical,
	instrucFunction

union all

--B1
--Full-time non-instructional staff by occupational category, Race/Ethnicity/gender

select 'B1', --part
	occCat1, -- field1
	reg, -- field2
	SUM(totalCount), -- field3
	null, -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11
from (
    select refactoremp.occCat1 occCat1,
		refactoremp.reg reg,
		COUNT(refactoremp.personId) totalCount
    from CohortRefactorEMP refactoremp
    where refactoremp.currentEmployee = 1
        and refactoremp.fullPartInd = 'Full Time'
        and refactoremp.isInstructional is null
        and refactoremp.occCat1 > 1
    group by refactoremp.occCat1,
            refactoremp.reg
    
    union
    
    select occcat1.ipedsOccCat1,
		raceethngender.ipedsReg,
		0
    from OccupationalCat1FMT occcat1
		cross join RaceEthnicityGenderFMT raceethngender
    where occcat1.ipedsOccCat1 > 1
)
group by occCat1,
        reg

union all

--B2
--Full-time non-instructional staff by tenure, medical school, and occupational category

select 'B2', -- part
	tenure, -- field1
	case when isMedical = 2 then 0 else isMedical end, -- field2
	occCat1, -- field3
	SUM(totalCount), -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11
from (
	select refactoremp.tenure tenure,
		refactoremp.isMedical isMedical,
		refactoremp.occCat1 occCat1,
		COUNT(refactoremp.personId) totalCount
	from CohortRefactorEMP refactoremp
	where refactoremp.currentEmployee = 1
		and refactoremp.fullPartInd = 'Full Time'
        and refactoremp.isInstructional is null
        and refactoremp.occCat1 BETWEEN 2 and 12
	group by refactoremp.tenure, 
		refactoremp.isMedical,
		refactoremp.occCat1

	union all

	select tenure.ipedsTenure,
		medind.ipedsMed,
		occcat1.ipedsOccCat1,
        0
	from OccupationalCat1FMT occcat1
		cross join TenureFMT tenure
		cross join MedicalIndFMT medind
	where occcat1.ipedsOccCat1 BETWEEN 2 and 12
)
group by tenure, 
	isMedical,
	occCat1

union all

--B3
--Full-time non-instructional staff by medical school, and occupational category

select 'B3', -- part
	case when isMedical = 2 then 0 else isMedical end, -- field1
	occCat1, -- field2
	SUM(totalCount), -- field3
	null, -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11
from (
	select refactoremp.occCat1 occCat1,
		refactoremp.isMedical isMedical,
        COUNT(refactoremp.personId) totalCount
	from CohortRefactorEMP refactoremp
	where refactoremp.currentEmployee = 1
		and refactoremp.fullPartInd = 'Full Time'
        and refactoremp.isInstructional is null
        and refactoremp.occCat1 BETWEEN 13 and 17
	group by refactoremp.occCat1, refactoremp.isMedical

	union

	select occcat1.ipedsOccCat1,
		medind.ipedsMed,
        0
	from OccupationalCat1FMT occcat1
		cross join MedicalIndFMT medind
	where occcat1.ipedsOccCat1 BETWEEN 13 and 17
)
group by isMedical,
        occCat1
        
union all

--D1
--Part-time staff by occupational category, Race/Ethnicity/gender

select 'D1', -- part
	occCat1, -- field1
	reg, -- field2
	SUM(totalCount), -- field3
	null, -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11
from (
	select refactoremp.occCat1 occCat1,
		refactoremp.reg reg,
        COUNT(refactoremp.personId) totalCount
	from CohortRefactorEMP refactoremp
	where refactoremp.currentEmployee = 1
		and refactoremp.fullPartInd = 'Part Time'
		and refactoremp.occCat1 is not null
	group by refactoremp.occCat1, 
			refactoremp.reg

	union

	select occcat1.ipedsOccCat1,
        raceethngender.ipedsReg,
        0
	from OccupationalCat1FMT occcat1
		cross join RaceEthnicityGenderFMT raceethngender
)
group by occcat1, 
        reg

union all

--D2
--Graduate assistants by occupational category and race/ethnicity/gender

select 'D2', -- part
	occCat4, -- field1
	reg, -- field2
	SUM(totalCount), -- field3
	null, -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11
from (
    select refactoremp.occCat4 occCat4,
		refactoremp.reg reg,
        COUNT(refactoremp.personId) totalCount
    from CohortRefactorEMP refactoremp
    where refactoremp.currentEmployee = 1
        and refactoremp.occCat4 is not null
    group by refactoremp.occCat4,
            refactoremp.reg
    
    union
    
    select occcat4.ipedsOccCat4,
           raceethngender.ipedsReg,
           0
    from OccupationalCat4FMT occcat4
       cross join RaceEthnicityGenderFMT raceethngender
    where occcat4.ipedsOccCat4 is not null
)
group by occCat4, 
        reg

union all

--D3
--Part-time staff by tenure, medical school, and occupational category

select 'D3', -- part
	tenure, -- field1
	case when isMedical = 2 then 0 else isMedical end, -- field2
	occCat3, -- field3
	SUM(totalCount), -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11
from (
	select refactoremp.tenure,
		refactoremp.isMedical isMedical,
		refactoremp.occCat3 occCat3,
		COUNT(refactoremp.personId) totalCount
	from CohortRefactorEMP refactoremp
	where refactoremp.currentEmployee = 1
		and refactoremp.fullPartInd = 'Part Time'
		and refactoremp.occCat3 < 16
	group by refactoremp.tenure,
		refactoremp.isMedical,
		refactoremp.occCat3

    union
    
	select tenure.ipedsTenure,
		medind.ipedsMed,
		occCat3.ipedsOccCat3,
		0
	from TenureFMT tenure
		cross join MedicalIndFMT medind
		cross join OccupationalCat3FMT occcat3
	where occcat3.ipedsOccCat3 < 16
)
group by tenure,
	    isMedical,
	    occcat3

union all

--D4
--Part-time Non-instructional staff by medical school, and occupational category

select 'D4', -- part
	case when isMedical = 2 then 0 else isMedical end,-- field1
	occCat3, -- field2
	SUM(totalCount), -- field3
	null, -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11
from (
	select refactoremp.isMedical isMedical,
		refactoremp.occCat3 occCat3,
		COUNT(refactoremp.personId) totalCount
	from CohortRefactorEMP refactoremp
	where refactoremp.currentEmployee = 1
		and refactoremp.fullPartInd = 'Part Time'
		and refactoremp.isInstructional is null
		and refactoremp.occCat3 > 15
	group by refactoremp.isMedical,
		refactoremp.occCat3
    
	union
    
	select medind.ipedsMed,
		occCat3.ipedsOccCat3,
		0
	from MedicalIndFMT medind
		cross join OccupationalCat3FMT occcat3
	where occcat3.ipedsOccCat3 > 15
)
group by isMedical,
        occcat3

union all

--G1
--Salaries of Instructional staff

-- per RTI: Currently while the instructions and the page items do no advise that the staff
--   “without faculty status” are added into the “No academic rank” the system does take both
--    into account them together when comparing Part A and Part G. 

select 'G1',
	rankCode, -- field1
	gender, -- field2
	CAST(ROUND(SUM(count12M), 0) AS BIGINT), -- field3
	CAST(ROUND(SUM(count11M), 0) AS BIGINT), -- field4
	CAST(ROUND(SUM(count10M), 0) AS BIGINT), -- field5
	CAST(ROUND(SUM(count9M), 0) AS BIGINT), -- field6
	CAST(ROUND(SUM(countL9M), 0) AS BIGINT), -- field7
	CAST(ROUND(SUM(sal12M), 0) AS BIGINT), -- field8
	CAST(ROUND(SUM(sal11M), 0) AS BIGINT), -- field9
	CAST(ROUND(SUM(sal10M), 0) AS BIGINT), -- field10
	CAST(ROUND(SUM(sal9M), 0) AS BIGINT)  -- field11
from ( 
--added Without faculty Status employees to No Academic Rank COUNT per RTI (see comment above)
    select case when refactoremp.rankCode = 7 then 6 else refactoremp.rankCode end rankCode,
		refactoremp.gender gender,
		SUM(refactoremp.count12M) count12M,
		SUM(refactoremp.count11M) count11M,
		SUM(refactoremp.count10M) count10M,
		SUM(refactoremp.count9M) count9M,
		SUM(refactoremp.countL9M) countL9M,
		SUM(refactoremp.sal12M) sal12M,
		SUM(refactoremp.sal11M) sal11M,
		SUM(refactoremp.sal10M) sal10M,
		SUM(refactoremp.sal9M) sal9M
    from CohortRefactorEMP refactoremp
    where refactoremp.currentEmployee = 1
        and refactoremp.isMedical = 2
        and refactoremp.fullPartInd = 'Full Time'
        and refactoremp.isInstructional = 1
    group by refactoremp.rankCode,
		    refactoremp.gender

union

    select facrank.ipedsRank,
		gender.ipedsGender,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0
	from FacultyRankFMT facrank
		cross join GenderFMT gender
    where facrank.ipedsRank < 7)
where rankCode is not null
group by rankCode,
	gender

union all

--G2
--Salaries of non-instructional staff

select 'G2', -- part
	occCat2, -- field1
	CAST(ROUND(SUM(totalSalary), 0) as BIGINT), -- field2
	null, -- field3
	null, -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11
from (
	select refactoremp.occCat2 occCat2,
		SUM(refactoremp.totalSalary) totalSalary
    from CohortRefactorEMP refactoremp
    where refactoremp.currentEmployee = 1
		and refactoremp.isMedical = 2
		and refactoremp.fullPartInd = 'Full Time'
        and refactoremp.isInstructional is null
        and refactoremp.occCat2 is not null
    group by refactoremp.occCat2
    
    union
    
	select occcat2.ipedsOccCat2,
		0
	from OccupationalCat2FMT occcat2
)
group by occcat2

union all

--H1
--Full-time new hire instructional staff by tenure status and race/ethnicity/gender

select 'H1', -- part
	tenure, -- field1
    reg, -- field2
    SUM(totalCount),-- field3
	null, -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11
from (
	select refactoremp.tenure tenure,
		refactoremp.reg reg,
		COUNT(refactoremp.personId) totalCount
	from CohortRefactorEMP refactoremp
	where refactoremp.newHire = 1
		and refactoremp.fullPartInd = 'Full Time'
        and refactoremp.isInstructional = 1
	group by refactoremp.tenure,
		refactoremp.reg
    
    union
    
    select tenure.ipedsTenure,
		raceethngender.ipedsReg,
		0
    from TenureFMT tenure
		cross join RaceEthnicityGenderFMT raceethngender
)
group by tenure,
	    reg

union all

--H2
--New hires by occupational category, Race/Ethnicity/gender

select 'H2', -- part
	occCat5, -- field1
    reg, -- field2
    SUM(totalCount),-- field3
    null, -- field4
	null, -- field5
	null, -- field6
	null, -- field7
	null, -- field8
	null, -- field9
	null, -- field10
	null -- field11 
from (
    select refactoremp.occCat5 occCat5,
        refactoremp.reg reg,
        COUNT(refactoremp.personId) totalCount
    from CohortRefactorEMP refactoremp
    where refactoremp.newHire = 1
        and refactoremp.fullPartInd = 'Full Time'
        and refactoremp.occCat5 is not null
    group by refactoremp.occCat5,
            refactoremp.reg
    
    union
    
    select occcat5.ipedsOccCat5,
        raceethngender.ipedsReg,
        0
    from OccupationalCat5FMT occcat5
		cross join RaceEthnicityGenderFMT raceethngender
    )
group by occcat5,
        reg

order by 1, 2, 3 asc
