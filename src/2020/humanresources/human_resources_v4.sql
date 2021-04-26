/********************

EVI PRODUCT:    DORIS 2020-21 IPEDS Survey Spring Collection
FILE NAME:      Human Resources v4 (HR3)
FILE DESC:      Human Resources for non-degree-granting institutions and related administrative offices
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
20210426            jhanicak                            PF-2153/PF-2161 Fix errors, add newest updates
20210420            akhasawneh                 			PF-2153 Correction to boolean field filters. Correction to surveyId.
20210203            akhasawneh                 			Initial version - 1m 2s (prod), 52s (test)

********************/

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.

--Production Default (Begin)
select '2021' surveyYear, 
	'HR3' surveyId,
	'HR Reporting End' repPeriodTag1, --used for all status updates and IPEDS tables
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2019-11-01' AS DATE) reportingDateStart, --newHireStartDate
	CAST('2020-10-31' AS DATE) reportingDateEnd, --newHireendDate
	'M' genderForUnknown, --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
--***** start survey-specific mods
    CAST('2020-11-01' AS DATE) asOfDate,
	'N' hrIncludeSecondarySalary --Y = Yes, N = No
--***** end survey-specific mods

/*
--Test Default (Begin)
select '1415' surveyYear, 
	'HR3' surveyId,
	'HR Reporting End' repPeriodTag1, --used for all status updates and IPEDS tables
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2013-11-01' AS DATE) reportingDateStart, --newHireStartDate
	CAST('2014-10-31' AS DATE) reportingDateEnd, --newHireendDate
	'M' genderForUnknown, --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
--***** start survey-specific mods
    CAST('2014-11-01' AS DATE) asOfDate,
	'N' hrIncludeSecondarySalary --Y = Yes, N = No
--***** end survey-specific mods
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
--***** start survey-specific mods
    ConfigLatest.reportingDateStart reportingDateStart,
    ConfigLatest.reportingDateEnd reportingDateEnd,
    ConfigLatest.asOfDate asOfDate,
    upper(ConfigLatest.hrIncludeSecondarySalary) hrIncludeSecondarySalary
    --'N' hrIncludeSecondarySalary
--***** end survey-specific mods
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'configFullYearTag' source,
		clientConfigENT.snapshotDate snapshotDate, 
		coalesce(clientConfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		coalesce(clientConfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
		defvalues.repPeriodTag1 repPeriodTag1,
--***** start survey-specific mods
        defvalues.reportingDateStart reportingDateStart,
        defvalues.reportingDateEnd reportingDateEnd,
        defvalues.asOfDate asOfDate,
        coalesce(clientConfigENT.hrIncludeSecondarySalary, defvalues.hrIncludeSecondarySalary) hrIncludeSecondarySalary,
--***** end survey-specific mods
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
--***** start survey-specific mods
        defvalues.reportingDateStart reportingDateStart,
        defvalues.reportingDateEnd reportingDateEnd,
        defvalues.asOfDate asOfDate,
        defvalues.hrIncludeSecondarySalary hrIncludeSecondarySalary,
--***** end survey-specific mods
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

/* --n/a for this version 
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
        to_date(empassignENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
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
*/

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

/* --Not applicable for v4
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


--Not applicable for v4
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
*/

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
            pers.annualSalary totalSalary,
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
    select distinct emp.personId personId, 
            emp.snapshotDate snapshotDate,
            emp.isNewHire isNewHire,
            emp.isCurrentEmployee isCurrentEmployee,
            emp.isIpedsMedicalOrDental isIpedsMedicalOrDental,
            emp.fullOrPartTimeStatus fullOrPartTimeStatus,
            (case when emp.primaryFunction != 'None' and emp.primaryFunction is not null then emp.primaryFunction
		    	else emp.standardOccupationalCategory 
	        end) employeeFunction,
	        emp.standardOccupationalCategory ESOC,
            emp.isFaculty isFaculty,
            emp.annualSalary annualSalary,
            emp.federalEmploymentCategory ECIP,
            emp.asOfDate asOfDate,
            emp.reportingDateStart reportingDateStart,
            emp.reportingDateEnd reportingDateEnd,
            emp.genderForNonBinary genderForNonBinary,
            emp.genderForUnknown genderForUnknown,
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
                    emp.personId,
                    personENT.personId
                order by
                    (case when to_date(personENT.snapshotDate,'YYYY-MM-DD') = emp.snapshotDate then 1 else 2 end) asc,
			        (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') > emp.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') < emp.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    personENT.recordActivityDate desc
            ), 1) personRn
    from EmployeePositionMCR emp
        left join Person personENT on emp.personId = personENT.personId
            and coalesce(personENT.isIpedsReportable, true) = true
            and ((coalesce(to_date(personENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
               and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= emp.asOfDate) 
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
	cohortemp.fullOrPartTimeStatus fullPartInd, --Full Time, Part Time, Other
	(case when cohortemp.ipedsGender = 'M' then cohortemp.ipedsEthnicity
		  when cohortemp.ipedsGender = 'F' then (case when cohortemp.ipedsEthnicity = '1' then '10' -- 'nonresident alien'
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
--Occupational Category - All Staff
	case when cohortemp.isCurrentEmployee = 1 then 
		case when cohortemp.employeeFunction in ('Instruction with Research/Public Service',
													'Instruction - Credit',
													'Instruction - Non-credit',
													'Instruction - Combined Credit/Non-credit'
													) then 1 -- Instruction
			when cohortemp.employeeFunction like '25-4%' then 2 -- Librarians, Curators, and Archivists
			when cohortemp.employeeFunction like '25-2%' then 3 -- Student and Academic Affairs and Other Education Services Occupations
			when cohortemp.employeeFunction like '25-3%' then 3
			when cohortemp.employeeFunction like '25-9%' then 3
			when cohortemp.employeeFunction like '11-%' then 4 -- Management Occupations
			when cohortemp.employeeFunction like '13-%' then 5
			when cohortemp.employeeFunction like '15-%' then 6 -- Computer, Engineering, and Science Occupations
			when cohortemp.employeeFunction like '17-%' then 6
			when cohortemp.employeeFunction like '19-%' then 6
			when cohortemp.employeeFunction like '21-%' then 7 -- Community, Social Service, Legal, Arts, Design, Entertainment, Sports and Media Occupations
			when cohortemp.employeeFunction like '23-%' then 7
			when cohortemp.employeeFunction like '27-%' then 7
			when cohortemp.employeeFunction like '29-%' then 8 -- Healthcare Practitioners and Technical Occupations
			when cohortemp.employeeFunction like '31-%' then 9 -- Service Occupations
			when cohortemp.employeeFunction like '33-%' then 9
			when cohortemp.employeeFunction like '35-%' then 9
			when cohortemp.employeeFunction like '37-%' then 9
			when cohortemp.employeeFunction like '39-%' then 9
			when cohortemp.employeeFunction like '41-%' then 10 -- Sales and Related Occupations
			when cohortemp.employeeFunction like '43-%' then 11 -- Office and Administrative Support Occupations
			when cohortemp.employeeFunction like '45-%' then 12 -- Natural Resources, Construction, and Maintenance Occupations
			when cohortemp.employeeFunction like '47-%' then 12
			when cohortemp.employeeFunction like '49-%' then 12
			when cohortemp.employeeFunction like '51-%' then 13 -- Production, Transportation, and Material Moving Occupations
			when cohortemp.employeeFunction like '53-%' then 13
			else null 
		end
	end occCat
from PersonMCR cohortemp
--from EmployeePositionMCR cohortemp --bypassing FacultyMCR which does not apply to this version
),

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/

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

OccupationalCatFMT AS (
select *
from (
	VALUES 
		(1), -- Instructional Staff
		(2), -- Librarians, Curators, and Archivists (25-4000)
		(3), -- Student and Academic Affairs and Other Education Services Occupations (25-2000 + 25-3000 + 25-9000)
		(4), -- Management Occupations (11-0000)
		(5), -- Business and Financial Operations Occupations (13-0000)
		(6), -- Computer, Engineering, and Science Occupations (15-0000 + 17-0000 + 19-0000)
		(7), -- Community, Social Service, Legal, Arts, Design, Entertainment, Sports and Media Occupations  (21-0000 + 23-0000 + 27-0000)
		(8), -- Healthcare Practitioners and Technical Occupations (29-0000)
		(9), -- Service Occupations (31-0000 + 33-0000 + 35-0000 + 37-0000 + 39-0000)
		(10), -- Sales and Related Occupations (41-0000)
		(11), -- Office and Administrative Support Occupations (43-0000)
		(12), -- Natural Resources, Construction, and Maintenance Occupations (45-0000 + 47-0000 + 49-0000)
		(13) -- Production, Transportation, and Material Moving Occupations (51-0000 + 53-0000)
	) occCat (ipedsOccCat)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs

Add the Part and description per the spec requirements
Add any additional documentation after the Part description
*****/

-- A1 
-- Full-time staff by racial/ethnic category, gender, and occupational category

select 'A1' part,
	occCat field1,
	reg field2,
	SUM(totalCount) field3,
	null field4,
	null field5,
	null field6,
	null field7,
	null field8,
	null field9,
	null field10,
	null field11
from (
	select refactoremp.occCat occCat,
		refactoremp.reg reg,
		COUNT(refactoremp.personId) totalCount
    from CohortRefactorEMP refactoremp
	where refactoremp.currentEmployee = 1
		and refactoremp.fullPartInd = 'Full Time'
		and refactoremp.occCat is not null
	group by refactoremp.occCat,
		refactoremp.reg
	
	union
	
	select occcat.ipedsOccCat,
		raceethngender.ipedsReg,
		0
	from OccupationalCatFMT occcat
		cross join RaceEthnicityGenderFMT raceethngender
	)
group by occCat, reg

union all

-- B1 
-- Part-time staff by racial/ethnic category, gender, and occupational category

select 'B1', --part
	occCat, -- field1
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
	select refactoremp.occCat occCat,
		refactoremp.reg reg,
		COUNT(refactoremp.personId) totalCount
    from CohortRefactorEMP refactoremp
    where refactoremp.currentEmployee = 1
        and refactoremp.fullPartInd = 'Part Time'
		and refactoremp.occCat is not null
	group by refactoremp.occCat,
            refactoremp.reg
	
	union
	
	select occcat.ipedsOccCat,
		raceethngender.ipedsReg,
		0
	from OccupationalCatFMT occcat
		cross join RaceEthnicityGenderFMT raceethngender
	)
group by occCat, reg
