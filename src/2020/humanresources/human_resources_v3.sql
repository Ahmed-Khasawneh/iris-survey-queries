/********************

EVI PRODUCT:    DORIS 2020-21 IPEDS Survey Spring Collection
FILE NAME:      Human Resources v3 (HR2)
FILE DESC:      Human Resources for degree-granting institutions and related administrative offices that have less than 15 full-time staff
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
20210420            akhasawneh                 		    PF-2153 Correction to boolean field filters. Correction to surveyId.
20210203            akhasawneh                 		    Initial version - 1m 30s (prod), 1m 2s (test)

********************/

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.

--Production Default (Begin)
select '2021' surveyYear, 
	'HR2' surveyId,
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
select '1415' surveyYear, 
	'HR2' surveyId,
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

/*
--Not applicable for v3
FacultyAppointmentMCR as (
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
			--pers.tenureStatus tenureStatus,
			--pers.nonTenureContractLength nonTenureContractLength,
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
			--fac.tenure tenureStatus,
			--fac.nonTenureContractLength nonTenureContractLength,
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
    from FacultyMCR fac 
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
/* --n/a for v3
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
*/ --n/a for v3
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
--Occupational category table
	case when cohortemp.isCurrentEmployee = 1 then 
                case when cohortemp.employeeFunction in (
								'Instruction with Research/Public Service', 
								'Instruction - Credit', 
								'Instruction - Non-credit', 
								'Instruction - Combined Credit/Non-credit')  then 1 -- Instruction
					when cohortemp.employeeFunction = 'Research' then 2 -- Research
					when cohortemp.employeeFunction = 'Public Service' then 3 -- Public Service
                    when cohortemp.employeeFunction like '25-400%' then 4 -- Librarians, Curators, and Archivists
                    when cohortemp.employeeFunction like '25-401%' then 4
                    when cohortemp.employeeFunction like '25-402%' then 4
                    when cohortemp.employeeFunction like '25-403%' then 4
                    when cohortemp.employeeFunction like '25-2%' then 5 -- Student and Academic Affairs and Other Education Services Occupations
                    when cohortemp.employeeFunction like '25-3%' then 5
                    when cohortemp.employeeFunction like '25-9%' then 5
                    when cohortemp.employeeFunction like '11-%' then 6 -- Management Occupations
                    when cohortemp.employeeFunction like '13-%' then 7 -- Business and Financial Operations Occupations
                    when cohortemp.employeeFunction like '15-%' then 8 -- Computer, Engineering, and Science Occupations
                    when cohortemp.employeeFunction like '17-%' then 8
                    when cohortemp.employeeFunction like '19-%' then 8
                    when cohortemp.employeeFunction like '21-%' then 9 -- Community, Social Service, Legal, Arts, Design, Entertainment, Sports and Media Occupations
                    when cohortemp.employeeFunction like '23-%' then 9
                    when cohortemp.employeeFunction like '27-%' then 9
                    when cohortemp.employeeFunction like '29-%' then 10 -- Healthcare Practitioners and Technical Occupations
                    when cohortemp.employeeFunction like '31-%' then 11 -- Service Occupations
                    when cohortemp.employeeFunction like '33-%' then 11 
                    when cohortemp.employeeFunction like '35-%' then 11 
                    when cohortemp.employeeFunction like '37-%' then 11 
                    when cohortemp.employeeFunction like '39-%' then 11
                    when cohortemp.employeeFunction like '41-%' then 12 -- Sales and Related Occupations
                    when cohortemp.employeeFunction like '43-%' then 13 -- Office and Administrative Support Occupations
                    when cohortemp.employeeFunction like '45-%' then 14 -- Natural Resources, Construction, and Maintenance Occupations
                    when cohortemp.employeeFunction like '47-%' then 14
                    when cohortemp.employeeFunction like '49-%' then 14
                    when cohortemp.employeeFunction like '51-%' then 15 -- Production, Transportation, and Material Moving Occupations
                    when cohortemp.employeeFunction like '53-%' then 15
					else null
				end
	end occCat,
--Graduate assistants occupational category table
	case when cohortemp.isCurrentEmployee = 1 then 
        case when cohortemp.employeeFunction = 'Graduate Assistant - Teaching' then 1 -- Teaching Assistants, Postsecondary (25-9044) , '25-9000'
            when cohortemp.employeeFunction = 'Graduate Assistant - Research' then 2 -- Research
            when cohortemp.employeeFunction = 'Graduate Assistant - Other' then 3 -- Other
				end
	end gaOccCat,
--Salaries occupational category table
	case when cohortemp.fullOrPartTimeStatus = 'Full Time' and cohortemp.isCurrentEmployee = 1 then 
            case when cohortemp.employeeFunction = 'Research' then 1 -- Research
                when cohortemp.employeeFunction = 'Public Service' then 2 -- Public Service
                when cohortemp.employeeFunction like '25-4%' then 3 -- Library and Student and Academic Affairs and Other Education Services Occupations
                when cohortemp.employeeFunction like '25-2%' then 3 
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
		else null
	end saOccCat,
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
	) rank (ipedsRank)
),

MedicalIndFMT AS (
select * 
from (
    VALUES
		(1), --Medical
		(2)  --NonMedical
	) med (ipedsMed)
),

OccupationalCatFMT AS (
select *
from (
	VALUES
		(1), -- Instructional Staff
		(2), -- Research Staff
		(3), -- Public Service Staff
		(4), -- Librarians, Curators, and Archivists (25-4000)
		(5), -- Student and Academic Affairs and Other Education Services Occupations (25-2000 + 25-3000 + 25-9000)
		(6), -- Management Occupations (11-0000)
		(7), -- Business and Financial Operations Occupations (13-0000)
		(8), -- Computer, Engineering, and Science Occupations (15-0000 + 17-0000 + 19-0000)
		(9), -- Community, Social Service, Legal, Arts, Design, Entertainment, Sports and Media Occupations  (21-0000 + 23-0000 + 27-0000)
		(10), -- Healthcare Practitioners and Technical Occupations (29-0000)
		(11), -- Service Occupations (31-0000 + 33-0000 + 35-0000 + 37-0000 + 39-0000)
		(12), -- Sales and Related Occupations (41-0000)
		(13), -- Office and Administrative Support Occupations (43-0000)
		(14), -- Natural Resources, Construction, and Maintenance Occupations (45-0000 + 47-0000 + 49-0000)
		(15) -- Production, Transportation, and Material Moving Occupations (51-0000 + 53-0000)
	) occCat (ipedsOccCat)
),

OccupationalCatGradFMT AS (
select *
from (
	VALUES 
		(1), -- Teaching Assistants, Postsecondary (25-9044)
		(2), -- Research
		(3) -- Other
	) gaOccCat (ipedsGaOccCat)
),

OccupationalCatSalaryFMT AS (
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
	) saOccCat (ipedsSaOccCat)
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
group by occCat,
		reg

union all

--B1 
-- Part-time staff by racial/ethnic category, gender, and occupational category

select 'B1', -- part
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
group by occCat,
	reg

union all

-- B2 
-- Graduate assistants by occupational category and race/ethnicity/gender

select 'B2', -- part
	gaOccCat, -- field1
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
	select refactoremp.gaOccCat gaOccCat,
		refactoremp.reg reg,
		COUNT(refactoremp.personId) totalCount
	from CohortRefactorEMP refactoremp
	where refactoremp.currentEmployee = 1
		and refactoremp.gaOccCat is not null
	group by refactoremp.gaOccCat,
		refactoremp.reg
	
	union
	
	select occcatgrad.ipedsGaOccCat,
		raceethngender.ipedsReg,
		0
	from OccupationalCatGradFMT occcatgrad
		cross join RaceEthnicityGenderFMT raceethngender
	)
group by gaOccCat,
		reg

union all

-- G1 
-- Salaries of Full-time Instructional staff

-- per RTI: Currently while the instructions and the page items do not advise that the staff
--   ‚without faculty status‚ are added into the ‚academic rank‚ the system does take both
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

-- G2 
-- Salaries of Full-time non-instructional staff

select 'G2', -- part
	saOccCat, -- field1
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
	select refactoremp.saOccCat saOccCat,
		SUM(refactoremp.totalSalary) totalSalary
	from CohortRefactorEMP refactoremp
	where refactoremp.currentEmployee = 1
		and refactoremp.fullPartInd = 'Full Time'
		and refactoremp.isInstructional is null
		and refactoremp.saOccCat is not null
	group by refactoremp.saOccCat
	
    union
	
	select occcatsalary.ipedsSaOccCat,
		0
	from OccupationalCatSalaryFMT occcatsalary
	)
group by saOccCat
