/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Spring Collection
FILE NAME:      Human Resources v2 (HR1)
FILE DESC:      Human Resources for degree-granting institutions and related administrative offices that have 15 or more full-time staff, No tenure System
AUTHOR:         Janet Hanicak
CREATED:        20191218

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Formatting Views
Cohort Creation
Cohort Refactoring
Survey Formatting

SUMMARY OF CHANGES

Date(yyyymmdd)      Author              Tag             Comments
-----------------   ----------------    -------------   ----------------------------------------------------------------------
20200527            jhanicak                            Remove reference to IPEDSClientConfig.hrIncludeTenure field PF-1488
20200427			jhanicak			jh 20200428		Add default values and filter most current record queries on prior query PF-1318
20200424 	    	akhasawneh 			ak 20200424 	Adding dummy date considerations (PF-1375)
20200423            akhasawneh          ak 20200423     Modification to use 'isInUSOnVisa' and 'isUSCitizen' for ethnicity (PF-1441)
20200203            jhanicak            jh 20200203     Added default values for IPEDSReportingPeriod and IPEDSClientConfig
20200106            akhasawneh                          Move original code to template 
20200103            jhanicak                            Initial version 

********************/
 
/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.

select '1920' surveyYear, 
        'HR1' surveyId,
		CAST('2019-11-01' AS TIMESTAMP) asOfDate,
		CAST('2018-11-01' AS TIMESTAMP) newHireStartDate,
		CAST('2019-10-31' AS TIMESTAMP) newHireEndDate,
		'N' hrIncludeSecondarySalary, --Y = Yes, N = No
		'M' genderForUnknown, --M = Male, F = Female
		'F' genderForNonBinary  --M = Male, F = Female

--Use for testing internally only
/*
select '1516' surveyYear, 
        'HR1' surveyId,
		CAST('2015-11-01' AS TIMESTAMP) asOfDate,
		CAST('2014-11-01' AS TIMESTAMP) newHireStartDate,
		CAST('2015-10-31' AS TIMESTAMP) newHireEndDate,
		'N' hrIncludeSecondarySalary, --Y = Yes, N = No
		'M' genderForUnknown, --M = Male, F = Female
		'F' genderForNonBinary  --M = Male, F = Female
*/
),

ConfigPerAsOfDate as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

--jh 20200428 rewrote and added default fields

select ConfigLatest.surveyYear surveyYear,
			ConfigLatest.hrIncludeSecondarySalary hrIncludeSecondarySalary,
			ConfigLatest.genderForUnknown genderForUnknown,
			ConfigLatest.genderForNonBinary genderForNonBinary,
			ConfigLatest.surveyId surveyId,
		    ConfigLatest.asOfDate asOfDate,
		    ConfigLatest.newHireStartDate newHireStartDate,
		    ConfigLatest.newHireEndDate newHireEndDate
from (
		select config.surveyCollectionYear surveyYear,
				nvl(config.hrIncludeSecondarySalary, defaultValues.hrIncludeSecondarySalary) hrIncludeSecondarySalary,
				nvl(config.genderForUnknown, defaultValues.genderForUnknown) genderForUnknown,
				nvl(config.genderForNonBinary, defaultValues.genderForNonBinary) genderForNonBinary,
				defaultValues.surveyId surveyId,
		        defaultValues.asOfDate asOfDate,
		        defaultValues.newHireStartDate newHireStartDate,
		        defaultValues.newHireEndDate newHireEndDate,
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
        select defaultValues.surveyYear surveyYear,
				defaultValues.hrIncludeSecondarySalary hrIncludeSecondarySalary,
				defaultValues.genderForUnknown genderForUnknown,
				defaultValues.genderForNonBinary genderForNonBinary,
				defaultValues.surveyId surveyId,
		        defaultValues.asOfDate asOfDate,
		        defaultValues.newHireStartDate newHireStartDate,
		        defaultValues.newHireEndDate newHireEndDate,
				1 configRn
        from DefaultValues defaultValues
		where defaultValues.surveyYear not in (select config.surveyCollectionYear
												from IPEDSClientConfig config
												where config.surveyCollectionYear = defaultValues.surveyYear)
		) ConfigLatest
where ConfigLatest.configRn = 1
),

ReportingDates as (
--Returns client specified reporting period for HR reporting and defaults to IPEDS specified date range if not otherwise defined 

--jh 20200428 rewrote and added Config fields

select surveyYear surveyYear,
		asOfDate asOfDate,
		newHireStartDate newHireStartDate,
		newHireEndDate newHireEndDate,
		hrIncludeSecondarySalary hrIncludeSecondarySalary,
		genderForUnknown genderForUnknown,
		genderForNonBinary genderForNonBinary
from (
	select ReportPeriod.surveyCollectionYear surveyYear,
			nvl(ReportPeriod.asOfDate, defaultValues.asOfDate) asOfDate,
			nvl(ReportPeriod.reportingDateStart, defaultValues.newHireStartDate) newHireStartDate,
			nvl(ReportPeriod.reportingDateEnd, defaultValues.newHireEndDate) newHireEndDate,
			defaultValues.hrIncludeSecondarySalary hrIncludeSecondarySalary,
			defaultValues.genderForUnknown genderForUnknown,
			defaultValues.genderForNonBinary genderForNonBinary,
			row_number() over (
				partition by
					reportPeriod.surveyCollectionYear
				order by
					reportPeriod.recordActivityDate desc
			) reportPeriodRn
	from ConfigPerAsOfDate defaultValues
		cross join IPEDSReportingPeriod ReportPeriod
	where reportPeriod.surveyCollectionYear = defaultValues.surveyYear
		and reportPeriod.surveyId like defaultValues.surveyId
	
	union
	
	select defValues.surveyYear surveyYear,
			defValues.asOfDate asOfDate,
			defValues.newHireStartDate newHireStartDate,
			defValues.newHireEndDate newHireEndDate,
			defValues.hrIncludeSecondarySalary hrIncludeSecondarySalary,
			defValues.genderForUnknown genderForUnknown,
			defValues.genderForNonBinary genderForNonBinary,
			1 reportPeriodRn
	from ConfigPerAsOfDate defValues
		where defValues.surveyYear not in (select reportPeriod.surveyCollectionYear
											from ConfigPerAsOfDate defaultValues
												cross join IPEDSReportingPeriod reportPeriod
											where reportPeriod.surveyCollectionYear = defaultValues.surveyYear
												and reportPeriod.surveyId like defaultValues.surveyId)
    )
where reportPeriodRn = 1
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

EmployeePerAsOfDate AS (
select *
from (
--jh 20200428 added ReportingDates and Config fields
    select employee.*,
		ReportingDates.asOfDate asOfDate,
		ReportingDates.hrIncludeSecondarySalary hrIncludeSecondarySalary,
		ReportingDates.genderForUnknown genderForUnknown,
		ReportingDates.genderForNonBinary genderForNonBinary,
--jh 20200123 added isNewHire and isCurrentEmployee fields here in order to remove ReportingDates from EmployeeBase
            CASE WHEN (employee.hireDate >= ReportingDates.newHireStartDate
                        and employee.hireDate <= ReportingDates.newHireEndDate)
                        THEN 1 
                 ELSE null 
            END isNewHire,
        CASE WHEN ((employee.terminationDate is null) 
					or (employee.terminationDate > ReportingDates.asOfDate
                    and employee.hireDate <= ReportingDates.asOfDate)) THEN 1 
			 ELSE null 
		END isCurrentEmployee,
		ROW_NUMBER() OVER (
		PARTITION BY
			employee.personId
		ORDER BY
			employee.recordActivityDate DESC
	) employeeRn
    from ReportingDates ReportingDates
-- ak 20200424 Adding dummy date considerations (PF-1375).
		inner join Employee employee 
			on ((employee.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and employee.recordActivityDate <= ReportingDates.asOfDate)
			        or employee.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
    where ((employee.terminationDate is null) -- all non-terminated employees
		or (employee.hireDate BETWEEN ReportingDates.newHireStartDate 
			and ReportingDates.newHireEndDate) -- new hires between Nov 1 and Oct 31
			or (employee.terminationDate > ReportingDates.asOfDate
				and employee.hireDate <= ReportingDates.asOfDate)) -- employees terminated after the as-of date
--and employee.employeeStatus = 'Active'
            and employee.isIpedsReportable = 1
    )
where employeeRn = 1
),

PrimaryJobPerAsOfDate AS (
select *
from (
    select job.*,
		employee.asOfDate asOfDate,
        row_number() over (
					partition by
						job.personId,
						job.position,
						job.suffix
					order by
						job.recordActivityDate desc
         ) jobRn
--jh 20200428 filter on records in EmployeePerAsOfDate and removed ReportingDates since needed fields have been added to EmployeePerAsOfDate
    from EmployeePerAsOfDate employee
-- ak 20200423 Adding dummy date considerations (PF-1375).
		inner join EmployeeAssignment job on employee.personId = job.personId
			and ((job.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and job.recordActivityDate <= employee.asOfDate)
			        or job.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and job.assignmentStartDate <= employee.asOfDate
			and (job.assignmentEndDate is null 
				or job.assignmentEndDate >= employee.asOfDate)
			and job.isUndergradStudent = 0
			and job.isWorkStudy = 0
			and job.isTempOrSeasonal = 0
			and job.isIpedsReportable = 1
			and job.assignmentType = 'Primary'
  )
where jobRn = 1
),

PersonPerAsOfDate AS (
select *
from (
    select person.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                person.personId
            ORDER BY
                person.recordActivityDate DESC
            ) personRn
--jh 20200428 filter on records in EmployeePerAsOfDate and removed ReportingDates since needed fields have been added to EmployeePerAsOfDate
    from EmployeePerAsOfDate employee
-- ak 20200424 Adding dummy date considerations (PF-1375).
		inner join Person person on employee.personId = person.personId
			and ((person.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and person.recordActivityDate <= employee.asOfDate)
			        or person.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
    where person.isIpedsReportable = 1
    )
where personRn = 1
),

PositionPerAsOfDate AS (
select *
from (
	select position.*,
		ROW_NUMBER() OVER (
		PARTITION BY
			position.position
		ORDER BY
			position.startDate DESC,
			position.recordActivityDate DESC
        ) positionRn
--jh 20200428 filter on records in PrimaryJobPerAsOfDate and removed ReportingDates since needed fields have been added to PrimaryJobPerAsOfDate
    from PrimaryJobPerAsOfDate job 
-- ak 20200424 Adding dummy date considerations (PF-1375).
		inner join EmployeePosition position on job.position = position.position
			and ((position.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and position.recordActivityDate <= job.asOfDate)
			        or position.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and position.startDate <= job.asOfDate
			and (position.endDate is null 
				or position.endDate >= job.asOfDate)
--and position.positionStatus = 'Active'
			and position.isIpedsReportable = 1
  )
where positionRn = 1
),

SecondaryJobSalary AS (
select personId personId,
         SUM(annualSalary) annualSalary
--jh 20200428 filter on records in EmployeePerAsOfDate and removed ReportingDates and ConfigPerAsOfDate since needed fields have been added to EmployeePerAsOfDate
from(
    select job.*,
		employee.asOfDate asOfDate,
        row_number() over (
					partition by
						job.personId,
						job.position,
						job.suffix
					order by
						job.recordActivityDate desc
         ) jobRn
--jh 20200428 filter on records in EmployeePerAsOfDate and removed ReportingDates since needed fields have been added to EmployeePerAsOfDate
from EmployeePerAsOfDate employee
-- ak 20200423 Adding dummy date considerations (PF-1375).
		inner join EmployeeAssignment job on employee.personId = job.personId 
			and ((job.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and job.recordActivityDate <= employee.asOfDate)
			        or job.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 	
			and job.assignmentStartDate <= employee.asOfDate
		and (job.assignmentEndDate is null 
				or job.assignmentEndDate >= employee.asOfDate)
		and job.isUndergradStudent = 0
		and job.isWorkStudy = 0
		and job.isTempOrSeasonal = 0
			and job.isIpedsReportable = 1
		and job.assignmentType = 'Secondary'
  )
where jobRn = 1
group by personId
),

FacultyPerAsOfDate AS (
select *
from (
    select faculty.*,
		ROW_NUMBER() OVER (
		PARTITION BY
			faculty.personId
		ORDER BY
			faculty.appointmentStartDate DESC,
			faculty.recordActivityDate DESC
          ) facultyRn
--jh 20200428 filter on records in EmployeePerAsOfDate and removed ReportingDates since needed fields have been added to EmployeePerAsOfDate
    from EmployeePerAsOfDate employee
-- ak 20200424 Adding dummy date considerations (PF-1375).
		left join Faculty faculty on employee.personId = faculty.personId
			and ((faculty.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and faculty.recordActivityDate <= employee.asOfDate)
			        or faculty.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 		
			and ((faculty.appointmentDecisionDate is not null
					and faculty.appointmentDecision = 'Accepted'
					and (faculty.appointmentStartDate <= employee.asOfDate
					and (faculty.appointmentEndDate is null 
						or faculty.appointmentEndDate >= employee.asOfDate)))
					or (faculty.facultyRank is not null))
			and faculty.isIpedsReportable = 1
  )
where facultyRn = 1
),

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/

TenureValues AS (
select * 
from (
	VALUES
		--(1), --Tenured (required if the school has tenure system)
		--(2), --On tenure track (required if the school has tenure system)
		(3), --Mutli-year contract or employment agreement. Not on Tenure Track or no tenure system (required from all DG institutions with 15 or more employees)
		(4), --Annual contract or employment agreement. Not on Tenure Track or no tenure system (required from all DG institutions with 15 or more employees)
		(5), --Less than annual contract or employment agreement.  Not on Tenure Track or no tenure system (required from all DG institutions with 15 or more employees)
		(6), --Without faculty status (required from all DG institutions with 15 or more employees)
		(7)  --Indefinite duration (continuing or at-will) contract or employment agreement. Not on Tenure Track or no tenure system (required from all DG institutions with 15 or more employees)
	) tenure (ipedsTenure)
),

RegValues AS (
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

RankValues AS (
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

MedValues AS (
select * 
from (
    VALUES
		(1), --Medical
		(2)  --NonMedical
	) med (ipedsMed)
),

OccCat1Values AS (
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

OccCat2Values AS (
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

OccCat3Values AS (
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

OccCat4Values AS (
select * 
from (
    VALUES
		(1), -- Teaching Assistants, Postsecondary (25-9044)
		(2), -- Research
		(3)  -- Other
	) occCat4 (ipedsOccCat4)
),

OccCat5Values AS (
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

InstrFuncValues AS (
select * 
from (
    VALUES
		(1), --Exclusively credit
		(2), --Exclusively not-for-credit
		(3), --Combined credit/not-for-credit
		(5)  --Instruction/research/public service
	) instrFunc (ipedsInstrFunc)
),

GenderValues AS (
select * 
from (
    VALUES
		(1), --Male
		(2)  --Female
  ) gender (ipedsGender)
),

/*****
BEGIN SECTION - Cohort Creation
The view below pulls the base cohort based on IPEDS survey requirements and the data model definitions
*****/

EmployeeBase AS (
select employee.personId personId,
--jh 20200123 moved the assignment of the isNewHire and isCurrentEmployee fields to EmployeePerAsOfDate
       employee.isNewHire isNewHire,
       employee.isCurrentEmployee isCurrentEmployee,
	job.fullOrPartTimeStatus fullOrPartTimeStatus,
	employee.isIpedsMedicalOrDental isIpedsMedicalOrDental,
		case when person.gender = 'Male' then 'M'
			when person.gender = 'Female' then 'F'
--jh 20200428 added config values
			when person.gender = 'Non-Binary' then employee.genderForNonBinary
			else employee.genderForUnknown
		end gender,
--jh 20200428 added ethnicity fields and moved reg field to EmployeeFinal in order to use modified gender values
        person.isInUSOnVisa isInUSOnVisa,
        person.isUSCitizen isUSCitizen,
        person.isHispanic isHispanic,
        person.isMultipleRaces isMultipleRaces,
	person.ethnicity ethnicity,
	job.isFaculty isFaculty,
	faculty.tenureStatus tenureStatus,
	faculty.nonTenureContractLength nonTenureContractLength,
	faculty.facultyRank facultyRank,
	job.position jobPosition,
	job.assignmentDescription jobDescription,
	position.positionDescription positionDescription,
	position.standardOccupationalCategory ESOC,
	position.skillClass skillClass,
	position.positionGroup positionGroup,
	position.positionClass positionClass,
	employee.primaryFunction primaryFunction,
	employee.employeeGroup employeeGroup,
	job.employeeClass employeeClass,
        position.federalEmploymentCategory ECIP, 
	job.annualSalary + nvl(secondJob.annualSalary,0) annualSalary,
	CASE WHEN employee.primaryFunction != 'None' 
				and employee.primaryFunction is not null THEN employee.primaryFunction
		 ELSE position.standardOccupationalCategory 
	END employeeFunction
from EmployeePerAsOfDate employee
	inner join PersonPerAsOfDate person on employee.personId = person.personId
	inner join PrimaryJobPerAsOfDate job on employee.personId = job.personId
	left join FacultyPerAsOfDate faculty on employee.personId = faculty.personId
	left join PositionPerAsOfDate position on job.position = position.position
	left join SecondaryJobSalary secondJob on employee.personId = secondJob.personId
),

/*****
BEGIN SECTION - Cohort Refactoring
The view below reformats and configures the base cohort fields based on the IPEDS survey specs
*****/

EmployeeFinal AS (
select EmployeeBase.personId personId,
	EmployeeBase.isCurrentEmployee currentEmployee, --1, null
	EmployeeBase.isNewHire newHire, --1, null
	EmployeeBase.fullOrPartTimeStatus fullPartInd, --Full Time, Part Time, Other
	CASE WHEN EmployeeBase.isIpedsMedicalOrDental = 1 THEN 1 ELSE 2 END isMedical, --1, null
    CASE WHEN EmployeeBase.gender = 'M' then 
--ak 20200423 (PF-1441) Modification to use 'isInUSOnVisa' and 'isUSCitizen' for ethnicity assignments
                        case when EmployeeBase.isUSCitizen = 1 then 
		                    (case when EmployeeBase.isHispanic = true then 2 -- 'hispanic/latino'
	    	                    when EmployeeBase.isMultipleRaces = true then 8 -- 'two or more races'
			                    when EmployeeBase.ethnicity != 'Unknown' and EmployeeBase.ethnicity is not null
				                then (case when EmployeeBase.ethnicity = 'Hispanic or Latino' then 2
                                        when EmployeeBase.ethnicity = 'American Indian or Alaskan Native' then 3
							            when EmployeeBase.ethnicity = 'Asian' then 4
							            when EmployeeBase.ethnicity = 'Black or African American' then 5
							            when EmployeeBase.ethnicity = 'Native Hawaiian or Other Pacific Islander' then 6
							            when EmployeeBase.ethnicity = 'Caucasian' then 7
							            else 9 end) 
			                        else 9 end) -- 'race and ethnicity unknown'
                                else (case when EmployeeBase.isInUSOnVisa = 1 then 1 -- 'nonresident alien'
                            else 9 end) -- 'race and ethnicity unknown'
		                end
            WHEN EmployeeBase.gender = 'F' then
--ak 20200423 (PF-1441) Modification to use 'isInUSOnVisa' and 'isUSCitizen' for ethnicity assignments
                        case when EmployeeBase.isUSCitizen = 1 then 
		                    (case when EmployeeBase.isHispanic = true then 11 -- 'hispanic/latino'
	    	                    when EmployeeBase.isMultipleRaces = true then 17 -- 'two or more races'
			                    when EmployeeBase.ethnicity != 'Unknown' and EmployeeBase.ethnicity is not null
				                then (case when EmployeeBase.ethnicity = 'Hispanic or Latino' then 11
                                        when EmployeeBase.ethnicity = 'American Indian or Alaskan Native' then 12
							            when EmployeeBase.ethnicity = 'Asian' then 13
							            when EmployeeBase.ethnicity = 'Black or African American' then 14
							            when EmployeeBase.ethnicity = 'Native Hawaiian or Other Pacific Islander' then 15
							            when EmployeeBase.ethnicity = 'Caucasian' then 16
							            else 18 end) 
			                        else 18 end) -- 'race and ethnicity unknown'
                                else (case when EmployeeBase.isInUSOnVisa = 1 then 10 -- 'nonresident alien'
                            else 18 end) -- 'race and ethnicity unknown'
		                end
        END reg,
	CASE WHEN EmployeeBase.gender = 'M' THEN 1 ELSE 2 END gender,
	EmployeeBase.employeeFunction employeeFunction,
	EmployeeBase.ESOC ESOC,
	EmployeeBase.isFaculty isFaculty,
	CASE WHEN EmployeeBase.employeeFunction in (
					'Instruction with Research/Public Service',
					'Instruction - Credit',
					'Instruction - Non-Credit',
					'Instruction - Combined Credit/Non-credit'
				) THEN 1
		ELSE null
	END isInstructional,
	EmployeeBase.tenureStatus tenureStatus,
	EmployeeBase.nonTenureContractLength nonTenureContractLength,
	CASE --WHEN EmployeeBase.tenureStatus = 'tenure' THEN 1
		 --WHEN EmployeeBase.tenureStatus = 'On tenure Track' THEN 2
		 WHEN EmployeeBase.nonTenureContractLength = 'Multi-year' THEN 3
		 WHEN EmployeeBase.nonTenureContractLength = 'Less than Annual' THEN 4
		 WHEN EmployeeBase.nonTenureContractLength = 'Annual' THEN 5
		 WHEN EmployeeBase.isFaculty is null THEN 6
		 WHEN EmployeeBase.nonTenureContractLength = 'Indefinite' THEN 7
		 ELSE 7 
	END tenure,
	CASE WHEN EmployeeBase.facultyRank = 'Professor' THEN 1
		WHEN EmployeeBase.facultyRank = 'Associate Professor' THEN 2
		WHEN EmployeeBase.facultyRank = 'Assistant Professor' THEN 3
		WHEN EmployeeBase.facultyRank = 'Instructor' THEN 4
		WHEN EmployeeBase.facultyRank = 'Lecturer' THEN 5
		WHEN EmployeeBase.facultyRank = 'No Academic Rank' THEN 6
		WHEN EmployeeBase.isFaculty is null THEN 7
		ELSE 6 
	END rankCode,
	EmployeeBase.annualSalary annualSalary,
--Occupational Category 1 - All Staff
	CASE WHEN EmployeeBase.isCurrentEmployee = 1 THEN
		CASE WHEN EmployeeBase.employeeFunction in ('Instruction with Research/Public Service', 'Instruction - Credit', 'Instruction - Non-Credit', 'Instruction - Combined Credit/Non-credit') THEN 1 -- Instruction
			WHEN EmployeeBase.employeeFunction = 'Research' THEN 2 --Research
			WHEN EmployeeBase.employeeFunction = 'Public Service' THEN 3 --Public Service
			WHEN EmployeeBase.employeeFunction = '25-4010' THEN 4
            WHEN EmployeeBase.employeeFunction = '25-4020' THEN 5
            WHEN EmployeeBase.employeeFunction = '25-4030' THEN 6
            WHEN EmployeeBase.employeeFunction in ('25-2000', '25-3000', '25-9000') THEN 7
            WHEN EmployeeBase.employeeFunction = '11-0000' THEN 8
            WHEN EmployeeBase.employeeFunction = '13-0000' THEN 9
            WHEN EmployeeBase.employeeFunction in ('15-0000', '17-0000', '19-0000') THEN 10
            WHEN EmployeeBase.employeeFunction in ('21-0000', '23-0000', '27-0000') THEN 11
            WHEN EmployeeBase.employeeFunction = '29-0000' THEN 12
            WHEN EmployeeBase.employeeFunction in ('31-0000', '33-0000', '35-0000', '37-0000', '39-0000') THEN 13
            WHEN EmployeeBase.employeeFunction = '41-0000' THEN 14
            WHEN EmployeeBase.employeeFunction = '43-0000' THEN 15
            WHEN EmployeeBase.employeeFunction in ('45-0000', '47-0000', '49-0000') THEN 16
            WHEN EmployeeBase.employeeFunction in ('51-0000', '53-0000') THEN 17
			ELSE null 
            END END occCat1,
--Occupational Category 2 - Full time salaries
	CASE WHEN EmployeeBase.fullOrPartTimeStatus = 'Full Time' and EmployeeBase.isCurrentEmployee = 1 THEN
		CASE WHEN EmployeeBase.employeeFunction = 'Research' THEN 1 --Research
			WHEN EmployeeBase.employeeFunction = 'Public Service' THEN 2 --Public Service
            WHEN EmployeeBase.employeeFunction in ('25-4010', '25-4020', '25-4030', '25-2000', '25-3000', '25-9000') THEN 3
            WHEN EmployeeBase.employeeFunction = '11-0000' THEN 4
            WHEN EmployeeBase.employeeFunction = '13-0000' THEN 5
            WHEN EmployeeBase.employeeFunction in ('15-0000', '17-0000', '19-0000') THEN 6
            WHEN EmployeeBase.employeeFunction in ('21-0000', '23-0000', '27-0000') THEN 7
            WHEN EmployeeBase.employeeFunction = '29-0000' THEN 8
            WHEN EmployeeBase.employeeFunction in ('31-0000', '33-0000', '35-0000', '37-0000', '39-0000') THEN 9
            WHEN EmployeeBase.employeeFunction = '41-0000' THEN 10
            WHEN EmployeeBase.employeeFunction = '43-0000' THEN 11
            WHEN EmployeeBase.employeeFunction in ('45-0000', '47-0000', '49-0000') THEN 12
            WHEN EmployeeBase.employeeFunction in ('51-0000', '53-0000') THEN 13
            ELSE null
            END ELSE null END occCat2,
--Occupational Category 3 - Part-time Staff
	CASE WHEN EmployeeBase.fullOrPartTimeStatus = 'Part Time' and EmployeeBase.isCurrentEmployee = 1 THEN
		CASE WHEN EmployeeBase.employeeFunction = 'Instruction - Credit' THEN 1 --'Credit'
			WHEN EmployeeBase.employeeFunction = 'Instruction - Non-Credit' THEN 2 --'NonCredit'
			WHEN EmployeeBase.employeeFunction = 'Instruction - Combined Credit/Non-credit' THEN 3 --'CombCredNonCred'
			WHEN EmployeeBase.employeeFunction = 'Instruction with Research/Public Service' THEN 4 --'IRP'
			WHEN EmployeeBase.employeeFunction = 'Research' THEN 5 --'R'
			WHEN EmployeeBase.employeeFunction = 'Public Service' THEN 6 --'PS'
			WHEN EmployeeBase.employeeFunction = '25-4010' THEN 7
			WHEN EmployeeBase.employeeFunction = '25-4020' THEN 8
			WHEN EmployeeBase.employeeFunction = '25-4030' THEN 9
			WHEN EmployeeBase.employeeFunction in ('25-2000', '25-3000', '25-9000') THEN 10
			WHEN EmployeeBase.employeeFunction = '11-0000' THEN 11
			WHEN EmployeeBase.employeeFunction = '13-0000' THEN 12
			WHEN EmployeeBase.employeeFunction in ('15-0000', '17-0000', '19-0000') THEN 13
			WHEN EmployeeBase.employeeFunction in ('21-0000', '23-0000', '27-0000') THEN 14
			WHEN EmployeeBase.employeeFunction = '29-0000' THEN 15
			WHEN EmployeeBase.employeeFunction in ('31-0000', '33-0000', '35-0000', '37-0000', '39-0000') THEN 16
			WHEN EmployeeBase.employeeFunction = '41-0000' THEN 17
			WHEN EmployeeBase.employeeFunction = '43-0000' THEN 18
			WHEN EmployeeBase.employeeFunction in ('45-0000', '47-0000', '49-0000') THEN 19
			WHEN EmployeeBase.employeeFunction in ('51-0000', '53-0000') THEN 20
			WHEN EmployeeBase.employeeFunction = 'Graduate Assistant - Teaching' THEN 22 -- Teaching Assistants, Postsecondary (25-9044) , '25-9000'
			WHEN EmployeeBase.employeeFunction = 'Graduate Assistant - Research' THEN 23 -- Graduate Assistant Research
			WHEN EmployeeBase.employeeFunction = 'Graduate Assistant - Other' THEN 24
            END ELSE null 
	END occCat3,
--Occupational category(4) - graduate assistants
	CASE 
		WHEN EmployeeBase.isCurrentEmployee = 1
			THEN CASE 
					WHEN EmployeeBase.employeeFunction = 'Graduate Assistant - Teaching' THEN 1 -- Teaching Assistants, Postsecondary (25-9044) , '25-9000'
					WHEN EmployeeBase.employeeFunction = 'Graduate Assistant - Research' THEN 2 -- Research
					WHEN EmployeeBase.employeeFunction = 'Graduate Assistant - Other' THEN 3 -- Other
				END
	END occCat4,
--Occupational category(5) - new hires
	CASE WHEN EmployeeBase.fullOrPartTimeStatus = 'Full Time' and EmployeeBase.isNewHire = 1 THEN
		CASE WHEN EmployeeBase.employeeFunction = 'Research' THEN 2 --Research
		WHEN EmployeeBase.employeeFunction = 'Public Service' THEN 3 --Public Service
			WHEN EmployeeBase.employeeFunction in ('25-4010', '25-4020', '25-4030', '25-2000', '25-3000', '25-9000') THEN 4
			WHEN EmployeeBase.employeeFunction = '11-0000' THEN 5
			WHEN EmployeeBase.employeeFunction = '13-0000' THEN 6
			WHEN EmployeeBase.employeeFunction in ('15-0000', '17-0000', '19-0000') THEN 7
			WHEN EmployeeBase.employeeFunction in ('21-0000', '23-0000', '27-0000') THEN 8
			WHEN EmployeeBase.employeeFunction = '29-0000' THEN 9
			WHEN EmployeeBase.employeeFunction in ('31-0000', '33-0000', '35-0000', '37-0000', '39-0000') THEN 10
			WHEN EmployeeBase.employeeFunction = '41-0000' THEN 11
			WHEN EmployeeBase.employeeFunction = '43-0000' THEN 12
			WHEN EmployeeBase.employeeFunction in ('45-0000', '47-0000', '49-0000') THEN 13
			WHEN EmployeeBase.employeeFunction in ('51-0000', '53-0000') THEN 14
			ELSE null 
		END ELSE null END occCat5,
	CASE WHEN EmployeeBase.ECIP = '12 Month Instructional' THEN 1 ELSE 0 END count12M,
	CASE WHEN EmployeeBase.ECIP = '11 Month Instructional' THEN 1 ELSE 0 END count11M,
	CASE WHEN EmployeeBase.ECIP = '10 Month Instructional' THEN 1 ELSE 0 END count10M,
	CASE WHEN EmployeeBase.ECIP = '9 Month Instructional' THEN 1 ELSE 0 END count9M,
	CASE WHEN EmployeeBase.ECIP = 'Other Full Time' THEN 1 ELSE 0 END countL9M,
	CASE WHEN EmployeeBase.ECIP = 'Unreported' THEN 1 ELSE 0 END countOther,
	CASE WHEN EmployeeBase.ECIP = '12 Month Instructional' THEN EmployeeBase.annualSalary ELSE 0 END sal12M,
	CASE WHEN EmployeeBase.ECIP = '11 Month Instructional' THEN EmployeeBase.annualSalary ELSE 0 END sal11M,
	CASE WHEN EmployeeBase.ECIP = '10 Month Instructional' THEN EmployeeBase.annualSalary ELSE 0 END sal10M,
	CASE WHEN EmployeeBase.ECIP = '9 Month Instructional' THEN EmployeeBase.annualSalary ELSE 0 END sal9M,
	CASE WHEN EmployeeBase.ECIP = 'Other Full Time' THEN EmployeeBase.annualSalary ELSE 0 END salL9M,
	CASE WHEN EmployeeBase.ECIP = 'Unreported' THEN EmployeeBase.annualSalary ELSE 0 END salOther
from EmployeeBase EmployeeBase
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
	select EmployeeFinal.tenure tenure,
		EmployeeFinal.rankCode rankCode,
		EmployeeFinal.reg reg,
		COUNT(EmployeeFinal.personId) totalCount
    from EmployeeFinal EmployeeFinal
    where EmployeeFinal.currentEmployee = 1
		and EmployeeFinal.isMedical = 2 -- Med = 1, NonMed = 2
		and EmployeeFinal.fullPartInd = 'Full Time'
		and EmployeeFinal.isInstructional = 1 
    group by EmployeeFinal.tenure,
		EmployeeFinal.rankCode,
        EmployeeFinal.reg
    
    union
    
    select tenure.ipedsTenure,
		rank.ipedsRank,
		reg.ipedsReg,
		0
    from TenureValues tenure
		cross join RankValues rank
		cross join RegValues reg
)
where rankCode < 7
            or (rankCode = 7 
                and tenure = 6)
group by tenure,
    	rankCode,
    	reg

union all

--A2
--Full-time instructional staff by tenure, medical school, and function

select 'A2', --part
	tenure,-- field1
	CASE WHEN isMedical = 2 THEN 0 ELSE isMedical END, -- Med = 1, NonMed = 2
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
    select EmployeeFinal.tenure tenure,
		EmployeeFinal.isMedical isMedical,
		CASE WHEN EmployeeFinal.employeeFunction = 'Instruction - Credit' THEN 1 --'Credit'
			WHEN EmployeeFinal.employeeFunction = 'Instruction - Non-Credit' THEN 2 --'NonCredit'
			WHEN EmployeeFinal.employeeFunction = 'Instruction - Combined Credit/Non-credit' THEN 3 --'CombCredNonCred'
			WHEN EmployeeFinal.employeeFunction = 'Instruction with Research/Public Service' THEN 5 --'IRP'
		END instrucFunction,
		COUNT(EmployeeFinal.personId) totalCount
	from EmployeeFinal EmployeeFinal
	where EmployeeFinal.currentEmployee = 1
		and EmployeeFinal.fullPartInd = 'Full Time'
		and EmployeeFinal.isInstructional = 1
	group by EmployeeFinal.tenure,
		EmployeeFinal.isMedical,
		EmployeeFinal.employeeFunction
    
	union
    
	select tenure.ipedsTenure,
		med.ipedsMed,
		instrFunc.ipedsInstrFunc,
		0
	from TenureValues tenure
		cross join MedValues med
		cross join InstrFuncValues instrFunc
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
	select EmployeeFinal.occCat1 occCat1,
		EmployeeFinal.reg reg,
		COUNT(EmployeeFinal.personId) totalCount
	from EmployeeFinal EmployeeFinal
	where EmployeeFinal.currentEmployee = 1
		and EmployeeFinal.fullPartInd = 'Full Time'
		and EmployeeFinal.isInstructional is null
        and EmployeeFinal.occCat1 > 1
    group by EmployeeFinal.occCat1,
		EmployeeFinal.reg
    
	union
    
    select occCat1.ipedsOccCat1,
		reg.ipedsReg,
		0
    from OccCat1Values occCat1
		cross join RegValues reg
    where occCat1.ipedsOccCat1 > 1
)
group by occCat1,
        reg

union all

--B2
--Full-time non-instructional staff by tenure, medical school, and occupational category

select 'B2', -- part
	tenure, -- field1
	CASE WHEN isMedical = 2 THEN 0 ELSE isMedical END, -- field2
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
	select EmployeeFinal.tenure tenure,
		EmployeeFinal.isMedical isMedical,
        EmployeeFinal.occCat1 occCat1,
        COUNT(EmployeeFinal.personId) totalCount
	from EmployeeFinal EmployeeFinal
	where EmployeeFinal.currentEmployee = 1
		and EmployeeFinal.fullPartInd = 'Full Time'
        and EmployeeFinal.isInstructional is null
        and EmployeeFinal.occCat1 between 2 and 12
	group by EmployeeFinal.tenure, 
		EmployeeFinal.isMedical,
		EmployeeFinal.occCat1

  union all

	select tenure.ipedsTenure,
		med.ipedsMed,
        occCat1.ipedsOccCat1,
        0
	from OccCat1Values occCat1
		cross join TenureValues tenure
		cross join MedValues med
	where occCat1.ipedsOccCat1 between 2 and 12
)
group by tenure, 
		isMedical,
		occCat1

union all

--B3
--Full-time non-instructional staff by medical school, and occupational category

select 'B3', -- part
	CASE WHEN isMedical = 2 THEN 0 ELSE isMedical END, -- field1
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
	select EmployeeFinal.occCat1 occCat1,
		EmployeeFinal.isMedical isMedical,
        COUNT(EmployeeFinal.personId) totalCount
	from EmployeeFinal EmployeeFinal
	where EmployeeFinal.currentEmployee = 1
		and EmployeeFinal.fullPartInd = 'Full Time'
		and EmployeeFinal.isInstructional is null
        and EmployeeFinal.occCat1 between 13 and 17
	group by EmployeeFinal.occCat1, EmployeeFinal.isMedical
	
	union

	select occCat1.ipedsOccCat1,
		med.ipedsMed,
        0
	from OccCat1Values occCat1
		cross join MedValues med
	where occCat1.ipedsOccCat1 between 13 and 17
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
	select EmployeeFinal.occCat1 occCat1,
		EmployeeFinal.reg reg,
        COUNT(EmployeeFinal.personId) totalCount
	from EmployeeFinal EmployeeFinal
	where EmployeeFinal.currentEmployee = 1
		and EmployeeFinal.fullPartInd = 'Part Time'
		and EmployeeFinal.occCat1 is not null
	group by EmployeeFinal.occCat1, 
			EmployeeFinal.reg

	union

	select occCat1.ipedsOccCat1,
		reg.ipedsReg,
        0
	from OccCat1Values occCat1
		cross join RegValues reg
)
group by occCat1, 
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
	select EmployeeFinal.occCat4 occCat4,
		EmployeeFinal.reg reg,
        COUNT(EmployeeFinal.personId) totalCount
    from EmployeeFinal EmployeeFinal
    where EmployeeFinal.currentEmployee = 1
        and EmployeeFinal.occCat4 in (1,2,3)
    group by EmployeeFinal.occCat4,
		EmployeeFinal.reg
    
    union
    
	select occCat4.ipedsOccCat4,
		reg.ipedsReg,
		0
	from OccCat4Values occCat4
		cross join RegValues reg
    where occCat4.ipedsOccCat4 in (1,2,3)
)
group by occCat4, 
		reg

union all

--D3
--Part-time staff by tenure, medical school, and occupational category

select 'D3', -- part
	tenure, -- field1
	CASE WHEN isMedical = 2 THEN 0 ELSE isMedical END, -- field2
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
	select EmployeeFinal.tenure,
		EmployeeFinal.isMedical isMedical,
		EmployeeFinal.occCat3 occCat3,
		COUNT(EmployeeFinal.personId) totalCount
	from EmployeeFinal EmployeeFinal
	where EmployeeFinal.currentEmployee = 1
		and EmployeeFinal.fullPartInd = 'Part Time'
		and EmployeeFinal.occCat3 < 16
	group by EmployeeFinal.tenure,
              EmployeeFinal.isMedical,
              EmployeeFinal.occCat3

    union
    
	select tenure.ipedsTenure,
		med.ipedsMed,
		occCat3.ipedsOccCat3,
		0
	from TenureValues tenure
		cross join MedValues med
		cross join OccCat3Values occCat3
	where occCat3.ipedsOccCat3 < 16
)
group by tenure,
	isMedical,
	occCat3

union all

--D4
--Part-time Non-instructional staff by medical school, and occupational category

select 'D4', -- part
	CASE WHEN isMedical = 2 THEN 0 ELSE isMedical END,-- field1
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
	select EmployeeFinal.isMedical isMedical,
		EmployeeFinal.occCat3 occCat3,
		COUNT(EmployeeFinal.personId) totalCount
	from EmployeeFinal EmployeeFinal
	where EmployeeFinal.currentEmployee = 1
		and EmployeeFinal.fullPartInd = 'Part Time'
		and EmployeeFinal.isInstructional is null
		and EmployeeFinal.occCat3 > 15
	group by EmployeeFinal.isMedical,
		EmployeeFinal.occCat3
	
	union
    
	select med.ipedsMed,
		occCat3.ipedsOccCat3,
		0
	from MedValues med
		cross join OccCat3Values occCat3
	where occCat3.ipedsOccCat3 > 15
)
group by isMedical,
        occCat3

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
	select CASE WHEN EmployeeFinal.rankCode = 7 THEN 6 ELSE EmployeeFinal.rankCode END rankCode,
		EmployeeFinal.gender gender,
		SUM(EmployeeFinal.count12M) count12M,
		SUM(EmployeeFinal.count11M) count11M,
		SUM(EmployeeFinal.count10M) count10M,
		SUM(EmployeeFinal.count9M) count9M,
		SUM(EmployeeFinal.countL9M) countL9M,
		SUM(EmployeeFinal.sal12M) sal12M,
		SUM(EmployeeFinal.sal11M) sal11M,
		SUM(EmployeeFinal.sal10M) sal10M,
		SUM(EmployeeFinal.sal9M) sal9M
    from EmployeeFinal EmployeeFinal
    where EmployeeFinal.currentEmployee = 1
		and EmployeeFinal.isMedical = 2
        and EmployeeFinal.fullPartInd = 'Full Time'
        and EmployeeFinal.isInstructional = 1
    group by EmployeeFinal.rankCode,
            EmployeeFinal.gender

	union

    select rank.ipedsRank,
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
	from RankValues rank
		cross join GenderValues gender
	where rank.ipedsRank < 7)
where rankCode is not null
group by rankCode,
        gender

union all

--G2
--Salaries of non-instructional staff

select 'G2', -- part
	occCat2, -- field1
	CAST(ROUND(SUM(annualSalary), 0) as BIGINT), -- field2
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
	select EmployeeFinal.occCat2 occCat2,
		SUM(EmployeeFinal.annualSalary) annualSalary
    from EmployeeFinal EmployeeFinal
    where EmployeeFinal.currentEmployee = 1
        and EmployeeFinal.isMedical = 2
        and EmployeeFinal.fullPartInd = 'Full Time'
        and EmployeeFinal.isInstructional  is null
        and EmployeeFinal.occCat2 is not null
    group by EmployeeFinal.occCat2
    
	union
    
	select occCat2.ipedsOccCat2,
		0
	from OccCat2Values occCat2
)
group by occCat2

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
    select EmployeeFinal.tenure tenure,
		EmployeeFinal.reg reg,
		COUNT(EmployeeFinal.personId) totalCount
	from EmployeeFinal EmployeeFinal
	where EmployeeFinal.newHire = 1
		and EmployeeFinal.fullPartInd = 'Full Time'
        and EmployeeFinal.isInstructional = 1
    group by EmployeeFinal.tenure,
            EmployeeFinal.reg
    
    union
    
    select tenure.ipedsTenure,
        reg.ipedsReg,
        0
    from TenureValues tenure
        cross join RegValues reg
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
	select EmployeeFinal.occCat5 occCat5,
		EmployeeFinal.reg reg,
        COUNT(EmployeeFinal.personId) totalCount
    from EmployeeFinal EmployeeFinal
    where EmployeeFinal.newHire = 1
        and EmployeeFinal.fullPartInd = 'Full Time'
    group by EmployeeFinal.occCat5,
            EmployeeFinal.reg
    
    union
    
    select occCat5.ipedsOccCat5,
        reg.ipedsReg,
        0
    from OccCat5Values occCat5
        cross join RegValues reg
    )
group by occCat5,
	reg
       
--order by 1, 2, 3, 4
