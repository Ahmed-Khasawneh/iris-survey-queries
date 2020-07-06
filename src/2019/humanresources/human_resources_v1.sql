/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Spring Collection
FILE NAME:      Human Resources v1 (HR1)
FILE DESC:      Human Resources for degree-granting institutions and related administrative offices that have 15 or more full-time staff and a tenure system
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
20200702			akhasawneh			ak 20200702		Modify HR report query with standardized view naming/aliasing convention (PF-1533) (Run time 3m 12s)
20200701            jhanicak                            Added new Faculty fields filtering and fixed OccCat case stmts PF-1536 (run time 3m 29s)
20200527            jhanicak                            Remove reference to IPEDSClientConfig.hrIncludeTenure field PF-1488
20200427			jhanicak			jh 20200428		Add default values and filter most current record queries on prior query PF-1318
20200424	    	akhasawneh			ak 20200424 	Adding dummy date considerations (PF-1375)
20200423            akhasawneh          ak 20200423     Modification to use 'isInUSOnVisa' and 'isUSCitizen' for ethnicity (PF-1441)
20200123            jhanicak            jh 20200123     Added default values for IPEDSReportingPeriod and IPEDSClientConfig
20200106            akhasawneh               			Move original code to template 
20200103            jhanicak                 			Initial version 

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
	CAST('2019-10-31' AS TIMESTAMP) newHireendDate,
	'N' hrIncludeSecondarySalary, --Y = Yes, N = No
	'M' genderForUnknown, --M = Male, F = Female
	'F' genderForNonBinary  --M = Male, F = Female

--Use for testing internally only
/*
select '1516' surveyYear, 
	'HR1' surveyId,
	CAST('2015-11-01' AS TIMESTAMP) asOfDate,
	CAST('2014-11-01' AS TIMESTAMP) newHireStartDate,
	CAST('2015-10-31' AS TIMESTAMP) newHireendDate,
	'N' hrIncludeSecondarySalary, --Y = Yes, N = No
	'M' genderForUnknown, --M = Male, F = Female
	'F' genderForNonBinary  --M = Male, F = Female
*/
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

--jh 20200428 rewrote and added default fields

select ConfigLatest.surveyYear surveyYear,
	ConfigLatest.hrIncludeSecondarySalary hrIncludeSecondarySalary,
	ConfigLatest.genderForUnknown genderForUnknown,
	ConfigLatest.genderForNonBinary genderForNonBinary,
	ConfigLatest.surveyId surveyId,
	ConfigLatest.asOfDate asOfDate,
	ConfigLatest.newHireStartDate newHireStartDate,
	ConfigLatest.newHireendDate newHireendDate
from (
	select clientconfigENT.surveyCollectionYear surveyYear,
		NVL(clientconfigENT.hrIncludeSecondarySalary, defvalues.hrIncludeSecondarySalary) hrIncludeSecondarySalary,
		NVL(clientconfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		NVL(clientconfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
		defvalues.surveyId surveyId,
		defvalues.asOfDate asOfDate,
		defvalues.newHireStartDate newHireStartDate,
		defvalues.newHireendDate newHireendDate,
		ROW_NUMBER() OVER (
			PARTITION BY
				clientconfigENT.surveyCollectionYear
			ORDER BY
				clientconfigENT.recordActivityDate DESC
		) configRn
	from IPEDSClientConfig clientconfigENT
		cross join DefaultValues defvalues
	where clientconfigENT.surveyCollectionYear = defvalues.surveyYear
		
	union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select defvalues.surveyYear surveyYear,
		defvalues.hrIncludeSecondarySalary hrIncludeSecondarySalary,
		defvalues.genderForUnknown genderForUnknown,
		defvalues.genderForNonBinary genderForNonBinary,
		defvalues.surveyId surveyId,
		defvalues.asOfDate asOfDate,
		defvalues.newHireStartDate newHireStartDate,
		defvalues.newHireendDate newHireendDate,
		1 configRn
	from DefaultValues defvalues
	where defvalues.surveyYear not in (select clientconfigENT.surveyCollectionYear
										from IPEDSClientConfig clientconfigENT
										where clientconfigENT.surveyCollectionYear = defvalues.surveyYear)
	) ConfigLatest
where ConfigLatest.configRn = 1
),

ReportingPeriodMCR as (
--Returns client specified reporting period for HR reporting and defaults to IPEDS specified date range if not otherwise defined 

--jh 20200428 rewrote and added Config fields

select surveyYear surveyYear,
	asOfDate asOfDate,
	newHireStartDate newHireStartDate,
	newHireendDate newHireendDate,
	hrIncludeSecondarySalary hrIncludeSecondarySalary,
	genderForUnknown genderForUnknown,
	genderForNonBinary genderForNonBinary
from (
	select repperiodENT.surveyCollectionYear surveyYear,
		NVL(repperiodENT.asOfDate, clientconfig.asOfDate) asOfDate,
		NVL(repperiodENT.reportingDateStart, clientconfig.newHireStartDate) newHireStartDate,
		NVL(repperiodENT.reportingDateend, clientconfig.newHireendDate) newHireendDate,
		clientconfig.hrIncludeSecondarySalary hrIncludeSecondarySalary,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary,
		ROW_NUMBER() OVER (
			PARTITION BY
				repperiodENT.surveyCollectionYear
			ORDER BY
				repperiodENT.recordActivityDate DESC
		) repperiodRn
	from ClientConfigMCR clientconfig
		cross join IPEDSReportingPeriod repperiodENT
	where repperiodENT.surveyCollectionYear = clientconfig.surveyYear
		and repperiodENT.surveyId like clientconfig.surveyId
	
	union
	
	select clientconfig.surveyYear surveyYear,
		clientconfig.asOfDate asOfDate,
		clientconfig.newHireStartDate newHireStartDate,
		clientconfig.newHireendDate newHireendDate,
		clientconfig.hrIncludeSecondarySalary hrIncludeSecondarySalary,
		clientconfig.genderForUnknown genderForUnknown,
		clientconfig.genderForNonBinary genderForNonBinary,
		1 repperiodRn
	from ClientConfigMCR clientconfig
		where clientconfig.surveyYear not in (select repperiodENT.surveyCollectionYear
											  from ClientConfigMCR clientconfig
												cross join IPEDSReportingPeriod repperiodENT
											  where repperiodENT.surveyCollectionYear = clientconfig.surveyYear
												and repperiodENT.surveyId like clientconfig.surveyId)
	)
where repperiodRn = 1
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

EmployeeMCR AS (
select *
from (
--jh 20200428 added ReportingPeriodMCR and Config fields
    select empENT.*,
		repperiod.asOfDate asOfDate,
		repperiod.hrIncludeSecondarySalary hrIncludeSecondarySalary,
		repperiod.genderForUnknown genderForUnknown,
		repperiod.genderForNonBinary genderForNonBinary,
	--jh 20200123 added isNewHire and isCurrentEmployee fields here in order to remove repperiod from EmployeeBase
			case when (empENT.hireDate >= repperiod.newHireStartDate
						and empENT.hireDate <= repperiod.newHireendDate)
						then 1 
				 else null 
			end isNewHire,
		case when ((empENT.terminationDate is null) 
					or (empENT.terminationDate > repperiod.asOfDate
					and empENT.hireDate <= repperiod.asOfDate)) then 1 
			 else null 
		end isCurrentEmployee,
		ROW_NUMBER() OVER (
			PARTITION BY
				empENT.personId
			ORDER BY
				empENT.recordActivityDate DESC
		) empRn
    from ReportingPeriodMCR repperiod
-- ak 20200424 Adding dummy date considerations (PF-1375).
		inner join Employee empENT 
			on ((empENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and empENT.recordActivityDate <= repperiod.asOfDate
                and empENT.employeeStatus = 'Active')
			        or empENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
    where ((empENT.terminationDate is null) -- all non-terminated employees
			or (empENT.hireDate BETWEEN repperiod.newHireStartDate 
				and repperiod.newHireendDate) -- new hires between Nov 1 and Oct 31
			or (empENT.terminationDate > repperiod.asOfDate
				and empENT.hireDate <= repperiod.asOfDate)) -- employees terminated after the as-of date
			and empENT.isIpedsReportable = 1
    )
where empRn = 1
),

EmployeeAssignmentMCR AS (
select *
from (
	select empassignENT.*,
		emp.asOfDate asOfDate,
        ROW_NUMBER() OVER (
			PARTITION BY
				empassignENT.personId,
				empassignENT.position,
				empassignENT.suffix
			ORDER BY
				empassignENT.recordActivityDate DESC
         ) jobRn
--jh 20200428 filter on records in EmployeeMCR and removed ReportingDates since needed fields have been added to EmployeeMCR
    from EmployeeMCR emp
-- ak 20200423 Adding dummy date considerations (PF-1375).
		inner join EmployeeAssignment empassignENT on emp.personId = empassignENT.personId
			and ((empassignENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and empassignENT.recordActivityDate <= emp.asOfDate
                and empassignENT.assignmentStatus = 'Active')
			        or empassignENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and empassignENT.assignmentStartDate <= emp.asOfDate
			and (empassignENT.assignmentendDate is null 
				or empassignENT.assignmentendDate >= emp.asOfDate)
			and empassignENT.isUndergradStudent = 0
			and empassignENT.isWorkStudy = 0
			and empassignENT.isTempOrSeasonal = 0
			and empassignENT.isIpedsReportable = 1
			and empassignENT.assignmentType = 'Primary'
  )
where jobRn = 1
),

PersonMCR AS (
select *
from (
    select personENT.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                personENT.personId
            ORDER BY
                personENT.recordActivityDate DESC
		) personRn
--jh 20200428 filter on records in EmployeeMCR and removed ReportingDates since needed fields have been added to EmployeeMCR
    from EmployeeMCR employee
-- ak 20200424 Adding dummy date considerations (PF-1375).
		inner join Person personENT on employee.personId = personENT.personId
			and ((personENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and personENT.recordActivityDate <= employee.asOfDate)
			        or personENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
    where personENT.isIpedsReportable = 1
    )
where personRn = 1
),

EmployeePositionMCR AS (
select *
from (
    select empposENT.*,
		ROW_NUMBER() OVER (
			PARTITION BY
				empposENT.position
			ORDER BY
				empposENT.startDate DESC,
				empposENT.recordActivityDate DESC
        ) empposRn
--jh 20200428 filter on records in EmployeeAssignmentMCR and removed ReportingDates since needed fields have been added to EmployeeAssignmentMCR
    from EmployeeAssignmentMCR empassign 
-- ak 20200424 Adding dummy date considerations (PF-1375).
		inner join EmployeePosition empposENT on empassign.position = empposENT.position
			and ((empposENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and empposENT.recordActivityDate <= empassign.asOfDate
                and empposENT.positionStatus = 'Active')
			        or empposENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and empposENT.startDate <= empassign.asOfDate
			and (empposENT.endDate is null 
				or empposENT.endDate >= empassign.asOfDate)
			and empposENT.isIpedsReportable = 1
	)
where empposRn = 1
),

EmployeeAssignmentMCR_SEC AS (
select personId personId,
	SUM(annualSalary) annualSalary
--jh 20200428 filter on records in EmployeeMCR and removed ReportingDates and ConfigPerAsOfDate since needed fields have been added to EmployeeMCR
from(
    select empassignENT.*,
		emp.asOfDate asOfDate,
		ROW_NUMBER() OVER (
			PARTITION BY
				empassignENT.personId,
				empassignENT.position,
				empassignENT.suffix
			ORDER BY
				empassignENT.recordActivityDate DESC
		 ) empassignRn
--jh 20200428 filter on records in EmployeeMCR and removed ReportingDates since needed fields have been added to EmployeeMCR
    from EmployeeMCR emp
-- ak 20200423 Adding dummy date considerations (PF-1375).
		inner join EmployeeAssignment empassignENT on emp.personId = empassignENT.personId
			and ((empassignENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and empassignENT.recordActivityDate <= emp.asOfDate
                and empassignENT.assignmentStatus = 'Active')
			        or empassignENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 
			and empassignENT.assignmentStartDate <= emp.asOfDate
			and (empassignENT.assignmentendDate is null 
				or empassignENT.assignmentendDate >= emp.asOfDate)
			and empassignENT.isUndergradStudent = 0
			and empassignENT.isWorkStudy = 0
			and empassignENT.isTempOrSeasonal = 0
			and empassignENT.isIpedsReportable = 1
			and empassignENT.assignmentType = 'Secondary'
  )
where empassignRn = 1
group by personId
),

FacultyMCR AS (
select *
from (
    select facultyENT.*,
		ROW_NUMBER() OVER (
			PARTITION BY
				facultyENT.personId,
				facultyENT.appointmentStartDate,
				facultyENT.facultyRankStartDate
            ORDER BY
				facultyENT.appointmentStartDate DESC,
				facultyENT.recordActivityDate DESC
          ) facultyRn
--jh 20200428 filter on records in EmployeeMCR and removed ReportingDates since needed fields have been added to EmployeeMCR
    from EmployeeMCR emp
-- ak 20200424 Adding dummy date considerations (PF-1375).
		left join Faculty facultyENT on emp.personId = facultyENT.personId
			and ((facultyENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP)
				and facultyENT.recordActivityDate <= emp.asOfDate)
			        or facultyENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP)) 			
			and ((facultyENT.appointmentDecisionDate is not null
					and facultyENT.appointmentDecision = 'Accepted'
                    and facultyENT.appointmentActionDate <= emp.asOfDate
					and (facultyENT.appointmentStartDate <= emp.asOfDate
					and (facultyENT.appointmentendDate is null 
						or facultyENT.appointmentendDate >= emp.asOfDate)))
					or (facultyENT.facultyRank is not null
                        and facultyENT.facultyRankStartDate <= emp.asOfDate
                        and facultyENT.facultyRankActionDate <= emp.asOfDate))
			and facultyENT.isIpedsReportable = 1
  )
where facultyRn = 1
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
),

/*****
BEGIN SECTION - Cohort Creation
The view below pulls the base cohort based on IPEDS survey requirements and the data model definitions
*****/

CohortEMP AS (
select emp.personId personId,
--jh 20200123 moved the assignment of the isNewHire and isCurrentEmployee fields to EmployeeMCR
	emp.isNewHire isNewHire,
	emp.asOfDate asOfDate,
	emp.isCurrentEmployee isCurrentEmployee,
	empassign.fullOrPartTimeStatus fullOrPartTimeStatus,
	emp.isIpedsMedicalOrDental isIpedsMedicalOrDental,
	case when person.gender = 'Male' then 'M'
		when person.gender = 'Female' then 'F'
--jh 20200428 added config values
		when person.gender = 'Non-Binary' then emp.genderForNonBinary
		else emp.genderForUnknown
	end gender,
--jh 20200428 added ethnicity fields and moved reg field to refactoremp in order to use modified gender values
	person.isInUSOnVisa isInUSOnVisa,
	person.visaStartDate visaStartDate,
	person.visaendDate visaendDate,
	person.isUSCitizen isUSCitizen,
	person.isHispanic isHispanic,
	person.isMultipleRaces isMultipleRaces,
	person.ethnicity ethnicity,
	empassign.isFaculty isFaculty,
	faculty.tenureStatus tenureStatus,
	faculty.nonTenureContractLength nonTenureContractLength,
	faculty.facultyRank facultyRank,
	empassign.position jobPosition,
	empassign.assignmentDescription jobDescription,
	position.positionDescription positionDescription,
	position.standardOccupationalCategory ESOC,
	position.skillClass skillClass,
	position.positionGroup positionGroup,
	position.positionClass positionClass,
	emp.primaryFunction primaryFunction,
	emp.employeeGroup employeeGroup,
	empassign.employeeClass employeeClass,
	position.federalEmploymentCategory ECIP, 
	empassign.annualSalary + NVL(empassignsec.annualSalary,0) annualSalary,
	case when emp.primaryFunction != 'None' 
				and emp.primaryFunction is not null then emp.primaryFunction
		 else position.standardOccupationalCategory 
	end employeeFunction
from EmployeeMCR emp
	inner join PersonMCR person on emp.personId = person.personId
	inner join EmployeeAssignmentMCR empassign on emp.personId = empassign.personId
	left join FacultyMCR faculty on emp.personId = faculty.personId
	left join EmployeePositionMCR position on empassign.position = position.position
	left join EmployeeAssignmentMCR_SEC empassignsec on emp.personId = empassignsec.personId
),

/*****
BEGIN SECTION - Cohort Refactoring
The view below reformats and configures the base cohort fields based on the IPEDS survey specs
*****/

CohortRefactorEMP AS (
select cohortemp.personId personId,
	cohortemp.isCurrentEmployee currentEmployee, --1, null
	cohortemp.isNewHire newHire, --1, null
	cohortemp.fullOrPartTimeStatus fullPartInd, --Full Time, Part Time, Other
	case when cohortemp.isIpedsMedicalOrDental = 1 then 1 else 2 end isMedical, --1, null
	case when cohortemp.gender = 'M' then 
--ak 20200423 (PF-1441) Modification to use 'isInUSOnVisa' and 'isUSCitizen' for ethnicity assignments
                        case when cohortemp.isUSCitizen = 1 then 
		                    (case when cohortemp.isHispanic = true then 2 -- 'hispanic/latino'
	    	                    when cohortemp.isMultipleRaces = true then 8 -- 'two or more races'
			                    when cohortemp.ethnicity != 'Unknown' and cohortemp.ethnicity is not null
				                then (case when cohortemp.ethnicity = 'Hispanic or Latino' then 2
                                        when cohortemp.ethnicity = 'American Indian or Alaskan Native' then 3
							            when cohortemp.ethnicity = 'Asian' then 4
							            when cohortemp.ethnicity = 'Black or African American' then 5
							            when cohortemp.ethnicity = 'Native Hawaiian or Other Pacific Islander' then 6
							            when cohortemp.ethnicity = 'Caucasian' then 7
							            else 9 end) 
			                        else 9 end) -- 'race and ethnicity unknown'
                                else (case when cohortemp.isInUSOnVisa = 1 and cohortemp.asOfDate BETWEEN cohortemp.visaStartDate and cohortemp.visaendDate then 1 -- 'nonresident alien'
                            else 9 end) -- 'race and ethnicity unknown'
		                end
            when cohortemp.gender = 'F' then
--ak 20200423 (PF-1441) Modification to use 'isInUSOnVisa' and 'isUSCitizen' for ethnicity assignments
                        case when cohortemp.isUSCitizen = 1 then 
		                    (case when cohortemp.isHispanic = true then 11 -- 'hispanic/latino'
	    	                    when cohortemp.isMultipleRaces = true then 17 -- 'two or more races'
			                    when cohortemp.ethnicity != 'Unknown' and cohortemp.ethnicity is not null
				                then (case when cohortemp.ethnicity = 'Hispanic or Latino' then 11
                                        when cohortemp.ethnicity = 'American Indian or Alaskan Native' then 12
							            when cohortemp.ethnicity = 'Asian' then 13
							            when cohortemp.ethnicity = 'Black or African American' then 14
							            when cohortemp.ethnicity = 'Native Hawaiian or Other Pacific Islander' then 15
							            when cohortemp.ethnicity = 'Caucasian' then 16
							            else 18 end) 
			                        else 18 end) -- 'race and ethnicity unknown'
                                else (case when cohortemp.isInUSOnVisa = 1 and cohortemp.asOfDate BETWEEN cohortemp.visaStartDate and cohortemp.visaendDate then 1 -- 'nonresident alien'
                            else 18 end) -- 'race and ethnicity unknown'
		                end
        end reg,
	case when cohortemp.gender = 'M' then 1 else 2 end gender,
	cohortemp.employeeFunction employeeFunction,
	cohortemp.ESOC ESOC,
	cohortemp.isFaculty isFaculty,
	case when cohortemp.employeeFunction in (
					'Instruction with Research/Public Service',
					'Instruction - Credit',
					'Instruction - Non-credit',
					'Instruction - Combined Credit/Non-credit'
				) then 1
		else null
	end isInstructional,
	cohortemp.tenureStatus tenureStatus,
	cohortemp.nonTenureContractLength nonTenureContractLength,
	case when cohortemp.tenureStatus = 'Tenured' then 1
		when cohortemp.tenureStatus = 'On Tenure Track' then 2
		when cohortemp.nonTenureContractLength = 'Multi-year' then 3
		when cohortemp.nonTenureContractLength = 'Less than Annual' then 4
		when cohortemp.nonTenureContractLength = 'Annual' then 5
		when cohortemp.isFaculty is null then 6
		when cohortemp.nonTenureContractLength = 'Indefinite' then 7
		else 7 
	end tenure,
	case when cohortemp.facultyRank = 'Professor' then 1
		when cohortemp.facultyRank = 'Associate Professor' then 2
		when cohortemp.facultyRank = 'Assistant Professor' then 3
		when cohortemp.facultyRank = 'Instructor' then 4
		when cohortemp.facultyRank = 'Lecturer' then 5
		when cohortemp.facultyRank = 'No Academic Rank' then 6
		when cohortemp.isFaculty is null then 7
		else 6 
	end rankCode,
	cohortemp.annualSalary annualSalary,
--Occupational Category 1 - All Staff
	case when cohortemp.isCurrentEmployee = 1 then
		case when cohortemp.employeeFunction in ('Instruction with Research/Public Service', 'Instruction - Credit', 'Instruction - Non-credit', 'Instruction - Combined Credit/Non-credit') then 1 -- Instruction
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
		end end occCat1,
--Occupational Category 2 - Full time salaries
	case when cohortemp.fullOrPartTimeStatus = 'Full Time' and cohortemp.isCurrentEmployee = 1 then
		case when cohortemp.employeeFunction = 'Research' then 1 --Research
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
            end else null end occCat2,
--Occupational Category 3 - Part-time Staff
	case when cohortemp.fullOrPartTimeStatus = 'Part Time' and cohortemp.isCurrentEmployee = 1 then
		case when cohortemp.employeeFunction = 'Instruction - Credit' then 1 --'Credit'
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
		end else null 
	end occCat3,
--Occupational category(4) - graduate assistants
	case 
		when cohortemp.isCurrentEmployee = 1
			then case 
					when cohortemp.employeeFunction = 'Graduate Assistant - Teaching' then 1 -- Teaching Assistants, Postsecondary (25-9044) , '25-9000'
					when cohortemp.employeeFunction = 'Graduate Assistant - Research' then 2 -- Research
					when cohortemp.employeeFunction = 'Graduate Assistant - Other' then 3 -- Other
				end
	end occCat4,
--Occupational category(5) - new hires
	case when cohortemp.fullOrPartTimeStatus = 'Full Time' and cohortemp.isNewHire = 1 then
		case when cohortemp.employeeFunction = 'Research' then 2 --Research
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
		end else null end occCat5,
	case when cohortemp.ECIP = '12 Month Instructional' then 1 else 0 end count12M,
	case when cohortemp.ECIP = '11 Month Instructional' then 1 else 0 end count11M,
	case when cohortemp.ECIP = '10 Month Instructional' then 1 else 0 end count10M,
	case when cohortemp.ECIP = '9 Month Instructional' then 1 else 0 end count9M,
	case when cohortemp.ECIP = 'Other Full Time' then 1 else 0 end countL9M,
	case when cohortemp.ECIP = 'Unreported' then 1 else 0 end countOther,
	case when cohortemp.ECIP = '12 Month Instructional' then cohortemp.annualSalary else 0 end sal12M,
	case when cohortemp.ECIP = '11 Month Instructional' then cohortemp.annualSalary else 0 end sal11M,
	case when cohortemp.ECIP = '10 Month Instructional' then cohortemp.annualSalary else 0 end sal10M,
	case when cohortemp.ECIP = '9 Month Instructional' then cohortemp.annualSalary else 0 end sal9M,
	case when cohortemp.ECIP = 'Other Full Time' then cohortemp.annualSalary else 0 end salL9M,
	case when cohortemp.ECIP = 'Unreported' then cohortemp.annualSalary else 0 end salOther
from CohortEMP cohortemp
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
        and refactoremp.occCat4 in (1,2,3)
    group by refactoremp.occCat4,
            refactoremp.reg
    
    union
    
    select occcat4.ipedsOccCat4,
           raceethngender.ipedsReg,
           0
    from OccupationalCat4FMT occcat4
       cross join RaceEthnicityGenderFMT raceethngender
    where occcat4.ipedsOccCat4 in (1,2,3)
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
	select refactoremp.occCat2 occCat2,
		SUM(refactoremp.annualSalary) annualSalary
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
     
--ORDER BY 1, 2, 3, 4
