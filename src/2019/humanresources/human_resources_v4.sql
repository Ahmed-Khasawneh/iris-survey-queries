/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Spring Collection
FILE NAME:      Human Resources v4 (HR3)
FILE DESC:      Human Resources for non-degree-granting institutions and related administrative offices
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
20200702			akhasawneh			ak 20200702		Modify HR report query with standardized view naming/aliasing convention (PF-1533) (Run time 1m 1s) 
20200702            jhanicak                            Fixed OccCat case stmts (run time 1m 5s)
20200527            jhanicak                            Remove reference to IPEDSClientConfig.hrIncludeTenure field PF-1488
20200427			jhanicak			jh 20200428		Add default values and filter most current record queries on prior query PF-1318
20200424 	    	akhasawneh 			ak 20200424 	Adding dummy date considerations (PF-1375)
20200423            akhasawneh          ak 20200423     Modification to use 'isInUSOnVisa' and 'isUSCitizen' for ethnicity (PF-1441)
20200205            jhanicak            jh 20200205     Added default values for IPEDSReportingPeriod and IPEDSClientConfig
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
),

/*****
BEGIN SECTION - Cohort Creation
The view below pulls the base cohort based on IPEDS survey requirements and the data model definitions
*****/

CohortEMP AS (
select emp.personId personId,
	emp.asOfDate asOfDate,
--jh 20200205 moved the assignment of the isCurrentEmployee field to EmployeePerAsOfDate
	emp.isCurrentEmployee isCurrentEmployee,
	empassign.fullOrPartTimeStatus fullOrPartTimeStatus,
	case when person.gender = 'Male' then 'M'
		when person.gender = 'Female' then 'F'
--jh 20200428 added config values
		when person.gender = 'Non-Binary' then emp.genderForNonBinary
		else emp.genderForUnknown
	end gender,
--jh 20200428 added ethnicity fields and moved reg field to EmployeeFinal in order to use modified gender values
	person.isInUSOnVisa isInUSOnVisa,
	person.visaStartDate visaStartDate,
	person.visaEndDate visaEndDate,
	person.isUSCitizen isUSCitizen,
	person.isHispanic isHispanic,
	person.isMultipleRaces isMultipleRaces,
	person.ethnicity ethnicity,
	case when emp.primaryFunction != 'None' 
				and emp.primaryFunction is not null then emp.primaryFunction
		 else position.standardOccupationalCategory 
	end employeeFunction
--jh 20200205 - Removed ReportingDates view and changed emp to EmployeeMCR
from EmployeeMCR emp
	inner join PersonMCR person on emp.personId = person.personId
	inner join EmployeeAssignmentMCR empassign on emp.personId = empassign.personId
	left join EmployeePositionMCR position on empassign.position = position.position
),

/*****
BEGIN SECTION - Cohort Refactoring
The view below reformats and configures the base cohort fields based on the IPEDS survey specs
*****/

CohortRefactorEMP AS (
select cohortemp.personId personId,
	cohortemp.isCurrentEmployee currentEmployee, --1, null
	cohortemp.fullOrPartTimeStatus fullPartInd, --Full Time, Part Time, Other
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
                                else (case when cohortemp.isInUSOnVisa = 1 and cohortemp.asOfDate between cohortemp.visaStartDate and cohortemp.visaEndDate then 1 -- 'nonresident alien'
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
                                else (case when cohortemp.isInUSOnVisa = 1 and cohortemp.asOfDate between cohortemp.visaStartDate and cohortemp.visaEndDate then 10 -- 'nonresident alien'
                            else 18 end) -- 'race and ethnicity unknown'
		                end
        end reg,
--Occupational Category - All Staff
	case 
		when cohortemp.isCurrentEmployee = 1
			then case 
					when cohortemp.employeeFunction in (
								'Instruction with Research/Public Service',
								'Instruction - Credit',
								'Instruction - Non-credit',
								'Instruction - Combined Credit/Non-credit'
								) 
						then 1 -- Instruction
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
from CohortEMP cohortemp
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
group by occCat,
	reg

--order by 1, 2, 3, 4
