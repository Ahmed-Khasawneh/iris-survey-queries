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
--jh 20200205 added isNewHire and isCurrentEmployee fields here in order to remove ReportingDates from EmployeeBase
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

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/

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

OccCatValues AS (
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

EmployeeBase AS (
select employee.personId personId,
--jh 20200205 moved the assignment of the isCurrentEmployee field to EmployeePerAsOfDate
       	employee.isCurrentEmployee isCurrentEmployee,
		job.fullOrPartTimeStatus fullOrPartTimeStatus,
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
	CASE WHEN employee.primaryFunction != 'None' 
					and employee.primaryFunction is not null THEN employee.primaryFunction
             ELSE position.standardOccupationalCategory 
		END employeeFunction
--jh 20200205 - Removed ReportingDates view and changed Employee to EmployeePerAsOfDate
from EmployeePerAsOfDate employee
	inner join PersonPerAsOfDate person on employee.personId = person.personId
	inner join PrimaryJobPerAsOfDate job on employee.personId = job.personId
	left join PositionPerAsOfDate position on job.position = position.position
),

/*****
BEGIN SECTION - Cohort Refactoring
The view below reformats and configures the base cohort fields based on the IPEDS survey specs
*****/

EmployeeFinal AS (
select EmployeeBase.personId personId,
	EmployeeBase.isCurrentEmployee currentEmployee, --1, null
	EmployeeBase.fullOrPartTimeStatus fullPartInd, --Full Time, Part Time, Other
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
--Occupational Category - All Staff
	CASE 
		WHEN EmployeeBase.isCurrentEmployee = 1
			THEN CASE 
					WHEN EmployeeBase.employeeFunction in (
								'Instruction with Research/Public Service',
								'Instruction - Credit',
								'Instruction - Non-Credit',
								'Instruction - Combined Credit/Non-credit'
								) 
						THEN 1 -- Instruction
					WHEN EmployeeBase.employeeFunction = '25-4000' THEN 2
					WHEN EmployeeBase.employeeFunction in ('25-2000', '25-3000', '25-9000') THEN 3
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
				END
	END occCat
from EmployeeBase EmployeeBase
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
	select EmployeeFinal.occCat occCat,
		EmployeeFinal.reg reg,
		COUNT(EmployeeFinal.personId) totalCount
    from EmployeeFinal EmployeeFinal
	where EmployeeFinal.currentEmployee = 1
		and EmployeeFinal.fullPartInd = 'Full Time'
		and EmployeeFinal.occCat is not null
	group by EmployeeFinal.occCat,
		EmployeeFinal.reg
	
	union
	
	select occCat.ipedsOccCat,
		reg.ipedsReg,
		0
	from OccCatValues occCat
	cross join RegValues reg
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
	select EmployeeFinal.occCat occCat,
		EmployeeFinal.reg reg,
		COUNT(EmployeeFinal.personId) totalCount
    from EmployeeFinal EmployeeFinal
    where EmployeeFinal.currentEmployee = 1
        and EmployeeFinal.fullPartInd = 'Part Time'
		and EmployeeFinal.occCat is not null
	group by EmployeeFinal.occCat,
            EmployeeFinal.reg
	
	union
	
	select occCat.ipedsOccCat,
		reg.ipedsReg,
		0
	from OccCatValues occCat
	cross join RegValues reg
	)
group by occCat,
	reg

--order by 1, 2, 3, 4