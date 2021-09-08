/********************

EVI PRODUCT:    DORIS 2021-22 IPEDS Survey Winter Collection
FILE NAME:      Student Financial Aid v5 (SFA)
FILE DESC:      Student Financial Aid for institutions with graduate students only
AUTHOR:         Ahmed Khasawneh
CREATED:        20210907

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Cohort Creation
Formatting Views
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)   Author             	Tag             	Comments
----------- 	--------------------	-------------   	-------------------------------------------------
20210907    	akhasawneh 									Initial version test 10s, prod 26s

One snapshot of each:
GI Bill - end of GI Bill reporting date
Department of Defense - end of DoD reporting date

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

--prod default block

select '2022' surveyYear, 
	'SFA' surveyId,
	'GI Bill' repPeriodTag4,
	'Department of Defense' repPeriodTag5,
	'Dept of Defense' repPeriodTag7, --temporary: old enum value
	CAST('2020-07-01' as DATE) giBillStartDate,
    CAST('2021-06-30' as DATE) giBillEndDate,
    CAST('2020-10-01' as DATE) dodStartDate,
    CAST('2021-09-30' as DATE) dodEndDate
/*
--testing default block
select '1415' surveyYear,  
	'SFA' surveyId,
	'GI Bill' repPeriodTag4,
	'Department of Defense' repPeriodTag5,
	'Dept of Defense' repPeriodTag7, --temporary: old enum value
    CAST('2013-07-01' as DATE) giBillStartDate,
    CAST('2014-06-30' as DATE) giBillEndDate,
    CAST('2013-10-01' as DATE) dodStartDate,
    CAST('2014-09-30' as DATE) dodEndDate
*/
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for all term codes and parts of term code for all snapshots. 

select termCode, 
	partOfTermCode, 
	financialAidYear,
	to_date(snapshotDate, 'YYYY-MM-DD') snapshotDate,
	to_date(startDate, 'YYYY-MM-DD') startDate,
	to_date(endDate, 'YYYY-MM-DD') endDate,
	academicYear,
	to_date(censusDate, 'YYYY-MM-DD') censusDate,
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
               coalesce(acadTermENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
        ) acadTermRn,
        acadTermENT.snapshotDate,
        acadTermENT.tags,
		coalesce(upper(acadtermENT.partOfTermCode), 1) partOfTermCode, 
		acadtermENT.recordActivityDate, 
		acadtermENT.termCodeDescription,       
		acadtermENT.partOfTermCodeDescription, 
		acadtermENT.startDate,
		acadtermENT.endDate,
		acadtermENT.academicYear,
		acadtermENT.financialAidYear,
		acadtermENT.censusDate,
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
    to_date(max(censusDate), 'YYYY-MM-DD') maxCensus,
    to_date(min(startDate), 'YYYY-MM-DD') minStart,
    to_date(max(endDate), 'YYYY-MM-DD') maxEnd,
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

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

MilitaryBenefitMCR as (
-- Returns GI Bill and Dept of Defense military benefits
-- do absolute value on amount or note in the ingestion query, since source records could be stored as debits or credits

--GI Bill
select personId, 
        benefitType, 
        termCode,
        sum(benefitAmount) benefitAmount,
        snapshotDate,
        startDate, 
        endDate,
        repPeriodTag4, --'GI Bill'
        repPeriodTag5, --'Department of Defense'
        repPeriodTag7 --'Dept of Defense'
from ( 
    select distinct MilitaryBenefitENT.personID personID,
        upper(MilitaryBenefitENT.termCode) termCode,
        MilitaryBenefitENT.benefitType benefitType,
        abs(MilitaryBenefitENT.benefitAmount) benefitAmount,
		to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		to_date(MilitaryBenefitENT.transactionDate, 'YYYY-MM-DD') transactionDate,
        MilitaryBenefitENT.tags tags,
        config.giBillStartDate startDate,
        config.giBillEndDate endDate,
        coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        config.repPeriodTag4 repPeriodTag4, --'GI Bill'
        config.repPeriodTag5 repPeriodTag5, --'Department of Defense'
        config.repPeriodTag7 repPeriodTag7, --'Dept of Defense'
        row_number() over (
            partition by
                MilitaryBenefitENT.personId,
			    MilitaryBenefitENT.benefitType,
			    MilitaryBenefitENT.termCode,
			    MilitaryBenefitENT.transactionDate,
			    MilitaryBenefitENT.benefitAmount
		    order by
				(case when array_contains(MilitaryBenefitENT.tags, config.repPeriodTag4) and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.giBillStartDate, 1) and date_add(config.giBillEndDate, 3) then 1 else 2 end) asc,
				(case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.giBillStartDate, 1) and date_add(config.giBillEndDate, 3) then 3 
				    else 4 end) asc,
                (case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') > config.giBillEndDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') < config.giBillStartDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                MilitaryBenefitENT.recordActivityDate desc
	    ) militarybenefitRn
    from MilitaryBenefit MilitaryBenefitENT
        cross join DefaultValues config
    where coalesce(MilitaryBenefitENT.isIpedsReportable, true) = true 
        and MilitaryBenefitENT.benefitType = config.repPeriodTag4
        and ((coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                and to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') between config.giBillStartDate and config.giBillEndDate
                and MilitaryBenefitENT.transactionDate between config.giBillStartDate and config.giBillEndDate)
            or (coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                and MilitaryBenefitENT.transactionDate between config.giBillStartDate and config.giBillEndDate))

    union

--Dept of Defense
    select distinct MilitaryBenefitENT.personID personID,
        upper(MilitaryBenefitENT.termCode) termCode,
        MilitaryBenefitENT.benefitType benefitType,
        abs(MilitaryBenefitENT.benefitAmount) benefitAmount,
		to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
		to_date(MilitaryBenefitENT.transactionDate, 'YYYY-MM-DD') transactionDate,
        MilitaryBenefitENT.tags tags,
        config.dodStartDate startDate,
        config.dodEndDate endDate,
        coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
        config.repPeriodTag4 repPeriodTag4, --'GI Bill'
        config.repPeriodTag5 repPeriodTag5, --'Department of Defense'
        config.repPeriodTag7 repPeriodTag7, --'Dept of Defense'
        row_number() over (
            partition by
                MilitaryBenefitENT.personId,
			    MilitaryBenefitENT.benefitType,
			    MilitaryBenefitENT.termCode,
			    MilitaryBenefitENT.transactionDate,
			    MilitaryBenefitENT.benefitAmount
		    order by
				(case when array_contains(MilitaryBenefitENT.tags, config.repPeriodTag5)
				            and to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.dodStartDate, 1) and date_add(config.dodEndDate, 3) then 1
				      else 2 end) asc,
				(case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') between date_sub(config.dodStartDate, 1) and date_add(config.dodEndDate, 3) then 3 
				    else 4 end) asc,
                (case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') > config.dodEndDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') < config.dodStartDate then to_date(MilitaryBenefitENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                MilitaryBenefitENT.recordActivityDate desc
	    ) militarybenefitRn
    from MilitaryBenefit MilitaryBenefitENT
        cross join DefaultValues config
    where coalesce(MilitaryBenefitENT.isIpedsReportable, true) = true 
        and MilitaryBenefitENT.benefitType in (config.repPeriodTag5, config.repPeriodTag7)
        and ((coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                and to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD') between config.dodStartDate and config.dodEndDate
                and MilitaryBenefitENT.transactionDate between config.dodStartDate and config.dodEndDate)
            or (coalesce(to_date(MilitaryBenefitENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                and MilitaryBenefitENT.transactionDate between config.dodStartDate and config.dodEndDate))
    )   
where militarybenefitRn = 1
group by personId, benefitType, termCode, snapshotDate, startDate, endDate, repPeriodTag4, repPeriodTag5, repPeriodTag7
),

MilitaryStuLevel as (
--Returns level of student receiving military benefit at time of reporting end date

select count(personId) recCount,
        sum(giCount) giCount,
        sum(giBillAmt) giBillAmt,
        sum(dodCount) dodCount,
        sum(dodAmt) dodAmt
from ( 
    select stu.personId personId,
            sum((case when stu.benefitType = stu.repPeriodTag4 then stu.benefitAmount else 0 end)) giBillAmt,
            sum((case when stu.benefitType in (stu.repPeriodTag5, stu.repPeriodTag7) then stu.benefitAmount else 0 end)) dodAmt,
            stu.studentLevel studentLevel,
            (case when stu.benefitType = stu.repPeriodTag4 then 1 else 0 end) giCount,
            (case when stu.benefitType in (stu.repPeriodTag5, stu.repPeriodTag7) then 1 else 0 end) dodCount
    from (
        select distinct miliben.personId personId,
                miliben.termCode termCode,
                (case when studentENT.studentLevel not in ('Undergraduate', 'Continuing Education', 'Other') then 2 else 1 end) studentLevel,
                miliben.benefitType,
                miliben.benefitAmount benefitAmount,
                miliben.repPeriodTag4,
                miliben.repPeriodTag5,
                miliben.repPeriodTag7,
                coalesce(row_number() over (
                    partition by
                        miliben.personId,
                        miliben.termCode,
                        miliben.benefitType,
                        miliben.benefitAmount
                    order by
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') = miliben.snapshotDate then 1 else 2 end) asc,
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') > miliben.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else miliben.snapshotDate end) asc,
                        (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') < miliben.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                        studentENT.recordActivityDate desc
                ), 1) studRn
        from MilitaryBenefitMCR miliben
            left join Student studentENT on miliben.personId = studentENT.personId
                and miliben.termCode = upper(studentENT.termCode)
                and ((coalesce(to_date(studentENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)  
                    and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= miliben.endDate)
                        or coalesce(to_date(studentENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
                and coalesce(studentENT.isIpedsReportable, true) = true
        ) stu
    where stu.studRn = 1 
    group by stu.personId, stu.studentLevel, stu.benefitType, stu.repPeriodTag4, stu.repPeriodTag5, stu.repPeriodTag7
    )
where studentLevel = 2
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

--Part G Section 2: Veteran's Benefits

--Valid values
--Student level: 2=Graduate, 3=Total (Total will be generated)
--Number of students: 0 to 999999, -2 or blank = not-applicable
--Total amount of aid: 0 to 999999999999, -2 or blank = not-applicable

select 'G' PART,
       max(FIELD2_1) FIELD2_1,
       max(FIELD3_1) FIELD3_1,
       round(max(FIELD4_1),0) FIELD4_1,
       max(FIELD5_1) FIELD5_1,
       round(max(FIELD6_1),0) FIELD6_1,
       null FIELD7_1,
       null FIELD8_1,
       null FIELD9_1,
       null FIELD10_1,
       null FIELD11_1,
       null FIELD12_1,
       null FIELD13_1,
       null FIELD14_1,
       null FIELD15_1,
       null FIELD16_1
from (
--if institution offers graduate level, count military benefits; if none, output 0 
    select 2 FIELD2_1, --Student Level 1=Undergraduate, 2=Graduate
       coalesce(mililevl.giCount, 0) FIELD3_1, --Post-9/11 GI Bill Benefits - Number of students receiving benefits/assistance
       coalesce(mililevl.giBillAmt, 0) FIELD4_1, --Post-9/11 GI Bill Benefits - Total dollar amount of benefits/assistance disbursed through the institution
       coalesce(mililevl.dodCount, 0) FIELD5_1, --Department of Defense Tuition Assistance Program - Number of students receiving benefits/assistance
       coalesce(mililevl.dodAmt, 0) FIELD6_1 --Department of Defense Tuition Assistance Program - Total dollar amount of benefits/assistance disbursed through the institution
    from MilitaryStuLevel mililevl
        
    union

--if no records exist in MilitaryBenefitMCR, output null values
    select 2,
        null,
        null,
        null,
        null
    ) 
