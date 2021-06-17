%sql

WITH CohortData as (
select *
from (
	VALUES
        ('120',1,1,0,0,0,0,0,0,0),
        ('2016',2,2,0,0,1,0,0,0,0),
        ('2946',2,1,1,0,0,0,3,3,3),
        ('3270',2,2,0,0,0,1,0,0,0),
        ('30081',3,1,1,0,0,0,3,3,3),
        ('30362',2,2,0,0,0,1,0,0,0),
        ('30553',1,1,1,0,0,0,3,3,3),
        ('31072',3,2,1,0,0,0,2,3,3),
        ('31222',4,1,0,0,0,0,0,0,0),
        ('31228',4,2,0,0,0,1,0,0,0),
        ('31230',1,1,0,1,0,0,0,0,0),
        ('31237',2,2,0,0,0,1,0,0,0),
        ('31246',2,1,1,0,0,0,2,2,3),
        ('31248',4,2,1,0,0,0,0,1,2),
        ('31249',3,1,1,0,0,0,0,0,1),
        ('31253',3,2,1,0,0,0,2,2,2),
        ('31254',2,2,0,1,0,0,0,0,0),
        ('31271',2,1,0,0,0,0,0,0,0),
        ('31331',4,2,0,0,1,0,0,0,0),
        ('31339',1,1,0,0,0,0,0,0,0),
        ('31351',2,2,0,0,1,0,0,0,0), --('31351',1,2,0,0,1,0,0,0,0),
        ('31376',1,1,1,0,0,0,2,2,2),
        ('31394',2,2,0,0,0,1,0,0,0),
        ('31421',3,1,1,0,0,0,1,2,3),
        ('31435',2,2,1,0,0,0,1,2,2),
        ('31436',2,1,1,0,0,0,1,1,1),
        ('31441',3,2,0,1,0,0,0,0,0),
        ('31476',2,1,0,1,0,0,0,0,0),
        ('31477',4,2,0,1,0,0,0,0,0),
        ('31555',4,2,1,0,0,0,0,0,3),
        ('31619',4,1,0,0,1,0,0,0,0),
        ('31620',4,2,1,0,0,0,1,1,1),
        ('31621',1,1,1,0,0,0,2,2,2),
        ('31637',3,2,0,0,0,0,0,0,0), --('31637',1,2,0,0,0,0,0,0,0),
        ('31640',1,1,0,1,0,0,0,0,0),
        ('31837',2,2,1,0,0,0,2,2,3),
        ('31862',3,1,1,0,0,0,3,3,3),
        ('31870',4,2,0,0,0,1,0,0,0),
        ('31889',3,1,0,1,0,0,0,0,0),
        ('31910',1,1,0,1,0,0,0,0,0),
        ('31922',3,2,1,0,0,0,0,1,2),
        ('32136',2,1,1,0,0,0,0,0,3),
        ('32186',4,2,0,0,1,0,0,0,0), --('32186',1,2,0,0,1,0,0,0,0),
        ('32352',1,1,1,0,0,0,1,1,2),
        ('32363',2,2,1,0,0,0,1,1,1),
        ('32364',4,1,0,0,0,1,0,0,0),
        ('32388',4,2,0,0,1,0,0,0,0),
        ('32402',4,1,0,1,0,0,0,0,0),
        ('32414',2,1,0,0,0,1,0,0,0)
    ) as CohortData (personId,cohortType,recipientType,awardInd,transferInd,exclusionInd,enrolledInd,fourYrMax,sixYrMax,eightYrMax)
),

Cohort as (
select *
from CohortData
),

FormatPartValues as (
select *
from (
	VALUES
		('A'), -- Full-time, first-time entering degree/certificate-seeking undergraduate (FTFT)
		('B'), -- Part-time, first-time entering degree/certificate-seeking undergraduate (FTPT) 
		('C'), -- Full-time, non-first-time entering degree/certificate-seeking undergraduate (NFTFT)
		('D') -- Part-time, non-first-time entering degree/certificate-seeking undergraduate (NFTPT)
	) as parts (surveyParts)
),

FormatPartA as (
select *
from (
	VALUES
		(1), -- Full-time, first-time entering degree/certificate-seeking undergraduate (FTFT)
		(2), -- Part-time, first-time entering degree/certificate-seeking undergraduate (FTPT) 
		(3), -- Full-time, non-first-time entering degree/certificate-seeking undergraduate (NFTFT)
		(4) -- Part-time, non-first-time entering degree/certificate-seeking undergraduate (NFTPT)
	) as studentCohort (cohortType)
),

FormatPartA2 as (
select *
from (
	VALUES
		(1), -- Pell Grant recipients
		(2) -- Non-Pell Grant recipients
	) as studentRecip (recipientType)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

-- Part A: Establishing Cohorts

select surveyParts part,
       cohortType field1, 
        recipientType field2,
        (case when surveyParts = 'A' then sum(cohortCount)
                when surveyParts = 'B' then sum(fourYr1)
                when surveyParts = 'C' then sum(sixYr1)
                when surveyParts = 'D' then sum(eightYr1) end) field3,
        (case when surveyParts = 'A' then sum(exclusionCount)
                when surveyParts = 'B' then sum(fourYr2)
                when surveyParts = 'C' then sum(sixYr2)
                when surveyParts = 'D' then sum(eightYr2) end) field4,
        (case when surveyParts = 'A' then null
                when surveyParts = 'B' then sum(fourYr3)
                when surveyParts = 'C' then sum(sixYr3)
                when surveyParts = 'D' then sum(eightYr3) end) field5,
        (case when surveyParts = 'A' then null
                when surveyParts = 'B' then null
                when surveyParts = 'C' then null
                when surveyParts = 'D' then sum(enrolledCount) end) field6,
        (case when surveyParts = 'A' then null
                when surveyParts = 'B' then null
                when surveyParts = 'C' then null
                when surveyParts = 'D' then sum(transferCount) end) field7
from (
    select *
    from (
        select studentCohort.cohortType cohortType,
                --cohort.cohortType cohortType,  --Cohort type 1 - 4
                studentRecip.recipientType recipientType,
               --cohort.recipientType recipientType,  --Recipient type 1-2
               coalesce(sum(cohortCount), 0) cohortCount,
               coalesce(sum(fourYr1), 0) fourYr1,
               coalesce(sum(fourYr2), 0) fourYr2,
               coalesce(sum(fourYr3), 0) fourYr3,
               coalesce(sum(sixYr1), 0) sixYr1,
               coalesce(sum(sixYr2), 0) sixYr2,
               coalesce(sum(sixYr3), 0) sixYr3,
               coalesce(sum(eightYr1), 0) eightYr1,
               coalesce(sum(eightYr2), 0) eightYr2,
               coalesce(sum(eightYr3), 0) eightYr3,
               coalesce(sum(enrolledInd), 0) enrolledCount, 
               coalesce(sum(transferInd), 0) transferCount,
               coalesce(sum(exclusionInd), 0) exclusionCount
        from FormatPartA studentCohort
             cross join FormatPartA2 studentRecip
            left join (
                select personId personId,
                        cohortType cohortType,  --Cohort type 1 - 4
                       recipientType recipientType,  --Recipient type 1-2
                       1 cohortCount,
                       (case when fourYrMax = 1 then 1 else 0 end) fourYr1,
                       (case when fourYrMax = 2 then 1 else 0 end) fourYr2,
                       (case when fourYrMax = 3 then 1 else 0 end) fourYr3,
                       (case when sixYrMax = 1 then 1 else 0 end) sixYr1,
                       (case when sixYrMax = 2 then 1 else 0 end) sixYr2,
                       (case when sixYrMax = 3 then 1 else 0 end) sixYr3,
                       (case when eightYrMax = 1 then 1 else 0 end) eightYr1,
                       (case when eightYrMax = 2 then 1 else 0 end) eightYr2,
                       (case when eightYrMax = 3 then 1 else 0 end) eightYr3,
                       enrolledInd enrolledInd, 
                       transferInd transferInd,
                       exclusionInd exclusionInd
                from Cohort 
                ) cohort on studentCohort.cohortType = cohort.cohortType
                    and studentRecip.recipientType = cohort.recipientType
                -- cross join FormatPartA2 studentRecip
                group by studentCohort.cohortType,
                        --cohort.cohortType,
                        studentRecip.recipientType
                       --cohort.recipientType
            )
       cross join FormatPartValues partval
    )   group by surveyParts,
       cohortType, 
       recipientType
