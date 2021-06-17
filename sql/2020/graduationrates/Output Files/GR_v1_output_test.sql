%sql

WITH Cohort as (
select *
from (
	VALUES
        ('120',2,0,0,'F',7,null),
        ('2016',2,0,0,'F',7,19),
        ('2946',2,0,0,'M',7,null),
        ('3270',2,0,0,'F',7,11),
        ('30081',2,1,0,'F',7,12),
        ('30362',2,0,0,'M',2,null),
        ('30553',2,0,0,'F',7,19),
        ('31072',3,0,0,'M',9,18),
        ('31222',3,0,0,'F',2,11),
        ('31228',3,0,0,'M',9,12),
        ('31230',3,0,1,'M',3,null),
        ('31237',2,0,0,'M',3,19),
        ('31246',2,1,0,'M',7,30),
        ('31248',2,1,0,'F',9,20),
        ('31249',3,1,0,'F',2,null),
        ('31253',3,1,0,'F',7,18),
        ('31254',2,0,0,'M',3,null),
        ('31271',2,0,0,'M',5,null),
        ('31331',3,0,0,'M',7,51),
        ('31339',2,0,0,'M',7,null),
        ('31351',3,0,0,'M',7,18),
        ('31376',3,1,0,'F',9,null),
        ('31394',2,0,0,'M',7,30),
        ('31421',3,1,0,'F',9,45),
        ('31435',2,1,0,'F',3,19),
        ('31436',2,1,0,'F',5,19),
        ('31441',3,0,1,'F',3,12),
        ('31476',2,0,1,'M',7,null),
        ('31477',2,0,1,'M',7,30),
        ('31555',2,0,0,'M',5,19),
        ('31619',2,0,0,'M',5,null),
        ('31620',2,0,0,'M',5,20),
        ('31621',2,0,0,'M',5,null),
        ('31637',2,0,0,'F',7,19),
        ('31640',2,0,1,'M',5,11),
        ('31837',2,1,0,'M',5,11),
        ('31862',3,1,0,'M',9,30),
        ('31870',3,0,0,'M',2,30),
        ('31889',3,0,1,'F',9,12),
        ('31910',3,0,1,'F',9,11),
        ('31922',3,1,0,'M',9,null),
        ('32136',2,1,0,'F',2,18),
        ('32186',2,0,0,'M',5,30),
        ('32352',2,1,0,'M',5,null),
        ('32363',2,0,0,'F',9,11),
        ('32364',2,0,0,'F',7,51),
        ('32388',2,0,0,'M',7,null),
        ('32402',2,0,1,'M',7,19),
        ('32414',2,0,0,'M',7,null)
    ) as Cohort (personId,subCohort,isPellRec,isSubLoanRec,ipedsGender,ipedsEthnicity,latestStatus)
),

CohortStatus as (

select personId personId,
        subCohort subCohort, --2 bach-seeking, 3 other-seeking
		isPellRec isPellRec,
		isSubLoanRec isSubLoanRec,
        ipedsGender ipedsGender,
		ipedsEthnicity ipedsEthnicity,
        latestStatus latestStatus,
        (case when ipedsEthnicity ='1' and ipedsGender = 'M' then 1 else 0 end) field1, --Nonresident Alien
        (case when ipedsEthnicity ='1' and ipedsGender = 'F' then 1 else 0 end) field2,
        (case when ipedsEthnicity ='2' and ipedsGender = 'M' then 1 else 0 end) field3, -- Hispanic/Latino
        (case when ipedsEthnicity ='2' and ipedsGender = 'F' then 1 else 0 end) field4,
        (case when ipedsEthnicity ='3' and ipedsGender = 'M' then 1 else 0 end) field5, -- American Indian or Alaska Native
        (case when ipedsEthnicity ='3' and ipedsGender = 'F' then 1 else 0 end) field6,
        (case when ipedsEthnicity ='4' and ipedsGender = 'M' then 1 else 0 end) field7, -- Asian
        (case when ipedsEthnicity ='4' and ipedsGender = 'F' then 1 else 0 end) field8,
        (case when ipedsEthnicity ='5' and ipedsGender = 'M' then 1 else 0 end) field9, -- Black or African American
        (case when ipedsEthnicity ='5' and ipedsGender = 'F' then 1 else 0 end) field10,
        (case when ipedsEthnicity ='6' and ipedsGender = 'M' then 1 else 0 end) field11, -- Native Hawaiian or Other Pacific Islander
        (case when ipedsEthnicity ='6' and ipedsGender = 'F' then 1 else 0 end) field12,
        (case when ipedsEthnicity ='7' and ipedsGender = 'M' then 1 else 0 end) field13, -- White
        (case when ipedsEthnicity ='7' and ipedsGender = 'F' then 1 else 0 end) field14,
        (case when ipedsEthnicity ='8' and ipedsGender = 'M' then 1 else 0 end) field15, -- Two or more races
        (case when ipedsEthnicity ='8' and ipedsGender = 'F' then 1 else 0 end) field16,
        (case when ipedsEthnicity ='9' and ipedsGender = 'M' then 1 else 0 end) field17, -- Race and ethnicity unknown
        (case when ipedsEthnicity ='9' and ipedsGender = 'F' then 1 else 0 end) field18,
        (case when latestStatus = 11 then 1 else 0 end) stat11, --Completers of programs of less than 2 years within 150% of normal time --all
        (case when latestStatus = 12 then 1 else 0 end) stat12, --Completers of programs of at least 2 but less than 4 years within 150% of normal time --all
        (case when latestStatus = 18 or latestStatus = 19 or latestStatus = 20 then 1 else 0 end) stat18, --Completed bachelor's degree or equivalent within 150% --all
        (case when latestStatus = 19 then 1 else 0 end) stat19, --Completers of bachelor's or equivalent degree programs in 4 years or less --subcohort 2
        (case when latestStatus = 20 then 1 else 0 end) stat20, --Completers of bachelor's or equivalent degree programs in 5 years --subcohort 2
        (case when latestStatus = 11
                or latestStatus = 12
                or latestStatus = 18
                or latestStatus = 19
                or latestStatus = 20 then 1 else 0 end) stat29, --Total completers within 150% --11, 12, 18
        (case when latestStatus = 30 then 1 else 0 end) stat30, --Total transfer-out students (non-completers)
        (case when latestStatus = 45 then 1 else 0 end) stat45, --Total exclusions  
        (case when latestStatus = 51 then 1 else 0 end) stat51 --Still enrolled
from Cohort
),

estabSubCohortPartB1 as (
select *
from (
    VALUES
--Establishing cohort table (section 1)
        (1), -- Revised cohort of all full-time, first-time degree or certificate seeking undergraduates, cohort year 2014
        (2) -- Subcohort of full-time, first-time students seeking a bachelor's or equivalent degree, cohort year 2014
        --(3)  -- Subcohort of full-time, first-time students seeking other than a bachelor's degree, cohort year 2014. Do not include in import file, will be generated.
    ) as PartBSection1 (subcoh)
),

estabSubCohort as (
select *
from (
    VALUES
--Establishing cohort table (section 1)
        --(1), -- Revised cohort of all full-time, first-time degree or certificate seeking undergraduates, cohort year 2014
        (2), -- Subcohort of full-time, first-time students seeking a bachelor's or equivalent degree, cohort year 2014
        (3)  -- Subcohort of full-time, first-time students seeking other than a bachelor's degree, cohort year 2014. Do not include in import file, will be generated.
    ) as estSubCohort (subcoh)
),

FormatPartB as (
select *
from (
	VALUES
		(11), -- 11 - Completers of programs of less than 2 years within 150% of normal time --all
		(12), -- 12 - Completers of programs of at least 2 but less than 4 years within 150% of normal time --all
		(18), -- 18 - Completers of bachelor's or equivalent degree programs within 150% of normal time --all
		(19), -- 19 - Completers of bachelor's or equivalent degree programs in 4 years or less --subcohort 2
		(20), -- 20 - Completers of bachelor's or equivalent degree programs in 5 years --subcohort 2
		(30), -- 30 - Total transfer-out students (non-completers)
		(45), -- 45 - Total exclusions 
		(51)  -- 51 - Still enrolled 
	) as PartB (cohortStatus)
),

FormatPartC as (
select *
from (
	VALUES
		(10), -- 10 - Number of students in cohort
		(18), -- 18 - Completed bachelor's degree or equivalent within 150%
		(29), -- 29 - Total completers within 150%
		(45)  -- 45 - Total exclusions
	) as PartC (cohortStatus)
)

select 'B' part,
        1 section, 
        subCohortPB1 line, --subcohort
        (case when subCohortPB1 = 1 then field1
              else field1_2 end) field1,
        (case when subCohortPB1 = 1 then field2
              else field2_2 end) field2,
        (case when subCohortPB1 = 1 then field3
              else field3_2 end) field3,
        (case when subCohortPB1 = 1 then field4
              else field4_2 end) field4,
        (case when subCohortPB1 = 1 then field5
              else field5_2 end) field5,
        (case when subCohortPB1 = 1 then field6
              else field6_2 end) field6,
        (case when subCohortPB1 = 1 then field7
              else field7_2 end) field7,
        (case when subCohortPB1 = 1 then field8
              else field8_2 end) field8,
        (case when subCohortPB1 = 1 then field9
              else field9_2 end) field9,
        (case when subCohortPB1 = 1 then field10
              else field10_2 end) field10,
        (case when subCohortPB1 = 1 then field11
              else field11_2 end) field11,
        (case when subCohortPB1 = 1 then field12
              else field12_2 end) field12,
        (case when subCohortPB1 = 1 then field13
              else field13_2 end) field13,
        (case when subCohortPB1 = 1 then field14
              else field14_2 end) field14,
        (case when subCohortPB1 = 1 then field15
              else field15_2 end) field15,
        (case when subCohortPB1 = 1 then field16
              else field16_2 end) field16,
        (case when subCohortPB1 = 1 then field17
              else field17_2 end) field17,
        (case when subCohortPB1 = 1 then field18
              else field18_2 end) field18
from ( 
    select pb1.subcoh subCohortPB1,
            sum(field1) field1, 
            sum(field1_2) field1_2,
            sum(field2) field2, 
            sum(field2_2) field2_2,
            sum(field3) field3, 
            sum(field3_2) field3_2,
            sum(field4) field4, 
            sum(field4_2) field4_2,
            sum(field5) field5,
            sum(field5_2) field5_2,
            sum(field6) field6, 
            sum(field6_2) field6_2,
            sum(field7) field7, 
            sum(field7_2) field7_2,
            sum(field8) field8,
            sum(field8_2) field8_2,
            sum(field9) field9,
            sum(field9_2) field9_2,
            sum(field10) field10, 
            sum(field10_2) field10_2,
            sum(field11) field11, 
            sum(field11_2) field11_2,
            sum(field12) field12, 
            sum(field12_2) field12_2,
            sum(field13) field13,
            sum(field13_2) field13_2,
            sum(field14) field14,
            sum(field14_2) field14_2,
            sum(field15) field15, 
            sum(field15_2) field15_2,
            sum(field16) field16, 
            sum(field16_2) field16_2,
            sum(field17) field17, 
            sum(field17_2) field17_2,
            sum(field18) field18,
            sum(field18_2) field18_2
    from (
        select personId personId,          
                field1 field1,
                (case when subCohort = 2 then field1 else 0 end) field1_2,
                field2 field2,
                (case when subCohort = 2 then field2 else 0 end) field2_2,
                field3 field3,
                (case when subCohort = 2 then field3 else 0 end) field3_2,
                field4 field4,
                (case when subCohort = 2 then field4 else 0 end) field4_2,
                field5 field5,
                (case when subCohort = 2 then field5 else 0 end) field5_2,
                field6 field6,
                (case when subCohort = 2 then field6 else 0 end) field6_2,
                field7 field7,
                (case when subCohort = 2 then field7 else 0 end) field7_2,
                field8 field8, 
                (case when subCohort = 2 then field8 else 0 end) field8_2,
                field9 field9,
                (case when subCohort = 2 then field9 else 0 end) field9_2,
                field10 field10,
                (case when subCohort = 2 then field10 else 0 end) field10_2,
                field11 field11,
                (case when subCohort = 2 then field11 else 0 end) field11_2,
                field12 field12,
                (case when subCohort = 2 then field12 else 0 end) field12_2,
                field13 field13,
                (case when subCohort = 2 then field13 else 0 end) field13_2,
                field14 field14,
                (case when subCohort = 2 then field14 else 0 end) field14_2,
                field15 field15,
                (case when subCohort = 2 then field15 else 0 end) field15_2,
                field16 field16, 
                (case when subCohort = 2 then field16 else 0 end) field16_2,
                field17 field17, 
                (case when subCohort = 2 then field17 else 0 end) field17_2,
                field18 field18, 
                (case when subCohort = 2 then field18 else 0 end) field18_2
            from CohortStatus
        )
    cross join estabSubCohortPartB1 pb1
    group by pb1.subcoh
)

union

select 'B' part,
        subCohortall section, --subcohort
        cohortStatusall line, --cohort status
        (case when cohortStatusall = 11 then field1_11
              when cohortStatusall = 12 then field1_12
              when cohortStatusall = 18 then field1_18
              when cohortStatusall = 19 then field1_19
              when cohortStatusall = 20 then field1_20
              when cohortStatusall = 30 then field1_30
              when cohortStatusall = 45 then field1_45
              when cohortStatusall = 51 then field1_51
            else 0 end) field1,
        (case when cohortStatusall = 11 then field2_11
              when cohortStatusall = 12 then field2_12
              when cohortStatusall = 18 then field2_18
              when cohortStatusall = 19 then field2_19
              when cohortStatusall = 20 then field2_20
              when cohortStatusall = 30 then field2_30
              when cohortStatusall = 45 then field2_45
              when cohortStatusall = 51 then field2_51
            else 0 end) field2,
        (case when cohortStatusall = 11 then field3_11
              when cohortStatusall = 12 then field3_12
              when cohortStatusall = 18 then field3_18
              when cohortStatusall = 19 then field3_19
              when cohortStatusall = 20 then field3_20
              when cohortStatusall = 30 then field3_30
              when cohortStatusall = 45 then field3_45
              when cohortStatusall = 51 then field3_51
            else 0 end) field3,
        (case when cohortStatusall = 11 then field4_11
              when cohortStatusall = 12 then field4_12
              when cohortStatusall = 18 then field4_18
              when cohortStatusall = 19 then field4_19
              when cohortStatusall = 20 then field4_20
              when cohortStatusall = 30 then field4_30
              when cohortStatusall = 45 then field4_45
              when cohortStatusall = 51 then field4_51
            else 0 end) field4,
        (case when cohortStatusall = 11 then field5_11
              when cohortStatusall = 12 then field5_12
              when cohortStatusall = 18 then field5_18
              when cohortStatusall = 19 then field5_19
              when cohortStatusall = 20 then field5_20
              when cohortStatusall = 30 then field5_30
              when cohortStatusall = 45 then field5_45
              when cohortStatusall = 51 then field5_51
            else 0 end) field5,
        (case when cohortStatusall = 11 then field6_11
              when cohortStatusall = 12 then field6_12
              when cohortStatusall = 18 then field6_18
              when cohortStatusall = 19 then field6_19
              when cohortStatusall = 20 then field6_20
              when cohortStatusall = 30 then field6_30
              when cohortStatusall = 45 then field6_45
              when cohortStatusall = 51 then field6_51
            else 0 end) field6,
        (case when cohortStatusall = 11 then field7_11
              when cohortStatusall = 12 then field7_12
              when cohortStatusall = 18 then field7_18
              when cohortStatusall = 19 then field7_19
              when cohortStatusall = 20 then field7_20
              when cohortStatusall = 30 then field7_30
              when cohortStatusall = 45 then field7_45
              when cohortStatusall = 51 then field7_51
            else 0 end) field7,
        (case when cohortStatusall = 11 then field8_11
              when cohortStatusall = 12 then field8_12
              when cohortStatusall = 18 then field8_18
              when cohortStatusall = 19 then field8_19
              when cohortStatusall = 20 then field8_20
              when cohortStatusall = 30 then field8_30
              when cohortStatusall = 45 then field8_45
              when cohortStatusall = 51 then field8_51
            else 0 end) field8,
        (case when cohortStatusall = 11 then field9_11
              when cohortStatusall = 12 then field9_12
              when cohortStatusall = 18 then field9_18
              when cohortStatusall = 19 then field9_19
              when cohortStatusall = 20 then field9_20
              when cohortStatusall = 30 then field9_30
              when cohortStatusall = 45 then field9_45
              when cohortStatusall = 51 then field9_51
            else 0 end) field9,
        (case when cohortStatusall = 11 then field10_11
              when cohortStatusall = 12 then field10_12
              when cohortStatusall = 18 then field10_18
              when cohortStatusall = 19 then field10_19
              when cohortStatusall = 20 then field10_20
              when cohortStatusall = 30 then field10_30
              when cohortStatusall = 45 then field10_45
              when cohortStatusall = 51 then field10_51
            else 0 end) field10,
        (case when cohortStatusall = 11 then field11_11
              when cohortStatusall = 12 then field11_12
              when cohortStatusall = 18 then field11_18
              when cohortStatusall = 19 then field11_19
              when cohortStatusall = 20 then field11_20
              when cohortStatusall = 30 then field11_30
              when cohortStatusall = 45 then field11_45
              when cohortStatusall = 51 then field11_51
            else 0 end) field11,
        (case when cohortStatusall = 11 then field12_11
              when cohortStatusall = 12 then field12_12
              when cohortStatusall = 18 then field12_18
              when cohortStatusall = 19 then field12_19
              when cohortStatusall = 20 then field12_20
              when cohortStatusall = 30 then field12_30
              when cohortStatusall = 45 then field12_45
              when cohortStatusall = 51 then field12_51
            else 0 end) field12,
        (case when cohortStatusall = 11 then field13_11
              when cohortStatusall = 12 then field13_12
              when cohortStatusall = 18 then field13_18
              when cohortStatusall = 19 then field13_19
              when cohortStatusall = 20 then field13_20
              when cohortStatusall = 30 then field13_30
              when cohortStatusall = 45 then field13_45
              when cohortStatusall = 51 then field13_51
            else 0 end) field13,
        (case when cohortStatusall = 11 then field14_11
              when cohortStatusall = 12 then field14_12
              when cohortStatusall = 18 then field14_18
              when cohortStatusall = 19 then field14_19
              when cohortStatusall = 20 then field14_20
              when cohortStatusall = 30 then field14_30
              when cohortStatusall = 45 then field14_45
              when cohortStatusall = 51 then field14_51
            else 0 end) field14,
        (case when cohortStatusall = 11 then field15_11
              when cohortStatusall = 12 then field15_12
              when cohortStatusall = 18 then field15_18
              when cohortStatusall = 19 then field15_19
              when cohortStatusall = 20 then field15_20
              when cohortStatusall = 30 then field15_30
              when cohortStatusall = 45 then field15_45
              when cohortStatusall = 51 then field15_51
            else 0 end) field15,
        (case when cohortStatusall = 11 then field16_11
              when cohortStatusall = 12 then field16_12
              when cohortStatusall = 18 then field16_18
              when cohortStatusall = 19 then field16_19
              when cohortStatusall = 20 then field16_20
              when cohortStatusall = 30 then field16_30
              when cohortStatusall = 45 then field16_45
              when cohortStatusall = 51 then field16_51
            else 0 end) field16,
        (case when cohortStatusall = 11 then field17_11
              when cohortStatusall = 12 then field17_12
              when cohortStatusall = 18 then field17_18
              when cohortStatusall = 19 then field17_19
              when cohortStatusall = 20 then field17_20
              when cohortStatusall = 30 then field17_30
              when cohortStatusall = 45 then field17_45
              when cohortStatusall = 51 then field17_51
            else 0 end) field17,
        (case when cohortStatusall = 11 then field18_11
              when cohortStatusall = 12 then field18_12
              when cohortStatusall = 18 then field18_18
              when cohortStatusall = 19 then field18_19
              when cohortStatusall = 20 then field18_20
              when cohortStatusall = 30 then field18_30
              when cohortStatusall = 45 then field18_45
              when cohortStatusall = 51 then field18_51
            else 0 end) field18
from (
    select subCoh.subcoh subCohortall,
        cohSt.cohortStatus cohortStatusall,
        sum(cohortStatus11) cohortStatus11,
        sum(cohortStatus12) cohortStatus12,
        sum(cohortStatus18) cohortStatus18,
        sum(cohortStatus19) cohortStatus19,
        sum(cohortStatus20) cohortStatus20,
        sum(cohortStatus30) cohortStatus30,
        sum(cohortStatus45) cohortStatus45,
        sum(cohortStatus51) cohortStatus51,
        sum(field1_11) field1_11, 
        sum(field1_12) field1_12,
        sum(field1_18) field1_18,
        sum(field1_19) field1_19,
        sum(field1_20) field1_20,
        sum(field1_30) field1_30,
        sum(field1_45) field1_45,
        sum(field1_51) field1_51,
        sum(field2_11) field2_11, 
        sum(field2_12) field2_12,
        sum(field2_18) field2_18,
        sum(field2_19) field2_19,
        sum(field2_20) field2_20,
        sum(field2_30) field2_30,
        sum(field2_45) field2_45,
        sum(field2_51) field2_51,
        sum(field3_11) field3_11, 
        sum(field3_12) field3_12,
        sum(field3_18) field3_18,
        sum(field3_19) field3_19,
        sum(field3_20) field3_20,
        sum(field3_30) field3_30,
        sum(field3_45) field3_45,
        sum(field3_51) field3_51,
        sum(field4_11) field4_11, 
        sum(field4_12) field4_12,
        sum(field4_18) field4_18,
        sum(field4_19) field4_19,
        sum(field4_20) field4_20,
        sum(field4_30) field4_30,
        sum(field4_45) field4_45,
        sum(field4_51) field4_51,
        sum(field5_11) field5_11, 
        sum(field5_12) field5_12,
        sum(field5_18) field5_18,
        sum(field5_19) field5_19,
        sum(field5_20) field5_20,
        sum(field5_30) field5_30,
        sum(field5_45) field5_45,
        sum(field5_51) field5_51,
        sum(field6_11) field6_11, 
        sum(field6_12) field6_12,
        sum(field6_18) field6_18,
        sum(field6_19) field6_19,
        sum(field6_20) field6_20,
        sum(field6_30) field6_30,
        sum(field6_45) field6_45,
        sum(field6_51) field6_51,
        sum(field7_11) field7_11, 
        sum(field7_12) field7_12,
        sum(field7_18) field7_18,
        sum(field7_19) field7_19,
        sum(field7_20) field7_20,
        sum(field7_30) field7_30,
        sum(field7_45) field7_45,
        sum(field7_51) field7_51,
        sum(field8_11) field8_11, 
        sum(field8_12) field8_12,
        sum(field8_18) field8_18,
        sum(field8_19) field8_19,
        sum(field8_20) field8_20,
        sum(field8_30) field8_30,
        sum(field8_45) field8_45,
        sum(field8_51) field8_51,
        sum(field9_11) field9_11, 
        sum(field9_12) field9_12,
        sum(field9_18) field9_18,
        sum(field9_19) field9_19,
        sum(field9_20) field9_20,
        sum(field9_30) field9_30,
        sum(field9_45) field9_45,
        sum(field9_51) field9_51,
        sum(field10_11) field10_11, 
        sum(field10_12) field10_12,
        sum(field10_18) field10_18,
        sum(field10_19) field10_19,
        sum(field10_20) field10_20,
        sum(field10_30) field10_30,
        sum(field10_45) field10_45,
        sum(field10_51) field10_51,
        sum(field11_11) field11_11, 
        sum(field11_12) field11_12,
        sum(field11_18) field11_18,
        sum(field11_19) field11_19,
        sum(field11_20) field11_20,
        sum(field11_30) field11_30,
        sum(field11_45) field11_45,
        sum(field11_51) field11_51,
        sum(field12_11) field12_11, 
        sum(field12_12) field12_12,
        sum(field12_18) field12_18,
        sum(field12_19) field12_19,
        sum(field12_20) field12_20,
        sum(field12_30) field12_30,
        sum(field12_45) field12_45,
        sum(field12_51) field12_51,
        sum(field13_11) field13_11, 
        sum(field13_12) field13_12,
        sum(field13_18) field13_18,
        sum(field13_19) field13_19,
        sum(field13_20) field13_20,
        sum(field13_30) field13_30,
        sum(field13_45) field13_45,
        sum(field13_51) field13_51,
        sum(field14_11) field14_11, 
        sum(field14_12) field14_12,
        sum(field14_18) field14_18,
        sum(field14_19) field14_19,
        sum(field14_20) field14_20,
        sum(field14_30) field14_30,
        sum(field14_45) field14_45,
        sum(field14_51) field14_51,
        sum(field15_11) field15_11, 
        sum(field15_12) field15_12,
        sum(field15_18) field15_18,
        sum(field15_19) field15_19,
        sum(field15_20) field15_20,
        sum(field15_30) field15_30,
        sum(field15_45) field15_45,
        sum(field15_51) field15_51,
        sum(field16_11) field16_11, 
        sum(field16_12) field16_12,
        sum(field16_18) field16_18,
        sum(field16_19) field16_19,
        sum(field16_20) field16_20,
        sum(field16_30) field16_30,
        sum(field16_45) field16_45,
        sum(field16_51) field16_51,
        sum(field17_11) field17_11, 
        sum(field17_12) field17_12,
        sum(field17_18) field17_18,
        sum(field17_19) field17_19,
        sum(field17_20) field17_20,
        sum(field17_30) field17_30,
        sum(field17_45) field17_45,
        sum(field17_51) field17_51,
        sum(field18_11) field18_11, 
        sum(field18_12) field18_12,
        sum(field18_18) field18_18,
        sum(field18_19) field18_19,
        sum(field18_20) field18_20,
        sum(field18_30) field18_30,
        sum(field18_45) field18_45,
        sum(field18_51) field18_51
    from estabSubCohort subCoh
        cross join FormatPartB cohSt
        left join (
            select personId personId,
                    subCohort subCohort,
                    stat11 cohortStatus11,
                    stat12 cohortStatus12,
                    stat18 cohortStatus18,
                    stat19 cohortStatus19,
                    stat20 cohortStatus20, 
                    stat30 cohortStatus30,
                    stat45 cohortStatus45,
                    stat51 cohortStatus51,
                    (case when stat11 = 1 then field1 else 0 end) field1_11, 
                    (case when stat12 = 1 then field1 else 0 end) field1_12,
                    (case when stat18 = 1 then field1 else 0 end) field1_18,
                    (case when stat19 = 1 then field1 else 0 end) field1_19,
                    (case when stat20 = 1 then field1 else 0 end) field1_20,
                    (case when stat30 = 1 then field1 else 0 end) field1_30,
                    (case when stat45 = 1 then field1 else 0 end) field1_45,
                    (case when stat51 = 1 then field1 else 0 end) field1_51,
                    (case when stat11 = 1 then field2 else 0 end) field2_11, 
                    (case when stat12 = 1 then field2 else 0 end) field2_12,
                    (case when stat18 = 1 then field2 else 0 end) field2_18,
                    (case when stat19 = 1 then field2 else 0 end) field2_19,
                    (case when stat20 = 1 then field2 else 0 end) field2_20,
                    (case when stat30 = 1 then field2 else 0 end) field2_30,
                    (case when stat45 = 1 then field2 else 0 end) field2_45,
                    (case when stat51 = 1 then field2 else 0 end) field2_51,
                    (case when stat11 = 1 then field3 else 0 end) field3_11, 
                    (case when stat12 = 1 then field3 else 0 end) field3_12,
                    (case when stat18 = 1 then field3 else 0 end) field3_18,
                    (case when stat19 = 1 then field3 else 0 end) field3_19,
                    (case when stat20 = 1 then field3 else 0 end) field3_20,
                    (case when stat30 = 1 then field3 else 0 end) field3_30,
                    (case when stat45 = 1 then field3 else 0 end) field3_45,
                    (case when stat51 = 1 then field3 else 0 end) field3_51,
                    (case when stat11 = 1 then field4 else 0 end) field4_11, 
                    (case when stat12 = 1 then field4 else 0 end) field4_12,
                    (case when stat18 = 1 then field4 else 0 end) field4_18,
                    (case when stat19 = 1 then field4 else 0 end) field4_19,
                    (case when stat20 = 1 then field4 else 0 end) field4_20,
                    (case when stat30 = 1 then field4 else 0 end) field4_30,
                    (case when stat45 = 1 then field4 else 0 end) field4_45,
                    (case when stat51 = 1 then field4 else 0 end) field4_51,
                    (case when stat11 = 1 then field5 else 0 end) field5_11, 
                    (case when stat12 = 1 then field5 else 0 end) field5_12,
                    (case when stat18 = 1 then field5 else 0 end) field5_18,
                    (case when stat19 = 1 then field5 else 0 end) field5_19,
                    (case when stat20 = 1 then field5 else 0 end) field5_20,
                    (case when stat30 = 1 then field5 else 0 end) field5_30,
                    (case when stat45 = 1 then field5 else 0 end) field5_45,
                    (case when stat51 = 1 then field5 else 0 end) field5_51,
                    (case when stat11 = 1 then field6 else 0 end) field6_11, 
                    (case when stat12 = 1 then field6 else 0 end) field6_12,
                    (case when stat18 = 1 then field6 else 0 end) field6_18,
                    (case when stat19 = 1 then field6 else 0 end) field6_19,
                    (case when stat20 = 1 then field6 else 0 end) field6_20,
                    (case when stat30 = 1 then field6 else 0 end) field6_30,
                    (case when stat45 = 1 then field6 else 0 end) field6_45,
                    (case when stat51 = 1 then field6 else 0 end) field6_51,
                    (case when stat11 = 1 then field7 else 0 end) field7_11, 
                    (case when stat12 = 1 then field7 else 0 end) field7_12,
                    (case when stat18 = 1 then field7 else 0 end) field7_18,
                    (case when stat19 = 1 then field7 else 0 end) field7_19,
                    (case when stat20 = 1 then field7 else 0 end) field7_20,
                    (case when stat30 = 1 then field7 else 0 end) field7_30,
                    (case when stat45 = 1 then field7 else 0 end) field7_45,
                    (case when stat51 = 1 then field7 else 0 end) field7_51,
                    (case when stat11 = 1 then field8 else 0 end) field8_11, 
                    (case when stat12 = 1 then field8 else 0 end) field8_12,
                    (case when stat18 = 1 then field8 else 0 end) field8_18,
                    (case when stat19 = 1 then field8 else 0 end) field8_19,
                    (case when stat20 = 1 then field8 else 0 end) field8_20,
                    (case when stat30 = 1 then field8 else 0 end) field8_30,
                    (case when stat45 = 1 then field8 else 0 end) field8_45,
                    (case when stat51 = 1 then field8 else 0 end) field8_51,
                    (case when stat11 = 1 then field9 else 0 end) field9_11, 
                    (case when stat12 = 1 then field9 else 0 end) field9_12,
                    (case when stat18 = 1 then field9 else 0 end) field9_18,
                    (case when stat19 = 1 then field9 else 0 end) field9_19,
                    (case when stat20 = 1 then field9 else 0 end) field9_20,
                    (case when stat30 = 1 then field9 else 0 end) field9_30,
                    (case when stat45 = 1 then field9 else 0 end) field9_45,
                    (case when stat51 = 1 then field9 else 0 end) field9_51,
                    (case when stat11 = 1 then field10 else 0 end) field10_11, 
                    (case when stat12 = 1 then field10 else 0 end) field10_12,
                    (case when stat18 = 1 then field10 else 0 end) field10_18,
                    (case when stat19 = 1 then field10 else 0 end) field10_19,
                    (case when stat20 = 1 then field10 else 0 end) field10_20,
                    (case when stat30 = 1 then field10 else 0 end) field10_30,
                    (case when stat45 = 1 then field10 else 0 end) field10_45,
                    (case when stat51 = 1 then field10 else 0 end) field10_51,
                    (case when stat11 = 1 then field11 else 0 end) field11_11, 
                    (case when stat12 = 1 then field11 else 0 end) field11_12,
                    (case when stat18 = 1 then field11 else 0 end) field11_18,
                    (case when stat19 = 1 then field11 else 0 end) field11_19,
                    (case when stat20 = 1 then field11 else 0 end) field11_20,
                    (case when stat30 = 1 then field11 else 0 end) field11_30,
                    (case when stat45 = 1 then field11 else 0 end) field11_45,
                    (case when stat51 = 1 then field11 else 0 end) field11_51,
                    (case when stat11 = 1 then field12 else 0 end) field12_11, 
                    (case when stat12 = 1 then field12 else 0 end) field12_12,
                    (case when stat18 = 1 then field12 else 0 end) field12_18,
                    (case when stat19 = 1 then field12 else 0 end) field12_19,
                    (case when stat20 = 1 then field12 else 0 end) field12_20,
                    (case when stat30 = 1 then field12 else 0 end) field12_30,
                    (case when stat45 = 1 then field12 else 0 end) field12_45,
                    (case when stat51 = 1 then field12 else 0 end) field12_51,
                    (case when stat11 = 1 then field13 else 0 end) field13_11, 
                    (case when stat12 = 1 then field13 else 0 end) field13_12,
                    (case when stat18 = 1 then field13 else 0 end) field13_18,
                    (case when stat19 = 1 then field13 else 0 end) field13_19,
                    (case when stat20 = 1 then field13 else 0 end) field13_20,
                    (case when stat30 = 1 then field13 else 0 end) field13_30,
                    (case when stat45 = 1 then field13 else 0 end) field13_45,
                    (case when stat51 = 1 then field13 else 0 end) field13_51,
                    (case when stat11 = 1 then field14 else 0 end) field14_11, 
                    (case when stat12 = 1 then field14 else 0 end) field14_12,
                    (case when stat18 = 1 then field14 else 0 end) field14_18,
                    (case when stat19 = 1 then field14 else 0 end) field14_19,
                    (case when stat20 = 1 then field14 else 0 end) field14_20,
                    (case when stat30 = 1 then field14 else 0 end) field14_30,
                    (case when stat45 = 1 then field14 else 0 end) field14_45,
                    (case when stat51 = 1 then field14 else 0 end) field14_51,
                    (case when stat11 = 1 then field15 else 0 end) field15_11, 
                    (case when stat12 = 1 then field15 else 0 end) field15_12,
                    (case when stat18 = 1 then field15 else 0 end) field15_18,
                    (case when stat19 = 1 then field15 else 0 end) field15_19,
                    (case when stat20 = 1 then field15 else 0 end) field15_20,
                    (case when stat30 = 1 then field15 else 0 end) field15_30,
                    (case when stat45 = 1 then field15 else 0 end) field15_45,
                    (case when stat51 = 1 then field15 else 0 end) field15_51,
                    (case when stat11 = 1 then field16 else 0 end) field16_11, 
                    (case when stat12 = 1 then field16 else 0 end) field16_12,
                    (case when stat18 = 1 then field16 else 0 end) field16_18,
                    (case when stat19 = 1 then field16 else 0 end) field16_19,
                    (case when stat20 = 1 then field16 else 0 end) field16_20,
                    (case when stat30 = 1 then field16 else 0 end) field16_30,
                    (case when stat45 = 1 then field16 else 0 end) field16_45,
                    (case when stat51 = 1 then field16 else 0 end) field16_51,
                    (case when stat11 = 1 then field17 else 0 end) field17_11, 
                    (case when stat12 = 1 then field17 else 0 end) field17_12,
                    (case when stat18 = 1 then field17 else 0 end) field17_18,
                    (case when stat19 = 1 then field17 else 0 end) field17_19,
                    (case when stat20 = 1 then field17 else 0 end) field17_20,
                    (case when stat30 = 1 then field17 else 0 end) field17_30,
                    (case when stat45 = 1 then field17 else 0 end) field17_45,
                    (case when stat51 = 1 then field17 else 0 end) field17_51,
                    (case when stat11 = 1 then field18 else 0 end) field18_11, 
                    (case when stat12 = 1 then field18 else 0 end) field18_12,
                    (case when stat18 = 1 then field18 else 0 end) field18_18,
                    (case when stat19 = 1 then field18 else 0 end) field18_19,
                    (case when stat20 = 1 then field18 else 0 end) field18_20,
                    (case when stat30 = 1 then field18 else 0 end) field18_30,
                    (case when stat45 = 1 then field18 else 0 end) field18_45,
                    (case when stat51 = 1 then field18 else 0 end) field18_51 
            from CohortStatus
            ) innerData on subCoh.subcoh = innerData.subCohort
        group by subCoh.subcoh, cohSt.cohortStatus
)
where not (subCohortall = 3
    and cohortStatusall = 19)
and not (subCohortall = 3
    and cohortStatusall = 20)
    
union

select 'C' part,
        subCohortall section, --subcohort
        cohortStatusall line, --cohort status
        (case when cohortStatusall = 10 then isPellRec 
              when cohortStatusall = 18 then isPellRec18
              when cohortStatusall = 29 then isPellRec29
            else isPellRec45 end) field1, --isPell,
        (case when cohortStatusall = 10 then isSubLoanRec 
              when cohortStatusall = 18 then isSubLoanRec18
              when cohortStatusall = 29 then isSubLoanRec29
            else isSubLoanRec45 end) field2, --isSubLoan,
	null field3,
	null field4,
	null field5,
	null field6,
	null field7,
	null field8,
	null field9,
	null field10,
	null field11,
	null field12,
	null field13,
	null field14,
	null field15,
	null field16,
	null field17,
	null field18
from (
    select  subCoh.subcoh subCohortall,
            cohSt.cohortStatus cohortStatusall,
            sum(isPellRec) isPellRec,
                sum(isPellRec18) isPellRec18,
                sum(isPellRec29) isPellRec29,
                sum(isPellRec45) isPellRec45,
                sum(isSubLoanRec) isSubLoanRec,
                sum(isSubLoanRec18) isSubLoanRec18,
                sum(isSubLoanRec29) isSubLoanRec29,
                sum(isSubLoanRec45) isSubLoanRec45,
                sum(stat18) stat18,
                sum(stat29) stat29,
                sum(stat45) stat45 
    from estabSubCohort subCoh 
        cross join FormatPartC cohSt
        left join (
            select personId personId,
                    subCohort subCohort,
                    (case when stat18 > 0 then 1 end) cohortStatus18,
                    (case when stat29 > 0 then 1 end) cohortStatus29,
                    (case when stat45 > 0 then 1 end) cohortStatus45,
                    isPellRec isPellRec,
                    isSubLoanRec isSubLoanRec,
                    stat18 stat18,
                    stat29 stat29,
                    stat45 stat45,
                    (case when stat18 = 1 then isPellRec else 0 end) isPellRec18,
                    (case when stat29 = 1 then isPellRec else 0 end) isPellRec29,
                    (case when stat45 = 1 then isPellRec else 0 end) isPellRec45,
                    (case when stat18 = 1 then isSubLoanRec else 0 end) isSubLoanRec18,
                    (case when stat29 = 1 then isSubLoanRec else 0 end) isSubLoanRec29,
                    (case when stat45 = 1 then isSubLoanRec else 0 end) isSubLoanRec45
            from CohortStatus
            ) innerData on subCoh.subcoh = innerData.subCohort
        group by subCoh.subcoh,
            cohSt.cohortStatus
        )
order by 1, 2, 3
