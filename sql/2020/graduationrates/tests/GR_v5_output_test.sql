%sql

--GR version 5

WITH Cohort as (
select *
from (
	VALUES
        ('120',2,0,0,'F',7,null),
        ('2016',2,0,0,'F',7,55),
        ('2946',2,0,0,'M',7,null),
        ('3270',2,0,0,'F',7,11),
        ('30081',2,1,0,'F',7,11),
        ('30362',2,0,0,'M',2,null),
        ('30553',2,0,0,'F',7,55),
        ('31072',3,0,0,'M',9,55),
        ('31222',3,0,0,'F',2,11),
        ('31228',3,0,0,'M',9,11),
        ('31230',3,0,1,'M',3,null),
        ('31237',2,0,0,'M',3,55),
        ('31246',2,1,0,'M',7,30),
        ('31248',2,1,0,'F',9,55),
        ('31249',3,1,0,'F',2,null),
        ('31253',3,1,0,'F',7,55),
        ('31254',2,0,0,'M',3,null),
        ('31271',2,0,0,'M',5,null),
        ('31331',3,0,0,'M',7,51),
        ('31339',2,0,0,'M',7,null),
        ('31351',3,0,0,'M',7,55),
        ('31376',3,1,0,'F',9,null),
        ('31394',2,0,0,'M',7,30),
        ('31421',3,1,0,'F',9,45),
        ('31435',2,1,0,'F',3,55),
        ('31436',2,1,0,'F',5,55),
        ('31441',3,0,1,'F',3,11),
        ('31476',2,0,1,'M',7,null),
        ('31477',2,0,1,'M',7,30),
        ('31555',2,0,0,'M',5,55),
        ('31619',2,0,0,'M',5,null),
        ('31620',2,0,0,'M',5,55),
        ('31621',2,0,0,'M',5,null),
        ('31637',2,0,0,'F',7,55),
        ('31640',2,0,1,'M',5,11),
        ('31837',2,1,0,'M',5,11),
        ('31862',3,1,0,'M',9,30),
        ('31870',3,0,0,'M',2,30),
        ('31889',3,0,1,'F',9,11),
        ('31910',3,0,1,'F',9,11),
        ('31922',3,1,0,'M',9,null),
        ('32136',2,1,0,'F',2,55),
        ('32186',2,0,0,'M',5,30),
        ('32352',2,1,0,'M',5,null),
        ('32363',2,0,0,'F',9,11),
        ('32364',2,0,0,'F',7,51),
        ('32388',2,0,0,'M',7,null),
        ('32402',2,0,1,'M',7,55),
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
        
--if awarded degree program is less than 2 years and within the awarded grad date 100%
        (case when latestStatus = 55 then 1 else 0 end) stat55,
--if awarded degree program is at least 2 years and less than 4 years and within the awarded grad date 100%
        --(case when latestStatus = 9 then 1 else 0 end) stat9,
        1 stat10,
        (case when latestStatus = 55 or latestStatus = 11 then 1 else 0 end) stat11, --Completers of programs of less than 2 years within 150% of normal time --all
        --(case when latestStatus = 9 or latestStatus = 12 then 1 else 0 end) stat12, --Completers of programs of at least 2 but less than 4 years within 150% of normal time --all
       -- (case when latestStatus = 18 or latestStatus = 19 or latestStatus = 20 then 1 else 0 end) stat18, --Completed bachelor's degree or equivalent within 150% --all
       -- (case when latestStatus = 19 then 1 else 0 end) stat19, --Completers of bachelor's or equivalent degree programs in 4 years or less --subcohort 2
       -- (case when latestStatus = 20 then 1 else 0 end) stat20, --Completers of bachelor's or equivalent degree programs in 5 years --subcohort 2
        --(case when latestStatus = 8
        --        or latestStatus = 9
        --        or latestStatus = 11
        --        or latestStatus = 12 then 1 else 0 end) stat29, --Total completers within 150% 
        (case when latestStatus = 30 then 1 else 0 end) stat30, --Total transfer-out students (non-completers)
        (case when latestStatus = 45 then 1 else 0 end) stat45, --Total exclusions  
        (case when latestStatus = 51 then 1 else 0 end) stat51 --Still enrolled
from Cohort
--where 1 = 2
),

FormatPartB as (
select *
from (
	VALUES
	    (10), -- 10 - Revised cohort of full-time, first-time degree or certificate seeking students, cohort year 2016 
		(11), -- 11 - Completers of programs of less than 2 years within 150% of normal time 
		--(11), -- 12 - Completers of programs of at least 2 but less than 4 years within 150% of normal time --all
		(30), -- 30 - Total transfer-out students (non-completers)
		(45), -- 45 - Total exclusions 
        (55), -- 55 - Completers of programs of less than 2 years within 100% of normal time 
		(51)  -- 51 - Still enrolled 
	) as PartB (cohortStatus)
),

FormatPartC as (
select *
from (
	VALUES
		(10), -- 10 - Number of students in cohort
		(11), -- 11 - Number of students that completed within 150% of normal time to completion
		(45)  -- 45 - Total exclusions
	) as PartC (cohortStatus)
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs
*****/

--Part B: Cohort of Full-time, First-time Degree or Certificate Seeking Students Cohort Year 2017

select 'B' part,
        cast(cohStat.cohortStatus as string) line, --cohort status
        cast(coalesce((case when cohStat.cohortStatus = 10 then field1_10
              when cohStat.cohortStatus = 11 then field1_11
              when cohStat.cohortStatus = 55 then field1_55
              when cohStat.cohortStatus = 30 then field1_30
              when cohStat.cohortStatus = 45 then field1_45
              when cohStat.cohortStatus = 51 then field1_51
            else 0 end), 0) as string) field1,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field2_10
              when cohStat.cohortStatus = 11 then field2_11
              when cohStat.cohortStatus = 55 then field2_55
              when cohStat.cohortStatus = 30 then field2_30
              when cohStat.cohortStatus = 45 then field2_45
              when cohStat.cohortStatus = 51 then field2_51
            else 0 end), 0) as string) field2,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field3_10
              when cohStat.cohortStatus = 11 then field3_11
              when cohStat.cohortStatus = 55 then field3_55
              when cohStat.cohortStatus = 30 then field3_30
              when cohStat.cohortStatus = 45 then field3_45
              when cohStat.cohortStatus = 51 then field3_51
            else 0 end), 0) as string) field3,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field4_10
              when cohStat.cohortStatus = 11 then field4_11
              when cohStat.cohortStatus = 55 then field4_55
              when cohStat.cohortStatus = 30 then field4_30
              when cohStat.cohortStatus = 45 then field4_45
              when cohStat.cohortStatus = 51 then field4_51
            else 0 end), 0) as string) field4,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field5_10
              when cohStat.cohortStatus = 11 then field5_11
              when cohStat.cohortStatus = 55 then field5_55
              when cohStat.cohortStatus = 30 then field5_30
              when cohStat.cohortStatus = 45 then field5_45
              when cohStat.cohortStatus = 51 then field5_51
            else 0 end), 0) as string) field5,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field6_10
              when cohStat.cohortStatus = 11 then field6_11
              when cohStat.cohortStatus = 55 then field6_55
              when cohStat.cohortStatus = 30 then field6_30
              when cohStat.cohortStatus = 45 then field6_45
              when cohStat.cohortStatus = 51 then field6_51
            else 0 end), 0) as string) field6,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field7_10
              when cohStat.cohortStatus = 11 then field7_11
              when cohStat.cohortStatus = 55 then field7_55
              when cohStat.cohortStatus = 30 then field7_30
              when cohStat.cohortStatus = 45 then field7_45
              when cohStat.cohortStatus = 51 then field7_51
            else 0 end), 0) as string) field7,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field8_10
              when cohStat.cohortStatus = 11 then field8_11
              when cohStat.cohortStatus = 55 then field8_55
              when cohStat.cohortStatus = 30 then field8_30
              when cohStat.cohortStatus = 45 then field8_45
              when cohStat.cohortStatus = 51 then field8_51
            else 0 end), 0) as string) field8,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field9_10
              when cohStat.cohortStatus = 11 then field9_11
              when cohStat.cohortStatus = 55 then field9_55
              when cohStat.cohortStatus = 30 then field9_30
              when cohStat.cohortStatus = 45 then field9_45
              when cohStat.cohortStatus = 51 then field9_51
            else 0 end), 0) as string) field9,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field10_10
              when cohStat.cohortStatus = 11 then field10_11
              when cohStat.cohortStatus = 55 then field10_55
              when cohStat.cohortStatus = 30 then field10_30
              when cohStat.cohortStatus = 45 then field10_45
              when cohStat.cohortStatus = 51 then field10_51
            else 0 end), 0) as string) field10,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field11_10
              when cohStat.cohortStatus = 11 then field11_11
              when cohStat.cohortStatus = 55 then field11_55
              when cohStat.cohortStatus = 30 then field11_30
              when cohStat.cohortStatus = 45 then field11_45
              when cohStat.cohortStatus = 51 then field11_51
            else 0 end), 0) as string) field11,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field12_10
              when cohStat.cohortStatus = 11 then field12_11
              when cohStat.cohortStatus = 55 then field12_55
              when cohStat.cohortStatus = 30 then field12_30
              when cohStat.cohortStatus = 45 then field12_45
              when cohStat.cohortStatus = 51 then field12_51
            else 0 end), 0) as string) field12,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field13_10
              when cohStat.cohortStatus = 11 then field13_11
              when cohStat.cohortStatus = 55 then field13_55
              when cohStat.cohortStatus = 30 then field13_30
              when cohStat.cohortStatus = 45 then field13_45
              when cohStat.cohortStatus = 51 then field13_51
            else 0 end), 0) as string) field13,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field14_10
              when cohStat.cohortStatus = 11 then field14_11
              when cohStat.cohortStatus = 55 then field14_55
              when cohStat.cohortStatus = 30 then field14_30
              when cohStat.cohortStatus = 45 then field14_45
              when cohStat.cohortStatus = 51 then field14_51
            else 0 end), 0) as string) field14,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field15_10
              when cohStat.cohortStatus = 11 then field15_11
              when cohStat.cohortStatus = 55 then field15_55
              when cohStat.cohortStatus = 30 then field15_30
              when cohStat.cohortStatus = 45 then field15_45
              when cohStat.cohortStatus = 51 then field15_51
            else 0 end), 0) as string) field15,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field16_10
              when cohStat.cohortStatus = 11 then field16_11
              when cohStat.cohortStatus = 55 then field16_55
              when cohStat.cohortStatus = 30 then field16_30
              when cohStat.cohortStatus = 45 then field16_45
              when cohStat.cohortStatus = 51 then field16_51
            else 0 end), 0) as string) field16,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field17_10
              when cohStat.cohortStatus = 11 then field17_11
              when cohStat.cohortStatus = 55 then field17_55
              when cohStat.cohortStatus = 30 then field17_30
              when cohStat.cohortStatus = 45 then field17_45
              when cohStat.cohortStatus = 51 then field17_51
            else 0 end), 0) as string) field17,
        cast(coalesce((case when cohStat.cohortStatus = 10 then field18_10
              when cohStat.cohortStatus = 11 then field18_11
              when cohStat.cohortStatus = 55 then field18_55
              when cohStat.cohortStatus = 30 then field18_30
              when cohStat.cohortStatus = 45 then field18_45
              when cohStat.cohortStatus = 51 then field18_51
            else 0 end), 0) as string) field18 
from FormatPartB cohStat
    cross join ( 
    select cohSt.cohortStatus cohortStatusall,
        sum(field1_10) field1_10,         
        sum(field1_11) field1_11, 
        sum(field1_55) field1_55,
        sum(field1_30) field1_30,
        sum(field1_45) field1_45,
        sum(field1_51) field1_51,
        sum(field2_10) field2_10,    
        sum(field2_11) field2_11, 
        sum(field2_55) field2_55,
        sum(field2_30) field2_30,
        sum(field2_45) field2_45,
        sum(field2_51) field2_51,
        sum(field3_10) field3_10,    
        sum(field3_11) field3_11, 
        sum(field3_55) field3_55,
        sum(field3_30) field3_30,
        sum(field3_45) field3_45,
        sum(field3_51) field3_51,
        sum(field4_10) field4_10,    
        sum(field4_11) field4_11, 
        sum(field4_55) field4_55,
        sum(field4_30) field4_30,
        sum(field4_45) field4_45,
        sum(field4_51) field4_51,
        sum(field5_10) field5_10,    
        sum(field5_11) field5_11, 
        sum(field5_55) field5_55,
        sum(field5_30) field5_30,
        sum(field5_45) field5_45,
        sum(field5_51) field5_51,
        sum(field6_10) field6_10,    
        sum(field6_11) field6_11, 
        sum(field6_55) field6_55,
        sum(field6_30) field6_30,
        sum(field6_45) field6_45,
        sum(field6_51) field6_51,
        sum(field7_10) field7_10,    
        sum(field7_11) field7_11, 
        sum(field7_55) field7_55,
        sum(field7_30) field7_30,
        sum(field7_45) field7_45,
        sum(field7_51) field7_51,
        sum(field8_10) field8_10,    
        sum(field8_11) field8_11, 
        sum(field8_55) field8_55,
        sum(field8_30) field8_30,
        sum(field8_45) field8_45,
        sum(field8_51) field8_51,
        sum(field9_10) field9_10,    
        sum(field9_11) field9_11, 
        sum(field9_55) field9_55,
        sum(field9_30) field9_30,
        sum(field9_45) field9_45,
        sum(field9_51) field9_51,
        sum(field10_10) field10_10,    
        sum(field10_11) field10_11, 
        sum(field10_55) field10_55,
        sum(field10_30) field10_30,
        sum(field10_45) field10_45,
        sum(field10_51) field10_51,
        sum(field11_10) field11_10,    
        sum(field11_11) field11_11, 
        sum(field11_55) field11_55,
        sum(field11_30) field11_30,
        sum(field11_45) field11_45,
        sum(field11_51) field11_51,
        sum(field12_10) field12_10,    
        sum(field12_11) field12_11, 
        sum(field12_55) field12_55,
        sum(field12_30) field12_30,
        sum(field12_45) field12_45,
        sum(field12_51) field12_51,
        sum(field13_10) field13_10,    
        sum(field13_11) field13_11, 
        sum(field13_55) field13_55,
        sum(field13_30) field13_30,
        sum(field13_45) field13_45,
        sum(field13_51) field13_51,
        sum(field14_10) field14_10,    
        sum(field14_11) field14_11, 
        sum(field14_55) field14_55,
        sum(field14_30) field14_30,
        sum(field14_45) field14_45,
        sum(field14_51) field14_51,
        sum(field15_10) field15_10,    
        sum(field15_11) field15_11, 
        sum(field15_55) field15_55,
        sum(field15_30) field15_30,
        sum(field15_45) field15_45,
        sum(field15_51) field15_51,
        sum(field16_10) field16_10,    
        sum(field16_11) field16_11, 
        sum(field16_55) field16_55,
        sum(field16_30) field16_30,
        sum(field16_45) field16_45,
        sum(field16_51) field16_51,
        sum(field17_10) field17_10,    
        sum(field17_11) field17_11, 
        sum(field17_55) field17_55,
        sum(field17_30) field17_30,
        sum(field17_45) field17_45,
        sum(field17_51) field17_51,
        sum(field18_10) field18_10,    
        sum(field18_11) field18_11, 
        sum(field18_55) field18_55,
        sum(field18_30) field18_30,
        sum(field18_45) field18_45,
        sum(field18_51) field18_51
    from FormatPartB cohSt
        left join (
            select personId personId,
                    10 cohortStat,
                    field1 field1_10,
                    (case when stat11 = 1 then field1 else 0 end) field1_11, 
                    (case when stat55 = 1 then field1 else 0 end) field1_55,
                    (case when stat30 = 1 then field1 else 0 end) field1_30,
                    (case when stat45 = 1 then field1 else 0 end) field1_45,
                    (case when stat51 = 1 then field1 else 0 end) field1_51,
                    field2 field2_10,
                    (case when stat11 = 1 then field2 else 0 end) field2_11, 
                    (case when stat55 = 1 then field2 else 0 end) field2_55,
                    (case when stat30 = 1 then field2 else 0 end) field2_30,
                    (case when stat45 = 1 then field2 else 0 end) field2_45,
                    (case when stat51 = 1 then field2 else 0 end) field2_51,
                    field3 field3_10,
                    (case when stat11 = 1 then field3 else 0 end) field3_11, 
                    (case when stat55 = 1 then field3 else 0 end) field3_55,
                    (case when stat30 = 1 then field3 else 0 end) field3_30,
                    (case when stat45 = 1 then field3 else 0 end) field3_45,
                    (case when stat51 = 1 then field3 else 0 end) field3_51,
                    field4 field4_10,
                    (case when stat11 = 1 then field4 else 0 end) field4_11, 
                    (case when stat55 = 1 then field4 else 0 end) field4_55,
                    (case when stat30 = 1 then field4 else 0 end) field4_30,
                    (case when stat45 = 1 then field4 else 0 end) field4_45,
                    (case when stat51 = 1 then field4 else 0 end) field4_51,
                    field5 field5_10,
                    (case when stat11 = 1 then field5 else 0 end) field5_11, 
                    (case when stat55 = 1 then field5 else 0 end) field5_55,
                    (case when stat30 = 1 then field5 else 0 end) field5_30,
                    (case when stat45 = 1 then field5 else 0 end) field5_45,
                    (case when stat51 = 1 then field5 else 0 end) field5_51,
                    field6 field6_10,
                    (case when stat11 = 1 then field6 else 0 end) field6_11, 
                    (case when stat55 = 1 then field6 else 0 end) field6_55,
                    (case when stat30 = 1 then field6 else 0 end) field6_30,
                    (case when stat45 = 1 then field6 else 0 end) field6_45,
                    (case when stat51 = 1 then field6 else 0 end) field6_51,
                    field7 field7_10,
                    (case when stat11 = 1 then field7 else 0 end) field7_11, 
                    (case when stat55 = 1 then field7 else 0 end) field7_55,
                    (case when stat30 = 1 then field7 else 0 end) field7_30,
                    (case when stat45 = 1 then field7 else 0 end) field7_45,
                    (case when stat51 = 1 then field7 else 0 end) field7_51,
                    field8 field8_10,
                    (case when stat11 = 1 then field8 else 0 end) field8_11, 
                    (case when stat55 = 1 then field8 else 0 end) field8_55,
                    (case when stat30 = 1 then field8 else 0 end) field8_30,
                    (case when stat45 = 1 then field8 else 0 end) field8_45,
                    (case when stat51 = 1 then field8 else 0 end) field8_51,
                    field9 field9_10,
                    (case when stat11 = 1 then field9 else 0 end) field9_11, 
                    (case when stat55 = 1 then field9 else 0 end) field9_55,
                    (case when stat30 = 1 then field9 else 0 end) field9_30,
                    (case when stat45 = 1 then field9 else 0 end) field9_45,
                    (case when stat51 = 1 then field9 else 0 end) field9_51,
                    field10 field10_10,
                    (case when stat11 = 1 then field10 else 0 end) field10_11, 
                    (case when stat55 = 1 then field10 else 0 end) field10_55,
                    (case when stat30 = 1 then field10 else 0 end) field10_30,
                    (case when stat45 = 1 then field10 else 0 end) field10_45,
                    (case when stat51 = 1 then field10 else 0 end) field10_51,
                    field11 field11_10,
                    (case when stat11 = 1 then field11 else 0 end) field11_11, 
                    (case when stat55 = 1 then field11 else 0 end) field11_55,
                    (case when stat30 = 1 then field11 else 0 end) field11_30,
                    (case when stat45 = 1 then field11 else 0 end) field11_45,
                    (case when stat51 = 1 then field11 else 0 end) field11_51,
                    field12 field12_10,
                    (case when stat11 = 1 then field12 else 0 end) field12_11, 
                    (case when stat55 = 1 then field12 else 0 end) field12_55,
                    (case when stat30 = 1 then field12 else 0 end) field12_30,
                    (case when stat45 = 1 then field12 else 0 end) field12_45,
                    (case when stat51 = 1 then field12 else 0 end) field12_51,
                    field13 field13_10,
                    (case when stat11 = 1 then field13 else 0 end) field13_11, 
                    (case when stat55 = 1 then field13 else 0 end) field13_55,
                    (case when stat30 = 1 then field13 else 0 end) field13_30,
                    (case when stat45 = 1 then field13 else 0 end) field13_45,
                    (case when stat51 = 1 then field13 else 0 end) field13_51,
                    field14 field14_10,
                    (case when stat11 = 1 then field14 else 0 end) field14_11, 
                    (case when stat55 = 1 then field14 else 0 end) field14_55,
                    (case when stat30 = 1 then field14 else 0 end) field14_30,
                    (case when stat45 = 1 then field14 else 0 end) field14_45,
                    (case when stat51 = 1 then field14 else 0 end) field14_51,
                    field15 field15_10,
                    (case when stat11 = 1 then field15 else 0 end) field15_11, 
                    (case when stat55 = 1 then field15 else 0 end) field15_55,
                    (case when stat30 = 1 then field15 else 0 end) field15_30,
                    (case when stat45 = 1 then field15 else 0 end) field15_45,
                    (case when stat51 = 1 then field15 else 0 end) field15_51,
                    field16 field16_10,
                    (case when stat11 = 1 then field16 else 0 end) field16_11, 
                    (case when stat55 = 1 then field16 else 0 end) field16_55,
                    (case when stat30 = 1 then field16 else 0 end) field16_30,
                    (case when stat45 = 1 then field16 else 0 end) field16_45,
                    (case when stat51 = 1 then field16 else 0 end) field16_51,
                    field17 field17_10,
                    (case when stat11 = 1 then field17 else 0 end) field17_11, 
                    (case when stat55 = 1 then field17 else 0 end) field17_55,
                    (case when stat30 = 1 then field17 else 0 end) field17_30,
                    (case when stat45 = 1 then field17 else 0 end) field17_45,
                    (case when stat51 = 1 then field17 else 0 end) field17_51,
                    field18 field18_10,
                    (case when stat11 = 1 then field18 else 0 end) field18_11, 
                    (case when stat55 = 1 then field18 else 0 end) field18_55,
                    (case when stat30 = 1 then field18 else 0 end) field18_30,
                    (case when stat45 = 1 then field18 else 0 end) field18_45,
                    (case when stat51 = 1 then field18 else 0 end) field18_51 
            from CohortStatus
        ) innerData on cohSt.cohortStatus = innerData.cohortStat
    group by cohSt.cohortStatus
    ) innerData2
where innerData2.cohortStatusall = 10 

union

--Part C: Pell recipients and recipients of a subsidized Direct Loan who did not receive a Pell Grant

select 'C' part,
        cast(cohStat.cohortStatus as string) line, --cohort status
        cast(coalesce((case when cohStat.cohortStatus = 10 then innerData2.isPellRec 
              when cohStat.cohortStatus = 11 then innerData2.isPellRec11
            else innerData2.isPellRec45 end), 0) as string) field1, --isPell,
        cast(coalesce((case when cohStat.cohortStatus = 10 then innerData2.isSubLoanRec
              when cohStat.cohortStatus = 11 then innerData2.isSubLoanRec11
            else innerData2.isSubLoanRec45 end), 0) as string) field2, --isSubLoan,
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
from FormatPartC cohStat
    cross join ( 
        select  cohSt.cohortStatus cohortStatusall,
                sum(stat10) stat10,
                sum(isPellRec) isPellRec,
                sum(isPellRec11) isPellRec11,
                sum(isPellRec45) isPellRec45,
                sum(isSubLoanRec) isSubLoanRec,
                sum(isSubLoanRec11) isSubLoanRec11,
                sum(isSubLoanRec45) isSubLoanRec45 
        from FormatPartC cohSt
            left join (
                select personId personId,
                        stat10 stat10,
                        10 cohortStat,
                        isPellRec isPellRec,
                        isSubLoanRec isSubLoanRec,
                        (case when stat11 = 1 then isPellRec else 0 end) isPellRec11,
                        (case when stat45 = 1 then isPellRec else 0 end) isPellRec45,
                        (case when stat11 = 1 then isSubLoanRec else 0 end) isSubLoanRec11,
                        (case when stat45 = 1 then isSubLoanRec else 0 end) isSubLoanRec45
                from CohortStatus
            ) innerData on cohSt.cohortStatus = innerData.cohortStat
        group by cohSt.cohortStatus
    ) innerData2
where innerData2.cohortStatusall = 10

order by 1, 2
