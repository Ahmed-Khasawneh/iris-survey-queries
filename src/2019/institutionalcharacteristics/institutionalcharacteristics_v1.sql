/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Fall Collection
FILE NAME:      Institutional Characteristics
FILE DESC:      IC for all institutions
AUTHOR:         Janet Hanicak
CREATED:        20200608

SECTIONS:
Most Recent Records
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)      	Author             	Tag             	Comments
-----------------   --------------------	-------------   	-------------------------------------------------
20200629            jhanicak                                    Added ClientConfigMCR to pull award level indicators from IPEDSClientConfig
                                                                Changed InstitCharUndergradGrad.academicYear to surveyCollectionYear in UGGRLatestRecord
                                                                Changed InstitCharDoctorate.academicYear to surveyCollectionYear in DRLatestRecord
                                                                Changed InstitCharUndergradGrad.provideMealPlans value 'Yes - fixed meal plan' to 'Yes - number in max plan' PF-1536 (Run time
								47s)
20200608            jhanicak          			    		    Initial version (Run time 53s)
	
********************/

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required.
*****/

WITH DefaultValues as (

select '1920' surveyYear,
        false diffTuitionAmounts,
        false institControlledHousing,
        'No' provideMealPlans,
        false ftftDegreeLiveOnCampus,
        'Y' icOfferUndergradAwardLevel,
        'Y' icOfferGraduateAwardLevel,
        'Y' icOfferDoctorAwardLevel
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

select surveyYear surveyYear,
	   icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
       icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
       icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
       diffTuitionAmounts diffTuitionAmounts,
       institControlledHousing institControlledHousing,
       ftftDegreeLiveOnCampus ftftDegreeLiveOnCampus,
       provideMealPlans provideMealPlans
from (
        select config.surveyCollectionYear surveyYear,
			coalesce(config.icOfferUndergradAwardLevel, defValues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
            coalesce(config.icOfferGraduateAwardLevel, defValues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
            coalesce(config.icOfferDoctorAwardLevel, defValues.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,
            defValues.diffTuitionAmounts diffTuitionAmounts,
            defValues.institControlledHousing institControlledHousing,
            defValues.ftftDegreeLiveOnCampus ftftDegreeLiveOnCampus,
            defValues.provideMealPlans provideMealPlans,
            row_number() over (
                partition by
                    config.surveyCollectionYear
                order by
                    config.recordActivityDate desc
                ) configRn
		from IPEDSClientConfig config
			cross join DefaultValues defValues
		where config.surveyCollectionYear = defValues.surveyYear 

    union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select  defVal.surveyYear surveyYear,
			defVal.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
            defVal.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
            defVal.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
            defVal.diffTuitionAmounts diffTuitionAmounts,
            defVal.institControlledHousing institControlledHousing,
            defVal.ftftDegreeLiveOnCampus ftftDegreeLiveOnCampus,
            defVal.provideMealPlans provideMealPlans,
			1 configRn
    from DefaultValues defVal
    where defVal.surveyYear not in (select config.surveyCollectionYear
                                      from IPEDSClientConfig config
                                     where config.surveyCollectionYear = defVal.surveyYear)
    	)
where configRn = 1	
),

UGGRLatestRecord as (

select *
from (
    select clientconfig.icOfferUndergradAwardLevel offerUG,
        clientconfig.icOfferGraduateAwardLevel offerGR,
        coalesce(UGGR.diffTuitionAmounts, clientconfig.diffTuitionAmounts) diffTuitionAmounts,
	    coalesce(UGGR.institControlledHousing, clientconfig.institControlledHousing) institControlledHousing,
	    coalesce(UGGR.ftftDegreeLiveOnCampus, clientconfig.ftftDegreeLiveOnCampus) ftftDegreeLiveOnCampus,
	    UGGR.housingCapacity housingCapacity,
	    coalesce(UGGR.provideMealPlans, clientconfig.provideMealPlans) provideMealPlans,
	    UGGR.mealsPerWeek mealsPerWeek,
	    UGGR.ugApplicationFee ugApplicationFee,
	    UGGR.grApplicationFee grApplicationFee,
	    UGGR.ugFTAvgTuitionDist ugFTAvgTuitionDist,
	    UGGR.ugFTAvgTuitionInSt ugFTAvgTuitionInSt,
	    UGGR.ugFTAvgTuitionOutSt ugFTAvgTuitionOutSt,
	    UGGR.ugFTReqFeesDist ugFTReqFeesDist,
	    UGGR.ugFTReqFeesInSt ugFTReqFeesInSt,
	    UGGR.ugFTReqFeesOutSt ugFTReqFeesOutSt,
	    UGGR.ugPTCredHrChargeDist ugPTCredHrChargeDist,
	    UGGR.ugPTCredHrChargeInSt ugPTCredHrChargeInSt,
	    UGGR.ugPTCredHrChargeOutSt ugPTCredHrChargeOutSt,
	    UGGR.ugFTCompFeeDist ugFTCompFeeDist,
	    UGGR.ugFTCompFeeInSt ugFTCompFeeInSt,
	    UGGR.ugFTCompFeeOutSt ugFTCompFeeOutSt,
	    UGGR.grFTAvgTuitionDist grFTAvgTuitionDist,
	    UGGR.grFTAvgTuitionInSt grFTAvgTuitionInSt,
	    UGGR.grFTAvgTuitionOutSt grFTAvgTuitionOutSt,
	    UGGR.grFTReqFeesDist grFTReqFeesDist,
	    UGGR.grFTReqFeesInSt grFTReqFeesInSt,
	    UGGR.grFTReqFeesOutSt grFTReqFeesOutSt,
	    UGGR.grPTCredHrChargeDist grPTCredHrChargeDist,
	    UGGR.grPTCredHrChargeInSt grPTCredHrChargeInSt,
	    UGGR.grPTCredHrChargeOutSt grPTCredHrChargeOutSt,
	    UGGR.typicalRoomChg typicalRoomChg,
	    UGGR.typicalBoardChg typicalBoardChg,
	    UGGR.typicalCombRoomBoardChg typicalCombRoomBoardChg,
	    UGGR.ugTuitionDist ugTuitionDist,
	    UGGR.ugTuitionGuarDist ugTuitionGuarDist,
	    UGGR.ugTuitionGuarIncrPercDist ugTuitionGuarIncrPercDist,
	    UGGR.ugFeeDist ugFeeDist,
	    UGGR.ugFeeGuarDist ugFeeGuarDist,
	    UGGR.ugFeeGuarIncrPercDist ugFeeGuarIncrPercDist,
	    UGGR.ugTuitionInSt ugTuitionInSt,
	    UGGR.ugTuitionGuarInSt ugTuitionGuarInSt,
	    UGGR.ugTuitionGuarIncrPercInSt ugTuitionGuarIncrPercInSt,
	    UGGR.ugFeeInSt ugFeeInSt,
	    UGGR.ugFeeGuarInSt ugFeeGuarInSt,
	    UGGR.ugFeeGuarIncrPercInSt ugFeeGuarIncrPercInSt,
	    UGGR.ugTuitionOutSt ugTuitionOutSt,
	    UGGR.ugTuitionGuarOutSt ugTuitionGuarOutSt,
	    UGGR.ugTuitionGuarIncrPercOutSt ugTuitionGuarIncrPercOutSt,
	    UGGR.ugFeeOutSt ugFeeOutSt,
	    UGGR.ugFeeGuarOutSt ugFeeGuarOutSt,
	    UGGR.ugFeeGuarIncrPercOutSt ugFeeGuarIncrPercOutSt,
	    UGGR.ugCompFeeDist ugCompFeeDist,
	    UGGR.ugCompFeeGuarDist ugCompFeeGuarDist,
	    UGGR.ugCompFeeGuarIncrPercDist ugCompFeeGuarIncrPercDist,
	    UGGR.ugCompFeeInSt ugCompFeeInSt,
	    UGGR.ugCompFeeGuarInSt ugCompFeeGuarInSt,
	    UGGR.ugCompFeeGuarIncrPercInSt ugCompFeeGuarIncrPercInSt,
	    UGGR.ugCompFeeOutSt ugCompFeeOutSt,
	    UGGR.ugCompFeeGuarOutSt ugCompFeeGuarOutSt,
	    UGGR.ugCompFeeGuarIncrPercOutSt ugCompFeeGuarIncrPercOutSt,
	    UGGR.ugBooksAndSupplies ugBooksAndSupplies,
	    UGGR.ugRoomBoardOnCampus ugRoomBoardOnCampus,
	    UGGR.ugExpensesOnCampus ugExpensesOnCampus,
	    UGGR.ugRoomBoardOffCampus ugRoomBoardOffCampus,
	    UGGR.ugExpensesOffCampus ugExpensesOffCampus,
	    UGGR.ugExpensesOffCampusFamily ugExpensesOffCampusFamily,
        row_number() over (
			partition by 
				UGGR.surveyCollectionYear
			order by  
				UGGR.recordActivityDate desc
		) UGGRRn
    from InstitCharUndergradGrad UGGR
        cross join ClientConfigMCR clientconfig on UGGR.surveyCollectionYear = clientconfig.surveyYear

union

--create a record with required values if no data returned
select clientconfig.icOfferUndergradAwardLevel offerUG,
        clientconfig.icOfferGraduateAwardLevel offerGR,
        clientconfig.diffTuitionAmounts diffTuitionAmounts,
	    clientconfig.institControlledHousing institControlledHousing,
	    clientconfig.ftftDegreeLiveOnCampus ftftDegreeLiveOnCampus,
	    null housingCapacity,
	    clientconfig.provideMealPlans provideMealPlans,
        null mealsPerWeek,
        null ugApplicationFee,
        null grApplicationFee,
        null ugFTAvgTuitionDist,
        null ugFTAvgTuitionInSt,
        null ugFTAvgTuitionOutSt,
        null ugFTReqFeesDist,
        null ugFTReqFeesInSt,
        null ugFTReqFeesOutSt,
        null ugPTCredHrChargeDist,
        null ugPTCredHrChargeInSt,
        null ugPTCredHrChargeOutSt,
        null ugFTCompFeeDist,
        null ugFTCompFeeInSt,
        null ugFTCompFeeOutSt,
        null grFTAvgTuitionDist,
        null grFTAvgTuitionInSt,
        null grFTAvgTuitionOutSt,
        null grFTReqFeesDist,
        null grFTReqFeesInSt,
        null grFTReqFeesOutSt,
        null grPTCredHrChargeDist,
        null grPTCredHrChargeInSt,
        null grPTCredHrChargeOutSt,
        null typicalRoomChg,
        null typicalBoardChg,
        null typicalCombRoomBoardChg,
        null ugTuitionDist,
        null ugTuitionGuarDist,
        null ugTuitionGuarIncrPercDist,
        null ugFeeDist,
        null ugFeeGuarDist,
        null ugFeeGuarIncrPercDist,
        null ugTuitionInSt,
        null ugTuitionGuarInSt,
        null ugTuitionGuarIncrPercInSt,
        null ugFeeInSt,
        null ugFeeGuarInSt,
        null ugFeeGuarIncrPercInSt,
        null ugTuitionOutSt,
        null ugTuitionGuarOutSt,
        null ugTuitionGuarIncrPercOutSt,
        null ugFeeOutSt,
        null ugFeeGuarOutSt,
        null ugFeeGuarIncrPercOutSt,
        null ugCompFeeDist,
        null ugCompFeeGuarDist,
        null ugCompFeeGuarIncrPercDist,
        null ugCompFeeInSt,
        null ugCompFeeGuarInSt,
        null ugCompFeeGuarIncrPercInSt,
        null ugCompFeeOutSt,
        null ugCompFeeGuarOutSt,
        null ugCompFeeGuarIncrPercOutSt,
        null ugBooksAndSupplies,
        null ugRoomBoardOnCampus,
        null ugExpensesOnCampus,
        null ugRoomBoardOffCampus,
        null ugExpensesOffCampus,
        null ugExpensesOffCampusFamily,
        1
    from ClientConfigMCR clientconfig
    where clientconfig.surveyYear not in (select UGGR.surveyCollectionYear
                                          from InstitCharUndergradGrad UGGR
                                          where UGGR.surveyCollectionYear = clientconfig.surveyYear)
    )
where UGGRRn = 1
and (offerUG = 'Y'
or offerGR = 'Y')
),

DRLatestRecord as (

select *
from (
    select clientconfig.icOfferDoctorAwardLevel offerDR,
            nvl((select diffTuitionAmounts
            from UGGRLatestRecord), clientconfig.diffTuitionAmounts) diffTuitionAmounts,
           DR.chiroAvgTuitionInSt chiroAvgTuitionInSt,
           DR.chiroReqFeesInSt chiroReqFeesInSt,
           DR.chiroAvgTuitionOutSt chiroAvgTuitionOutSt,
           DR.chiroReqFeesOutSt chiroReqFeesOutSt,
           DR.dentAvgTuitionInSt dentAvgTuitionInSt,
           DR.dentReqFeesInSt dentReqFeesInSt,
           DR.dentAvgTuitionOutSt dentAvgTuitionOutSt,
           DR.dentReqFeesOutSt dentReqFeesOutSt,
           DR.medAvgTuitionInSt medAvgTuitionInSt,
           DR.medReqFeesInSt medReqFeesInSt,
           DR.medAvgTuitionOutSt medAvgTuitionOutSt,
           DR.medReqFeesOutSt medReqFeesOutSt,
           DR.optomAvgTuitionInSt optomAvgTuitionInSt,
           DR.optomReqFeesInSt optomReqFeesInSt,
           DR.optomAvgTuitionOutSt optomAvgTuitionOutSt,
           DR.optomReqFeesOutSt optomReqFeesOutSt,
           DR.osteoAvgTuitionInSt osteoAvgTuitionInSt,
           DR.osteoReqFeesInSt osteoReqFeesInSt,
           DR.osteoAvgTuitionOutSt osteoAvgTuitionOutSt,
           DR.osteoReqFeesOutSt osteoReqFeesOutSt,
           DR.pharmAvgTuitionInSt pharmAvgTuitionInSt,
           DR.pharmReqFeesInSt pharmReqFeesInSt,
           DR.pharmAvgTuitionOutSt pharmAvgTuitionOutSt,
           DR.pharmReqFeesOutSt pharmReqFeesOutSt,
           DR.podiaAvgTuitionInSt podiaAvgTuitionInSt,
           DR.podiaReqFeesInSt podiaReqFeesInSt,
           DR.podiaAvgTuitionOutSt podiaAvgTuitionOutSt,
           DR.podiaReqFeesOutSt podiaReqFeesOutSt,
           DR.vetAvgTuitionInSt vetAvgTuitionInSt,
           DR.vetReqFeesInSt vetReqFeesInSt,
           DR.vetAvgTuitionOutSt vetAvgTuitionOutSt,
           DR.vetReqFeesOutSt vetReqFeesOutSt,
           DR.lawAvgTuitionInSt lawAvgTuitionInSt,
           DR.lawReqFeesInSt lawReqFeesInSt,
           DR.lawAvgTuitionOutSt lawAvgTuitionOutSt,
           DR.lawReqFeesOutSt lawReqFeesOutSt,
           row_number() over (
			partition by 
				DR.surveyCollectionYear
			order by  
				DR.recordActivityDate desc
		) DRRn
    from InstitCharDoctorate DR
        cross join ClientConfigMCR clientconfig on DR.surveyCollectionYear = clientconfig.surveyYear
    
union

--create a record with required values if no data returned
select clientconfig.icOfferDoctorAwardLevel offerDR,
        clientconfig.diffTuitionAmounts diffTuitionAmounts,
        null chiroAvgTuitionInSt,
        null chiroReqFeesInSt,
        null chiroAvgTuitionOutSt,
        null chiroReqFeesOutSt,
        null dentAvgTuitionInSt,
        null dentReqFeesInSt,
        null dentAvgTuitionOutSt,
        null dentReqFeesOutSt,
        null medAvgTuitionInSt,
        null medReqFeesInSt,
        null medAvgTuitionOutSt,
        null medReqFeesOutSt,
        null optomAvgTuitionInSt,
        null optomReqFeesInSt,
        null optomAvgTuitionOutSt,
        null optomReqFeesOutSt,
        null osteoAvgTuitionInSt,
        null osteoReqFeesInSt,
        null osteoAvgTuitionOutSt,
        null osteoReqFeesOutSt,
        null pharmAvgTuitionInSt,
        null pharmReqFeesInSt,
        null pharmAvgTuitionOutSt,
        null pharmReqFeesOutSt,
        null podiaAvgTuitionInSt,
        null podiaReqFeesInSt,
        null podiaAvgTuitionOutSt,
        null podiaReqFeesOutSt,
        null vetAvgTuitionInSt,
        null vetReqFeesInSt,
        null vetAvgTuitionOutSt,
        null vetReqFeesOutSt,
        null lawAvgTuitionInSt,
        null lawReqFeesInSt,
        null lawAvgTuitionOutSt,
        null lawReqFeesOutSt,
        1
from ClientConfigMCR clientconfig
where clientconfig.surveyYear not in (select DRR.surveyCollectionYear
                                       from InstitCharDoctorate DRR
                                      where DRR.surveyCollectionYear = clientconfig.surveyYear)
)
where DRRn = 1
and offerDR = 'Y'
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs.
*****/

/*
Part A - General information and application fees
*/

select 'A'                                                                      part,   --PART - "A"
       ugApplicationFee                                                         field1, --DA03 0 to 999999, -2 or blank if no full-time undergraduates, or no undergraduate award levels offered, or no application fee for admission required.
       grApplicationFee                                                         field2, --DA04 0 to 999999, -2 or blank if no full-time graduates, or no graduate award levels offered, or no application fee for admission required.
       case when diffTuitionAmounts = 1 then '01' 
                    else '02' end                                               field3, --DA07 01 = Yes, 02 = No
       case when institControlledHousing = 1 then '01' 
                    else '02' end                                               field4, --DA08 01 = Yes, 02 = No
       case when institControlledHousing = 1 then housingCapacity 
            else null end                                                       field5, --DA09 If DA08=01 (Yes), then 1 to 999999. If DA08=02 (No), then -2 or blank (not applicable).
       case provideMealPlans
            when 'Yes - number in max plan' then '01'
            when 'Yes - number varies' then '02'
            else '03'
        end                                                                     field6, --DA10 01 = Yes, Number of meals per week in the maximum meal plan offered. 02 = Yes, Number of meals per week can vary 03 = No
       case
           when provideMealPlans = 'Yes - number in max plan' then mealsPerWeek
           else null 
           end                                                                  field7, --DA11 If DA10=01 (Yes), then 2 to 99. If unlimited, then 99. If DA10=02 (meals vary) or 03 (No), then -2 or blank (not applicable).
       coalesce(case when ftftDegreeLiveOnCampus = 1 then '01' else '02' end,
                '02')                                                           field8, --DA06 01 = Yes and we do not allow any exceptions, 02 = No
       null                                                                     field9,
       null                                                                     field10,
       null                                                                     field11,
       null                                                                     field12,
       null                                                                     field13,
       null                                                                     field14,
       null                                                                     field15,
       null                                                                     field16,
       null                                                                     field17,
       null                                                                     field18,
       null                                                                     field19,
       null                                                                     field20,
       null                                                                     field21,
       null                                                                     field22,
       null                                                                     field23,
       null                                                                     field24,
       null                                                                     field25,
       null                                                                     field26,
       null                                                                     field27,
       null                                                                     field28,
       null                                                                     field29,
       null                                                                     field30,
       null                                                                     field31,
       null                                                                     field32,
       null                                                                     field33
from UGGRLatestRecord

union

/* Part B - Undergraduate student charges

Instructions:
If institution does not offer undergraduate level awards, DB01-DB12 are not applicable, and no Part "B" record should be included on the file. 
If institution offers undergraduate level awards, but has only part-time students, then only DB07, DB08, and DB09 are applicable; all other variables should be blank or -2. 
Out-of-state amounts DB03, DB06, DB09, and DB12 should be consistent with DA07 above. 
If institution charges different tuition for in-state and in-district students but not for out-of-state students, then out-of-state amounts should be -2 or blank (not applicable). 
If institution charges different tuition for in-state and out-of-state students but not for in-district students (locality), then in-district amounts should be -2 or blank (not applicable). 
If institution only charges one tuition rate for all students, then report these amounts using the in-district variables. 
If the institution has a single lump sum charge for tuition, required fees, and room and board, enter values in DB10, DB11, and DB12, and enter blank or -2 (not applicable) for DA02 through DA11.
*/

select 'B',                                                                       --PART - "B"
       ugFTAvgTuitionDist,                                                        --field1 DB01 Avg tuition, In-district 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else ugFTAvgTuitionInSt end,    --field2 DB02 Avg tuition, In-state 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else ugFTAvgTuitionOutSt end,   --field3 DB03 Avg tuition, Out-of-state 0 to 999999, -2 or blank = not applicable
       ugFTReqFeesDist,                                                           --field4 DB04 Required fees, In-district 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else ugFTReqFeesInSt end,       --field5 DB05 Required fees,In-state 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else ugFTReqFeesOutSt end,      --field6 DB06 Required fees, Out-of-state 0 to 999999, -2 or blank = not applicable
       ugPTCredHrChargeDist,                                                      --field7 DB07 Per credit hour charge , In-district 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else ugPTCredHrChargeInSt end,  --field8 DB08 Per credit hour charge , In-state 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else ugPTCredHrChargeOutSt end, --field9 DB09 Per credit hour charge , Out-of-state 0 to 999999, -2 or blank = not applicable
       ugFTCompFeeDist,                                                           --field10 DB10 Comprehensive fee, In-district 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else ugFTCompFeeInSt end,       --field11 DB11 Comprehensive fee, In-state 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else ugFTCompFeeOutSt end,      --field12 DB12 Comprehensive fee, Out-of-state 0 to 999999, -2 or blank = not applicable
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from UGGRLatestRecord
where offerUG = 'Y'

union

/*Part C - Graduate student charges 

Instructions:

If institution does not offer graduate level awards, DC01-DC09 are not applicable, and no Part "C" record should be included on the file. 
If institution offers graduate level awards, but has only part-time students, then only DC07, DC08, and DC09 are applicable; all other variables should be -2 or blank. 
Out-of-state amounts DC03, DC06, and DC09 should be consistent with DA07 above. 
If institution charges different tuition for in-state and in-district students but not for out-of-state students, then out-of-state amounts should be -2 or blank (not applicable). 
If institution charges different tuition for in-state and out-of-state students but not for in-district students (locality), then in-district amounts should be -2 or blank (not applicable). 
If institution only charges one tuition rate for all students, then report these amounts using the in-district variables.

Please, do not include tuition for Doctor's Degree - Professional Practice programs.
*/

select 'C',                                                                       --PART - "C"
       grFTAvgTuitionDist,                                                        --field1 DC01 Average tuition In-district amount 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else grFTAvgTuitionInSt end,    --field2 DC02 Average tuition In-State amount 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else grFTAvgTuitionOutSt end,   --field3 DC03 Average tuition Out-of-state amount 0 to 999999, -2 or blank = not applicable
       grFTReqFeesDist,                                                           --field4 DC04 Required fees In-district amount 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else grFTReqFeesInSt end,       --field5 DC05 Required fees In-State amount 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else grFTReqFeesOutSt end,      --field6 DC06 Required fees Out-of-state amount 0 to 999999, -2 or blank = not applicable
       grPTCredHrChargeDist,                                                      --field7 DC07 Per credit hour charge In-district amount 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else grPTCredHrChargeInSt end,  --field8 DC08 Per credit hour charge In-State amount 0 to 999999, -2 or blank = not applicable
       case when diffTuitionAmounts = 0 then null else grPTCredHrChargeOutSt end, --field9 DC09 Per credit hour charge Out-of-state amount 0 to 999999, -2 or blank = not applicable
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from UGGRLatestRecord
where offerGR = 'Y'

union

/*Part D - Room and Board charges 

Instructions:
Room and board charges should be consistent with DA08 and DA10 above. 
If institution does not provide on-campus housing or board or meal plans, no Part "D" record should be included on the file. 
If institution does provide room and board, but does not separate room and board charges, provide combined charge in DD03, and leave DD01 and DD02 as -2 or blank (not applicable).

This section is not required from non-degree-granting institutions.
*/

select 'D',                                                                     --PART - "D"
       case when institControlledHousing = 0 then null else typicalRoomChg end, --field1 DD01 Room Charge 0 to 999999, -2 or blank = not applicable
       case when provideMealPlans = 'No' then null else typicalBoardChg end,    --field2 DD02 Board Charge 0 to 999999, -2 or blank = not applicable
       case
           when provideMealPlans = 'No' or institControlledHousing = 0 then null
           else typicalCombRoomBoardChg end,                                    --field3 DD03 Combined Room and Board Charge 0 to 999999, -2 or blank = not applicable
       null, -- field4,
       null, -- field5,
       null, -- field6,
       null, -- field7,
       null, -- field8,
       null, -- field9,
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from UGGRLatestRecord
where provideMealPlans != 'No'
   or institControlledHousing = 1

union

/*Part E - Cost of attendance for full-time, first-time degree/certificate-seeking undergraduate students 

Instructions:
These are the data that will be available to the public on the College Navigator website. 
If institution does not offer undergraduate level awards or does not have any first-time full-time students, DE011 through DE12 are not applicable, and no Part "E" record should be included on the file. 
Out-of-state amount DE03 and DE06 should be consistent with DA07 above. 
If institution charges different tuition for in-state and in-district students but not for out-of-state students, then out-of-state amounts should be -2 or blank (not applicable). 
If institution charges different tuition for in-state and out-of-state students but not for in-district students (locality), then in-district amounts should be -2 or blank (not applicable). 
If institution only charges one tuition rate for all students, then report these amounts using the in-district variables. 
If institution does not provide on-campus housing and board or meal plans, then DE08 should be -2 or blank (not applicable). 
If you want to change data reported for academic year 2017-18 or 2018-19, you must do it on the Student Financial Aid component in the Winter collection. 
If you would like to enter a caveat for these data, you must also enter the caveat in the data collection system.
*/

select 'E',                                                                                              --PART - "E"
       ugTuitionDist,                                                                                    --field1 DE011 Published tuition In-district 0 to 999999, -2 or blank = not applicable
       coalesce(case when ugTuitionGuarDist = 1 then '1' else '0' end, '0'),                             --field2 TUITGQ01 Do you have tuition guarantee In-district 0 = No, 1 = Yes
       coalesce(ugTuitionGuarIncrPercDist, '0'),                                                         --field3 TUITGP01 Guaranteed tuition increase In-district 0 to 100
       ugFeeDist,                                                                                        --field4 DE012 Published fees In-district 0 to 999999, -2 or blank = not applicable
       coalesce(case when ugFeeGuarDist = 1 then '1' else '0' end, '0'),                                 --field5 FEEGQ01 Do you have fee guarantee In-district 0 = No, 1 = Yes
       coalesce(ugFeeGuarIncrPercDist, '0'),                                                             --field6 FEEGP01 Guaranteed fee increase In-district 0 to 100

       case when diffTuitionAmounts = 0 then null else ugTuitionInSt end,                                --field7 DE021 Published tuition In-state 0 to 999999, -2 or blank = not applicable
       coalesce(case when diffTuitionAmounts = 1 and ugTuitionGuarInSt = 1 then '1' else '0' end, '0'),  --field8 TUITGQ02 Do you have tuition guarantee In-state 0 = No, 1 = Yes
       coalesce(case when diffTuitionAmounts = 0 then '0' else ugTuitionGuarIncrPercInSt end, '0'),      --field9 TUITGP02 Guaranteed tuition increase In-state 0 to 100
       case when diffTuitionAmounts = 0 then null else ugFeeInSt end,                                    --field10 DE022 Published fees In-state 0 to 999999, -2 or blank = not applicable
       coalesce(case when diffTuitionAmounts = 1 and ugFeeGuarInSt = 1 then '1' else '0' end, '0'),      --field11 FEEGQ02 Do you have fee guarantee In-state 0 = No, 1 = Yes
       coalesce(case when diffTuitionAmounts = 0 then '0' else ugFeeGuarIncrPercInSt end, '0'),          --field12 FEEGP02 Guaranteed fee increase In-state 0 to 100

       case when diffTuitionAmounts = 0 then null else ugTuitionOutSt end,                               --field13 DE031 Published tuition Out-of-state 0 to 999999, -2 or blank = not applicable
       coalesce(case when diffTuitionAmounts = 1 and ugTuitionGuarOutSt = 1 then '1' else '0' end, '0'), --field14 TUITGQ03 Do you have tuition guarantee Out-of-state 0 = No, 1 = Yes
       coalesce(case when diffTuitionAmounts = 0 then '0' else ugTuitionGuarIncrPercOutSt end, '0'),     --field15 TUITGP03 Guaranteed tuition increase Out-of-state 0 to 100
       case when diffTuitionAmounts = 0 then null else ugFeeOutSt end,                                   --field16 DE032 Published fees Out-of-state 0 to 999999, -2 or blank = not applicable
       coalesce(case when diffTuitionAmounts = 1 and ugFeeGuarOutSt = 1 then '1' else '0' end, '0'),     --field17 FEEGQ03 Do you have fee guarantee Out-of-state 0 = No, 1 = Yes
       coalesce(case when diffTuitionAmounts = 0 then '0' else ugFeeGuarIncrPercOutSt end, '0'),         --field18 FEEGP03 Guaranteed fee increase Out-of-state 0 to 100

       ugCompFeeDist,                                                                                    --field19 DE04 Comprehensive fee In-district 0 to 999999, -2 or blank = not applicable
       coalesce(case when ugCompFeeGuarDist = 1 then '1' else '0' end, '0'),                             --field20 COMPFGQ04 Do you have fee guarantee In-district 0 = No, 1 = Yes
       coalesce(ugCompFeeGuarIncrPercDist, '0'),                                                         --field21 COMPFGP04 Guaranteed fee increase In-district 0 to 100

       case when diffTuitionAmounts = 0 then null else ugCompFeeInSt end,                                --field22 DE05 Comprehensive fee In-state 0 to 999999, -2 or blank = not applicable
       coalesce(case when diffTuitionAmounts = 1 and ugCompFeeGuarInSt = 1 then '1' else '0' end,
                '0'),                                                                                    --field23 COMPFGQ05 Do you have fee guarantee In-state 0 = No, 1 = Yes
       coalesce(case when diffTuitionAmounts = 0 then '0' else ugCompFeeGuarIncrPercInSt end,
                '0'),                                                                                    --field24 COMPFGP05 Guaranteed fee increase In-state 0 to 100

       case when diffTuitionAmounts = 0 then null else ugCompFeeOutSt end,                               --field25 DE06 Comprehensive fee Out-of-state 0 to 999999, -2 or blank = not applicable
       coalesce(case when diffTuitionAmounts = 1 and ugCompFeeGuarOutSt = 1 then '1' else '0' end,
                '0'),                                                                                    --field26 COMPFGQ06 Do you have fee guarantee Out-of-state 0 = No, 1 = Yes
       coalesce(case when diffTuitionAmounts = 0 then '0' else ugCompFeeGuarIncrPercOutSt end,
                '0'),                                                                                    --field27 COMPFGP06 Guaranteed fee increase Out-of-state 0 to 100

       ugBooksAndSupplies,                                                                               --field28 DE07 Books and supplies 0 to 999999, -2 or blank = not applicable
       case
           when provideMealPlans != 'No' and institControlledHousing = 1 then ugRoomBoardOnCampus
           else null end,                                                                                --field29 DE08 On campus room and board 0 to 999999, -2 or blank = not applicable
       ugExpensesOnCampus,                                                                               --field30 DE09 On campus other expenses 0 to 999999, -2 or blank = not applicable
       ugRoomBoardOffCampus,                                                                             --field31 DE10 Off campus (not with family) Room and board 0 to 999999, -2 or blank = not applicable
       ugExpensesOffCampus,                                                                              --field32 DE11 Off campus (not with family) Other expenses 0 to 999999, -2 or blank = not applicable
       ugExpensesOffCampusFamily                                                                         --field33 DE12 Off campus (with family) Other expenses 0 to 999999, -2 or blank = not applicable
from UGGRLatestRecord
where offerUG = 'Y'

union

/*Part F - Doctor's-Professional Practice student charges 

Instructions:
This section is applicable only to 4-year schools that offer Doctor's-Professional Practice degrees. 
If your institution does not offer this level of award, do not include any Part "F" records in the file. 
If institution does not charge different tuition for out-of-state students, then valid amounts for DF04 and DF05 are -2 or blank (not applicable).

1. Chiropractic (D.C. or D.C.M.)
2. Dentistry (D.D.S. or D.M.D.)
3. Medicine (M.D.)
4. Optometry (O.D.)
5. Osteopathic Medicine (D.O.)
6. Pharmacy (Pharm. D.)
7. Podiatry (Pod.D., D.P., or D.P.M.)
8. Veterinary Medicine (D.V.M.)
9. Law ( J.D.)
*/

--Part F repeats through the Doctor's-professional program code list using a series of unions on valid "DF01" codes. 
select 'F',                                              --PART - "E"
       '1',                                              --field1 DF01 Chiropractic Doctor's professional program code values 1 - 9
       coalesce(chiroAvgTuitionInSt, '0'),               --field2 DF02 Average tuition, In-state amount 0 to 999999
       coalesce(chiroReqFeesInSt, '0'),                  --field3 DF03 Average tuition, In-state amount 0 to 999999
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(chiroAvgTuitionOutSt, '0') end, --field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(chiroReqFeesOutSt, '0') end,    --field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       null, -- field6,
       null, -- field7,
       null, -- field8,
       null, -- field9,
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from DRLatestRecord
where offerDR = 'Y'

union

select 'F',                                             --PART - "F"
       '2',                                             --field1 DF01 Dentistry Doctor's professional program code
       coalesce(dentAvgTuitionInSt, '0'),               --field2 DF02 Average tuition, In-state amount 0 to 999999
       coalesce(dentReqFeesInSt, '0'),                  --field3 DF03 Average tuition, In-state amount 0 to 999999
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(dentAvgTuitionOutSt, '0') end, --field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(dentReqFeesOutSt, '0') end,    --field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       null, -- field6,
       null, -- field7,
       null, -- field8,
       null, -- field9,
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from DRLatestRecord
where offerDR = 'Y'

union

select 'F',
       '3',                                            --field1 DF01 Medicine Doctor's professional program code
       coalesce(medAvgTuitionInSt, '0'),               --field2 DF02 Average tuition, In-state amount 0 to 999999
       coalesce(medReqFeesInSt, '0'),                  --field3 DF03 Average tuition, In-state amount 0 to 999999
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(medAvgTuitionOutSt, '0') end, --field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(medReqFeesOutSt, '0') end,    --field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       null, -- field6,
       null, -- field7,
       null, -- field8,
       null, -- field9,
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from DRLatestRecord
where offerDR = 'Y'

union

select 'F',
       '4',                                              --field1 DF01 Optometry Doctor's professional program code
       coalesce(optomAvgTuitionInSt, '0'),               --field2 DF02 Average tuition, In-state amount 0 to 999999
       coalesce(optomReqFeesInSt, '0'),                  --field3 DF03 Average tuition, In-state amount 0 to 999999
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(optomAvgTuitionOutSt, '0') end, --field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(optomReqFeesOutSt, '0') end,    --field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       null, -- field6,
       null, -- field7,
       null, -- field8,
       null, -- field9,
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from DRLatestRecord
where offerDR = 'Y'

union

select 'F',
       '5',                                              --field1 DF01 Osteopathic Medicine Doctor's professional program code
       coalesce(osteoAvgTuitionInSt, '0'),               --field2 DF02 Average tuition, In-state amount 0 to 999999
       coalesce(osteoReqFeesInSt, '0'),                  --field3 DF03 Average tuition, In-state amount 0 to 999999
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(osteoAvgTuitionOutSt, '0') end, --field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(osteoReqFeesOutSt, '0') end,    --field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       null, -- field6,
       null, -- field7,
       null, -- field8,
       null, -- field9,
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from DRLatestRecord
where offerDR = 'Y'

union

select 'F',
       '6',                                              --field1 DF01 Pharmacy Doctor's professional program code
       coalesce(pharmAvgTuitionInSt, '0'),               --field2 DF02 Average tuition, In-state amount 0 to 999999
       coalesce(pharmReqFeesInSt, '0'),                  --field3 DF03 Average tuition, In-state amount 0 to 999999
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(pharmAvgTuitionOutSt, '0') end, --field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(pharmReqFeesOutSt, '0') end,    --field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       null, -- field6,
       null, -- field7,
       null, -- field8,
       null, -- field9,
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from DRLatestRecord
where offerDR = 'Y'

union

select 'F',
       '7',                                              --field1 DF01 Podiatry Doctor's professional program code
       coalesce(podiaAvgTuitionInSt, '0'),               --field2 DF02 Average tuition, In-state amount 0 to 999999
       coalesce(podiaReqFeesInSt, '0'),                  --field3 DF03 Average tuition, In-state amount 0 to 999999
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(podiaAvgTuitionOutSt, '0') end, --field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(podiaReqFeesOutSt, '0') end,    --field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       null, -- field6,
       null, -- field7,
       null, -- field8,
       null, -- field9,
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from DRLatestRecord
where offerDR = 'Y'

union

select 'F',
       '8',                                            --field1 DF01 Veterinary Medicine Doctor's professional program code
       coalesce(vetAvgTuitionInSt, '0'),               --field2 DF02 Average tuition, In-state amount 0 to 999999
       coalesce(vetReqFeesInSt, '0'),                  --field3 DF03 Average tuition, In-state amount 0 to 999999
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(vetAvgTuitionOutSt, '0') end, --field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(vetReqFeesOutSt, '0') end,    --field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       null, -- field6,
       null, -- field7,
       null, -- field8,
       null, -- field9,
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from DRLatestRecord
where offerDR = 'Y'

union

select 'F',
       '9',                                            --field1 DF01 Law Doctor's professional program code
       coalesce(lawAvgTuitionInSt, '0'),               --field2 DF02 Average tuition, In-state amount 0 to 999999
       coalesce(lawReqFeesInSt, '0'),                  --field3 DF03 Average tuition, In-state amount 0 to 999999
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(lawAvgTuitionOutSt, '0') end, --field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       case
           when diffTuitionAmounts = 0 then null
           else coalesce(lawReqFeesOutSt, '0') end,    --field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
       null, -- field6,
       null, -- field7,
       null, -- field8,
       null, -- field9,
       null, -- field10,
       null, -- field11,
       null, -- field12,
       null, -- field13,
       null, -- field14,
       null, -- field15,
       null, -- field16,
       null, -- field17,
       null, -- field18,
       null, -- field19,
       null, -- field20,
       null, -- field21,
       null, -- field22,
       null, -- field23,
       null, -- field24,
       null, -- field25,
       null, -- field26,
       null, -- field27,
       null, -- field28,
       null, -- field29,
       null, -- field30,
       null, -- field31,
       null, -- field32,
       null  -- field33
from DRLatestRecord
where offerDR = 'Y'
