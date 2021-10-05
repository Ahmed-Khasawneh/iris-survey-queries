/********************

EVI PRODUCT:    DORIS 2021-22 IPEDS Survey Fall Collection
FILE NAME:      Institutional Characteristics
FILE DESC:      IC for all institutions
AUTHOR:         akhasawneh
CREATED:        20210907

SECTIONS:
Most Recent Records
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)      	Author             	Tag             	Comments
-----------------   --------------------	-------------   	-------------------------------------------------
20210907            akhasawneh          			    		Initial version (Run time 1m 33s)
	
********************/

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required.
*****/

WITH DefaultValues as (

select '2122' surveyYear,
	false diffTuitionAmounts,
	false institControlledHousing,
	'NO' provideMealPlans,
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
	select clientconfigENT.surveyCollectionYear surveyYear,
		coalesce(clientconfigENT.icOfferUndergradAwardLevel, defvalues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
		coalesce(clientconfigENT.icOfferGraduateAwardLevel, defvalues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
		coalesce(clientconfigENT.icOfferDoctorAwardLevel, defvalues.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,
		defvalues.diffTuitionAmounts diffTuitionAmounts,
		defvalues.institControlledHousing institControlledHousing,
		defvalues.ftftDegreeLiveOnCampus ftftDegreeLiveOnCampus,
		defvalues.provideMealPlans provideMealPlans,
		row_number() over (
			partition by
				clientconfigENT.surveyCollectionYear
			order by
				clientconfigENT.snapshotDate desc,
				clientconfigENT.recordActivityDate desc
			) configRn
	from IPEDSClientConfig clientconfigENT
		cross join DefaultValues defvalues
	where clientconfigENT.surveyCollectionYear = defvalues.surveyYear 
		and  array_contains(clientconfigENT.tags, 'IC Report Start')

    union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select  defvalues.surveyYear surveyYear,
		defvalues.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		defvalues.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
		defvalues.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
		defvalues.diffTuitionAmounts diffTuitionAmounts,
		defvalues.institControlledHousing institControlledHousing,
		defvalues.ftftDegreeLiveOnCampus ftftDegreeLiveOnCampus,
		defvalues.provideMealPlans provideMealPlans,
		1 configRn
    from DefaultValues defvalues
    where defvalues.surveyYear not in (select clientconfigENT1.surveyCollectionYear
                                       from IPEDSClientConfig clientconfigENT1
                                       where clientconfigENT1.surveyCollectionYear = defvalues.surveyYear
									   and array_contains(clientconfigENT1.tags, 'IC Report Start')
									   and clientconfigENT1.snapshotDate = (select max(clientconfigENT2.snapshotDate)
																		    from IPEDSClientConfig clientconfigENT2
																		    where clientconfigENT2.surveyCollectionYear = clientconfigENT1.surveyCollectionYear
																		    and  array_contains(clientconfigENT2.tags, 'IC Report Start')))
    	)
where configRn = 1	
),

InstitCharUndergradGradMCR as (

select *
from (
    select upper(clientconfig.icOfferUndergradAwardLevel) offerUG,
        upper(clientconfig.icOfferGraduateAwardLevel) offerGR,
        COALESCE(instcharuggrENT.diffTuitionAmounts, clientconfig.diffTuitionAmounts) diffTuitionAmounts,
	    COALESCE(instcharuggrENT.institControlledHousing, clientconfig.institControlledHousing) institControlledHousing,
	    COALESCE(instcharuggrENT.ftftDegreeLiveOnCampus, clientconfig.ftftDegreeLiveOnCampus) ftftDegreeLiveOnCampus,
	    instcharuggrENT.housingCapacity housingCapacity,
	    upper(COALESCE(instcharuggrENT.provideMealPlans, clientconfig.provideMealPlans)) provideMealPlans,
	    instcharuggrENT.mealsPerWeek mealsPerWeek,
	    instcharuggrENT.ugApplicationFee ugApplicationFee,
	    instcharuggrENT.grApplicationFee grApplicationFee,
	    instcharuggrENT.ugFTAvgTuitionDist ugFTAvgTuitionDist,
	    instcharuggrENT.ugFTAvgTuitionInSt ugFTAvgTuitionInSt,
	    instcharuggrENT.ugFTAvgTuitionOutSt ugFTAvgTuitionOutSt,
	    instcharuggrENT.ugFTReqFeesDist ugFTReqFeesDist,
	    instcharuggrENT.ugFTReqFeesInSt ugFTReqFeesInSt,
	    instcharuggrENT.ugFTReqFeesOutSt ugFTReqFeesOutSt,
	    instcharuggrENT.ugPTCredHrChargeDist ugPTCredHrChargeDist,
	    instcharuggrENT.ugPTCredHrChargeInSt ugPTCredHrChargeInSt,
	    instcharuggrENT.ugPTCredHrChargeOutSt ugPTCredHrChargeOutSt,
	    instcharuggrENT.ugFTCompFeeDist ugFTCompFeeDist,
	    instcharuggrENT.ugFTCompFeeInSt ugFTCompFeeInSt,
	    instcharuggrENT.ugFTCompFeeOutSt ugFTCompFeeOutSt,
	    instcharuggrENT.grFTAvgTuitionDist grFTAvgTuitionDist,
	    instcharuggrENT.grFTAvgTuitionInSt grFTAvgTuitionInSt,
	    instcharuggrENT.grFTAvgTuitionOutSt grFTAvgTuitionOutSt,
	    instcharuggrENT.grFTReqFeesDist grFTReqFeesDist,
	    instcharuggrENT.grFTReqFeesInSt grFTReqFeesInSt,
	    instcharuggrENT.grFTReqFeesOutSt grFTReqFeesOutSt,
	    instcharuggrENT.grPTCredHrChargeDist grPTCredHrChargeDist,
	    instcharuggrENT.grPTCredHrChargeInSt grPTCredHrChargeInSt,
	    instcharuggrENT.grPTCredHrChargeOutSt grPTCredHrChargeOutSt,
	    instcharuggrENT.typicalRoomChg typicalRoomChg,
	    instcharuggrENT.typicalBoardChg typicalBoardChg,
	    instcharuggrENT.typicalCombRoomBoardChg typicalCombRoomBoardChg,
	    instcharuggrENT.ugTuitionDist ugTuitionDist,
	    instcharuggrENT.ugTuitionGuarDist ugTuitionGuarDist,
	    instcharuggrENT.ugTuitionGuarIncrPercDist ugTuitionGuarIncrPercDist,
	    instcharuggrENT.ugFeeDist ugFeeDist,
	    instcharuggrENT.ugFeeGuarDist ugFeeGuarDist,
	    instcharuggrENT.ugFeeGuarIncrPercDist ugFeeGuarIncrPercDist,
	    instcharuggrENT.ugTuitionInSt ugTuitionInSt,
	    instcharuggrENT.ugTuitionGuarInSt ugTuitionGuarInSt,
	    instcharuggrENT.ugTuitionGuarIncrPercInSt ugTuitionGuarIncrPercInSt,
	    instcharuggrENT.ugFeeInSt ugFeeInSt,
	    instcharuggrENT.ugFeeGuarInSt ugFeeGuarInSt,
	    instcharuggrENT.ugFeeGuarIncrPercInSt ugFeeGuarIncrPercInSt,
	    instcharuggrENT.ugTuitionOutSt ugTuitionOutSt,
	    instcharuggrENT.ugTuitionGuarOutSt ugTuitionGuarOutSt,
	    instcharuggrENT.ugTuitionGuarIncrPercOutSt ugTuitionGuarIncrPercOutSt,
	    instcharuggrENT.ugFeeOutSt ugFeeOutSt,
	    instcharuggrENT.ugFeeGuarOutSt ugFeeGuarOutSt,
	    instcharuggrENT.ugFeeGuarIncrPercOutSt ugFeeGuarIncrPercOutSt,
	    instcharuggrENT.ugCompFeeDist ugCompFeeDist,
	    instcharuggrENT.ugCompFeeGuarDist ugCompFeeGuarDist,
	    instcharuggrENT.ugCompFeeGuarIncrPercDist ugCompFeeGuarIncrPercDist,
	    instcharuggrENT.ugCompFeeInSt ugCompFeeInSt,
	    instcharuggrENT.ugCompFeeGuarInSt ugCompFeeGuarInSt,
	    instcharuggrENT.ugCompFeeGuarIncrPercInSt ugCompFeeGuarIncrPercInSt,
	    instcharuggrENT.ugCompFeeOutSt ugCompFeeOutSt,
	    instcharuggrENT.ugCompFeeGuarOutSt ugCompFeeGuarOutSt,
	    instcharuggrENT.ugCompFeeGuarIncrPercOutSt ugCompFeeGuarIncrPercOutSt,
	    instcharuggrENT.ugBooksAndSupplies ugBooksAndSupplies,
	    instcharuggrENT.ugRoomBoardOnCampus ugRoomBoardOnCampus,
	    instcharuggrENT.ugExpensesOnCampus ugExpensesOnCampus,
	    instcharuggrENT.ugRoomBoardOffCampus ugRoomBoardOffCampus,
	    instcharuggrENT.ugExpensesOffCampus ugExpensesOffCampus,
	    instcharuggrENT.ugExpensesOffCampusFamily ugExpensesOffCampusFamily,
        row_number() over (
			partition by 
				instcharuggrENT.surveyCollectionYear
			order by  
				instcharuggrENT.snapshotDate desc,
				instcharuggrENT.recordActivityDate desc
		) instcharuggrRn
    from InstitCharUndergradGrad instcharuggrENT
        cross join ClientConfigMCR clientconfig on instcharuggrENT.surveyCollectionYear = clientconfig.surveyYear

	union

	--create a record with required values if no data returned
	select upper(clientconfig.icOfferUndergradAwardLevel) offerUG,
		upper(clientconfig.icOfferGraduateAwardLevel) offerGR,
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
	where clientconfig.surveyYear not in (select instcharuggrENT1.surveyCollectionYear
										  from InstitCharUndergradGrad instcharuggrENT1
										  where instcharuggrENT1.surveyCollectionYear = clientconfig.surveyYear
										  and  array_contains(instcharuggrENT1.tags, 'IC Report Start')
										  and instcharuggrENT1.snapshotDate = (select max(instcharuggrENT2.snapshotDate)
																			  from InstitCharUndergradGrad instcharuggrENT2
																			  where instcharuggrENT2.surveyCollectionYear = instcharuggrENT1.surveyCollectionYear
																			  and  array_contains(instcharuggrENT2.tags, 'IC Report Start')))
    )
where instcharuggrRn = 1
	and (offerUG = 'Y'
		or offerGR = 'Y')
),

InstitCharDrMCR as (

select *
from (
    select upper(clientconfig.icOfferDoctorAwardLevel) offerDR,
		nvl((select instcharuggr.diffTuitionAmounts
			 from InstitCharUndergradGradMCR instcharuggr), clientconfig.diffTuitionAmounts) diffTuitionAmounts,
		instchardrENT.chiroAvgTuitionInSt chiroAvgTuitionInSt,
		instchardrENT.chiroReqFeesInSt chiroReqFeesInSt,
		instchardrENT.chiroAvgTuitionOutSt chiroAvgTuitionOutSt,
		instchardrENT.chiroReqFeesOutSt chiroReqFeesOutSt,
		instchardrENT.dentAvgTuitionInSt dentAvgTuitionInSt,
		instchardrENT.dentReqFeesInSt dentReqFeesInSt,
		instchardrENT.dentAvgTuitionOutSt dentAvgTuitionOutSt,
		instchardrENT.dentReqFeesOutSt dentReqFeesOutSt,
		instchardrENT.medAvgTuitionInSt medAvgTuitionInSt,
		instchardrENT.medReqFeesInSt medReqFeesInSt,
		instchardrENT.medAvgTuitionOutSt medAvgTuitionOutSt,
		instchardrENT.medReqFeesOutSt medReqFeesOutSt,
		instchardrENT.optomAvgTuitionInSt optomAvgTuitionInSt,
		instchardrENT.optomReqFeesInSt optomReqFeesInSt,
		instchardrENT.optomAvgTuitionOutSt optomAvgTuitionOutSt,
		instchardrENT.optomReqFeesOutSt optomReqFeesOutSt,
		instchardrENT.osteoAvgTuitionInSt osteoAvgTuitionInSt,
		instchardrENT.osteoReqFeesInSt osteoReqFeesInSt,
		instchardrENT.osteoAvgTuitionOutSt osteoAvgTuitionOutSt,
		instchardrENT.osteoReqFeesOutSt osteoReqFeesOutSt,
		instchardrENT.pharmAvgTuitionInSt pharmAvgTuitionInSt,
		instchardrENT.pharmReqFeesInSt pharmReqFeesInSt,
		instchardrENT.pharmAvgTuitionOutSt pharmAvgTuitionOutSt,
		instchardrENT.pharmReqFeesOutSt pharmReqFeesOutSt,
		instchardrENT.podiaAvgTuitionInSt podiaAvgTuitionInSt,
		instchardrENT.podiaReqFeesInSt podiaReqFeesInSt,
		instchardrENT.podiaAvgTuitionOutSt podiaAvgTuitionOutSt,
		instchardrENT.podiaReqFeesOutSt podiaReqFeesOutSt,
		instchardrENT.vetAvgTuitionInSt vetAvgTuitionInSt,
		instchardrENT.vetReqFeesInSt vetReqFeesInSt,
		instchardrENT.vetAvgTuitionOutSt vetAvgTuitionOutSt,
		instchardrENT.vetReqFeesOutSt vetReqFeesOutSt,
		instchardrENT.lawAvgTuitionInSt lawAvgTuitionInSt,
		instchardrENT.lawReqFeesInSt lawReqFeesInSt,
		instchardrENT.lawAvgTuitionOutSt lawAvgTuitionOutSt,
		instchardrENT.lawReqFeesOutSt lawReqFeesOutSt,
		row_number() over (
			partition by 
				instchardrENT.surveyCollectionYear
			order by  
				instchardrENT.snapshotDate desc,
				instchardrENT.recordActivityDate desc
		) instchardrRn
    from InstitCharDoctorate instchardrENT
        cross join ClientConfigMCR clientconfig on instchardrENT.surveyCollectionYear = clientconfig.surveyYear
    
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
	where clientconfig.surveyYear not in (select instchardrENT1.surveyCollectionYear
										   from InstitCharDoctorate instchardrENT1
										   where instchardrENT1.surveyCollectionYear = clientconfig.surveyYear
										   and  array_contains(instchardrENT1.tags, 'IC Report Start')
										   and instchardrENT1.snapshotDate = (select max(instchardrENT2.snapshotDate)
																			  from InstitCharDoctorate instchardrENT2
																			  where instchardrENT2.surveyCollectionYear = instchardrENT1.surveyCollectionYear
																			  and  array_contains(instchardrENT2.tags, 'IC Report Start')))
)
where instchardrRn = 1
	and offerDR = 'Y'
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs.
*****/

/*
Part A - General information and application fees
*/

select 'A' part,   																	--PART - "A"
	instcharuggr.ugApplicationFee field1, 											--DA03 0 to 999999, -2 or blank if no full-time undergraduates, or no undergraduate award levels offered, 
																					  -- or no application fee for admission required.
	instcharuggr.grApplicationFee field2, 											--DA04 0 to 999999, -2 or blank if no full-time graduates, or no graduate award levels offered, or no
																					  -- application fee for admission required.
	case when instcharuggr.diffTuitionAmounts = 1 then '01' 
		else '02' 
	end field3, 																	--DA07 01 = YES, 02 = NO
	case when instcharuggr.institControlledHousing = 1 then '01' 
		else '02' 
	end field4, 																	--DA08 01 = YES, 02 = NO
	case when instcharuggr.institControlledHousing = 1 then instcharuggr.housingCapacity 
		else null 
	end field5, 																	--DA09 If DA08=01 (YES), then 1 to 999999. If DA08=02 (NO), then -2 or blank (not applicable).
	case instcharuggr.provideMealPlans
		when 'YES - NUMBER IN MAX PLAN' then '01'
		when 'YES - NUMBER VARIES' then '02'
		else '03'
	end field6, 																	--DA10 01 = YES, Number of meals per week in the maximum meal plan offered. 02 = YES, Number of meals per 
																					  -- week can vary 03 = NO
	case
	   when instcharuggr.provideMealPlans = 'YES - NUMBER IN MAX PLAN' then instcharuggr.mealsPerWeek
	   else null 
	   end field7, 																	--DA11 If DA10=01 (YES), then 2 to 99. If unlimited, then 99. If DA10=02 (meals vary) or 03 (NO), then -2 or
																					  -- blank (not applicable).
	COALESCE(case when instcharuggr.ftftDegreeLiveOnCampus = 1 then '01' 
				else '02' 
				end, '02') field8, --DA06 01 = YES and we do not allow any exceptions, 02 = NO
	null field9,
	null field10,
	null field11,
	null field12,
	null field13,
	null field14,
	null field15,
	null field16,
	null field17,
	null field18,
	null field19,
	null field20,
	null field21,
	null field22,
	null field23,
	null field24,
	null field25,
	null field26,
	null field27,
	null field28,
	null field29,
	null field30,
	null field31,
	null field32,
	null field33
from InstitCharUndergradGradMCR instcharuggr

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

select 'B',																	--PART - "B"
	instcharuggr.ugFTAvgTuitionDist,										--field1 DB01 Avg tuition, In-district 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugFTAvgTuitionInSt end,							--field2 DB02 Avg tuition, In-state 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugFTAvgTuitionOutSt end,							--field3 DB03 Avg tuition, Out-of-state 0 to 999999, -2 or blank = not applicable
	instcharuggr.ugFTReqFeesDist,											--field4 DB04 Required fees, In-district 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugFTReqFeesInSt end,								--field5 DB05 Required fees,In-state 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugFTReqFeesOutSt end,								--field6 DB06 Required fees, Out-of-state 0 to 999999, -2 or blank = not applicable
	instcharuggr.ugPTCredHrChargeDist,										--field7 DB07 Per credit hour charge , In-district 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugPTCredHrChargeInSt end,							--field8 DB08 Per credit hour charge , In-state 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugPTCredHrChargeOutSt end,						--field9 DB09 Per credit hour charge , Out-of-state 0 to 999999, -2 or blank = not applicable
	instcharuggr.ugFTCompFeeDist,											--field10 DB10 Comprehensive fee, In-district 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugFTCompFeeInSt end,								--field11 DB11 Comprehensive fee, In-state 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugFTCompFeeOutSt end,								--field12 DB12 Comprehensive fee, Out-of-state 0 to 999999, -2 or blank = not applicable
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
from InstitCharUndergradGradMCR instcharuggr
where instcharuggr.offerUG = 'Y'

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

select 'C',																	--PART - "C"
	instcharuggr.grFTAvgTuitionDist,										--field1 DC01 Average tuition In-district amount 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.grFTAvgTuitionInSt end,							--field2 DC02 Average tuition In-State amount 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.grFTAvgTuitionOutSt end,							--field3 DC03 Average tuition Out-of-state amount 0 to 999999, -2 or blank = not applicable
	instcharuggr.grFTReqFeesDist,											--field4 DC04 Required fees In-district amount 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.grFTReqFeesInSt end,								--field5 DC05 Required fees In-State amount 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.grFTReqFeesOutSt end,								--field6 DC06 Required fees Out-of-state amount 0 to 999999, -2 or blank = not applicable
	instcharuggr.grPTCredHrChargeDist,										--field7 DC07 Per credit hour charge In-district amount 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.grPTCredHrChargeInSt end,							--field8 DC08 Per credit hour charge In-State amount 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.grPTCredHrChargeOutSt end,						--field9 DC09 Per credit hour charge Out-of-state amount 0 to 999999, -2 or blank = not applicable
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
from InstitCharUndergradGradMCR instcharuggr
where instcharuggr.offerGR = 'Y'

union

/*Part D - Room and Board charges 

Instructions:
Room and board charges should be consistent with DA08 and DA10 above. 
If institution does not provide on-campus housing or board or meal plans, no Part "D" record should be included on the file. 
If institution does provide room and board, but does not separate room and board charges, provide combined charge in DD03, and leave DD01 and DD02 as -2 or blank (not applicable).

This section is not required from non-degree-granting institutions.
*/

select 'D',																	--PART - "D"
	case when instcharuggr.institControlledHousing = 0 then null 
		else instcharuggr.typicalRoomChg end,								--field1 DD01 Room Charge 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.provideMealPlans = 'NO' then null 
		else instcharuggr.typicalBoardChg end,								--field2 DD02 Board Charge 0 to 999999, -2 or blank = not applicable
	case
	   when instcharuggr.provideMealPlans = 'NO' 
			or instcharuggr.institControlledHousing = 0 then null
	   else instcharuggr.typicalCombRoomBoardChg end,						--field3 DD03 Combined Room and Board Charge 0 to 999999, -2 or blank = not applicable
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
from InstitCharUndergradGradMCR instcharuggr
where instcharuggr.provideMealPlans != 'NO'
   or instcharuggr.institControlledHousing = 1

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

select 'E',																						--PART - "E"
	instcharuggr.ugTuitionDist,																	--field1 DE011 Published tuition In-district 0 to 999999, -2 or blank = not applicable
	COALESCE(case when instcharuggr.ugTuitionGuarDist = 1 then '1' 
		else '0' end, '0'),                             										--field2 TUITGQ01 Do you have tuition guarantee In-district 0 = NO, 1 = YES
	COALESCE(instcharuggr.ugTuitionGuarIncrPercDist, '0'),										--field3 TUITGP01 Guaranteed tuition increase In-district 0 to 100
	ugFeeDist,																					--field4 DE012 Published fees In-district 0 to 999999, -2 or blank = not applicable
	COALESCE(case when instcharuggr.ugFeeGuarDist = 1 then '1' 
		else '0' end, '0'),																		--field5 FEEGQ01 Do you have fee guarantee In-district 0 = NO, 1 = YES
	COALESCE(instcharuggr.ugFeeGuarIncrPercDist, '0'),											--field6 FEEGP01 Guaranteed fee increase In-district 0 to 100

	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugTuitionInSt end,													--field7 DE021 Published tuition In-state 0 to 999999, -2 or blank = not applicable
	COALESCE(case when instcharuggr.diffTuitionAmounts = 1 
				and instcharuggr.ugTuitionGuarInSt = 1 then '1' 
			else '0' end, '0'),																	--field8 TUITGQ02 Do you have tuition guarantee In-state 0 = NO, 1 = YES
	COALESCE(case when instcharuggr.diffTuitionAmounts = 0 then '0' 
		else instcharuggr.ugTuitionGuarIncrPercInSt end, '0'),									--field9 TUITGP02 Guaranteed tuition increase In-state 0 to 100
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugFeeInSt end,														--field10 DE022 Published fees In-state 0 to 999999, -2 or blank = not applicable
	COALESCE(case when instcharuggr.diffTuitionAmounts = 1 
				and instcharuggr.ugFeeGuarInSt = 1 then '1' 
			else '0' end, '0'),																	--field11 FEEGQ02 Do you have fee guarantee In-state 0 = NO, 1 = YES
	COALESCE(case when instcharuggr.diffTuitionAmounts = 0 then '0' 
		else instcharuggr.ugFeeGuarIncrPercInSt end, '0'),										--field12 FEEGP02 Guaranteed fee increase In-state 0 to 100

	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugTuitionOutSt end,													--field13 DE031 Published tuition Out-of-state 0 to 999999, -2 or blank = not applicable
	COALESCE(case when instcharuggr.diffTuitionAmounts = 1 
				and instcharuggr.ugTuitionGuarOutSt = 1 then '1' 
			else '0' end, '0'),																	--field14 TUITGQ03 Do you have tuition guarantee Out-of-state 0 = NO, 1 = YES
	COALESCE(case when instcharuggr.diffTuitionAmounts = 0 then '0' 
		else instcharuggr.ugTuitionGuarIncrPercOutSt end, '0'),									--field15 TUITGP03 Guaranteed tuition increase Out-of-state 0 to 100
	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugFeeOutSt end,														--field16 DE032 Published fees Out-of-state 0 to 999999, -2 or blank = not applicable
	COALESCE(case when instcharuggr.diffTuitionAmounts = 1 
				and instcharuggr.ugFeeGuarOutSt = 1 then '1' 
			else '0' end, '0'),																	--field17 FEEGQ03 Do you have fee guarantee Out-of-state 0 = NO, 1 = YES
	COALESCE(case when instcharuggr.diffTuitionAmounts = 0 then '0' 
		else instcharuggr.ugFeeGuarIncrPercOutSt end, '0'),										--field18 FEEGP03 Guaranteed fee increase Out-of-state 0 to 100

	ugCompFeeDist,																				--field19 DE04 Comprehensive fee In-district 0 to 999999, -2 or blank = not applicable
	COALESCE(case when instcharuggr.ugCompFeeGuarDist = 1 then '1' 
		else '0' end, '0'),																		--field20 COMPFGQ04 Do you have fee guarantee In-district 0 = NO, 1 = YES
	COALESCE(instcharuggr.ugCompFeeGuarIncrPercDist, '0'),										--field21 COMPFGP04 Guaranteed fee increase In-district 0 to 100

	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugCompFeeInSt end,													--field22 DE05 Comprehensive fee In-state 0 to 999999, -2 or blank = not applicable
	COALESCE(case when instcharuggr.diffTuitionAmounts = 1 
				and instcharuggr.ugCompFeeGuarInSt = 1 then '1' 
			else '0' end,
			'0'),																				--field23 COMPFGQ05 Do you have fee guarantee In-state 0 = NO, 1 = YES
	COALESCE(case when instcharuggr.diffTuitionAmounts = 0 then '0' 
		else instcharuggr.ugCompFeeGuarIncrPercInSt end, '0'),									--field24 COMPFGP05 Guaranteed fee increase In-state 0 to 100

	case when instcharuggr.diffTuitionAmounts = 0 then null 
		else instcharuggr.ugCompFeeOutSt end,													--field25 DE06 Comprehensive fee Out-of-state 0 to 999999, -2 or blank = not applicable
	COALESCE(case when instcharuggr.diffTuitionAmounts = 1 
				and instcharuggr.ugCompFeeGuarOutSt = 1 then '1' 
			else '0' end, '0'),																	--field26 COMPFGQ06 Do you have fee guarantee Out-of-state 0 = NO, 1 = YES
	COALESCE(case when instcharuggr.diffTuitionAmounts = 0 then '0' 
			else instcharuggr.ugCompFeeGuarIncrPercOutSt end, '0'),								--field27 COMPFGP06 Guaranteed fee increase Out-of-state 0 to 100

	instcharuggr.ugBooksAndSupplies,															--field28 DE07 Books and supplies 0 to 999999, -2 or blank = not applicable
	case when instcharuggr.provideMealPlans != 'NO' 
			and instcharuggr.institControlledHousing = 1 then instcharuggr.ugRoomBoardOnCampus
		else null end,																			--field29 DE08 On campus room and board 0 to 999999, -2 or blank = not applicable
	instcharuggr.ugExpensesOnCampus,															--field30 DE09 On campus other expenses 0 to 999999, -2 or blank = not applicable
	instcharuggr.ugRoomBoardOffCampus,															--field31 DE10 Off campus (not with family) Room and board 0 to 999999, -2 or blank = not applicable
	instcharuggr.ugExpensesOffCampus,															--field32 DE11 Off campus (not with family) Other expenses 0 to 999999, -2 or blank = not applicable
	instcharuggr.ugExpensesOffCampusFamily														--field33 DE12 Off campus (with family) Other expenses 0 to 999999, -2 or blank = not applicable
from InstitCharUndergradGradMCR instcharuggr
where instcharuggr.offerUG = 'Y'

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
select 'F',															--PART - "E"
	'1',															--field1 DF01 Chiropractic Doctor's professional program code values 1 - 9
	COALESCE(instchardr.chiroAvgTuitionInSt, '0'),					--field2 DF02 Average tuition, In-state amount 0 to 999999
	COALESCE(instchardr.chiroReqFeesInSt, '0'),						--field3 DF03 Average tuition, In-state amount 0 to 999999
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.chiroAvgTuitionOutSt, '0') end,		--field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.chiroReqFeesOutSt, '0') end,		--field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
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
from InstitCharDrMCR instchardr
where instchardr.offerDR = 'Y'

union

select 'F',														--PART - "F"
	'2',														--field1 DF01 Dentistry Doctor's professional program code
	COALESCE(instchardr.dentAvgTuitionInSt, '0'),				--field2 DF02 Average tuition, In-state amount 0 to 999999
	COALESCE(instchardr.dentReqFeesInSt, '0'),					--field3 DF03 Average tuition, In-state amount 0 to 999999
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.dentAvgTuitionOutSt, '0') end,	--field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.dentReqFeesOutSt, '0') end,		--field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
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
from InstitCharDrMCR instchardr
where instchardr.offerDR = 'Y'

union

select 'F',
	'3',														--field1 DF01 Medicine Doctor's professional program code
	COALESCE(instchardr.medAvgTuitionInSt, '0'),				--field2 DF02 Average tuition, In-state amount 0 to 999999
	COALESCE(instchardr.medReqFeesInSt, '0'),					--field3 DF03 Average tuition, In-state amount 0 to 999999
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.medAvgTuitionOutSt, '0') end,	--field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.medReqFeesOutSt, '0') end,		--field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
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
from InstitCharDrMCR instchardr
where instchardr.offerDR = 'Y'

union

select 'F',
	'4',															--field1 DF01 Optometry Doctor's professional program code
	COALESCE(instchardr.optomAvgTuitionInSt, '0'),					--field2 DF02 Average tuition, In-state amount 0 to 999999
	COALESCE(instchardr.optomReqFeesInSt, '0'),						--field3 DF03 Average tuition, In-state amount 0 to 999999
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.optomAvgTuitionOutSt, '0') end,		--field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.optomReqFeesOutSt, '0') end,		--field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
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
from InstitCharDrMCR instchardr
where instchardr.offerDR = 'Y'

union

select 'F',
	'5',															--field1 DF01 Osteopathic Medicine Doctor's professional program code
	COALESCE(instchardr.osteoAvgTuitionInSt, '0'),					--field2 DF02 Average tuition, In-state amount 0 to 999999
	COALESCE(instchardr.osteoReqFeesInSt, '0'),						--field3 DF03 Average tuition, In-state amount 0 to 999999
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.osteoAvgTuitionOutSt, '0') end,		--field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.osteoReqFeesOutSt, '0') end,		--field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
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
from InstitCharDrMCR instchardr
where instchardr.offerDR = 'Y'

union

select 'F',
	'6',															--field1 DF01 Pharmacy Doctor's professional program code
	COALESCE(instchardr.pharmAvgTuitionInSt, '0'),					--field2 DF02 Average tuition, In-state amount 0 to 999999
	COALESCE(instchardr.pharmReqFeesInSt, '0'),						--field3 DF03 Average tuition, In-state amount 0 to 999999
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.pharmAvgTuitionOutSt, '0') end,		--field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.pharmReqFeesOutSt, '0') end,		--field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
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
from InstitCharDrMCR instchardr
where instchardr.offerDR = 'Y'

union

select 'F',
	'7',															--field1 DF01 Podiatry Doctor's professional program code
	COALESCE(instchardr.podiaAvgTuitionInSt, '0'),					--field2 DF02 Average tuition, In-state amount 0 to 999999
	COALESCE(instchardr.podiaReqFeesInSt, '0'),						--field3 DF03 Average tuition, In-state amount 0 to 999999
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.podiaAvgTuitionOutSt, '0') end,		--field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.podiaReqFeesOutSt, '0') end,		--field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
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
from InstitCharDrMCR instchardr
where instchardr.offerDR = 'Y'

union

select 'F',
	'8',														--field1 DF01 Veterinary Medicine Doctor's professional program code
	COALESCE(instchardr.vetAvgTuitionInSt, '0'),				--field2 DF02 Average tuition, In-state amount 0 to 999999
	COALESCE(instchardr.vetReqFeesInSt, '0'),					--field3 DF03 Average tuition, In-state amount 0 to 999999
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.vetAvgTuitionOutSt, '0') end,	--field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.vetReqFeesOutSt, '0') end,		--field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
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
from InstitCharDrMCR instchardr
where instchardr.offerDR = 'Y'

union

select 'F',
	'9',														--field1 DF01 Law Doctor's professional program code
	COALESCE(instchardr.lawAvgTuitionInSt, '0'),				--field2 DF02 Average tuition, In-state amount 0 to 999999
	COALESCE(instchardr.lawReqFeesInSt, '0'),					--field3 DF03 Average tuition, In-state amount 0 to 999999
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.lawAvgTuitionOutSt, '0') end,	--field4 DF04 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
	case
	   when instchardr.diffTuitionAmounts = 0 then null
	   else COALESCE(instchardr.lawReqFeesOutSt, '0') end,		--field5 DF05 Average tuition, Out-of-state amount 0 to 999999, -2 or blank (not applicable)
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
from InstitCharDrMCR instchardr
where instchardr.offerDR = 'Y'
