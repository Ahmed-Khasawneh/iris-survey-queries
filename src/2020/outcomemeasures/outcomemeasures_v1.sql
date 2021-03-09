/********************

EVI PRODUCT:	DORIS 2020-21 IPEDS Survey  
FILE NAME: 		Outcome Measures v1 (OM1)
FILE DESC:      Outcome Measures for all institutions
AUTHOR:         jhanicak
CREATED:        20210108

SECTIONS:
Reporting Dates/Terms
Most Recent Records 
Student Counts
Course Counts
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)  	Author             	    Tag             	Comments
----------- 		--------------------	-------------   	-------------------------------------------------
20210125            jhanicak                                    PF-1887 Added TransferMCR and minor fixes (runtime: test 23m 10s, prod 24m 25s)
20210108	        jhanicak				                    Initial version PF-1409 (runtime: test 27m, 21s prod 29m 57s)

********************/ 

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (
/*******************************************************************
 Assigns all hard-coded values to variables. All date and version 
 adjustments and default values should be modified here. 

 In contrast to the Graduation Rates report which is based on specific terms,
 Outcome Measures is based on a full academic year and includes any
 full or partial term that starts within the academic year 7/1/2012 thru 6/30/2013
  ------------------------------------------------------------------
 Each client will need to determine how to identify and/or pull the 
 terms for their institution based on how they track the terms.  
 For some schools, it could be based on dates or academic year and for others,
 it may be by listing specific terms. 
 *******************************************************************/ 

--Prod blocks (2)
select '2021' surveyYear, 
	'OM1' surveyId,  
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2012-07-01' AS DATE) reportingDateStart,
	CAST('2013-06-30' AS DATE) reportingDateEnd,
	'201310' termCode, --Fall 2012
	'1' partOfTermCode, 
	CAST('2012-09-14' AS DATE) censusDate,
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
--***** start survey-specific mods
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
    CAST('2013-05-30' as DATE) financialAidEndDate,
    CAST('2016-08-31' as DATE) earliestStatusDate,
    CAST('2018-08-31' as DATE) midStatusDate,
    CAST('2020-08-31' as DATE) latestStatusDate,
    CAST('2021-02-10' as DATE) surveyCloseDate,
	'000000' ncSchoolCode,
    '00' ncBranchCode
--***** end survey-specific mods

union

select '2021' surveyYear, 
	'OM1' surveyId,  
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2012-07-01' AS DATE) reportingDateStart,
	CAST('2013-06-30' AS DATE) reportingDateEnd,
	'201230' termCode, --Summer 2012
	'1' partOfTermCode, 
	CAST('2012-06-10' AS DATE) censusDate,
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
--***** start survey-specific mods
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
    CAST('2013-05-30' as DATE) financialAidEndDate,
    CAST('2016-08-31' as DATE) earliestStatusDate,
    CAST('2018-08-31' as DATE) midStatusDate,
    CAST('2020-08-31' as DATE) latestStatusDate,
    CAST('2021-02-10' as DATE) surveyCloseDate,
	'000000' ncSchoolCode,
    '00' ncBranchCode
--***** end survey-specific mods

/*
--Testing blocks (2 min)
select '1415' surveyYear,  
	'OM1' surveyId,  
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
	CAST('9999-09-09' as DATE) snapshotDate,   
	CAST('2006-07-01' as DATE) reportingDateStart,
    CAST('2007-06-30' as DATE) reportingDateEnd, 
	'200710' termCode, --Fall 2006
	'1' partOfTermCode,
	CAST('2006-09-10' AS DATE) censusDate,
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
--***** start survey-specific mods
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4, 
    CAST('2013-05-30' as DATE) financialAidEndDate,
    CAST('2010-08-31' as DATE) earliestStatusDate,
    CAST('2012-08-31' as DATE) midStatusDate,
    CAST('2014-08-31' as DATE) latestStatusDate,
    CAST('2021-02-10' as DATE) surveyCloseDate,
	'000000' ncSchoolCode,
    '00' ncBranchCode
--***** end survey-specific mods

union

select '1415' surveyYear,  
	'OM1' surveyId,  
	'August End' repPeriodTag1, --used for all status updates and IPEDS tables
	'Financial Aid Year End' repPeriodTag2, --used to pull FA for cohort
	CAST('9999-09-09' as DATE) snapshotDate,   
	CAST('2006-07-01' as DATE) reportingDateStart,
    CAST('2007-06-30' as DATE) reportingDateEnd, 
	'200630' termCode, --Summer 2006
	'1' partOfTermCode,
	CAST('2006-05-25' AS DATE) censusDate,
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
--***** start survey-specific mods
    'Student Transfer Data' repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4, 
    CAST('2013-05-30' as DATE) financialAidEndDate,
    CAST('2010-08-31' as DATE) earliestStatusDate,
    CAST('2012-08-31' as DATE) midStatusDate,
    CAST('2014-08-31' as DATE) latestStatusDate,
    CAST('2021-02-10' as DATE) surveyCloseDate,
	'000000' ncSchoolCode,
    '00' ncBranchCode
--***** end survey-specific mods
*/
),

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 

--  1st union 1st order - pull snapshot for defvalues.repPeriodTag1 
--  1st union 2nd order - pull snapshot for defvalues.repPeriodTag2
--  1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--  2nd union - pull default values if no record in IPEDSReportingPeriod

select distinct RepDates.surveyYear	surveyYear,
    RepDates.source source,
    coalesce(upper(RepDates.surveySection), 'COHORT') surveySection,
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
    RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,
    to_date(RepDates.censusDate,'YYYY-MM-DD') censusDate
from (
    select repperiodENT.surveyCollectionYear surveyYear,
	    'IPEDSReportingPeriod' source,
		repperiodENT.snapshotDate snapshotDate,
		repPeriodENT.surveyId surveyId,
		repPeriodENT.surveySection surveySection,
		coalesce(repperiodENT.termCode, defvalues.termCode) termCode,
		coalesce(repperiodENT.partOfTermCode, defvalues.partOfTermCode) partOfTermCode,
		defvalues.censusDate censusDate,
		row_number() over (	
			partition by 
				repPeriodENT.surveyCollectionYear,
                repPeriodENT.surveyId,
                repPeriodENT.surveySection, 
				repperiodENT.termCode,
				repperiodENT.partOfTermCode	
			order by 
			    (case when array_contains(repperiodENT.tags, defvalues.repPeriodTag1) then 1
			         else 3 end) asc,
			     repperiodENT.snapshotDate desc,
                repperiodENT.recordActivityDate desc	
		) reportPeriodRn	
		from IPEDSReportingPeriod repperiodENT
		    inner join DefaultValues defvalues on repperiodENT.surveyId = defvalues.surveyId
	    and repperiodENT.surveyCollectionYear = defvalues.surveyYear
	    where repperiodENT.termCode is not null
		and repperiodENT.partOfTermCode is not null
	
    union 
 
	select defvalues.surveyYear surveyYear,
	    'DefaultValues' source,
		CAST('9999-09-09' as DATE) snapshotDate,
		defvalues.surveyId surveyId, 
		null surveySection,
		defvalues.termCode termCode,
		defvalues.partOfTermCode partOfTermCode, 
		defvalues.censusDate censusDate,
		1
	from DefaultValues defvalues
    where defvalues.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = defvalues.surveyYear
											and upper(repperiodENT.surveyId) = defvalues.surveyId 
											and repperiodENT.termCode is not null
											and repperiodENT.partOfTermCode is not null) 
    ) RepDates
where RepDates.reportPeriodRn = 1
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 
 
--  1st union 1st order - pull snapshot where same as ReportingPeriodMCR snapshotDate
--  1st union 2nd order - pull closet snapshot before ReportingPeriodMCR snapshotDate
--  1st union 3rd order - pull closet snapshot after ReportingPeriodMCR snapshotDate
--  2nd union - pull default values if no record in IPEDSClientConfig

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.source source,
    to_date(ConfigLatest.snapshotDate,'YYYY-MM-DD') snapshotDate,
    ConfigLatest.repperiodSnapshotDate repperiodSnapshotDate,
    upper(ConfigLatest.instructionalActivityType) instructionalActivityType,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
	ConfigLatest.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
	ConfigLatest.repPeriodTag3 repPeriodTag3,
    ConfigLatest.repPeriodTag4 repPeriodTag4,
    ConfigLatest.financialAidEndDate financialAidEndDate,
    ConfigLatest.earliestStatusDate earliestStatusDate,
    ConfigLatest.midStatusDate midStatusDate,
    ConfigLatest.latestStatusDate latestStatusDate,
	ConfigLatest.ncSchoolCode ncSchoolCode,
    ConfigLatest.ncBranchCode ncBranchCode,
    ConfigLatest.reportingDateStart reportingDateStart,
    ConfigLatest.surveyCloseDate surveyCloseDate
--***** end survey-specific mods
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'configFullYearTag' source,
		clientConfigENT.snapshotDate snapshotDate, 
		repperiod.snapshotDate repperiodSnapshotDate,
        coalesce(clientConfigENT.instructionalActivityType, defvalues.instructionalActivityType) instructionalActivityType,	
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
        defvalues.repPeriodTag3 repPeriodTag3,
        defvalues.repPeriodTag4 repPeriodTag4,
        defvalues.financialAidEndDate financialAidEndDate,
        defvalues.earliestStatusDate earliestStatusDate,
        defvalues.midStatusDate midStatusDate,
        defvalues.latestStatusDate latestStatusDate,
		coalesce(clientConfigENT.ncSchoolCode, defvalues.ncSchoolCode) ncSchoolCode,
        coalesce(clientConfigENT.ncBranchCode, defvalues.ncBranchCode) ncBranchCode,
        defvalues.reportingDateStart reportingDateStart,
        defvalues.surveyCloseDate surveyCloseDate,
--***** end survey-specific mods
		row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
			    (case when to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				clientConfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientConfigENT
	    inner join DefaultValues defvalues on clientConfigENT.surveyCollectionYear = defvalues.surveyYear
		inner join ReportingPeriodMCR repperiod on clientConfigENT.surveyCollectionYear = repperiod.surveyYear

    union

	select defvalues.surveyYear surveyYear,
	    'default' source,
	    CAST('9999-09-09' as DATE) snapshotDate,
	    null repperiodSnapshotDate, 
        defvalues.instructionalActivityType instructionalActivityType,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
        defvalues.repPeriodTag3 repPeriodTag3,
        defvalues.repPeriodTag4 repPeriodTag4,
        defvalues.financialAidEndDate financialAidEndDate,
        defvalues.earliestStatusDate earliestStatusDate,
        defvalues.midStatusDate midStatusDate,
        defvalues.latestStatusDate latestStatusDate,
		defvalues.ncSchoolCode ncSchoolCode,
        defvalues.ncBranchCode ncBranchCode,
        defvalues.reportingDateStart reportingDateStart,
        defvalues.surveyCloseDate surveyCloseDate,
--***** end survey-specific mods
		1 configRn
    from DefaultValues defvalues
    where defvalues.surveyYear not in (select max(configENT.surveyCollectionYear)
										from IPEDSClientConfig configENT
										where configENT.surveyCollectionYear = defvalues.surveyYear)
	) ConfigLatest
where ConfigLatest.configRn = 1	
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for all term codes and parts of term code for all snapshots. 

-- jh 20201007 Included tags field to pull thru for 'Pre-Fall Summer Census' check
--				Moved check on termCode censusDate and snapshotDate to view AcademicTermReporting

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
    select distinct acadtermENT.termCode, 
        row_number() over (
            partition by 
                acadTermENT.snapshotDate,
                acadTermENT.termCode,
                acadTermENT.partOfTermCode
            order by
                acadTermENT.recordActivityDate desc
        ) acadTermRn,
        acadTermENT.snapshotDate,
        acadTermENT.tags,
		acadtermENT.partOfTermCode, 
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
		acadtermENT.requiredFTCreditHoursGR,
	    acadtermENT.requiredFTCreditHoursUG,
	    acadtermENT.requiredFTClockHoursUG,
		acadtermENT.isIPEDSReportable
	from AcademicTerm acadtermENT
	where acadtermENT.isIPEDSReportable = 1
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

AcademicTermReporting as (
--Combines ReportingPeriodMCR and AcademicTermMCR in order to use the correct snapshot dates for the reporting terms

--	first order field: assign 1 if acadterm.snapshotDate falls within the acadterm.census range and the acadterm.tags value matches with the acadterm.termType
--  first order field: assign 2 if acadterm.snapshotDate falls within the acadterm.census range but acadterm.tags/acadterm.termType values don't match
--  first order field: assign 3 if acadterm.snapshotDate doesn't fall within the acadterm.census range and acadterm.tags/acadterm.termType values don't match
--  second order field: acadterm.snapshotDates before the acadterm.census range ordered so the snapshotDate closest to the censusDate is first - ascending
--  third order field: acadterm.snapshotDates after the acadterm.census range ordered so the snapshotDate closest to the censusDate is first - descending
--  if none of the above, order by repperiod.snapshotDate

select repPerTerms.yearType yearType,
        repPerTerms.surveySection surveySection,
        repPerTerms.termCode termCode,
        repPerTerms.partOfTermCode partOfTermCode,
        repPerTerms.financialAidYear financialAidYear,
        repPerTerms.termOrder termOrder,
        repPerTerms.maxCensus maxCensus,
        coalesce(repPerTerms.acadTermSSDate, repPerTerms.repPeriodSSDate) snapshotDate,
        repPerTerms.termClassification termClassification,
        repPerTerms.termType termType,
        repPerTerms.startDate startDate,
        repPerTerms.endDate endDate,
        repPerTerms.censusDate censusDate,
        repPerTerms.requiredFTCreditHoursGR,
	    repPerTerms.requiredFTCreditHoursUG,
	    repPerTerms.requiredFTClockHoursUG,
		repPerTerms.instructionalActivityType,
	    repPerTerms.equivCRHRFactor equivCRHRFactor,
        (case when repPerTerms.termClassification = 'Standard Length' then 1
             when repPerTerms.termClassification is null then (case when repPerTerms.termType in ('Fall', 'Spring') then 1 else 2 end)
             else 2
        end) fullTermOrder
from (
select distinct 'CY' yearType,
        repperiod.surveySection surveySection,
        repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        acadterm.financialAidYear financialAidYear,
        acadterm.snapshotDate acadTermSSDate,
        repperiod.snapshotDate repPeriodSSDate,
        acadterm.tags tags,
        coalesce(acadterm.censusDate, repperiod.censusDate) censusDate,
		row_number() over (
            partition by 
                repperiod.termCode,
                repperiod.partOfTermCode
            order by
                (case when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') 
                            and ((array_contains(acadterm.tags, 'Fall Census') and acadterm.termType = 'Fall')
                                or (array_contains(acadterm.tags, 'Spring Census') and acadterm.termType = 'Spring')
                                or (array_contains(acadterm.tags, 'Pre-Fall Summer Census') and acadterm.termType = 'Summer')
                                or (array_contains(acadterm.tags, 'Post-Fall Summer Census') and acadterm.termType = 'Summer')) then 1
                      when acadterm.snapshotDate <= to_date(date_add(acadterm.censusdate, 3), 'YYYY-MM-DD') 
                            and acadterm.snapshotDate >= to_date(date_sub(acadterm.censusDate, 1), 'YYYY-MM-DD') then 2
                     else 3 end) asc,
                (case when acadterm.snapshotDate > acadterm.censusDate then acadterm.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when acadterm.snapshotDate < acadterm.censusDate then acadterm.snapshotDate else CAST('1900-09-09' as DATE) end) desc
            ) acadTermRnReg,
            termorder.termOrder termOrder,
        termorder.maxCensus maxCensus,
        acadterm.termClassification termClassification,
        acadterm.termType termType,
        acadterm.startDate startDate,
        acadterm.endDate endDate,
        acadterm.requiredFTCreditHoursGR,
	    acadterm.requiredFTCreditHoursUG,
	    acadterm.requiredFTClockHoursUG,
		clientconfig.instructionalActivityType,
	    coalesce(acadterm.requiredFTCreditHoursUG/
		    coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor
     from ReportingPeriodMCR repperiod 
        left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
	            and repperiod.partOfTermCode = acadterm.partOfTermCode
		left join AcademicTermOrder termorder on termOrder.termCode = repperiod.termCode
		inner join ClientConfigMCR clientconfig on repperiod.surveyYear = clientconfig.surveyYear
		) repPerTerms
where repPerTerms.acadTermRnReg = 1 
),

AcademicTermReportingRefactor as (
--Returns all records from AcademicTermReporting, converts Summer terms to Pre-Fall or Post-Spring and creates reportingDateStart/End

-- jh 20201007 Added to return new Summer termTypes and reportingDateStart/End

select rep.*,
        (select first(financialAidYear) from AcademicTermReporting where termType = 'Fall') financialAidYearFall,
        (select first(termCode) from AcademicTermReporting where termType = 'Fall') termCodeFall,
        (select first(termOrder) from AcademicTermReporting where termType = 'Fall') termCodeFallOrder,
        (select first(startDate) from AcademicTermReporting where termType = 'Fall' and partOfTermCode = '1') termStartDateFall,
        (case when rep.termType = 'Summer' and rep.termClassification != 'Standard Length' then 
                    case when (select max(rep2.termOrder)
                    from AcademicTermReporting rep2
                    where rep2.termType = 'Summer') < (select max(rep2.termOrder)
                                                        from AcademicTermReporting rep2
                                                        where rep2.termType = 'Fall') then 'Pre-Fall Summer'
                    else 'Post-Spring Summer' end
                else rep.termType end) termTypeNew,
        potMax.partOfTermCode maxPOT
from AcademicTermReporting rep
    inner join (select rep3.termCode,
                        rep3.partOfTermCode,
                       row_number() over (
			                partition by
			                    rep3.termCode
			                order by
				                rep3.censusDate desc,
				                rep3.endDate desc
		                ) potRn
                from AcademicTermReporting rep3) potMax on rep.termCode = potMax.termCode
                        and potMax.potRn = 1
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

CampusMCR as ( 
-- Returns most recent campus record for all snapshots in the ReportingPeriod
-- We only use campus for international status. We are maintaining the ability to look at a campus at different points in time through relevant snapshots. 
-- Will there ever be a case where a campus changes international status? 
-- Otherwise, we could just get all unique campus codes and forget about when the record was made.

-- jh 20200911 Removed ReportingPeriodMCR reference and changed filter date from regper.reportingperiodend to campusENT.snapshotDate 

select campus,
	isInternational,
	snapshotDate,
	tags
from ( 
    select upper(campusENT.campus) campus,
		campusENT.campusDescription,
		campusENT.isInternational,
		to_date(campusENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
		campusENT.tags tags,
		row_number() over (
			partition by
			    campusENT.snapshotDate, 
				campusENT.campus
			order by
				campusENT.recordActivityDate desc
		) campusRn
	from Campus campusENT
	where campusENT.isIpedsReportable = 1 
		and ((to_date(campusENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
			and to_date(campusENT.recordActivityDate,'YYYY-MM-DD') <= to_date(campusENT.snapshotDate,'YYYY-MM-DD'))
				or to_date(campusENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
	)
where campusRn = 1
),

RegStatusMCR as (

select personId
from (
    select distinct regENT.personId personId, 
            row_number() over (
                partition by
                    regENT.personId
                order by 
                    (case when array_contains(regENT.tags, config.repPeriodTag1) then 1 else 2 end) asc,
                    regENT.snapshotDate desc
            ) regRn
        from Registration regENT
            cross join (select first(latestStatusDate) latestStatusDate,
                                    first(repPeriodTag1) repPeriodTag1
                            from ClientConfigMCR) config
            inner join AcademicTermMCR repperiod on repperiod.startDate between date_sub(config.latestStatusDate, 90) and date_add(config.latestStatusDate, 60)
                and repperiod.termCode = regENT.termCode
                and repperiod.partOfTermCode = regENT.partOfTermCode
        where ((to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                            and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= config.latestStatusDate)
                        or (to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                            and ((to_date(regENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                                    and to_date(regENT.recordActivityDate,'YYYY-MM-DD') <= config.latestStatusDate)
                                or to_date(regENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE)))) 
                and regENT.isEnrolled = 1
                and regENT.isIpedsReportable = 1
        )
where regRn = 1 
),

RegistrationMCR as ( 
--Returns most current student enrollment records as of the term within period and where course is viable

--jh 20210122 modified financialAidYear to use the AcademicTermReportingRefactor financialAidYearFall value

select *
from (
    select regData.yearType, 
        regData.surveySection,
        regData.snapshotDate,
        regData.termCode,
        regData.partOfTermCode,
        regData.regENTSSD regENTSSD,
        regData.repSSD repSSD,
        campus.snapshotDate campusSSD,
        regData.financialAidYear,
        regData.termorder,
        regData.maxCensus,
        regData.censusDate,
        regData.fullTermOrder,
        regData.termType,
        regData.startDate,
        regData.requiredFTCreditHoursGR,
        regData.requiredFTCreditHoursUG,
        regData.requiredFTClockHoursUG,
		regData.instructionalActivityType,
        regData.equivCRHRFactor,
        regData.personId,                    
        regData.crn,
        regData.crnLevel,    
        regData.crnGradingMode,
        regData.campus,
        coalesce(campus.isInternational, false) isInternational,
        row_number() over (
                partition by
                    regData.yearType,
                    regData.surveySection,
                    regData.termCode,
                    regData.partOfTermCode,
                    regData.personId,
                    regData.crn,
                    regData.crnLevel
                order by 
                    (case when campus.snapshotDate = regData.snapshotDate then 1 else 2 end) asc,
                    (case when campus.snapshotDate > regData.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                    (case when campus.snapshotDate < regData.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc
            ) regCampRn
    from ( 
        select regENT.personId personId,
            repperiod.snapshotDate snapshotDate,
            to_date(regENT.snapshotDate, 'YYYY-MM-DD') regENTSSD,
            repperiod.snapshotDate repSSD,
            regENT.termCode termCode,
            regENT.partOfTermCode partOfTermCode, 
            repperiod.surveySection surveySection,
            repperiod.financialAidYearFall financialAidYear,
            repperiod.termorder termorder,
            repperiod.maxCensus maxCensus,
            repperiod.fullTermOrder fullTermOrder,
            repperiod.termTypeNew termType,
            repperiod.yearType yearType,
            repperiod.startDate startDate,
            repperiod.censusDate censusDate,
            repperiod.requiredFTCreditHoursGR,
            repperiod.requiredFTCreditHoursUG,
            repperiod.requiredFTClockHoursUG,
		    repperiod.instructionalActivityType,
            repperiod.equivCRHRFactor,
            upper(regENT.campus) campus,
            coalesce(regENT.crnGradingMode, 'Standard') crnGradingMode,                    
            upper(regENT.crn) crn,
            regENT.crnLevel crnLevel,
            row_number() over (
                partition by
                    repperiod.yearType,
                    repperiod.surveySection,
                    regENT.termCode,
                    regENT.partOfTermCode,
                    regENT.personId,
                    regENT.crn,
                    regENT.crnLevel
                order by 
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    regENT.recordActivityDate desc
            ) regRn
        from AcademicTermReportingRefactor repperiod   
            inner join Registration regENT on repperiod.termCode = regENT.termCode
                and repperiod.partOfTermCode = regENT.partOfTermCode
                and ((to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                            and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
                        or (to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                            and ((to_date(regENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                                    and to_date(regENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                                or to_date(regENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE)))) 
                and regENT.isEnrolled = 1
                and regENT.isIpedsReportable = 1 
                ) regData
            left join CampusMCR campus on regData.campus = campus.campus
        where regData.regRn = 1
    )
where regCampRn = 1
),

StudentMCR as (
--Returns most up to date student academic information as of the reporting term codes and part of term census periods.  

select stuData.yearType,
        stuData.surveySection,
        stuData.snapshotDate snapshotDate,
        stuData.termCode, 
        stuData.termOrder,
        stuData.financialAidYear,
        stuData.maxCensus,
        stuData.termType,
        stuData.startDate,
        stuData.censusDate,
        stuData.maxCensus,
        stuData.fullTermOrder,
        stuData.startDate,
        stuData.personId,
        coalesce((case when stuData.studentType = 'High School' then true
                    when stuData.studentLevel = 'Continuing Ed' then true
                    when stuData.studentLevel = 'Occupational/Professional' then true
                    when stuData.studentLevel = 'Other' then true
                  else stuData.isNonDegreeSeeking end), false) isNonDegreeSeeking,
        stuData.studentLevel,
        stuData.studentType,
        stuData.residency,
        stuData.campus
from ( 
	 select reg.yearType yearType,
            reg.snapshotDate snapshotDate,
            to_date(studentENT.snapshotDate,'YYYY-MM-DD') stuSSD,
            reg.surveySection surveySection,
            reg.termCode termCode, 
            reg.termOrder termOrder,
            reg.censusDate censusDate,
            reg.maxCensus maxCensus,
            reg.termType termType,
            reg.startDate startDate,
            reg.fullTermOrder fullTermOrder, --1 for 'full' (standard), 2 for non-standard
            reg.financialAidYear financialAidYear,
            studentENT.personId personId,
            studentENT.isNonDegreeSeeking isNonDegreeSeeking,
            studentENT.studentLevel studentLevel,
            studentENT.studentType studentType,
            studentENT.residency residency,
            upper(studentENT.campus) campus,
            row_number() over (
                partition by
                    reg.yearType,
                    reg.surveySection,
                    studentENT.personId,                    
                    studentENT.termCode
                order by
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') = reg.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(studentENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(studentENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    studentENT.recordActivityDate desc
            ) studRn
	from RegistrationMCR reg
		inner join Student studentENT on reg.personId = studentENT.personId 
			and reg.termCode = studentENT.termCode
			and ((to_date(studentENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)  
				and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= reg.censusDate
				and studentENT.studentStatus = 'Active') --do not report Study Abroad students
					or to_date(studentENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
			and studentENT.isIpedsReportable = 1
	) stuData
where stuData.studRn = 1 
),

StudentRefactor as (
--Determine student info based on full term and degree-seeking status

select *
from ( 
select stu.yearType yearType,
        acadTermCode.surveySection surveySection,
        acadTermCode.snapshotDate snapshotDate,
        stu.firstFullTerm firstFullTerm,
        acadTermCode.censusDate censusDate,
        acadTermCode.maxCensus maxCensus,
        acadTermCode.termOrder,
        acadTermCode.termType,
        stu.personId,
       stu.isNonDegreeSeeking,
       (case when stu.studentLevel = 'Undergrad' then 
                (case when stu.studentTypeTermType = 'Fall' and stu.studentType = 'Returning' and stu.preFallStudType is not null then stu.preFallStudType
                      else stu.studentType 
                end)
             else stu.studentType 
        end) studentType,
        stu.studentLevel studentLevelORIG,
        (case when stu.studentLevel = 'Graduate' then 'GR'
                          else 'UG'
        end) studentLevel,
        stu.campus campus,
        coalesce(campus.isInternational, false) isInternational,        
	    row_number() over (
                partition by
                    stu.yearType,
                    acadTermCode.surveySection,
                    stu.personId
                order by 
                    (case when campus.snapshotDate = acadTermCode.snapshotDate then 1 else 2 end) asc,
                    (case when campus.snapshotDate > acadTermCode.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                    (case when campus.snapshotDate < acadTermCode.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc
            ) regCampRn
    from ( 
        select distinct personId,
            yearType,
            min(isNonDegreeSeeking) isNonDegreeSeeking,
            max(studentType) studentType,
            max(firstFullTerm) firstFullTerm,
            max(studentLevel) studentLevel,
            max(studentTypeTermType) studentTypeTermType,
            max(preFallStudType) preFallStudType,
            max(campus) campus
        from (
            select distinct personId,
                yearType,
                termCode,
                FFTRn,
                NDSRn,
                termType,
                (case when isNonDegreeSeeking = true then 1 else 0 end) isNonDegreeSeeking, 
                (case when isNonDegreeSeeking = false then
                    (case when studentLevel != 'Undergrad' then null
                        when NDSRn = 1 and FFTRn = 1 then studentType
                        when NDSRn = 1 then 'Returning'
                    end)
                    else null
                end) studentType,
                (case when isNonDegreeSeeking = false then
                    (case when studentLevel != 'Undergrad' then null
                        when NDSRn = 1 and FFTRn = 1 then termType
                        when NDSRn = 1 then termType
                    end)
                    else null
                end) studentTypeTermType,
                (case when termType = 'Pre-Fall Summer' then studentType else null end) preFallStudType,
                (case when FFTRn = 1 then studentLevel else null end) studentLevel,
                (case when FFTRn = 1 then termCode else null end) firstFullTerm,
				(case when FFTRn = 1 then campus else null end) campus
            from (
                select yearType,
                    surveySection,
                    snapshotDate,
                    termCode, 
                    termOrder,
                    termType,
                    fullTermOrder,
                    personId,
                    isNonDegreeSeeking,
                    studentType,
                    studentLevel,
					campus,
                    row_number() over (
                                partition by
                                    personId,
                                    yearType
                            order by isNonDegreeSeeking asc,
                                    fullTermOrder asc, --all standard length terms first
                                    termOrder asc, --order by term to find first standard length term
                                    startDate asc --get record for term with earliest start date (consideration for parts of term only)
                            ) NDSRn,
                    row_number() over (
                                partition by
                                    personId,
                                    yearType
                            order by fullTermOrder asc, --all standard length terms first
                                    termOrder asc, --order by term to find first standard length term
                                    startDate asc --get record for term with earliest start date (consideration for parts of term only)
                            ) FFTRn
                      from StudentMCR stu
                    )
                )
            group by personId, yearType
       ) stu    
    inner join AcademicTermReportingRefactor acadTermCode on acadTermCode.termCode = stu.firstFullTerm
        and acadTermCode.partOfTermCode = acadTermCode.maxPOT
        and coalesce(acadTermCode.yearType, 'CY') = coalesce(stu.yearType, 'CY')
    left join CampusMCR campus on stu.campus = campus.campus
   )
where regCampRn = 1 
and isInternational = false
and studentLevel != 'GR'
and isNonDegreeSeeking = false
),

CourseSectionMCR as (
--Included to get enrollment hours of a CRN
    
select *
from (
    select stu.yearType,
        reg.surveySection surveySection,
        reg.snapshotDate snapshotDate,
        stu.snapshotDate stuSSD,
        to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') courseSectionSSD,
        reg.termCode termCode,
        reg.partOfTermCode partOfTermCode,
        reg.financialAidYear,
        reg.censusDate,
        reg.termType,
        reg.termOrder,
        reg.requiredFTCreditHoursUG,
	    reg.requiredFTClockHoursUG,
	    reg.instructionalActivityType,
	    stu.personId personId,
        stu.studentLevel,
	    stu.studentType,
	    stu.isNonDegreeSeeking,
        to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
        reg.crn,
        reg.crnLevel,
        coursesectENT.subject,
        coursesectENT.courseNumber,
        coursesectENT.section,
        coursesectENT.enrollmentHours,
        reg.equivCRHRFactor,
        reg.isInternational,
        coursesectENT.isClockHours,
        reg.crnGradingMode,
        row_number() over (
                partition by
                    reg.yearType,
                    reg.surveySection,
                    reg.termCode,
                    reg.partOfTermCode,
                    reg.personId,
                    reg.crn,
                    coursesectENT.crn,
                    reg.crnLevel,
                    coursesectENT.subject,
                    coursesectENT.courseNumber
                order by
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') = reg.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') > reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') < reg.snapshotDate then to_date(coursesectENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    coursesectENT.recordActivityDate desc
            ) courseRn
    from RegistrationMCR reg   
        inner join StudentRefactor stu on stu.personId = reg.personId
            and stu.firstFullTerm = reg.termCode
            and coalesce(reg.yearType, 'CY') = coalesce(stu.yearType, 'CY')
            and stu.surveySection = reg.surveySection
        left join CourseSection coursesectENT on reg.termCode = coursesectENT.termCode
            and reg.partOfTermCode = coursesectENT.partOfTermCode
            and reg.crn = upper(coursesectENT.crn)
            and coursesectENT.isIpedsReportable = 1
            and ((to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                    and to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') <= reg.censusDate
				    and coursesectENT.sectionStatus = 'Active')
                or to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))  
    )
where courseRn = 1
),

CourseSectionScheduleMCR as (
--Returns course scheduling related info for the registration CRN. 

select *
from (
	select coursesect.yearType yearType,
	    coursesect.surveySection surveySection,
	    coursesect.snapshotDate snapshotDate, 
	    coursesect.courseSectionSSD courseSectionSSD,
	    to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') courseSectSchedSSD,
	    coursesect.termCode termCode,
	    coursesect.partOfTermCode partOfTermCode,
	    coursesect.financialAidYear,
		coursesect.censusDate censusDate,
		coursesect.termType termType,
		coursesect.termOrder termOrder, 
		coursesect.requiredFTCreditHoursUG,
	    coursesect.requiredFTClockHoursUG,
	    coursesect.instructionalActivityType,
        coursesect.personId personId,
	    coursesect.studentLevel,
	    coursesect.studentType,
	    coursesect.isNonDegreeSeeking,
		to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
	    coursesect.crn crn,
		coursesect.subject subject,
		coursesect.courseNumber courseNumber,
		coursesect.section section,
		coursesectschedENT.section schedSection,
		coursesect.crnLevel crnLevel,
		coursesect.enrollmentHours enrollmentHours,
		coursesect.equivCRHRFactor equivCRHRFactor,
		coursesect.isInternational isInternational,
		coursesect.isClockHours isClockHours,
        coursesect.crnGradingMode crnGradingMode,
        coalesce(coursesectschedENT.meetingType, 'Classroom/On Campus') meetingType,
		row_number() over (
			partition by
			    coursesect.yearType,
			    coursesect.surveySection,
			    coursesect.termCode, 
				coursesect.partOfTermCode,
                coursesect.personId,
			    coursesect.crn,
			    coursesect.crnLevel,
			    coursesect.subject,
                coursesect.courseNumber
			order by
			    (case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') = coursesect.snapshotDate then 1 else 2 end) asc,
                (case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') > coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') < coursesect.snapshotDate then to_date(coursesectschedENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
			    coursesectschedENT.recordActivityDate desc
		) courseSectSchedRn
	from CourseSectionMCR coursesect
	    left join CourseSectionSchedule coursesectschedENT ON coursesect.termCode = coursesectschedENT.termCode 
			            and coursesect.partOfTermCode = coursesectschedENT.partOfTermCode
			            and coursesect.crn = upper(coursesectschedENT.crn)
			            and coursesectschedENT.isIpedsReportable = 1 
	                    and ((to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                            and to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') <= coursesect.censusDate)
                                or to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))  
	)
where courseSectSchedRn = 1
),

CourseMCR as (
--Included to get course type information

select *
from (
	select coursesectsched.yearType yearType,
	    coursesectsched.surveySection surveySection,
	    coursesectsched.snapshotDate snapshotDate,
	    coursesectsched.courseSectionSSD courseSectionSSD,
	    coursesectsched.courseSectSchedSSD courseSectSchedSSD,
	    to_date(courseENT.snapshotDate, 'YYYY-MM-DD') courseSSD,
	    coursesectsched.termCode termCode,
		coursesectsched.partOfTermCode partOfTermCode,
		coursesectsched.financialAidYear,
	    termorder.termOrder courseTermOrder,
	    coursesectsched.termOrder termOrder,
	    coursesectsched.censusDate censusDate,
	    coursesectsched.termType termType,
	    coursesectsched.requiredFTCreditHoursUG,
	    coursesectsched.requiredFTClockHoursUG,
	    coursesectsched.instructionalActivityType,
        coursesectsched.personId personId,
	    coursesectsched.studentType,
	    coursesectsched.studentLevel,
	    coursesectsched.isNonDegreeSeeking,
	    coursesectsched.crn crn,
		coursesectsched.section section,
		coursesectsched.section schedSection,
		coursesectsched.subject subject,
		coursesectsched.courseNumber courseNumber,
		coursesectsched.crnLevel courseLevel,
		coalesce(courseENT.isRemedial, 0) isRemedial,
		coalesce(courseENT.isESL, 0) isESL,
		coursesectsched.meetingType meetingType,
		coursesectsched.enrollmentHours enrollmentHours,
		coursesectsched.isClockHours isClockHours,
        coursesectsched.equivCRHRFactor equivCRHRFactor,
        coursesectsched.crnGradingMode crnGradingMode,
        coursesectsched.isInternational isInternational,
	    to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
	    row_number() over (
			partition by
			    coursesectsched.yearType,
                coursesectsched.surveySection,
			    coursesectsched.termCode, 
				coursesectsched.partOfTermCode,
                coursesectsched.personId,
			    coursesectsched.crn,
			    coursesectsched.crnLevel,
			    coursesectsched.subject,
                coursesectsched.courseNumber
			order by
			    (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') = coursesectsched.snapshotDate then 1 else 2 end) asc,
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') > coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                (case when to_date(courseENT.snapshotDate, 'YYYY-MM-DD') < coursesectsched.snapshotDate then to_date(courseENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
			    termorder.termOrder desc,
			    courseENT.recordActivityDate desc
		) courseRn
	from CourseSectionScheduleMCR coursesectsched
	    left join Course courseENT on coursesectsched.subject = upper(courseENT.subject) 
			        and coursesectsched.courseNumber = upper(courseENT.courseNumber) 
			        and coursesectsched.crnLevel = courseENT.courseLevel 
			        and courseENT.isIpedsReportable = 1
			        and ((to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
				        and to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') <= coursesectsched.censusDate
				        and courseENT.courseStatus = 'Active') 
					        or to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
		left join AcademicTermOrder termorder on termorder.termCode = courseENT.termCodeEffective
            and termorder.termOrder <= coursesectsched.termOrder
	)
where courseRn = 1
),

/*****
BEGIN SECTION - Student Counts
This set of views is used to transform and aggregate records from MCR views above for unduplicated student count
*****/

CourseTypeCountsSTU as (
-- View used to break down course category type counts for student

select yearType,
    surveySection,
    snapshotDate,
    censusDate,
    termOrder,
    financialAidYear,
    personId,
    (case when studentType = 'First Time' and timeStatus = 'FT' then '1' -- First-time, full-time entering students (FTFT)
          when studentType = 'First Time' and timeStatus = 'PT' then '2' -- First-time, part-time entering students (FTPT)
          when timeStatus = 'FT' then '3' -- Non-first-time, full-time entering students (NFTFT)
          when timeStatus = 'PT' then '4' -- Non-first-time, part-time entering students (NFTPT)
    end) cohortType,
    config.financialAidEndDate,
    config.repPeriodTag1,
    config.repPeriodTag2,
    config.repPeriodTag3,
    config.earliestStatusDate,
    config.midStatusDate,
    config.latestStatusDate,
    config.ncSchoolCode,
    config.ncBranchCode,
    config.reportingDateStart,
    config.surveyCloseDate
from ( 
    select yearType,
            surveySection,
            snapshotDate,
            censusDate,
            termOrder,
            financialAidYear,
            personId,
            --(case when studentLevel = 'UG' or isNonDegreeSeeking = true then
                    (case when instructionalActivityType in ('CR', 'B') then 
                                (case when totalCreditHrsCalc >= requiredFTCreditHoursUG then 'FT' else 'PT' end)
                          when instructionalActivityType = 'CL' then 
                                (case when totalClockHrs >= requiredFTClockHoursUG then 'FT' else 'PT' end) 
                          else 'UG null' end)
            --    else null end) 
             timeStatus,
            studentLevel,
            studentType,
            isNonDegreeSeeking,
            (case when totalCredCourses > 0 --exclude students not enrolled for credit
                            then (case when totalESLCourses = totalCredCourses then 0 --exclude students enrolled only in ESL courses/programs
                                       when totalCECourses = totalCredCourses then 0 --exclude students enrolled only in continuing ed courses (non-degree seeking)
                                       when totalOccCourses = totalCredCourses then 0 --exclude students enrolled only in occupational courses (non-degree seeking)
                                       when totalIntlCourses = totalCredCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
                                       when totalAuditCourses = totalCredCourses then 0 --exclude students exclusively auditing classes
                                       -- when... then 0 --exclude PHD residents or interns
                                       -- when... then 0 --exclude students in experimental Pell programs
                                       else 1
                                  end)
                  when totalRemCourses = totalCourses and isNonDegreeSeeking = 0 then 1 --include students taking remedial courses if degree-seeking
                  else 0 
             end) ipedsInclude
    from (
         select course.yearType yearType,
                course.surveySection surveySection,
                course.snapshotDate,
                course.censusDate censusDate,
                course.termOrder termOrder,
                course.financialAidYear,
                course.instructionalActivityType,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId personId,
                course.studentLevel,
                course.studentType,
                course.isNonDegreeSeeking,
                sum((case when course.enrollmentHours >= 0 then 1 else 0 end)) totalCourses,
                sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 then course.enrollmentHours else 0 end)) totalCreditHrs,
                sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then coalesce(course.enrollmentHours, 0) else 0 end)) totalCreditUGHrs,
                --sum((case when course.isClockHours = 0 and course.enrollmentHours > 0 and course.courseLevel = 'Graduate' then coalesce(course.enrollmentHours, 0) else 0 end)) totalCreditGRHrs,
                sum((case when course.isClockHours = 1 and course.enrollmentHours > 0 and course.courseLevel = 'Undergrad' then course.enrollmentHours else 0 end)) totalClockHrs,
                --sum((case when course.enrollmentHours = 0 then 1 else 0 end)) totalNonCredCourses,
                sum((case when course.enrollmentHours > 0 then 1 else 0 end)) totalCredCourses,
                --sum((case when course.meetingType = 'Online/Distance Learning' then 1 else 0 end)) totalDECourses,
                --sum((case when course.courseLevel = 'Undergrad' then 1 else 0 end)) totalUGCourses,
                --sum((case when course.courseLevel = 'Graduate' then 1 else 0 end)) totalGRCourses,
                sum((case when course.courseLevel = 'Continuing Ed' then 1 else 0 end)) totalCECourses,
                sum((case when course.courseLevel = 'Occupational/Professional' then 1 else 0 end)) totalOccCourses,
                sum((case when course.isESL = 1 then 1 else 0 end)) totalESLCourses,
                sum((case when course.isRemedial = 1 then 1 else 0 end)) totalRemCourses,
                sum((case when course.isInternational = 1 then 1 else 0 end)) totalIntlCourses,
                sum((case when course.crnGradingMode = 'Audit' then 1 else 0 end)) totalAuditCourses,
                sum((case when course.courseLevel = 'Undergrad' then
                        (case when course.instructionalActivityType in ('CR', 'B') and course.isClockHours = 0 then course.enrollmentHours
                              when course.instructionalActivityType = 'B' and course.isClockHours = 1 then course.equivCRHRFactor * course.enrollmentHours
                              else 0 end)
                    else 0 end)) totalCreditHrsCalc
        from CourseMCR course
        group by course.yearType,
                course.surveySection,
                course.snapshotDate,
                course.censusDate,
                course.termOrder,
                course.financialAidYear,
                course.instructionalActivityType,
                course.requiredFTCreditHoursUG,
                course.requiredFTClockHoursUG,
                course.personId,
                course.studentLevel,
                course.studentType,
                course.isNonDegreeSeeking
        )
    )
    cross join (select first(financialAidEndDate) financialAidEndDate,
                        first(repPeriodTag1) repPeriodTag1,
                        first(repPeriodTag2) repPeriodTag2,
                        first(repPeriodTag3) repPeriodTag3,
                        first(earliestStatusDate) earliestStatusDate,
                        first(midStatusDate) midStatusDate,
                        first(latestStatusDate) latestStatusDate,
                        first(ncSchoolCode) ncSchoolCode,
                        first(ncBranchCode) ncBranchCode,
                        first(reportingDateStart) reportingDateStart,
                        first(surveyCloseDate) surveyCloseDate
                    from ClientConfigMCR) config
where ipedsInclude = 1
),

FinancialAidMCR as (
-- View to determine if student received a Pell Grant

select course2.personId personId,
        course2.yearType yearType,
        course2.financialAidYear financialAidYear,
        course2.censusDate censusDate,
        course2.termOrder termOrder,
        course2.cohortType cohortType,
        (case when sum(finaid.IPEDSOutcomeMeasuresAmount) > 0 then 1 else 2 end) recipientType,
        course2.financialAidEndDate,
        course2.repPeriodTag1,
        course2.repPeriodTag2,
        course2.repPeriodTag3,
        course2.earliestStatusDate,
        course2.midStatusDate,
        course2.latestStatusDate,
        course2.ncSchoolCode,
        course2.ncBranchCode,
        course2.reportingDateStart,
        course2.surveyCloseDate
from CourseTypeCountsSTU course2
    left join (  
        select DISTINCT course.yearType yearType,
            course.surveySection surveySection,
            to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') snapshotDateFA,
            course.financialAidYear financialAidYear,
            course.personId personId,
            FinancialAidENT.fundType fundType,
            FinancialAidENT.fundCode fundCode,
            FinancialAidENT.fundSource fundSource,
            FinancialAidENT.recordActivityDate recordActivityDate,
            FinancialAidENT.termCode termCode,
            FinancialAidENT.awardStatus awardStatus,
            FinancialAidENT.isPellGrant isPellGrant,
            FinancialAidENT.acceptedAmount acceptedAmount,
            FinancialAidENT.offeredAmount offeredAmount,
            FinancialAidENT.paidAmount paidAmount,
            (case when FinancialAidENT.IPEDSOutcomeMeasuresAmount is not null and FinancialAidENT.IPEDSOutcomeMeasuresAmount > 0 then FinancialAidENT.IPEDSOutcomeMeasuresAmount
                 else FinancialAidENT.paidAmount
            end) IPEDSOutcomeMeasuresAmount,
            FinancialAidENT.isIPEDSReportable isIPEDSReportable,
            row_number() over (
                partition by
                     course.yearType,
                     course.surveySection,
                     course.financialAidYear,
                     FinancialAidENT.termCode,
                     course.personId
		        order by 	
                    (case when array_contains(FinancialAidENT.tags, course.repPeriodTag2) then 1 else 2 end) asc,
                    (case when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') between date_sub(course.financialAidEndDate, 30) and date_add(course.financialAidEndDate, 10) then 1 else 2 end) asc,	    
                    (case when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') > course.financialAidEndDate then to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') < course.financialAidEndDate then to_date(FinancialAidENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    FinancialAidENT.recordActivityDate desc,
                    (case when FinancialAidENT.awardStatus in ('Source Offered', 'Student Accepted') then 1 else 2 end) asc
            ) finAidRn
        from CourseTypeCountsSTU course   
        inner join FinancialAid FinancialAidENT on course.personId = FinancialAidENT.personId
	        and course.financialAidYear = FinancialAidENT.financialAidYear
	        and FinancialAidENT.isPellGrant = 1
	        and (FinancialAidENT.paidAmount > 0 
	            or FinancialAidENT.IPEDSOutcomeMeasuresAmount > 0)
	        and FinancialAidENT.awardStatus not in ('Source Denied', 'Cancelled')
		    and FinancialAidENT.isIpedsReportable = 1
		    and ((to_date(FinancialAidENT.awardStatusActionDate , 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
			        and to_date(FinancialAidENT.awardStatusActionDate , 'YYYY-MM-DD') <= course.financialAidEndDate)
                or (to_date(FinancialAidENT.awardStatusActionDate , 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                    and ((to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                            and to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD') <= course.financialAidEndDate)
                        or to_date(FinancialAidENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))))
        ) finaid on course2.personId = finaid.personId
	        and course2.financialAidYear = finaid.financialAidYear
	        and course2.yearType = finaid.yearType
	        and course2.surveySection = finaid.surveySection
            and finaid.finAidRn = 1
group by course2.personId, 
        course2.cohortType, 
        course2.yearType,
        course2.financialAidYear,
        course2.censusDate,
        course2.termOrder,
        course2.financialAidEndDate,
        course2.repPeriodTag1,
        course2.repPeriodTag2,
        course2.repPeriodTag3,
        course2.earliestStatusDate,
        course2.midStatusDate,
        course2.latestStatusDate,
        course2.ncSchoolCode,
        course2.ncBranchCode,
        course2.reportingDateStart,
        course2.surveyCloseDate
),

AwardMCR as (
--Pulls all distinct student awards obtained as-of four year status date '2010-08-31'

select cohort2.personId personId,
        cohort2.cohortType cohortType,
        cohort2.recipientType recipientType,
        awardData.awardedDate awardedDate,
        (case when awardData.awardRn = 1 then awardData.degree else null end) degree,
        (case when awardData.awardRn = 1 then awardData.degreeLevel else null end) degreeLevel, 
        (case when awardData.awardRn = 1 then awardData.degreeProgram else null end) degreeProgram,
        cohort2.termOrder termOrder,       
        cohort2.repPeriodTag1,
        cohort2.repPeriodTag2,
        cohort2.repPeriodTag3,
        cohort2.earliestStatusDate,
        cohort2.midStatusDate,
        cohort2.latestStatusDate,
        cohort2.ncSchoolCode,
        cohort2.ncBranchCode,
        cohort2.reportingDateStart,
        cohort2.surveyCloseDate
from FinancialAidMCR cohort2
    left join
        (select distinct awardENT.personId personId,
                row_number() over (
                partition by
                    awardENT.personId,
                    awardENT.awardedDate,
                    awardENT.degreeProgram,
                    awardENT.degreeLevel,
                    awardENT.degree
                order by
                    (case when array_contains(awardENT.tags, cohort.repPeriodTag1) then 1 end) asc,
                    awardENT.snapshotDate desc,
                    awardENT.recordActivityDate desc
                ) as awardRn,
                to_date(awardENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
                to_date(awardENT.awardedDate, 'YYYY-MM-DD') awardedDate,
                to_date(awardENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
                upper(awardENT.degree) degree,
                upper(awardENT.degreeProgram) degreeProgram,
                awardENT.degreeLevel degreeLevel,
                coalesce(campus.isInternational, false) isInternational
            from FinancialAidMCR cohort
                inner join Award awardENT on cohort.personId = awardENT.personId
                    and awardENT.isIpedsReportable = 1
                    and awardENT.awardStatus = 'Awarded'
                    and awardENT.degreeLevel is not null
                    and awardENT.awardedDate is not null
                    and awardENT.degreeLevel != 'Continuing Ed'
                    and to_date(awardENT.awardedDate,'YYYY-MM-DD') between cohort.censusDate and cohort.latestStatusDate
                    and ((to_date(awardENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                            and to_date(awardENT.recordActivityDate,'YYYY-MM-DD') <= cohort.latestStatusDate)
                        or to_date(awardENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
                inner join AcademicTermOrder termorder on termorder.termCode = awardENT.awardedTermCode
                left join CampusMCR campus on upper(awardENT.campus) = campus.campus
		) awardData on cohort2.personId = awardData.personId
            and awardData.isInternational = false
            and ((awardData.awardRn = 1 or awardData.awardRn is null)
    ) 
),

DegreeMCR as (
-- Pulls degree information as of the reporting period

select cohort2.personId personId,
        cohort2.cohortType cohortType,
        cohort2.recipientType recipientType,
        (case when (awardDeg.fourYrMax + awardDeg.sixYrMax + awardDeg.eightYrMax) > 0 then 1 else 0 end) awardInd,
        coalesce(awardDeg.fourYrMax, 0) fourYrMax,
        coalesce(awardDeg.sixYrMax, 0) sixYrMax,
        coalesce(awardDeg.eightYrMax, 0) eightYrMax,
        cohort2.termOrder termOrder,        
        cohort2.repPeriodTag1,
        cohort2.repPeriodTag2,
        cohort2.repPeriodTag3,
        cohort2.earliestStatusDate,
        cohort2.midStatusDate,
        cohort2.latestStatusDate,
        cohort2.ncSchoolCode,
        cohort2.ncBranchCode,
        cohort2.reportingDateStart,
        cohort2.surveyCloseDate
from FinancialAidMCR cohort2 --AwardMCR brings back one record per student per award, but we need one record per student on this join
    left join
        (select personId,
                max((case when awardEarliestStatus = 1 then awardLevelNo else 0 end)) fourYrMax,
                max((case when awardEarliestStatus = 1 or awardMidStatus = 1 then awardLevelNo else 0 end)) sixYrMax,
                max((case when awardEarliestStatus = 1 or awardMidStatus = 1 or awardLatestStatus = 1 then awardLevelNo else 0 end)) eightYrMax
        from (
            select award.personId personId,
                award.degree degree,
                award.degreeLevel degreeLevel,
                award.degreeProgram degreeProgram,
                award.awardedDate,
                (case when award.awardedDate < award.earliestStatusDate then 1 end) awardEarliestStatus,
                (case when award.awardedDate between award.earliestStatusDate and award.midStatusDate then 1 end) awardMidStatus,
                (case when award.awardedDate between award.midStatusDate and award.latestStatusDate then 1 end) awardLatestStatus,
                degreeENT.awardLevel awardLevel,
                (case when degreeENT.awardLevel = 'Associates Degree' then 2                   
                        when degreeENT.awardLevel = 'Bachelors Degree' then 3 
                        when degreeENT.awardLevel in ('Postsecondary (<300 clock/<9 semester credit/<13 quarter credit hours)', 
                                                    'Postsecondary (300-899 clock/9-29 semester credit/13-44 quarter credit hours)', 
                                                    'Postsecondary (900-1800 clock/30-60 semester credit/45-90 quarter credit hours)',
                                                    'Postsecondary (>1800 clock/>60 semester credit/>90 quarter credit hours)') then 1
                        when upper(degreeENT.degreeDescription) like '%CERT%' then 1 else 0 end) awardLevelNo,
                row_number() over (
			        partition by
			            award.personId,
			            award.awardedDate,
			            award.degree,
				        degreeENT.awardLevel
			        order by
			            (case when array_contains(degreeENT.tags, award.repPeriodTag1) then 1 else 2 end) asc,
                        (case when to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') between date_sub(award.latestStatusDate, 1) and date_add(award.latestStatusDate, 3) then 1 else 2 end) asc,	    
                        (case when to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') > award.latestStatusDate then to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                        (case when to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') < award.latestStatusDate then to_date(degreeENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
			            degreeENT.recordActivityDate desc
		        ) as degreeRn
            from AwardMCR award
                inner join Degree degreeENT on award.degree = upper(degreeENT.degree)
                    and award.degreeLevel = degreeENT.degreeLevel
                    and degreeENT.awardLevel is not null
                    and degreeENT.isIpedsReportable = 1
                    and degreeENT.isNonDegreeSeeking = 0
                    and ((to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                            and to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') <= award.latestStatusDate)
                        or to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
        )
        where degreeRn = 1
        group by personId
    ) awardDeg on cohort2.personId = awardDeg.personId
),

TransferMCR as (
--Pulls students who left the institution and can be removed from the cohort for one of the IPEDS allowable reasons

select cohort2.personId personId,
        cohort2.cohortType cohortType,
        cohort2.recipientType recipientType,
        cohort2.awardInd awardInd,
        (case when transf.personId is not null then 1 else 0 end) transferInd,
        cohort2.fourYrMax fourYrMax,
        cohort2.fourYrMax sixYrMax,
        cohort2.fourYrMax eightYrMax,
        cohort2.termOrder termOrder,   
        cohort2.repPeriodTag1,
        cohort2.repPeriodTag2,
        cohort2.repPeriodTag3,
        cohort2.earliestStatusDate,
        cohort2.midStatusDate,
        cohort2.latestStatusDate,
        cohort2.ncSchoolCode,
        cohort2.ncBranchCode,
        cohort2.reportingDateStart,
        cohort2.surveyCloseDate
from DegreeMCR cohort2
    left join
        (select transferENT.personId personId,
                row_number() over (
                    partition by
                        transferENT.personId
                    order by
                        (case when array_contains(transferENT.tags, deg.repPeriodTag3) then 1 else 2 end) asc,
                        transferENT.snapshotDate desc,
                        transferENT.recordActivityDate desc
                    ) transRn
        from DegreeMCR deg
            inner join Transfer transferENT on deg.personId = transferENT.personId
                and to_date(transferENT.startDate,'YYYY-MM-DD') >= deg.reportingDateStart
                and (transferENT.collegeCodeBranch != concat(deg.ncSchoolCode, '-', deg.ncBranchCode)
                    and transferENT.collegeCodeBranch != concat(deg.ncSchoolCode, deg.ncBranchCode))
                and ((to_date(transferENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                        and to_date(transferENT.recordActivityDate,'YYYY-MM-DD') <= deg.surveyCloseDate)
                    or to_date(transferENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
        where deg.awardInd = 0
    ) transf on cohort2.personId = transf.personId
        and transf.transRn = 1
),

CohortExclusionMCR as (
--Pulls students who left the institution and can be removed from the cohort for one of the IPEDS allowable reasons

select cohort2.personId personId,
        cohort2.cohortType cohortType,
        cohort2.recipientType recipientType,
        cohort2.awardInd awardInd,  
        cohort2.transferInd transferInd,
        (case when exclusion.personId is not null then 1 else 0 end) exclusionInd,
        (case when exclusion.personId is null and regStatus.personId is not null then 1 else 0 end) enrolledInd,
        cohort2.fourYrMax fourYrMax,
        cohort2.sixYrMax sixYrMax,
        cohort2.eightYrMax eightYrMax,
        cohort2.termOrder termOrder, 
        cohort2.repPeriodTag1,
        cohort2.repPeriodTag2,
        cohort2.repPeriodTag3,
        cohort2.earliestStatusDate,
        cohort2.midStatusDate,
        cohort2.latestStatusDate,
        cohort2.ncSchoolCode,
        cohort2.ncBranchCode,
        cohort2.reportingDateStart
from TransferMCR cohort2
    left join
        ( select exclusionENT.personId personId,
                to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
                row_number() over (
                    partition by
                        exclusionENT.personId
                      order by 	
                        (case when array_contains(exclusionENT.tags, stu.repPeriodTag1) then 1 else 2 end) asc,
                        (case when to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') between date_sub(stu.latestStatusDate, 1) and date_add(stu.latestStatusDate, 3) then 1 else 2 end) asc,	    
                        (case when to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') > stu.latestStatusDate then to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                        (case when to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') < stu.latestStatusDate then to_date(exclusionENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                        termorder.termOrder desc,
                        exclusionENT.recordActivityDate desc
                ) exclRn
        from TransferMCR stu
            inner join CohortExclusion exclusionENT on stu.personId = exclusionENT.personId
                and exclusionENT.exclusionReason in ('Died', 'Medical Leave', 'Military Leave', 'Foreign Aid Service', 'Religious Leave')
                and exclusionENT.isIPEDSReportable = 1
                and ((to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                    and to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') <= stu.latestStatusDate)
                        or to_date(exclusionENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
            inner join AcademicTermOrder termorder on exclusionENT.termCodeEffective = termorder.termCode
                and termorder.termOrder >= stu.termOrder
        where stu.transferInd = 0
    ) exclusion on cohort2.personId = exclusion.personId
            and exclusion.exclRn = 1
    left join RegStatusMCR regStatus on cohort2.personId = regStatus.personId
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
                studentRecip.recipientType recipientType,
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
                from CohortExclusionMCR 
                ) cohort on studentCohort.cohortType = cohort.cohortType
                    and studentRecip.recipientType = cohort.recipientType
                group by studentCohort.cohortType,
                        studentRecip.recipientType
            )
       cross join FormatPartValues partval
    )   group by surveyParts,
       cohortType, 
       recipientType
