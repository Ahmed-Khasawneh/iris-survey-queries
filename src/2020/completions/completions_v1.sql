/********************

EVI PRODUCT:    DORIS 2020-21 IPEDS Survey Fall Collection
FILE NAME:      Completions
FILE DESC:      Completions for all institutions
AUTHOR:         Ahmed Khasawneh
CREATED:        20200914

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Cohort Creation
Award Level Offerings 
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)      	Author             	Tag             	Comments
-----------------   --------------------	-------------   	-------------------------------------------------
20200914            akhasawneh          			    		Initial version (Run time 30m)
	
Implementation Notes:

The following changes were implemented for the 2020-21 data collection period:

	- There is a new distance education question on the CIP Data screen.
	- Subbaccalaureate certificates that are less than one year in length have been segmented into two subcategories based on duration.
	- There is a new FAQ elaborating on certificates that should be included in this component.
                     
********************/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.
--IPEDS specs define reporting period as July 1, 2018 and June 30, 2019.
 
select '2021' surveyYear,
	'COM' surveyId,
	CAST('2019-07-01' AS DATE) reportingDateStart,
	CAST('2020-06-30' AS DATE) reportingDateEnd,
	'202010' termCode, 
	'1' partOfTermCode, 
	CAST('2020-10-15' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term

/*
select '1415' surveyYear,
	'COM' surveyId,
	CAST('2013-07-01' AS DATE) reportingDateStart,
	CAST('2014-06-30' AS DATE) reportingDateEnd, 
	'201430' termCode,
	'1' partOfTermCode,
	CAST('2014-06-01' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
    
union

select '1415' surveyYear,  
	'COM' surveyId,  
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201430' termCode,
	'A' partOfTermCode, 
	CAST('2014-06-01' AS DATE) censusDate, 
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term

union

select '1415' surveyYear,  
	'COM' surveyId,  
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201430' termCode,
	'B' partOfTermCode, 
	CAST('2014-07-10' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term

union

select '1415' surveyYear,  
	'COM' surveyId,  
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201330' termCode,
	'1' partOfTermCode,
	CAST('2013-06-10' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term

union

select '1415' surveyYear,  
	'COM' surveyId,  
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201330' termCode,
	'A' partOfTermCode, 
	CAST('2013-06-10' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term

union

select '1415' surveyYear,  
	'COM' surveyId,  
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201330' termCode,
	'B' partOfTermCode, 
	CAST('2013-07-10' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
    
union

select '1415' surveyYear,
	'COM' surveyId,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201410' termCode,
	'1' partOfTermCode,
	CAST('2013-09-13' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
    
union

select '1415' surveyYear,
	'COM' surveyId,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
	'201410' termCode,
	'A' partOfTermCode, 
	CAST('2013-09-13' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
    
union

select '1415' surveyYear,
	'COM' surveyId,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201410' termCode,
	'B' partOfTermCode, 
	CAST('2013-11-08' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
    
union
select '1415' surveyYear,
	'COM' surveyId,
	CAST('2013-07-01' AS DATE) reportingDateStart,
	CAST('2014-06-30' AS DATE) reportingDateEnd, 
	'201420' termCode,
	'1' partOfTermCode, 
	CAST('2014-01-13' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term    
    
union 

select '1415' surveyYear,
	'COM' surveyId,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
	'201420' termCode,
	'A' partOfTermCode,
	CAST('2014-01-10' AS DATE) censusDate,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
*/
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

--          1st union 1st order - pull snapshot for 'Full Year Term End' where compGradDateOrTerm = 'T' or 'Full Year June End' where compGradDateOrTerm = 'D' 
--          1st union 2nd order - pull snapshot for 'Full Year Term End' or 'Full Year June End'
--          1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--          2nd union - pull default values if no record in IPEDSClientConfig

select DISTINCT
    ConfigLatest.surveyYear surveyYear,
    upper(ConfigLatest.surveyId) surveyId,
    ConfigLatest.termCode termCode,
    ConfigLatest.partOfTermCode partOfTermCode,
    ConfigLatest.reportingDateStart reportingDateStart,
    ConfigLatest.reportingDateEnd reportingDateEnd,
    upper(ConfigLatest.acadOrProgReporter) acadOrProgReporter,
    upper(ConfigLatest.genderForUnknown) genderForUnknown,
	upper(ConfigLatest.genderForNonBinary) genderForNonBinary,
    upper(ConfigLatest.compGradDateOrTerm) compGradDateOrTerm,
    ConfigLatest.snapshotDate snapshotDate--,
    --ConfigLatest.tags
from (
	select clientConfigENT.surveyCollectionYear surveyYear,
		clientConfigENT.snapshotDate snapshotDate, 
		--clientConfigENT.tags tags,
		defvalues.surveyId surveyId,
		defvalues.termCode termCode,
		defvalues.partOfTermCode partOfTermCode,
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		coalesce(clientConfigENT.acadOrProgReporter, defvalues.acadOrProgReporter) acadOrProgReporter,
		coalesce(clientConfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		coalesce(clientConfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
		coalesce(clientConfigENT.compGradDateOrTerm, defvalues.compGradDateOrTerm) compGradDateOrTerm,
        row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
			    (case when clientConfigENT.compGradDateOrTerm = 'T' 
						and array_contains(clientConfigENT.tags, 'Full Year Term End') then 1
			         when clientConfigENT.compGradDateOrTerm = 'D' 
						and array_contains(clientConfigENT.tags, 'Full Year June End') then 1
			         when array_contains(clientConfigENT.tags, 'Full Year June End') 
						or array_contains(clientConfigENT.tags, 'Full Year Term End') then 2
				 else 3 end) asc,
			    clientConfigENT.snapshotDate desc,
				clientConfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientConfigENT
		cross join DefaultValues defvalues
	where clientConfigENT.surveyCollectionYear = defvalues.surveyYear

	union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select defvalues.surveyYear surveyYear,
	    CAST('9999-09-09' as DATE) snapshotDate,
	    --array() tags,
		defvalues.surveyId surveyId,
		defvalues.termCode termCode,
		defvalues.partOfTermCode partOfTermCode,
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		defvalues.acadOrProgReporter acadOrProgReporter,
		defvalues.genderForUnknown genderForUnknown,
		defvalues.genderForNonBinary genderForNonBinary,
		defvalues.compGradDateOrTerm compGradDateOrTerm,
		1 configRn
	from DefaultValues defvalues
	where defvalues.surveyYear not in (select MAX(configENT.surveyCollectionYear)
										from IPEDSClientConfig configENT
										where configENT.surveyCollectionYear = defvalues.surveyYear)
    ) ConfigLatest
where ConfigLatest.configRn = 1
),

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 

--          1st union 1st order - pull snapshot for 'Full Year Term End' where compGradDateOrTerm = 'T' or 'Full Year June End' where compGradDateOrTerm = 'D' 
--          1st union 2nd order - pull snapshot for 'Full Year Term End' or 'Full Year June End'
--          1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--          2nd union - pull default values if no record in IPEDSReportingPeriod

select DISTINCT
    RepDates.surveyYear surveyYear,
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
    --RepDates.tags tags,
    RepDates.termCode termCode,	
    RepDates.partOfTermCode partOfTermCode,
    RepDates.compGradDateOrTerm compGradDateOrTerm,
    RepDates.genderForUnknown genderForUnknown,
    RepDates.genderForNonBinary genderForNonBinary,
    to_date(RepDates.reportingDateStart,'YYYY-MM-DD') reportingDateStart,
    to_date(RepDates.reportingDateEnd,'YYYY-MM-DD') reportingDateEnd
from (
    select repPeriodENT.surveyCollectionYear surveyYear,
        'repperiod' source,
		repPeriodENT.snapshotDate snapshotDate,
		--repPeriodENT.tags tags,
        clientconfig.surveyId surveyId,
        coalesce(repPeriodENT.termCode, clientconfig.termCode) termCode,
        coalesce(repPeriodENT.partOfTermCode, clientconfig.partOfTermCode) partOfTermCode,
        coalesce(repPeriodENT.reportingDateStart, clientconfig.reportingDateStart) reportingDateStart,
        coalesce(repPeriodENT.reportingDateEnd, clientconfig.reportingDateEnd) reportingDateEnd,
        clientconfig.acadOrProgReporter acadOrProgReporter,
        clientconfig.genderForUnknown genderForUnknown,
        clientconfig.genderForNonBinary genderForNonBinary,
        clientconfig.compGradDateOrTerm compGradDateOrTerm,
        row_number() over (	
            partition by 
                repPeriodENT.surveyCollectionYear,
                repPeriodENT.surveyId,
                repPeriodENT.surveySection,
                repPeriodENT.termCode,
                repPeriodENT.partOfTermCode	
            order by
                (case when clientconfig.compGradDateOrTerm = 'T' 
						and array_contains(repPeriodENT.tags, 'Full Year Term End') then 1
			         when clientconfig.compGradDateOrTerm = 'D' 
						and array_contains(repPeriodENT.tags, 'Full Year June End') then 1
			         when array_contains(repPeriodENT.tags, 'Full Year June End') 
						or array_contains(repPeriodENT.tags, 'Full Year Term End') then 2
				 else 3 end) asc,
			    repPeriodENT.snapshotDate desc,
				repPeriodENT.recordActivityDate desc
        ) reportPeriodRn	
    from IPEDSReportingPeriod repPeriodENT
        inner join ClientConfigMCR clientconfig on repperiodENT.surveyCollectionYear = clientconfig.surveyYear
            and upper(repperiodENT.surveyId) = clientconfig.surveyId
            and repperiodENT.termCode is not null
            and repperiodENT.partOfTermCode is not null

	union
	
--Pulls default values when IPEDSReportingPeriod record doesn't exist
	select clientconfig.surveyYear surveyYear,
		'default' source, 
		clientconfig.snapshotDate snapshotDate,
		--clientconfig.tags tags,
        clientconfig.surveyId surveyId,
        clientconfig.termCode termCode,
        clientconfig.partOfTermCode partOfTermCode,
        clientconfig.reportingDateStart reportingDateStart,
        clientconfig.reportingDateEnd reportingDateEnd,
        clientconfig.acadOrProgReporter acadOrProgReporter,
        clientconfig.genderForUnknown genderForUnknown,
        clientconfig.genderForNonBinary genderForNonBinary,
        clientconfig.compGradDateOrTerm compGradDateOrTerm,
        1 reportPeriodRn
	from ClientConfigMCR clientconfig
	where clientconfig.surveyYear not in (select repperiodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repperiodENT
										  where repperiodENT.surveyCollectionYear = clientconfig.surveyYear
											and upper(repperiodENT.surveyId) = clientconfig.surveyId 
											and repperiodENT.termCode is not null
											and repperiodENT.partOfTermCode is not null)

	)  RepDates
where RepDates.reportPeriodRn = 1
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for each term and part of term code. 
--PartOfTerm code defines a subcategory of a termCode that may have different start, end and census dates. 

select termCode, 
	partOfTermCode,
	to_date(startDate, 'YYYY-MM-DD') startDate,
	to_date(endDate, 'YYYY-MM-DD') endDate,
	academicYear,
	to_date(censusDate, 'YYYY-MM-DD') censusDate,
    snapshotTermCode,
    to_date(snapshotDate, 'YYYY-MM-DD') snapshotDate,
    --tags,
    snapshotPartTermCode
from (
    select distinct acadtermENT.termCode, 
        (case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
            then acadTermENT.termCode 
            else null end) snapshotTermCode,
        (case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
            then acadTermENT.partOfTermCode 
            else null end) snapshotPartTermCode,
        row_number() over (
            partition by
                acadTermENT.termCode,
                acadTermENT.partOfTermCode
            order by
                (case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD')
					and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD')
				then acadTermENT.termCode end) desc,
                (case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD')
					and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD')
				then acadTermENT.partOfTermCode end) desc,
                acadTermENT.snapshotDate desc,
                acadTermENT.recordActivityDate desc
        ) acadTermRn,
        acadTermENT.snapshotDate,
        --acadTermENT.tags,
		acadtermENT.partOfTermCode, 
		acadtermENT.recordActivityDate, 
		acadtermENT.termCodeDescription,       
		acadtermENT.partOfTermCodeDescription, 
		acadtermENT.startDate,
		acadtermENT.endDate,
		acadtermENT.academicYear,
		acadtermENT.censusDate,
		acadtermENT.isIPEDSReportable
	from AcademicTerm acadtermENT 
	where acadtermENT.isIPEDSReportable = 1
	) 
where acadTermRn = 1

union  

select defvalues.termCode termCode, 
	'1' partOfTermCode,
	to_date(defvalues.reportingDateStart, 'YYYY-MM-DD') startDate,
	to_date(defvalues.reportingDateEnd, 'YYYY-MM-DD') endDate,
	null academicYear,
	null censusDate,
    defvalues.termCode snapshotTermCode,
    null snapshotDate,
    --null tags,
    1 snapshotPartTermCode
from DefaultValues defvalues
where not exists (select trm.termCode 
                  from academicTerm trm
                  inner join ReportingPeriodMCR repper
                    on repper.termCode = trm.termCode
                    and trm.termCode is not null)
),

AcademicTermOrder as (
-- Orders term codes based on date span and keeps the numeric value of the greatest term/part of term record. 

select termCode termCode, 
    max(termOrder) termOrder
from (
	select acadterm.termCode termCode,
		row_number() over (
			order by  
				acadterm.startDate asc,
				acadterm.endDate asc
        ) termOrder    
	from AcademicTermMCR acadterm
	)
group by termCode
),

AcademicTermReporting as (
-- Combines ReportingPeriodMCR and AcademicTermMCR to get termCode info only for terms in reporting period
					
select *,
    (case when compGradDateOrTerm = 'D'
        then surveyDateStart
        else termMinStartDate
	end) reportingDateStart,  --minimum start date of reporting for either date or term option for client
    (case when compGradDateOrTerm = 'D'
        then surveyDateEnd
        else termMaxEndDate --termMaxEndDate
	end) reportingDateEnd --maximum end date of reporting for either date or term option for client
from (
    select repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        termorder.termOrder termOrder,
        acadterm.snapshotTermCode snapshotTermCode,
        acadterm.snapshotDate snapshotDate,
        acadterm.startDate startDate,
        acadterm.endDate endDate,
        minTermStart.startDateMin termMinStartDate,
        maxTermEnd.endDateMax termMaxEndDate,
        repperiod.compGradDateOrTerm compGradDateOrTerm,
        repperiod.reportingDateStart surveyDateStart,
        repperiod.reportingDateEnd surveyDateEnd,
        repperiod.genderForUnknown genderForUnknown,
        repperiod.genderForNonBinary genderForNonBinary
    from ReportingPeriodMCR repperiod
		left join AcademicTermOrder termorder
			on termOrder.termCode = repperiod.termCode
        left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
	            and repperiod.partOfTermCode = acadterm.partOfTermCode
		left join (select min(acadterm1.startDate) startDateMin,
						acadterm1.termCode termCode
                    from ReportingPeriodMCR repperiod1
						inner join AcademicTermMCR acadterm1 on repperiod1.termCode = acadterm1.termCode
							and repperiod1.partOfTermCode = acadterm1.partOfTermCode
			         group by acadterm1.termCode
			         ) minTermStart on repperiod.termCode = minTermStart.termCode
		left join (select max(acadterm1.endDate) endDateMax,
						acadterm1.termCode termCode
                    from ReportingPeriodMCR repperiod1
						inner join AcademicTermMCR acadterm1 on repperiod1.termCode = acadterm1.termCode
							and repperiod1.partOfTermCode = acadterm1.partOfTermCode
			         group by acadterm1.termCode
					) maxTermEnd on repperiod.termCode = maxTermEnd.termCode
	)
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

AwardMCR as (
--Pulls all distinct student awards obtained withing the reporting terms/dates.

select DISTINCT *
from (
    select awardENT.personId personId, 
        upper(awardENT.degree) degree, 
        awardENT.degreeLevel degreeLevel, 
        to_date(awardENT.awardedDate, 'YYYY-MM-DD') awardedDate, 
        awardENT.awardedTermCode awardedTermCode, 
        awardENT.awardStatus awardStatus, 
        upper(awardENT.college) college, 
        upper(awardENT.campus) campus, 
        repperiod.termOrder termOrder, 
        repperiod.genderForUnknown genderForUnknown,
        repperiod.genderForNonBinary genderForNonBinary,
        to_date(awardENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
		repperiod.reportingDateEnd reportingDateEnd,
        --awardENT.tags tags,
        repperiod.compGradDateOrTerm compGradDateOrTerm,
        row_number() over (
            partition by     
                awardENT.personId,
                (case when repperiod.compGradDateOrTerm = 'D' 
                    then awardENT.awardedDate
                    else awardENT.awardedTermCode
                end),
                awardENT.degreeLevel,
                awardENT.degree
            order by
                (case when repperiod.compGradDateOrTerm = 'T' and array_contains(awardENT.tags, 'Full Year Term End') then 1
			         when repperiod.compGradDateOrTerm = 'D' and array_contains(awardENT.tags, 'Full Year June End') then 1
			         when array_contains(awardENT.tags, 'Full Year June End') or array_contains(awardENT.tags, 'Full Year Term End') then 2
			         else 3 end) asc,
			    awardENT.snapshotDate desc,
                repperiod.termOrder desc,
                awardENT.recordActivityDate desc
        ) as AwardRn 
	from AcademicTermReporting repperiod
	    inner join Award awardENT on ((repperiod.compGradDateOrTerm = 'D'
			                            and (to_date(awardENT.awardedDate,'YYYY-MM-DD') >= repperiod.reportingDateStart 
				                        and to_date(awardENT.awardedDate,'YYYY-MM-DD') <= repperiod.reportingDateEnd)) 
			or (repperiod.compGradDateOrTerm = 'T'
				and awardENT.awardedTermCode = repperiod.termCode))
            and awardENT.isIpedsReportable = 1
		    and awardENT.awardStatus = 'Awarded'
		    and awardENT.degreeLevel is not null
		    and awardENT.degreeLevel != 'Continuing Ed'
-- Remove for testing...
		and ((to_date(awardENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
		and to_date(awardENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.reportingDateEnd)
				or to_date(awardENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
-- ...Remove for testing
    )
where AwardRn = 1
),

AwardRefactor as (
--Returns all award data with the most current snapshotDate

select award.personId personId,
    award.awardedDate awardedDate,
    award.awardedTermCode awardedTermCode,
    award.degreeLevel degreeLevel,
    award.degree degree,
    award.college college,
    award.campus campus,
    award.termOrder termOrder,
    award.genderForUnknown genderForUnknown,
    award.genderForNonBinary genderForNonBinary,
    award.compGradDateOrTerm compGradDateOrTerm,
    award.reportingDateEnd reportingDateEnd,
    MAX(repperiod.snapshotDate) snapshotDate
from AwardMCR award
    left join AcademicTermReporting repperiod on repperiod.termCode = award.awardedTermCode
where award.awardedDate is not null
group by award.personId, 
    award.awardedDate, 
    award.awardedTermCode, 
    award.degreeLevel, 
    award.degree, 
    award.college, 
    award.campus, 
    award.termOrder, 
    award.genderForUnknown, 
    award.genderForNonBinary, 
    award.compGradDateOrTerm,
    award.reportingDateEnd
),

CampusMCR as ( 
-- Returns most recent campus record for each campus available per the ReportingPeriod.

select *
from (
    select award2.personId personId,
		award2.degree degree,
		award2.awardedDate awardedDate,
		award2.awardedTermCode awardedTermCode,
		award2.termOrder termOrder,
		award2.genderForUnknown genderForUnknown,
		award2.genderForNonBinary genderForNonBinary,
		award2.degreeLevel degreeLevel,
		award2.college college, 
		award2.campus campus,
		award2.reportingDateEnd reportingDateEnd,
		campusRec.isInternational isInternational,
		campusRec.snapshotDate snapshotDate,
        row_number() over (
            partition by
                award2.personId,
                award2.awardedDate,
                award2.degreeLevel,
                award2.degree,
                award2.campus
            order by
                campusRec.recordActivityDate desc
        ) campusRn
    from AwardRefactor award2
		left join (select award.awardedDate awardedDate,
		                upper(campusENT.campus) campus,
                        campusENT.isInternational isInternational,
                        to_date(campusENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
                        to_date(campusENT.snapshotDate,'YYYY-MM-DD') snapshotDate
                    from AwardRefactor award
                        inner join Campus campusENT on upper(campusENT.campus) = award.campus
                            and to_date(campusENT.snapshotDate,'YYYY-MM-DD') = award.snapshotDate
                            and campusENT.isIpedsReportable = 1
                            and award.awardedDate is not null
                            and campusENT.campus is not null
                            and campusENT.snapshotDate is not null
        ) CampusRec on award2.campus = CampusRec.campus 
            and award2.awardedDate = CampusRec.awardedDate
            and award2.snapshotDate = CampusRec.snapshotDate
-- Remove for testing... 
		    and ((CampusRec.recordActivityDate != CAST('9999-09-09' as DATE)
			    and CampusRec.recordActivityDate <= award2.awardedDate)
				    or CampusRec.recordActivityDate = CAST('9999-09-09' as DATE))
-- Remove for testing...
	        and award2.awardedDate is not null
	        and CampusRec.campus is not null
	        and CampusRec.snapshotDate is not null
-- ...Remove for testing
	)
where campusRn = 1
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

select *
from (
    select campus2.personId personId,
		campus2.degree degree,
		campus2.awardedDate awardedDate,
		campus2.awardedTermCode awardedTermCode,
		campus2.termOrder termOrder,
		campus2.genderForUnknown genderForUnknown,
		campus2.genderForNonBinary genderForNonBinary,
		campus2.isInternational isInternational,
		campus2.snapshotDate snapshotDate,
		campus2.degreeLevel degreeLevel,
		campus2.college college, 
		campus2.campus campus,
		campus2.reportingDateEnd reportingDateEnd,
		PersonRec.snapshotDate perSnapshotDate,
		PersonRec.gender gender,
		PersonRec.isHispanic isHispanic,
		PersonRec.isMultipleRaces isMultipleRaces,
		PersonRec.ethnicity ethnicity,
		PersonRec.isInUSOnVisa isInUSOnVisa,
		PersonRec.visaStartDate visaStartDate,
		PersonRec.visaEndDate visaEndDate,
		PersonRec.isUSCitizen isUSCitizen,
		PersonRec.birthDate birthDate,
        row_number() over (
			partition by
				campus2.personId,
				PersonRec.personId,
				campus2.awardedDate,
				campus2.degreeLevel,
				campus2.degree
			order by
				PersonRec.recordActivityDate desc
		) personRn 
    from CampusMCR campus2
        left join (  
                select distinct personENT.personId personId,
                        to_date(personENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
                        to_date(personENT.birthDate,'YYYY-MM-DD') birthDate,
                        personENT.ethnicity ethnicity,
                        personENT.isHispanic isHispanic,
                        personENT.isMultipleRaces isMultipleRaces,
                        personENT.isInUSOnVisa isInUSOnVisa,
                        to_date(personENT.visaStartDate,'YYYY-MM-DD') visaStartDate,
                        to_date(personENT.visaEndDate,'YYYY-MM-DD') visaEndDate,
                        personENT.isUSCitizen isUSCitizen,
                        personENT.gender gender,
                        to_date(personENT.snapshotDate,'YYYY-MM-DD') snapshotDate
                from CampusMCR campus
                    inner join Person personENT on campus.personId = personENT.personId 
                        and to_date(personENT.snapshotDate,'YYYY-MM-DD') = campus.snapshotDate
                        and personENT.isIpedsReportable = 1
                where campus.awardedDate is not null
                    and campus.snapshotDate is not null
                ) PersonRec on campus2.personId = PersonRec.personId 
                and campus2.snapshotDate = PersonRec.snapshotDate
                and ((PersonRec.recordActivityDate != CAST('9999-09-09' AS DATE)
                    and PersonRec.recordActivityDate <= campus2.awardedDate) 
                        or PersonRec.recordActivityDate = CAST('9999-09-09' AS DATE))
    where campus2.awardedDate is not null
        and campus2.snapshotDate is not null
    )
where personRn = 1
),

AcademicTrackMCR as (
--Returns most up to date student academic track information as of their award date and term. 

select personId personId,
    fieldOfStudyActionDate fieldOfStudyActionDate,
    perSnapshotDate perSnapshotDate,
	degree degree,
	college college,
	snapshotDate snapshotDate,
	degreeLevel degreeLevel,
	awardedDate awardedDate,
	awardedTermCode awardedTermCode,
	genderForUnknown genderForUnknown,
	genderForNonBinary genderForNonBinary,
	isInternational isInternational,
	snapshotDate snapshotDate,
	gender gender,
	isHispanic isHispanic,
	isMultipleRaces isMultipleRaces,
	ethnicity ethnicity,
	isInUSOnVisa isInUSOnVisa,
	visaStartDate visaStartDate,
	visaEndDate visaEndDate,
	isUSCitizen isUSCitizen,
	birthDate birthDate,
	termCodeEffective termCodeEffective,
	degreeProgram degreeProgram,
	academicTrackLevel academicTrackLevel,
	fieldOfStudy fieldOfStudy,
	fieldOfStudyType fieldOfStudyType,
	fieldOfStudyPriority fieldOfStudyPriority,
	termOrder termOrder,
	row_number() over (
		partition by
			personId
		order by
			fieldOfStudyPriority asc
	) fosRn
from ( 
    select person2.personId personId,
        AcadTrackRec.fieldOfStudyActionDate fieldOfStudyActionDate,
        person2.perSnapshotDate perSnapshotDate,
		person2.degree degree,
		person2.college college,
        person2.degreeLevel degreeLevel,
		person2.awardedDate awardedDate,
		person2.awardedTermCode awardedTermCode,
		person2.genderForUnknown genderForUnknown,
		person2.genderForNonBinary genderForNonBinary,
		person2.isInternational isInternational,
		person2.snapshotDate snapshotDate,
		person2.gender gender,
		person2.isHispanic isHispanic,
		person2.isMultipleRaces isMultipleRaces,
		person2.ethnicity ethnicity,
		person2.isInUSOnVisa isInUSOnVisa,
		person2.visaStartDate visaStartDate,
		person2.visaEndDate visaEndDate,
		person2.isUSCitizen isUSCitizen,
		person2.birthDate birthDate,	
		AcadTrackRec.termCodeEffective termCodeEffective,
		AcadTrackRec.degreeProgram degreeProgram,
		AcadTrackRec.academicTrackLevel academicTrackLevel,
        AcadTrackRec.fieldOfStudy fieldOfStudy,
		AcadTrackRec.fieldOfStudyType fieldOfStudyType,
		AcadTrackRec.fieldOfStudyPriority fieldOfStudyPriority,
		AcadTrackRec.academicTrackStatus academicTrackStatus,
		AcadTrackRec.termOrder termOrder,
		row_number() over (
			partition by
			    person2.personId,
				AcadTrackRec.personId,
				person2.awardedDate,
                person2.degreeLevel,
                person2.degree,
				AcadTrackRec.degreeProgram,
				AcadTrackRec.fieldOfStudyPriority
			order by
				AcadTrackRec.termOrder desc,
				AcadTrackRec.recordActivityDate desc
		) acadtrackRn
	from PersonMCR person2
	    left join ( 
                select distinct acadtrackENT.personId personId,
                        acadtrackENT.termCodeEffective termCodeEffective,
						to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') fieldOfStudyActionDate,
                        termorder.termOrder termOrder,
                        to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
                        acadtrackENT.academicTrackLevel academicTrackLevel,
						upper(acadtrackENT.degreeProgram) degreeProgram, 
						upper(acadtrackENT.fieldOfStudy) fieldOfStudy,
                        acadtrackENT.fieldOfStudyType fieldOfStudyType,
						acadtrackENT.fieldOfStudyPriority fieldOfStudyPriority,
                        upper(acadtrackENT.degree) degree,
                        upper(acadtrackENT.college) college,
                        upper(acadtrackENT.campus) campus,
                        acadtrackENT.academicTrackStatus academicTrackStatus,
                        to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') snapshotDate
            from PersonMCR person
                inner join AcademicTrack acadtrackENT ON person.personId = acadTrackENT.personId
                    and person.degree = upper(acadTrackENT.degree)
                    and (person.college = upper(acadTrackENT.college)
                        or acadTrackENT.college is null)
                    and (person.campus = upper(acadTrackENT.campus)
                        or acadTrackENT.campus is null)
                    and person.degreeLevel = acadTrackENT.academicTrackLevel
                    and acadTrackENT.fieldOfStudyType = 'Major'
                    --and acadTrackENT.fieldOfStudy is not null
--This causes null values in the academicTrack sourced fields because this field is null in the source data. 
                    and acadTrackENT.academicTrackStatus = 'Completed'
                    and to_date(acadTrackENT.snapshotDate, 'YYYY-MM-DD') = person.snapshotDate
                    and acadTrackENT.isIpedsReportable = 1
					and acadTrackENT.isCurrentFieldOfStudy = 1
                inner join AcademicTermOrder termorder on termorder.termCode = acadtrackENT.termCodeEffective
			where person.awardedDate is not null
			    and person.snapshotDate is not null
            ) AcadTrackRec 
				ON person2.personId = AcadTrackRec.personId 
                                and person2.degree = AcadTrackRec.degree
			    and (person2.college = AcadTrackRec.college
                        or AcadTrackRec.college is null)
                and (person2.campus = upper(AcadTrackRec.campus)
                        or AcadTrackRec.campus is null)
			    and person2.degreeLevel = AcadTrackRec.academicTrackLevel
-- Added to use check the date for when the academicTrack record was made active. Use termCodeEffective if
-- the dummy date is being used. 
                and ((AcadTrackRec.fieldOfStudyActionDate != CAST('9999-09-09' AS DATE)
					and AcadTrackRec.fieldOfStudyActionDate <= person2.reportingDateEnd)
                    or AcadTrackRec.fieldOfStudyActionDate = CAST('9999-09-09' AS DATE)) 
-- and to use the termCodeEffective...
				and ((AcadTrackRec.fieldOfStudyActionDate = CAST('9999-09-09' AS DATE)
				and  AcadTrackRec.termOrder <= person2.termOrder)
					or AcadTrackRec.fieldOfStudyActionDate != CAST('9999-09-09' AS DATE))
-- Maintaining activityDate filtering from prior versions.					
                and ((AcadTrackRec.recordActivityDate != CAST('9999-09-09' AS DATE)
				and AcadTrackRec.recordActivityDate <= person2.reportingDateEnd)
                    or AcadTrackRec.recordActivityDate = CAST('9999-09-09' AS DATE)) 
                and person2.snapshotDate = AcadTrackRec.snapshotDate
        where person2.awardedDate is not null
            and person2.snapshotDate is not null
                
	)
where acadtrackRn = 1
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

CohortSTU as (
--Builds out the reporting population with relevant reporting fields.

select *,
       (case when IPEDSethnicity = '1' and IPEDSGender = 'M' then 1 else 0 end) reg1, --(CRACE01) - Nonresident Alien, M, (0 to 999999)
       (case when IPEDSethnicity = '1' and IPEDSGender = 'F' then 1 else 0 end) reg2, --(CRACE02) - Nonresident Alien, F, (0 to 999999)
       (case when IPEDSethnicity = '2' and IPEDSGender = 'M' then 1 else 0 end) reg3, --(CRACE25) - Hispanic/Latino, M, (0 to 999999)
       (case when IPEDSethnicity = '2' and IPEDSGender = 'F' then 1 else 0 end) reg4, --(CRACE26) - Hispanic/Latino, F, (0 to 999999)
       (case when IPEDSethnicity = '3' and IPEDSGender = 'M' then 1 else 0 end) reg5, --(CRACE27) - American Indian or Alaska Native, M, (0 to 999999)
       (case when IPEDSethnicity = '3' and IPEDSGender = 'F' then 1 else 0 end) reg6, --(CRACE28) - American Indian or Alaska Native, F, (0 to 999999)
       (case when IPEDSethnicity = '4' and IPEDSGender = 'M' then 1 else 0 end) reg7, --(CRACE29) - Asian, M, (0 to 999999)
       (case when IPEDSethnicity = '4' and IPEDSGender = 'F' then 1 else 0 end) reg8, --(CRACE30) - Asian, F, (0 to 999999)
       (case when IPEDSethnicity = '5' and IPEDSGender = 'M' then 1 else 0 end) reg9, --(CRACE31) - Black or African American, M, (0 to 999999)
       (case when IPEDSethnicity = '5' and IPEDSGender = 'F' then 1 else 0 end) reg10, --(CRACE32) - Black or African American, F, (0 to 999999)
       (case when IPEDSethnicity = '6' and IPEDSGender = 'M' then 1 else 0 end) reg11, --(CRACE33) - Native Hawaiian or Other Pacific Islander, M, (0 to 999999)
       (case when IPEDSethnicity = '6' and IPEDSGender = 'F' then 1 else 0 end) reg12, --(CRACE34) - Native Hawaiian or Other Pacific Islander, F, (0 to 999999)
       (case when IPEDSethnicity = '7' and IPEDSGender = 'M' then 1 else 0 end) reg13, --(CRACE35) - White, M, (0 to 999999)
       (case when IPEDSethnicity = '7' and IPEDSGender = 'F' then 1 else 0 end) reg14, --(CRACE36) - White, F, (0 to 999999)
       (case when IPEDSethnicity = '8' and IPEDSGender = 'M' then 1 else 0 end) reg15, --(CRACE37) - Two or more races, M, (0 to 999999)
       (case when IPEDSethnicity = '8' and IPEDSGender = 'F' then 1 else 0 end) reg16, --(CRACE38) - Two or more races, M, (0 to 999999)
       (case when IPEDSethnicity = '9' and IPEDSGender = 'M' then 1 else 0 end) reg17, --(CRACE13) - Race and ethnicity unknown, M, (0 to 999999)
       (case when IPEDSethnicity = '9' and IPEDSGender = 'F' then 1 else 0 end) reg18  --(CRACE14) - Race and ethnicity unknown, F, (0 to 999999)
from (
    select DISTINCT 
        personId personId,
        (case when gender = 'Male' then 'M'
            when gender = 'Female' then 'F'
            when gender = 'Non-Binary' then genderForNonBinary
            else genderForUnknown
        end) IPEDSGender,
        (case when isUSCitizen = 1 then 
            (case when isHispanic = true then '2' -- 'hispanic/latino'
                  when isMultipleRaces = true then '8' -- 'two or more races'
                  when ethnicity != 'Unknown' and ethnicity is not null
                    then (case when ethnicity = 'Hispanic or Latino' then '2'
                               when ethnicity = 'American Indian or Alaskan Native' then '3'
                               when ethnicity = 'Asian' then '4'
                               when ethnicity = 'Black or African American' then '5'
                               when ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
                               when ethnicity = 'Caucasian' then '7'
                        else '9' end) 
                    else '9' end) -- 'race and ethnicity unknown'
                else (case when isInUSOnVisa = 1 and awardedDate between visaStartDate and visaEndDate then '1' -- 'nonresident alien'
                        else '9' end) -- 'race and ethnicity unknown'
        end) IPEDSethnicity,
        (case when asOfAge < 18 then 'AGE1' --Under 18
            when asOfAge >= 18 and asOfAge <= 24 then 'AGE2' --18-24
            when asOfAge >= 25 and asOfAge <= 39 then 'AGE3' --25-39
            when asOfAge >= 40 then 'AGE4' --40 and above
            else 'AGE5' --Age unknown
        end) IPEDSAgeGroup,
        awardedDate awardedDate,
        awardedTermCode awardedTermCode,
        fosRn FOSPriority,
        isInternational isInternational,
        degree degree,
        degreeLevel degreeLevel,
        academicTrackLevel academicTrackLevel,
        degreeProgram degreeProgram,
        fieldOfStudy fieldOfStudy, 
        fieldOfStudyType fieldOfStudyType,
        fieldOfStudyPriority fieldOfStudyPriority
    from (
		select DISTINCT 
			acadtrack.personId personId,
			acadtrack.gender gender,
			acadtrack.isHispanic isHispanic,
			acadtrack.isMultipleRaces isMultipleRaces,
			acadtrack.ethnicity ethnicity,
			acadtrack.isInUSOnVisa isInUSOnVisa,
			acadtrack.visaStartDate visaStartDate,
			acadtrack.visaEndDate visaEndDate,
			acadtrack.isUSCitizen isUSCitizen,
			floor(DATEDIFF(acadtrack.awardedDate, acadtrack.birthDate) / 365) asOfAge,
			acadtrack.awardedDate awardedDate,
			acadtrack.awardedTermCode awardedTermCode,
			acadtrack.genderForUnknown genderForUnknown,
			acadtrack.genderForNonBinary genderForNonBinary,
			acadtrack.isInternational,
			acadtrack.degree degree,
			acadtrack.degreeLevel degreeLevel,
		    acadtrack.academicTrackLevel academicTrackLevel,
			acadtrack.fosRn fosRn,
			acadtrack.degreeProgram degreeProgram,
            acadtrack.fieldOfStudy fieldOfStudy, 
            acadtrack.fieldOfStudyType fieldOfStudyType,
            acadtrack.fieldOfStudyPriority fieldOfStudyPriority
		from AcademicTrackMCR acadtrack
		where fosRn < 3
		)
    where personId is not null
    )
where personId is not null
),

/*****
BEGIN SECTION - Award Level Offerings
This set of views pulls all degree/major combinations
*****/

DegreeACAT as (
-- Pulls degree information as of the reporting period

select * 
from (
    select upper(degreeENT.degree) degree,
        degreeENT.degreeLevel,
        degreeENT.awardLevel,
        to_date(degreeENT.snapshotDate,'YYYY-MM-DD') snapshotDate, 
		row_number() over (
			partition by
				degreeENT.degree,
				degreeENT.degreeLevel,
				degreeENT.awardLevel
			order by
			    degreeENT.snapshotDate desc,
				degreeENT.recordActivityDate desc
		) as degreeRn
	from Degree degreeENT
        cross join (select max(repperiod1.reportingDateEnd) reportingDateEnd
                    from ReportingPeriodMCR repperiod1
                    where repperiod1.reportingDateEnd is not null) repperiod
    where degreeENT.awardLevel is not null
        and degreeENT.isIpedsReportable = 1
		and  degreeENT.isNonDegreeSeeking = 0
-- Remove for testing...
		and ((to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
			and to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.reportingDateEnd)
				or to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
-- ...Remove for testing
   )
where degreeRn = 1
),

FOScipc as ( 
-- Pulls major information as of the reporting period and joins to the degrees on curriculumRule

select * 
from (
    select upper(fosENT.fieldOfStudy) fieldOfStudy,
        fosENT.fieldOfStudyType fieldOfStudyType,
        fosENT.cipCode cipCode,
        fosENT.cipCodeVersion cipCodeVersion,
        to_date(fosENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
        row_number() over (
            partition by
                fosENT.cipCode,
                fosENT.fieldOfStudy,
                fosENT.fieldOfStudyType
            order by
                fosENT.snapshotDate desc,
                fosENT.recordActivityDate desc
        ) as fosRn
    from FieldOfStudy fosENT 
        cross join (select max(repperiod1.reportingDateEnd) reportingDateEnd
                    from ReportingPeriodMCR repperiod1
                    where repperiod1.reportingDateEnd is not null) repperiod
-- Remove for testing...
    where ((to_date(fosENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
        and to_date(fosENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.reportingDateEnd)
            or to_date(fosENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
		and SUBSTR(CAST(fosENT.cipCodeVersion as STRING), 3, 2) >= 20 -- Current CIPCode standard was released 2020. new specs introduced per decade. 
-- ...Remove for testing
    )
    where fosRn = 1
),

DegreeFOSProgram as (
-- Should pull back DegreeMajor and DegreeACAT and give a program distanceEducationOption. This will we used to replace DegreeMajor in DegreeMajorSTU below.
select *
from (
    select upper(programENT.degreeProgram) degreeProgram,
           programENT.degreeLevel degreeLevel,
           upper(programENT.degree) degree,
            upper(programENT.major) fieldOfStudy,
            upper(programENT.college) college,
            upper(programENT.campus) campus,
			upper(programENT.department) department,
			to_date(programENT.startDate, 'YYYY-MM-DD') startDate,
			programENT.termCodeEffective termCodeEffective,
			coalesce(programENT.distanceEducationOption, 'No DE option') distanceEducationOption,
			to_date(programENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
        --foscipc.cipcode cipCode,
		CONCAT(LPAD(SUBSTR(CAST(foscipc.cipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(foscipc.cipCode as STRING), 3, 6), 4, '0')) cipCode,
        degacat.awardLevel awardLevel,
        case when degacat.awardLevel = 'Postsecondary (<300 clock/<9 semester credit/<13 quarter credit hours)' then '1a' 
            when degacat.awardLevel = 'Postsecondary (300-899 clock/9-29 semester credit/13-44 quarter credit hours)' then '1b'
            when degacat.awardLevel = 'Postsecondary (900-1800 clock/30-60 semester credit/45-90 quarter credit hours)' then '2'
            when degacat.awardLevel = 'Associates Degree' then '3' 
            when degacat.awardLevel = 'Postsecondary (>1800 clock/>60 semester credit/>90 quarter credit hours)' then '4'
            when degacat.awardLevel = 'Bachelors Degree' then '5'
            when degacat.awardLevel = 'Post-baccalaureate Certificate' then '6'
            when degacat.awardLevel = 'Masters Degree' then '7'
            when degacat.awardLevel = 'Post-Masters Certificate' then '8'
            when degacat.awardLevel = 'Doctors Degree (Research/Scholarship)' then '17'
            when degacat.awardLevel = 'Doctors Degree (Professional Practice)' then '18'
            when degacat.awardLevel = 'Doctors Degree (Other)' then '19'
        end ACATPartAB,
        case when degacat.awardLevel = 'Associates Degree' then '3' 
		    when degacat.awardLevel = 'Bachelors Degree' then '4'  
		    when degacat.awardLevel = 'Masters Degree' then '5' 
		    when degacat.awardLevel in ('Doctors Degree (Research/Scholarship)',
									    'Doctors Degree (Professional Practice)',
									    'Doctors Degree (Other)') then '6'	 
		    when degacat.awardLevel in ('Post-baccalaureate Certificate',
		    						    'Post-Masters Certificate') then '7'				
		    when degacat.awardLevel = 'Postsecondary (<300 clock/<9 semester credit/<13 quarter credit hours)' then '8' 
		    when degacat.awardLevel = 'Postsecondary (300-899 clock/9-29 semester credit/13-44 quarter credit hours)' then '9' 
		    when degacat.awardLevel in ('Postsecondary (900-1800 clock/30-60 semester credit/45-90 quarter credit hours)',
								        'Postsecondary (>1800 clock/>60 semester credit/>90 quarter credit hours)') then '2' 
        end ACATPartD,
		row_number() over (
			partition by
			    programENT.degreeProgram,
			    programENT.degreeLevel,
                programENT.degree,
                programENT.major,
                programENT.campus,
				programENT.distanceEducationOption
			order by
			    programENT.snapshotDate desc,
				programENT.recordActivityDate desc
		) programRN
	from DegreeProgram programENT 
		left join AcademicTermOrder termorder on termorder.termCode = programENT.termCodeEffective
		    and programENT.termCodeEffective is not null
		left join DegreeACAT degacat on degacat.degree = programENT.degree
		    and degacat.degreeLevel = programENT.degreeLevel
		    and to_date(degacat.snapshotDate, 'YYYY-MM-DD') = to_date(programENT.snapshotDate, 'YYYY-MM-DD')
		left join FOScipc foscipc on foscipc.fieldOfStudy = programENT.major
		    and to_date(foscipc.snapshotDate, 'YYYY-MM-DD') = to_date(programENT.snapshotDate, 'YYYY-MM-DD')
		    and foscipc.fieldOfStudyType = 'Major'
        cross join (select max(repperiod1.reportingDateEnd) reportingDateEnd
                    from ReportingPeriodMCR repperiod1
                    where repperiod1.reportingDateEnd is not null) repdateend
	where programENT.isIPEDSReportable = 1
	    and programENT.isForStudentAcadTrack = 1
	    and programENT.isForDegreeAcadHistory = 1
	    and ((programENT.recordActivityDate != CAST('9999-09-09' AS DATE)
		    and programENT.recordActivityDate <= repdateend.reportingDateEnd)
			    or programENT.recordActivityDate = CAST('9999-09-09' AS DATE)) 
    	and ((programENT.startDate != CAST('9999-09-09' AS DATE)
		    and programENT.startDate <= repdateend.reportingDateEnd)
			or programENT.startDate = CAST('9999-09-09' AS DATE)) 
		and ((programENT.termCodeEffective is not null
			and  termorder.termOrder <= (select max(repperiod.termOrder)
										  from AcademicTermReporting repperiod))
				or programENT.termCodeEffective is null)
	    and foscipc.cipCode is not null
	    and degacat.awardLevel is not null
    )
where programRn = 1
),

DistanceEdCountsSTU as (

select cipCode cipCode,
	awardLevel awardLevel,
	case when DEisDistanceEd = totalPrograms then 1 -- All programs in this CIP code in this award level can be completed entirely via distance education.
		when DEisDistanceEd = 0 then 2 -- None of the programs in this CIP code in this award level can be completed entirely via distance education.
		when totalPrograms > DEisDistanceEd
			and DEisDistanceEd > 0 then 3 -- Some programs in this CIP code in this award level can be completed entirely via distance education.
	end DEAvailability,
	case when totalPrograms > DEisDistanceEd
			and DEisDistanceEd > 0   
            then (case when DEMandatoryOnsite > 0 then 1 else 0 end)
		else null
	end DESomeRequiredOnsite,
    case when totalPrograms > DEisDistanceEd
			and DEisDistanceEd > 0   
            then (case when DEMOptionalOnsite > 0 then 1 else 0 end)
        else null
	end DESomeOptionalOnsite
from (
    select cipCode cipCode,
		awardLevel awardLevel,
		SUM(1) totalPrograms,
		SUM(case when distanceEducationOption = 'No distance Ed' then 1 else 0 end) DENotDistanceEd,
		SUM(case when distanceEducationOption in ('Entirely Distance Ed',
												'Online with mandatory onsite component',
												'Online with non-mandatory onsite component') then 1 else 0 end) DEisDistanceEd,
		SUM(case when distanceEducationOption = 'Entirely Distance Ed' then 1 else 0 end) DEEntirelyDistanceEd,
		SUM(case when distanceEducationOption = 'Online with mandatory onsite component' then 1 else 0 end) DEMandatoryOnsite,
		SUM(case when distanceEducationOption = 'Online with non-mandatory onsite component' then 1 else 0 end) DEMOptionalOnsite
	from (
		select distinct program.awardLevel awardLevel,
			program.cipcode cipCode,
			program.distanceEducationOption distanceEducationOption,
			program.degreeProgram degreeProgram
		from DegreeFOSProgram program
		)
		group by cipCode, awardLevel
	)
),

DegreeFOSProgramSTU as (
-- Pulls all CIPCodes and ACAT levels including student awarded levels.

select distinct coalesce(stuCIP.MajorNum, 1) majorNum,
	degfosprog.cipCode cipCode,
--	degfosprog.ACAT ACAT,
	degfosprog.awardLevel awardLevel,
    degfosprog.ACATPartAB ACATPartAB,
    degfosprog.ACATPartD ACATPartD,
	decounts.DEAvailability DEAvailability,
	decounts.DESomeRequiredOnsite DESomeRequiredOnsite,
	decounts.DESomeOptionalOnsite DESomeOptionalOnsite,
	coalesce(stuCIP.FIELD1, 0) FIELD1,
	coalesce(stuCIP.FIELD2, 0) FIELD2,
	coalesce(stuCIP.FIELD3, 0) FIELD3,
	coalesce(stuCIP.FIELD4, 0) FIELD4,
	coalesce(stuCIP.FIELD5, 0) FIELD5,
	coalesce(stuCIP.FIELD6, 0) FIELD6,
	coalesce(stuCIP.FIELD7, 0) FIELD7,
	coalesce(stuCIP.FIELD8, 0) FIELD8,
	coalesce(stuCIP.FIELD9, 0) FIELD9,
	coalesce(stuCIP.FIELD10, 0) FIELD10,
	coalesce(stuCIP.FIELD11, 0) FIELD11,
	coalesce(stuCIP.FIELD12, 0) FIELD12,
	coalesce(stuCIP.FIELD13, 0) FIELD13,
	coalesce(stuCIP.FIELD14, 0) FIELD14,
	coalesce(stuCIP.FIELD15, 0) FIELD15,
	coalesce(stuCIP.FIELD16, 0) FIELD16,
	coalesce(stuCIP.FIELD17, 0) FIELD17,
	coalesce(stuCIP.FIELD18, 0) FIELD18
from DegreeFOSProgram degfosprog
	inner join DistanceEdCountsSTU decounts on degfosprog.CIPCode = decounts.CIPCode	
		and degfosprog.awardLevel = decounts.awardLevel
    left join (
            select distinct cohortstu.FOSPriority MajorNum, 
                            --cohortstu.cipCode CIPCode,
							--cohortstu.awardLevel awardLevel,
							cohortstu.academicTrackLevel academicTrackLevel,
							cohortstu.degreeProgram degreeProgram,
							cohortstu.degree degree,
							cohortstu.fieldOfStudy fieldOfStudy,
							--cohortstu.fieldOfStudyPriority fieldOfStudyPriority,
                            SUM(cohortstu.reg1) FIELD1, --Nonresident Alien
                            SUM(cohortstu.reg2) FIELD2,
                            SUM(cohortstu.reg3) FIELD3, -- Hispanic/Latino
                            SUM(cohortstu.reg4) FIELD4,
                            SUM(cohortstu.reg5) FIELD5, -- American Indian or Alaska Native
                            SUM(cohortstu.reg6) FIELD6,
                            SUM(cohortstu.reg7) FIELD7, -- Asian
                            SUM(cohortstu.reg8) FIELD8,
                            SUM(cohortstu.reg9) FIELD9, -- Black or African American
                            SUM(cohortstu.reg10) FIELD10,
                            SUM(cohortstu.reg11) FIELD11, -- Native Hawaiian or Other Pacific Islander
                            SUM(cohortstu.reg12) FIELD12,
                            SUM(cohortstu.reg13) FIELD13, -- White
                            SUM(cohortstu.reg14) FIELD14,
                            SUM(cohortstu.reg15) FIELD15, -- Two or more races
                            SUM(cohortstu.reg16) FIELD16,
                            SUM(cohortstu.reg17) FIELD17, -- Race and ethnicity unknown
                            SUM(cohortstu.reg18) FIELD18                    
            from CohortSTU cohortstu
            group by cohortstu.FOSPriority, 
                      --          cohortstu.cipCode,
                        --        cohortstu.awardLevel
                cohortstu.academicTrackLevel,
                cohortstu.degreeProgram,
                cohortstu.degree,
                cohortstu.fieldOfStudy
            ) stuCIP on degfosprog.degreeLevel = stuCIP.academicTrackLevel
				and degfosprog.degreeProgram = stuCIP.degreeProgram
				and degfosprog.degree = stuCIP.degree
				and degfosprog.fieldOfStudy = stuCIP.fieldOfStudy
)

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs

fields are used as follows...
PART                      - Should reflect the appropriate survey part
MAJORNUM                  - Used to determine if major is 1 or 2.
CIPC_LEVL				  - Used to list CIP codes in 'XX.XXXX' form.
AWLEVEL					  - Used to categorize the level of the award numerically (1-8 or 17-19) according to the IPEDS index of values.
DistanceED				  - Used to label CIP offerings if they are offered in a distance ed format (1 = Yes, 2 = No).
Field1 - Field18          - Used for numeric values. Specifically, counts of gender/ethnicity categories described by the IPEDS index of values.
****/

-- Part A: Completions - CIP Data
--Report based on award. If a student obtains more than one award, they may be counted more than once.
--If a program has a traditional offering and a distance education option, completions should be reported regardless of whether or not the program was completed through distance education.

select 'A'                      part ,       	--(PART)	- "A"
       majorNum                 majornum,   --(MAJORNUM)	- 1 = First Major, 2 = Second Major
       cipCode                  cipcode,    --(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
       ACATPartAB               awlevel,    --(AWLEVEL)		- 1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2;
       null                    distanceed, --(DistanceED)  ***NOT USED IN PART A***
       FIELD1                  field1,     --(CRACE01) 	- Nonresident Alien, M, (0 to 999999)
       FIELD2                  field2,     --(CRACE02) 	- Nonresident Alien, F, (0 to 999999)
       FIELD3                  field3,     --(CRACE25) 	- Hispanic/Latino, M, (0 to 999999)					
       FIELD4                  field4,     --(CRACE26) 	- Hispanic/Latino, F, (0 to 999999) 
       FIELD5                  field5,     --(CRACE27) 	- American Indian or Alaska Native, M, (0 to 999999)
       FIELD6                  field6,     --(CRACE28) 	- American Indian or Alaska Native, F, (0 to 999999)
       FIELD7                  field7,     --(CRACE29) 	- Asian, M, (0 to 999999)
       FIELD8                  field8,     --(CRACE30) 	- Asian, F, (0 to 999999)
       FIELD9                  field9,     --(CRACE31) 	- Black or African American, M, (0 to 999999)
       FIELD10                 field10,    --(CRACE32) 	- Black or African American, F, (0 to 999999)
       FIELD11                 field11,    --(CRACE33) 	- Native Hawaiian or Other Pacific Islander, M, (0 to 999999)
       FIELD12                 field12,    --(CRACE34) 	- Native Hawaiian or Other Pacific Islander, F, (0 to 999999)
       FIELD13                 field13,    --(CRACE35) 	- White, M, (0 to 999999)
       FIELD14                 field14,    --(CRACE36) 	- White, F, (0 to 999999)
       FIELD15                 field15,    --(CRACE37) 	- Two or more races, M, (0 to 999999)
       FIELD16                 field16,    --(CRACE38) 	- Two or more races, F, (0 to 999999)
       FIELD17                 field17,    --(CRACE13) 	- Race and ethnicity unknown, M, (0 to 999999)		
       FIELD18                 field18     --(CRACE14) 	- Race and ethnicity unknown, F, (0 to 999999) 
from DegreeFOSProgramSTU
where (majorNum = 1
	or (majorNum = 2
		and ACATPartAB IN ('3', '5', '7', '17', '18', '19'))) --1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2
--    and exists (select a.personId from CohortSTU a) 
    and cipcode not in ('06.0101','06.0201','15.3402','18.1101',
                        '23.0200','23.0102','42.0102')

union

--Part B: Completions - Distance Education
--Used to distinguish if an award is offered in a distance education format. A "distance education program" is "a program for which all the required coursework 
--for program completion is able to be completed via distance education courses."

select 'B',             --(PART)		- "B"
       majorNum,        --(MAJORNUM)	- 1 = First Major, 2 = Second Major
       cipCode,         --(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
       ACATPartAB,      --(AWLEVEL)		- 1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2;
       DEAvailability,  --(DistanceED)  - Is at least one program within this CIP code offered as a distance education program? (1 = All, 2 = Some, 3 = None)
       DESomeRequiredOnsite, --(DistanceED31)     - At least one program in this CIP code in this award level has a mandatory onsite component. (0 = No, 1 = Yes)
       DESomeOptionalOnsite, --(DistanceED32)     - At least one program in this CIP code in this award level has a non-mandatory onsite component. (0 = No, 1 = Yes)
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null
from DegreeFOSProgramSTU
where (majorNum = 1
	or (majorNum = 2
		and ACATPartAB IN ('3', '5', '7', '17', '18', '19'))) --1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2
--    and exists (select a.personId from CohortSTU a) 
    and cipcode not in ('06.0101','06.0201','15.3402','18.1101',
                        '23.0200','23.0102','42.0102')

union

--Part C: All Completers
--Count each student only once, regardless of how many awards he/she earned. The intent of this screen is to collect an unduplicated count 
--of total numbers of completers.

select 'C' ,    --PART 	 - "C"
        null,
        null,
        null,
        null,
        SUM(coalesce(cohortstu.reg1, 0)), --Nonresident Alien
	    SUM(coalesce(cohortstu.reg2, 0)),
		SUM(coalesce(cohortstu.reg3, 0)), -- Hispanic/Latino
        SUM(coalesce(cohortstu.reg4, 0)),
        SUM(coalesce(cohortstu.reg5, 0)), -- American Indian or Alaska Native
        SUM(coalesce(cohortstu.reg6, 0)),
        SUM(coalesce(cohortstu.reg7, 0)), -- Asian
        SUM(coalesce(cohortstu.reg8, 0)),
        SUM(coalesce(cohortstu.reg9, 0)), -- Black or African American
        SUM(coalesce(cohortstu.reg10, 0)),
        SUM(coalesce(cohortstu.reg11, 0)), -- Native Hawaiian or Other Pacific Islander
        SUM(coalesce(cohortstu.reg12, 0)),
        SUM(coalesce(cohortstu.reg13, 0)), -- White
        SUM(coalesce(cohortstu.reg14, 0)),
        SUM(coalesce(cohortstu.reg15, 0)), -- Two or more races
        SUM(coalesce(cohortstu.reg16, 0)),
        SUM(coalesce(cohortstu.reg17, 0)), -- Race and ethnicity unknown
        SUM(coalesce(cohortstu.reg18, 0)) 
from CohortSTU cohortstu
where personId is not null

union 
--Part D: Completers by Level
--Each student should be counted once per award level. For example, if a student earned a master's degree and a doctor's degree, he/she 
--should be counted once in master's and once in doctor's. A student earning two master's degrees should be counted only once.									  

select 'D',                                                              --PART      - "D"
       null,
       null,
       deg.awardLevel ACATPartD,                                                    --CTLEVEL - 2 to 9.
       null,
       coalesce(SUM(case when IPEDSGender = 'M' then 1 else 0 end), 0),      --(CRACE15)	- Men, 0 to 999999
       coalesce(SUM(case when IPEDSGender = 'F' then 1 else 0 end), 0),      --(CRACE16) - Women, 0 to 999999 
       coalesce(SUM(case when IPEDSethnicity = '1' then 1 else 0 end), 0),   --(CRACE17) - Nonresident Alien, 0 to 999999
       coalesce(SUM(case when IPEDSethnicity = '2' then 1 else 0 end), 0),   --(CRACE41) - Hispanic/Latino, 0 to 999999
       coalesce(SUM(case when IPEDSethnicity = '3' then 1 else 0 end), 0),   --(CRACE42) - American Indian or Alaska Native, 0 to 999999
       coalesce(SUM(case when IPEDSethnicity = '4' then 1 else 0 end), 0),   --(CRACE43) - Asian, 0 to 999999
       coalesce(SUM(case when IPEDSethnicity = '5' then 1 else 0 end), 0),   --(CRACE44) - Black or African American, 0 to 999999
       coalesce(SUM(case when IPEDSethnicity = '6' then 1 else 0 end), 0),   --(CRACE45) - Native Hawaiian or Other Pacific Islander, 0 to 999999
       coalesce(SUM(case when IPEDSethnicity = '7' then 1 else 0 end), 0),   --(CRACE46) - White, 0 to 999999
       coalesce(SUM(case when IPEDSethnicity = '8' then 1 else 0 end), 0),   --(CRACE47) - Two or more races, 0 to 999999
       coalesce(SUM(case when IPEDSethnicity = '9' then 1 else 0 end), 0),   --(CRACE23) - Race and ethnicity unknown, 0 to 999999
       coalesce(SUM(case when IPEDSAgeGroup = 'AGE1' then 1 else 0 end), 0), --(AGE1) - Under 18, 0 to 999999
       coalesce(SUM(case when IPEDSAgeGroup = 'AGE2' then 1 else 0 end), 0), --(AGE2) - 18-24, 0 to 999999
       coalesce(SUM(case when IPEDSAgeGroup = 'AGE3' then 1 else 0 end), 0), --(AGE3) - 25-39, 0 to 999999
       coalesce(SUM(case when IPEDSAgeGroup = 'AGE4' then 1 else 0 end), 0), --(AGE4) - 40 and above, 0 to 999999
       coalesce(SUM(case when IPEDSAgeGroup = 'AGE5' then 1 else 0 end), 0), --(AGE5) - Age unknown, 0 to 999999
       null,
       null
from CohortSTU cohortstu
    inner join DegreeFOSProgram deg on deg.degree = cohortstu.degree
        and deg.degreeLevel = cohortstu.degreeLevel
where deg.degree is not null
    and deg.degreeLevel is not null
--	and exists (select a.personId from CohortSTU a) 
group by deg.awardLevel

union

--Dummy set to return default formatting if no student awards exist.
select *
from (
    VALUES
        ('A', 1, '01.0101', '1a', null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('B', 1, '01.0101', '1a', 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        --('C', null, null, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('D', null, null, '2', null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null)
    ) as dummySet(PART, MAJORNUM, CIPC_LEVL, AWLEVEL, isDistanceED, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, 
                  FIELD10, FIELD11, FIELD12, FIELD13, FIELD14, FIELD15, FIELD16, FIELD17, FIELD18)
where not exists (select a.personId from CohortSTU a) 
