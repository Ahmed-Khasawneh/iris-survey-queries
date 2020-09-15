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

20200914            akhasawneh          			    		Initial version (Run time 1hr+)
	
	
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
	'202110' termCode,
	'1' partOfTermCode,
	CAST('2019-07-01' as DATE) reportingDateStart,
    CAST('2020-06-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
/*   
union

select '1920' surveyYear,
	'COM' surveyId,
	'202030' termCode,
	'1' partOfTermCode,
	CAST('2018-07-01' as DATE) reportingDateStart,
    CAST('2019-06-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term

union

select '1415' surveyYear,
	'COM' surveyId,
	'201430' termCode,
	'1' partOfTermCode,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
    
union

select '1415' surveyYear,
	'COM' surveyId,
	'201410' termCode,
	'1' partOfTermCode,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
    
union

select '1415' surveyYear,
	'COM' surveyId,
	'201410' termCode,
	'A' partOfTermCode,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term

union

select '1415' surveyYear,
	'COM' surveyId,
	'201410' termCode,
	'B' partOfTermCode,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
    
union

select '1415' surveyYear,
	'COM' surveyId,
	'201420' termCode,
	'1' partOfTermCode,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term    
    
union

select '1415' surveyYear,
	'COM' surveyId,
	'201420' termCode,
	'A' partOfTermCode,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
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
--          3rd union - pull default values if no record in IPEDSClientConfig

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
    ConfigLatest.snapshotDate snapshotDate,
    ConfigLatest.tags
from (
	select clientConfigENT.surveyCollectionYear surveyYear,
		clientConfigENT.snapshotDate snapshotDate, 
		clientConfigENT.tags tags,
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
			    (case when clientConfigENT.compGradDateOrTerm = 'T' and array_contains(clientConfigENT.tags, 'Full Year Term End') then 1
			         when clientConfigENT.compGradDateOrTerm = 'D' and array_contains(clientConfigENT.tags, 'Full Year June End') then 1
			         when array_contains(clientConfigENT.tags, 'Full Year June End') or array_contains(clientConfigENT.tags, 'Full Year Term End') then 2
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
	    array() tags,
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
--          3rd union - pull default values if no record in IPEDSReportingPeriod

select DISTINCT
    RepDates.surveyYear surveyYear,
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
    RepDates.tags tags,
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
		repPeriodENT.tags tags,
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
                (case when clientconfig.compGradDateOrTerm = 'T' and array_contains(repPeriodENT.tags, 'Full Year Term End') then 1
			         when clientconfig.compGradDateOrTerm = 'D' and array_contains(repPeriodENT.tags, 'Full Year June End') then 1
			         when array_contains(repPeriodENT.tags, 'Full Year June End') or array_contains(repPeriodENT.tags, 'Full Year Term End') then 2
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
		clientconfig.tags tags,
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

select termCode termCode, 
	partOfTermCode partOfTermCode,
	to_date(startDate, 'YYYY-MM-DD') startDate,
	to_date(endDate, 'YYYY-MM-DD') endDate,
	academicYear,
	to_date(censusDate, 'YYYY-MM-DD') censusDate,
    snapshotTermCode,
    to_date(snapshotDate, 'YYYY-MM-DD') snapshotDate,
    tags,
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
                    then acadTermENT.termCode 
                else acadTermENT.snapshotDate end) desc,
                (case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                            and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
                    then acadTermENT.partOfTermCode 
                else acadTermENT.snapshotDate end) desc,
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
    null tags,
    1 snapshotPartTermCode
from DefaultValues defvalues
where not exists (select trm.termCode 
                  from academicTerm trm
                  inner join ReportingPeriodMCR repper
                    on repper.termCode = trm.termCode
                    and trm.termCode is not null)
),

AcadTermOrder as (
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
          left join AcadTermOrder termorder
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
        awardENT.tags tags,
        repperiod.compGradDateOrTerm compGradDateOrTerm,
        row_number() over (
            partition by     
                awardENT.personId,
                case when repperiod.compGradDateOrTerm = 'D' 
                    then awardENT.awardedDate
                    else awardENT.awardedTermCode
                end,
                awardENT.degreeLevel,
                awardENT.degree
            order by
                repperiod.termOrder desc,
                awardENT.recordActivityDate desc
        ) as AwardRn 
	from AcademicTermReporting repperiod
	    inner join Award awardENT on ((repperiod.compGradDateOrTerm = 'D'
			and (awardENT.awardedDate >= repperiod.reportingDateStart 
				and awardENT.awardedDate <= repperiod.reportingDateEnd)) 
			or (repperiod.compGradDateOrTerm = 'T'
				and awardENT.awardedTermCode = repperiod.termCode))
            and awardENT.isIpedsReportable = 1
		    and awardENT.awardStatus = 'Awarded'
		    and awardENT.degreeLevel is not null
		    and awardENT.degreeLevel != 'Continuing Ed'
		    and array_contains(awardENT.tags, 'June End')
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
    MAX(repperiod.snapshotDate) snapshotDate
from AwardMCR award
    inner join AcademicTermReporting repperiod
    on repperiod.termCode = award.awardedTermCode
where award.awardedDate is not null
    and ((repperiod.compGradDateOrTerm = 'D' 
    and award.awardedDate >= repperiod.snapshotDate)
        or repperiod.compGradDateOrTerm = 'T')
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
    award.compGradDateOrTerm
),

CampusMCR as ( 
-- Returns most recent campus record for each campus available per the ReportingPeriod.

select *
from (
    select award2.personId personId,
        award2.awardedDate awardedDate,
        award2.degreeLevel degreeLevel,
        award2.degree degree,
        award2.campus campus,
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
                    where award.awardedDate is not null
        ) CampusRec on award2.campus = CampusRec.campus 
            and award2.awardedDate = CampusRec.awardedDate
            and award2.snapshotDate = CampusRec.snapshotDate
-- Remove for testing... 
		and ((CampusRec.recordActivityDate != CAST('9999-09-09' as DATE)
			and CampusRec.recordActivityDate <= award2.awardedDate)
				or CampusRec.recordActivityDate = CAST('9999-09-09' as DATE))
	where award2.awardedDate is not null
-- ...Remove for testing
	)
where campusRn = 1
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

select *
from (
    select award2.personId,
        award2.awardedDate awardedDate,
        award2.degreeLevel degreeLevel,
        award2.degree degree,
        PersonRec.birthDate birthDate,
        PersonRec.ethnicity ethnicity,
        PersonRec.isHispanic,
        PersonRec.isMultipleRaces,
        PersonRec.isInUSOnVisa,
        PersonRec.visaStartDate visaStartDate,
        PersonRec.visaEndDate visaEndDate,
        PersonRec.isUSCitizen,
        PersonRec.gender gender,
        PersonRec.nation nation,
        PersonRec.state state,
        row_number() over (
                    partition by
                        award2.personId,
                        PersonRec.personId,
                        award2.awardedDate,
                        award2.degreeLevel,
                        award2.degree
                    order by
                        PersonRec.recordActivityDate desc
                ) personRn
    from AwardRefactor award2
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
                        upper(personENT.nation) nation,
                        upper(personENT.state) state,
                        to_date(personENT.snapshotDate,'YYYY-MM-DD') snapshotDate
                from AwardRefactor award
                    inner join Person personENT on award.personId = personENT.personId 
                        and to_date(personENT.snapshotDate,'YYYY-MM-DD') = award.snapshotDate
                        and personENT.isIpedsReportable = 1
                        and award.awardedDate is not null
                ) PersonRec on award2.personId = PersonRec.personId 
                and award2.snapshotDate = PersonRec.snapshotDate
                and ((PersonRec.recordActivityDate != CAST('9999-09-09' AS DATE)
                    and PersonRec.recordActivityDate <= award2.awardedDate) 
                        or PersonRec.recordActivityDate = CAST('9999-09-09' AS DATE))
    where award2.awardedDate is not null
    )
where personRn = 1
),

AcademicTrackMCR as (
--Returns most up to date student academic track information as of their award date and term. 

select personId personId,
		awardedDate awardedDate,
		awardedTermCode awardedTermCode,
        degree degree,
		degreeLevel degreeLevel,
		degreeProgram degreeProgram,
		fieldOfStudy fieldOfStudy,
		curriculumCode curriculumCode,		
		college college,
        fieldOfStudyType fieldOfStudyType,
		fieldOfStudyPriority fieldOfStudyPriority,
        snapshotDate snapshotDate,
        termOrder termOrder,
		row_number() over (
			partition by
			    personId
			order by
                fieldOfStudyPriority asc
		) fosRn
from ( 
	select award2.personId personId,
		award2.awardedDate awardedDate,
		award2.awardedTermCode awardedTermCode,
		AcadTrackRec.termCodeEffective termCodeEffective,
		AcadTrackRec.degreeProgram degreeProgram,
        award2.degree degree,
        award2.degreeLevel degreeLevel,
        AcadTrackRec.fieldOfStudy fieldOfStudy,
		AcadTrackRec.fieldOfStudyType fieldOfStudyType,
		AcadTrackRec.fieldOfStudyPriority fieldOfStudyPriority,
		AcadTrackRec.curriculumCode curriculumCode,
		award2.college college,
		award2.snapshotDate snapshotDate,
		AcadTrackRec.termOrder termOrder,
		row_number() over (
			partition by
			    award2.personId,
				AcadTrackRec.personId,
				award2.awardedDate,
                award2.degreeLevel,
                award2.degree,
				AcadTrackRec.degreeProgram,
                AcadTrackRec.fieldOfStudyType,
				AcadTrackRec.fieldOfStudyPriority
			order by
				AcadTrackRec.termOrder desc,
				AcadTrackRec.recordActivityDate desc
		) acadtrackRn
	from AwardRefactor award2
	    left join ( 
                select distinct acadtrackENT.personId personId,
                        acadtrackENT.termCodeEffective termCodeEffective,
						acadtrackENT.fieldOfStudyActionDate fieldOfStudyActionDate,
                        termorder.termOrder termOrder,
                        to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
                        acadtrackENT.academicTrackLevel academicTrackLevel,
						acadtrackENT.degreeProgram degreeProgram, 
						acadtrackENT.fieldOfStudy fieldOfStudy,
                        acadtrackENT.fieldOfStudyType fieldOfStudyType,
						acadtrackENT.fieldOfStudyPriority fieldOfStudyPriority,
                        upper(acadtrackENT.curriculumCode) curriculumCode,
                        upper(acadtrackENT.degree) degree,
                        upper(acadtrackENT.college) college,
                        acadtrackENT.curriculumStatus curriculumStatus,
                        to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') snapshotDate
            from AwardRefactor award
                inner join AcademicTrack acadtrackENT ON award.personId = acadTrackENT.personId
                    and award.degree = upper(acadTrackENT.degree)
                    and (award.college = upper(acadTrackENT.college)
                        or acadTrackENT.college is null)
                    and award.degreeLevel = acadTrackENT.academicTrackLevel
                    and acadTrackENT.fieldOfStudyType = 'Major'
--This causes null values in the academicTrack sourced fields because this field is null in the source data. 
                    --and acadTrackENT.curriculumStatus = 'Completed'
                    and to_date(acadTrackENT.snapshotDate, 'YYYY-MM-DD') = award.snapshotDate
                    and acadTrackENT.isIpedsReportable = 1
					and acadTrackENT.isCurrentFieldOfStudy = 1
					and award.awardedDate is not null
                inner join AcadTermOrder termorder on termorder.termCode = acadtrackENT.termCodeEffective
            ) AcadTrackRec 
				ON award2.personId = AcadTrackRec.personId 
                                and award2.degree = AcadTrackRec.degree
			    and (award2.college = AcadTrackRec.college
                        or AcadTrackRec.college is null)
			    and award2.degreeLevel = AcadTrackRec.academicTrackLevel
-- Added to use check the date for when the academicTrack record was made active. Use termCodeEffective if
-- the dummy date is being used. 
                and ((AcadTrackRec.fieldOfStudyActionDate != CAST('9999-09-09' AS DATE)
				and AcadTrackRec.fieldOfStudyActionDate <= award2.awardedDate)
                    or AcadTrackRec.fieldOfStudyActionDate = CAST('9999-09-09' AS DATE)) 
-- and to use the termCodeEffective...
				and ((AcadTrackRec.fieldOfStudyActionDate = CAST('9999-09-09' AS DATE)
				and  AcadTrackRec.termOrder <= award2.termOrder)
					or AcadTrackRec.fieldOfStudyActionDate != CAST('9999-09-09' AS DATE))
-- Maintaining activityDate filtering from prior versions.					
                and ((AcadTrackRec.recordActivityDate != CAST('9999-09-09' AS DATE)
				and AcadTrackRec.recordActivityDate <= award2.awardedDate)
                    or AcadTrackRec.recordActivityDate = CAST('9999-09-09' AS DATE)) 
                and award2.snapshotDate = AcadTrackRec.snapshotDate
                and award2.awardedDate is not null
	)
where acadtrackRn = 1
),

DegreeMCR as (
--Returns most up to 'degree' information as of the reporting term codes and part of term census periods.
--This information is used for Retention cohort filtering on awardLevel

select *
from (
	select acadtrack2.personId personId,
		acadtrack2.degree degree,
	    acadtrack2.degreeLevel degreeLevel,
	    DegreeRec.awardLevel awardLevel,
	    acadtrack2.awardedDate awardedDate,
	    DegreeRec.isNonDegreeSeeking isNonDegreeSeeking,
	    DegreeRec.snapshotDate snapshotDate,
		row_number() over (
			partition by
			    acadtrack2.personId,
			    acadtrack2.awardedDate,
                acadtrack2.degreeLevel,
			    acadtrack2.degree,
				DegreeRec.degree
			order by
				DegreeRec.recordActivityDate desc
		) degreeRn
	from AcademicTrackMCR acadtrack2
	    left join (
            select upper(degreeENT.degree) degree,
	               to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
	               degreeENT.degreeLevel degreeLevel,
	               degreeENT.awardLevel awardLevel,
	               degreeENT.isNonDegreeSeeking isNonDegreeSeeking,
                   to_date(degreeENT.snapshotDate,'YYYY-MM-DD') snapshotDate
            from AcademicTrackMCR acadtrack
                    inner join Degree degreeENT ON upper(degreeENT.degree) = acadtrack.degree
                            and acadtrack.degreeLevel = degreeENT.degreeLevel
                            and to_date(degreeENT.snapshotDate,'YYYY-MM-DD') = acadtrack.snapshotDate
			                and degreeENT.isIpedsReportable = 1
			        ) DegreeRec 
			on DegreeRec.degree = acadtrack2.degree
			and ((DegreeRec.recordActivityDate != CAST('9999-09-09' AS DATE)
				and DegreeRec.recordActivityDate <= acadtrack2.awardedDate)
					or DegreeRec.recordActivityDate = CAST('9999-09-09' AS DATE)) 
            and acadtrack2.snapshotDate = DegreeRec.snapshotDate
	)
where degreeRn = 1
),

FieldOfStudyMCR as (
--*!!* FieldOfStudy as ( --*!!*
--Returns most up to 'major' information as of the reporting term codes and part of term census periods.

select *
from (
	select acadtrack2.personId personId,
		acadtrack2.fieldOfStudy fieldOfStudy,
		acadtrack2.degree degree,
	    acadtrack2.degreeLevel degreeLevel,
	    acadtrack2.awardedDate awardedDate,
		CONCAT(LPAD(SUBSTR(CAST(FOSRec.cipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(FOSRec.cipCode as STRING), 3, 6), 4, '0')) cipCode, 
		row_number() over (
			partition by
			    acadtrack2.personId,
			    acadtrack2.awardedDate,
				acadtrack2.degreeLevel,
                acadtrack2.degree,
			    acadtrack2.fieldOfStudy,
			    acadtrack2.fieldOfStudyType, --Major, Minor, Certification, Concentration, Special Program
				FOSRec.fieldOfStudy
			order by
				FOSRec.recordActivityDate desc
		) fosRn
	from AcademicTrackMCR acadtrack2
		left join (
		    select upper(fosENT.fieldOfStudy) fieldOfStudy,
		           fosENT.fieldOfStudyType fieldOfStudyType,
		           to_date(fosENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
		           fosENT.cipCode cipCode, 
                   to_date(fosENT.snapshotDate,'YYYY-MM-DD') snapshotDate
		      from AcademicTrackMCR acadtrack
		        inner join FieldOfStudy fosENT ON upper(fosENT.fieldOfStudy) = acadtrack.fieldOfStudy
					and fosENT.fieldOfStudyType = acadtrack.fieldOfStudyType
                    and to_date(fosENT.snapshotDate,'YYYY-MM-DD') = acadtrack.snapshotDate
			        and fosENT.isIpedsReportable = 1
			  ) FOSRec 
			on FOSRec.fieldOfStudy = acadtrack2.fieldOfStudy
			and FOSRec.snapshotDate = acadtrack2.snapshotDate
-- ADDED JOIN ON FIELDOFSTUDYTYPE TO ONLY RETURN MAJORS SINCE WE ONLY GRAB MAJORS IN THE AcademicTrackMCR VIEW
			and FOSRec.fieldOfStudyType = acadtrack2.fieldOfStudyType
			and ((FOSRec.recordActivityDate != CAST('9999-09-09' AS DATE)
				and FOSRec.recordActivityDate <= acadtrack2.awardedDate)
					or FOSRec.recordActivityDate = CAST('9999-09-09' AS DATE)) 
            and 
	)
where fosRn = 1
),

ProgramMCR as (
--Returns most up to 'FieldOfStudy' information as of the reporting term codes and part of term census periods.

select *
from (
	select acadtrack2.personId personId,
		acadtrack2.fieldOfStudy fieldOfStudy,
		acadtrack2.degree degree,
	    acadtrack2.degreeLevel degreeLevel,
	    acadtrack2.awardedDate awardedDate,
		ProgramRec.degreeProgram degreeProgram,
		ProgramRec.distanceEducationOption distanceEducationOption,
		row_number() over (
			partition by
			    acadtrack2.personId,
			    acadtrack2.awardedDate,
				acadtrack2.degreeLevel,
                acadtrack2.degree,
			    acadtrack2.fieldOfStudy,
				ProgramRec.degreeProgram
			order by
				ProgramRec.recordActivityDate desc
		) programRn
	from AcademicTrackMCR acadtrack2
		left join (
		    select upper(programENT.major) major,
		           programENT.degreeProgram degreeProgram,
		           programENT.distanceEducationOption distanceEducationOption,
		           programENT.recordActivityDate recordActivityDate,
                   to_date(programENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
                   to_date(programENT.startDate, 'YYYY-MM-DD') startDate,
                   termOrder.termOrder termOrder
		      from AcademicTrackMCR acadtrack
		        inner join DegreeProgram programENT ON upper(programENT.major) = acadtrack.fieldOfStudy
					and programENT.degreeProgram = acadtrack.degreeProgram
                    and to_date(programENT.snapshotDate,'YYYY-MM-DD') = acadtrack.snapshotDate
			        and programENT.isIpedsReportable = 1
-- NOT SURE WHICH ONE WE ARE SUPPOSED TO USE HERE
					and programENT.isForDegreeAcadHistory = 1
					and programENT.isForStudentAcadTrack = 1
                inner join AcadTermOrder termorder on termorder.termCode = programENT.termCodeEffective
			  ) ProgramRec 
			on ProgramRec.major = acadtrack2.fieldOfStudy
			and ProgramRec.degreeProgram = acadtrack2.degreeProgram
            and ProgramRec.snapshotDate = acadtrack2.snapshotDate
-- Added to use check the date for when the academicTrack record was made active. Use termCodeEffective if
-- the dummy date is being used. 
			and ((ProgramRec.startDate != CAST('9999-09-09' AS DATE)
				and ProgramRec.startDate <= acadtrack2.awardedDate)
                    or ProgramRec.startDate = CAST('9999-09-09' AS DATE)) 
-- and to use the termCodeEffective...
			and ((ProgramRec.startDate = CAST('9999-09-09' AS DATE)
				and  ProgramRec.termOrder <= acadtrack2.termOrder)
					or ProgramRec.startDate != CAST('9999-09-09' AS DATE))
-- Maintaining activityDate filtering from prior versions.	
			and ((ProgramRec.recordActivityDate != CAST('9999-09-09' AS DATE)
				and ProgramRec.recordActivityDate <= acadtrack2.awardedDate)
					or ProgramRec.recordActivityDate = CAST('9999-09-09' AS DATE)) 
	)
where programRn = 1
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

CohortSTU as (
--Builds out the reporting population with relevant reporting fields.

select DISTINCT 
    award.personId personId,
    person.gender gender,
    person.isHispanic isHispanic,
    person.isMultipleRaces isMultipleRaces,
    person.ethnicity ethnicity,
    person.isInUSOnVisa isInUSOnVisa,
    person.visaStartDate visaStartDate,
    person.visaEndDate visaEndDate,
    person.isUSCitizen isUSCitizen,
    floor(DATEDIFF(award.awardedDate, person.birthDate) / 365) asOfAge,
    award.degree degree,
    award.awardedDate awardedDate,
    award.awardedTermCode awardedTermCode,
    award.genderForUnknown genderForUnknown,
    award.genderForNonBinary genderForNonBinary,
    campus.isInternational,
    acadtrack.fieldOfStudyPriority FOSPriority,
    coalesce(acadtrack.fosRn, 1) fosRn,
    degree.awardLevel awardLevel,
    degree.isNonDegreeSeeking isNonDegreeSeeking,
    fos.fieldOfStudy fieldOfStudy,
    fos.cipCode cipCode,
    program.distanceEducationOption,
	program.degreeProgram
from AwardRefactor award
	inner join PersonMCR person on person.personId = award.personId
	    and person.awardedDate = award.awardedDate
	    and person.degreeLevel = award.degreeLevel
	    and person.degree = award.degree
	inner join AcademicTrackMCR acadtrack on acadtrack.personId = award.personId
	    and acadtrack.awardedDate = award.awardedDate
	    and acadtrack.degreeLevel = award.degreeLevel
	    and acadtrack.degree = award.degree
		and acadtrack.fosRn < 3
	inner join DegreeMCR degree on degree.personId = acadtrack.personId
	    and acadtrack.awardedDate = degree.awardedDate
	    and acadtrack.degreeLevel = degree.degreeLevel
	    and acadtrack.degree = degree.degree
	    and degree.awardLevel is not null
	inner join FieldOfStudyMCR fos on fos.personId = acadtrack.personId
	    and acadtrack.awardedDate = fos.awardedDate
	    and acadtrack.degreeLevel = fos.degreeLevel
	    and acadtrack.degree = fos.degree
	    and acadtrack.fieldOfStudy = fos.fieldOfStudy
	    and fos.cipCode is not null
	inner join ProgramMCR program on program.personId = acadtrack.personId
	    and acadtrack.awardedDate = program.awardedDate
	    and acadtrack.degreeLevel = program.degreeLevel
		and acadtrack.degreeProgram = program.degreeProgram
	    and acadtrack.degree = program.degree
	    and acadtrack.fieldOfStudy = program.fieldOfStudy
--	    and program.cipCode is not null 
    inner join CampusMCR campus on campus.personId = award.personId
        and campus.campus = award.campus
        and campus.awardedDate = award.awardedDate
	    and campus.degreeLevel = award.degreeLevel
	    and campus.degree = award.degree
		and campus.isInternational != 1
),

CohortRefactorSTU as (
-- This view is used to refactor the 'StudentCohort' view records in terms of IPEDS reportable values

select *,
       case when IPEDSethnicity = '1' and IPEDSGender = 'M' then 1 else 0 end reg1, --(CRACE01) - Nonresident Alien, M, (0 to 999999)
       case when IPEDSethnicity = '1' and IPEDSGender = 'F' then 1 else 0 end reg2, --(CRACE02) - Nonresident Alien, F, (0 to 999999)
       case when IPEDSethnicity = '2' and IPEDSGender = 'M' then 1 else 0 end reg3, --(CRACE25) - Hispanic/Latino, M, (0 to 999999)
       case when IPEDSethnicity = '2' and IPEDSGender = 'F' then 1 else 0 end reg4, --(CRACE26) - Hispanic/Latino, F, (0 to 999999)
       case when IPEDSethnicity = '3' and IPEDSGender = 'M' then 1 else 0 end reg5, --(CRACE27) - American Indian or Alaska Native, M, (0 to 999999)
       case when IPEDSethnicity = '3' and IPEDSGender = 'F' then 1 else 0 end reg6, --(CRACE28) - American Indian or Alaska Native, F, (0 to 999999)
       case when IPEDSethnicity = '4' and IPEDSGender = 'M' then 1 else 0 end reg7, --(CRACE29) - Asian, M, (0 to 999999)
       case when IPEDSethnicity = '4' and IPEDSGender = 'F' then 1 else 0 end reg8, --(CRACE30) - Asian, F, (0 to 999999)
       case when IPEDSethnicity = '5' and IPEDSGender = 'M' then 1 else 0 end reg9, --(CRACE31) - Black or African American, M, (0 to 999999)
       case when IPEDSethnicity = '5' and IPEDSGender = 'F' then 1 else 0 end reg10, --(CRACE32) - Black or African American, F, (0 to 999999)
       case when IPEDSethnicity = '6' and IPEDSGender = 'M' then 1 else 0 end reg11, --(CRACE33) - Native Hawaiian or Other Pacific Islander, M, (0 to 999999)
       case when IPEDSethnicity = '6' and IPEDSGender = 'F' then 1 else 0 end reg12, --(CRACE34) - Native Hawaiian or Other Pacific Islander, F, (0 to 999999)
       case when IPEDSethnicity = '7' and IPEDSGender = 'M' then 1 else 0 end reg13, --(CRACE35) - White, M, (0 to 999999)
       case when IPEDSethnicity = '7' and IPEDSGender = 'F' then 1 else 0 end reg14, --(CRACE36) - White, F, (0 to 999999)
       case when IPEDSethnicity = '8' and IPEDSGender = 'M' then 1 else 0 end reg15, --(CRACE37) - Two or more races, M, (0 to 999999)
       case when IPEDSethnicity = '8' and IPEDSGender = 'F' then 1 else 0 end reg16, --(CRACE38) - Two or more races, M, (0 to 999999)
       case when IPEDSethnicity = '9' and IPEDSGender = 'M' then 1 else 0 end reg17, --(CRACE13) - Race and ethnicity unknown, M, (0 to 999999)
       case when IPEDSethnicity = '9' and IPEDSGender = 'F' then 1 else 0 end reg18  --(CRACE14) - Race and ethnicity unknown, F, (0 to 999999)
from (
    select DISTINCT 
        cohortstu.personId personId,
        case when cohortstu.gender = 'Male' then 'M'
            when cohortstu.gender = 'Female' then 'F'
            when cohortstu.gender = 'Non-Binary' then cohortstu.genderForNonBinary
            else cohortstu.genderForUnknown
        end IPEDSGender,
        case when cohortstu.isUSCitizen = 1 then 
            (case when cohortstu.isHispanic = true then '2' -- 'hispanic/latino'
                  when cohortstu.isMultipleRaces = true then '8' -- 'two or more races'
                  when cohortstu.ethnicity != 'Unknown' and cohortstu.ethnicity is not null
                    then (case when cohortstu.ethnicity = 'Hispanic or Latino' then '2'
                               when cohortstu.ethnicity = 'American Indian or Alaskan Native' then '3'
                               when cohortstu.ethnicity = 'Asian' then '4'
                               when cohortstu.ethnicity = 'Black or African American' then '5'
                               when cohortstu.ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
                               when cohortstu.ethnicity = 'Caucasian' then '7'
                        else '9' end) 
                    else '9' end) -- 'race and ethnicity unknown'
                else (case when cohortstu.isInUSOnVisa = 1 and cohortstu.awardedDate between cohortstu.visaStartDate and cohortstu.visaEndDate then '1' -- 'nonresident alien'
                        else '9' end) -- 'race and ethnicity unknown'
        end IPEDSethnicity,
        case when cohortstu.asOfAge < 18 then 'AGE1' --Under 18
            when cohortstu.asOfAge >= 18 and cohortstu.asOfAge <= 24 then 'AGE2' --18-24
            when cohortstu.asOfAge >= 25 and cohortstu.asOfAge <= 39 then 'AGE3' --25-39
            when cohortstu.asOfAge >= 40 then 'AGE4' --40 and above
            else 'AGE5' --Age unknown
        end IPEDSAgeGroup,
        cohortstu.degree degree,
        cohortstu.awardedDate awardedDate,
        cohortstu.awardedTermCode awardedTermCode,
        case when cohortstu.awardLevel = 'Associates Degree' then 3                        	-- 3 - Associate's degree
            when cohortstu.awardLevel = 'Bachelors Degree' then 4                        	-- 4 - Bachelor's degree
            when cohortstu.awardLevel = 'Masters Degree' then 5                          	-- 5 - Master's degree
            when cohortstu.awardLevel in ('Doctors Degree (Research/Scholarship)',
											'Doctors Degree (Professional Practice)',
											'Doctors Degree (Other)')
				then 6	-- 6 - Doctor's degree (research/scholarship, professional practice, other)
			when cohortstu.awardLevel in ('Post-baccalaureate Certificate',
											'Post-Masters Certificate')
				then 7	-- 7 - Postbaccalaureate certificate or Post-master's certificate				
			when cohortstu.awardLevel = 'Postsecondary (<300 clock/<9 semester credit/<13 quarter credit hours)' then 8 
			when cohortstu.awardLevel = 'Postsecondary (300-899 clock/9-29 semester credit/13-44 quarter credit hours)' then 9 
			when cohortstu.awardLevel in ('Postsecondary (900-1800 clock/30-60 semester credit/45-90 quarter credit hours)',
			                                'Postsecondary (>1800 clock/>60 semester credit/>90 quarter credit hours)') then 2 
        end partDACAT,
        cohortstu.fosRn FOSPriority,
        cohortstu.isInternational isInternational,
        cohortstu.isNonDegreeSeeking isNonDegreeSeeking,
        cohortstu.fieldOfStudy,
        cohortstu.cipCode cipCode,
        cohortstu.awardLevel awardLevel,
        cohortstu.distanceEducationOption distanceEducationOption,
		cohortstu.degreeProgram degreeProgram
    from CohortSTU cohortstu
    where cohortstu.personId is not null
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
    select degreeENT.*,
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
    select fosENT.*,
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
-- ...Remove for testing
		and SUBSTR(CAST(fosENT.cipCodeVersion as STRING), 3, 2) >= 20 -- Current CIPCode standaard was released 2020. new specs introduced per decade. 
    )
    where fosRn = 1
),

DegreeFOSProgram as (
-- Should pull back DegreeMajor and DegreeACAT and give a program distanceEducationOption. This will we used to replace DegreeMajor in DegreeMajorSTU below.
select *
from (
    select programENT.*,
        --foscipc.cipcode cipCode,
		CONCAT(LPAD(SUBSTR(CAST(foscipc.cipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(foscipc.cipCode as STRING), 3, 6), 4, '0')) cipCode,
        degacat.awardLevel awardLevel,
        programENT.distanceEducationOption,
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
		left join AcadTermOrder termorder on termorder.termCode = programENT.termCodeEffective
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
	    --and programENT.isForAdmissions = 1
	    and ((programENT.recordActivityDate != CAST('9999-09-09' AS DATE)
		    and programENT.recordActivityDate <= repdateend.reportingDateEnd)
			    or programENT.recordActivityDate = CAST('9999-09-09' AS DATE)) 
    	and ((programENT.startDate != CAST('9999-09-09' AS DATE)
		    and programENT.startDate <= repdateend.reportingDateEnd)
			or programENT.startDate = CAST('9999-09-09' AS DATE)) 
		and ((programENT.startDate = CAST('9999-09-09' AS DATE)
			and  termorder.termOrder <= (select max(repperiod.termOrder)
										  from AcademicTermReporting repperiod))
				or programENT.startDate != CAST('9999-09-09' AS DATE))
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
		sum(1) totalPrograms,
		sum(case when distanceEducationOption = 'No distance Ed' then 1 else 0 end) DENotDistanceEd,
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
    case when degfosprog.awardLevel = 'Postsecondary (<300 clock/<9 semester credit/<13 quarter credit hours)' then '1a' 
        when degfosprog.awardLevel = 'Postsecondary (300-899 clock/9-29 semester credit/13-44 quarter credit hours)' then '1b'
        when degfosprog.awardLevel = 'Postsecondary (900-1800 clock/30-60 semester credit/45-90 quarter credit hours)' then '2'
        when degfosprog.awardLevel = 'Associates Degree' then 3 
        when degfosprog.awardLevel = 'Postsecondary (>1800 clock/>60 semester credit/>90 quarter credit hours)' then '4'
        when degfosprog.awardLevel = 'Bachelors Degree' then '5'
        when degfosprog.awardLevel = 'Post-baccalaureate Certificate' then '6'
        when degfosprog.awardLevel = 'Masters Degree' then '7'
        when degfosprog.awardLevel = 'Post-Masters Certificate' then '8'
        when degfosprog.awardLevel = 'Doctors Degree (Research/Scholarship)' then '17'
        when degfosprog.awardLevel = 'Doctors Degree (Professional Practice)' then '18'
        when degfosprog.awardLevel = 'Doctors Degree (Other)' then '19'
    end ACAT,
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
            select distinct refactorstu.FOSPriority MajorNum, 
                            refactorstu.cipCode CIPCode,
							refactorstu.awardLevel awardLevel,
                            SUM(refactorstu.reg1) FIELD1, --Nonresident Alien
                            SUM(refactorstu.reg2) FIELD2,
                            SUM(refactorstu.reg3) FIELD3, -- Hispanic/Latino
                            SUM(refactorstu.reg4) FIELD4,
                            SUM(refactorstu.reg5) FIELD5, -- American Indian or Alaska Native
                            SUM(refactorstu.reg6) FIELD6,
                            SUM(refactorstu.reg7) FIELD7, -- Asian
                            SUM(refactorstu.reg8) FIELD8,
                            SUM(refactorstu.reg9) FIELD9, -- Black or African American
                            SUM(refactorstu.reg10) FIELD10,
                            SUM(refactorstu.reg11) FIELD11, -- Native Hawaiian or Other Pacific Islander
                            SUM(refactorstu.reg12) FIELD12,
                            SUM(refactorstu.reg13) FIELD13, -- White
                            SUM(refactorstu.reg14) FIELD14,
                            SUM(refactorstu.reg15) FIELD15, -- Two or more races
                            SUM(refactorstu.reg16) FIELD16,
                            SUM(refactorstu.reg17) FIELD17, -- Race and ethnicity unknown
                            SUM(refactorstu.reg18) FIELD18                    
            from CohortRefactorSTU refactorstu
            where (refactorstu.FOSPriority = 1
                                or (refactorstu.FOSPriority = 2
									and refactorstu.awardLevel IN ('Associates Degree', 'Bachelors Degree',
																	'Masters Degree', 'Doctors Degree (Research/Scholarship)',
																	'Doctors Degree (Professional Practice)',
																	'Doctors Degree (Other)')))
            group by refactorstu.FOSPriority, 
                                refactorstu.cipCode,
                                refactorstu.awardLevel
            ) stuCIP on degfosprog.cipCode = stuCIP.cipCode
                and degfosprog.awardLevel = stuCIP.awardLevel
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
       ACAT                     awlevel,    --(AWLEVEL)		- 1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2;
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

union

--Part B: Completions - Distance Education
--Used to distinguish if an award is offered in a distance education format. A "distance education program" is "a program for which all the required coursework 
--for program completion is able to be completed via distance education courses."

select 'B',             --(PART)		- "B"
       majorNum,        --(MAJORNUM)	- 1 = First Major, 2 = Second Major
       cipCode,         --(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
       ACAT,            --(AWLEVEL)		- 1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2;
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

union

--Part C: All Completers
--Count each student only once, regardless of how many awards he/she earned. The intent of this screen is to collect an unduplicated count 
--of total numbers of completers.

select 'C' ,    --PART 	 - "C"
        null,
        null,
        null,
        null,
        coalesce(SUM(refactorstu.reg1), 0), --Nonresident Alien
	    coalesce(SUM(refactorstu.reg2), 0),
		coalesce(SUM(refactorstu.reg3), 0), -- Hispanic/Latino
        coalesce(SUM(refactorstu.reg4), 0),
        coalesce(SUM(refactorstu.reg5), 0), -- American Indian or Alaska Native
        coalesce(SUM(refactorstu.reg6), 0),
        coalesce(SUM(refactorstu.reg7), 0), -- Asian
        coalesce(SUM(refactorstu.reg8), 0),
        coalesce(SUM(refactorstu.reg9), 0), -- Black or African American
        coalesce(SUM(refactorstu.reg10), 0),
        coalesce(SUM(refactorstu.reg11), 0), -- Native Hawaiian or Other Pacific Islander
        coalesce(SUM(refactorstu.reg12), 0),
        coalesce(SUM(refactorstu.reg13), 0), -- White
        coalesce(SUM(refactorstu.reg14), 0),
        coalesce(SUM(refactorstu.reg15), 0), -- Two or more races
        coalesce(SUM(refactorstu.reg16), 0),
        coalesce(SUM(refactorstu.reg17), 0), -- Race and ethnicity unknown
        coalesce(SUM(refactorstu.reg18), 0) 
from CohortRefactorSTU refactorstu

union

--Part D: Completers by Level
--Each student should be counted once per award level. For example, if a student earned a master's degree and a doctor's degree, he/she 
--should be counted once in master's and once in doctor's. A student earning two master's degrees should be counted only once.

select 'D',                                                              --PART      - "D"
       null,
       null,
       partDACAT,                                                    --CTLEVEL - 2 to 9.
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
from CohortRefactorSTU refactorstu
group by partDACAT

union

--Dummy set to return default formatting if no student awards exist.
select *
from (
    VALUES
        ('A', 1, '01.0101', 2, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('B', 1, '01.0101', 2, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        --('C', null, null, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('D', null, null, 2, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null)
    ) as dummySet(PART, MAJORNUM, CIPC_LEVL, AWLEVEL, isDistanceED, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, 
                  FIELD10, FIELD11, FIELD12, FIELD13, FIELD14, FIELD15, FIELD16, FIELD17, FIELD18)
where not exists (select a.personId from CohortSTU a) 
--where not exists (select a.ACAT from DegreeFOSProgramSTU a) 

/*
union 

select *
from (
    VALUES
        --('A', 1, '01.0101', 1, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        --('B', 1, '01.0101', 1, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)--,
        ('C', null, null, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('D', null, null, 1, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null)
    ) as dummySet(PART, MAJORNUM, CIPC_LEVL, AWLEVEL, isDistanceED, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, 
                  FIELD10, FIELD11, FIELD12, FIELD13, FIELD14, FIELD15, FIELD16, FIELD17, FIELD18)
where not exists (select a.ACAT from CohortRefactorSTU a) 
*/
