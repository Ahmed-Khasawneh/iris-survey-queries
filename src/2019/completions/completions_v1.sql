%sql

/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Fall Collection
FILE NAME:      Completions
FILE DESC:      Completions for all institutions
AUTHOR:         Ahmed Khasawneh
CREATED:        20200605

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Cohort Creation
Award Level Offerings 
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)      	Author             	Tag             	Comments
-----------------   --------------------	-------------   	-------------------------------------------------
20200728			akhasawneh				ak 20200728			Added support for multiple/historic ingestions (PF-1579) -Run time 19m 27s, test data 24m, 23s
20200727            jhanicak                jh 20200727         Added upper() to strings for comparison
                                                                Added to_date functions to strip time
                                                                Changed nvl to coalesce
                                                                Multiple bug fixes
                                                                Modified views to use inner join in inner query and left join in outer query
                                                                -Run time 6m 9s, test data 7m, 45s
20200629            jhanicak                                    Changed Major.curriculumRuleActivityDate to Major.curriculumRuleActionDate in MajorCIPC view PF-1536
                                                                Changed Degree.curriculumRuleActivityDate to Degree.curriculumRuleActionDate in DegreeACAT view PF-1536
                                                                Added filter for Person.visaStartDate and Person.visaEndDate to CohortRefactorSTU view PF-1536
                                                                Replaced CAST('2015-10-01' as DATE) with CAST(award.awardedDate as DATE) in CohortSTU view
                                                                Brought IPEDSClientConfig.genderForNonBinary and genderForUnknown thru to AwardMCR and used in CohortRefactorSTU
                                                                Added additional test cases in DefaultValues -Run time 3m 46s (Prod default) 7m 21s (test defaults - added additional parts of term)
20200618			akhasawneh				ak 20200618			Modify Completions report query with standardized view naming/aliasing convention (PF-1530) -Run time 3m 58s (Prod default) 4m 12s (test defaults)
20200611            akhasawneh              ak 20200611         Modified to not reference term code as a numeric indicator of term ordering (PF-1494) -Run time 6m 42s (Prod default) 9m 26s (test defaults)
20200605            akhasawneh          			    		Initial version (Run time 12m 30s)
	
	
Implementation Notes:

What to Include:

    -   Formal awards conferred as the result of completion of an academic or occupational/vocational program of study. 
    (Note that only CIP codes describing academic or occupational/vocational programs of study are valid CIP codes on the 
    Completions component.) The instructional activity completed as part of the program of study must be credit-bearing,
    but can be measured in credit hours, contact hours, or some other unit of measurement.
    -   Awards conferred by the postsecondary institution.
    -   Awards conferred in the reporting period.
    -   Multiple awards conferred to a single student.

What to Exclude:
    -   Awards earned, but not yet conferred.
    -   Awards conferred by branches of your institution located in foreign countries.
    -   Honorary degrees.
    -   Awards conferred by an entity other than the postsecondary institution (such as the state, or an industry certificate).
    -   Informal awards such as certificates of merit, completion, attendance, or transfer.
    -   Awards earned as the result of an avocational, basic skills, residency, or other program not
    recognized by IPEDS as academic or occupational/vocational.

    **If there were no awards conferred for a specific CIP code and award level during the academic year
    but the institution still offers the program at that award level, submit a record (First major only)
    with the CIP code and award level with zeroes for CRACE01, CRACE02, CRACE13, CRACE14 and CRACE25 through CRACE38.
                     
********************/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.
--IPEDS specs define reporting period as July 1, 2018 and June 30, 2019.

select '1920' surveyYear,
	'COM' surveyId,
	'202010' termCode,
	'1' partOfTermCode,
	CAST('2018-07-01' as DATE) reportingDateStart,
    CAST('2019-06-30' as DATE) reportingDateEnd,
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

select '1415' surveyYear,
	'COM' surveyId,
	'201410' termCode,
	'1' partOfTermCode,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'D' compGradDateOrTerm --D = Date, T = Term
    
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
    'D' compGradDateOrTerm --D = Date, T = Term

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
    'D' compGradDateOrTerm --D = Date, T = Term
    
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
    'D' compGradDateOrTerm --D = Date, T = Term    
    
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
    'D' compGradDateOrTerm --D = Date, T = Term

*/
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

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
-- ak 20200728 Adding snapshotDate reference
    ConfigLatest.snapshotDate snapshotDate,
    ConfigLatest.tags
from (
	select clientConfigENT.surveyCollectionYear surveyYear,
-- ak 20200728 Adding snapshotDate reference
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
				clientConfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientConfigENT
		cross join DefaultValues defvalues
	where clientConfigENT.surveyCollectionYear = defvalues.surveyYear
	    and array_contains(clientConfigENT.tags, 'June End')
		
	union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select defvalues.surveyYear surveyYear,
-- ak 20200728 Adding snapshotDate reference
	    CAST('9999-09-09' as DATE) snapshotDate,
	    null tags,
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
	where defvalues.surveyYear not in (select MAX(config.surveyCollectionYear)
										from IPEDSClientConfig config
										where config.surveyCollectionYear = defvalues.surveyYear)
    ) ConfigLatest
where ConfigLatest.configRn = 1
),

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 

select DISTINCT
    RepDates.surveyYear surveyYear,
-- ak 20200728 Adding snapshotDate reference
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
-- ak 20200728 Adding snapshotDate reference
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
				repPeriodENT.recordActivityDate desc
        ) reportPeriodRn	
    from IPEDSReportingPeriod repPeriodENT
        inner join ClientConfigMCR clientconfig on repperiodENT.surveyCollectionYear = clientconfig.surveyYear
            and upper(repperiodENT.surveyId) = clientconfig.surveyId
            and repperiodENT.termCode is not null
            and repperiodENT.partOfTermCode is not null
            and array_contains(repperiodENT.tags, 'June End')

	union
	
--Pulls default values when IPEDSReportingPeriod record doesn't exist
	select clientconfig.surveyYear surveyYear,
		'default' source, 
-- ak 20200728 Adding snapshotDate reference
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
	where clientconfig.surveyYear not in (select repPeriodENT.surveyCollectionYear
										  from IPEDSReportingPeriod repPeriodENT
										  where repPeriodENT.surveyCollectionYear = clientconfig.surveyYear
											and upper(repPeriodENT.surveyId) = clientconfig.surveyId)

	)  RepDates
where RepDates.reportPeriodRn = 1
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for each term and part of term code. 
--PartOfTerm code defines a subcategory of a termCode that may have different start, end and census dates. 
-- ak 20200728 Move to after the ReportingPeriodMCR and create a 3rd view to join the ReportingPeriodMCR and AcademicTermMCR

select termCode, 
	partOfTermCode, 
	to_date(startDate, 'YYYY-MM-DD') startDate,
	to_date(endDate, 'YYYY-MM-DD') endDate,
	academicYear,
	to_date(censusDate, 'YYYY-MM-DD') censusDate,
-- ak 20200728 Adding snapshotDate reference and determining termCode tied to the snapshotDate
    snapshotTermCode,
    to_date(snapshotDate, 'YYYY-MM-DD') snapshotDate,
    tags,
    snapshotPartTermCode
from (
    select distinct acadtermENT.termCode, 
-- ak 20200728 Adding snapshotDate reference and determining termCode tied to the snapshotDate
        case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
            then acadTermENT.termCode 
            else null end snapshotTermCode,
        case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
            then acadTermENT.partOfTermCode 
            else null end snapshotPartTermCode,
        row_number() over (
            partition by 
                acadTermENT.termCode,
                acadTermENT.partOfTermCode
            order by
                case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                            and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
                    then acadTermENT.termCode 
                else acadTermENT.snapshotDate end desc,
                case when to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') <= to_date(date_add(acadTermENT.censusdate, 3), 'YYYY-MM-DD') 
                            and to_date(acadTermENT.snapshotDate, 'YYYY-MM-DD') >= to_date(date_sub(acadTermENT.censusDate, 1), 'YYYY-MM-DD') 
                    then acadTermENT.partOfTermCode 
                else acadTermENT.snapshotDate end desc,
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
),

AcadTermOrder as (
-- Orders term codes based on date span and keeps the numeric value of the greatest term/part of term record. 
--ak 20200616 Adding term order indicator (PF-1494)

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
-- ak 20200728 Added AcademicTermReporting view

select *,
    to_date(case when compGradDateOrTerm = 'D' 
        then surveyDateStart
        else minStartDate
	end, 'YYYY-MM-DD') reportingDateStart,  --minimum start date of reporting for either date or term option for client
    to_date(case when compGradDateOrTerm = 'D' 
        then surveyDateEnd
        else maxEndDate
	end, 'YYYY-MM-DD') reportingDateEnd --maximum end date of reporting for either date or term option for client
from (
    select
        acadterm.termCode termCode,
        acadterm.partOfTermCode partOfTermCode,
        termorder.termOrder termOrder,
-- ak 20200728 Adding snapshotTermCode reference
        acadterm.snapshotTermCode snapshotTermCode,
        acadterm.snapshotDate snapshotDate,
        acadterm.startDate startDate,
        acadterm.tags tags,
        acadterm.endDate endDate,
        repperiod.compGradDateOrTerm compGradDateOrTerm,
        repperiod.reportingDateStart surveyDateStart,
        repperiod.reportingDateEnd surveyDateEnd,
        repperiod.genderForUnknown genderForUnknown,
        repperiod.genderForNonBinary genderForNonBinary,
        (select min(acadterm.startDate)
        from ReportingPeriodMCR repperiod1
		    inner join AcademicTermMCR acadterm
			    on acadterm.termCode = repperiod1.termCode 
			    and acadterm.partOfTermCode = repperiod1.partOfTermCode) minStartDate,
        (select max(acadterm.endDate)
        from ReportingPeriodMCR repperiod1
		    inner join AcademicTermMCR acadterm
			    on acadterm.termCode = repperiod1.termCode 
			    and acadterm.partOfTermCode = repperiod1.partOfTermCode) maxEndDate
    from AcademicTermMCR acadterm 
        left join ReportingPeriodMCR repperiod
	        on acadterm.termCode = repperiod.termCode
	            and acadterm.partOfTermCode = repperiod.partOfTermCode
 --ak 20200616 Adding term order indicator (PF-1494)
		inner join AcadTermOrder termorder
			on termOrder.termCode = repperiod.termCode
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
--ak 20200611 Adding term order indicator (PF-1494)
        repperiod.termOrder termOrder, 
        repperiod.genderForUnknown genderForUnknown,
        repperiod.genderForNonBinary genderForNonBinary,
-- ak 20200728 Adding snapshotDate/snapshotTerm reference
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
-- ak 20200728 Added AwardRefactor view

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
where ((repperiod.compGradDateOrTerm = 'D' 
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
        award2.campus campus,
		campusRec.isInternational isInternational,
		campusRec.snapshotDate snapshotDate,
        row_number() over (
            partition by
                award2.personId,
                award2.campus
            order by
                campusRec.recordActivityDate desc
        ) campusRn
    from AwardRefactor award2
		left join (select upper(campusENT.campus) campus,
                            campusENT.isInternational isInternational,
                            to_date(campusENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
-- ak 20200728 Adding snapshotDate reference.
                            to_date(campusENT.snapshotDate,'YYYY-MM-DD') snapshotDate
                    from AwardRefactor award
                        inner join Campus campusENT on upper(campusENT.campus) = award.campus
-- ak 20200728 Adding snapshotDate reference to return Campus information from the award record's snapshot
                            and to_date(campusENT.snapshotDate,'YYYY-MM-DD') = award.snapshotDate
                            and campusENT.isIpedsReportable = 1
        ) CampusRec on award2.campus = CampusRec.campus    
-- ak 20200728 Adding snapshotDate reference to return Campus information from the award record's snapshot
            and award2.snapshotDate = CampusRec.snapshotDate
-- Remove for testing... 
		and ((CampusRec.recordActivityDate != CAST('9999-09-09' as DATE)
			and CampusRec.recordActivityDate <= award2.awardedDate)
				or CampusRec.recordActivityDate = CAST('9999-09-09' as DATE))
-- ...Remove for testing
	)
where campusRn = 1
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

select *
from (
    select award2.personId,
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
                        PersonRec.personId
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
-- ak 20200728 Adding snapshotDate reference.
                        to_date(personENT.snapshotDate,'YYYY-MM-DD') snapshotDate
                from AwardRefactor award
                    inner join Person personENT on award.personId = personENT.personId 
-- ak 20200728 Adding snapshotDate reference to return Person information from the award record's snapshot
                                and to_date(personENT.snapshotDate,'YYYY-MM-DD') = award.snapshotDate
                        and personENT.isIpedsReportable = 1
                ) PersonRec on award2.personId = PersonRec.personId 
-- ak 20200728 Adding snapshotDate reference to return Person information from the award record's snapshot
                and award2.snapshotDate = PersonRec.snapshotDate
--ak 20200406 Including dummy date changes. (PF-1368)
                and ((PersonRec.recordActivityDate != CAST('9999-09-09' AS DATE)
                    and PersonRec.recordActivityDate <= award2.awardedDate) 
                        or PersonRec.recordActivityDate = CAST('9999-09-09' AS DATE))
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
		curriculumCode curriculumCode,		
		college college,
        fieldOfStudyPriority fieldOfStudyPriority,
-- ak 20200728 Adding snapshotDate reference.
        snapshotDate snapshotDate,
-- jh 20200227 add rownumber of fieldOfStudyPriority in order to get the 2 minimum values
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
        award2.degree degree,
        award2.degreeLevel degreeLevel,
		AcadTrackRec.fieldOfStudyPriority fieldOfStudyPriority,
		AcadTrackRec.curriculumCode curriculumCode,
		award2.college college,
-- ak 20200728 Adding snapshotDate reference.
		award2.snapshotDate snapshotDate,
		row_number() over (
			partition by
			    award2.personId,
				AcadTrackRec.personId,
                AcadTrackRec.fieldOfStudyPriority
			order by
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
				AcadTrackRec.termOrder desc,
				AcadTrackRec.recordActivityDate desc
		) acadtrackRn
	from AwardRefactor award2
	    left join ( 
                select distinct acadtrackENT.personId personId,
                        acadtrackENT.termCodeEffective termCodeEffective,
                        termorder.termOrder termOrder,
                        to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
                        acadtrackENT.academicTrackLevel academicTrackLevel,
                        acadtrackENT.fieldOfStudyPriority fieldOfStudyPriority,
                        upper(acadtrackENT.curriculumCode) curriculumCode,
                        upper(acadtrackENT.degree) degree,
                        upper(acadtrackENT.college) college,
                        acadtrackENT.curriculumStatus curriculumStatus,
-- ak 20200728 Adding snapshotDate reference.
                        to_date(acadtrackENT.snapshotDate, 'YYYY-MM-DD') snapshotDate
            from AwardRefactor award
                inner join AcademicTrack acadtrackENT ON award.personId = acadTrackENT.personId
                    and award.degree = upper(acadTrackENT.degree)
                    and (award.college = upper(acadTrackENT.college)
                        or acadTrackENT.college is null)
                    and award.degreeLevel = acadTrackENT.academicTrackLevel
                    and acadTrackENT.fieldOfStudyType = 'Major'
                    and acadTrackENT.curriculumStatus = 'Completed'
-- ak 20200728 Adding snapshotDate reference to return AcademicTrack information from the award record's snapshot
                    and to_date(acadTrackENT.snapshotDate, 'YYYY-MM-DD') = award.snapshotDate
                    and acadTrackENT.isIpedsReportable = 1
                inner join AcadTermOrder termorder on termorder.termCode = acadtrackENT.termCodeEffective
            ) AcadTrackRec ON award2.personId = AcadTrackRec.personId 
                                and award2.degree = AcadTrackRec.degree
			    and (award2.college = AcadTrackRec.college
                        or AcadTrackRec.college is null)
			    and award2.degreeLevel = AcadTrackRec.academicTrackLevel
--ak 20200406 Including dummy date changes. (PF-1368)
                and ((AcadTrackRec.recordActivityDate != CAST('9999-09-09' AS DATE)
				and AcadTrackRec.recordActivityDate <= award2.awardedDate)
                    or AcadTrackRec.recordActivityDate = CAST('9999-09-09' AS DATE)) 
--ak 20200611 Adding changes for termCode ordering. (PF-1494)
--***need more testing on this filter**
-----                and AcadTrackRec.termOrder <= award2.termOrder
-- ak 20200728 Adding snapshotDate reference to return AcademicTrack information from the award record's snapshot
                and award2.snapshotDate = AcadTrackRec.snapshotDate
	)
where acadtrackRn = 1
),

DegreeMCR as (
--Returns most up to 'degree' information as of the reporting term codes and part of term census periods.
--This information is used for Retention cohort filtering on awardLevel
-- ak 20200427 Adding Degree entitiy to pull award level for 'Bachelor level' Retention cohort filtering. (PF-1434)

select *
from (
	select acadtrack2.personId personId,
		acadtrack2.degree degree,
	    acadtrack2.degreeLevel degreeLevel,
	    DegreeRec.awardLevel awardLevel,
	    DegreeRec.ACAT ACAT,
	    DegreeRec.isNonDegreeSeeking isNonDegreeSeeking,
	    DegreeRec.snapshotDate snapshotDate,
		row_number() over (
			partition by
			    acadtrack2.personId,
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
	               case when degreeENT.awardLevel = 'Postsecondary < 1 year Award' then 1            	-- 1 - Postsecondary award, certificate, or diploma of (less than 1 academic year)
                        when degreeENT.awardLevel = 'Postsecondary > 1 year < 2 years Award' then 2   	-- 2 - Postsecondary award, certificate, or diploma of (at least 1 but less than 2 academic years)
                        when degreeENT.awardLevel = 'Associates Degree' then 3                        	-- 3 - Associate's degree
                        when degreeENT.awardLevel = 'Postsecondary > 2 years < 4 years Award' then 4  	-- 4 - Postsecondary award, certificate, or diploma of (at least 2 but less than 4 academic years)
                        when degreeENT.awardLevel = 'Bachelors Degree' then 5                        	-- 5 - Bachelor's degree
                        when degreeENT.awardLevel = 'Post-baccalaureate Certificate' then 6           	-- 6 - Postbaccalaureate certificate
                        when degreeENT.awardLevel = 'Masters Degree' then 7                          	-- 7 - Master's degree
                        when degreeENT.awardLevel = 'Post-Masters Certificate' then 8                	-- 8 - Post-master's certificate
                        when degreeENT.awardLevel = 'Doctors Degree (Research/Scholarship)' then 17		-- 17 - Doctor's degree - research/scholarship
                        when degreeENT.awardLevel = 'Doctors Degree (Professional Practice)' then 18	-- 18 - Doctor's degree - professional practice
                        when degreeENT.awardLevel = 'Doctors Degree (Other)' then 19					-- 19 - Doctor's degree - other
                    end ACAT,
	               degreeENT.isNonDegreeSeeking isNonDegreeSeeking,
-- ak 20200728 Adding snapshotDate reference.
                   to_date(degreeENT.snapshotDate,'YYYY-MM-DD') snapshotDate
            from AcademicTrackMCR acadtrack
                    inner join Degree degreeENT ON upper(degreeENT.degree) = acadtrack.degree
                            and acadtrack.degreeLevel = degreeENT.degreeLevel
-- ak 20200728 Adding snapshotDate reference to return Degree information from the award record's snapshot
                            and to_date(degreeENT.snapshotDate,'YYYY-MM-DD') = acadtrack.snapshotDate
			                and degreeENT.isIpedsReportable = 1
			        ) DegreeRec on DegreeRec.degree = acadtrack2.degree
-- ak 20200406 Including dummy date changes. (PF-1368)
			and ((DegreeRec.recordActivityDate != CAST('9999-09-09' AS DATE)
				and DegreeRec.recordActivityDate <= acadtrack2.awardedDate)
					or DegreeRec.recordActivityDate = CAST('9999-09-09' AS DATE)) 
-- ak 20200728 Adding snapshotDate reference to return AcademicTrack information from the award record's snapshot
            and acadtrack2.snapshotDate = DegreeRec.snapshotDate
	)
where degreeRn = 1
),

MajorMCR as (
--Returns most up to 'major' information as of the reporting term codes and part of term census periods.

select *
from (
	select acadtrack2.personId personId,
		acadtrack2.curriculumCode major,
--jh 20200727 if cipCode is null, default to cipCode for General Studies
		coalesce(CONCAT(LPAD(SUBSTR(CAST(MajorRec.cipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(MajorRec.cipCode as STRING), 3, 6), 4, '0')), '24.0102') cipCode, 
		MajorRec.isDistanceEd,
		row_number() over (
			partition by
			    acadTrack2.personId,
			    acadtrack2.curriculumCode,
				MajorRec.major
			order by
				MajorRec.recordActivityDate desc
		) majorRn
	from AcademicTrackMCR acadtrack2
		left join (
		    select upper(majorENT.major) major,
		           to_date(majorENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
		           majorENT.cipCode cipCode, 
		           majorENT.isDistanceEd isDistanceEd,
-- ak 20200728 Adding snapshotDate reference.
                   to_date(majorENT.snapshotDate,'YYYY-MM-DD') snapshotDate
		      from AcademicTrackMCR acadtrack
		        inner join Major majorENT ON upper(majorENT.major) = acadtrack.curriculumCode
-- ak 20200728 Adding snapshotDate reference to return Major information from the award record's snapshot
                    and to_date(majorENT.snapshotDate,'YYYY-MM-DD') = acadtrack.snapshotDate
			        and majorENT.isIpedsReportable = 1
			  ) MajorRec on MajorRec.major = acadtrack2.curriculumCode
-- ak 20200728 Adding snapshotDate reference to return Major information from the award record's snapshot
                    and MajorRec.snapshotDate = acadtrack2.snapshotDate
-- ak 20200406 Including dummy date changes. (PF-1368)
			and ((MajorRec.recordActivityDate != CAST('9999-09-09' AS DATE)
				and MajorRec.recordActivityDate <= acadtrack2.awardedDate)
					or MajorRec.recordActivityDate = CAST('9999-09-09' AS DATE)) 
	)
where majorRn = 1
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
    floor(DATEDIFF(CAST(award.awardedDate as DATE), CAST(person.birthDate as DATE)) / 365) asOfAge,
    floor(DATEDIFF(award.awardedDate, person.birthDate) / 365) asOfAge2,
    award.degree degree,
    award.awardedDate awardedDate,
    award.awardedTermCode awardedTermCode,
    award.genderForUnknown genderForUnknown,
    award.genderForNonBinary genderForNonBinary,
    campus.isInternational,
    acadtrack.fieldOfStudyPriority FOSPriority,
    acadtrack.fosRn fosRn,
    degree.awardLevel awardLevel,
    degree.ACAT ACAT,
    degree.isNonDegreeSeeking isNonDegreeSeeking,
    major.major major,
    major.cipCode cipCode,
    major.isDistanceEd isDistanceEd
from AwardRefactor award
	inner join PersonMCR person on person.personId = award.personId
	inner join AcademicTrackMCR acadtrack on acadtrack.personId = award.personId
--jh 20200227 pull the 2 minimum fieldOfStudyPriority values
		and acadtrack.fosRn < 3
	inner join DegreeMCR degree on degree.personId = award.personId
	    and degree.awardLevel is not null
	inner join MajorMCR major on major.personId = award.personId
    inner join CampusMCR campus on campus.campus = award.campus
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
        cohortstu.ACAT ACAT,
        cohortstu.fosRn FOSPriority,
        cohortstu.isInternational isInternational,
        cohortstu.isNonDegreeSeeking isNonDegreeSeeking,
        cohortstu.major,
        cohortstu.cipCode cipCode,
        cohortstu.awardLevel awardLevel,
        case when cohortstu.isDistanceEd = 'Y' then 1
            else 2
        end isDistanceEd
    from CohortSTU cohortstu
    )
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
        degreeENT.awardLevel awardLevel,
        case when degreeENT.awardLevel = 'Postsecondary < 1 year Award' then 1            	-- 1 - Postsecondary award, certificate, or diploma of (less than 1 academic year)
            when degreeENT.awardLevel = 'Postsecondary > 1 year < 2 years Award' then 2   	-- 2 - Postsecondary award, certificate, or diploma of (at least 1 but less than 2 academic years)
            when degreeENT.awardLevel = 'Associates Degree' then 3                        	-- 3 - Associate's degree
            when degreeENT.awardLevel = 'Postsecondary > 2 years < 4 years Award' then 4  	-- 4 - Postsecondary award, certificate, or diploma of (at least 2 but less than 4 academic years)
            when degreeENT.awardLevel = 'Bachelors Degree' then 5                        	-- 5 - Bachelor's degree
            when degreeENT.awardLevel = 'Post-baccalaureate Certificate' then 6           	-- 6 - Postbaccalaureate certificate
            when degreeENT.awardLevel = 'Masters Degree' then 7                          	-- 7 - Master's degree
            when degreeENT.awardLevel = 'Post-Masters Certificate' then 8                	-- 8 - Post-master's certificate
            when degreeENT.awardLevel = 'Doctors Degree (Research/Scholarship)' then 17		-- 17 - Doctor's degree - research/scholarship
            when degreeENT.awardLevel = 'Doctors Degree (Professional Practice)' then 18	-- 18 - Doctor's degree - professional practice
            when degreeENT.awardLevel = 'Doctors Degree (Other)' then 19					-- 19 - Doctor's degree - other
        end ACAT,
		row_number() over (
			partition by
				degreeENT.degree,
				degreeENT.awardLevel,
				degreeENT.curriculumRule
			order by
-- ak 20200728 Adding snapshotDate to return most recent snapshot. Not tied to awardedDate.
			    degreeENT.snapshotDate desc,
				degreeENT.curriculumRuleActionDate desc,
				degreeENT.recordActivityDate desc
		) as degreeRn
    from AcademicTermReporting repperiod
        inner join Degree degreeENT on to_date(degreeENT.curriculumRuleActionDate,'YYYY-MM-DD') <= repperiod.reportingDateEnd
            and degreeENT.awardLevel is not null
		    and degreeENT.isIpedsReportable = 1
-- Remove for testing...
		and ((to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
			and to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.reportingDateEnd)
				or to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
-- ...Remove for testing
-- ak 20200611 Adding term order indicator (PF-1494)
		left join AcadTermOrder acadterm on degreeENT.curriculumRuleTermCodeEff = acadterm.termCode
	where acadterm.termorder <= repperiod.termorder  
-- jh 20200727 Added this filter, but based on client testing, might need to remove 
	    or degreeENT.curriculumRuleTermCodeEff = '000000'
   )
where degreeRn = 1
),

DegreeMajor as ( 
-- Pulls major information as of the reporting period and joins to the degrees on curriculumRule

select degreeacat.degree degree,
    degreeacat.ACAT ACAT,
    degreeacat.curriculumRuleTermCodeEff degreeTCEff,
    majorcipc.major major,
    coalesce(CONCAT(LPAD(SUBSTR(CAST(majorcipc.cipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(majorcipc.cipCode as STRING), 3, 6), 4, '0')), '24.0102') cipCode, --default to cipCode for General Studies
    majorcipc.curriculumRuleTermCodeEff majorTCEff,
    case when SUM(case when majorcipc.isDistanceEd = 1 then 1 else 0 end) > 0 then 1 
         else 2
    end isDistanceEd
from DegreeACAT degreeacat
    inner join (
            select upper(majorENT.major) major,
                majorENT.cipCode cipCode, 
                majorENT.curriculumRule curriculumRule,
                majorENT.curriculumRuleTermCodeEff curriculumRuleTermCodeEff,
                majorENT.isDistanceEd isDistanceEd,
                row_number() over (
                    partition by
                        majorENT.major,
                        majorENT.cipCode,
                        majorENT.curriculumRule
                    order by
-- ak 20200728 Adding snapshotDate to return most recent snapshot. Not tied to awardedDate.
                        majorENT.snapshotDate desc,
                        majorENT.curriculumRuleActionDate desc,
                        majorENT.recordActivityDate desc
                ) as majorRn
            from AcademicTermReporting repperiod
                inner join Major majorENT on to_date(majorENT.curriculumRuleActionDate,'YYYY-MM-DD') <= repperiod.reportingDateEnd
                    and majorENT.isIpedsReportable = 1
-- Remove for testing...
                and ((to_date(majorENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                    and to_date(majorENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.reportingDateEnd)
                        or to_date(majorENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
-- ...Remove for testing
--ak 20200611 Adding term order indicator (PF-1494)
                left join AcadTermOrder acadterm on majorENT.curriculumRuleTermCodeEff = acadterm.termCode
            where acadterm.termorder <= repperiod.termorder 
-- jh 20200727 Added this filter, but based on client testing, might need to remove 
                or majorENT.curriculumRuleTermCodeEff = '000000' 
            ) majorcipc on degreeacat.curriculumRule = majorcipc.curriculumRule
where majorcipc.majorRn = 1
group by majorcipc.cipCode,
        majorcipc.major,
        degreeacat.ACAT,
        degreeacat.degree,
        majorcipc.curriculumRuleTermCodeEff,
        degreeacat.curriculumRuleTermCodeEff
),

DegreeMajorSTU as (
-- Pulls all CIPCodes and ACAT levels including student awarded levels.

select coalesce(stuCIP.MajorNum, 1) majorNum,
	degmajor.cipCode cipCode,
	degmajor.ACAT ACAT,
	coalesce(stuCIP.isDistanceEd, degmajor.isDistanceEd) isDistanceEd,
	stuCIP.FIELD1 FIELD1,
	stuCIP.FIELD2 FIELD2,
	stuCIP.FIELD3 FIELD3,
	stuCIP.FIELD4 FIELD4,
	stuCIP.FIELD5 FIELD5,
	stuCIP.FIELD6 FIELD6,
	stuCIP.FIELD7 FIELD7,
	stuCIP.FIELD8 FIELD8,
	stuCIP.FIELD9 FIELD9,
	stuCIP.FIELD10 FIELD10,
	stuCIP.FIELD11 FIELD11,
	stuCIP.FIELD12 FIELD12,
	stuCIP.FIELD13 FIELD13,
	stuCIP.FIELD14 FIELD14,
	stuCIP.FIELD15 FIELD15,
	stuCIP.FIELD16 FIELD16,
	stuCIP.FIELD17 FIELD17,
	stuCIP.FIELD18 FIELD18
from DegreeMajor degmajor
    left join (
            select distinct refactorstu.FOSPriority MajorNum, 
                            refactorstu.cipCode CIPCode,
                            refactorstu.ACAT ACAT,
                            case when SUM(case when refactorstu.isDistanceEd = 1 then 1 else 0 end) > 0 then 1 
                                else 2
                            end isDistanceEd,
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
                                and refactorstu.ACAT IN (3, 5, 7, 17, 18, 19)))
            group by refactorstu.FOSPriority, 
                                refactorstu.cipCode,
                                refactorstu.ACAT
            ) stuCIP on degmajor.cipCode = stuCIP.cipCode
                and degmajor.ACAT = stuCIP.ACAT
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

select 'A'                      part,       	--(PART)	- "A"
       majorNum                 majornum,   --(MAJORNUM)	- 1 = First Major, 2 = Second Major
       cipCode                  cipcode,    --(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
       ACAT                     awlevel,    --(AWLEVEL)		- 1 to 8 and 17 to 19 for MAJORNUM=1 & 3,5,7,17,18,19 for MAJORNUM=2
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
from DegreeMajorSTU

union

--Part B: Completions - Distance Education
--Used to distinguish if an award is offered in a distance education format. A "distance education program" is "a program for which all the required coursework 
--for program completion is able to be completed via distance education courses."

select 'B',             --(PART)		- "B"
       majorNum,        --(MAJORNUM)	- 1 = First Major, 2 = Second Major
       cipCode,         --(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
       ACAT,            --(AWLEVEL)		- 1 to 8 and 17 to 19 for MAJORNUM=1 & 3,5,7,17,18,19 for MAJORNUM=2
       isDistanceEd,    --(DistanceED)	- 1= Yes, 2=No
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
       null,
       null,
       null
from DegreeMajorSTU

union

--Part C: All Completers
--Count each student only once, regardless of how many awards he/she earned. The intent of this screen is to collect an unduplicated count 
--of total numbers of completers.

select 'C',    --PART 	 - "C"
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
       ACAT,                                                    --CTLEVEL - 1 to 7
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
group by ACAT

union

--Dummy set to return default formatting if no student awards exist.
select *
from (
    VALUES
        ('A', 1, '01.0101', 1, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('B', 1, '01.0101', 1, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        --('C', null, null, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('D', null, null, 1, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null)
    ) as dummySet(PART, MAJORNUM, CIPC_LEVL, AWLEVEL, isDistanceED, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, 
                  FIELD10, FIELD11, FIELD12, FIELD13, FIELD14, FIELD15, FIELD16, FIELD17, FIELD18)
where not exists (select a.personId from CohortSTU a) 
