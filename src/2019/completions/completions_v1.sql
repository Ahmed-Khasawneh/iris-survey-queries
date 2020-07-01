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

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

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
--Use for testing internally only
--July 1, YYYY-1 and June 30, YYYY.
select '1415' surveyYear,
    'COM' surveyId,
    '201330' termCode,
    '1' partOfTermCode,
    CAST('2013-07-01' as DATE) reportingDateStart, --CAST('2013-06-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, --CAST('2014-05-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term

union

select '1415' surveyYear,
    'COM' surveyId,
    '201410' termCode,
    '1' partOfTermCode,
    CAST('2013-07-01' as DATE) reportingDateStart, --CAST('2013-06-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, --CAST('2014-05-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
    
union

select '1415' surveyYear,
    'COM' surveyId,
    '201420' termCode,
    '1' partOfTermCode,
    CAST('2013-07-01' as DATE) reportingDateStart, --CAST('2013-06-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, --CAST('2014-05-30' as DATE) reportingDateEnd,
    'A' acadOrProgReporter, --A = Academic, P = Program
    'M' genderForUnknown, --M = Male, F = Female
    'F' genderForNonBinary,  --M = Male, F = Female
    'T' compGradDateOrTerm --D = Date, T = Term
*/
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for each term and part of term code. 
--PartOfTerm code defines a subcategory of a termCode that may have different start, end and census dates. 
select *,
--ak 20200611 Adding term order indicator (PF-1494)
    row_number() over (
        order by  
            startDate asc,
            endDate asc
    ) termOrder
from ( 
		select acadTermENT.*,
			row_number() over (
				partition by 
					acadTermENT.termCode,
					acadTermENT.partOfTermCode
				order by  
					acadTermENT.recordActivityDate desc
			) acadTermRn
		from AcademicTerm acadTermENT
		where acadTermENT.isIPEDSReportable = 1 
	)
where acadTermRn = 1
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

select DISTINCT 
    ConfigLatest.surveyYear surveyYear,
    ConfigLatest.surveyId surveyId,
    ConfigLatest.termCode termCode,
    ConfigLatest.partOfTermCode partOfTermCode,
    ConfigLatest.reportingDateStart reportingDateStart,
    ConfigLatest.reportingDateEnd reportingDateEnd,
    ConfigLatest.acadOrProgReporter acadOrProgReporter,
    ConfigLatest.genderForUnknown genderForUnknown,
    ConfigLatest.genderForNonBinary genderForNonBinary,
    ConfigLatest.compGradDateOrTerm compGradDateOrTerm
from (
	select clientConfigENT.surveyCollectionYear surveyYear,
		defvalues.surveyId surveyId,
		defvalues.termCode termCode,
		defvalues.partOfTermCode partOfTermCode,
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		NVL(clientConfigENT.acadOrProgReporter, defvalues.acadOrProgReporter) acadOrProgReporter,
		NVL(clientConfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		NVL(clientConfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
		NVL(clientConfigENT.compGradDateOrTerm, defvalues.compGradDateOrTerm) compGradDateOrTerm,
		row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
				clientConfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientConfigENT
		cross join DefaultValues defvalues
	where clientConfigENT.surveyCollectionYear = defvalues.surveyYear
		
	union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select defvalues.surveyYear surveyYear,
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
    RepDates.termCode termCode,	
--ak 20200611 Adding term order indicator (PF-1494)    
    (select max(acadterm1.termOrder)
     from AcademicTermMCR acadterm1 
     where acadterm1.termCode = RepDates.termCode) termOrder,
    case when RepDates.compGradDateOrTerm = 'D' 
        then RepDates.reportingDateStart
        else RepDates.minStartDate
	end censusStart,  
    case when RepDates.compGradDateOrTerm = 'D' 
        then RepDates.reportingDateEnd
        else RepDates.maxEndDate
	end censusEnd,  
    RepDates.compGradDateOrTerm compGradDateOrTerm
from (
    select repPeriodENT.surveyCollectionYear surveyYear,
        clientconfig.surveyId surveyId,
        NVL(repPeriodENT.termCode, clientconfig.termCode) termCode,
        (select min(acadterm.startDate)
         from IPEDSReportingPeriod ReportPeriodENT1
			 inner join AcademicTermMCR acadterm
				on acadterm.termCode = ReportPeriodENT1.termCode 
				and acadterm.partOfTermCode = ReportPeriodENT1.partOfTermCode) minStartDate,
        (select max(acadterm.endDate)
         from IPEDSReportingPeriod ReportPeriodENT1
			inner join AcademicTermMCR acadterm
				on acadterm.termCode = ReportPeriodENT1.termCode 
				and acadterm.partOfTermCode = ReportPeriodENT1.partOfTermCode) maxEndDate,
        NVL(repPeriodENT.reportingDateStart, clientconfig.reportingDateStart) reportingDateStart,
        NVL(repPeriodENT.reportingDateEnd, clientconfig.reportingDateEnd) reportingDateEnd,
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
		cross join ClientConfigMCR clientconfig
    where repPeriodENT.surveyCollectionYear = clientconfig.surveyYear
		and repPeriodENT.surveyId = clientconfig.surveyId
	
--Added unions to use default values when IPEDSReportingPeriod record doesn't exist so valid output file will always be generated

	union
	
--Pulls default values for Fall when IPEDSReportingPeriod record doesn't exist
	select clientconfig.surveyYear surveyYear,
        clientconfig.surveyId surveyId,
        clientconfig.termCode termCode,
        (select min(acadterm1.startDate)
         from ClientConfigMCR clientconfig1
			inner join AcademicTermMCR acadterm1
				on acadterm1.termCode = clientconfig1.termCode 
				and acadterm1.partOfTermCode = clientconfig1.partOfTermCode),
        (select max(acadterm1.endDate)
         from ClientConfigMCR clientconfig1
			inner join AcademicTermMCR acadterm1
				on acadterm1.termCode = clientconfig1.termCode 
				and acadterm1.partOfTermCode = clientconfig1.partOfTermCode),
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
											and repPeriodENT.surveyId = clientconfig.surveyId)
	)  RepDates
inner join AcademicTermMCR acadterm 
	on acadterm.termCode = RepDates.termCode
where RepDates.reportPeriodRn = 1
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

AwardMCR as (
--Pulls all distinct student awards obtained withing the reporting terms/dates.

select DISTINCT *
from (
    select awardENT.*,
--ak 20200611 Adding term order indicator (PF-1494)
        repperiod.termOrder termOrder,
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
                awardENT.recordActivityDate desc
        ) as AwardRn
	from Award awardENT 
		cross join ReportingPeriodMCR repperiod
	where awardENT.isIpedsReportable = 1
		and ((repperiod.compGradDateOrTerm = 'D'
			and (awardENT.awardedDate >= repperiod.censusStart
				and awardENT.awardedDate <= repperiod.censusEnd))
			or (repperiod.compGradDateOrTerm = 'T'
				and awardENT.awardedTermCode = repperiod.termCode))
		and awardENT.awardStatus = 'Awarded'
		and awardENT.degreeLevel is not null
		and awardENT.degreeLevel != 'Continuing Ed'
-- Remove for testing...
		and ((awardENT.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
			and awardENT.recordActivityDate <= repperiod.censusEnd)
				or awardENT.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
    )
where AwardRn = 1
),

CampusMCR as ( 
-- Returns most recent campus record for each campus available per the ReportingPeriod.

select *
from (
    select award.personId,
        campusENT.*,
        row_number() over (
            partition by
                award.personId,
                campusENT.campus
            order by
                campusENT.recordActivityDate desc
        ) campusRn
    from AwardMCR award
		inner join Campus campusENT 
			on campusENT.campus = award.campus
    where campusENT.isIpedsReportable = 1
-- Remove for testing... 
		and ((campusENT.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
			and campusENT.recordActivityDate <= award.awardedDate)
				or campusENT.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
	)
where campusRn = 1
),

PersonMCR as (
--Returns most up to date student personal information as of their award date. 

select *
from (
    select personENT.*,
        row_number() over (
            partition by
                personENT.personId
            order by
                personENT.recordActivityDate desc
        ) as personRn
    from AwardMCR award
		inner join Person personENT 
			on personENT.personId = award.personId

	where personENT.isIpedsReportable = 1 --true
-- Remove for testing...
		and ((personENT.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
			and personENT.recordActivityDate <= award.awardedDate)
				or personENT.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
	)
where personRn = 1
),

AcademicTrackMCR as (
--Returns most up to date student academic track information as of their award date and term. 

select *
from (
	select award.personId personId,
		award.degree degree,
		award.awardedDate awardedDate,
		award.awardedTermCode awardedTermCode,
		acadTrackENT.termCodeEffective termCodeEffective,
		acadTrackENT.curriculumCode curriculumCode,
		award.degreeLevel degreeLevel,
		award.college awardCollege,
		acadTrackENT.college acadCollege,
		acadTrackENT.fieldOfStudyPriority FOSPriority,
		acadTrackENT.curriculumCode major,
		row_number() over (
			partition by
				acadTrackENT.personId,
				acadTrackENT.academicTrackLevel,
				acadTrackENT.degree,
				acadTrackENT.fieldOfStudyPriority
			order by
--ak 20200611 Adding term order indicator (PF-1494)
				acadterm.termOrder desc,
				acadTrackENT.recordActivityDate desc
		) as acadTrackRn
    from AwardMCR award
		inner join AcademicTrack acadTrackENT 
			on award.personId = acadTrackENT.personId
			and award.degree = acadTrackENT.degree
			and award.college = acadTrackENT.college
			and award.degreeLevel = acadTrackENT.academicTrackLevel
			and acadTrackENT.fieldOfStudyType = 'Major'
			and acadTrackENT.curriculumStatus = 'Completed'
			and acadTrackENT.isIpedsReportable = 1
-- Remove for testing...
			and ((acadTrackENT.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
				and acadTrackENT.recordActivityDate <= award.awardedDate)
				or acadTrackENT.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
--ak 20200611 Adding term order indicator (PF-1494)
		inner join AcademicTermMCR acadterm
			on acadterm.termCode = acadTrackENT.termCodeEffective
	where award.termOrder >= acadterm.termOrder
	)
where acadTrackRn = 1
),

DegreeMCR as (
-- Pulls degree information as of the date the reported student received their award.

select * 
from (
    select acadtrack.personId personId,
        degreeENT.*, 
		row_number() over (
			partition by
				acadtrack.personId,
				degreeENT.degree,
				degreeENT.awardLevel
			order by
				degreeENT.recordActivityDate desc
		) as degreeRn
    from Degree degreeENT
		inner join AcademicTrackMCR acadtrack
			on acadtrack.degree = degreeENT.degree
			and acadtrack.degreeLevel = degreeENT.degreeLevel
			and degreeENT.isIpedsReportable = 1 --true
-- Remove for testing...
			and ((degreeENT.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
				and degreeENT.recordActivityDate <= acadtrack.awardedDate)
					or degreeENT.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
    )
where degreeRn = 1
),

MajorMCR as (
-- Pulls most relevant major information as of the date the reported student received their award.

select * 
from (
    select acadtrack.personId personId,
        majorENT.*, 
        row_number() over (
			partition by
				acadtrack.personId,
				majorENT.major,
				majorENT.cipCode
			order by
				majorENT.recordActivityDate desc
		) as majorRn
    from Major majorENT
		inner join AcademicTrackMCR acadtrack
			on acadtrack.major = majorENT.major
			and majorENT.isIpedsReportable = 1 --true
-- Remove for testing...
			and ((majorENT.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
				and majorENT.recordActivityDate <= acadtrack.awardedDate)
					or majorENT.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
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
    person.personId personId,
    person.gender gender,
    person.isHispanic isHispanic,
    person.isMultipleRaces isMultipleRaces,
    person.ethnicity ethnicity,
    person.isInUSOnVisa isInUSOnVisa,
    person.isUSCitizen isUSCitizen,
    floor(DATEDIFF(CAST('2015-10-01' as DATE), CAST(person.birthDate as DATE)) / 365) asOfAge,
    award.degree,
    award.awardedDate,
    award.awardedTermCode,
    campus.isInternational,
    acadtrack.FOSPriority,
    degree.awardLevel,
    degree.isNonDegreeSeeking,
    major.major,
    major.cipCode,
    major.isDistanceEd
from AwardMCR award
	inner join PersonMCR person
		on person.personId = award.personId
	left join CampusMCR campus
		on campus.personId = award.personId
		and campus.campus = award.campus
		and campus.isInternational != 1
	inner join AcademicTrackMCR acadtrack
		on acadtrack.personId = award.personId
	inner join DegreeMCR degree
		on degree.personId = award.personId
	inner join MajorMCR major
		on major.personId = award.personId
),

CohortRefactorSTU as (
-- This view is used to refactor the 'StudentCohort' view records in terms of IPEDS reportable values

select DISTINCT 
    cohortstu.personId personId,
    case when cohortstu.gender = 'Male' then 'M'
        when cohortstu.gender = 'Female' then 'F'
        when cohortstu.gender = 'Non-Binary' then 
            (select genderForNonBinary
             from IPEDSClientConfig)
        else (select genderForUnknown
			 from IPEDSClientConfig)
    end IPEDSGender,
    case when cohortstu.isUSCitizen = 1 then 
        (case when cohortstu.isHispanic = true then 2 -- 'hispanic/latino'
              when cohortstu.isMultipleRaces = true then 8 -- 'two or more races'
              when cohortstu.ethnicity != 'Unknown' and cohortstu.ethnicity is not null
                then (case when cohortstu.ethnicity = 'Hispanic or Latino' then 2
						   when cohortstu.ethnicity = 'American Indian or Alaskan Native' then 3
                           when cohortstu.ethnicity = 'Asian' then 4
                           when cohortstu.ethnicity = 'Black or African American' then 5
                           when cohortstu.ethnicity = 'Native Hawaiian or Other Pacific Islander' then 6
                           when cohortstu.ethnicity = 'Caucasian' then 7
                    else 9 end) 
			    else 9 end) -- 'race and ethnicity unknown'
            else (case when cohortstu.isInUSOnVisa = 1 then 1 -- 'nonresident alien'
                    else 9 end) -- 'race and ethnicity unknown'
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
    case when cohortstu.awardLevel = 'Postsecondary < 1 year Award' then 1            	-- 1 - Postsecondary award, certificate, or diploma of (less than 1 academic year)
        when cohortstu.awardLevel = 'Postsecondary > 1 year < 2 years Award' then 2   	-- 2 - Postsecondary award, certificate, or diploma of (at least 1 but less than 2 academic years)
        when cohortstu.awardLevel = 'Associates Degree' then 3                        	-- 3 - Associate's degree
        when cohortstu.awardLevel = 'Postsecondary > 2 years < 4 years Award' then 4  	-- 4 - Postsecondary award, certificate, or diploma of (at least 2 but less than 4 academic years)
        when cohortstu.awardLevel = 'Bachelors Degree' then 5                        	-- 5 - Bachelor's degree
        when cohortstu.awardLevel = 'Post-baccalaureate Certificate' then 6           	-- 6 - Postbaccalaureate certificate
        when cohortstu.awardLevel = 'Masters Degree' then 7                          	-- 7 - Master's degree
        when cohortstu.awardLevel = 'Post-Masters Certificate' then 8                	-- 8 - Post-master's certificate
        when cohortstu.awardLevel = 'Doctors Degree (Research/Scholarship)' then 17		-- 17 - Doctor's degree - research/scholarship
        when cohortstu.awardLevel = 'Doctors Degree (Professional Practice)' then 18	-- 18 - Doctor's degree - professional practice
        when cohortstu.awardLevel = 'Doctors Degree (Other)' then 19					-- 19 - Doctor's degree - other
    end ACAT,
    case when cohortstu.FOSPriority = 1 then 1 else 2 end FOSPriority,
    cohortstu.isInternational isInternational,
    cohortstu.isNonDegreeSeeking isNonDegreeSeeking,
    cohortstu.major,
    CONCAT(LPAD(SUBSTR(CAST(cohortstu.cipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(cohortstu.cipCode as STRING), 3, 6), 4, '0')) cipCode,
    cohortstu.awardLevel awardLevel,
    case when cohortstu.isDistanceEd = 'Y' then 1
        else 2
    end isDistanceEd
from CohortSTU cohortstu
),

/*****
BEGIN SECTION - Remaining CIP offerings section Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

DegreeACAT as (
-- Pulls degree information as of the reporting period

select * 
from (
    select degreeENT.*, 
		row_number() over (
			partition by
				degreeENT.degree,
				degreeENT.awardLevel,
				degreeENT.curriculumRule
			order by
				degreeENT.curriculumRuleActivityDate desc,
				degreeENT.recordActivityDate desc
		) as degreeRn
    from Degree degreeENT
		cross join ReportingPeriodMCR repperiod
--ak 20200611 Adding term order indicator (PF-1494)
		inner join AcademicTermMCR acadterm
			on acadterm.termCode = degreeENT.curriculumRuleTermCodeEff
    where degreeENT.curriculumRuleActivityDate <= repperiod.censusEnd
		and degreeENT.isIpedsReportable = 1 --true
		and acadterm.termOrder <= (select max(repperiod2.termOrder) 
								   from reportingPeriodMCR repperiod2)
-- Remove for testing...
		and ((degreeENT.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
			and degreeENT.recordActivityDate <= repperiod.censusEnd)
				or degreeENT.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
    )
where degreeRn = 1
),

MajorCIPC as ( 
-- Pulls major information as of the reporting period.

select * 
from (
    select majorENT.*, 
		row_number() over (
			partition by
				majorENT.major,
				majorENT.cipCode,
				majorENT.curriculumRule
			order by
				majorENT.curriculumRuleActivityDate desc,
				majorENT.recordActivityDate desc
		) as majorRn
    from Major majorENT
		cross join ReportingPeriodMCR repperiod
--ak 20200611 Adding term order indicator (PF-1494)
		inner join AcademicTermMCR acadterm
			on acadterm.termCode = majorENT.curriculumRuleTermCodeEff
    where majorENT.curriculumRuleActivityDate <= repperiod.censusEnd
		and majorENT.isIpedsReportable = 1 --true
		and acadterm.termOrder <= (select max(repperiod2.termOrder) 
									from reportingPeriodMCR repperiod2)
-- Remove for testing...
		and ((majorENT.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
			and majorENT.recordActivityDate <= repperiod.censusEnd)
				or majorENT.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
    )
where majorRn = 1
),

DegreeMajor as (
-- Pulls all CIPCodes and ACAT levels.

select CONCAT(LPAD(SUBSTR(CAST(majorcipc.CipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(majorcipc.CipCode as STRING), 3, 6), 4, '0')) cipCode,
	degreeacat.awardLevel awardLevel,
	case when degreeacat.awardLevel = 'Postsecondary < 1 year Award' then 1				-- 1 - Postsecondary award, certificate, or diploma of (less than 1 academic year)
		when degreeacat.awardLevel = 'Postsecondary > 1 year < 2 years Award' then 2	-- 2 - Postsecondary award, certificate, or diploma of (at least 1 but less than 2 academic years)
        when degreeacat.awardLevel = 'Associates Degree' then 3                        		-- 3 - Associate's degree
        when degreeacat.awardLevel = 'Postsecondary > 2 years < 4 years Award' then 4  		-- 4 - Postsecondary award, certificate, or diploma of (at least 2 but less than 4 academic years)
        when degreeacat.awardLevel = 'Bachelors Degree' then 5                        		-- 5 - Bachelor's degree
        when degreeacat.awardLevel = 'Post-baccalaureate Certificate' then 6           		-- 6 - Postbaccalaureate certificate
        when degreeacat.awardLevel = 'Masters Degree' then 7                          		-- 7 - Master's degree
        when degreeacat.awardLevel = 'Post-Masters Certificate' then 8                		-- 8 - Post-master's certificate
        when degreeacat.awardLevel = 'Doctors Degree (Research/Scholarship)' then 17     	-- 17 - Doctor's degree - research/scholarship
        when degreeacat.awardLevel = 'Doctors Degree (Professional Practice)' then 18    	-- 18 - Doctor's degree - professional practice
        when degreeacat.awardLevel = 'Doctors Degree (Other)' then 19                    	-- 19 - Doctor's degree - other
    end ACAT,
    case when SUM(case when majorcipc.isDistanceEd = 1 then 1 else 0 end) > 0 then 1 
        else 2
    end isDistanceEd
from DegreeACAT degreeacat
    inner join MajorCIPC majorcipc on degreeacat.curriculumRule = majorcipc.curriculumRule
where majorcipc.CipCode is not null
    and degreeacat.awardLevel is not null
group by majorcipc.cipCode, degreeacat.awardLevel
),

DegreeMajorSTU as (
-- Pulls all CIPCodes and ACAT levels including student awarded levels.

select coalesce(stuCIP.MajorNum, 1) majorNum,
	degmajor.cipCode cipCode,
	degmajor.ACAT ACAT,
	coalesce(stuCIP.isDistanceEd, degmajor.isDistanceEd) isDistanceEd,
	NVL(stuCIP.FIELD1, 0) FIELD1,
	NVL(stuCIP.FIELD2, 0) FIELD2,
	NVL(stuCIP.FIELD3, 0) FIELD3,
	NVL(stuCIP.FIELD4, 0) FIELD4,
	NVL(stuCIP.FIELD5, 0) FIELD5,
	NVL(stuCIP.FIELD6, 0) FIELD6,
	NVL(stuCIP.FIELD7, 0) FIELD7,
	NVL(stuCIP.FIELD8, 0) FIELD8,
	NVL(stuCIP.FIELD9, 0) FIELD9,
	NVL(stuCIP.FIELD10, 0) FIELD10,
	NVL(stuCIP.FIELD11, 0) FIELD11,
	NVL(stuCIP.FIELD12, 0) FIELD12,
	NVL(stuCIP.FIELD13, 0) FIELD13,
	NVL(stuCIP.FIELD14, 0) FIELD14,
	NVL(stuCIP.FIELD15, 0) FIELD15,
	NVL(stuCIP.FIELD16, 0) FIELD16,
	NVL(stuCIP.FIELD17, 0) FIELD17,
	NVL(stuCIP.FIELD18, 0) FIELD18
from DegreeMajor degmajor
    left join (select refactorstu.FOSPriority MajorNum, 
					refactorstu.cipCode CIPCode,
					refactorstu.ACAT ACAT,
					case when SUM(case when refactorstu.isDistanceEd = 1 then 1 else 0 end) > 0 then 1 
						else 2
					end isDistanceEd,
					SUM(case when refactorstu.IPEDSethnicity ='1' and refactorstu.IPEDSGender = 'M' then 1 else 0 end) FIELD1, --Nonresident Alien
					SUM(case when refactorstu.IPEDSethnicity ='1' and refactorstu.IPEDSGender = 'F' then 1 else 0 end) FIELD2,
					SUM(case when refactorstu.IPEDSethnicity ='2' and refactorstu.IPEDSGender = 'M' then 1 else 0 end) FIELD3, -- Hispanic/Latino
					SUM(case when refactorstu.IPEDSethnicity ='2' and refactorstu.IPEDSGender = 'F' then 1 else 0 end) FIELD4,
					SUM(case when refactorstu.IPEDSethnicity ='3' and refactorstu.IPEDSGender = 'M' then 1 else 0 end) FIELD5, -- American Indian or Alaska Native
					SUM(case when refactorstu.IPEDSethnicity ='3' and refactorstu.IPEDSGender = 'F' then 1 else 0 end) FIELD6,
					SUM(case when refactorstu.IPEDSethnicity ='4' and refactorstu.IPEDSGender = 'M' then 1 else 0 end) FIELD7, -- Asian
					SUM(case when refactorstu.IPEDSethnicity ='4' and refactorstu.IPEDSGender = 'F' then 1 else 0 end) FIELD8,
					SUM(case when refactorstu.IPEDSethnicity ='5' and refactorstu.IPEDSGender = 'M' then 1 else 0 end) FIELD9, -- Black or African American
					SUM(case when refactorstu.IPEDSethnicity ='5' and refactorstu.IPEDSGender = 'F' then 1 else 0 end) FIELD10,
					SUM(case when refactorstu.IPEDSethnicity ='6' and refactorstu.IPEDSGender = 'M' then 1 else 0 end) FIELD11, -- Native Hawaiian or Other Pacific Islander
					SUM(case when refactorstu.IPEDSethnicity ='6' and refactorstu.IPEDSGender = 'F' then 1 else 0 end) FIELD12,
					SUM(case when refactorstu.IPEDSethnicity ='7' and refactorstu.IPEDSGender = 'M' then 1 else 0 end) FIELD13, -- White
					SUM(case when refactorstu.IPEDSethnicity ='7' and refactorstu.IPEDSGender = 'F' then 1 else 0 end) FIELD14,
					SUM(case when refactorstu.IPEDSethnicity ='8' and refactorstu.IPEDSGender = 'M' then 1 else 0 end) FIELD15, -- Two or more races
					SUM(case when refactorstu.IPEDSethnicity ='8' and refactorstu.IPEDSGender = 'F' then 1 else 0 end) FIELD16,
					SUM(case when refactorstu.IPEDSethnicity ='9' and refactorstu.IPEDSGender = 'M' then 1 else 0 end) FIELD17, -- Race and ethnicity unknown
					SUM(case when refactorstu.IPEDSethnicity ='9' and refactorstu.IPEDSGender = 'F' then 1 else 0 end) FIELD18
				from DegreeMajor degmajor
					inner join CohortRefactorSTU refactorstu on degmajor.cipCode = refactorstu.cipCode
						and degmajor.ACAT = refactorstu.ACAT
				where (refactorstu.FOSPriority = 1
					or (refactorstu.FOSPriority = 2
					and refactorstu.ACAT IN (3, 5, 7, 17, 18, 19)))
					and refactorstu.awardLevel is not null
				group by refactorstu.FOSPriority, refactorstu.cipCode, refactorstu.ACAT
			) stuCIP 
		on degmajor.cipCode = stuCIP.cipCode
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
*****/

-- Part A: Completions - CIP Data
--Report based on award. If a student obtains more than one award, they may be counted more than once.
--If a program has a traditional offering and a distance education option, completions should be reported regardless of whether or not the program was completed through distance education.

select 'A'                     part,       	--(PART)		- "A"
       NVL(majorNum, 1)        majornum,   --(MAJORNUM)	- 1 = First Major, 2 = Second Major
       NVL(cipCode, '01.0101') cipcode,    --(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
       NVL(ACAT, 1)            awlevel,    --(AWLEVEL)		- 1 to 8 and 17 to 19 for MAJORNUM=1 & 3,5,7,17,18,19 for MAJORNUM=2
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

select 'B',                      --(PART)		- "B"
       NVL(majorNum, 1),        --(MAJORNUM)	- 1 = First Major, 2 = Second Major
       NVL(cipCode, '01.0101'), --(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
       NVL(ACAT, 1),            --(AWLEVEL)		- 1 to 8 and 17 to 19 for MAJORNUM=1 & 3,5,7,17,18,19 for MAJORNUM=2
       NVL(isDistanceEd, 2),    --(DistanceED)	- 1= Yes, 2=No
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
       NVL(SUM(case when IPEDSethnicity = '1' and IPEDSGender = 'M' then 1 else 0 end),
           0), --(CRACE01) - Nonresident Alien, M, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '1' and IPEDSGender = 'F' then 1 else 0 end),
           0), --(CRACE02) - Nonresident Alien, F, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '2' and IPEDSGender = 'M' then 1 else 0 end),
           0), --(CRACE25) - Hispanic/Latino, M, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '2' and IPEDSGender = 'F' then 1 else 0 end),
           0), --(CRACE26) - Hispanic/Latino, F, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '3' and IPEDSGender = 'M' then 1 else 0 end),
           0), --(CRACE27) - American Indian or Alaska Native, M, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '3' and IPEDSGender = 'F' then 1 else 0 end),
           0), --(CRACE28) - American Indian or Alaska Native, F, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '4' and IPEDSGender = 'M' then 1 else 0 end),
           0), --(CRACE29) - Asian, M, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '4' and IPEDSGender = 'F' then 1 else 0 end),
           0), --(CRACE30) - Asian, F, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '5' and IPEDSGender = 'M' then 1 else 0 end),
           0), --(CRACE31) - Black or African American, M, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '5' and IPEDSGender = 'F' then 1 else 0 end),
           0), --(CRACE32) - Black or African American, F, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '6' and IPEDSGender = 'M' then 1 else 0 end),
           0), --(CRACE33) - Native Hawaiian or Other Pacific Islander, M, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '6' and IPEDSGender = 'F' then 1 else 0 end),
           0), --(CRACE34) - Native Hawaiian or Other Pacific Islander, F, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '7' and IPEDSGender = 'M' then 1 else 0 end),
           0), --(CRACE35) - White, M, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '7' and IPEDSGender = 'F' then 1 else 0 end),
           0), --(CRACE36) - White, F, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '8' and IPEDSGender = 'M' then 1 else 0 end),
           0), --(CRACE37) - Two or more races, M, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '8' and IPEDSGender = 'F' then 1 else 0 end),
           0), --(CRACE38) - Two or more races, M, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '9' and IPEDSGender = 'M' then 1 else 0 end),
           0), --(CRACE13) - Race and ethnicity unknown, M, (0 to 999999)
       NVL(SUM(case when IPEDSethnicity = '9' and IPEDSGender = 'F' then 1 else 0 end),
           0)  --(CRACE14) - Race and ethnicity unknown, F, (0 to 999999)
from (
         select distinct refactorstu.personId       personId,
                         refactorstu.IPEDSethnicity IPEDSethnicity,
                         refactorstu.IPEDSGender    IPEDSGender
         from CohortRefactorSTU refactorstu
     )

union

--Part D: Completers by Level
--Each student should be counted once per award level. For example, if a student earned a master's degree and a doctor's degree, he/she 
--should be counted once in master's and once in doctor's. A student earning two master's degrees should be counted only once.
select 'D',                                                              --PART      - "D"
       null,
       null,
       NVL(ACAT, 1),                                                    --CTLEVEL - 1 to 7
       null,
       NVL(SUM(case when IPEDSGender = 'M' then 1 else 0 end), 0),      --(CRACE15)	- Men, 0 to 999999
       NVL(SUM(case when IPEDSGender = 'F' then 1 else 0 end), 0),      --(CRACE16) - Women, 0 to 999999 
       NVL(SUM(case when IPEDSethnicity = '1' then 1 else 0 end), 0),   --(CRACE17) - Nonresident Alien, 0 to 999999
       NVL(SUM(case when IPEDSethnicity = '2' then 1 else 0 end), 0),   --(CRACE41) - Hispanic/Latino, 0 to 999999
       NVL(SUM(case when IPEDSethnicity = '3' then 1 else 0 end), 0),   --(CRACE42) - American Indian or Alaska Native, 0 to 999999
       NVL(SUM(case when IPEDSethnicity = '4' then 1 else 0 end), 0),   --(CRACE43) - Asian, 0 to 999999
       NVL(SUM(case when IPEDSethnicity = '5' then 1 else 0 end), 0),   --(CRACE44) - Black or African American, 0 to 999999
       NVL(SUM(case when IPEDSethnicity = '6' then 1 else 0 end), 0),   --(CRACE45) - Native Hawaiian or Other Pacific Islander, 0 to 999999
       NVL(SUM(case when IPEDSethnicity = '7' then 1 else 0 end), 0),   --(CRACE46) - White, 0 to 999999
       NVL(SUM(case when IPEDSethnicity = '8' then 1 else 0 end), 0),   --(CRACE47) - Two or more races, 0 to 999999
       NVL(SUM(case when IPEDSethnicity = '9' then 1 else 0 end), 0),   --(CRACE23) - Race and ethnicity unknown, 0 to 999999
       NVL(SUM(case when IPEDSAgeGroup = 'AGE1' then 1 else 0 end), 0), --(AGE1) - Under 18, 0 to 999999
       NVL(SUM(case when IPEDSAgeGroup = 'AGE2' then 1 else 0 end), 0), --(AGE2) - 18-24, 0 to 999999
       NVL(SUM(case when IPEDSAgeGroup = 'AGE3' then 1 else 0 end), 0), --(AGE3) - 25-39, 0 to 999999
       NVL(SUM(case when IPEDSAgeGroup = 'AGE4' then 1 else 0 end), 0), --(AGE4) - 40 and above, 0 to 999999
       NVL(SUM(case when IPEDSAgeGroup = 'AGE5' then 1 else 0 end), 0), --(AGE5) - Age unknown, 0 to 999999
       null,
       null
from 
(
    select distinct refactorstu.personId personId,
        refactorstu.IPEDSGender IPEDSGender,
        refactorstu.IPEDSEthnicity IPEDSEthnicity,
        refactorstu.IPEDSAgeGroup IPEDSAgeGroup,
        refactorstu.ACAT ACAT
    from CohortRefactorSTU refactorstu
)
group by ACAT

union

--Dummy set to return default formatting if no student awards exist.
select *
from(
    VALUES
        ('A', 1, '01.0101', 1, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('B', 1, '01.0101', 1, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        --('C', null, null, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('D', null, null, 1, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null)
    ) as dummySet(PART, MAJORNUM, CIPC_LEVL, AWLEVEL, isDistanceED, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, 
                  FIELD10, FIELD11, FIELD12, FIELD13, FIELD14, FIELD15, FIELD16, FIELD17, FIELD18)
where not exists (select a.personId from CohortSTU a) 
