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
    CAST('2018-06-01' as DATE) reportingDateStart,
    CAST('2019-05-30' as DATE) reportingDateEnd,
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

AcadTermLatestRecord as (
--Returns most recent (recordActivityDate) term code record for each term and part of term code. 
--PartOfTerm code defines a subcategory of a termCode that may have different start, end and census dates. 

select *
from ( 
	select acadTerm.*,
		row_number() over (
			partition by 
				acadTerm.termCode,
				acadTerm.partOfTermCode
			order by  
				acadTerm.recordActivityDate desc
		) acadTermRn
	from AcademicTerm acadTerm
	where acadTerm.isIPEDSReportable = 1 
	)
where acadTermRn = 1
),

ConfigPerAsOfDate as (
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
	select config.surveyCollectionYear surveyYear,
		defaultValues.surveyId surveyId,
		defaultValues.termCode termCode,
		defaultValues.partOfTermCode partOfTermCode,
		defaultValues.reportingDateStart reportingDateStart,
		defaultValues.reportingDateEnd reportingDateEnd,
		NVL(config.acadOrProgReporter, defaultValues.acadOrProgReporter) acadOrProgReporter,
		NVL(config.genderForUnknown, defaultValues.genderForUnknown) genderForUnknown,
		NVL(config.genderForNonBinary, defaultValues.genderForNonBinary) genderForNonBinary,
		NVL(config.compGradDateOrTerm, defaultValues.compGradDateOrTerm) compGradDateOrTerm,
		row_number() over (
			partition by
				config.surveyCollectionYear
			order by
				config.recordActivityDate desc
            ) configRn
	from IPEDSClientConfig config
	cross join DefaultValues defaultValues
	where config.surveyCollectionYear = defaultValues.surveyYear
		
	union

-- Defaults config information to avoid IRIS validator errors if an applicable 'IPEDSClientConfig' record is not present. 
	select defaultValues.surveyYear surveyYear,
		defaultValues.surveyId surveyId,
		defaultValues.termCode termCode,
		defaultValues.partOfTermCode partOfTermCode,
		defaultValues.reportingDateStart reportingDateStart,
		defaultValues.reportingDateEnd reportingDateEnd,
		defaultValues.acadOrProgReporter acadOrProgReporter,
		defaultValues.genderForUnknown genderForUnknown,
		defaultValues.genderForNonBinary genderForNonBinary,
		defaultValues.compGradDateOrTerm compGradDateOrTerm,
		1 configRn
	from DefaultValues defaultValues
	where defaultValues.surveyYear not in (select MAX(config.surveyCollectionYear)
											from IPEDSClientConfig config
											where config.surveyCollectionYear = defaultValues.surveyYear)
    ) ConfigLatest
where ConfigLatest.configRn = 1
),

ReportingPeriod as (
--Returns applicable term/part of term codes for this survey submission year. 

select DISTINCT
    RepDates.surveyYear surveyYear,
    RepDates.termCode termCode,	
    RepDates.partOfTermCode partOfTermCode,	
    RepDates.genderForUnknown genderForUnknown,
    RepDates.genderForNonBinary genderForNonBinary,
    case when RepDates.compGradDateOrTerm = 'D' 
		then RepDates.reportingDateEnd 
        else academicTerm.endDate 
    end censusDate,    
    RepDates.maxRepTerm maxRepTerm,
    RepDates.maxRepPartOfTerm maxRepPartOfTerm,
    case when RepDates.compGradDateOrTerm = 'D' 
        then RepDates.reportingDateEnd
        else (select MAX(term.endDate)
              from AcadTermLatestRecord term 
              where term.termCode = RepDates.maxRepTerm)
	end maxRepDate,
    case when RepDates.compGradDateOrTerm = 'D' 
        then RepDates.reportingDateStart
        else (select MIN(term.endDate)
              from AcadTermLatestRecord term 
              where term.termCode = RepDates.minRepTerm)
	end minRepDate,
    RepDates.reportingDateStart reportingDateStart,
    RepDates.reportingDateEnd reportingDateEnd, --use as census date for termCode/PartOfTermCode combo
    RepDates.compGradDateOrTerm compGradDateOrTerm
from (
    select ReportPeriod.surveyCollectionYear surveyYear,
        defaultValues.surveyId surveyId,
        NVL(ReportPeriod.termCode, defaultValues.termCode) termCode,
        NVL(ReportPeriod.partOfTermCode, defaultValues.partOfTermCode) partOfTermCode,
        NVL(ReportPeriod.reportingDateStart, defaultValues.reportingDateStart) reportingDateStart,
        NVL(ReportPeriod.reportingDateEnd, defaultValues.reportingDateEnd) reportingDateEnd,
        NVL((select MAX(IPEDSRep.termCode)
             from IPEDSReportingPeriod IPEDSRep
             where IPEDSRep.surveyCollectionYear = defaultValues.surveyYear
             and IPEDSRep.surveyId = defaultValues.surveyId), defaultValues.termCode) maxRepTerm,
        NVL((select MAX(IPEDSRep.partOfTermCode)
             from IPEDSReportingPeriod IPEDSRep
             where IPEDSRep.surveyCollectionYear = defaultValues.surveyYear
             and IPEDSRep.surveyId = defaultValues.surveyId), defaultValues.partOfTermCode) maxRepPartOfTerm,
        NVL((select MIN(IPEDSRep.termCode)
             from IPEDSReportingPeriod IPEDSRep
             where IPEDSRep.surveyCollectionYear = defaultValues.surveyYear
             and IPEDSRep.surveyId = defaultValues.surveyId), defaultValues.termCode) minRepTerm,
        NVL((select MIN(IPEDSRep.partOfTermCode)
             from IPEDSReportingPeriod IPEDSRep
             where IPEDSRep.surveyCollectionYear = defaultValues.surveyYear
             and IPEDSRep.surveyId = defaultValues.surveyId), defaultValues.partOfTermCode) minRepPartOfTerm,
        defaultValues.acadOrProgReporter acadOrProgReporter,
        defaultValues.genderForUnknown genderForUnknown,
        defaultValues.genderForNonBinary genderForNonBinary,
        defaultValues.compGradDateOrTerm compGradDateOrTerm,
        row_number() over (	
            partition by 
                ReportPeriod.surveyCollectionYear,
                ReportPeriod.surveyId,
                ReportPeriod.surveySection,
                ReportPeriod.termCode,
                ReportPeriod.partOfTermCode	
            order by 
				ReportPeriod.recordActivityDate desc
        ) reportPeriodRn	
    from IPEDSReportingPeriod ReportPeriod
    cross join ConfigPerAsOfDate defaultValues
    where ReportPeriod.surveyCollectionYear = defaultValues.surveyYear
    and ReportPeriod.surveyId = defaultValues.surveyId
	
--Added unions to use default values when IPEDSReportingPeriod record doesn't exist so valid output file will always be generated

	union
	
--Pulls default values for Fall when IPEDSReportingPeriod record doesn't exist
	select defaultValues.surveyYear surveyYear,
        defaultValues.surveyId surveyId,
        defaultValues.termCode termCode,
        defaultValues.partOfTermCode partOfTermCode,
        defaultValues.reportingDateStart reportingDateStart,
        defaultValues.reportingDateEnd reportingDateEnd,
        (select MAX(config.termCode)
         from ConfigPerAsOfDate config) maxRepTerm,  
        (select MAX(config.partOfTermCode)
         from ConfigPerAsOfDate config) maxRepPartOfTerm,  
        (select MIN(config.termCode)
         from ConfigPerAsOfDate config) minRepTerm,  
        (select MIN(config.partOfTermCode)
         from ConfigPerAsOfDate config) minRepPartOfTerm, 
        defaultValues.acadOrProgReporter acadOrProgReporter,
        defaultValues.genderForUnknown genderForUnknown,
        defaultValues.genderForNonBinary genderForNonBinary,
        defaultValues.compGradDateOrTerm compGradDateOrTerm,
        1 reportPeriodRn
	from ConfigPerAsOfDate defaultValues
	where defaultValues.surveyYear not in (select ReportPeriod.surveyCollectionYear
										   from IPEDSReportingPeriod ReportPeriod
										   where ReportPeriod.surveyCollectionYear = defaultValues.surveyYear
										   and ReportPeriod.surveyId = defaultValues.surveyId)
	)  RepDates
inner join AcadTermLatestRecord AcademicTerm 
on AcademicTerm.termCode = RepDates.termCode
and AcademicTerm.partOfTermCode = RepDates.partOfTermCode
where RepDates.reportPeriodRn = 1
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

AwardsPerAsOfReporting as (
--Pulls all distinct student awards obtained withing the reporting terms/dates.

select DISTINCT *
from (
    select award.*,
        row_number() over (
            partition by                              
                award.personId,
                case when repPeriod.compGradDateOrTerm = 'D' 
                    then award.awardedDate
                    else award.awardedTermCode
                end,
                award.degreeLevel,
                award.degree
            order by
                award.recordActivityDate desc
        ) as AwardRn
	from Award award 
	cross join ReportingPeriod repPeriod
	where award.isIpedsReportable = 1
-- Remove for testing...
	and ((award.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
		and award.recordActivityDate <= (select max(repEnd.reportingDateEnd)
										  from ReportingPeriod repEnd))
			or award.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
	and award.awardStatus = 'Awarded'
	and award.degreeLevel is not null
	and award.degreeLevel != 'Continuing Ed'
	and (Award.awardedDate >= repPeriod.minRepDate
		and Award.awardedDate <= repPeriod.maxRepDate)
    )
where AwardRn = 1
),

CampusPerAsOfReporting as ( 
-- Returns most recent campus record for each campus available per the ReportingPeriod.

select *
from (
    select campus.*,
        row_number() over (
            partition by
                campus.campus
            order by
                campus.recordActivityDate desc
        ) campusRn
    from Campus campus 
    cross join ReportingPeriod repPeriod		
    where campus.isIpedsReportable = 1
-- Remove for testing... 
    and ((campus.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
        and campus.recordActivityDate <= repPeriod.maxRepDate)
		    or campus.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
	)
where campusRn = 1
),

PersonPerAsOfReporting as (
--Returns most up to date student personal information as of their award date. 

select *
from (
    select person.*,
        row_number() over (
            partition by
                person.personId
            order by
                person.recordActivityDate desc
        ) as personRn
    from AwardsPerAsOfReporting awrd
    inner join Person person 
        on person.personId = awrd.personId
-- Remove for testing...
    where ((person.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
        and person.recordActivityDate <= awrd.awardedDate)
        or person.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
		and person.isIpedsReportable = 1 --true
	)
where personRn = 1
),

AcademicTrackPerAsOfReporting as (
--Returns most up to date student academic track information as of their award date and term. 

select *
from (
	select award.personId personId,
    award.degree degree,
    award.awardedDate awardedDate,
    award.awardedTermCode awardedTermCode,
    acadTrack.termCodeEffective termCodeEffective,
    acadTrack.curriculumCode curriculumCode,
    award.degreeLevel degreeLevel,
    award.college awardCollege,
    acadTrack.college acadCollege,
    acadTrack.fieldOfStudyPriority FOSPriority,
    acadTrack.curriculumCode major,
    row_number() over (
        partition by
            acadTrack.personId,
            acadTrack.academicTrackLevel,
            acadTrack.degree,
            acadTrack.fieldOfStudyPriority
        order by
            acadTrack.termCodeEffective desc,
            acadTrack.recordActivityDate desc
    ) as acadTrackRn
    from AwardsPerAsOfReporting award
    inner join AcademicTrack acadTrack 
        on award.personId = acadTrack.personId
        and award.degree = acadTrack.degree
        and award.college = acadTrack.college
-- Remove for testing...
        and ((award.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
            and award.recordActivityDate <= award.awardedDate)
            or award.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
        and award.awardedTermCode >= acadTrack.termCodeEffective
        and award.degreeLevel = acadTrack.academicTrackLevel
        and acadTrack.fieldOfStudyType = 'Major'
        and acadTrack.curriculumStatus = 'Completed'
        and acadTrack.isIpedsReportable = 1
	)
where acadTrackRn = 1
),

DegreePerStudent as (
-- Pulls degree information as of the date the reported student received their award.

select * 
from (
    select acadTrackStu.personId personId,
        degree.*, 
		row_number() over (
			partition by
				acadTrackStu.personId,
				degree.degree,
				degree.awardLevel
			order by
				degree.recordActivityDate desc
		) as degreeRn
    from Degree degree
    inner join AcademicTrackPerAsOfReporting acadTrackStu
        on acadTrackStu.degree = degree.degree
        and acadTrackStu.degreeLevel = degree.degreeLevel
-- Remove for testing...
        and ((degree.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
            and degree.recordActivityDate <= acadTrackStu.awardedDate)
            or degree.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
        and degree.isIpedsReportable = 1 --true
    )
where degreeRn = 1
),

MajorPerStudent as (
-- Pulls most relevant major information as of the date the reported student received their award.

select * 
from (
    select acadTrackStu.personId personId,
        major.*, 
        row_number() over (
			partition by
				acadTrackStu.personId,
				major.major,
				major.cipCode
			order by
				major.recordActivityDate desc
		) as majorRn
    from Major major
    inner join AcademicTrackPerAsOfReporting acadTrackStu
        on acadTrackStu.major = major.major
-- Remove for testing...
        and ((major.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
            and major.recordActivityDate <= acadTrackStu.awardedDate)
            or major.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
        and major.isIpedsReportable = 1 --true
    )
where majorRn = 1
),

/*****
BEGIN SECTION - Cohort Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

StudentCohort as (
--Builds out the reporting population with relevant reporting fields.

select DISTINCT 
    pers.personId personId,
    pers.gender gender,
    pers.isHispanic isHispanic,
    pers.isMultipleRaces isMultipleRaces,
    pers.ethnicity ethnicity,
    pers.isInUSOnVisa isInUSOnVisa,
    pers.isUSCitizen isUSCitizen,
    floor(DATEDIFF(CAST('2015-10-01' as DATE), CAST(pers.birthDate as DATE)) / 365) asOfAge,
    awrd.degree,
    awrd.awardedDate,
    awrd.awardedTermCode,
    camp.isInternational,
    acadTrack.FOSPriority,
    deg.awardLevel,
    deg.isNonDegreeSeeking,
    majr.major,
    majr.cipCode,
    majr.isDistanceEd
from AwardsPerAsOfReporting awrd
inner join PersonPerAsOfReporting pers
    on pers.personId = awrd.personId
inner join CampusPerAsOfReporting camp
    on camp.campus = awrd.campus
	and camp.isInternational != 1
inner join AcademicTrackPerAsOfReporting acadTrack
    on acadTrack.personId = awrd.personId
inner join DegreePerStudent deg
    on deg.personId = awrd.personId
inner join MajorPerStudent majr
    on majr.personId = awrd.personId
),

StudentCohortRefactor as (
-- This view is used to refactor the 'StudentCohort' view records in terms of IPEDS reportable values

select DISTINCT 
    cohort.personId personId,
    case when cohort.gender = 'Male' then 'M'
        when cohort.gender = 'Female' then 'F'
        when cohort.gender = 'Non-Binary' then 
            (select genderForNonBinary
            from IPEDSClientConfig)
        else (select genderForUnknown
				 from IPEDSClientConfig)
    end IPEDSGender,
    case when cohort.isUSCitizen = 1 then 
        (case when cohort.isHispanic = true then 2 -- 'hispanic/latino'
            when cohort.isMultipleRaces = true then 8 -- 'two or more races'
            when cohort.ethnicity != 'Unknown' and cohort.ethnicity is not null
                then (case when cohort.ethnicity = 'Hispanic or Latino' then 2
                        when cohort.ethnicity = 'American Indian or Alaskan Native' then 3
                        when cohort.ethnicity = 'Asian' then 4
                        when cohort.ethnicity = 'Black or African American' then 5
                        when cohort.ethnicity = 'Native Hawaiian or Other Pacific Islander' then 6
                        when cohort.ethnicity = 'Caucasian' then 7
                    else 9 end) 
			    else 9 end) -- 'race and ethnicity unknown'
            else (case when cohort.isInUSOnVisa = 1 then 1 -- 'nonresident alien'
                    else 9 end) -- 'race and ethnicity unknown'
    end IPEDSethnicity,
    case when cohort.asOfAge < 18 then 'AGE1' --Under 18
        when cohort.asOfAge >= 18 and cohort.asOfAge <= 24 then 'AGE2' --18-24
        when cohort.asOfAge >= 25 and cohort.asOfAge <= 39 then 'AGE3' --25-39
        when cohort.asOfAge >= 40 then 'AGE4' --40 and above
        else 'AGE5' --Age unknown
    end IPEDSAgeGroup,
    cohort.degree degree,
    cohort.awardedDate awardedDate,
    cohort.awardedTermCode awardedTermCode,
    case when cohort.awardLevel = 'Postsecondary < 1 year Award' then 1            	-- 1 - Postsecondary award, certificate, or diploma of (less than 1 academic year)
        when cohort.awardLevel = 'Postsecondary > 1 year < 2 years Award' then 2   	-- 2 - Postsecondary award, certificate, or diploma of (at least 1 but less than 2 academic years)
        when cohort.awardLevel = 'Associates Degree' then 3                        	-- 3 - Associate's degree
        when cohort.awardLevel = 'Postsecondary > 2 years < 4 years Award' then 4  	-- 4 - Postsecondary award, certificate, or diploma of (at least 2 but less than 4 academic years)
        when cohort.awardLevel = 'Bachelors Degree' then 5                        	-- 5 - Bachelor's degree
        when cohort.awardLevel = 'Post-baccalaureate Certificate' then 6           	-- 6 - Postbaccalaureate certificate
        when cohort.awardLevel = 'Masters Degree' then 7                          	-- 7 - Master's degree
        when cohort.awardLevel = 'Post-Masters Certificate' then 8                	-- 8 - Post-master's certificate
        when cohort.awardLevel = 'Doctors Degree (Research/Scholarship)' then 17	-- 17 - Doctor's degree - research/scholarship
        when cohort.awardLevel = 'Doctors Degree (Professional Practice)' then 18	-- 18 - Doctor's degree - professional practice
        when cohort.awardLevel = 'Doctors Degree (Other)' then 19					-- 19 - Doctor's degree - other
    end ACAT,
    case when cohort.FOSPriority = 1 then 1 else 2 end FOSPriority,
    cohort.isInternational isInternational,
    cohort.isNonDegreeSeeking isNonDegreeSeeking,
    cohort.major,
    CONCAT(LPAD(SUBSTR(CAST(cohort.cipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(cohort.cipCode as STRING), 3, 6), 4, '0')) cipCode,
    cohort.awardLevel awardLevel,
    case when cohort.isDistanceEd = 'Y' then 1
        else 2
    end isDistanceEd
from StudentCohort cohort
),

/*****
BEGIN SECTION - Remaining CIP offerings section Creation
This set of views is used to pull current cohort information based on the most current record views above.
*****/

DegreePerCIPC as (
-- Pulls degree information as of the reporting period

select * 
from (
    select degree.*, 
		row_number() over (
			partition by
				degree.degree,
				degree.awardLevel,
				degree.curriculumRule
			order by
				degree.curriculumRuleActivityDate desc,
				degree.recordActivityDate desc
		) as degreeRn
    from Degree degree
    cross join ReportingPeriod repPeriod
-- Remove for testing...
    where ((degree.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
        and degree.recordActivityDate <= repPeriod.maxRepDate)
            or degree.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
    and degree.curriculumRuleActivityDate <= repPeriod.maxRepDate
    and degree.isIpedsReportable = 1 --true
    )
where degreeRn = 1
),

MajorPerCIPC as ( 
-- Pulls major information as of the reporting period.

select * 
from (
    select major.*, 
        row_number() over (
                partition by
                    major.major,
                    major.cipCode,
                    major.curriculumRule
                order by
                    major.curriculumRuleActivityDate desc,
                    major.recordActivityDate desc
            ) as majorRn
    from Major major
    cross join ReportingPeriod repPeriod
-- Remove for testing...
    where ((major.recordActivityDate != CAST('9999-09-09' as TIMESTAMP)
        and major.recordActivityDate <= repPeriod.maxRepDate)
            or major.recordActivityDate = CAST('9999-09-09' as TIMESTAMP))
-- ...Remove for testing
    and major.curriculumRuleActivityDate <= repPeriod.maxRepDate
    and major.isIpedsReportable = 1 --true
    )
where majorRn = 1
),

FullCIPList as (
-- Pulls all CIPCodes and ACAT levels.

select CONCAT(LPAD(SUBSTR(CAST(majorCIPC.CipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(majorCIPC.CipCode as STRING), 3, 6), 4, '0')) cipCode,
        degCIPC.awardLevel awardLevel,
        case when degCIPC.awardLevel = 'Postsecondary < 1 year Award' then 1			-- 1 - Postsecondary award, certificate, or diploma of (less than 1 academic year)
            when degCIPC.awardLevel = 'Postsecondary > 1 year < 2 years Award' then 2	-- 2 - Postsecondary award, certificate, or diploma of (at least 1 but less than 2 academic years)
        when degCIPC.awardLevel = 'Associates Degree' then 3                        	-- 3 - Associate's degree
        when degCIPC.awardLevel = 'Postsecondary > 2 years < 4 years Award' then 4  	-- 4 - Postsecondary award, certificate, or diploma of (at least 2 but less than 4 academic years)
        when degCIPC.awardLevel = 'Bachelors Degree' then 5                        		-- 5 - Bachelor's degree
        when degCIPC.awardLevel = 'Post-baccalaureate Certificate' then 6           	-- 6 - Postbaccalaureate certificate
        when degCIPC.awardLevel = 'Masters Degree' then 7                          		-- 7 - Master's degree
        when degCIPC.awardLevel = 'Post-Masters Certificate' then 8                		-- 8 - Post-master's certificate
        when degCIPC.awardLevel = 'Doctors Degree (Research/Scholarship)' then 17     	-- 17 - Doctor's degree - research/scholarship
        when degCIPC.awardLevel = 'Doctors Degree (Professional Practice)' then 18    	-- 18 - Doctor's degree - professional practice
        when degCIPC.awardLevel = 'Doctors Degree (Other)' then 19                    	-- 19 - Doctor's degree - other
    end ACAT,
    case when SUM(case when majorCIPC.isDistanceEd = 1 then 1 else 0 end) > 0 then 1 
        else 2
    end isDistanceEd
from DegreePerCIPC degCIPC
    inner join MajorPerCIPC majorCIPC on degCIPC.curriculumRule = majorCIPC.curriculumRule
where majorCIPC.CipCode is not null
    and degCIPC.awardLevel is not null
group by majorCIPC.cipCode, degCIPC.awardLevel
),

FullStudCIPList as (
-- Pulls all CIPCodes and ACAT levels including student awarded levels.

select coalesce(stuCIP.MajorNum, 1) majorNum,
        fullCIP.cipCode cipCode,
        fullCIP.ACAT ACAT,
        coalesce(stuCIP.isDistanceEd, fullCIP.isDistanceEd) isDistanceEd,
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
from FullCIPList fullCIP
    left join (select cohRef.FOSPriority MajorNum, 
	    cohRef.cipCode CIPCode,
        cohRef.ACAT ACAT,
        case when SUM(case when cohRef.isDistanceEd = 1 then 1 else 0 end) > 0 then 1 
            else 2
        end isDistanceEd,
		SUM(case when cohRef.IPEDSethnicity ='1' and cohRef.IPEDSGender = 'M' then 1 else 0 end) FIELD1, --Nonresident Alien
        SUM(case when cohRef.IPEDSethnicity ='1' and cohRef.IPEDSGender = 'F' then 1 else 0 end) FIELD2,
        SUM(case when cohRef.IPEDSethnicity ='2' and cohRef.IPEDSGender = 'M' then 1 else 0 end) FIELD3, -- Hispanic/Latino
        SUM(case when cohRef.IPEDSethnicity ='2' and cohRef.IPEDSGender = 'F' then 1 else 0 end) FIELD4,
        SUM(case when cohRef.IPEDSethnicity ='3' and cohRef.IPEDSGender = 'M' then 1 else 0 end) FIELD5, -- American Indian or Alaska Native
        SUM(case when cohRef.IPEDSethnicity ='3' and cohRef.IPEDSGender = 'F' then 1 else 0 end) FIELD6,
        SUM(case when cohRef.IPEDSethnicity ='4' and cohRef.IPEDSGender = 'M' then 1 else 0 end) FIELD7, -- Asian
        SUM(case when cohRef.IPEDSethnicity ='4' and cohRef.IPEDSGender = 'F' then 1 else 0 end) FIELD8,
        SUM(case when cohRef.IPEDSethnicity ='5' and cohRef.IPEDSGender = 'M' then 1 else 0 end) FIELD9, -- Black or African American
        SUM(case when cohRef.IPEDSethnicity ='5' and cohRef.IPEDSGender = 'F' then 1 else 0 end) FIELD10,
        SUM(case when cohRef.IPEDSethnicity ='6' and cohRef.IPEDSGender = 'M' then 1 else 0 end) FIELD11, -- Native Hawaiian or Other Pacific Islander
        SUM(case when cohRef.IPEDSethnicity ='6' and cohRef.IPEDSGender = 'F' then 1 else 0 end) FIELD12,
        SUM(case when cohRef.IPEDSethnicity ='7' and cohRef.IPEDSGender = 'M' then 1 else 0 end) FIELD13, -- White
        SUM(case when cohRef.IPEDSethnicity ='7' and cohRef.IPEDSGender = 'F' then 1 else 0 end) FIELD14,
        SUM(case when cohRef.IPEDSethnicity ='8' and cohRef.IPEDSGender = 'M' then 1 else 0 end) FIELD15, -- Two or more races
        SUM(case when cohRef.IPEDSethnicity ='8' and cohRef.IPEDSGender = 'F' then 1 else 0 end) FIELD16,
        SUM(case when cohRef.IPEDSethnicity ='9' and cohRef.IPEDSGender = 'M' then 1 else 0 end) FIELD17, -- Race and ethnicity unknown
        SUM(case when cohRef.IPEDSethnicity ='9' and cohRef.IPEDSGender = 'F' then 1 else 0 end) FIELD18
	from FullCIPList fullList
	    inner join StudentCohortRefactor cohRef on fullList.cipCode = cohRef.cipCode
	        and fullList.ACAT = cohRef.ACAT
    where (cohRef.FOSPriority = 1
            or (cohRef.FOSPriority = 2
            and cohRef.ACAT IN (3, 5, 7, 17, 18, 19)))
    and cohRef.awardLevel is not null
	group by cohRef.FOSPriority, cohRef.cipCode, cohRef.ACAT
	) stuCIP on fullCIP.cipCode = stuCIP.cipCode
	    and fullCIP.ACAT = stuCIP.ACAT
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

select 'A' PART, 								--(PART)		- "A"
        NVL(majorNum,1) MAJORNUM,				--(MAJORNUM)	- 1 = First Major, 2 = Second Major
        NVL(cipCode, '00.0000') CIPCODE,		--(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
        NVL(ACAT, 1) AWLEVEL,					--(AWLEVEL)		- 1 to 8 and 17 to 19 for MAJORNUM=1 & 3,5,7,17,18,19 for MAJORNUM=2
        null DistanceED,						--(DistanceED)  ***NOT USED IN PART A***
        FIELD1 FIELD1,							--(CRACE01) 	- Nonresident Alien, M, (0 to 999999)
        FIELD2 FIELD2,							--(CRACE02) 	- Nonresident Alien, F, (0 to 999999)
        FIELD3 FIELD3,							--(CRACE25) 	- Hispanic/Latino, M, (0 to 999999)					
        FIELD4 FIELD4,							--(CRACE26) 	- Hispanic/Latino, F, (0 to 999999) 
        FIELD5 FIELD5,							--(CRACE27) 	- American Indian or Alaska Native, M, (0 to 999999)
        FIELD6 FIELD6,							--(CRACE28) 	- American Indian or Alaska Native, F, (0 to 999999)
        FIELD7 FIELD7,							--(CRACE29) 	- Asian, M, (0 to 999999)
        FIELD8 FIELD8,							--(CRACE30) 	- Asian, F, (0 to 999999)
        FIELD9 FIELD9,							--(CRACE31) 	- Black or African American, M, (0 to 999999)
        FIELD10 FIELD10,						--(CRACE32) 	- Black or African American, F, (0 to 999999)
        FIELD11 FIELD11,						--(CRACE33) 	- Native Hawaiian or Other Pacific Islander, M, (0 to 999999)
        FIELD12 FIELD12,						--(CRACE34) 	- Native Hawaiian or Other Pacific Islander, F, (0 to 999999)
        FIELD13 FIELD13,						--(CRACE35) 	- White, M, (0 to 999999)
        FIELD14 FIELD14,						--(CRACE36) 	- White, F, (0 to 999999)
        FIELD15 FIELD15,						--(CRACE37) 	- Two or more races, M, (0 to 999999)
        FIELD16 FIELD16,						--(CRACE38) 	- Two or more races, F, (0 to 999999)
        FIELD17 FIELD17,						--(CRACE13) 	- Race and ethnicity unknown, M, (0 to 999999)		
        FIELD18 FIELD18							--(CRACE14) 	- Race and ethnicity unknown, F, (0 to 999999)
from FullStudCIPList
	
union 

--Part B: Completions - Distance Education
--Used to distinguish if an award is offered in a distance education format. A "distance education program" is "a program for which all the required coursework 
--for program completion is able to be completed via distance education courses."

select 'B' PART,								--(PART)		- "B"
    NVL(majorNum, 1),							--(MAJORNUM)	- 1 = First Major, 2 = Second Major
    NVL(cipCode, '00.0000'),					--(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
    NVL(ACAT, 1),								--(AWLEVEL)		- 1 to 8 and 17 to 19 for MAJORNUM=1 & 3,5,7,17,18,19 for MAJORNUM=2
    NVL(isDistanceEd, 2),						--(DistanceED)	- 1= Yes, 2=No
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
from FullStudCIPList

union

--Part C: All Completers
--Count each student only once, regardless of how many awards he/she earned. The intent of this screen is to collect an unduplicated count 
--of total numbers of completers.

select 'C',																				 --PART 	 - "C"
    null,
    null,
    null,
    null,
    NVL(SUM(case when IPEDSethnicity ='1' and IPEDSGender = 'M' then 1 else 0 end),0), --(CRACE01) - Nonresident Alien, M, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='1' and IPEDSGender = 'F' then 1 else 0 end),0), --(CRACE02) - Nonresident Alien, F, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='2' and IPEDSGender = 'M' then 1 else 0 end),0), --(CRACE25) - Hispanic/Latino, M, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='2' and IPEDSGender = 'F' then 1 else 0 end),0), --(CRACE26) - Hispanic/Latino, F, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='3' and IPEDSGender = 'M' then 1 else 0 end),0), --(CRACE27) - American Indian or Alaska Native, M, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='3' and IPEDSGender = 'F' then 1 else 0 end),0), --(CRACE28) - American Indian or Alaska Native, F, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='4' and IPEDSGender = 'M' then 1 else 0 end),0), --(CRACE29) - Asian, M, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='4' and IPEDSGender = 'F' then 1 else 0 end),0), --(CRACE30) - Asian, F, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='5' and IPEDSGender = 'M' then 1 else 0 end),0), --(CRACE31) - Black or African American, M, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='5' and IPEDSGender = 'F' then 1 else 0 end),0), --(CRACE32) - Black or African American, F, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='6' and IPEDSGender = 'M' then 1 else 0 end),0), --(CRACE33) - Native Hawaiian or Other Pacific Islander, M, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='6' and IPEDSGender = 'F' then 1 else 0 end),0), --(CRACE34) - Native Hawaiian or Other Pacific Islander, F, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='7' and IPEDSGender = 'M' then 1 else 0 end),0), --(CRACE35) - White, M, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='7' and IPEDSGender = 'F' then 1 else 0 end),0), --(CRACE36) - White, F, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='8' and IPEDSGender = 'M' then 1 else 0 end),0), --(CRACE37) - Two or more races, M, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='8' and IPEDSGender = 'F' then 1 else 0 end),0), --(CRACE38) - Two or more races, M, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='9' and IPEDSGender = 'M' then 1 else 0 end),0), --(CRACE13) - Race and ethnicity unknown, M, (0 to 999999)
    NVL(SUM(case when IPEDSethnicity ='9' and IPEDSGender = 'F' then 1 else 0 end),0)  --(CRACE14) - Race and ethnicity unknown, F, (0 to 999999)
from (
    select distinct cohRef.personId personId,
        cohRef.IPEDSethnicity IPEDSethnicity,
        cohRef.IPEDSGender IPEDSGender
    from studentCohortRefactor cohRef	 
    )
    
union

--Part D: Completers by Level
--Each student should be counted once per award level. For example, if a student earned a master's degree and a doctor's degree, he/she 
--should be counted once in master's and once in doctor's. A student earning two master's degrees should be counted only once.
select 'D',																--"D"
    null,
    null,
    NVL(ACAT,1),														--CTLEVEL - 1 to 7
    null,
    NVL(SUM(case when IPEDSGender = 'M' then 1 else 0 end),0), 		--(CRACE15)	- Men, 0 to 999999
    NVL(SUM(case when IPEDSGender = 'F' then 1 else 0 end),0), 		--(CRACE16) - Women, 0 to 999999 
    NVL(SUM(case when IPEDSethnicity ='1' then 1 else 0 end),0), 	--(CRACE17) - Nonresident Alien, 0 to 999999
    NVL(SUM(case when IPEDSethnicity ='2' then 1 else 0 end),0), 	--(CRACE41) - Hispanic/Latino, 0 to 999999
    NVL(SUM(case when IPEDSethnicity ='3' then 1 else 0 end),0), 	--(CRACE42) - American Indian or Alaska Native, 0 to 999999
    NVL(SUM(case when IPEDSethnicity ='4' then 1 else 0 end),0), 	--(CRACE43) - Asian, 0 to 999999
    NVL(SUM(case when IPEDSethnicity ='5' then 1 else 0 end),0), 	--(CRACE44) - Black or African American, 0 to 999999
    NVL(SUM(case when IPEDSethnicity ='6' then 1 else 0 end),0), 	--(CRACE45) - Native Hawaiian or Other Pacific Islander, 0 to 999999
    NVL(SUM(case when IPEDSethnicity ='7' then 1 else 0 end),0), 	--(CRACE46) - White, 0 to 999999
    NVL(SUM(case when IPEDSethnicity ='8' then 1 else 0 end),0), 	--(CRACE47) - Two or more races, 0 to 999999
    NVL(SUM(case when IPEDSethnicity ='9' then 1 else 0 end),0), 	--(CRACE23) - Race and ethnicity unknown, 0 to 999999
    NVL(SUM(case when IPEDSAgeGroup ='AGE1' then 1 else 0 end),0), 	--(AGE1) - Under 18, 0 to 999999
    NVL(SUM(case when IPEDSAgeGroup ='AGE2' then 1 else 0 end),0), 	--(AGE2) - 18-24, 0 to 999999
    NVL(SUM(case when IPEDSAgeGroup ='AGE3' then 1 else 0 end),0), 	--(AGE3) - 25-39, 0 to 999999
    NVL(SUM(case when IPEDSAgeGroup ='AGE4' then 1 else 0 end),0), 	--(AGE4) - 40 and above, 0 to 999999
    NVL(SUM(case when IPEDSAgeGroup ='AGE5' then 1 else 0 end),0), 	--(AGE5) - Age unknown, 0 to 999999
    null,
    null
from 
(
    select distinct cohRef.personId personId,
        cohRef.IPEDSGender IPEDSGender,
        cohRef.IPEDSEthnicity IPEDSEthnicity,
        cohRef.IPEDSAgeGroup IPEDSAgeGroup,
        cohRef.ACAT ACAT
    from StudentCohortRefactor cohRef
)
group by ACAT

union

--Dummy set to return default formatting if no student awards exist.
select *
from(
    VALUES
        ('A', 1, '00.0000', 1, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('B', 1, '00.0000', 1, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        --('C', null, null, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('D', null, null, 1, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null)
    ) as dummySet(PART, MAJORNUM, CIPC_LEVL, AWLEVEL, isDistanceED, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, 
                  FIELD10, FIELD11, FIELD12, FIELD13, FIELD14, FIELD15, FIELD16, FIELD17, FIELD18)
where not exists (select a.personId from StudentCohort a) 
