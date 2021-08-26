/********************

EVI PRODUCT:    DORIS 2021-22 IPEDS Survey Fall Collection
FILE NAME:      Completions
FILE DESC:      Completions for all institutions
AUTHOR:         Ahmed Khasawneh
CREATED:        20210914

SECTIONS:
Reporting Dates/Terms
Most Recent Records
Cohort Creation
Award Level Offerings 
Survey Formatting

SUMMARY OF CHANGES
Date(yyyymmdd)      	Author             	Tag             	Comments
-----------------   --------------------	-------------   	-------------------------------------------------

20211209            akhasawneh          			    		Initial version (Run time 14m)

SNAPSHOT REQUIREMENTS
One of the following is required:

Academic Year End - end date of last term used for reporting; this option is best for clients with
					IPEDSClientConfig.compGradDateOrTerm = 'T'
June End - survey official end of reporting - June 30; this option is best for clients with
					IPEDSClientConfig.compGradDateOrTerm = 'D'

IMPLEMENTATION NOTES
The following changes were implemented for the 2021-22 data collection period:

	- Subbaccalaureate certificates that are less than one year in length have been segmented into two subcategories based on duration.
	- There is a new FAQ elaborating on certificates that should be included in this component.                   

********************/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.
--IPEDS specs define reporting period as July 1, 2020 and June 30, 2021.

--prod default block
select '2021' surveyYear,
    'COM' surveyId,
    'Academic Year End' repPeriodTag1,
    'June End' repPeriodTag2,
    CAST('2020-07-01' AS DATE) reportingDateStart,
    CAST('2021-06-30' AS DATE) reportingDateEnd,
    '202110' termCode, --Fall 2020
    '1' partOfTermCode,
    'M' genderForUnknown, --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
    'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'T' compGradDateOrTerm --D = Date, T = Term
/* 
--test default block
select '1415' surveyYear,  
	'COM' surveyId,  
	'Academic Year End' repPeriodTag1,
	'June End' repPeriodTag2,
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201410' termCode,
	'1' partOfTermCode,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
	'T' compGradDateOrTerm --D = Date, T = Term
*/
),

ClientConfigMCR as (
-- Pulls client reporting preferences from the IPEDSClientConfig entity. 

--  1st union 1st order - pull snapshot where same as ReportingPeriodMCR snapshotDate
--  1st union 2nd order - pull closet snapshot before ReportingPeriodMCR snapshotDate
--  1st union 3rd order - pull closet snapshot after ReportingPeriodMCR snapshotDate
--  2nd union - pull default values if no record in IPEDSClientConfig

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.surveyId surveyId,
    ConfigLatest.source source,
    ConfigLatest.snapshotDate snapshotDate,
    ConfigLatest.reportingDateStart reportingDateStart,
    ConfigLatest.reportingDateEnd reportingDateEnd,
    ConfigLatest.genderForUnknown genderForUnknown,
    ConfigLatest.genderForNonBinary genderForNonBinary,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
    ConfigLatest.repPeriodTag2 repPeriodTag2,
    ConfigLatest.compGradDateOrTerm compGradDateOrTerm
from (
     select clientConfigENT.surveyCollectionYear surveyYear,
            defvalues.surveyId surveyId,
            'configFullYearTag' source,
            clientConfigENT.snapshotDate snapshotDate,
            defvalues.reportingDateStart reportingDateStart,
            defvalues.reportingDateEnd reportingDateEnd,
            coalesce(upper(clientConfigENT.genderForUnknown), defvalues.genderForUnknown) genderForUnknown,
            coalesce(upper(clientConfigENT.genderForNonBinary), defvalues.genderForNonBinary) genderForNonBinary,
            defvalues.repPeriodTag1 repPeriodTag1,
            defvalues.repPeriodTag2 repPeriodTag2,
            coalesce(upper(clientConfigENT.compGradDateOrTerm), defvalues.compGradDateOrTerm) compGradDateOrTerm,
            coalesce(row_number() over (
                partition by
                    clientConfigENT.surveyCollectionYear
                order by
                (case when coalesce(upper(clientConfigENT.compGradDateOrTerm), defvalues.compGradDateOrTerm) = 'T' and array_contains(clientConfigENT.tags, defvalues.repPeriodTag1) then 1
                     when coalesce(upper(clientConfigENT.compGradDateOrTerm), defvalues.compGradDateOrTerm) = 'D' and array_contains(clientConfigENT.tags, defvalues.repPeriodTag2) then 1
                     else 2 end) asc,
                    clientConfigENT.recordActivityDate desc
            ), 1) configRn
        from IPEDSClientConfig clientConfigENT
            cross join DefaultValues defvalues on clientConfigENT.surveyCollectionYear = defvalues.surveyYear

    union

        select defvalues.surveyYear surveyYear,
            defvalues.surveyId surveyId,
            'default' source,
            CAST('9999-09-09' as DATE) snapshotDate,
            defvalues.reportingDateStart reportingDateStart,
            defvalues.reportingDateEnd reportingDateEnd,
            defvalues.genderForUnknown genderForUnknown,
            defvalues.genderForNonBinary genderForNonBinary,
            defvalues.repPeriodTag1 repPeriodTag1,
            defvalues.repPeriodTag2 repPeriodTag2,
            defvalues.compGradDateOrTerm compGradDateOrTerm,
            1 configRn
        from DefaultValues defvalues
        where defvalues.surveyYear not in (select max(configENT.surveyCollectionYear)
                                            from IPEDSClientConfig configENT
                                            where configENT.surveyCollectionYear = defvalues.surveyYear)
) ConfigLatest
    where ConfigLatest.configRn = 1
),

ReportingPeriodMCR as (
    --Returns applicable term/part of term codes for this survey submission year. 

    --  1st union 1st order - pull snapshot for defvalues.repPeriodTag1 
    --  1st union 2nd order - pull snapshot for defvalues.repPeriodTag2
    --  1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
    --  2nd union - pull default values if no record in IPEDSReportingPeriod

select distinct RepDates.surveyYear	surveyYear,
        RepDates.source source,
        RepDates.surveySection surveySection,
        RepDates.snapshotDate snapshotDate,
        RepDates.reportingDateStart reportingDateStart,
        RepDates.reportingDateEnd reportingDateEnd,
        RepDates.termCode termCode,
        RepDates.partOfTermCode partOfTermCode,
        RepDates.repPeriodTag1 repPeriodTag1,
        RepDates.repPeriodTag2 repPeriodTag2
from ( 
    select config.surveyYear surveyYear,
        'IPEDSReportingPeriod' source,
        config.snapshotDate snapshotDate,
        config.surveyId surveyId,
        coalesce(upper(repPeriodENT.surveySection), 'COHORT') surveySection,
        config.reportingDateStart reportingDateStart,
        config.reportingDateEnd reportingDateEnd,
        upper(repperiodENT.termCode) termCode,
        coalesce(upper(repperiodENT.partOfTermCode), 1) partOfTermCode,
        config.repPeriodTag1 repPeriodTag1,
        config.repPeriodTag2 repPeriodTag2,
        coalesce(row_number() over (	
        partition by 
            config.surveyYear, --repPeriodENT.surveyCollectionYear,
            config.surveyId, --repPeriodENT.surveyId,
            repPeriodENT.surveySection, 
            repperiodENT.termCode,
            repperiodENT.partOfTermCode	
        order by
            (case when repperiodENT.snapshotDate = config.snapshotDate then 1 else 2 end) asc,
            (case when repperiodENT.snapshotDate > config.snapshotDate then repperiodENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
            (case when repperiodENT.snapshotDate < config.snapshotDate then repperiodENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc, 
            repperiodENT.recordActivityDate desc	
        ),1) reportPeriodRn
    from ClientConfigMCR config
        inner join IPEDSReportingPeriod repperiodENT on repperiodENT.surveyCollectionYear = config.surveyYear
            and upper(repperiodENT.surveyId) = config.surveyId
            and repperiodENT.termCode is not null 

    union

    select defvalues.surveyYear surveyYear,
        'DefaultValues' source,
        CAST('9999-09-09' as DATE) snapshotDate,
        defvalues.surveyId surveyId,
        null surveySection,
        defvalues.reportingDateStart reportingDateStart,
        defvalues.reportingDateEnd reportingDateEnd,
        defvalues.termCode termCode,
        defvalues.partOfTermCode partOfTermCode,
        defvalues.repPeriodTag1 repPeriodTag1,
        defvalues.repPeriodTag2 repPeriodTag2,
        1
    from DefaultValues defvalues
    where defvalues.surveyYear not in (select repperiodENT.surveyCollectionYear
                                        from IPEDSReportingPeriod repperiodENT
                                         where repperiodENT.surveyCollectionYear = defvalues.surveyYear
                                            and upper(repperiodENT.surveyId) = defvalues.surveyId
                                            and repperiodENT.termCode is not null) 
    ) RepDates
where RepDates.reportPeriodRn = 1
),

AcademicTermMCR as (
--Returns most recent (recordActivityDate) term code record for all term codes and parts of term code for all snapshots. 

select termCode,
        partOfTermCode,
        financialAidYear,
        snapshotDate,
        startDate,
        endDate,
        academicYear,
        censusDate,
        termType,
        termClassification,
        requiredFTCreditHoursGR,
        requiredFTCreditHoursUG,
        requiredFTClockHoursUG,
        tags
from (
    select distinct upper(acadtermENT.termCode) termCode,
            coalesce(row_number() over (
                partition by 
                    acadTermENT.termCode,
                    acadTermENT.partOfTermCode
                order by						
                    acadTermENT.recordActivityDate desc
            ), 1) acadTermRn,
            config.snapshotDate snapshotDate,
            acadTermENT.tags,
            coalesce(upper(acadtermENT.partOfTermCode), 1) partOfTermCode,
            acadtermENT.termCodeDescription,
            acadtermENT.partOfTermCodeDescription,
            to_date(acadtermENT.startDate, 'YYYY-MM-DD') startDate,
            to_date(acadtermENT.endDate, 'YYYY-MM-DD') endDate,
            acadtermENT.academicYear,
            acadtermENT.financialAidYear,
            to_date(acadtermENT.censusDate, 'YYYY-MM-DD') censusDate,
            acadtermENT.termType,
            acadtermENT.termClassification,
            coalesce(acadtermENT.requiredFTCreditHoursGR, 9) requiredFTCreditHoursGR, 
            coalesce(acadtermENT.requiredFTCreditHoursUG, 12) requiredFTCreditHoursUG,
            coalesce(acadtermENT.requiredFTClockHoursUG, 24) requiredFTClockHoursUG,
            coalesce(acadtermENT.isIPEDSReportable, true) isIPEDSReportable
        from ClientConfigMCR config
            inner join AcademicTerm acadtermENT on config.snapshotDate = acadTermENT.snapshotDate
                and coalesce(acadtermENT.isIPEDSReportable, true) = true 
    )
where acadTermRn = 1
),

AcademicTermOrder as (
-- Orders term codes based on date span and keeps the numeric value of the greatest term/part of term record. 

select termCode termCode,
        max(termOrder) termOrder,
        max(censusDate) maxCensus,
        min(startDate) minStart,
        max(endDate) maxEnd,
        termType termType
from (
    select acadterm.termCode termCode,
            acadterm.partOfTermCode partOfTermCode,
            acadterm.termType termType,
            acadterm.censusDate censusDate,
            acadterm.startDate startDate,
            acadterm.endDate endDate,
            coalesce(row_number() over (
                order by  
                acadterm.censusDate asc
            ), 1) termOrder
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

select 'CY' yearType,
        repperiod.surveySection surveySection,
        repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        acadterm.tags tags,
        acadterm.snapshotDate snapshotDate,
        repperiod.snapshotDate repPeriodSSDate,
        acadterm.financialAidYear financialAidYear,
        acadterm.censusDate censusDate,
        repperiod.reportingDateStart reportingDateStart,
        repperiod.reportingDateEnd reportingDateEnd,
        acadterm.termClassification termClassification,
        acadterm.termType termType,
        acadterm.startDate startDate,
        acadterm.endDate endDate,
        termorder.termOrder termOrder,
        termorder.minStart minStart,
        termorder.maxEnd maxEnd,
        termorder.maxCensus maxCensus,
        (case when acadterm.termClassification = 'Standard Length' then 1
            when acadterm.termClassification is null then (case when acadterm.termType in ('Fall', 'Spring') then 1 else 2 end)
            else 2
        end) fullTermOrder,
        acadterm.requiredFTCreditHoursGR requiredFTCreditHoursGR,
        acadterm.requiredFTCreditHoursUG requiredFTCreditHoursUG,
        acadterm.requiredFTClockHoursUG requiredFTClockHoursUG,
        clientconfig.genderForUnknown genderForUnknown,
        clientconfig.genderForNonBinary genderForNonBinary,
        coalesce(acadterm.requiredFTCreditHoursUG/
        coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
        clientconfig.repPeriodTag1 repPeriodTag1,
        clientconfig.repPeriodTag2 repPeriodTag2,
        clientconfig.compGradDateOrTerm compGradDateOrTerm,
        (case when clientconfig.compGradDateOrTerm = 'D' then repperiod.reportingDateStart
          else termorder.minStart
          end) compReportingDateStart, --minimum start date of reporting for either date or term option for client
        (case when clientconfig.compGradDateOrTerm = 'D' then repperiod.reportingDateEnd
          else termorder.maxEnd --termMaxEndDate
          end) compReportingDateEnd --maximum end date of reporting for either date or term option for client
from ReportingPeriodMCR repperiod
    left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
        and repperiod.partOfTermCode = acadterm.partOfTermCode
    left join AcademicTermOrder termorder on termOrder.termCode = repperiod.termCode
    inner join ClientConfigMCR clientconfig on repperiod.surveyYear = clientconfig.surveyYear
),

AcademicTermReportingRefactor as (
--Returns all records from AcademicTermReporting, converts Summer terms to Pre-Fall or Post-Spring and creates reportingDateStart/End

select rep.*,
        (case when rep.termType = 'Summer' and rep.termClassification != 'Standard Length' then 
                case when (select max(rep2.termOrder)
                            from AcademicTermReporting rep2
                            where rep2.termType = 'Summer') < (select max(rep2.termOrder)
                                                                from AcademicTermReporting rep2
                                                                where rep2.termType = 'Fall') then 'Pre-Fall Summer'
                else 'Post-Spring Summer' end
            else rep.termType end) termTypeNew,
        potMax.partOfTermCode maxPOT,
        (select max(rep3.termOrder)
        from AcademicTermReporting rep3) maxTermOrder
from AcademicTermReporting rep
        inner join (select rep3.termCode,
            rep3.partOfTermCode,
            coalesce(row_number() over (
                        partition by
                            rep3.termCode
                        order by
                            rep3.censusDate desc,
                            rep3.endDate desc), 1
                    ) potRn
    from AcademicTermReporting rep3) potMax on rep.termCode = potMax.termCode
            and potMax.potRn = 1
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

--jh 20211130 Reordered fields for consistency and accuracy; other minor mods

CampusMCR as (
-- Returns most recent campus record for all snapshots in the ReportingPeriod
-- We only use campus for international status. We are maintaining the ability to look at a campus at different points in time through relevant snapshots. 
-- Will there ever be a case where a campus changes international status? 
-- Otherwise, we could just get all unique campus codes and forget about when the record was made.

select campus,
        isInternational
from ( 
    select upper(campusENT.campus) campus,
            campusENT.campusDescription,
            coalesce(campusENT.isInternational, false) isInternational,
            coalesce(row_number() over (
                partition by
                    campusENT.campus
                order by
                    campusENT.recordActivityDate desc
            ), 1) campusRn
        from Campus campusENT
            inner join AcademicTermReportingRefactor acadterm on acadterm.snapshotDate = campusENT.snapshotDate
        where coalesce(campusENT.isIpedsReportable, true) = true
            and ((coalesce(to_date(campusENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' as DATE)
                and to_date(campusENT.recordActivityDate,'YYYY-MM-DD') <= acadterm.snapshotDate)
            or coalesce(to_date(campusENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' as DATE))
    )
where campusRn = 1
),

DegreeProgramMCR as (

select *
    from (
    select distinct upper(programENT.degreeProgram) degreeProgram,
            upper(programENT.degree) degree,
            upper(programENT.major) major,
            upper(programENT.college) college,
            upper(programENT.campus) campus,
            coalesce(campus.isInternational, false) isInternationalDegProg,
            upper(programENT.department) department,
            coalesce(programENT.isESL, false) isESL,
            programENT.startDate startDate,
            upper(programENT.termCodeEffective) termCodeEffective,
            termorder.termOrder termorder,
            coalesce(programENT.distanceEducationType, 'Not distance education') distanceEducationType,
            repperiod.snapshotDate snapshotDate,
            repperiod.compReportingDateEnd compReportingDateEnd,
            coalesce(row_number() over (
                partition by
                    programENT.degreeProgram,
                    --programENT.degree,
                    programENT.major,
                    programENT.distanceEducationType,
                    programENT.campus
                order by
                    termorder.termOrder desc,
                    programENT.startDate desc,
                    programENT.recordActivityDate desc
            ), 1) programRN
        from AcademicTermReportingRefactor repperiod
            inner join DegreeProgram programENT on repperiod.snapshotDate = programENT.snapshotDate
                and (to_date(programENT.startDate,'YYYY-MM-DD') <= repperiod.compReportingDateEnd
                    or programENT.startDate is null)
                and coalesce(programENT.isIPEDSReportable, true) = true
                --and programENT.isForStudentAcadTrack = true
                --and programENT.isForDegreeAcadHistory = true
                and ((coalesce(to_date(programENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                        and to_date(programENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.compReportingDateEnd)
                    or coalesce(to_date(programENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))
            left join AcademicTermOrder termorder on termorder.termCode = upper(programENT.termCodeEffective)
                and termorder.termOrder <= repperiod.maxTermOrder
            left join CampusMCR campus on upper(programENT.campus) = campus.campus
    )
    where programRn = 1
        and isESL = false
        and major is not null
),

DegreeMCR as (
-- Pulls degree information as of the reporting period

select degreeProgram,
        degreeLevel,
        degree,
        awardLevel,
        major,
        campus,
        isInternationalDegProg,
        snapshotDate,
        compReportingDateEnd,
        (case when distanceEducationType = 'Not distance education' then 1 else 0 end) DENotDistanceEd,
        (case when distanceEducationType in ('Distance education with no onsite component', 'Distance education with mandatory onsite component', 'Distance education with non-mandatory onsite component') then 1 else 0 end) DEisDistanceEd,
        (case when distanceEducationType = 'Distance education with no onsite component' then 1 else 0 end) DEEntirelyDistanceEd,
        (case when distanceEducationType = 'Distance education with mandatory onsite component' then 1 else 0 end) DEMandatoryOnsite,
        (case when distanceEducationType = 'Distance education with non-mandatory onsite component' then 1 else 0 end) DEMOptionalOnsite
    from (
    select degprog.degreeProgram degreeProgram,
            degreeENT.degreeLevel degreeLevel,
            degprog.degree degree,
            degprog.major major,
            --removed college and department - these could possibly be needed for certain clients
            degprog.campus campus,
            degprog.isInternationalDegProg isInternationalDegProg,
            degprog.distanceEducationType distanceEducationType,
            degprog.snapshotDate snapshotDate,
            degprog.compReportingDateEnd compReportingDateEnd,
            degreeENT.awardLevel awardLevel,
            coalesce(row_number() over (
                partition by
                    degprog.degreeProgram,
                    degprog.campus,
                    degprog.distanceEducationType
                order by
                    degreeENT.recordActivityDate desc
            ), 1) as degreeRn
        from DegreeProgramMCR degprog
            left join Degree degreeENT on degprog.snapshotDate = degreeENT.snapshotDate
                and degprog.degree = upper(degreeENT.degree)
                and coalesce(degreeENT.isIpedsReportable, true) = true
                and coalesce(degreeENT.isNonDegreeSeeking, false) = false
                and ((coalesce(to_date(degreeENT.recordActivityDate,'YYYY-MM-DD'),CAST('9999-09-09' AS DATE))  != CAST('9999-09-09' as DATE)
                        and to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') <= degprog.compReportingDateEnd)
                    or coalesce(to_date(degreeENT.recordActivityDate,'YYYY-MM-DD'),CAST('9999-09-09' AS DATE))  = CAST('9999-09-09' as DATE))
    )
where degreeRn = 1
    and awardLevel is not null
),

FieldOfStudyMCR as (
-- Pulls major information as of the reporting period and joins to the degrees on curriculumRule

select *
from (
    select degprog.degreeProgram degreeProgram,
            degprog.degreeLevel degreeLevel,
            degprog.degree degree,
            degprog.major major,
            degprog.campus campus,
            degprog.isInternationalDegProg isInternationalDegProg,
            degprog.snapshotDate snapshotDate,
            degprog.compReportingDateEnd compReportingDateEnd,
            degprog.awardLevel awardLevel,
            degprog.DENotDistanceEd,
            degprog.DEisDistanceEd,
            degprog.DEEntirelyDistanceEd,
            degprog.DEMandatoryOnsite,
            degprog.DEMOptionalOnsite,
            CONCAT(LPAD(SUBSTR(CAST(fosENT.cipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(fosENT.cipCode as STRING), 3, 6), 4, '0')) cipCode,
            coalesce(row_number() over (
                partition by
                    degprog.degreeProgram,
                    degprog.degreeLevel,
                    degprog.awardLevel,
                    degprog.degree,
                    fosENT.fieldOfStudy,
                    degprog.major,
                    fosENT.cipCode,
                    degprog.campus
                order by
                    fosENT.recordActivityDate desc
            ), 1) as fosRn
    from DegreeMCR degprog
        left join FieldOfStudy fosENT on degprog.snapshotDate = fosENT.snapshotDate
            and degprog.major = upper(fosENT.fieldOfStudy)
            and fosENT.fieldOfStudyType = 'Major'
            --and fosENT.cipCode is not null
            and ((coalesce(to_date(fosENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' as DATE)
            and to_date(fosENT.recordActivityDate,'YYYY-MM-DD') <= degprog.compReportingDateEnd)
            or coalesce(to_date(fosENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' as DATE))
         --   and SUBSTR(CAST(fosENT.cipCodeVersion as STRING), 3, 2) >= 20 -- Current CIPCode standard was released 2021. new specs introduced per decade.
    )
where fosRn = 1
    and cipCode is not null
),

AwardMCR as (
--Pulls all distinct student awards obtained withing the reporting terms/dates.

select *
from (
    select awardENT.personId personId,
            repperiod.yearType yearType,
            upper(awardENT.degreeProgram) degreeProgram,
            repperiod.surveySection surveySection,
            repperiod.snapshotDate termSSD,
            repperiod.genderForUnknown genderForUnknown,
            repperiod.genderForNonBinary genderForNonBinary,
            repperiod.reportingDateStart reportingDateStart,
            repperiod.reportingDateEnd reportingDateEnd,
            repperiod.compReportingDateStart compReportingDateStart,
            repperiod.compReportingDateEnd compReportingDateEnd,
            repperiod.termCode termCode,
            repperiod.maxTermOrder maxTermOrder,
            termorder.termOrder termOrder,
            repperiod.maxCensus maxCensus,
            repperiod.compGradDateOrTerm compGradDateOrTerm,
            awardENT.snapshotDate snapshotDate,
            to_date(awardENT.awardedDate, 'YYYY-MM-DD') awardedDate,
            coalesce(to_date(awardENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
            upper(awardENT.awardedTermCode) awardedTermCode,
            awardENT.awardStatus awardStatus,
            upper(awardENT.collegeOverride) collegeOverride,
            upper(awardENT.divisionOverride) divisionOverride,
            upper(awardENT.departmentOverride) departmentOverride,
            upper(awardENT.campusOverride) campusOverride,
            coalesce(campus.isInternational, false) isInternationalAward,
            coalesce(row_number() over (
                partition by
                    awardENT.personId,
                    awardENT.awardedDate,
                    awardENT.degreeProgram
                order by
                    awardENT.recordActivityDate desc
            ), 1) as awardRn
    from AcademicTermReportingRefactor repperiod
        inner join Award awardENT on repperiod.snapshotDate = awardENT.snapshotDate
            and ((repperiod.compGradDateOrTerm = 'D' and to_date(awardENT.awardedDate,'YYYY-MM-DD') between 
                repperiod.compReportingDateStart and repperiod.compReportingDateEnd)
            or (repperiod.compGradDateOrTerm = 'T' and upper(awardENT.awardedTermCode) = repperiod.termCode))
            and coalesce(awardENT.isIpedsReportable, true) = true
            and awardENT.awardedDate is not null
            and ((coalesce(to_date(awardENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  != CAST('9999-09-09' as DATE)
                    and to_date(awardENT.recordActivityDate,'YYYY-MM-DD')  <= repperiod.reportingDateEnd)
                or coalesce(to_date(awardENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  = CAST('9999-09-09' as DATE))
       inner join AcademicTermOrder termorder on termorder.termCode = upper(awardENT.awardedTermCode)
                            and termorder.termOrder <= repperiod.maxTermOrder
        left join CampusMCR campus on upper(awardENT.campus) = campus.campus
    ) awardData
where awardData.awardRn = 1
    and awardData.awardStatus = 'Awarded'
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

select pers.personId personId,
        pers.yearType yearType,
        pers.degreeProgram degreeProgram,
        pers.awardedTermCode awardedTermCode,
        pers.awardedDate awardedDate,
        pers.collegeOverride collegeOverride,
        pers.campusOverride campusOverride,
        pers.isInternationalAward isInternationalAward,
        pers.snapshotDate snapshotDate,
        pers.surveySection surveySection,
        pers.reportingDateEnd reportingDateEnd,
        pers.maxTermOrder maxTermOrder,
        (case when pers.gender = 'Male' then 'M'
            when pers.gender = 'Female' then 'F' 
            when pers.gender = 'Non-Binary' then pers.genderForNonBinary
            else pers.genderForUnknown
        end) ipedsGender,
        (case when pers.isUSCitizen = true or ((pers.isInUSOnVisa = true or pers.awardedDate between pers.visaStartDate and pers.visaEndDate)
            and pers.visaType in ('Employee Resident', 'Other Resident')) then 
            (case when pers.isHispanic = true then '2' 
                when pers.isMultipleRaces = true then '8' 
                when pers.ethnicity != 'Unknown' and pers.ethnicity is not null then
                    (case when pers.ethnicity = 'Hispanic or Latino' then '2'
                        when pers.ethnicity = 'American Indian or Alaskan Native' then '3'
                        when pers.ethnicity = 'Asian' then '4'
                        when pers.ethnicity = 'Black or African American' then '5'
                        when pers.ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
                        when pers.ethnicity = 'Caucasian' then '7'
                        else '9' 
                    end) 
                    else '9' end) -- 'race and ethnicity unknown'
            when ((pers.isInUSOnVisa = true or pers.awardedDate between pers.visaStartDate and pers.visaEndDate)
                and pers.visaType in ('Student Non-resident', 'Employee Non-resident', 'Other Non-resident')) then '1' -- 'nonresident alien'
            else '9' -- 'race and ethnicity unknown'
        end) ipedsEthnicity,
        (case when pers.asOfAge < 18 then 'AGE1' 
            when pers.asOfAge >= 18 and pers.asOfAge <= 24 then 'AGE2' 
            when pers.asOfAge >= 25 and pers.asOfAge <= 39 then 'AGE3' 
            when pers.asOfAge >= 40 then 'AGE4' 
            else 'AGE5' --Age unknown
        end) ipedsAgeGroup
from (
    select distinct award.personId personId,
            award.yearType yearType,
            award.degreeProgram degreeProgram,
            award.awardedTermCode awardedTermCode,
            award.awardedDate awardedDate,
            award.collegeOverride collegeOverride,
            award.campusOverride campusOverride,
            award.isInternationalAward isInternationalAward,
            award.snapshotDate snapshotDate,
            award.surveySection surveySection,
            award.maxTermOrder maxTermOrder,
            award.genderForUnknown genderForUnknown,
            award.genderForNonBinary genderForNonBinary,
            award.reportingDateEnd reportingDateEnd,
            award.compGradDateOrTerm compGradDateOrTerm,
            floor(DATEDIFF(award.awardedDate, to_date(personENT.birthDate,'YYYY-MM-DD')) / 365) asOfAge,
            to_date(personENT.birthDate,'YYYY-MM-DD') birthDate,
            personENT.ethnicity ethnicity,
            coalesce(personENT.isHispanic, false) isHispanic,
            coalesce(personENT.isMultipleRaces, false) isMultipleRaces,
            coalesce(personENT.isInUSOnVisa, false) isInUSOnVisa,
            to_date(personENT.visaStartDate,'YYYY-MM-DD') visaStartDate,
            to_date(personENT.visaEndDate,'YYYY-MM-DD') visaEndDate,
            personENT.visaType visaType,
            coalesce(personENT.isUSCitizen, true) isUSCitizen,
            personENT.gender gender,
            upper(personENT.nation) nation,
            upper(personENT.state) state,
            coalesce(row_number() over (
                partition by
                    award.personId,
                    personENT.personId,
                    award.awardedDate
                order by
                     personENT.recordActivityDate desc
                ), 1) personRn
        from AwardMCR award
            left join Person personENT on award.personId = personENT.personId
                and award.snapshotDate = personENT.snapshotDate
                and coalesce(personENT.isIpedsReportable, true) = true
                and ((coalesce(to_date(personENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  != CAST('9999-09-09' AS DATE)
                        and to_date(personENT.recordActivityDate,'YYYY-MM-DD')  <= award.awardedDate)
                or coalesce(to_date(personENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  = CAST('9999-09-09' AS DATE))
) pers
    where pers.personRn = 1
),

AcademicTrackMCR as (
--Returns most up to date student academic track information as of their award date and term. 

select person2.personId personId,
        person2.yearType yearType,
        person2.awardedDate awardedDate,
        person2.awardedTermCode awardedTermCode,
        person2.maxTermOrder maxTermOrder,
        person2.snapshotDate snapshotDate,
        person2.ipedsGender ipedsGender,
        person2.ipedsEthnicity ipedsEthnicity,
        person2.ipedsAgeGroup ipedsAgeGroup,
        person2.collegeOverride collegeOverride,
        person2.campusOverride campusOverride,
        person2.isInternationalAward isInternationalAward,
        AcadTrackRec.isInternational isInternationalAcadTrack,
        AcadTrackRec.termCodeEffective termCodeEffective,
        AcadTrackRec.degreeProgram degreeProgram,
        AcadTrackRec.fieldOfStudy fieldOfStudy,
        AcadTrackRec.fieldOfStudyType fieldOfStudyType,
        AcadTrackRec.fieldOfStudyPriority fieldOfStudyPriority,
        AcadTrackRec.academicTrackStatus academicTrackStatus,
        AcadTrackRec.fieldOfStudyActionDate fieldOfStudyActionDate,
        AcadTrackRec.recordActivityDate recordActivityDate,
        AcadTrackRec.termOrder acadTrackTermOrder,
        coalesce(row_number() over (
            partition by
                person2.personId,
                person2.awardedDate,
                person2.degreeProgram
        order by
            AcadTrackRec.fieldOfStudyPriority asc), 1
        ) fosRn
from PersonMCR person2
    left join (
        select * from ( 
            select distinct acadtrackENT.personId personId,
                    person1.yearType yearType,
                    person1.awardedDate awardedDate,
                    upper(acadtrackENT.termCodeEffective) termCodeEffective,
                    coalesce(to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) fieldOfStudyActionDate,
                    termorder.termOrder termOrder,
                    coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
                    upper(acadtrackENT.degreeProgram) degreeProgram,
                    upper(acadtrackENT.fieldOfStudy) fieldOfStudy,
                    acadtrackENT.fieldOfStudyType fieldOfStudyType,
                    acadtrackENT.fieldOfStudyPriority fieldOfStudyPriority,
                    upper(acadtrackENT.collegeOverride) collegeOverride,
                    upper(acadtrackENT.campusOverride) campusOverride,
                    coalesce(campus.isInternational, false) isInternational,
                    acadtrackENT.academicTrackStatus academicTrackStatus,
                    coalesce(row_number() over (
                        partition by
                            person1.personId,
                            acadtrackENT.personId,
                            person1.awardedDate,
                            acadtrackENT.degreeProgram,
                            acadtrackENT.fieldOfStudyPriority
                        order by
                             termorder.termOrder desc,
                             acadtrackENT.fieldOfStudyActionDate desc,
                             acadtrackENT.recordActivityDate desc,
                             (case when acadtrackENT.academicTrackStatus = 'Completed' then 1 else 2 end) asc
                       ), 1) acadtrackRn 
            from PersonMCR person1
                inner join AcademicTrack acadtrackENT ON person1.personId = acadtrackENT.personId
                    and person1.snapshotDate = acadtrackENT.snapshotDate
                    and person1.degreeProgram = upper(acadTrackENT.degreeProgram)
                    and ((coalesce(to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                             and to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD') <= person1.awardedDate)
                            or (coalesce(to_date(acadtrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                                and ((coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))!= CAST('9999-09-09' as DATE)
                                        and to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD') <= person1.awardedDate)
                                    or coalesce(to_date(acadtrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' as DATE))))
                    and acadTrackENT.fieldOfStudyType = 'Major'
                    and coalesce(acadTrackENT.isIpedsReportable, true) = true
                --and coalesce(acadTrackENT.isCurrentFieldOfStudy, true) = true
                inner join AcademicTermOrder termorder on termorder.termCode = upper(acadtrackENT.termCodeEffective)
                                and termorder.termOrder <= person1.maxTermOrder
                left join CampusMCR campus on campus.campus = upper(acadtrackENT.campusOverride)
        )
        where acadtrackRn = 1
    ) AcadTrackRec on person2.personID = AcadTrackRec.personID
            and person2.awardedDate = AcadTrackRec.awardedDate
            and person2.degreeProgram = AcadTrackRec.degreeProgram   
),

Completer as (
--Returns all completer records to use for Parts C & D

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
    (case when IPEDSethnicity = '9' and IPEDSGender = 'F' then 1 else 0 end) reg18, --(CRACE14) - Race and ethnicity unknown, F, (0 to 999999)
    (case when awardLevel = 'Associates Degree' then '3' 
        when awardLevel = 'Bachelors Degree' then '4'  
        when awardLevel = 'Masters Degree' then '5' 
        when awardLevel in ('Doctors Degree (Research/Scholarship)',
                                    'Doctors Degree (Professional Practice)',
                                    'Doctors Degree (Other)') then '6'	 
        when awardLevel in ('Post-baccalaureate Certificate',
                                    'Post-Masters Certificate') then '7'				
        when awardLevel = 'Postsecondary (<300 clock/<9 semester credit/<13 quarter credit hours)' then '8' 
        when awardLevel = 'Postsecondary (300-899 clock/9-29 semester credit/13-44 quarter credit hours)' then '9' 
        when awardLevel in ('Postsecondary (900-1800 clock/30-60 semester credit/45-90 quarter credit hours)',
                                    'Postsecondary (>1800 clock/>60 semester credit/>90 quarter credit hours)') then '2' 
    end) ACATPartD
from (
    select acadtrack.fosRn majorNum,
            acadtrack.degreeProgram degreeProgram,
            degprog.degree degree,
            acadtrack.fieldOfStudy fieldOfStudy,
            acadtrack.personId personId,
            acadtrack.ipedsGender ipedsGender,
            acadtrack.ipedsEthnicity ipedsEthnicity,
            acadtrack.ipedsAgeGroup ipedsAgeGroup,
            degprog.awardLevel awardLevel,
            degprog.cipCode cipCode
    from AcademicTrackMCR acadtrack
        inner join FieldOfStudyMCR degprog on degprog.degreeProgram = acadtrack.degreeProgram
            and degprog.major = acadtrack.fieldOfStudy
    where acadtrack.fosRn < 3
        and coalesce(acadtrack.isInternationalAward, acadtrack.isInternationalAcadTrack, degprog.isInternationalDegProg, false) = false
    )
),

DegreeFOSProgramSTU as (
-- Pulls all CIPCodes and ACAT levels including student awarded levels to use for Parts A & B

select majorNum majorNum,
    cipCode cipCode,
    awardLevel awardLevel,
    (case when awardLevel = 'Postsecondary (<300 clock/<9 semester credit/<13 quarter credit hours)' then '1a' 
        when awardLevel = 'Postsecondary (300-899 clock/9-29 semester credit/13-44 quarter credit hours)' then '1b'
        when awardLevel = 'Postsecondary (900-1800 clock/30-60 semester credit/45-90 quarter credit hours)' then '2'
        when awardLevel = 'Associates Degree' then '3' 
        when awardLevel = 'Postsecondary (>1800 clock/>60 semester credit/>90 quarter credit hours)' then '4'
        when awardLevel = 'Bachelors Degree' then '5'
        when awardLevel = 'Post-baccalaureate Certificate' then '6'
        when awardLevel = 'Masters Degree' then '7'
        when awardLevel = 'Post-Masters Certificate' then '8'
        when awardLevel = 'Doctors Degree (Research/Scholarship)' then '17'
        when awardLevel = 'Doctors Degree (Professional Practice)' then '18'
        when awardLevel = 'Doctors Degree (Other)' then '19'
    end) ACATPartAB,
    (case when awardLevel = 'Associates Degree' then '3' 
        when awardLevel = 'Bachelors Degree' then '4'  
        when awardLevel = 'Masters Degree' then '5' 
        when awardLevel in ('Doctors Degree (Research/Scholarship)',
                                    'Doctors Degree (Professional Practice)',
                                    'Doctors Degree (Other)') then '6'	 
        when awardLevel in ('Post-baccalaureate Certificate',
                                    'Post-Masters Certificate') then '7'				
        when awardLevel = 'Postsecondary (<300 clock/<9 semester credit/<13 quarter credit hours)' then '8' 
        when awardLevel = 'Postsecondary (300-899 clock/9-29 semester credit/13-44 quarter credit hours)' then '9' 
        when awardLevel in ('Postsecondary (900-1800 clock/30-60 semester credit/45-90 quarter credit hours)',
                                    'Postsecondary (>1800 clock/>60 semester credit/>90 quarter credit hours)') then '2' 
    end) ACATPartD,
    (case when DEisDistanceEd = totalPrograms then 1 -- All programs in this CIP code in this award level can be completed entirely via distance education.
     when DEisDistanceEd = 0 then 2 -- None of the programs in this CIP code in this award level can be completed entirely via distance education.
     when totalPrograms > DEisDistanceEd and DEisDistanceEd > 0 then 3 -- Some programs in this CIP code in this award level can be completed entirely via distance education.
    end) DEAvailability,
--AK 20211209: Removed for 2021-2022, some DE is not required to be broken down into mandatory and option onsite.
/*    (case when totalPrograms > DEisDistanceEd
        and DEisDistanceEd > 0   
        then (case when DEMandatoryOnsite > 0 then 1 else 0 end)
    else null
    end) DESomeRequiredOnsite,
    (case when totalPrograms > DEisDistanceEd
        and DEisDistanceEd > 0   
        then (case when DEMOptionalOnsite > 0 then 1 else 0 end)
    else null
    end) DESomeOptionalOnsite,
*/
    FIELD1, --Nonresident Alien
    FIELD2,
    FIELD3, -- Hispanic/Latino
    FIELD4,
    FIELD5, -- American Indian or Alaska Native
    FIELD6,
    FIELD7, -- Asian
    FIELD8,
    FIELD9, -- Black or African American
    FIELD10,
    FIELD11, -- Native Hawaiian or Other Pacific Islander
    FIELD12,
    FIELD13, -- White
    FIELD14,
    FIELD15, -- Two or more races
    FIELD16,
    FIELD17, -- Race and ethnicity unknown
    FIELD18
from ( 
    select majorNum majorNum,
            cipCode cipCode,
            awardLevel awardLevel,
            count(*) totalPrograms,
            SUM(DENotDistanceEd) DENotDistanceEd,
            SUM(DEisDistanceEd) DEisDistanceEd,
            SUM(DEEntirelyDistanceEd) DEEntirelyDistanceEd,
--AK 20211209
            --SUM(DEMandatoryOnsite) DEMandatoryOnsite,
            --SUM(DEMOptionalOnsite) DEMOptionalOnsite,
            SUM(FIELD1) FIELD1, --Nonresident Alien
            SUM(FIELD2) FIELD2,
            SUM(FIELD3) FIELD3, -- Hispanic/Latino
            SUM(FIELD4) FIELD4,
            SUM(FIELD5) FIELD5, -- American Indian or Alaska Native
            SUM(FIELD6) FIELD6,
            SUM(FIELD7) FIELD7, -- Asian
            SUM(FIELD8) FIELD8,
            SUM(FIELD9) FIELD9, -- Black or African American
            SUM(FIELD10) FIELD10,
            SUM(FIELD11) FIELD11, -- Native Hawaiian or Other Pacific Islander
            SUM(FIELD12) FIELD12,
            SUM(FIELD13) FIELD13, -- White
            SUM(FIELD14) FIELD14,
            SUM(FIELD15) FIELD15, -- Two or more races
            SUM(FIELD16) FIELD16,
            SUM(FIELD17) FIELD17, -- Race and ethnicity unknown
            SUM(FIELD18) FIELD18
    from (
        select distinct coalesce(stuCIP.MajorNum, 1) majorNum,
                degfosprog.cipCode cipCode,
                degfosprog.awardLevel awardLevel,
                degfosprog.degreeProgram degreeProgram,
                degfosprog.DENotDistanceEd DENotDistanceEd,
                degfosprog.DEisDistanceEd DEisDistanceEd,
                degfosprog.DEEntirelyDistanceEd DEEntirelyDistanceEd,
                --degfosprog.DEMandatoryOnsite DEMandatoryOnsite,
                --degfosprog.DEMOptionalOnsite DEMOptionalOnsite,
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
        from FieldOfStudyMCR degfosprog
            left join (
            select acadtrack.majorNum majorNum,
                    acadtrack.cipCode cipCode,
                    acadtrack.awardLevel awardLevel,
                    acadtrack.degreeProgram degreeProgram,
                    SUM(acadtrack.reg1) FIELD1, --Nonresident Alien
                    SUM(acadtrack.reg2) FIELD2,
                    SUM(acadtrack.reg3) FIELD3, -- Hispanic/Latino
                    SUM(acadtrack.reg4) FIELD4,
                    SUM(acadtrack.reg5) FIELD5, -- American Indian or Alaska Native
                    SUM(acadtrack.reg6) FIELD6,
                    SUM(acadtrack.reg7) FIELD7, -- Asian
                    SUM(acadtrack.reg8) FIELD8,
                    SUM(acadtrack.reg9) FIELD9, -- Black or African American
                    SUM(acadtrack.reg10) FIELD10,
                    SUM(acadtrack.reg11) FIELD11, -- Native Hawaiian or Other Pacific Islander
                    SUM(acadtrack.reg12) FIELD12,
                    SUM(acadtrack.reg13) FIELD13, -- White
                    SUM(acadtrack.reg14) FIELD14,
                    SUM(acadtrack.reg15) FIELD15, -- Two or more races
                    SUM(acadtrack.reg16) FIELD16,
                    SUM(acadtrack.reg17) FIELD17, -- Race and ethnicity unknown
                    SUM(acadtrack.reg18) FIELD18
                from Completer acadtrack
                group by acadtrack.majorNum, 
                    acadtrack.cipCode,
                    acadtrack.awardLevel,
                    acadtrack.degreeProgram
            ) stuCIP on degfosprog.awardLevel = stuCIP.awardLevel
                    and degfosprog.cipCode = stuCIP.cipCode
                    and degfosprog.degreeProgram = stuCIP.degreeProgram
        )
    group by majorNum,
            cipCode,
            awardLevel
    )
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

select 'A'                      part , --(PART)	- "A"
    majorNum                 majornum, --(MAJORNUM)	- 1 = First Major, 2 = Second Major
    cipCode                  cipcode, --(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
    ACATPartAB               awlevel, --(AWLEVEL)		- 1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2;
    null                    distanceed, --(DistanceED)  ***NOT USED IN PART A***
    FIELD1                  field1, --(CRACE01) 	- Nonresident Alien, M, (0 to 999999)
    FIELD2                  field2, --(CRACE02) 	- Nonresident Alien, F, (0 to 999999)
    FIELD3                  field3, --(CRACE25) 	- Hispanic/Latino, M, (0 to 999999)					
    FIELD4                  field4, --(CRACE26) 	- Hispanic/Latino, F, (0 to 999999) 
    FIELD5                  field5, --(CRACE27) 	- American Indian or Alaska Native, M, (0 to 999999)
    FIELD6                  field6, --(CRACE28) 	- American Indian or Alaska Native, F, (0 to 999999)
    FIELD7                  field7, --(CRACE29) 	- Asian, M, (0 to 999999)
    FIELD8                  field8, --(CRACE30) 	- Asian, F, (0 to 999999)
    FIELD9                  field9, --(CRACE31) 	- Black or African American, M, (0 to 999999)
    FIELD10                 field10, --(CRACE32) 	- Black or African American, F, (0 to 999999)
    FIELD11                 field11, --(CRACE33) 	- Native Hawaiian or Other Pacific Islander, M, (0 to 999999)
    FIELD12                 field12, --(CRACE34) 	- Native Hawaiian or Other Pacific Islander, F, (0 to 999999)
    FIELD13                 field13, --(CRACE35) 	- White, M, (0 to 999999)
    FIELD14                 field14, --(CRACE36) 	- White, F, (0 to 999999)
    FIELD15                 field15, --(CRACE37) 	- Two or more races, M, (0 to 999999)
    FIELD16                 field16, --(CRACE38) 	- Two or more races, F, (0 to 999999)
    FIELD17                 field17, --(CRACE13) 	- Race and ethnicity unknown, M, (0 to 999999)		
    FIELD18                 field18
--(CRACE14) 	- Race and ethnicity unknown, F, (0 to 999999) 
from DegreeFOSProgramSTU
where (majorNum = 1
    or (majorNum = 2
    and ACATPartAB in ('3', '5', '7', '17', '18', '19'))) --1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2
    and cipCode not in ('06.0101','06.0201','15.3402','18.1101','23.0200','23.0102','42.0102')

union

--Part B: Completions - Distance Education
--Used to distinguish if an award is offered in a distance education format. A "distance education program" is "a program for which all the required coursework 
--for program completion is able to be completed via distance education courses."
--valid ACAT values are 1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2

select 'B',
    majorNum, --(MAJORNUM)	- 1 = First Major, 2 = Second Major
    cipCode, --(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
    ACATPartAB, --(AWLEVEL)		- 1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2;
    DEAvailability, --(DistanceED)  - Is at least one program within this CIP code offered as a distance education program? (1 = All, 2 = Some, 3 = None)
--AK 20211209 specification of 'Some' DE is no longer needed in 2021-22 survey
    --DESomeRequiredOnsite, --(DistanceED31)     - At least one program in this CIP code in this award level has a mandatory onsite component. (0 = No, 1 = Yes)
    --DESomeOptionalOnsite, --(DistanceED32)     - At least one program in this CIP code in this award level has a non-mandatory onsite component. (0 = No, 1 = Yes)
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
from DegreeFOSProgramSTU
where (majorNum = 1
    or (majorNum = 2
    and ACATPartAB in ('3', '5', '7', '17', '18', '19')))
    and cipCode not in ('06.0101','06.0201','15.3402','18.1101','23.0200','23.0102','42.0102')

union

--Part C: All Completers
--Count each student only once, regardless of how many awards he/she earned. The intent of this screen is to collect an unduplicated count 
--of total numbers of completers.

select 'C' ,
    null,
    null,
    null,
    null,
    coalesce(sum(field1), 0) field1, --Nonresident Alien
    coalesce(sum(field2), 0) field2,
    coalesce(sum(field3), 0) field3, -- Hispanic/Latino
    coalesce(sum(field4), 0) field4,
    coalesce(sum(field5), 0) field5, -- American Indian or Alaska Native
    coalesce(sum(field6), 0) field6,
    coalesce(sum(field7), 0) field7, -- Asian
    coalesce(sum(field8), 0) field8,
    coalesce(sum(field9), 0) field9, -- Black or African American
    coalesce(sum(field10), 0) field10,
    coalesce(sum(field11), 0) field11, -- Native Hawaiian or Other Pacific Islander
    coalesce(sum(field12), 0) field12,
    coalesce(sum(field13), 0) field13, -- White
    coalesce(sum(field14), 0) field14,
    coalesce(sum(field15), 0) field15, -- Two or more races
    coalesce(sum(field16), 0) field16,
    coalesce(sum(field17), 0) field17, -- Race and ethnicity unknown
    coalesce(sum(field18), 0) field18
from (
select distinct personId,
        first(reg1) field1, --(CRACE01) 	- Nonresident Alien, M, (0 to 999999)
        first(reg2) field2, --(CRACE02) 	- Nonresident Alien, F, (0 to 999999)
        first(reg3) field3, --(CRACE25) 	- Hispanic/Latino, M, (0 to 999999)					
        first(reg4) field4, --(CRACE26) 	- Hispanic/Latino, F, (0 to 999999) 
        first(reg5) field5, --(CRACE27) 	- American Indian or Alaska Native, M, (0 to 999999)
        first(reg6) field6, --(CRACE28) 	- American Indian or Alaska Native, F, (0 to 999999)
        first(reg7) field7, --(CRACE29) 	- Asian, M, (0 to 999999)
        first(reg8) field8, --(CRACE30) 	- Asian, F, (0 to 999999)
        first(reg9) field9, --(CRACE31) 	- Black or African American, M, (0 to 999999)
        first(reg10) field10, --(CRACE32) 	- Black or African American, F, (0 to 999999)
        first(reg11) field11, --(CRACE33) 	- Native Hawaiian or Other Pacific Islander, M, (0 to 999999)
        first(reg12) field12, --(CRACE34) 	- Native Hawaiian or Other Pacific Islander, F, (0 to 999999)
        first(reg13) field13, --(CRACE35) 	- White, M, (0 to 999999)
        first(reg14) field14, --(CRACE36) 	- White, F, (0 to 999999)
        first(reg15) field15, --(CRACE37) 	- Two or more races, M, (0 to 999999)
        first(reg16) field16, --(CRACE38) 	- Two or more races, F, (0 to 999999)
        first(reg17) field17, --(CRACE13) 	- Race and ethnicity unknown, M, (0 to 999999)		
        first(reg18) field18
    --(CRACE14) 	- Race and ethnicity unknown, F, (0 to 999999) 
    from Completer
    group by personId
  )

union

--Part D: Completers by Level
--Each student should be counted only once per award level. For example, if a student earned a master's and doctor's degree, 
--he/she should be counted once in master's and once in doctor's. A student who earned two master's degrees should be counted only once.

--The total number of students reported by gender must equal the total number of students reported by race and ethnicity. Each of the 
--two previously mentioned totals must equal the total number of students reported by age.

--Note: Records with a count of zero should not be submitted, EXCEPT for cases where there were no awards conferred for a specific 
--Completers level during academic year 2020-20 but the institution still offers the program at that completers level. 

select 'D',
    null,
    null,
    ACATPartD ACATPartD, --CTLEVEL - 2 to 9.
    null,
    coalesce(SUM(field_1), 0), --(CRACE15)	- Men, 0 to 999999
    coalesce(SUM(field_2), 0), --(CRACE16) - Women, 0 to 999999 
    coalesce(SUM(field_3), 0), --(CRACE17) - Nonresident Alien, 0 to 999999
    coalesce(SUM(field_4), 0), --(CRACE41) - Hispanic/Latino, 0 to 999999
    coalesce(SUM(field_5), 0), --(CRACE42) - American Indian or Alaska Native, 0 to 999999
    coalesce(SUM(field_6), 0), --(CRACE43) - Asian, 0 to 999999
    coalesce(SUM(field_7), 0), --(CRACE44) - Black or African American, 0 to 999999
    coalesce(SUM(field_8), 0), --(CRACE45) - Native Hawaiian or Other Pacific Islander, 0 to 999999
    coalesce(SUM(field_9), 0), --(CRACE46) - White, 0 to 999999
    coalesce(SUM(field_10), 0), --(CRACE47) - Two or more races, 0 to 999999
    coalesce(SUM(field_11), 0), --(CRACE23) - Race and ethnicity unknown, 0 to 999999
    coalesce(SUM(field_12), 0), --(AGE1) - Under 18, 0 to 999999
    coalesce(SUM(field_13), 0), --(AGE2) - 18-24, 0 to 999999
    coalesce(SUM(field_14), 0), --(AGE3) - 25-39, 0 to 999999
    coalesce(SUM(field_15), 0), --(AGE4) - 40 and above, 0 to 999999
    coalesce(SUM(field_16), 0), --(AGE5) - Age unknown, 0 to 999999
    null,
    null
from ( 
       select distinct ACATPartD,
            personId personId,
            (case when IPEDSGender = 'M' then 1 else 0 end) field_1, --(CRACE15)	-- Men, 0 to 999999
            (case when IPEDSGender = 'F' then 1 else 0 end) field_2, --(CRACE16) - Women, 0 to 999999 
            (case when IPEDSethnicity = '1' then 1 else 0 end) field_3, --(CRACE17) - Nonresident Alien, 0 to 999999
            (case when IPEDSethnicity = '2' then 1 else 0 end) field_4, --(CRACE41) - Hispanic/Latino, 0 to 999999
            (case when IPEDSethnicity = '3' then 1 else 0 end) field_5, --(CRACE42) - American Indian or Alaska Native, 0 to 999999
            (case when IPEDSethnicity = '4' then 1 else 0 end) field_6, --(CRACE43) - Asian, 0 to 999999
            (case when IPEDSethnicity = '5' then 1 else 0 end) field_7, --(CRACE44) - Black or African American, 0 to 999999
            (case when IPEDSethnicity = '6' then 1 else 0 end) field_8, --(CRACE45) - Native Hawaiian or Other Pacific Islander, 0 to 999999
            (case when IPEDSethnicity = '7' then 1 else 0 end) field_9, --(CRACE46) - White, 0 to 999999
            (case when IPEDSethnicity = '8' then 1 else 0 end) field_10, --(CRACE47) - Two or more races, 0 to 999999
            (case when IPEDSethnicity = '9' then 1 else 0 end) field_11, --(CRACE23) - Race and ethnicity unknown, 0 to 999999
            (case when IPEDSAgeGroup = 'AGE1' then 1 else 0 end) field_12, --(AGE1) - Under 18, 0 to 999999
            (case when IPEDSAgeGroup = 'AGE2' then 1 else 0 end) field_13, --(AGE2) - 18-24, 0 to 999999
            (case when IPEDSAgeGroup = 'AGE3' then 1 else 0 end) field_14, --(AGE3) - 25-39, 0 to 999999
            (case when IPEDSAgeGroup = 'AGE4' then 1 else 0 end) field_15, --(AGE4) - 40 and above, 0 to 999999
            (case when IPEDSAgeGroup = 'AGE5' then 1 else 0 end) field_16
        --(AGE5) - Age unknown, 0 to 999999 
        from Completer
        --cohortstu 
        --left join DegreeMCR deg on deg.degree = cohortstu.degree
        --    and deg.degreeLevel = cohortstu.degreeLevel

    union

        select distinct deg2.ACATPartD,
            null,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0
        from DegreeFOSProgramSTU deg2
)
group by ACATPartD

union

--Dummy set to return default formatting if no student awards exist.

select *
from (
    VALUES
            ('A', 1, '01.0101', '3', null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            ('B', 1, '01.0101', '3', 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
            ('D', null, null, '3', null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null)
    ) as dummySet(PART, MAJORNUM, CIPC_LEVL, AWLEVEL, isDistanceED, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, 
                  FIELD10, FIELD11, FIELD12, FIELD13, FIELD14, FIELD15, FIELD16, FIELD17, FIELD18)
where not exists (select a.cipCode
                from DegreeFOSProgramSTU a)

union

--Dummy set to return default formatting if no student awards exist.
select *
from (
    VALUES
            ('C', null, null, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    ) as dummySet(PART, MAJORNUM, CIPC_LEVL, AWLEVEL, isDistanceED, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, 
                  FIELD10, FIELD11, FIELD12, FIELD13, FIELD14, FIELD15, FIELD16, FIELD17, FIELD18)
where not exists (select a.personId
                from AcademicTrackMCR a) 
   
