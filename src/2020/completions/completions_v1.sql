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
20201130            jhanicak                                    Updated to latest views and formatting; mods for DM changes;
                                                                Removed most snapshot logic since only one snapshot used PF-1759 
                                                                Run time: prod 18m 302  test 29m 31s 
20200914            akhasawneh          			    		Initial version (Run time 30m)

SNAPSHOT REQUIREMENTS
One of the following is required:

Full Year Term End - end date of last term used for reporting; this option is best for clients with
					IPEDSClientConfig.compGradDateOrTerm = 'T'
Full Year June End - survey official end of reporting - June 30; this option is best for clients with
					IPEDSClientConfig.compGradDateOrTerm = 'D'

IMPLEMENTATION NOTES
The following changes were implemented for the 2020-21 data collection period:

	- There is a new distance education question on the CIP Data screen.
	- Subbaccalaureate certificates that are less than one year in length have been segmented into two subcategories based on duration.
	- There is a new FAQ elaborating on certificates that should be included in this component.                   

********************/

WITH DefaultValues as (
--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.
--IPEDS specs define reporting period as July 1, 2018 and June 30, 2019.

--jh 20201130 Changed to match fields and formatting of other surveys

--prod default blocks (2)
select '2021' surveyYear, 
	'E1D' surveyId,  
	'Full Year Term End' repPeriodTag1,
	'Full Year June End' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2019-07-01' AS DATE) reportingDateStart,
	CAST('2020-06-30' AS DATE) reportingDateEnd,
	'202010' termCode, --Fall 2019
	'1' partOfTermCode, 
	CAST('2019-10-15' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
	'T' compGradDateOrTerm --D = Date, T = Term
--***** end survey-specific mods

union

select '2021' surveyYear, 
	'E1D' surveyId,  
	'Full Year Term End' repPeriodTag1,
	'Full Year June End' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
	CAST('2019-07-01' AS DATE) reportingDateStart,
	CAST('2020-06-30' AS DATE) reportingDateEnd,
	'201930' termCode, --Summer 2019
	'1' partOfTermCode, 
	CAST('2019-06-10' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
	'T' compGradDateOrTerm --D = Date, T = Term
--***** end survey-specific mods

/*
select '1415' surveyYear,  
	'E1D' surveyId,  
	'Full Year Term End' repPeriodTag1,
	'Full Year June End' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201410' termCode,
	'1' partOfTermCode,
	CAST('2013-09-13' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
	'T' compGradDateOrTerm --D = Date, T = Term
--***** end survey-specific mods
 
union 

select '1415' surveyYear,  
	'E1D' surveyId,  
	'Full Year Term End' repPeriodTag1,
	'Full Year June End' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,   
	CAST('2013-07-01' as DATE) reportingDateStart,
    CAST('2014-06-30' as DATE) reportingDateEnd, 
	'201330' termCode,
	'1' partOfTermCode,
	CAST('2013-06-10' AS DATE) censusDate,
	'M' genderForUnknown,   --'Valid values: M = Male, F = Female; Default value (if no record or null value): M'
	'F' genderForNonBinary, --'Valid values: M = Male, F = Female; Default value (if no record or null value): F'
    'CR' instructionalActivityType, --'Valid values: CR = Credit, CL = Clock, B = Both; Default value (if no record or null value): CR'
    'A' acadOrProgReporter, --'Valid values: A = Academic, H = Hybrid, P = Program; Default value (if no record or null value): A'
    'U' publicOrPrivateInstitution, --'Valid values: U = Public, R = Private; Default value (if no record or null value): U'
    'Y' icOfferUndergradAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferGraduateAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
    'Y' icOfferDoctorAwardLevel, --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
--***** start survey-specific mods
	'T' compGradDateOrTerm --D = Date, T = Term
--***** end survey-specific mods
*/
),

ReportingPeriodMCR as (
--Returns applicable term/part of term codes for this survey submission year. 

--  1st union 1st order - pull snapshot for defvalues.repPeriodTag1 
--  1st union 2nd order - pull snapshot for defvalues.repPeriodTag2
--  1st union 3rd order - pull other snapshot, ordered by snapshotDate desc
--  2nd union - pull default values if no record in IPEDSReportingPeriod

--jh 20201130 set default value of surveySection field for surveys like Completions that do not have section values;
--				added repPeriodTag fields to capture values for other views, as needed

select distinct RepDates.surveyYear	surveyYear,
    RepDates.source source,
    coalesce(upper(RepDates.surveySection), 'COHORT') surveySection,
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
    RepDates.termCode termCode,	
	RepDates.partOfTermCode partOfTermCode,
    to_date(RepDates.censusDate,'YYYY-MM-DD') censusDate,
	to_date(RepDates.reportingDateStart,'YYYY-MM-DD') reportingDateStart,
    to_date(RepDates.reportingDateEnd,'YYYY-MM-DD') reportingDateEnd,
    RepDates.repPeriodTag1 repPeriodTag1,
	RepDates.repPeriodTag2 repPeriodTag2
from (
    select repperiodENT.surveyCollectionYear surveyYear,
	    'IPEDSReportingPeriod' source,
		repperiodENT.snapshotDate snapshotDate,
		repPeriodENT.surveyId surveyId,
		repPeriodENT.surveySection surveySection,
		coalesce(repperiodENT.reportingDateStart, defvalues.reportingDateStart) reportingDateStart,
		coalesce(repperiodENT.reportingDateEnd, defvalues.reportingDateEnd) reportingDateEnd,
		coalesce(repperiodENT.termCode, defvalues.termCode) termCode,
		coalesce(repperiodENT.partOfTermCode, defvalues.partOfTermCode) partOfTermCode,
		defvalues.censusDate censusDate,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
		row_number() over (	
			partition by 
				repPeriodENT.surveyCollectionYear,
                repPeriodENT.surveyId,
                repPeriodENT.surveySection, 
				repperiodENT.termCode,
				repperiodENT.partOfTermCode	
			order by 
			    (case when array_contains(repperiodENT.tags, defvalues.repPeriodTag1) then 1
                     when array_contains(repperiodENT.tags, defvalues.repPeriodTag2) then 2
			         else 3 end) asc,
			     repperiodENT.snapshotDate desc,
                repperiodENT.recordActivityDate desc	
		) reportPeriodRn	
		from IPEDSReportingPeriod repperiodENT
		    inner join DefaultValues defvalues on repperiodENT.surveyId = defvalues.surveyId
	            and repperiodENT.surveyCollectionYear = defvalues.surveyYear
	    where repperiodENT.termCode is not null
		and repperiodENT.partOfTermCode is not null
--***** start survey-specific mods - Completions must use one of two snapshots
        and (array_contains(repperiodENT.tags, defvalues.repPeriodTag1)
            or array_contains(repperiodENT.tags, defvalues.repPeriodTag2))
--***** end survey-specific mods

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
		defvalues.censusDate censusDate,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
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

--jh 20201130 added repPeriodTag fields to capture values for other views, as needed

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.source source,
    to_date(ConfigLatest.snapshotDate,'YYYY-MM-DD') snapshotDate,
    ConfigLatest.repperiodSnapshotDate repperiodSnapshotDate,
	upper(ConfigLatest.genderForUnknown) genderForUnknown,
	upper(ConfigLatest.genderForNonBinary) genderForNonBinary,
    upper(ConfigLatest.instructionalActivityType) instructionalActivityType,
    upper(ConfigLatest.acadOrProgReporter) acadOrProgReporter,
    upper(ConfigLatest.publicOrPrivateInstitution) publicOrPrivateInstitution,
    upper(ConfigLatest.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
    upper(ConfigLatest.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
    upper(ConfigLatest.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
	ConfigLatest.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods - Completions must use one of two snapshots
    upper(ConfigLatest.compGradDateOrTerm) compGradDateOrTerm
--***** end survey-specific mods
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'configFullYearTag' source,
		clientConfigENT.snapshotDate snapshotDate, 
		repperiod.snapshotDate repperiodSnapshotDate,
		coalesce(clientConfigENT.genderForUnknown, defvalues.genderForUnknown) genderForUnknown,
		coalesce(clientConfigENT.genderForNonBinary, defvalues.genderForNonBinary) genderForNonBinary,
        coalesce(clientConfigENT.instructionalActivityType, defvalues.instructionalActivityType) instructionalActivityType,
        coalesce(clientconfigENT.acadOrProgReporter, defvalues.acadOrProgReporter) acadOrProgReporter,
        coalesce(clientconfigENT.publicOrPrivateInstitution, defvalues.publicOrPrivateInstitution) publicOrPrivateInstitution,
        coalesce(clientConfigENT.icOfferUndergradAwardLevel, defvalues.icOfferUndergradAwardLevel) icOfferUndergradAwardLevel,
		coalesce(clientConfigENT.icOfferGraduateAwardLevel, defvalues.icOfferGraduateAwardLevel) icOfferGraduateAwardLevel,
        coalesce(clientConfigENT.icOfferDoctorAwardLevel, defvalues.icOfferDoctorAwardLevel) icOfferDoctorAwardLevel,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
		coalesce(clientConfigENT.compGradDateOrTerm, defvalues.compGradDateOrTerm) compGradDateOrTerm,
--***** end survey-specific mods
		row_number() over (
			partition by
				clientConfigENT.surveyCollectionYear
			order by
			    (case when to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                (case when to_date(clientConfigENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(clientConfigENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
				clientConfigENT.recordActivityDate desc
		) configRn
	from IPEDSClientConfig clientConfigENT
	    inner join DefaultValues defvalues on clientConfigENT.surveyCollectionYear = defvalues.surveyYear
		inner join ReportingPeriodMCR repperiod on clientConfigENT.surveyCollectionYear = repperiod.surveyYear
--***** start survey-specific mods
where (array_contains(clientConfigENT.tags, defvalues.repPeriodTag1)
            or array_contains(clientConfigENT.tags, defvalues.repPeriodTag2))
--***** end survey-specific mods

    union

	select defvalues.surveyYear surveyYear,
	    'default' source,
	    CAST('9999-09-09' as DATE) snapshotDate,
	    null repperiodSnapshotDate,  
		defvalues.genderForUnknown genderForUnknown,
		defvalues.genderForNonBinary genderForNonBinary,
        defvalues.instructionalActivityType instructionalActivityType,
        defvalues.acadOrProgReporter acadOrProgReporter,
        defvalues.publicOrPrivateInstitution publicOrPrivateInstitution,
        defvalues.icOfferUndergradAwardLevel icOfferUndergradAwardLevel,
		defvalues.icOfferGraduateAwardLevel icOfferGraduateAwardLevel,
        defvalues.icOfferDoctorAwardLevel icOfferDoctorAwardLevel,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
        defvalues.compGradDateOrTerm compGradDateOrTerm,
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
	requiredFTClockHoursUG
from ( 
    select distinct acadtermENT.termCode, 
        row_number() over (
            partition by 
                acadTermENT.termCode,
                acadTermENT.partOfTermCode
            order by
	    		(case when clientconfig.compGradDateOrTerm = 'D' and array_contains(acadTermENT.tags, clientconfig.repPeriodTag2) then 1
					  when clientconfig.compGradDateOrTerm = 'T' and array_contains(acadTermENT.tags, clientconfig.repPeriodTag1) then 1
					  else 2 end) asc,
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
--***** start survey-specific mods
	    cross join (select first(compGradDateOrTerm) compGradDateOrTerm,
	                        first(repPeriodTag1) repPeriodTag1,
	                        first(repPeriodTag2) repPeriodTag2
	                   from ClientConfigMCR) clientconfig
--***** end survey-specific mods
	where acadtermENT.isIPEDSReportable = 1
	)
where acadTermRn = 1
),

AcademicTermOrder as (
-- Orders term codes based on date span and keeps the numeric value of the greatest term/part of term record. 

--jh 20201130 Added new fields to capture min start and max end date for terms;
--				changed row_number order by field to censusDate

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

--jh 20201130 Minimized to remove inline view since only one snapshot; default yearType to 'CY'

select 'CY' yearType,
		repperiod.surveySection surveySection,
		repperiod.termCode termCode,
        repperiod.partOfTermCode partOfTermCode,
        acadterm.snapshotDate snapshotDate,
        repperiod.snapshotDate repPeriodSSDate,
        acadterm.financialAidYear financialAidYear,
		coalesce(acadterm.censusDate, repperiod.censusDate) censusDate,
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
		clientconfig.instructionalActivityType instructionalActivityType,
	    coalesce(acadterm.requiredFTCreditHoursUG/
		    coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
		clientconfig.repPeriodTag1 repPeriodTag1,
		clientconfig.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
        clientconfig.compGradDateOrTerm compGradDateOrTerm,
        (case when clientconfig.compGradDateOrTerm = 'D' then repperiod.reportingDateStart
              else termorder.minStart
	    end) compReportingDateStart,  --minimum start date of reporting for either date or term option for client
        (case when clientconfig.compGradDateOrTerm = 'D' then repperiod.reportingDateEnd
              else termorder.maxEnd --termMaxEndDate
	    end) compReportingDateEnd  --maximum end date of reporting for either date or term option for client
--***** end survey-specific mods
     from ReportingPeriodMCR repperiod 
        left join AcademicTermMCR acadterm on repperiod.termCode = acadterm.termCode
	            and repperiod.partOfTermCode = acadterm.partOfTermCode
		left join AcademicTermOrder termorder on termOrder.termCode = repperiod.termCode
		inner join ClientConfigMCR clientconfig on repperiod.surveyYear = clientconfig.surveyYear
),

AcademicTermReportingRefactor as (
--Returns all records from AcademicTermReporting, converts Summer terms to Pre-Fall or Post-Spring and creates reportingDateStart/End

--jh 20201130 Added maxTermOrder field; Removed reportingDateStart and End fields, since they were added to AcademicTermOrder
--jh 20201007 View created to return new Summer termTypes and reportingDateStart/End

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

--jh 20201130 Reordered fields for consistency and accuracy; other minor mods

CampusMCR as ( 
-- Returns most recent campus record for all snapshots in the ReportingPeriod
-- We only use campus for international status. We are maintaining the ability to look at a campus at different points in time through relevant snapshots. 
-- Will there ever be a case where a campus changes international status? 
-- Otherwise, we could just get all unique campus codes and forget about when the record was made.

-- jh 20200911 Removed ReportingPeriodMCR reference and changed filter date from regper.reportingperiodend to campusENT.snapshotDate 
--             Removed snapshotDate in partition by

select campus,
	isInternational,
	snapshotDate
from ( 
    select upper(campusENT.campus) campus,
		campusENT.campusDescription,
		campusENT.isInternational,
		to_date(campusENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
		row_number() over (
			partition by
			    campusENT.campus
			order by
				campusENT.recordActivityDate desc
		) campusRn
	from Campus campusENT 
        inner join AcademicTermReportingRefactor acadterm on acadterm.snapshotDate = to_date(campusENT.snapshotDate,'YYYY-MM-DD')
	where campusENT.isIpedsReportable = 1 
		and ((to_date(campusENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
			and to_date(campusENT.recordActivityDate,'YYYY-MM-DD') <= to_date(campusENT.snapshotDate,'YYYY-MM-DD'))
				or to_date(campusENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
	)
where campusRn = 1
),

DegreeProgramMCR as (
-- Returns all degreeProgram values at the end of the reporting period

--jh 20201130 Moved DegreeProgram view before Degree and FieldOfStudy

select *
from (
        select upper(programENT.degreeProgram) degreeProgram,
                programENT.degreeLevel degreeLevel,
                upper(programENT.degree) degree,
                upper(programENT.major) major,
                upper(programENT.college) college,
                upper(programENT.campus) campus,
                coalesce(campus.isInternational, false) isInternational,
                upper(programENT.department) department,
                to_date(programENT.startDate, 'YYYY-MM-DD') startDate,
                to_date(programENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
                programENT.termCodeEffective termCodeEffective,
                termorder.termOrder termorder,
                coalesce(programENT.distanceEducationOption, 'No DE option') distanceEducationOption,
                to_date(programENT.snapshotDate, 'YYYY-MM-DD') snapshotDate,
                repperiod.compReportingDateEnd compReportingDateEnd,
                row_number() over (
                    partition by
                        programENT.degreeProgram,
                        programENT.degreeLevel,
                        programENT.degree,
                        programENT.major,
                        programENT.campus,
                        programENT.distanceEducationOption
                    order by
                        programENT.recordActivityDate desc
                ) programRN
        from AcademicTermReportingRefactor repperiod 
            inner join DegreeProgram programENT on repperiod.snapshotDate = to_date(programENT.snapshotDate, 'YYYY-MM-DD')
                and ((programENT.startDate is not null and to_date(programENT.startDate,'YYYY-MM-DD') <= repperiod.compReportingDateEnd)
                        or programENT.startDate is null)
                and programENT.isIPEDSReportable = 1
                and programENT.isForStudentAcadTrack = 1
                and programENT.isForDegreeAcadHistory = 1
                and ((to_date(programENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                    and to_date(programENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.compReportingDateEnd)
                        or to_date(programENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
                and ((to_date(programENT.startDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
                    and to_date(programENT.startDate,'YYYY-MM-DD') <= repperiod.compReportingDateEnd)
                        or to_date(programENT.startDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
            left join AcademicTermOrder termorder on termorder.termCode = programENT.termCodeEffective
                and ((programENT.termCodeEffective is not null and termorder.termOrder <= repperiod.maxTermOrder)
                    or programENT.termCodeEffective is null)
            left join CampusMCR campus on upper(programENT.campus) = campus.campus
        )
where programRn = 1
   and isInternational = false
),

DegreeMCR as (
-- Pulls degree information as of the reporting period

--jh 20201130 Moved and renamed view; added filter to remove records with no awardLevel; 
--              Fixed the enum values for distanceEducationOption

select degreeProgram,
        degreeLevel,
        degree,
        awardLevel,
        major,
        campus,
        snapshotDate,
        compReportingDateEnd,
        (case when distanceEducationOption = 'No DE option' then 1 else 0 end) DENotDistanceEd,
		(case when distanceEducationOption in ('DE with no onsite component', 'DE with mandatory onsite component', 'DE with non-mandatory onsite component') then 1 else 0 end) DEisDistanceEd,
		(case when distanceEducationOption = 'DE with no onsite component' then 1 else 0 end) DEEntirelyDistanceEd,
		(case when distanceEducationOption = 'DE with mandatory onsite component' then 1 else 0 end) DEMandatoryOnsite,
		(case when distanceEducationOption = 'DE with non-mandatory onsite component' then 1 else 0 end) DEMOptionalOnsite
from (
    select degprog.degreeProgram degreeProgram,
            degprog.degreeLevel degreeLevel,
            degprog.degree degree,
            degprog.major major,
--removed college and department - these could possibly be needed for certain clients
            degprog.campus campus,
			degprog.distanceEducationOption distanceEducationOption,
			degprog.snapshotDate snapshotDate,
            degprog.compReportingDateEnd compReportingDateEnd,
            degreeENT.awardLevel awardLevel,
		row_number() over (
			partition by
			    degprog.degreeProgram,
			    degprog.degreeLevel,
				degreeENT.awardLevel,
                degprog.degree,
                degprog.major,
                degprog.campus,
				degprog.distanceEducationOption
			order by
			    degreeENT.recordActivityDate desc
		) as degreeRn
	from DegreeProgramMCR degprog
        left join Degree degreeENT on degprog.snapshotDate = to_date(degreeENT.snapshotDate,'YYYY-MM-DD') 
            and degprog.degree = upper(degreeENT.degree)
            and degprog.degreeLevel = degreeENT.degreeLevel
        and degreeENT.isIpedsReportable = 1
		and  degreeENT.isNonDegreeSeeking = 0
		and ((to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
			and to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') <= degprog.compReportingDateEnd)
				or to_date(degreeENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
   )
where degreeRn = 1
    and awardLevel is not null
),

FieldOfStudyMCR as ( 
-- Pulls major information as of the reporting period and joins to the degrees on curriculumRule

--jh 20201130 Moved and renamed view; added filter to remove records with no cipCode

select *
from (
    select degprog.degreeProgram degreeProgram,
            degprog.degreeLevel degreeLevel,
            degprog.degree degree,
            degprog.major major,
            degprog.campus campus,
			degprog.snapshotDate snapshotDate,
            degprog.compReportingDateEnd compReportingDateEnd,
            degprog.awardLevel awardLevel,
            degprog.DENotDistanceEd,
		    degprog.DEisDistanceEd,
		    degprog.DEEntirelyDistanceEd,
		    degprog.DEMandatoryOnsite,
		    degprog.DEMOptionalOnsite,
            CONCAT(LPAD(SUBSTR(CAST(fosENT.cipCode as STRING), 1, 2), 2, '0'), '.', RPAD(SUBSTR(CAST(fosENT.cipCode as STRING), 3, 6), 4, '0')) cipCode,
            row_number() over (
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
            ) as fosRn
       from DegreeMCR degprog
            left join FieldOfStudy fosENT on degprog.snapshotDate = to_date(fosENT.snapshotDate,'YYYY-MM-DD')
                and degprog.major = upper(fosENT.fieldOfStudy)
                and fosENT.fieldOfStudyType = 'Major'
                and ((to_date(fosENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
                        and to_date(fosENT.recordActivityDate,'YYYY-MM-DD') <= degprog.compReportingDateEnd)
                    or to_date(fosENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
		        and SUBSTR(CAST(fosENT.cipCodeVersion as STRING), 3, 2) >= 20 -- Current CIPCode standard was released 2020. new specs introduced per decade.
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
            repperiod.surveySection surveySection, 
            repperiod.snapshotDate termSSD,
            repperiod.genderForUnknown genderForUnknown,
            repperiod.genderForNonBinary genderForNonBinary,
            repperiod.reportingDateStart reportingDateStart,
		    repperiod.reportingDateEnd reportingDateEnd,
            repperiod.compReportingDateStart compReportingDateStart,
		    repperiod.compReportingDateEnd compReportingDateEnd,
		    repperiod.termCode termCode,
		    termorder.termOrder termOrder,
		    repperiod.maxCensus maxCensus,
            repperiod.compGradDateOrTerm compGradDateOrTerm,
            to_date(awardENT.snapshotDate,'YYYY-MM-DD') snapshotDate,
            to_date(awardENT.awardedDate, 'YYYY-MM-DD') awardedDate, 
            awardENT.awardedTermCode awardedTermCode, 
            upper(awardENT.degree) degree, 
            awardENT.degreeLevel degreeLevel, 
            awardENT.awardStatus awardStatus, 
            upper(awardENT.college) college, 
            upper(awardENT.campus) campus,
            coalesce(campus.isInternational, false) isInternational,
            row_number() over (
                partition by
                    awardENT.personId,
                    awardENT.awardedDate,
                    awardENT.degreeLevel,
                    awardENT.degree
                order by
                	awardENT.recordActivityDate desc
            ) as awardRn
	from AcademicTermReportingRefactor repperiod 
	    inner join Award awardENT on repperiod.snapshotDate = to_date(awardENT.snapshotDate,'YYYY-MM-DD')
	        and ((repperiod.compGradDateOrTerm = 'D' and to_date(awardENT.awardedDate,'YYYY-MM-DD') between 
                        to_date(repperiod.compReportingDateStart,'YYYY-MM-DD') and to_date(repperiod.compReportingDateEnd,'YYYY-MM-DD'))
		 			or (repperiod.compGradDateOrTerm = 'T' and awardENT.awardedTermCode = repperiod.termCode))
            and awardENT.isIpedsReportable = 1
		    and awardENT.awardStatus = 'Awarded'
		    and awardENT.degreeLevel is not null
		    and awardENT.awardedDate is not null
		    and awardENT.degreeLevel != 'Continuing Ed'
		    and ((to_date(awardENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' as DATE)
		            and to_date(awardENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.reportingDateEnd)
		    	or to_date(awardENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' as DATE))
		inner join AcademicTermOrder termorder on termorder.termCode = awardENT.awardedTermCode
        left join CampusMCR campus on upper(awardENT.campus) = campus.campus
		) awardData
where awardData.awardRn = 1
    and awardData.isInternational = false
),

PersonMCR as (
--Returns most up to date student personal information as of the reporting term codes and part of term census periods. 

--jh 20201130 Include new visaType field and code for ipedsEthnicity

select pers.personId personId,
        pers.yearType yearType,
        pers.awardedTermCode awardedTermCode,
        pers.awardedDate awardedDate,
        pers.degree degree, 
        pers.degreeLevel degreeLevel,
        pers.college college, 
        pers.campus campus,
        pers.snapshotDate snapshotDate,
        pers.surveySection surveySection,
        pers.reportingDateEnd reportingDateEnd,
        pers.termOrder termOrder,
        (case when pers.gender = 'Male' then 'M'
            when pers.gender = 'Female' then 'F' 
            when pers.gender = 'Non-Binary' then pers.genderForNonBinary
            else pers.genderForUnknown
        end) ipedsGender,
        (case when pers.isUSCitizen = 1 or ((pers.isInUSOnVisa = 1 or pers.awardedDate between pers.visaStartDate and pers.visaEndDate)
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
            when ((pers.isInUSOnVisa = 1 or pers.awardedDate between pers.visaStartDate and pers.visaEndDate)
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
            award.awardedTermCode awardedTermCode,
            award.awardedDate awardedDate,
            award.degree degree, 
            award.degreeLevel degreeLevel,
            award.college college, 
            award.campus campus,
            to_date(award.snapshotDate,'YYYY-MM-DD') snapshotDate,
            award.surveySection surveySection,
            award.termOrder termOrder, 
            award.genderForUnknown genderForUnknown,
            award.genderForNonBinary genderForNonBinary,
		    award.reportingDateEnd reportingDateEnd,
            award.compGradDateOrTerm compGradDateOrTerm,
            floor(DATEDIFF(award.awardedDate, to_date(personENT.birthDate,'YYYY-MM-DD')) / 365) asOfAge,
            to_date(personENT.birthDate,'YYYY-MM-DD') birthDate,
            personENT.ethnicity ethnicity,
            personENT.isHispanic isHispanic,
            personENT.isMultipleRaces isMultipleRaces,
            personENT.isInUSOnVisa isInUSOnVisa,
            to_date(personENT.visaStartDate,'YYYY-MM-DD') visaStartDate,
            to_date(personENT.visaEndDate,'YYYY-MM-DD') visaEndDate,
            personENT.visaType visaType,
            personENT.isUSCitizen isUSCitizen,
            personENT.gender gender,
            upper(personENT.nation) nation,
            upper(personENT.state) state,
            row_number() over (
                partition by
                    award.personId,
                    personENT.personId,
                    award.yearType,
                    award.awardedDate,
				    award.degreeLevel,
				    award.degree
                order by
                     personENT.recordActivityDate desc
                ) personRn
    from AwardMCR award 
        left join Person personENT on award.personId = personENT.personId
            and award.snapshotDate = to_date(personENT.snapshotDate,'YYYY-MM-DD')
            and personENT.isIpedsReportable = 1
            and ((to_date(personENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
               and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= award.awardedDate) 
                or to_date(personENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
    ) pers
where pers.personRn = 1
),

AcademicTrackMCR as (
--Returns most up to date student academic track information as of their award date and term. 

--jh 20201130 Modified academicTrackStatus values to pull both 'Completed' and 'In Progress' but prioritize 'Completed'

select personId personId,
    yearType yearType,
	degree degree,
	college college,
	campus campus,
	snapshotDate snapshotDate,
	degreeLevel degreeLevel,
	awardedDate awardedDate,
	awardedTermCode awardedTermCode,
	termOrder termOrder,
	ipedsGender ipedsGender,
    ipedsEthnicity ipedsEthnicity,
    ipedsAgeGroup ipedsAgeGroup,
	termCodeEffective termCodeEffective,
	degreeProgram degreeProgram,
	academicTrackLevel academicTrackLevel,
	fieldOfStudy fieldOfStudy,
	row_number() over (
		partition by
			personId,
			awardedDate,
			degree
		order by
			fieldOfStudyPriority asc
	) fosRn
from ( 
    select person2.personId personId,
        person2.yearType yearType,
        person2.awardedDate awardedDate,
		person2.awardedTermCode awardedTermCode,
		person2.termOrder termOrder,
		person2.snapshotDate snapshotDate,
		person2.ipedsGender ipedsGender,
        person2.ipedsEthnicity ipedsEthnicity,
        person2.ipedsAgeGroup ipedsAgeGroup,
		person2.degree degree,
		person2.college college,
		coalesce(person2.campus, AcadTrackRec.campus) campus,
        person2.degreeLevel degreeLevel,
		AcadTrackRec.termCodeEffective termCodeEffective,
		AcadTrackRec.degreeProgram degreeProgram,
		AcadTrackRec.academicTrackLevel academicTrackLevel,
        AcadTrackRec.fieldOfStudy fieldOfStudy,
		AcadTrackRec.fieldOfStudyType fieldOfStudyType,
		AcadTrackRec.fieldOfStudyPriority fieldOfStudyPriority,
		AcadTrackRec.academicTrackStatus academicTrackStatus,
        AcadTrackRec.fieldOfStudyActionDate fieldOfStudyActionDate,
        to_date(AcadTrackRec.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
		AcadTrackRec.termOrder acadTrackTermOrder,
		row_number() over (
			partition by
			    person2.personId,
				AcadTrackRec.personId,
				person2.yearType,
				person2.awardedDate,
                person2.degreeLevel,
                person2.degree,
				AcadTrackRec.degreeProgram,
				AcadTrackRec.fieldOfStudyPriority
			order by
			    (case when AcadTrackRec.academicTrackStatus = 'Completed' then 1 else 2 end) asc,
			    AcadTrackRec.termOrder desc,
			    AcadTrackRec.fieldOfStudyActionDate desc,
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
                    and person.snapshotDate = to_date(acadTrackENT.snapshotDate,'YYYY-MM-DD')
                    and person.degree = upper(acadTrackENT.degree)
                    and (person.college = upper(acadTrackENT.college)
                        or acadTrackENT.college is null
                        or person.college is null)
                    and (person.campus = upper(acadTrackENT.campus)
                        or acadTrackENT.campus is null
                        or person.campus is null)
                    and person.degreeLevel = acadTrackENT.academicTrackLevel
                    and acadTrackENT.fieldOfStudyType = 'Major'
                    and acadTrackENT.fieldOfStudy is not null
                    and acadTrackENT.academicTrackStatus in ('Completed', 'In Progress')
                    and acadTrackENT.isIpedsReportable = 1
					and acadTrackENT.isCurrentFieldOfStudy = 1
                inner join AcademicTermOrder termorder on termorder.termCode = acadtrackENT.termCodeEffective
            ) AcadTrackRec 
				on person2.personId = AcadTrackRec.personId
                    and person2.degree = AcadTrackRec.degree
			        and (person2.college = AcadTrackRec.college
                        or AcadTrackRec.college is null
                        or person2.college is null)
                    and (person2.campus = AcadTrackRec.campus
                        or AcadTrackRec.campus is null
                        or person2.campus is null)
			        and person2.degreeLevel = AcadTrackRec.academicTrackLevel
-- Added to use check the date for when the academicTrack record was made active. Use termCodeEffective if
-- the dummy date is being used. 
                    and ((to_date(AcadTrackRec.fieldOfStudyActionDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
					        and to_date(AcadTrackRec.fieldOfStudyActionDate, 'YYYY-MM-DD') <= person2.reportingDateEnd)
                        or (to_date(AcadTrackRec.fieldOfStudyActionDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)
                            and  AcadTrackRec.termOrder <= person2.termOrder))
-- Maintaining activityDate filtering from prior versions.					
                    and ((to_date(AcadTrackRec.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
				        and to_date(AcadTrackRec.recordActivityDate, 'YYYY-MM-DD') <= person2.reportingDateEnd)
                            or to_date(AcadTrackRec.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS DATE))
	)
where acadtrackRn = 1
),

Completer as (
--Returns all completer records to use for Parts C & D

--jh 20201130 Created view to pull all completers and assign ACAT value for part D

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
       (case when IPEDSethnicity = '9' and IPEDSGender = 'F' then 1 else 0 end) reg18,  --(CRACE14) - Race and ethnicity unknown, F, (0 to 999999)
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
            acadtrack.academicTrackLevel academicTrackLevel,
            acadtrack.degreeProgram degreeProgram,
            acadtrack.degree degree,
            acadtrack.fieldOfStudy fieldOfStudy,
            acadtrack.campus campus,
            acadtrack.personId personId,
            acadtrack.ipedsGender ipedsGender,
            acadtrack.ipedsEthnicity ipedsEthnicity,
            acadtrack.ipedsAgeGroup ipedsAgeGroup,
            degprog.awardLevel awardLevel,
            degprog.cipCode cipCode
    from AcademicTrackMCR acadtrack
        inner join FieldOfStudyMCR degprog on degprog.degreeLevel = acadtrack.academicTrackLevel
                    and degprog.degreeProgram = acadtrack.degreeProgram
                    and degprog.degree = acadtrack.degree
                    and degprog.major = acadtrack.fieldOfStudy
                    and (degprog.campus = acadtrack.campus
                            or acadtrack.campus is null
                            or degprog.campus is null)
    where acadtrack.fosRn < 3
    )
),

DegreeFOSProgramSTU as (
-- Pulls all CIPCodes and ACAT levels including student awarded levels to use for Parts A & B

--jh 20201130   Minimized views, so ACATPartAB, ACATPartD and the DE fields were relocated here

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
        case when DEisDistanceEd = totalPrograms then 1 -- All programs in this CIP code in this award level can be completed entirely via distance education.
             when DEisDistanceEd = 0 then 2 -- None of the programs in this CIP code in this award level can be completed entirely via distance education.
             when totalPrograms > DEisDistanceEd and DEisDistanceEd > 0 then 3 -- Some programs in this CIP code in this award level can be completed entirely via distance education.
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
        end DESomeOptionalOnsite,
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
            SUM(DEMandatoryOnsite) DEMandatoryOnsite,
            SUM(DEMOptionalOnsite) DEMOptionalOnsite,
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
                --degfosprog.campus campus,
                degfosprog.DENotDistanceEd DENotDistanceEd,
                degfosprog.DEisDistanceEd DEisDistanceEd,
                degfosprog.DEEntirelyDistanceEd DEEntirelyDistanceEd,
                degfosprog.DEMandatoryOnsite DEMandatoryOnsite,
                degfosprog.DEMOptionalOnsite DEMOptionalOnsite,
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
            and ACATPartAB in ('3', '5', '7', '17', '18', '19'))) --1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2
    and cipCode not in ('06.0101','06.0201','15.3402','18.1101','23.0200','23.0102','42.0102')

union

--Part B: Completions - Distance Education
--Used to distinguish if an award is offered in a distance education format. A "distance education program" is "a program for which all the required coursework 
--for program completion is able to be completed via distance education courses."
--valid ACAT values are 1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2

select 'B', 
       majorNum,             --(MAJORNUM)	- 1 = First Major, 2 = Second Major
       cipCode,              --(CIPCODE)		- xx.xxxx, valid CIP codes, (must have leading zero if applicable)
       ACATPartAB,           --(AWLEVEL)		- 1a, 1b, 2 to 8 and 17 to 19 for MAJORNUM=1; 3, 5, 7, 17, 18, 19 for MAJORNUM=2;
       DEAvailability,       --(DistanceED)  - Is at least one program within this CIP code offered as a distance education program? (1 = All, 2 = Some, 3 = None)
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
            first(reg1) field1,     --(CRACE01) 	- Nonresident Alien, M, (0 to 999999)
            first(reg2) field2,      --(CRACE02) 	- Nonresident Alien, F, (0 to 999999)
            first(reg3) field3,      --(CRACE25) 	- Hispanic/Latino, M, (0 to 999999)					
            first(reg4) field4,      --(CRACE26) 	- Hispanic/Latino, F, (0 to 999999) 
            first(reg5) field5,      --(CRACE27) 	- American Indian or Alaska Native, M, (0 to 999999)
            first(reg6) field6,      --(CRACE28) 	- American Indian or Alaska Native, F, (0 to 999999)
            first(reg7) field7,      --(CRACE29) 	- Asian, M, (0 to 999999)
            first(reg8) field8,      --(CRACE30) 	- Asian, F, (0 to 999999)
            first(reg9) field9,      --(CRACE31) 	- Black or African American, M, (0 to 999999)
            first(reg10) field10,    --(CRACE32) 	- Black or African American, F, (0 to 999999)
            first(reg11) field11,    --(CRACE33) 	- Native Hawaiian or Other Pacific Islander, M, (0 to 999999)
            first(reg12) field12,    --(CRACE34) 	- Native Hawaiian or Other Pacific Islander, F, (0 to 999999)
            first(reg13) field13,    --(CRACE35) 	- White, M, (0 to 999999)
            first(reg14) field14,    --(CRACE36) 	- White, F, (0 to 999999)
            first(reg15) field15,    --(CRACE37) 	- Two or more races, M, (0 to 999999)
            first(reg16) field16,    --(CRACE38) 	- Two or more races, F, (0 to 999999)
            first(reg17) field17,    --(CRACE13) 	- Race and ethnicity unknown, M, (0 to 999999)		
            first(reg18) field18     --(CRACE14) 	- Race and ethnicity unknown, F, (0 to 999999) 
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
--Completers level during academic year 2019-20 but the institution still offers the program at that completers level. 

select 'D', 
       null,
       null,
       ACATPartD ACATPartD,  --CTLEVEL - 2 to 9.
       null,
       coalesce(SUM(field_1), 0),   --(CRACE15)	- Men, 0 to 999999
       coalesce(SUM(field_2), 0),   --(CRACE16) - Women, 0 to 999999 
       coalesce(SUM(field_3), 0),   --(CRACE17) - Nonresident Alien, 0 to 999999
       coalesce(SUM(field_4), 0),   --(CRACE41) - Hispanic/Latino, 0 to 999999
       coalesce(SUM(field_5), 0),   --(CRACE42) - American Indian or Alaska Native, 0 to 999999
       coalesce(SUM(field_6), 0),   --(CRACE43) - Asian, 0 to 999999
       coalesce(SUM(field_7), 0),   --(CRACE44) - Black or African American, 0 to 999999
       coalesce(SUM(field_8), 0),   --(CRACE45) - Native Hawaiian or Other Pacific Islander, 0 to 999999
       coalesce(SUM(field_9), 0),   --(CRACE46) - White, 0 to 999999
       coalesce(SUM(field_10), 0),  --(CRACE47) - Two or more races, 0 to 999999
       coalesce(SUM(field_11), 0),  --(CRACE23) - Race and ethnicity unknown, 0 to 999999
       coalesce(SUM(field_12), 0),  --(AGE1) - Under 18, 0 to 999999
       coalesce(SUM(field_13), 0),  --(AGE2) - 18-24, 0 to 999999
       coalesce(SUM(field_14), 0),  --(AGE3) - 25-39, 0 to 999999
       coalesce(SUM(field_15), 0),  --(AGE4) - 40 and above, 0 to 999999
       coalesce(SUM(field_16), 0),  --(AGE5) - Age unknown, 0 to 999999
       null,
       null
from ( 
    select distinct ACATPartD,
           personId personId,
           (case when IPEDSGender = 'M' then 1 else 0 end) field_1,         --(CRACE15)	-- Men, 0 to 999999
           (case when IPEDSGender = 'F' then 1 else 0 end) field_2,         --(CRACE16) - Women, 0 to 999999 
           (case when IPEDSethnicity = '1' then 1 else 0 end) field_3,      --(CRACE17) - Nonresident Alien, 0 to 999999
           (case when IPEDSethnicity = '2' then 1 else 0 end) field_4,      --(CRACE41) - Hispanic/Latino, 0 to 999999
           (case when IPEDSethnicity = '3' then 1 else 0 end) field_5,      --(CRACE42) - American Indian or Alaska Native, 0 to 999999
           (case when IPEDSethnicity = '4' then 1 else 0 end) field_6,      --(CRACE43) - Asian, 0 to 999999
           (case when IPEDSethnicity = '5' then 1 else 0 end) field_7,      --(CRACE44) - Black or African American, 0 to 999999
           (case when IPEDSethnicity = '6' then 1 else 0 end) field_8,      --(CRACE45) - Native Hawaiian or Other Pacific Islander, 0 to 999999
           (case when IPEDSethnicity = '7' then 1 else 0 end) field_9,      --(CRACE46) - White, 0 to 999999
           (case when IPEDSethnicity = '8' then 1 else 0 end) field_10,     --(CRACE47) - Two or more races, 0 to 999999
           (case when IPEDSethnicity = '9' then 1 else 0 end) field_11,     --(CRACE23) - Race and ethnicity unknown, 0 to 999999
           (case when IPEDSAgeGroup = 'AGE1' then 1 else 0 end) field_12,   --(AGE1) - Under 18, 0 to 999999
           (case when IPEDSAgeGroup = 'AGE2' then 1 else 0 end) field_13,   --(AGE2) - 18-24, 0 to 999999
           (case when IPEDSAgeGroup = 'AGE3' then 1 else 0 end) field_14,   --(AGE3) - 25-39, 0 to 999999
           (case when IPEDSAgeGroup = 'AGE4' then 1 else 0 end) field_15,   --(AGE4) - 40 and above, 0 to 999999
           (case when IPEDSAgeGroup = 'AGE5' then 1 else 0 end) field_16    --(AGE5) - Age unknown, 0 to 999999 
    from Completer --cohortstu 
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

--jh 20201130 Separated dummy records since parts A and B are program related and C is student related; Part D combines the 
--              program and student records, but if there are no completers, the awardLevel values in A/B should still print
--              and show 0 
--             Uncommented part C since I switched the coalesce and sum functions to match the other parts
--             Changed all level values to 3 for Associates and have D matching A & B

--Dummy set to return default formatting if no student awards exist.
select *
from (
    VALUES
        ('A', 1, '01.0101', '3', null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ('B', 1, '01.0101', '3', 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        ('D', null, null, '3', null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null)
    ) as dummySet(PART, MAJORNUM, CIPC_LEVL, AWLEVEL, isDistanceED, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, 
                  FIELD10, FIELD11, FIELD12, FIELD13, FIELD14, FIELD15, FIELD16, FIELD17, FIELD18)
where not exists (select a.cipCode from DegreeFOSProgramSTU a) 

union

--Dummy set to return default formatting if no student awards exist.
select *
from (
    VALUES
        ('C', null, null, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    ) as dummySet(PART, MAJORNUM, CIPC_LEVL, AWLEVEL, isDistanceED, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, 
                  FIELD10, FIELD11, FIELD12, FIELD13, FIELD14, FIELD15, FIELD16, FIELD17, FIELD18)
where not exists (select a.personId from AcademicTrackMCR a) 
