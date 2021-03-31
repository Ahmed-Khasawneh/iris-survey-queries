/********************

EVI PRODUCT:    DORIS 2020-21 IPEDS Survey Spring Collection
FILE NAME:      Finance v1 (F1B) 
FILE DESC:      Finance for degree-granting public institutions using GASB Reporting Standards
AUTHOR:         Janet Hanicak
CREATED:        20191219

SECTIONS:
       Reporting Dates 
       Most Recent Records 
       Survey Formatting  
    
SUMMARY OF CHANGES

Date(yyyymmdd)      Author              Tag              Comments
-----------------   ----------------    -------------    ----------------------------------------------------------------------------------
20210325			ckeller								 Modified Reporting Dates and MCR sections to accommodate multiple snapshots
														 Added new rows in Survey Formatting section required for 2021 survey collection
20200622			akhasawneh			ak 20200622		 Modify Finance report query with standardized view naming/aliasing convention (PF-1532) -Run time 8m 24s																			
20200412			jhanicak			jh 20204012		 Added dummy date option for recordActivityDate in most current record queries PF-1374
														 Added DefaultValues query and rewrote other queries to use PF-1418
														 Removed all 'prior' queries - not needed
20200306            jd.hysler           jdh 2020-03-30   Janet & JD discussed Long-Term Debt retired & acquired. 
                                                         Long-Term debt retired as Any long-term Accounts that had a balance greater than 0 
                                                         at the begining of FY and had a balance of 0 at the end of the FY. 
                                                         Long-Term debt acquired as Any long-term Accounts that did not exist or had a balance 
                                                         of 0 at the begining of FY and had a balance greater than 0 at the end of the FY.  
20200303            jd.hysler           jdh 2020-03-04   PF-1297 Modify Finance all versions to use latest records from finance entities 
                                                         - Removed FYPerAsOfDate/FYPerPriorAsOfDate
                                                         - Removed FiscalYearMCR/BothCOASPerFYPriorAsOfDate
                                                         - Move IPEDSClientConfig values into FiscalYearMCR/COASPerFYPriorAsOfDate
                                                         - Added GL cte for most recent record views for GeneralLedgerReporting
                                                         - Added GL_Prior cte for most recent record views for GeneralLedgerReporting for prior fiscal year
                                                         - Added OL cte for most recent record views for OperatingLedgerReporting
                                                         - Added OL_Prior cte for most recent record views for OperatingLedgerReporting for prior fiscal year
                                                         - Removed cross join with ClientConfigMCR since values are now already in FiscalYearMCR
                                                         - Changed PART Queries to use new in-line GL/OL Structure
                                                                --  1-Changed GeneralLedgerReporting to GL inline view  
                                                                --  2-Removed the join to COAS       
                                                                --  3-Removed GL.isIpedsReportable = 1 
                                                                --  4-move fiscalPeriod from join to where clause
                                                                --  5-added Parent/Child idenfier 
20200218            jhanicak            jh 20200218      Added Config values/conditionals for Parts M and H
20200127            jhanicak            jh 20200127      PF-1254 Added default values for IPEDSReportingPeriod and IPEDSClientConfig
20200107            jhanicak            jh 20200107      Added parenthesis to where clause
20200106            jd.hysler                            Move original code to template 

********************/

/*
add header back in --CHECK
DefaultValues - add comments value for each field from IPEDSClientConfig that includes all possible values and default value
Add 4 fields to each formatting section --CHECK

*/
WITH DefaultValues AS
(
 select '2021' surveyYear, -- 'YYyy (1920 for 2019-2020)'
	CAST('2020-09-30' AS DATE) asOfDate, 
	'F1B' surveyId,
	'Current' surveySection,
	'Fiscal Year Lockdown' repPeriodTag1, --used for all status updates and IPEDS tables
	null repPeriodTag2, --used to pull FA for cohort
    null repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2019-10-01' AS DATE) reportingDateStart,
	CAST('2020-09-30' AS DATE) reportingDateEnd,
	'20' termCode, --Fiscal Year
	'U' finGPFSAuditOpinion,  -- all versions -- 'Valid values: Q = Qualified, UQ = Unqualified, U = Unknown/In Progress; Default value (if no record or null value): U'
	'A' finAthleticExpenses,  -- all versions -- 'Valid values: A = Auxiliary Enterprises, S = Student Services, N = Does not participate, O = Other; Default value (if no record or null value): A'
	'Y' finEndowmentAssets,  -- all versions -- 'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
	'Y' finPensionBenefits,  -- all versions -- 'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
	'B' finReportingModel,  -- v1, v4 -- 'Valid values: B = Business type, G = Government, GB = Government with Business; Default value (if no record or null value): B'
	'P' finPellTransactions, -- v2, v3, v5, v6 -- 'Valid values: P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants; Default value (if no record or null value): P'
	'LLC' finBusinessStructure, -- v3, v6 -- 'Valid values: SP = Sole Proprietorship, P = Partnership, C = C Corp, SC = S Corp, LLC = LLC; Default value (if no record or null value): LLC'
	'M' finTaxExpensePaid, -- v6 -- 'Valid values: M = multi-institution or multi-campus organization indicated in IC Header, 
	                             -- N = multi-institution or multi-campus organization NOT indicated in IC Header, I = reporting institution; Default value (if no record or null value): M' 
	'P' finParentOrChildInstitution  -- v1, v2, v3 -- 'Valid values: P = Parent, C = Child; Default value (if no record or null value): P'
	
union

select '2021' surveyYear, -- 'YYyy (1920 for 2019-2020)'
	CAST('2020-09-30' AS DATE) asOfDate, 
	'F1B' surveyId,
	'Prior' surveySection,
	'Fiscal Year Lockdown' repPeriodTag1, --used for all status updates and IPEDS tables
	null repPeriodTag2, --used to pull FA for cohort
    null repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2019-10-01' AS DATE) reportingDateStart,
	CAST('2020-09-30' AS DATE) reportingDateEnd,
	'19' termCode, --Fiscal Year
	'U' finGPFSAuditOpinion,  -- all versions -- 'Valid values: Q = Qualified, UQ = Unqualified, U = Unknown/In Progress; Default value (if no record or null value): U'
	'A' finAthleticExpenses,  -- all versions -- 'Valid values: A = Auxiliary Enterprises, S = Student Services, N = Does not participate, O = Other; Default value (if no record or null value): A'
	'Y' finEndowmentAssets,  -- all versions -- 'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
	'Y' finPensionBenefits,  -- all versions -- 'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
	'B' finReportingModel,  -- v1, v4 -- 'Valid values: B = Business type, G = Government, GB = Government with Business; Default value (if no record or null value): B'
	'P' finPellTransactions, -- v2, v3, v5, v6 -- 'Valid values: P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants; Default value (if no record or null value): P'
	'LLC' finBusinessStructure, -- v3, v6 -- 'Valid values: SP = Sole Proprietorship, P = Partnership, C = C Corp, SC = S Corp, LLC = LLC; Default value (if no record or null value): LLC'
	'M' finTaxExpensePaid, -- v6 -- 'Valid values: M = multi-institution or multi-campus organization indicated in IC Header, 
	                             -- N = multi-institution or multi-campus organization NOT indicated in IC Header, I = reporting institution; Default value (if no record or null value): M' 
	'P' finParentOrChildInstitution  -- v1, v2, v3 -- 'Valid values: P = Parent, C = Child; Default value (if no record or null value): P'

/*
--used for internal testing only
select '1415' surveyYear, -- 'YYyy (1920 for 2019-2020)'
	CAST('2014-09-30' AS DATE) asOfDate, 
	'F1B' surveyId,
	'Current' surveySection,
	'Fiscal Year Lockdown' repPeriodTag1, --used for all status updates and IPEDS tables
	null repPeriodTag2, --used to pull FA for cohort
    null repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2013-10-01' AS DATE) reportingDateStart,
	CAST('2014-09-30' AS DATE) reportingDateEnd,
	'14' termCode, --Fiscal Year
	'U' finGPFSAuditOpinion,  -- all versions --'Valid values: Q = Qualified, UQ = Unqualified, U = Unknown/In Progress; Default value (if no record or null value): U'
	'A' finAthleticExpenses,  -- all versions -- 'Valid values: A = Auxiliary Enterprises, S = Student Services, N = Does not participate, O = Other; Default value (if no record or null value): A'
	'Y' finEndowmentAssets,  -- all versions --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
	'Y' finPensionBenefits,  -- all versions --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
	'B' finReportingModel,  -- v1, v4 --'Valid values: B = Business type, G = Government, GB = Government with Business; Default value (if no record or null value): B'
	'P' finPellTransactions, -- v2, v3, v5, v6 --'Valid values: P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants; Default value (if no record or null value): P'
	'LLC' finBusinessStructure, -- v3, v6 --'Valid values: SP = Sole Proprietorship, P = Partnership, C = C Corp, SC = S Corp, LLC = LLC; Default value (if no record or null value): LLC'
	'M' finTaxExpensePaid, -- v6 -- 'Valid values: M = multi-institution or multi-campus organization indicated in IC Header, 
	                             -- N = multi-institution or multi-campus organization NOT indicated in IC Header, I = reporting institution; Default value (if no record or null value): M' 
	'P' finParentOrChildInstitution  -- v1, v2, v3 -- 'Valid values: P = Parent, C = Child; Default value (if no record or null value): P'
	
union

select '1415' surveyYear, -- 'YYyy (1920 for 2019-2020)'
	CAST('2014-09-30' AS DATE) asOfDate,  
	'F1B' surveyId,
	'Prior' surveySection,
	'Fiscal Year Lockdown' repPeriodTag1, --used for all status updates and IPEDS tables
	null repPeriodTag2, --used to pull FA for cohort
    null repPeriodTag3, --used to pull transfer out data for last update
    null repPeriodTag4,
	CAST('9999-09-09' as DATE) snapshotDate,
	CAST('2013-10-01' AS DATE) reportingDateStart,
	CAST('2014-09-30' AS DATE) reportingDateEnd,
	'13' termCode, --Fiscal Year
	'U' finGPFSAuditOpinion,  -- all versions --'Valid values: Q = Qualified, UQ = Unqualified, U = Unknown/In Progress; Default value (if no record or null value): U'
	'A' finAthleticExpenses,  -- all versions -- 'Valid values: A = Auxiliary Enterprises, S = Student Services, N = Does not participate, O = Other; Default value (if no record or null value): A'
	'Y' finEndowmentAssets,  -- all versions --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
	'Y' finPensionBenefits,  -- all versions --'Valid values: Y = Yes, N = No; Default value (if no record or null value): Y'
	'B' finReportingModel,  -- v1, v4 --'Valid values: B = Business type, G = Government, GB = Government with Business; Default value (if no record or null value): B'
	'P' finPellTransactions, -- v2, v3, v5, v6 --'Valid values: P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants; Default value (if no record or null value): P'
	'LLC' finBusinessStructure, -- v3, v6 --'Valid values: SP = Sole Proprietorship, P = Partnership, C = C Corp, SC = S Corp, LLC = LLC; Default value (if no record or null value): LLC'
	'M' finTaxExpensePaid, -- v6 -- 'Valid values: M = multi-institution or multi-campus organization indicated in IC Header, 
	                             -- N = multi-institution or multi-campus organization NOT indicated in IC Header, I = reporting institution; Default value (if no record or null value): M' 
	'P' finParentOrChildInstitution  -- v1, v2, v3 -- 'Valid values: P = Parent, C = Child; Default value (if no record or null value): P'
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
    RepDates.surveySection surveySection,
    RepDates.fiscalYear fiscalYear,
    RepDates.asOfDate asOfDate,
    to_date(RepDates.snapshotDate,'YYYY-MM-DD') snapshotDate,
	RepDates.reportingDateStart reportingDateStart,
    RepDates.reportingDateEnd reportingDateEnd,
    RepDates.repPeriodTag1 repPeriodTag1,
	RepDates.repPeriodTag2 repPeriodTag2,
	RepDates.tags tags
from (
    select defvalues.asOfDate asOfDate,
		repperiodENT.surveyCollectionYear surveyYear,
		repperiodENT.termCode fiscalYear,
	    'IPEDSReportingPeriod' source,
		repperiodENT.snapshotDate snapshotDate,
		repPeriodENT.surveyId surveyId,
		coalesce(repperiodENT.surveySection, defvalues.surveySection) surveySection,
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
	    defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    repperiodENT.tags tags,
		row_number() over (	
			partition by 
				repPeriodENT.surveyCollectionYear,
                repPeriodENT.surveyId,
                repPeriodENT.surveySection, 
				repperiodENT.termCode
			order by 
			    (case when array_contains(repperiodENT.tags, defvalues.repPeriodTag1) then 1
                     else 2 end) asc,
			     repperiodENT.snapshotDate desc,
                repperiodENT.recordActivityDate desc	
		) reportPeriodRn	
	from IPEDSReportingPeriod repperiodENT
	    left join DefaultValues defvalues on repperiodENT.surveyId = defvalues.surveyId
	            and repperiodENT.surveySection = defvalues.surveySection
	where repperiodENT.termCode is not null --Finance does not use termCode, but fiscalYear in this field for F1B
	   and repperiodENT.surveyCollectionYear = defvalues.surveyYear
	   --and repperiodENT.surveyId = defvalues.surveyId
	   --and repperiodENT.surveySection = defvalues.surveySection

    union 
 
	select defvalues.asOfDate asOfDate,
		defvalues.surveyYear surveyYear,
		defvalues.termCode fiscalYear,
	    'DefaultValues' source,
		CAST('9999-09-09' as DATE) snapshotDate,
		defvalues.surveyId surveyId, 
		defvalues.surveySection surveySection,
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
	    null,
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

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

ClientConfigMCR as (
--Returns client customizations for this survey submission year. 

--  1st union 1st order - pull snapshot where same as ReportingPeriodMCR snapshotDate
--  1st union 2nd order - pull closet snapshot before ReportingPeriodMCR snapshotDate
--  1st union 3rd order - pull closet snapshot after ReportingPeriodMCR snapshotDate
--  2nd union - pull default values if no record in IPEDSClientConfig

select ConfigLatest.surveyYear surveyYear,
    ConfigLatest.source source,
    ConfigLatest.asOfDate asOfDate,
    to_date(ConfigLatest.snapshotDate,'YYYY-MM-DD') snapshotDate,
    ConfigLatest.repperiodSnapshotDate repperiodSnapshotDate,
	ConfigLatest.reportingDateStart reportingDateStart,
    ConfigLatest.reportingDateEnd reportingDateEnd,
    ConfigLatest.repPeriodTag1 repPeriodTag1,
	ConfigLatest.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
	ConfigLatest.finGPFSAuditOpinion finGPFSAuditOpinion,
	ConfigLatest.finAthleticExpenses finAthleticExpenses,
	ConfigLatest.finEndowmentAssets finEndowmentAssets,
	ConfigLatest.finPensionBenefits finPensionBenefits,
	ConfigLatest.finReportingModel finReportingModel,
	ConfigLatest.finParentOrChildInstitution finParentOrChildInstitution
--***** end survey-specific mods
from (
    select clientConfigENT.surveyCollectionYear surveyYear,
        'IPEDS Client Config' source,
        repperiod.asOfDate asOfDate,
		clientConfigENT.snapshotDate snapshotDate, 
		repperiod.snapshotDate repperiodSnapshotDate,
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,		
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
        coalesce(clientConfigENT.finGPFSAuditOpinion, defvalues.finGPFSAuditOpinion) finGPFSAuditOpinion,
		coalesce(clientConfigENT.finAthleticExpenses, defvalues.finAthleticExpenses) finAthleticExpenses,
		coalesce(clientConfigENT.finEndowmentAssets, defvalues.finEndowmentAssets) finEndowmentAssets,
		coalesce(clientConfigENT.finPensionBenefits, defvalues.finPensionBenefits) finPensionBenefits,
		coalesce(clientConfigENT.finReportingModel, defvalues.finReportingModel) finReportingModel,
		coalesce(clientConfigENT.finParentOrChildInstitution, defvalues.finParentOrChildInstitution) finParentOrChildInstitution,
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
	    inner join ReportingPeriodMCR repperiod on clientConfigENT.surveyCollectionYear = repperiod.surveyYear
        cross join DefaultValues defvalues --on clientConfigENT.surveyCollectionYear = defvalues.surveyYear

    union

	select defvalues.surveyYear surveyYear,
	    'DefaultValues' source,
	    defvalues.asOfDate asOfDate,
	    CAST('9999-09-09' as DATE) snapshotDate,
	    null repperiodSnapshotDate, 
		defvalues.reportingDateStart reportingDateStart,
		defvalues.reportingDateEnd reportingDateEnd,
		defvalues.repPeriodTag1 repPeriodTag1,
	    defvalues.repPeriodTag2 repPeriodTag2,
--***** start survey-specific mods
		defvalues.finGPFSAuditOpinion finGPFSAuditOpinion,
	    defvalues.finAthleticExpenses finAthleticExpenses,
    	defvalues.finEndowmentAssets finEndowmentAssets,
    	defvalues.finPensionBenefits finPensionBenefits,
    	defvalues.finReportingModel finReportingModel,
    	defvalues.finParentOrChildInstitution finParentOrChildInstitution,
--***** end survey-specific mods
		1 configRn
    from DefaultValues defvalues
    where defvalues.surveyYear not in (select max(configENT.surveyCollectionYear)
										from IPEDSClientConfig configENT
										where configENT.surveyCollectionYear = defvalues.surveyYear)
	) ConfigLatest
where ConfigLatest.configRn = 1	
),

ChartOfAccountsMCR AS (
select chartOfAccountsId,
    case when isParent = 'True' then 'P'
		when isChild = 'True' then 'C' end COASParentChild,
    asOfDate,
    fiscalYear,
    surveyYear,
    surveySection,
    snapshotDate
from (
	select ROW_NUMBER() OVER (
			PARTITION BY
			    repperiod.fiscalYear,
				coasENT.chartOfAccountsId
			ORDER BY
			    (case when to_date(coasENT.snapshotDate,'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(coasENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(coasENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
			    (case when to_date(coasENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(coasENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                coasENT.startDate DESC,
				coasENT.recordActivityDate DESC
	) AS coasRn,
    coasENT.chartOfAccountsId,
    coasENT.chartOfAccountsTitle,
    coasENT.statusCode,
    coasENT.startDate,
    coasENT.endDate,
    coasENT.isParent,
    coasENT.isChild,
    coasENT.isIPEDSReportable,
    to_date(coasENT.recordActivityDate, 'YYYY-MM-DD') recordActivityDate,
    repperiod.snapshotDate snapshotDate,
    repperiod.asOfDate asOfDate,
    repperiod.fiscalYear fiscalYear,
    repperiod.surveyYear surveyYear,
    repperiod.surveySection surveySection
	from ReportingPeriodMCR repperiod
		left join ChartOfAccounts coasENT ON coasENT.startDate <= repperiod.asOfDate
--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
			and ((to_date(coasENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS TIMESTAMP) 
				and to_date(coasENT.recordActivityDate, 'YYYY-MM-DD') <= repperiod.asOfDate)
					or to_date(coasENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS TIMESTAMP))
			and (coasENT.endDate IS NULL or to_date(coasENT.endDate, 'YYYY-MM-DD') <= repperiod.asOfDate)
			and to_date(coasENT.startDate, 'YYYY-MM-DD') <= repperiod.asOfDate 
			and coasENT.isIPEDSReportable = 1
  )
where coasRn = 1
    and ((recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) 
            and statusCode = 'Active')
        or recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
),

FiscalYearMCR AS (
select FYData.asOfDate asOfDate,
        FYData.surveySection surveySection,
	CASE FYData.finGPFSAuditOpinion
				when 'Q' then 1
				when 'UQ' then 2
				when 'U' then 3 END finGPFSAuditOpinion, --1=Unqualified, 2=Qualified, 3=Don't know
	CASE FYData.finAthleticExpenses 
				WHEN 'A' THEN 1
				WHEN 'S' THEN 2 
				WHEN 'N' THEN 3 
				WHEN 'O' THEN 4 END finAthleticExpenses, --1=Auxiliary enterprises 2=Student services 3=Does not participate in intercollegiate athletics 4=Other (specify in caveats box below)
	CASE FYData.finReportingModel 
				WHEN 'B' THEN 1
				WHEN 'G' THEN 2
				WHEN 'GB' THEN 3 END finReportingModel, --1=Business Type Activities 2=Governmental Activities 3=Governmental Activities with Business-Type Activities
	FYData.finParentOrChildInstitution institParentChild,
	FYData.fiscalYear2Char fiscalYear2Char,
	FYData.fiscalPeriodCode fiscalPeriodCode,
	FYData.snapshotDate snapshotDate, 
	FYData.chartOfAccountsId chartOfAccountsId,
	FYData.fiscalYear fiscalYear,
    FYData.COASParentChild COASParentChild,
    FYData.startDate,
    FYData.endDate
from (
	select ROW_NUMBER() OVER (
			PARTITION BY
				coas.chartOfAccountsId,
				coas.fiscalYear,
				fiscalyearENT.fiscalPeriodCode
			ORDER BY
			    (case when to_date(fiscalyearENT.snapshotDate,'YYYY-MM-DD') = coas.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(fiscalyearENT.snapshotDate, 'YYYY-MM-DD') > coas.snapshotDate then to_date(fiscalyearENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
			    (case when to_date(fiscalyearENT.snapshotDate, 'YYYY-MM-DD') < coas.snapshotDate then to_date(fiscalyearENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				fiscalyearENT.startDate DESC,
				fiscalyearENT.recordActivityDate DESC
		) AS fiscalyearRn,
	    coas.chartOfAccountsId chartOfAccountsId,
	    coas.fiscalYear fiscalYear,
	    fiscalyearENT.fiscalYear4Char,
	    fiscalyearENT.fiscalYear2Char,
	    fiscalyearENT.fiscalPeriodCode,
	    to_date(fiscalyearENT.recordActivityDate,'YYYY-MM-DD') recordActivityDate,
	    fiscalyearENT.fiscalPeriod,
	    fiscalyearENT.startDate,
	    fiscalyearENT.endDate,
	    fiscalyearENT.isIPEDSReportable,
	    coas.snapshotDate snapshotDate,
		coas.asOfDate asOfDate,
		coas.COASParentChild COASParentChild,
		coas.surveySection surveySection,
		clientconfig.finGPFSAuditOpinion finGPFSAuditOpinion,
		clientconfig.finAthleticExpenses finAthleticExpenses,
		clientconfig.finReportingModel finReportingModel,
		clientconfig.finParentOrChildInstitution finParentOrChildInstitution
	from  ChartOfAccountsMCR coas
	    inner join ClientConfigMCR clientconfig on coas.surveyYear = clientconfig.surveyYear
	    left join FiscalYear fiscalyearENT on fiscalyearENT.fiscalYear2Char = coas.fiscalYear
	        and fiscalyearENT.chartOfAccountsID = coas.chartOfAccountsID
			and fiscalyearENT.fiscalPeriod in ('Year Begin', 'Year End')
		--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
		    and ((to_date(fiscalyearENT.recordActivityDate, 'YYYY-MM-DD') != CAST('9999-09-09' AS TIMESTAMP) 
				and to_date(fiscalyearENT.recordActivityDate, 'YYYY-MM-DD') <= coas.asOfDate)
					or to_date(fiscalyearENT.recordActivityDate, 'YYYY-MM-DD') = CAST('9999-09-09' AS TIMESTAMP))
			and (fiscalyearENT.endDate IS NULL or to_date(fiscalyearENT.endDate, 'YYYY-MM-DD') <= coas.asOfDate)
			and to_date(fiscalyearENT.startDate, 'YYYY-MM-DD') <= coas.asOfDate 
			and fiscalyearENT.isIPEDSReportable = 1
        ) FYData 
where FYData.fiscalyearRn = 1
),

GeneralLedgerMCR AS (
select *
from (
    select fiscalyr.fiscalYear2Char fiscalYear2Char,
		fiscalyr.institParentChild institParentChild, --is the institution a Parent or Child 
		fiscalyr.COASParentChild COASParentChild,     --is the COAS a Parent or Child account
		fiscalyr.asOfDate asOfDate,
        fiscalyr.surveySection surveySection,
        fiscalyr.fiscalPeriodCode fiscalPeriodCode,
        fiscalyr.chartOfAccountsId chartOfAccountsId,
        fiscalyr.snapshotDate snapshotDate,
        genledgerENT.accountingString accountingString,
        genledgerENT.recordActivityDate recordActivityDate,
        genledgerENT.accountType accountType,
        genledgerENT.beginBalance beginBalance,
        genledgerENT.endBalance endBalance,
        genledgerENT.assetCurrent assetCurrent,
        genledgerENT.assetCapitalLand assetCapitalLand,
        genledgerENT.assetCapitalInfrastructure assetCapitalInfrastructure,
        genledgerENT.assetCapitalBuildings assetCapitalBuildings,
        genledgerENT.assetCapitalEquipment assetCapitalEquipment,
        genledgerENT.assetCapitalConstruction assetCapitalConstruction,
        genledgerENT.assetCapitalIntangibleAsset assetCapitalIntangibleAsset,
        genledgerENT.assetCapitalOther assetCapitalOther,
        genledgerENT.assetNoncurrentOther assetNoncurrentOther,
        genledgerENT.deferredOutflow deferredOutflow,
        genledgerENT.liabCurrentLongtermDebt liabCurrentLongtermDebt,
        genledgerENT.liabCurrentOther liabCurrentOther,
        genledgerENT.liabNoncurrentLongtermDebt liabNoncurrentLongtermDebt,
        genledgerENT.liabNoncurrentOther liabNoncurrentOther,
        genledgerENT.deferredInflow deferredInflow,
        genledgerENT.accumDepreciation accumDepreciation,
        genledgerENT.accumAmmortization accumAmmortization,
        genledgerENT.isCapitalRelatedDebt isCapitalRelatedDebt,
        genledgerENT.isRestrictedExpendOrTemp isRestrictedExpendOrTemp,
        genledgerENT.isRestrictedNonExpendOrPerm isRestrictedNonExpendOrPerm,
        genledgerENT.isUnrestricted isUnrestricted,
        genledgerENT.isEndowment isEndowment,
        genledgerENT.isPensionGASB isPensionGASB,
        genledgerENT.isOPEBRelatedGASB isOPEBRelatedGASB,
        genledgerENT.isSinkingOrDebtServFundGASB isSinkingOrDebtServFundGASB,
        genledgerENT.isBondFundGASB isBondFundGASB,
        genledgerENT.isNonBondFundGASB isNonBondFundGASB,
        genledgerENT.isCashOrSecurityAssetGASB isCashOrSecurityAssetGASB,
        genledgerENT.isIPEDSReportable isIPEDSReportable,
		ROW_NUMBER() OVER (
			PARTITION BY
				fiscalyr.chartOfAccountsId,
				fiscalyr.fiscalYear2Char,
				fiscalyr.fiscalPeriodCode,
				genledgerENT.accountingString
			ORDER BY
			    (case when to_date(genledgerENT.snapshotDate,'YYYY-MM-DD') = fiscalyr.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(genledgerENT.snapshotDate, 'YYYY-MM-DD') > fiscalyr.snapshotDate then to_date(genledgerENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
			    (case when to_date(genledgerENT.snapshotDate, 'YYYY-MM-DD') < fiscalyr.snapshotDate then to_date(genledgerENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				genledgerENT.recordActivityDate DESC
		) AS genledgerRn
	from FiscalYearMCR fiscalyr
		left join GeneralLedgerReporting genledgerENT on genledgerENT.chartOfAccountsId = fiscalyr.chartOfAccountsId
				and genledgerENT.fiscalYear2Char = fiscalyr.fiscalYear2Char  
				and genledgerENT.fiscalPeriodCode = fiscalyr.fiscalPeriodCode
--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
				and ((to_date(genledgerENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS TIMESTAMP) 
				    and to_date(genledgerENT.recordActivityDate,'YYYY-MM-DD') <= fiscalyr.asOfDate)
				   or to_date(genledgerENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS TIMESTAMP))
				and genledgerENT.isIPEDSReportable = 1
  )
where genledgerRn = 1 
),

OperatingLedgerMCR AS (
select *
from (
    select fiscalyr.fiscalYear2Char fiscalYear2Char,
		fiscalyr.institParentChild institParentChild, --is the institution a Parent or Child 
		fiscalyr.COASParentChild COASParentChild,     --is the COAS a Parent or Child account
		fiscalyr.asOfDate asOfDate,
        fiscalyr.surveySection surveySection,
        fiscalyr.fiscalPeriodCode fiscalPeriodCode,
        fiscalyr.chartOfAccountsId chartOfAccountsId,
        fiscalyr.snapshotDate snapshotDate,
        oppledgerENT.accountingString accountingString,
        oppledgerENT.recordActivityDate recordActivityDate,
        oppledgerENT.accountType accountType,
        oppledgerENT.beginBalance beginBalance,
        oppledgerENT.endBalance endBalance,
        oppledgerENT.revTuitionAndFees revTuitionAndFees,
        oppledgerENT.revFedGrantsContractsOper revFedGrantsContractsOper,
        oppledgerENT.revFedGrantsContractsNOper revFedGrantsContractsNOper,
        oppledgerENT.revFedApproprations revFedApproprations,
        oppledgerENT.revStateGrantsContractsOper revStateGrantsContractsOper,
        oppledgerENT.revStateGrantsContractsNOper revStateGrantsContractsNOper,
        oppledgerENT.revStateApproprations revStateApproprations,
        oppledgerENT.revStateCapitalAppropriations revStateCapitalAppropriations,
        oppledgerENT.revLocalGrantsContractsOper revLocalGrantsContractsOper,
        oppledgerENT.revLocalGrantsContractsNOper revLocalGrantsContractsNOper,
        oppledgerENT.revLocalApproprations revLocalApproprations,
        oppledgerENT.revLocalTaxApproprations revLocalTaxApproprations,
        oppledgerENT.revLocalCapitalAppropriations revLocalCapitalAppropriations,
        oppledgerENT.revPrivGrantsContractsOper revPrivGrantsContractsOper,
        oppledgerENT.revPrivGrantsContractsNOper revPrivGrantsContractsNOper,
        oppledgerENT.revPrivGifts revPrivGifts,
        oppledgerENT.revAffiliatedOrgnGifts revAffiliatedOrgnGifts,
        oppledgerENT.revAuxEnterprSalesServices revAuxEnterprSalesServices,
        oppledgerENT.revHospitalSalesServices revHospitalSalesServices,
        oppledgerENT.revEducActivSalesServices revEducActivSalesServices,
        oppledgerENT.revOtherSalesServices revOtherSalesServices,
        oppledgerENT.revIndependentOperations revIndependentOperations,
        oppledgerENT.revInvestmentIncome revInvestmentIncome,
        oppledgerENT.revCapitalGrantsGifts revCapitalGrantsGifts,
        oppledgerENT.revAddToPermEndowments revAddToPermEndowments,
        oppledgerENT.revReleasedAssets revReleasedAssets,
        oppledgerENT.revPropAndNonPropTaxes revPropAndNonPropTaxes,
        oppledgerENT.revInterestEarnings revInterestEarnings,
        oppledgerENT.revDividendEarnings revDividendEarnings,
        oppledgerENT.revRealizedCapitalGains revRealizedCapitalGains,
        oppledgerENT.revRealizedOtherGains revRealizedOtherGains,
        oppledgerENT.revUnrealizedGains revUnrealizedGains,
        oppledgerENT.revExtraordGains revExtraordGains,
        oppledgerENT.revOwnerEquityAdjustment revOwnerEquityAdjustment,
        oppledgerENT.revSumOfChangesAdjustment revSumOfChangesAdjustment,
        oppledgerENT.revOtherOper revOtherOper,
        oppledgerENT.revOtherNOper revOtherNOper,
        oppledgerENT.revOther revOther,
        oppledgerENT.expFAPellGrant expFAPellGrant,
        oppledgerENT.expFANonPellFedGrants expFANonPellFedGrants,
        oppledgerENT.expFAStateGrants expFAStateGrants,
        oppledgerENT.expFALocalGrants expFALocalGrants,
        oppledgerENT.expFAInstitGrantsRestr expFAInstitGrantsRestr,
        oppledgerENT.expFAInstitGrantsUnrestr expFAInstitGrantsUnrestr,
        oppledgerENT.expSalariesWages expSalariesWages,
        oppledgerENT.expBenefits expBenefits,
        oppledgerENT.expOperMaintSalariesWages expOperMaintSalariesWages,
        oppledgerENT.expOperMaintBenefits expOperMaintBenefits,
        oppledgerENT.expOperMaintOther expOperMaintOther,
        oppledgerENT.expCapitalConstruction expCapitalConstruction,
        oppledgerENT.expCapitalEquipPurch expCapitalEquipPurch,
        oppledgerENT.expCapitalLandPurchOther expCapitalLandPurchOther,
        oppledgerENT.expDepreciation expDepreciation,
        oppledgerENT.expInterest expInterest,
        oppledgerENT.expFedIncomeTax expFedIncomeTax,
        oppledgerENT.expStateLocalIncomeTax expStateLocalIncomeTax,
        oppledgerENT.expExtraordLosses expExtraordLosses,
        oppledgerENT.expOwnerEquityAdjustment expOwnerEquityAdjustment,
        oppledgerENT.expSumOfChangesAdjustment expSumOfChangesAdjustment,
        oppledgerENT.expOther expOther,
        oppledgerENT.discAllowTuitionFees discAllowTuitionFees,
        oppledgerENT.discAllowAuxEnterprise discAllowAuxEnterprise,
        oppledgerENT.discAllowPatientContract discAllowPatientContract,
        oppledgerENT.isInstruction isInstruction,
        oppledgerENT.isResearch isResearch,
        oppledgerENT.isPublicService isPublicService,
        oppledgerENT.isAcademicSupport isAcademicSupport,
        oppledgerENT.isStudentServices isStudentServices,
        oppledgerENT.isInstitutionalSupport isInstitutionalSupport,
        oppledgerENT.isScholarshipFellowship isScholarshipFellowship,
        oppledgerENT.isAuxiliaryEnterprises isAuxiliaryEnterprises,
        oppledgerENT.isHospitalServices isHospitalServices,
        oppledgerENT.isIndependentOperations isIndependentOperations,
        oppledgerENT.isAgricultureOrExperiment isAgricultureOrExperiment,
        oppledgerENT.isPensionGASB isPensionGASB,
        oppledgerENT.isOPEBRelatedGASB isOPEBRelatedGASB,
        oppledgerENT.isStateRetireFundGASB isStateRetireFundGASB,
        oppledgerENT.isUnrestrictedFASB isUnrestrictedFASB,
        oppledgerENT.isRestrictedTempFASB isRestrictedTempFASB,
        oppledgerENT.isRestrictedPermFASB isRestrictedPermFASB,
        oppledgerENT.isIPEDSReportable isIPEDSReportable,
        oppledgerENT.fiscalYear2Char,
		ROW_NUMBER() OVER (
			PARTITION BY
				fiscalyr.chartOfAccountsId,
				fiscalyr.fiscalYear2Char,
				fiscalyr.fiscalPeriodCode,
				oppledgerENT.accountingString
			ORDER BY
			    (case when to_date(oppledgerENT.snapshotDate,'YYYY-MM-DD') = fiscalyr.snapshotDate then 1 else 2 end) asc,
			    (case when to_date(oppledgerENT.snapshotDate, 'YYYY-MM-DD') > fiscalyr.snapshotDate then to_date(oppledgerENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
			    (case when to_date(oppledgerENT.snapshotDate, 'YYYY-MM-DD') < fiscalyr.snapshotDate then to_date(oppledgerENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
				oppledgerENT.recordActivityDate DESC
		) AS OLRn
    from FiscalYearMCR fiscalyr
		left join OperatingLedgerReporting oppledgerENT on oppledgerENT.chartOfAccountsId = fiscalyr.chartOfAccountsId
			and oppledgerENT.fiscalYear2Char = fiscalyr.fiscalYear2Char  
			and oppledgerENT.fiscalPeriodCode = fiscalyr.fiscalPeriodCode
--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
			and ((to_date(oppledgerENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS TIMESTAMP) 
				    and to_date(oppledgerENT.recordActivityDate,'YYYY-MM-DD') <= fiscalyr.asOfDate)
				   or to_date(oppledgerENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS TIMESTAMP))
				and oppledgerENT.isIPEDSReportable = 1
		where fiscalyr.surveySection = 'Current'
  )
where OLRn = 1
) 

/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs 

PARTS: 
    Part 9: General Information
    Part A: Statement of Financial Position 
    Part P: While the form shows Part A Page 2 as a component of Part A, The specs have us listing as Part P 
    Part D: Summary of Changes in Net Position
    Part E: Scholarships and Fellowships
    Part B: Revenues and Other Additions
    Part C: Expenses and Other Deductions
    Part M: Pension and Postemployment Benefits Other than Pension (OPEB) Information
    Part H: Endowment Assets 
    Part J: Revenues
    Part K: Expenditures
    Part L: Debts and Assets
****/

--Part 9
--General Information 

--jdh 2020-03-04 Removed cross join with ClientConfigMCR since values are now already in FiscalYearMCR
--swapped CASE Statements for COAS fields used for Section 9 (General Information)

select DISTINCT '9' part,
	1 sort,
	CAST(MONTH(coalesce(first(coas.startDate), CAST('2020-01-01' AS DATE))) as string) field1, --Month of beginning date of fiscal year covered, 2-digit month between 1 and 12
	CAST(YEAR(coalesce(first(coas.startDate), CAST('2020-01-01' AS DATE))) as string) field2, --Year of beginning date of fiscal year covered, 4-digit year between 2019 and 2020
	CAST(MONTH(coalesce(first(coas.endDate), CAST('2020-10-01' AS DATE))) as string) field3, --Month of end date of fiscal year covered. Your fiscal year must end before October 1, 2020. 2-digit month between 1 and 12
	CAST(YEAR(coalesce(first(coas.endDate), CAST('2020-10-01' AS DATE))) as string) field4, --Year of end date of fiscal year covered. Your fiscal year must end before October 1, 2020. 4-digit year between 2019 and 2020
	first(coas.finGPFSAuditOpinion) field5,  --1=Unqualified, 2=Qualified, 3=Don't know
	first(coas.finReportingModel) field6,  --1=Business Type Activities 2=Governmental Activities 3=Governmental Activities with Business-Type Activities
	first(coas.finAthleticExpenses) field7,   --1=Auxiliary enterprises 2=Student services 3=Does not participate in intercollegiate athletics 4=Other (specify in caveats box below)
	1 field8,  --Intercollegiate Athletics: b) If your institution participates in intercollegiate athletics, are these revenues included in: Sales and services of educational activities	1= Yes, 2=No
	1 field9,  --Intercollegiate Athletics: b) If your institution participates in intercollegiate athletics, are these revenues included in: Sales and services of auxiliary enterprises	1= Yes, 2=No
	1 field10,  --Intercollegiate Athletics: b) If your institution participates in intercollegiate athletics, are these revenues included in: Does not have intercollegiate athletics revenue	1= Yes, 2=No
	1 field11,  --Intercollegiate Athletics: b) If your institution participates in intercollegiate athletics, are these revenues included in: Other (explain in caveat box)	1= Yes, 2=No
	NULL field12

--*************************************************************************************************

--2021 Imprt Spec looks for FOUR new athletics fields (ATH_REV_1, ATH_REV_2, ATH_REV_3, ATH_REV_4)

--****************************************************************************************************
from FiscalYearMCR coas 
where coas.surveySection = 'Current'

union 

--Part A
--Statement of Net Assets   

-- Line 01 - Total current assets - 
/*  Report all current assets on this line. Include cash and cash equivalents, investments, 
    accounts and notes receivables (net of allowance for uncollectible amounts), inventories, 
    and all other assets classified as current assets. 	 
*/

--jdh 2020-03-04 Changed to use new in-line GL/OL Structure

select 'A',
	2,
	'1', --Total current assets
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.assetCurrent = 'Y'
 
union

-- Line 31 - Depreciable capital assets, net of depreciation - 
/*  Report all capital assets reduced by the total accumulated depreciation. Capital assets include improvements to land, 
    easements, buildings, building improvements, vehicles, machinery, equipment, infrastructure, and all other tangible 
    or intangible depreciable assets that are used in operations and that have initial useful lives extending beyond a 
    single reporting period. Include only depreciable capital assets on this line; non-depreciable capital assets will 
    be included on line 04. 
    Report the net amount of all depreciable capital assets after reducing the gross amount for accumulated depreciation.  
*/ 
select 'A',
	3,
	'31', --Depreciable capital assets, net of depreciation
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (genledger.assetCapitalLand = 'Y'
										or genledger.assetCapitalInfrastructure = 'Y'
										or genledger.assetCapitalBuildings = 'Y'
										or genledger.assetCapitalEquipment = 'Y'
										or genledger.assetCapitalConstruction = 'Y'
										or genledger.assetCapitalIntangibleAsset = 'Y'
										or genledger.assetCapitalOther = 'Y') 
								THEN genledger.endBalance ELSE 0 END) 
		- SUM(CASE WHEN genledger.accumDepreciation = 'Y' THEN genledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and (genledger.assetCapitalLand = 'Y' 
		or genledger.assetCapitalInfrastructure = 'Y' 
		or genledger.assetCapitalBuildings = 'Y' 
		or genledger.assetCapitalEquipment = 'Y' 
		or genledger.assetCapitalConstruction = 'Y' 
		or genledger.assetCapitalIntangibleAsset = 'Y' 
		or genledger.assetCapitalOther = 'Y' 
		or genledger.accumDepreciation = 'Y')
    
union

-- Line 05 - Total noncurrent assets
-- Report the total of all noncurrent assets as reported in the institution-s GPFS.  

select 'A',
	4,
	'5', --Total noncurrent assets
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and (genledger.assetCapitalLand = 'Y'
		or genledger.assetCapitalInfrastructure = 'Y'
		or genledger.assetCapitalBuildings = 'Y'
		or genledger.assetCapitalEquipment = 'Y'
		or genledger.assetCapitalConstruction = 'Y'
		or genledger.assetCapitalIntangibleAsset = 'Y'
		or genledger.assetCapitalOther = 'Y'
		or genledger.assetNoncurrentOther = 'Y') 

union

-- Line 19 - Deferred outflows of resources - 
/*  Report the deferred outflows of resources as recognized in the institution-s GPFS and in accordance with GASB 63.
    Definition: A consumption of net assets by a government that is applicable to future periods.
    Examples of deferred outflows of resources include changes in fair values in hedging instruments and changes in 
    the net pension liability that are not considered pension expense (as described in GASB Statement 68, Accounting 
    and Financial Reporting for Pensions: an amendment of GASB Statement No. 27). 
*/ 

select 'A',
	5,
	'19', --Deferred outflows of resources
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.deferredOutflow = 'Y' 

union

-- Line 07 - Long-term debt, current portion - 
/*  Report the amount due in the next operating cycle (usually a year) for amounts
    otherwise reported as long-term or noncurrent debt. Include only outstanding debt
    on this line; the current portion of other long-term liabilities, such as compensated
    absences, will be included on line 08. 
*/ 

select  'A',
	6,
	'7', --Long-term debt, current portion
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.liabCurrentLongtermDebt = 'Y' 
	
union

-- Line 09 -Total current liabilitie
-- Report the total of all current liabilities as reported in the institution-s GPFS. 
 
select 'A',
	7,
	'9', --Total current liabilities
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and (genledger.liabCurrentLongtermDebt = 'Y' 
		or genledger.liabCurrentOther = 'Y') 

union

-- Line 10 - Long-term debt

select 'A',
	8,
	'10', --Long-term debt
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.liabNoncurrentLongtermDebt = 'Y' 

union

select 'A',
	9,
	'12', --Total noncurrent liabilities
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
    and genledger.institParentChild = 'P'
    and (genledger.liabNoncurrentLongtermDebt = 'Y' 
		or genledger.liabNoncurrentOther = 'Y') 

union

select 'A',
	10,
	'20', --Deferred inflows of resources
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.deferredInflow = 'Y' 
	
union

select 'A',
	11,
	'14', --Net assets invested in capital assets, net of related debt
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (genledger.assetCapitalLand = 'Y'
							or genledger.assetCapitalInfrastructure = 'Y'
							or genledger.assetCapitalBuildings = 'Y'
							or genledger.assetCapitalEquipment = 'Y'
							or genledger.assetCapitalConstruction = 'Y'
							or genledger.assetCapitalIntangibleAsset = 'Y'
							or genledger.assetCapitalOther = 'Y') THEN genledger.endBalance ELSE 0 END)
				- SUM(CASE WHEN genledger.accumDepreciation = 'Y' or genledger.isCapitalRelatedDebt = 1 THEN genledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
    and genledger.institParentChild = 'P'
    and (genledger.assetCapitalLand = 'Y'
		or genledger.assetCapitalInfrastructure = 'Y'
		or genledger.assetCapitalBuildings = 'Y'
		or genledger.assetCapitalEquipment = 'Y'
		or genledger.assetCapitalConstruction = 'Y'
		or genledger.assetCapitalIntangibleAsset = 'Y'
		or genledger.assetCapitalOther = 'Y'
		or genledger.accumDepreciation = 'Y' 
		or genledger.isCapitalRelatedDebt = 1) 

union

select 'A',
	12,
	'15', --Restricted expendable net assets
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.accountType = 'Asset' 
    and genledger.isRestrictedExpendOrTemp = 1 
	
union

select 'A',
	13,
	'16', --Restricted non-expendable net assets
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.accountType = 'Asset' 
    and genledger.isRestrictedNonExpendOrPerm = 1 
	
union

select 'P',
	14,
	'21', --Land and land improvements
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = genledger.COASParentChild
	and genledger.assetCapitalLand = 'Y'

union

-- Line 22 - Infrastructure
-- Report infrastructure assets such as roads, bridges, drainage systems, water and sewer systems, etc.

select 'P',
	15,
	'22', --Infrastructure
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriodCode = 'Year End'
   and genledger.institParentChild = genledger.COASParentChild
   and genledger.assetCapitalInfrastructure = 'Y'

union

-- Line 23 - Buildings
/*  Report structures built for occupancy or use, such as for classrooms, research, administrative offices, storage, etc.
    Include built-in fixtures and equipment that are essentially part of the permanent structure.
*/

select 'P',
	16,
	'23', --Buildings
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = genledger.COASParentChild
	and genledger.assetCapitalBuildings = 'Y'

union

-- Line 32 - Equipment, including art and library collections - 
/*  Report moveable tangible property such as research equipment, vehicles, office equipment, library collections 
    (capitalized amount of books, films, tapes, and other materials maintained in library collections intended for use by patrons), 
    and capitalized art collections.
*/

select 'P',
	17,
	'32', --Equipment
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriodCode = 'Year End'
   and genledger.institParentChild = genledger.COASParentChild
   and genledger.assetCapitalEquipment = 'Y'

union

-- Line 27 - Construction in progress - 
-- Report capital assets under construction and not yet placed into service.

select 'P',
	18,
	'27', --Construction in Progress
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriodCode = 'Year End'
   and genledger.institParentChild = genledger.COASParentChild
   and genledger.assetCapitalConstruction = 'Y'

union

-- Line 28 - Accumulated depreciation - 
-- Report all depreciation amounts, including depreciation on assets that may not be included on any of the above lines.

select 'P',
	19,
	'28', --Accumulated depreciation
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriodCode = 'Year End'
   and genledger.institParentChild = genledger.COASParentChild
   and genledger.accumDepreciation = 'Y'

union

-- Line 33 - Intangible assets, net of accumulated amortization - 
/*  Report all assets consisting of certain nonmaterial rights and benefits of an institution, such as patents, copyrights, trademarks and goodwill. 
    The amount report should be reduced by total accumulated amortization.
*/

select 'P',
	20,
	'33', --Intangible assets, net of accumulated amortization
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN genledger.assetCapitalIntangibleAsset = 'Y' THEN genledger.endBalance ELSE 0 END) 
				- SUM(CASE WHEN genledger.accumAmmortization = 'Y' THEN genledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriodCode = 'Year End'
   and genledger.institParentChild = genledger.COASParentChild
   and (genledger.assetCapitalIntangibleAsset = 'Y'
		or genledger.accumAmmortization = 'Y')

union

-- Line 34 - Other capital assets  
-- Report all other amounts for capital assets not reported in lines 21 through 28, and lines 32 and 33.

select 'P',
	21,
	'34', --Other capital assets
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriodCode = 'Year End'
   and genledger.institParentChild = genledger.COASParentChild
   and genledger.assetCapitalOther = 'Y'
   
union

select 'D',
	22,
	'1', --Total revenues and other additions
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger  
where oppledger.fiscalPeriodCode= 'Year End' 
    and oppledger.institParentChild = 'P'
    and oppledger.accountType = 'Revenue'

union

select 'D',
    23,
    '2', --Total expenses and deductions
    CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger  
where oppledger.fiscalPeriodCode= 'Year End' 
    and oppledger.institParentChild = 'P'
    and oppledger.accountType = 'Expense'
	
union

select 'D',
	24,
	'4', --Net assets beginning of year
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN genledger.accountType = 'Asset' THEN genledger.beginBalance ELSE 0 END))), 0) as BIGINT) 
		- CAST(NVL(ABS(ROUND(SUM(CASE WHEN genledger.accountType = 'Liability' THEN genledger.beginBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year Begin' 
    and genledger.institParentChild = 'P'
    and (genledger.accountType = 'Asset' 
		or genledger.accountType = 'Liability')
		
union


--Part E 
-- Scholarships and Fellowships
/*  This part is intended to report details about scholarships and fellowships.
    For each source on lines 01-06, enter the amount of resources received that are used for scholarships and fellowships. 
    Scholarships and fellowships include: grants-in-aid, trainee stipends, tuition and fee waivers, and prizes to students. 
    Student grants do not include amounts provided to students as payments for teaching or research or as fringe benefits.

    For lines 08 and 09, identify amounts that are reported in the GPFS as allowances only. "Discount and allowance" means 
    the institution displays the financial aid amount as a deduction from tuition and fees or a deduction from auxiliary
    enterprise revenues in its GPFS.
*/

-- Line 01 - Pell grants (federal) - 
-- Report the gross amount of Pell Grants made available to recipients by your institution. This is the gross Pell Grants received as federal grant revenue for the fiscal year.

select 'E',
	25,
	'1', --Pell grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
   and oppledger.institParentChild = oppledger.COASParentChild
   and oppledger.expFAPellGrant = 'Y'
	
union

select 'E',
	26,
	'2', --Other Federal grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFANonPellFedGrants = 'Y'
	
union

select 'E',
	27,
	'3', --Grants by state government
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFAStateGrants = 'Y'
    
union

select 'E',
	28,
	'4', --Grants by local government
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFALocalGrants = 'Y'
	
union

select 'E',
	29,
	'5', --Institutional grants from restricted resources
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild

union

-- Line 07 - Total revenue that funds scholarships and fellowships 
/*  Report the total revenue used to fund scholarships and fellowships from sources in lines 01 to 06. 
    Check this amount with the corresponding amount on their GPFS or underlying records. If these amounts differ materially, 
    the data provider is advised to check the other amounts provided on this screen for data entry errors. 
*/

select 'E',
	30,
	'7', -- Total revenue that funds scholarships and fellowships
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.expFAPellGrant = 'Y'             --E1 Pell grants
		or oppledger.expFANonPellFedGrants = 'Y'     --E2 Other federal grants
		or oppledger.expFAStateGrants = 'Y'          --E3 Grants by state government
		or oppledger.expFALocalGrants = 'Y'          --E4 Grants by local government
		or oppledger.expFAInstitGrantsRestr = 'Y'    --E5 Institutional grants from restricted resources
		or oppledger.expFAInstitGrantsUnrestr = 'Y') --E6 Institutional grants from unrestricted resources

union

select 'E',
	31,
	'8', --Discounts and allowances applied to tuition and fees
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.discAllowTuitionFees = 'Y'

union

select 'E',
	32,
	'9', --Discounts and allowances applied to sales and services of auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.discAllowAuxEnterprise = 'Y'
	
union

select 'Q',
	33,
	'12', --Pell grants (federal)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' 
		 THEN oppledger.endBalance
		 ELSE 0 END))), 0) as BIGINT),
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowAuxEnterprise = 'Y' 
		 THEN oppledger.endBalance
		 ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFAPellGrant = 'Y' 
 
union

select 'Q',
	34,
	'13', --Other federal grants (Do NOT include FDSL amounts)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' 
		 THEN oppledger.endBalance
		 ELSE 0 END))), 0) as BIGINT),
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowAuxEnterprise = 'Y' 
		 THEN oppledger.endBalance
		 ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFANonPellFedGrants = 'Y'
 
union

select 'Q',
	35,
	'14', --Grants by state government
CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' 
		 THEN oppledger.endBalance
		 ELSE 0 END))), 0) as BIGINT),
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowAuxEnterprise = 'Y' 
		 THEN oppledger.endBalance
		 ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFAStateGrants = 'Y'
 
union

select 'Q',
	36,
	'15', --Grants by local government
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' 
		 THEN oppledger.endBalance
		 ELSE 0 END))), 0) as BIGINT),
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowAuxEnterprise = 'Y' 
		 THEN oppledger.endBalance
		 ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFALocalGrants = 'Y'
 
union

select 'Q',
	37,
	'16', --Endowments and gifts
CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' 
		 THEN oppledger.endBalance
		 ELSE 0 END))), 0) as BIGINT),
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowAuxEnterprise = 'Y' 
		 THEN oppledger.endBalance
		 ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.discAllowAuxEnterprise = 'Y' 
	
union

-- Part B  
-- Revenues and Other Additions, Operating Revenue
/*  This part is intended to report revenues by source.
    Includes all operating revenues, nonoperating revenues, and other additions for the reporting period. This includes unrestricted 
    and restricted revenues and additions, whether expendable or nonexpendable.

    Exclude from revenue (and expenses) interfund or intraorganizational charges and credits. Interfund and intraorganizational 
    charges and credits include interdepartmental charges, indirect costs, and reclassifications from temporarily restricted net assets.
*/

-- Line 01 - Tuition & fees, after deducting discounts & allowances - 
/*  Report all tuition & fees (including student activity fees) revenue received from students for education purposes. 
    Include revenues for tuition and fees net of discounts & allowances from institutional and governmental scholarships, waivers, etc. 
    (report gross revenues minus discounts and allowances). Include here those tuition and fees that are remitted to the state as 
    an offset to state appropriations. (Charges for room, board, and other services rendered by auxiliary enterprises are not reported here; 
    see line 05.) 
*/

select 'B',
	38,
	'1', --Tuition and fees (after deducting discounts and allowances)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revTuitionAndFees = 'Y' THEN oppledger.endBalance ELSE 0 END) 
					 - SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revTuitionAndFees = 'Y' 
		or oppledger.discAllowTuitionFees = 'Y')

union

select 'B',
	39,
	'2', --Federal operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revFedGrantsContractsOper = 'Y'

union

select 'B',
	40,
	'3', --State operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revStateGrantsContractsOper = 'Y'

union

select 'B',
	41,
	'4a', --Local government operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revLocalGrantsContractsOper = 'Y'

union

select 'B',
	42,
	'4b', --Private operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revPrivGrantsContractsOper = 'Y'

union

select 'B',
	43,
	'5', --Sales and services of auxiliary enterprises (after deducting discounts and allowances)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revAuxEnterprSalesServices = 'Y' THEN oppledger.endBalance ELSE 0 END) 
					 - SUM(CASE WHEN oppledger.discAllowAuxEnterprise = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revAuxEnterprSalesServices = 'Y'
		or oppledger.discAllowAuxEnterprise = 'Y')

union

select 'B',
	44,
	'6', --Sales and services of hospitals (after deducting patient contractual allowances)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revHospitalSalesServices = 'Y' THEN oppledger.endBalance ELSE 0 END) 
					- SUM(CASE WHEN oppledger.discAllowPatientContract = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revHospitalSalesServices = 'Y'
		or oppledger.discAllowPatientContract = 'Y')

union

select 'B',
	45,
	'26', --Sales & services of educational activities
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revEducActivSalesServices = 'Y'

union

select 'B',
	46,
	'7', --Independent operations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revIndependentOperations = 'Y'

union


-- Line 09 - Total Operating Revenues - 
-- Report total operating revenues from your GPFS.

select 'B',
	47,
	'9', --Total operating revenues
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.revTuitionAndFees = 'Y'           --B1 Tuition and fees (after deducting discounts and allowances)
								or oppledger.revFedGrantsContractsOper = 'Y'       --B2 Federal operating grants and contracts
								or oppledger.revStateGrantsContractsOper = 'Y'     --B3 State operating grants and contracts
								or oppledger.revLocalGrantsContractsOper = 'Y'     --B4a Local government operating grants and contracts
								or oppledger.revPrivGrantsContractsOper = 'Y'      --B5b Private operating grants and contracts
								or oppledger.revAuxEnterprSalesServices = 'Y'      --B5 Sales and services of auxiliary enterprises (after deducting discounts and allowances)
								or oppledger.revHospitalSalesServices = 'Y'        --B6 Sales and services of hospitals (after deducting patient contractual allowances)
								or oppledger.revEducActivSalesServices = 'Y'       --B26 Sales & services of educational activities
								or oppledger.revOtherSalesServices = 'Y'
								or oppledger.revIndependentOperations = 'Y'        --B7 Independent operations
								or oppledger.revOtherOper = 'Y'                    --B8 Other sources - operating
									) THEN oppledger.endBalance ELSE 0 END) -
					SUM(CASE WHEN (oppledger.discAllowTuitionFees = 'Y'            --B1 Tuition and fees discounts and allowances
								or oppledger.discAllowAuxEnterprise = 'Y'          --B5 auxiliary enterprises discounts and allowances
								or oppledger.discAllowPatientContract = 'Y'        --B6 patient contractual allowances
									) THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revTuitionAndFees = 'Y'  
		or oppledger.revFedGrantsContractsOper = 'Y'  
		or oppledger.revStateGrantsContractsOper = 'Y'  
		or oppledger.revLocalGrantsContractsOper = 'Y'  
		or oppledger.revPrivGrantsContractsOper = 'Y'  
		or oppledger.revAuxEnterprSalesServices = 'Y'  
		or oppledger.revHospitalSalesServices = 'Y' 
		or oppledger.revEducActivSalesServices = 'Y'  
		or oppledger.revOtherSalesServices = 'Y' 
		or oppledger.revIndependentOperations = 'Y'  
		or oppledger.revOtherOper = 'Y' 
		or oppledger.discAllowTuitionFees = 'Y' 
		or oppledger.discAllowAuxEnterprise = 'Y' 
		or oppledger.discAllowPatientContract = 'Y')

union

select 'B',
	48,
	'10', --Federal appropriations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revFedApproprations = 'Y'

union

select 'B',
	49,
	'11', --State appropriations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revStateApproprations = 'Y'

union

select 'B',
	50,
	'12', --Local appropriations, education district taxes, and similar support
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revLocalApproprations = 'Y' 
		or oppledger.revLocalTaxApproprations = 'Y')
   
union

select 'B',
	51,
	'13', --Federal nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revFedGrantsContractsNOper = 'Y'

union

select 'B',
	52,
	'14', --State nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revStateGrantsContractsNOper = 'Y'

union

select 'B',
	53,
	'15', --Local government nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revLocalGrantsContractsNOper = 'Y'

union

select 'B',
	54,
	'16', --Gifts, including contributions from affiliated organizations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revPrivGifts = 'Y' 
		or oppledger.revAffiliatedOrgnGifts = 'Y')

union

select 'B',
	55,
	'17', --Investment income
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revInvestmentIncome = 'Y'


union

select 'B',
	56,
	'19', --Total nonoperating revenues
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revFedApproprations = 'Y'               --B10 Federal appropriations
		or oppledger.revStateApproprations = 'Y'         --B11 State appropriations
		or oppledger.revLocalApproprations = 'Y'         --B12 Local appropriations, education district taxes, and similar support
		or oppledger.revLocalTaxApproprations = 'Y'      --B12 Local appropriations, education district taxes, and similar support
		or oppledger.revFedGrantsContractsNOper = 'Y'    --B13 Federal nonoperating grants
		or oppledger.revStateGrantsContractsNOper = 'Y'  --B14 State nonoperating grants
		or oppledger.revLocalGrantsContractsNOper = 'Y'  --B15 Local government nonoperating grants
		or oppledger.revPrivGifts = 'Y'                  --B16 Gifts, including contributions from affiliated organizations
		or oppledger.revAffiliatedOrgnGifts = 'Y'        --B16 Gifts, including contributions from affiliated organizations
		or oppledger.revInvestmentIncome = 'Y'           --B17 Investment income
		or oppledger.revOtherNOper = 'Y'                 --Other sources - nonoperating
		or oppledger.revPrivGrantsContractsNOper = 'Y')  --Other - Private nonoperating grants

union

select 'B',
	57,
	'20', --Capital appropriations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revStateCapitalAppropriations = 'Y' 
		or oppledger.revLocalCapitalAppropriations = 'Y')

union

select 'B',
	58,
	'21', --Capital grants and gifts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revCapitalGrantsGifts = 'Y'

union

select 'B',
	59,
	'22', --Additions to permanent endowments
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revAddToPermEndowments = 'Y'

union

select 'B',
	60,
	'25', --Total all revenues and other additions
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Revenue' THEN oppledger.endBalance ELSE 0 END) -
					   SUM(CASE WHEN oppledger.accountType = 'Revenue Discount' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.accountType = 'Revenue'
		or oppledger.accountType = 'Revenue Discount')
		
union

-- Part C   
-- Expenses and Other Deductions: Functional Classification
/*  This part is intended to collect expenses by function. All expenses recognized in the GPFS should be reported using the 
    expense functions provided on lines 01-14. These categories are consistent with NACUBO Advisory Report 2000-8, 
    Recommended Disclosure of Alternative Expense Classification Information for Public Higher Education Institutions.
    The total for expenses on line 19 should agree with the total expenses reported in your GPFS including interest expense 
    and any other nonoperating expenses.
*/

select 'C',
	61,
	'1', --Instruction
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19))
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense'

union

select 'C',
	62,
	'2', --Research
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isResearch = 1

union

select 'C',
	63,
	'3', --Public service
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isPublicService = 1

union

select 'C',
	64,
	'5', --Academic support
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isAcademicSupport = 1

union

select 'C',
	65,
	'6', --Student services
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isStudentServices = 1

union

select 'C',
	66,
	'7', --Institutional support
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y'  THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isInstitutionalSupport = 1

union

select 'C',
	67,
	'11', --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isAuxiliaryEnterprises = 1

union

select 'C',
	68,
	'12', --Hospital services
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isHospitalServices = 1

union

select 'C',
	69,
	'13', --Independent operations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y'  THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isIndependentOperations = 1

union

select 'C',
	70,
	'19', --Total expenses and deductions
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Expense' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Expense' and oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19) 19_2
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expBenefits = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Benefits 19_3
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expOperMaintSalariesWages = 'Y' or oppledger.expOperMaintBenefits = 'Y' or oppledger.expOperMaintOther = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Oper and Maint of Plant 19_4
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expDepreciation = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Depreciation 19_5
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expInterest = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),   --Interest 19,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.accountType = 'Expense'
		or oppledger.expSalariesWages = 'Y'
		or oppledger.expBenefits = 'Y'
		or oppledger.expOperMaintSalariesWages = 'Y'
		or oppledger.expOperMaintBenefits = 'Y'
		or oppledger.expOperMaintOther = 'Y'
		or oppledger.expDepreciation = 'Y'
		or oppledger.expInterest = 'Y')	  
		
union

-- Part M  
-- Pension Information
/*  Pension and Other Postemployment Benefits (OPEB) Information (Only applicable for institutions that indicate "Yes" to the screening question)
    This section collects information on expenses, liabilities, and/or deferrals related to one or more defined benefit pension plans 
    (either a single employer, agent employer or cost-sharing multiple employer) and/or Other Postemployment Benefits (OPEB) plans 
    in which your institution participates. Note that Part M is only required from institutions that include liabilities, expenses, and/or 
    deferrals for one or more defined benefit pension and/or OPEB plans in their General Purpose Financial Statement. 
*/

--jh 20200218 added conditional for IPEDSClientConfig.finPensionBenefits
select 'M',
	71,
	'1', --Pension expense
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) 
		else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = 'P'
	and oppledger.accountType = 'Expense' 
	and oppledger.isPensionGASB = 1

union

select 'M',
	72,
	'2', --Net Pension liability
	CAST(case when (select clientconfig.finPensionBenefits
		from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		else 0
			end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL											
from GeneralLedgerMCR genledger
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Liability' 
	and genledger.isPensionGASB = 1

union

select 'M',
	73,
	'3', --Deferred inflows (an acquisition of net assets) related to pension
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Liability' 
	and genledger.deferredInflow = 'Y' 
	and genledger.isPensionGASB = 1

union

select 'M',
	74,
	'4', --Deferred outflows(a consumption of net assets) related to pension
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset' 
	and genledger.deferredOutflow = 'Y' 
	and genledger.isPensionGASB = 1
    
union

select 'M',
	75,
	'5', --OPEB Expense
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = 'P'
	and oppledger.accountType = 'Expense' 
	and oppledger.isOPEBRelatedGASB = 1 

union

select 'M',
	76,
	'6', --Net OPEB Liability
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Liability' 
	and genledger.isOPEBRelatedGASB = 1

union

select 'M',
	77,
	'7', --Deferred inflow related to OPEB
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Liability' 
	and genledger.deferredInflow = 'Y' 
	and genledger.isOPEBRelatedGASB = 1

union

select 'M',
	78,
	'8', --Deferred outflow related to OPEB
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset' 
	and genledger.deferredOutflow = 'Y' 
	and genledger.isOPEBRelatedGASB = 1
	
union

-- Part H   
-- Details of Endowment Assets
/*  This part is intended to report details about endowments.
    This part appears only for institutions answering "Yes" to the general information question regarding endowment assets.
    Report the amounts of gross investments of endowment, term endowment, and funds functioning as endowment for the institution 
    and any of its foundations, other affiliated organizations, and component units. DO NOT reduce investments by liabilities for Part H.

    For institutions participating in the NACUBO-Commonfund Study of Endowments (NCSE), this amount should be comparable with values 
    reported to NACUBO. NCSE asks that endowment information be reported as of June 30th regardless of WHEN the institution's fiscal year ends.
*/

--jh 20200218 added conditional for IPEDSClientConfig.finEndowmentAssets
select 'H',
	79,
	'1', --Value of endowment assets at the beginning of the fiscal year
	CAST(case when (select clientconfig.finEndowmentAssets
			from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.beginBalance))), 0) 
				else 0
					end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriodCode = 'Year Begin'
	and genledger.institParentChild = genledger.COASParentChild
	and genledger.accountType = 'Asset' 
	and genledger.isEndowment = 1

union

select 'H',
	80,
	'2', --Value of endowment assets at the END of the fiscal year
	CAST(case when (select clientconfig.finEndowmentAssets
		from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = genledger.COASParentChild
	and genledger.accountType = 'Asset' 
	and genledger.isEndowment = 1

union

select 'H',
	81,
	'3', --New gifts and additions
	0,
	/*CAST(case when (select clientconfig.finEndowmentAssets
		from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(oppledger.endBalance - oppledger.beginBalance))), 0) 
		 else 0
	end as BIGINT) */
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	'a'
/*from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild	
	and oppledger.revAddToPermEndowments = 'Y' */

union

select 'H',
	82,
	'3', --Endowment net investment return
	0,
	/*CAST(case when (select clientconfig.finEndowmentAssets
		from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(oppledger.endBalance - oppledger.beginBalance))), 0) 
		 else 0
	end as BIGINT)*/
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	'b'
/*from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revInvestmentIncome = 'Y' */
	
union

select 'H',
	83,
	'3', --"Spending Distribution for Current Use"
	0,
	/*CAST(case when (select clientconfig.finEndowmentAssets
		from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(oppledger.endBalance - oppledger.beginBalance))), 0) 
		 else 0
	end as BIGINT) */
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	'c'
/*from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild */
	
union

select 'N',
	84,
	'1', --Operating income (Loss) + net nonoperating revenues (expenses)
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger  
where oppledger.fiscalPeriodCode= 'Year End' 
    and oppledger.institParentChild = 'P'
   -- and oppledger.accountType = 'Revenue'

union

select 'N',
	85,
	'2', --Operating revenues + nonoperating revenues
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger  
where oppledger.fiscalPeriodCode= 'Year End' 
    and oppledger.institParentChild = 'P'
    and oppledger.accountType = 'Revenue'
	
union

-- Part three calculated same as Ipeds Instructions for part D-3: 
--"Change in net position during year – This amount is generated by subtracting line (D)02 (total expense) from line (D)01(Total revenue)."

select 'N',
	86,
	'3', --change in net position
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Revenue' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)
		- CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Expense' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger  
where oppledger.fiscalPeriodCode = 'Year End' 
    and oppledger.institParentChild = 'P'
   -- and oppledger.accountType = 'Revenue'
	
union

-- Part  N-4 calculated same as part D-3: 

select 'N',
	87,
	'4', --Net assets beginning of year
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN genledger.accountType = 'Asset' THEN genledger.beginBalance ELSE 0 END))), 0) as BIGINT) 
		- CAST(NVL(ABS(ROUND(SUM(CASE WHEN genledger.accountType = 'Liability' THEN genledger.beginBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year Begin' 
    and genledger.institParentChild = 'P'
    and genledger.isPensionGASB <> 1
    and genledger.isOPEBRelatedGASB <> 1
    and (genledger.accountType = 'Asset' 
		or genledger.accountType = 'Liability')
		
union

select 'N',
	88,
	'5', --Expendable net assets
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.accountType = 'Asset' 
    and genledger.isPensionGASB <> 1
    and genledger.isOPEBRelatedGASB <> 1
    and (genledger.isUnrestricted = 1 OR genledger.isRestrictedNonExpendOrPerm <> 1 )
    
union

-- Currently using "capital-related" debt. We may want to revisit this to see if we need to specify "plant-related" debt.

select 'N',
	89,
	'6', --Plant Related Debt
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.accountType = 'Liability' 
    and genledger.isPensionGASB <> 1
    and genledger.isOPEBRelatedGASB <> 1
    and genledger.isCapitalRelatedDebt = 1
    
union
				
select 'N',
	90,
	'7', --Total Expense
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger  
where oppledger.fiscalPeriodCode= 'Year End' 
    and oppledger.institParentChild = 'P'
    and oppledger.accountType = 'Expense'

union

--Part J   
-- Revenues for Bureau of Census
/*  General Instructions for Parts J, K and L
    Report data for the same fiscal year as reported in parts A through E. Report gross amounts but exclude interfund transfers. 
    Include the transactions of all funds of your institution. 
 */

-- Line 01 - Tuition and fees   (Do not include in file)
/*  All amounts will be obtained from Parts B and E. The Census Bureau includes tuition and fees from part B and 
    excludes discounts and allowances (applied to tuition) from Part E.
*/
 
-- Line 02 - Sales and services -- 
/*  Report separately only sales and service attributable to activities indicated for column 2 and column 5. 
    All other amounts will be obtained from Parts B and E, or will be calculated.
*/ 

select 'J',
	91,
	'2', --Sales and services
	NULL, --Total amount J2,1 --(Do not send in file, Calculated )
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 0 
										and oppledger.isAuxiliaryEnterprises = 0 
										and oppledger.isHospitalServices = 0) 
									and (oppledger.revEducActivSalesServices = 'Y' 
										or oppledger.revOtherSalesServices = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Education and general/independent operations  
	NULL, --Auxiliary enterprises J2,3
	NULL, --Hospitals J2,4
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.isAgricultureOrExperiment = 1 
										and oppledger.revOtherSalesServices = 'Y' 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and ((oppledger.isAgricultureOrExperiment = 0 
		and oppledger.isAuxiliaryEnterprises = 0 
		and oppledger.isHospitalServices = 0) 
		or (oppledger.isAgricultureOrExperiment = 1 
			and oppledger.revOtherSalesServices = 'Y'))

union

-- Line 3 - Federal grants and contracts (excluding Pell)
/*  Include both operating and non-operating grants, but exclude Pell and other student grants
	and any Federal loans received on behalf of the students. Include all other direct Federal
	grants, including research grants, in the appropriate column.
*/

select 'J',
	92,
	'3', --Federal grants and contracts (excluding Pell)
	NULL, --Total amount  -- (Do not include in file) 
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
										and oppledger.isAuxiliaryEnterprises = 0 
										and oppledger.isHospitalServices = 0) 
									and (oppledger.revFedGrantsContractsOper = 'Y' 
										or oppledger.revFedGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations 
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAuxiliaryEnterprises = 1 
										and (oppledger.revFedGrantsContractsOper = 'Y' 
											or oppledger.revFedGrantsContractsNOper = 'Y'))) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and (oppledger.revFedGrantsContractsOper = 'Y' 
											or oppledger.revFedGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  ,   --Hospitals  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 and (oppledger.revFedGrantsContractsOper = 'Y' or oppledger.revFedGrantsContractsNOper = 'Y')) THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (((oppledger.isAgricultureOrExperiment = 0 
		and oppledger.isAuxiliaryEnterprises = 0 
		and oppledger.isHospitalServices = 0) 
		and (oppledger.revFedGrantsContractsOper = 'Y' 
			or oppledger.revFedGrantsContractsNOper = 'Y'))
		or ((oppledger.isAuxiliaryEnterprises = 1 
			and (oppledger.revFedGrantsContractsOper = 'Y' 
				or oppledger.revFedGrantsContractsNOper = 'Y')))
		or (oppledger.isHospitalServices = 1 
			and (oppledger.revFedGrantsContractsOper = 'Y' 
				or oppledger.revFedGrantsContractsNOper = 'Y')))

union

-- Line 04 - State appropriations, current and capital
-- Include all operating and non-operating appropriations, as well as all current and capital appropriations. 

select 'J',
	93,
	'4', --State appropriations, current and capital
	NULL, --Total amount  
/*
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
											and oppledger.isAuxiliaryEnterprises = 0 
											and oppledger.isHospitalServices = 0) 
										and (oppledger.revStateApproprations = 'Y' 
											or oppledger.revStateCapitalAppropriations = 'Y')
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations 2-7
*/
    CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) and (oppledger.revStateApproprations = 'Y' or oppledger.revStateCapitalAppropriations = 'Y')) THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations 2-7


	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
										and (oppledger.revStateApproprations = 'Y' 
											or oppledger.revStateCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises 3-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and (oppledger.revStateApproprations = 'Y' 
											or oppledger.revStateCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Hospitals 3-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and (oppledger.revStateApproprations = 'Y' 
											or oppledger.revStateCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 2-7
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode= 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (((oppledger.isAgricultureOrExperiment = 0 
			and oppledger.isAuxiliaryEnterprises = 0 
			and oppledger.isHospitalServices = 0) 
		and (oppledger.revStateApproprations = 'Y' 
			or oppledger.revStateCapitalAppropriations = 'Y'))
		or (oppledger.isAuxiliaryEnterprises = 1 
			and (oppledger.revStateApproprations = 'Y' 
				or oppledger.revStateCapitalAppropriations = 'Y'))
		or (oppledger.isHospitalServices = 1 
			and (oppledger.revStateApproprations = 'Y' 
				or oppledger.revStateCapitalAppropriations = 'Y'))
		or (oppledger.isAgricultureOrExperiment = 1 
			and (oppledger.revStateApproprations = 'Y' 
				or oppledger.revStateCapitalAppropriations = 'Y')))

union

-- Line 5 - State grants and contracts
-- Include state grants and contracts, both operating and non-operating, in the proper column. Do not include state student grant aid.*/

select 'J',
	94,
	'5', --State grants and contracts
	NULL, --Total amount  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
											and oppledger.isAuxiliaryEnterprises = 0 
											and oppledger.isHospitalServices = 0) 
										and (oppledger.revStateGrantsContractsOper = 'Y' 
											or oppledger.revStateGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
											and (oppledger.revStateGrantsContractsOper = 'Y' 
												or oppledger.revStateGrantsContractsNOper = 'Y'))
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises 3-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
											and (oppledger.revStateGrantsContractsOper = 'Y' 
												or oppledger.revStateGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Hospitals 3-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
											and (oppledger.revStateGrantsContractsOper = 'Y' 
												or oppledger.revStateGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Agriculture extension/experiment services 2-7
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL   
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (((oppledger.isAgricultureOrExperiment = 0 
				and oppledger.isAuxiliaryEnterprises = 0 
				and oppledger.isHospitalServices = 0) 
			and (oppledger.revStateGrantsContractsOper = 'Y' 
				or oppledger.revStateGrantsContractsNOper = 'Y'))
		or (oppledger.isAuxiliaryEnterprises = 1 
			and (oppledger.revStateGrantsContractsOper = 'Y' 
				or oppledger.revStateGrantsContractsNOper = 'Y'))
		or (oppledger.isHospitalServices = 1 
			and (oppledger.revStateGrantsContractsOper = 'Y' 
				or oppledger.revStateGrantsContractsNOper = 'Y'))
		or (oppledger.isAgricultureOrExperiment = 1 
			and (oppledger.revStateGrantsContractsOper = 'Y' 
				or oppledger.revStateGrantsContractsNOper = 'Y')))

union

-- Line 06 - Local appropriations, current and capital
/*  Include local government appropriations in the appropriate column, regardless of whether appropriations were for 
    current or capital. This generally applies only to local institutions of higher education.
*/

select 'J',
	95,
	'6', --Local appropriations, current and capital
	NULL, --Total amount   -- (Do not include in file) 
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
											and oppledger.isAuxiliaryEnterprises = 0 
											and oppledger.isHospitalServices = 0) 
										and (oppledger.revLocalApproprations = 'Y' 
											or oppledger.revLocalCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
										and (oppledger.revLocalApproprations = 'Y' 
											or oppledger.revLocalCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises 3-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
											and (oppledger.revLocalApproprations = 'Y' 
												or oppledger.revLocalCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Hospitals 3-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
											and (oppledger.revLocalApproprations = 'Y' 
												or oppledger.revLocalCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 2-7
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (((oppledger.isAgricultureOrExperiment = 0 
		and oppledger.isAuxiliaryEnterprises = 0 
		and oppledger.isHospitalServices = 0) 
		and (oppledger.revLocalApproprations = 'Y' 
				or oppledger.revLocalCapitalAppropriations = 'Y'))
		or (oppledger.isAuxiliaryEnterprises = 1 
			and (oppledger.revLocalApproprations = 'Y' 
				or oppledger.revLocalCapitalAppropriations = 'Y'))
		or (oppledger.isHospitalServices = 1 
			and (oppledger.revLocalApproprations = 'Y' 
				or oppledger.revLocalCapitalAppropriations = 'Y'))
		or (oppledger.isAgricultureOrExperiment = 1 
			and (oppledger.revLocalApproprations = 'Y' 
				or oppledger.revLocalCapitalAppropriations = 'Y')))

union

-- Line 07 - Local grants and contracts
/*  Include state grants and contracts, both operating and non-operating, in the proper column. 
    Do not include state student grant aid.
*/

select 'J',
	96,
	'7', --Local grants and contracts
	NULL, --Total amount   -- (Do not include in file) 
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
											and oppledger.isAuxiliaryEnterprises = 0 
											and oppledger.isHospitalServices = 0) 
										and (oppledger.revLocalGrantsContractsOper = 'Y' 
											or oppledger.revLocalGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Education and general/independent operations 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
											and (oppledger.revLocalGrantsContractsOper = 'Y' 
										or oppledger.revLocalGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises 3-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
											and (oppledger.revLocalGrantsContractsOper = 'Y' 
												or oppledger.revLocalGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Hospitals 3-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
											and (oppledger.revLocalGrantsContractsOper = 'Y' 
												or oppledger.revLocalGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 2-7
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (((oppledger.isAgricultureOrExperiment = 0 
			and oppledger.isAuxiliaryEnterprises = 0 	
			and oppledger.isHospitalServices = 0) 
		and (oppledger.revLocalGrantsContractsOper = 'Y' 
			or oppledger.revLocalGrantsContractsNOper = 'Y'))
		or (oppledger.isAuxiliaryEnterprises = 1 
			and (oppledger.revLocalGrantsContractsOper = 'Y' 
				or oppledger.revLocalGrantsContractsNOper = 'Y'))
		or (oppledger.isHospitalServices = 1 
			and (oppledger.revLocalGrantsContractsOper = 'Y' 
				or oppledger.revLocalGrantsContractsNOper = 'Y'))
		or (oppledger.isAgricultureOrExperiment = 1 
			and (oppledger.revLocalGrantsContractsOper = 'Y' 
				or oppledger.revLocalGrantsContractsNOper = 'Y')))
	
union

-- Line 08 - Receipts from property and non-property taxes - Total all funds
/*  This item applies only to local institutions of higher education. Include in column 1 any revenue from locally imposed property taxes or
    other taxes levied by the local higher education district. Include all funds - current, restricted, unrestricted and debt service.
    Exclude taxes levied by another government and transferred to the local higher education district by the levying government. 
*/

select 'J',
	97,
	'8', --Receipts from property and non-property taxes - Total all funds
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL, 
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revPropAndNonPropTaxes = 'Y'

union

-- Line 09 - Gifts and private grants, NOT including capital grants 
/*  Include grants from private organizations and individuals here. Include additions to
    permanent endowments if they are gifts. Exclude gifts to component units and capital contributions.   
*/
 
select 'J',
	98,
	'9', --Gifts and private grants, NOT including capital grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revPrivGrantsContractsOper = 'Y' 
		or oppledger.revPrivGrantsContractsNOper = 'Y' 
		or oppledger.revPrivGifts = 'Y' 
		or oppledger.revAffiliatedOrgnGifts = 'Y')

union

-- Line 10 - Interest earnings
-- Report the total interest earned in column 1. Include all funds and endowments.  

select 'J',
	99,
	'10', --Interest earnings
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild 
	and oppledger.revInterestEarnings = 'Y'

union

-- Line 11 - Dividend earnings
/*  Dividends should be reported separately if available. Report only the total, in column 1,
    from all funds including endowments but excluding dividends of any component units. Note: if
    dividends are not separately available, please report include with Interest earnings in J10, column 1. 
*/

select 'J',
	100,
	'11', --Dividend earnings
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild  
	and oppledger.revDividendEarnings = 'Y'

union

-- Line 12 - Realized capital gains
/*  Report only the total earnings. Do not include unrealized gains.
    Also, include all other miscellaneous revenue. Use column 1 only. 
*/

select 'J',
	101,
	'12', --Realized capital gains
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild 
	and oppledger.revRealizedCapitalGains = 'Y'
	
union

-- Part K   
-- Expenditures for Bureau of the Census 
 
-- Line 02 - Employee benefits
/* 	Report the employee benefits for staff associated with Education and General, Auxiliary Enterprises,
	Hospitals, and for Agricultural extension/experiment services, if applicable. 
*/

select 'K',
	102,
	'2', --Employee benefits, total
	NULL, --Total amount 8
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
											and oppledger.isAuxiliaryEnterprises = 0 
											and oppledger.isHospitalServices = 0) 
										and (oppledger.expBenefits = 'Y' 
											or oppledger.expOperMaintBenefits = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
											and (oppledger.expBenefits = 'Y' 
												or oppledger.expOperMaintBenefits = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and (oppledger.expBenefits = 'Y' 
											or oppledger.expOperMaintBenefits = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  K24, --Hospitals 
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and (oppledger.expBenefits = 'Y' 
											or oppledger.expOperMaintBenefits = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services  
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild 
	and (((oppledger.isAgricultureOrExperiment = 0 
			and oppledger.isAuxiliaryEnterprises = 0 
			and oppledger.isHospitalServices = 0)
				and (oppledger.expBenefits = 'Y' 
					or oppledger.expOperMaintBenefits = 'Y'))
        or (oppledger.isAuxiliaryEnterprises = 1 
			and (oppledger.expBenefits = 'Y' 
				or oppledger.expOperMaintBenefits = 'Y'))
        or (oppledger.isHospitalServices = 1 
			and (oppledger.expBenefits = 'Y' 
				or oppledger.expOperMaintBenefits = 'Y'))
        or (oppledger.isAgricultureOrExperiment = 1 
			and (oppledger.expBenefits = 'Y' 
				or oppledger.expOperMaintBenefits = 'Y')))

union

-- Line 03 - Payment to state retirement funds
/*  Applies to state institutions only. Include amounts paid to retirement systems operated by
    your state government only. Include employer contributions only. Exclude employee contributions withheld. 
*/

select 'K',
	103,
	'3', --Payment to state retirement funds
	NULL, --Total amount  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
											and oppledger.isAuxiliaryEnterprises = 0 
											and oppledger.isHospitalServices = 0) 
										and oppledger.accountType = 'Expense' 
										and oppledger.isStateRetireFundGASB = 1) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
										and oppledger.accountType = 'Expense' 
										and oppledger.isStateRetireFundGASB = 1) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Auxiliary enterprises  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and oppledger.accountType = 'Expense' 
										and oppledger.isStateRetireFundGASB = 1) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Hospitals  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and oppledger.accountType = 'Expense' 
										and oppledger.isStateRetireFundGASB = 1) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Agriculture extension/experiment services  
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild 
	and (((oppledger.isAgricultureOrExperiment = 0 
			and oppledger.isAuxiliaryEnterprises = 0 
			and oppledger.isHospitalServices = 0) 
				and oppledger.accountType = 'Expense' 
				and oppledger.isStateRetireFundGASB = 1)
		or (oppledger.isAuxiliaryEnterprises = 1 
			and oppledger.accountType = 'Expense' 
			and oppledger.isStateRetireFundGASB = 1)
		or (oppledger.isHospitalServices = 1 
			and oppledger.accountType = 'Expense' 
			and oppledger.isStateRetireFundGASB = 1)
		or (oppledger.isAgricultureOrExperiment = 1 
			and oppledger.accountType = 'Expense' 
			and oppledger.isStateRetireFundGASB = 1))

union

-- Line 04 - Current expenditures including salaries
/*  Report all current expenditures including salaries, employee benefits, supplies, materials, contracts and professional services, 
    utilities, travel, and insurance.  Exclude scholarships and fellowships, capital outlay, interest(report on line 8), 
    employer contributions to state retirement systems (applies to state institutions only) and depreciation. 
*/

select 'K',
	104,
	'4', --Current expenditures including salaries
	NULL, --Total amount 8
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
											and oppledger.isAuxiliaryEnterprises = 0 
											and oppledger.isHospitalServices = 0) 
										and ((oppledger.expSalariesWages = 'Y' 
											or oppledger.expOperMaintSalariesWages = 'Y' 
											or oppledger.expOperMaintOther = 'Y' 
											or oppledger.expOther = 'Y') 
										and oppledger.isStateRetireFundGASB = 1)) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Education and general/independent operations 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
											and ((oppledger.expSalariesWages = 'Y' 
												or oppledger.expOperMaintSalariesWages = 'Y' 
												or oppledger.expOperMaintOther = 'Y' 
												or oppledger.expOther = 'Y') 
											and oppledger.isStateRetireFundGASB = 1)) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
											and ((oppledger.expSalariesWages = 'Y' 
												or oppledger.expOperMaintSalariesWages = 'Y' 
												or oppledger.expOperMaintOther = 'Y' 
												or oppledger.expOther = 'Y') 
											and oppledger.isStateRetireFundGASB = 1)) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Hospitals 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
											and ((oppledger.expSalariesWages = 'Y' 
												or oppledger.expOperMaintSalariesWages = 'Y' 
												or oppledger.expOperMaintOther = 'Y' 
												or oppledger.expOther = 'Y') 
											and oppledger.isStateRetireFundGASB = 1)) 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 2-7
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild 
		and (((oppledger.isAgricultureOrExperiment = 0 
			and oppledger.isAuxiliaryEnterprises = 0 
			and oppledger.isHospitalServices = 0) 
				and ((oppledger.expSalariesWages = 'Y' 
					or oppledger.expOperMaintSalariesWages = 'Y' 
					or oppledger.expOperMaintOther = 'Y' 
					or oppledger.expOther = 'Y') 
				and oppledger.isStateRetireFundGASB = 1))
		or (oppledger.isAuxiliaryEnterprises = 1 
			and ((oppledger.expSalariesWages = 'Y' 
				or oppledger.expOperMaintSalariesWages = 'Y' 
				or oppledger.expOperMaintOther = 'Y' 
				or oppledger.expOther = 'Y') 
			 and oppledger.isStateRetireFundGASB = 1))
	   or (oppledger.isHospitalServices = 1 
			and ((oppledger.expSalariesWages = 'Y' 
				or oppledger.expOperMaintSalariesWages = 'Y' 
				or oppledger.expOperMaintOther = 'Y' 
				or oppledger.expOther = 'Y') 
			 and oppledger.isStateRetireFundGASB = 1))
	   or (oppledger.isAgricultureOrExperiment = 1 
			and ((oppledger.expSalariesWages = 'Y' 
				or oppledger.expOperMaintSalariesWages = 'Y' 
				or oppledger.expOperMaintOther = 'Y' 
				or oppledger.expOther = 'Y') 
			 and oppledger.isStateRetireFundGASB = 1)))

union

-- Line 05 - Capital outlay, construction
/*  Construction from all funds (plant, capital, or bond funds) includes expenditure for the construction of new structures and other
    permanent improvements, additions replacements, and major alterations. Report in proper column according to function. 
*/

select 'K',
	105,
	'5', --Capital outlay, construction
	NULL, --Total amount -- K5,1    -- (Do not include in file) 
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
											and oppledger.isAuxiliaryEnterprises = 0 
											and oppledger.isHospitalServices = 0) 
										and oppledger.expCapitalConstruction = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
											and oppledger.expCapitalConstruction = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
											and oppledger.expCapitalConstruction = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Hospitals  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
											and oppledger.expCapitalConstruction = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Agriculture extension/experiment services  
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild 
		and (((oppledger.isAgricultureOrExperiment = 0 
				and oppledger.isAuxiliaryEnterprises = 0 	
				and oppledger.isHospitalServices = 0) 
			and oppledger.expCapitalConstruction = 'Y')
		or (oppledger.isAuxiliaryEnterprises = 1 
			and oppledger.expCapitalConstruction = 'Y')
		or (oppledger.isHospitalServices = 1 
			and oppledger.expCapitalConstruction = 'Y')
		or (oppledger.isAgricultureOrExperiment = 1 
			and oppledger.expCapitalConstruction = 'Y'))

union

--  Line 06 - Capital outlay, equipment purchases
--  Equipment purchases from all funds (plant, capital, or bond funds). 
 
select 'K',
	106,
	'6',  --Capital outlay, equipment purchases
	NULL, --Total amount   -- K6,1    -- (Do not include in file) 
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
											and oppledger.isAuxiliaryEnterprises = 0 
											and oppledger.isHospitalServices = 0) 
										and oppledger.expCapitalEquipPurch = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Education and general/independent operations 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
										and oppledger.expCapitalEquipPurch = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and oppledger.expCapitalEquipPurch = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Hospitals  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and oppledger.expCapitalEquipPurch = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Agriculture extension/experiment services  
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild 
		and (((oppledger.isAgricultureOrExperiment = 0 
			and oppledger.isAuxiliaryEnterprises = 0 
			and oppledger.isHospitalServices = 0) 
		and oppledger.expCapitalEquipPurch = 'Y')
		or (oppledger.isAuxiliaryEnterprises = 1 
			and oppledger.expCapitalEquipPurch = 'Y')
		or (oppledger.isHospitalServices = 1 
			and oppledger.expCapitalEquipPurch = 'Y')
		or (oppledger.isAgricultureOrExperiment = 1 
			and oppledger.expCapitalEquipPurch = 'Y'))

union

-- Line 07 - Capital outlay, land purchases
/*  from all funds (plant, capital, or bond funds), include the cost of land and existing structures, as well as the purchase of rights-of-way.
    Include all capital outlay other than Construction if not specified elsewhere. 
*/

select 'K',
	107,
	'7',  --Capital outlay, land purchases
	NULL, --Total amount  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and oppledger.expCapitalLandPurchOther = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
										and oppledger.expCapitalLandPurchOther = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Auxiliary enterprises  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and oppledger.expCapitalLandPurchOther = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Hospitals  
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and oppledger.expCapitalLandPurchOther = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild 
	and (((oppledger.isAgricultureOrExperiment = 0 
			and oppledger.isAuxiliaryEnterprises = 0 
			and oppledger.isHospitalServices = 0) 
			and oppledger.expCapitalLandPurchOther = 'Y')
        or (oppledger.isAuxiliaryEnterprises = 1 
			and oppledger.expCapitalLandPurchOther = 'Y')
        or (oppledger.isHospitalServices = 1 
			and oppledger.expCapitalLandPurchOther = 'Y')
        or (oppledger.isAgricultureOrExperiment = 1 
			and oppledger.expCapitalLandPurchOther = 'Y'))

union

-- Line 08 - Interest paid on revenue debt only. 
/*  Includes interest on debt issued by the institution, such as that which is repayable from pledged earnings, 
    charges or gees (e.g. dormitory, stadium, or student union revenue bonds). Report only the total, 
    in column 1. Excludes interest expenditure of the parent state or local government on debt issued on behalf 
    of the institution and backed by that parent government. Also excludes interest on debt issued by a state 
    dormitory or housing finance agency on behalf of the institution. 
*/

select 'K',
	108,
	'8', --Interest on debt outstanding, all funds and activities
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriodCode = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild 
	and oppledger.expInterest = 'Y'

union

-- Part L   
-- Debt and Assets for Census Bureau 

/*  Lines 01 through 06 - Include all debt issued in the name of the institution. Long-term debt and short-term debt are 
    distinguished by length of term for repayment, with one year being the boundary. Short-term debt must be interest bearing. 
    Do not include the current portion of long-term debt as short-term debt. Instead include this in the total long-term debt outstanding. 

    Lines 07, 08, and 09 - Report the total amount of cash and security assets held in each category. Report assets at book 
    value to the extent possible. Includes cash on hand in each type of fund. Sinking funds are those used exclusively to service debt. 
    Bond funds are those established by your institution to disburse revenue bond proceeds. All other funds might include current, 
    plant, or endowment funds. Exclude the value of fixed assets and exclude any student loan funds established by the Federal government.
*/

select 'L',
	109,
	'1', --Long-term debt outstanding at beginning of fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.beginBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year Begin'
	and genledger.institParentChild = 'P'
--jh 20200107 added parenthesis for or clause
	and (genledger.liabCurrentLongtermDebt = 'Y' 
		or genledger.liabNoncurrentLongtermDebt = 'Y')

union

-- Line 02 - Long-term debt issued during fiscal year  

--jdh 2020-03-30 Janet & JD discussed: 
--  Decision was made to identify Long-Term debt acquired as 
--  Any long-term Accounts that did not exist or had a balance of 0 at the begining of FY
--  and had a balance greater than 0 at the end of the FY. 

select 'L',
	110,
	'2', --Long-term debt issued during fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), -- Amount of LongTermDebt acquired
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
	left Join GeneralLedgerMCR genledger1 on genledger.accountingString = genledger1.accountingString
		and genledger1.fiscalPeriodCode = 'Year Begin'
where genledger.fiscalPeriodCode = 'Year End' 
	and genledger.institParentChild = 'P'  
	and (genledger.liabCurrentLongtermDebt = 'Y' 
		or genledger.liabNoncurrentLongtermDebt = 'Y')
	and  genledger.endBalance > 0
	and (genledger1.chartOfAccountsId is null 
		or NVL(genledger1.beginBalance,0) <= 0)
union

-- Line 03 - Long-term debt retired during fiscal year 

--jdh 2020-03-30 Janet & JD discussed: 
--  Decision was made to identify Long-Term debt retired as 
--  Any long-term Accounts that had a balance greater than 0 at the begining of FY
--  and had a balance of 0 at the end of the FY.  

select 'L',
	111,
	'3', --Long-term debt retired during fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.beginBalance))), 0) as BIGINT), -- Amount of LongTermDebt retired
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
left Join GeneralLedgerMCR genledger1 on genledger.accountingString = genledger1.accountingString
	and genledger1.fiscalPeriodCode = 'Year End'
where genledger.fiscalPeriodCode = 'Year Begin' 
	and genledger.institParentChild = 'P'  
	and (genledger.liabCurrentLongtermDebt = 'Y' 
		or genledger.liabNoncurrentLongtermDebt = 'Y')
	and  genledger.beginBalance > 0
    and (genledger1.chartOfAccountsId is null 
		or NVL(genledger1.endBalance,0) <= 0)

union

-- Line 04 - Long-term debt outstanding at END of fiscal year

select 'L',
	112,
	'4', --Long-term debt outstanding at END of fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and (genledger.liabCurrentLongtermDebt = 'Y' 
		or genledger.liabNoncurrentLongtermDebt = 'Y')

union

select 'L',
	113,
	'5', --Short-term debt outstanding at beginning of fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.beginBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year Begin'
	and genledger.institParentChild = 'P'
--jh 20200107 added parenthesis for or clause
	and (genledger.liabCurrentOther = 'Y' 
		or genledger.liabNoncurrentOther = 'Y') 

union

-- Line 06 - Short-term debt outstanding at END of fiscal year

select 'L',
	114,
	'6', --Short-term debt outstanding at END of fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT)  ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
--jh 20200107 added parenthesis for or clause
	and (genledger.liabCurrentOther = 'Y' 
		or genledger.liabNoncurrentOther = 'Y')

union

/*  Lines 07, 08, and 09 - 
    Report the total amount of cash and security assets held in each category.
    Report assets at book value to the extent possible. Includes cash on hand in each type of fund. Sinking
    funds are those used exclusively to service debt. Bond funds are those established by your institution to
    disburse revenue bond proceeds. All other funds might include current, plant, or endowment funds. Exclude
    the value of fixed assets and exclude any student loan funds established by the Federal government. 
*/ 

-- Line 07 - Total cash and security assets held at END of fiscal year in sinking or debt service funds

select 'L',
	115,
	'7', --Total cash and security assets held at END of fiscal year in sinking or debt service funds
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset'
	and genledger.isSinkingOrDebtServFundGASB = 1 
	and genledger.isCashOrSecurityAssetGASB = 1

union

-- Line 08 - Total cash and security assets held at END of fiscal year in bond funds

select 'L',
	116,
	'8', --Total cash and security assets held at END of fiscal year in bond funds
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset' 
	and genledger.isBondFundGASB = 1 
	and genledger.isCashOrSecurityAssetGASB = 1

union

-- Line 09 - Total cash and security assets held at END of fiscal year in all other funds

select 'L',
	117,
	'9', --Total cash and security assets held at END of fiscal year in all other funds
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriodCode = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset' 
	and genledger.isNonBondFundGASB = 1 
	and genledger.isCashOrSecurityAssetGASB = 1

order by sort
