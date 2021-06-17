/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Spring Collection
FILE NAME:      Finance v2 (F2B) 
FILE DESC:      Finance for degree-granting private, not-for-profit institutions and public institutions using FASB Reporting Standards
AUTHOR:         Janet Hanicak
CREATED:        20191220

SECTIONS:
    Reporting Dates 
    Most Recent Records
    Survey Formatting

SUMMARY OF CHANGES

Date(yyyymmdd)      Author              Tag             Comments
-----------------   ----------------    -------------   -------------------------------------------------
20200622			akhasawneh			ak 20200622		 Modify Finance report query with standardized view naming/aliasing convention (PF-1532) -Run time 7m 28s	
20200412			jhanicak			jh 20204012		 Added dummy date option for recordActivityDate in most current record queries PF-1374
														 Added DefaultValues query and rewrote other queries to use PF-1418
														 Removed all 'prior' queries - not needed
20200303            jhysler             jdh 2020-03-03  PF-1297 Modified all sections to pull from OperatingLedgerMCR or genledger and added parent/child logic 
20200226            jhanicak            jh 20200226     PF-1257
                                                        Added most recent record views for GeneralLedgerReporting and OperatorLedgerReporting
                                                        Removed FYPerAsOfDate/FYPerPriorAsOfDate
                                                        Modified COASPerFYAsOfDate/COASPerFYPriorAsOfDate to include IPEDSClientConfig values
                                                        and most recent records based on COASPerAsOfDate/COASPerPriorAsOfDate and FiscalYear
                                                        Removed join of ClientConfigMCR since config values are in COASPerFYAsOfDate
                                                        Modified all sections to pull from OperatingLedgerMCR or genledger and added parent/child logic
20200218            jhanicak            jh 20200218	PF-1254 Added default values for IPEDSReportingPeriod and IPEDSClientConfig
                                                        Added Config values/conditionals for Part H
20200103            jd.hysler                           Move original code to template 
20191220            jhanicak                            Initial version

********************/
 
/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

--jh 20200412 Added DefaultValues query and rewrote other queries to use PF-1418
WITH DefaultValues AS
(
select '1920' surveyYear,
	CAST(UNIX_TIMESTAMP('10/01/2019', 'MM/dd/yyyy') AS TIMESTAMP) asOfDate,      
	CAST(UNIX_TIMESTAMP('10/01/2018', 'MM/dd/yyyy') AS TIMESTAMP) priorAsOfDate, 
	'F2B' surveyId,
	'Current' currentSection,
	'Prior' priorSection,
	'U' finGPFSAuditOpinion,  --U = unknown/in progress -- all versions
	'A' finAthleticExpenses,  --A = Auxiliary Enterprises -- all versions
	'Y' finEndowmentAssets,  --Y = Yes -- all versions
	'Y' finPensionBenefits,  --Y = Yes -- all versions
	'B' finReportingModel,  --B = Business -- v1, v4
	'P' finPellTransactions, --P = Pass through -- v2, v3, v5, v6
	'LLC' finBusinessStructure, --LLC = Limited liability corp -- v3, v6
	'M' finTaxExpensePaid, --M = multi-institution or multi-campus organization indicated in IC Header -- v6
	'P' finParentOrChildInstitution  --P = Parent -- v1, v2, v3
	    
/*
--used for internal testing only
select '1415' surveyYear,
	CAST(UNIX_TIMESTAMP('10/01/2014', 'MM/dd/yyyy') AS TIMESTAMP) asOfDate,     
	CAST(UNIX_TIMESTAMP('10/01/2013', 'MM/dd/yyyy') AS TIMESTAMP) priorAsOfDate, 
	'F2B' surveyId,
	'Current' currentSection,
	'Prior' priorSection,
	'U' finGPFSAuditOpinion,  --U = unknown/in progress -- all versions
	'A' finAthleticExpenses,  --A = Auxiliary Enterprises -- all versions
	'Y' finEndowmentAssets,  --Y = Yes -- all versions
	'Y' finPensionBenefits,  --Y = Yes -- all versions
	'B' finReportingModel,  --B = Business -- v1, v4
	'P' finPellTransactions, --P = Pass through -- v2, v3, v5, v6
	'LLC' finBusinessStructure, --LLC = Limited liability corp -- v3, v6
	'M' finTaxExpensePaid, --M = multi-institution or multi-campus organization indicated in IC Header -- v6
	'P' finParentOrChildInstitution  --P = Parent -- v1, v2, v3
*/
),

--jh 20200412 Added DefaultValues query and rewrote other queries to use PF-1418
ReportingPeriodMCR AS
(
select repValues.surveyYear surveyYear,
   repValues.surveyId surveyId,
   MAX(repValues.asOfDate) asOfDate,
   MAX(repValues.priorAsOfDate) priorAsOfDate,
   repValues.currentSection currentSection,
   repValues.priorSection priorSection,
   repValues.finGPFSAuditOpinion finGPFSAuditOpinion,
   repValues.finAthleticExpenses finAthleticExpenses,
   repValues.finEndowmentAssets finEndowmentAssets,
   repValues.finPensionBenefits finPensionBenefits,
   repValues.finPellTransactions finPellTransactions,
   repValues.finParentOrChildInstitution finParentOrChildInstitution
from 
    (select NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.currentSection) THEN repPeriodENT.asOfDate END, defvalues.asOfDate) asOfDate,
		NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.priorSection) THEN repPeriodENT.asOfDate END, defvalues.priorAsOfDate) priorAsOfDate,
		repPeriodENT.surveyCollectionYear surveyYear,
		repPeriodENT.surveyId surveyId,
		defvalues.currentSection currentSection,
		defvalues.priorSection priorSection,
		defvalues.finGPFSAuditOpinion finGPFSAuditOpinion,
		defvalues.finAthleticExpenses finAthleticExpenses,
		defvalues.finEndowmentAssets finEndowmentAssets,
		defvalues.finPensionBenefits finPensionBenefits,
		defvalues.finPellTransactions finPellTransactions,
		defvalues.finParentOrChildInstitution finParentOrChildInstitution,
		ROW_NUMBER() OVER (
			PARTITION BY
				repPeriodENT.surveyCollectionYear,
				repPeriodENT.surveyId,
				repPeriodENT.surveySection
			ORDER BY
				repPeriodENT.recordActivityDate DESC
		) AS reportPeriodRn
	from DefaultValues defvalues
		cross join IPEDSReportingPeriod repPeriodENT
	where repPeriodENT.surveyCollectionYear = defvalues.surveyYear
		and repPeriodENT.surveyId = defvalues.surveyId
	) repValues 
where repValues.reportPeriodRn = 1
group by repValues.surveyYear, 
	repValues.surveyId,
	repValues.currentSection,
	repValues.priorSection,
	repValues.finGPFSAuditOpinion,
	repValues.finAthleticExpenses,
	repValues.finEndowmentAssets,
	repValues.finPensionBenefits,
	repValues.finPellTransactions,
	repValues.finParentOrChildInstitution
union

select defvalues.surveyYear surveyYear,
	defvalues.surveyId surveyId,
	defvalues.asOfDate asOfDate,
	defvalues.priorAsOfDate priorAsOfDate,
	defvalues.currentSection currentSection,
	defvalues.priorSection priorSection,
	defvalues.finGPFSAuditOpinion finGPFSAuditOpinion,
	defvalues.finAthleticExpenses finAthleticExpenses,
	defvalues.finEndowmentAssets finEndowmentAssets,
	defvalues.finPensionBenefits finPensionBenefits, 
	defvalues.finPellTransactions finPellTransactions,
	defvalues.finParentOrChildInstitution finParentOrChildInstitution
from DefaultValues defvalues
where defvalues.surveyYear not in (select surveyYear
                                   from DefaultValues defValues1
									cross join IPEDSReportingPeriod repPeriodENT
                                   where repPeriodENT.surveyCollectionYear = defValues1.surveyYear
									and repPeriodENT.surveyId = defValues1.surveyId)
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

--jh 20200412 Added DefaultValues query and rewrote other queries to use PF-1418

ClientConfigMCR AS
--Pulls client-given data for part 9, including:
--finGPFSAuditOpinion - all versions
--finAthleticExpenses - all versions
--finEndowmentAssets - all versions
--finPensionBenefits - all versions
--finReportingModel - v1, v4
--finPellTransactions - v2, v3, v5, v6
--finBusinessStructure - v3, v6
--finTaxExpensePaid - v6
--finParentOrChildInstitution - v1, v2, v3

(
select surveyCollectionYear surveyYear,
	asOfDate asOfDate,
	priorAsOfDate priorAsOfDate,
	finGPFSAuditOpinion finGPFSAuditOpinion,
	finAthleticExpenses finAthleticExpenses,
	finEndowmentAssets finEndowmentAssets,
	finPensionBenefits finPensionBenefits,
	finPellTransactions finPellTransactions,
	finParentOrChildInstitution finParentOrChildInstitution
from (
    select repperiod.surveyYear surveyCollectionYear,
	   repperiod.asOfDate asOfDate,
	   repperiod.priorAsOfDate priorAsOfDate,
	   NVL(clientConfigENT.finGPFSAuditOpinion, repperiod.finGPFSAuditOpinion) finGPFSAuditOpinion,
	   NVL(clientConfigENT.finAthleticExpenses, repperiod.finAthleticExpenses) finAthleticExpenses,
	   NVL(clientConfigENT.finEndowmentAssets, repperiod.finEndowmentAssets) finEndowmentAssets,
	   NVL(clientConfigENT.finPensionBenefits, repperiod.finPensionBenefits) finPensionBenefits,
	   NVL(clientConfigENT.finPellTransactions, repperiod.finPellTransactions) finPellTransactions,
	   NVL(clientConfigENT.finParentOrChildInstitution, repperiod.finParentOrChildInstitution) finParentOrChildInstitution,
	   ROW_NUMBER() OVER (
			PARTITION BY
				clientConfigENT.surveyCollectionYear
			ORDER BY
				clientConfigENT.recordActivityDate DESC
	   ) AS configRn
    from IPEDSClientConfig clientConfigENT
		inner join ReportingPeriodMCR repperiod on repperiod.surveyYear = clientConfigENT.surveyCollectionYear
      )
where configRn = 1

union

select repperiod.surveyYear,
	repperiod.asOfDate asOfDate,
	repperiod.priorAsOfDate priorAsOfDate,
	repperiod.finGPFSAuditOpinion finGPFSAuditOpinion,
	repperiod.finAthleticExpenses finAthleticExpenses,
	repperiod.finEndowmentAssets finEndowmentAssets,
	repperiod.finPensionBenefits finPensionBenefits, 
	repperiod.finPellTransactions finPellTransactions,
	repperiod.finParentOrChildInstitution finParentOrChildInstitution
from ReportingPeriodMCR repperiod
where repperiod.surveyYear not in (select clientconfigENT.surveyCollectionYear
								   from IPEDSClientConfig clientconfigENT
									inner join ReportingPeriodMCR repperiod1 
										on repperiod1.surveyYear = clientconfigENT.surveyCollectionYear)
),

ChartOfAccountsMCR AS (
select *
from (
    select coasENT.*,
		ROW_NUMBER() OVER (
			PARTITION BY
				coasENT.chartOfAccountsId
			ORDER BY
				coasENT.startDate DESC,
				coasENT.recordActivityDate DESC
		) AS coasRn
    from ClientConfigMCR clientconfig
		inner join ChartOfAccounts coasENT ON coasENT.startDate <= clientconfig.asOfDate
--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
			and ((coasENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) 
				and coasENT.recordActivityDate <= clientconfig.asOfDate)
					or coasENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
			and (coasENT.endDate IS NULL or coasENT.endDate >= clientconfig.asOfDate)
			and coasENT.statusCode = 'Active'  
			and coasENT.isIPEDSReportable = 1
  )
where COASRn = 1
),

--jh 20200412 Added DefaultValues query and rewrote other queries to use PF-1418

FiscalYearMCR AS (
select FYData.asOfDate asOfDate,
	FYData.priorAsOfDate priorAsOfDate,
	CASE FYData.finGPFSAuditOpinion
		when 'Q' then 1
		when 'UQ' then 2
		when 'U' then 3 END finGPFSAuditOpinion, --1=Unqualified, 2=Qualified, 3=Don't know
	CASE FYData.finAthleticExpenses 
		when 'A' then 1
		when 'S' then 2 
		when 'N' then 3 
		when 'O' then 4 END finAthleticExpenses, --1=Auxiliary enterprises 2=Student services 3=Does not participate in intercollegiate athletics 4=Other (specify in caveats box below)
	CASE FYData.finPellTransactions
		when 'P' then 1
		when 'F' then 2
		when 'N' then 3 END finPellTransactions,
	FYData.finParentOrChildInstitution institParentChild,
	FYData.fiscalYear4Char fiscalYear4Char,
	FYData.fiscalYear2Char fiscalYear2Char,
	FYData.fiscalPeriodCode fiscalPeriodCode,
	--case when FYData.fiscalPeriod = '1st Month' then 'Year Begin' else 'Year End' end fiscalPeriod, --test only
	FYData.fiscalPeriod fiscalPeriod,
	FYData.startDate startDate,
	FYData.endDate endDate,
	FYData.chartOfAccountsId chartOfAccountsId,
	case when coas.isParent = 1 then 'P'
		when coas.isChild = 1 then 'C' end COASParentChild
from (select fiscalyearENT.*,
			clientconfig.asOfDate asOfDate,
			clientconfig.priorAsOfDate priorAsOfDate,
			clientconfig.finGPFSAuditOpinion finGPFSAuditOpinion,                
			clientconfig.finAthleticExpenses finAthleticExpenses,
			clientconfig.finPellTransactions finPellTransactions,
			clientconfig.finParentOrChildInstitution finParentOrChildInstitution,
			ROW_NUMBER() OVER (
			PARTITION BY
				fiscalyearENT.chartOfAccountsId,
				fiscalyearENT.fiscalYear4Char,
				fiscalyearENT.fiscalPeriodCode
			ORDER BY
				fiscalyearENT.fiscalYear4Char DESC,
				fiscalyearENT.fiscalPeriodCode DESC,
				fiscalyearENT.recordActivityDate DESC
			) AS FYRn
	from ClientConfigMCR clientconfig
		left join FiscalYear fiscalyearENT on fiscalyearENT.startDate <= clientconfig.asOfDate
			and fiscalyearENT.fiscalPeriod in ('Year Begin', 'Year End')
			--and fiscalyearENT.fiscalPeriod in ('1st Month', '12th Month') --test only
			--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
			and ((fiscalyearENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) 
				and fiscalyearENT.recordActivityDate <= clientconfig.asOfDate)
					or fiscalyearENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
			and (fiscalyearENT.endDate IS NULL or fiscalyearENT.endDate >= clientconfig.asOfDate)
			and fiscalyearENT.isIPEDSReportable = 1
	) FYData 
	left join ChartOfAccountsMCR coas on FYData.chartOfAccountsId = coas.chartOfAccountsId	
where FYData.FYRn = 1
),

--jh 20200226 Added most recent record views for GeneralLedgerReporting

GeneralLedgerMCR AS (
select *
from (
    select genledgerENT.*,
		fiscalyr.fiscalPeriod fiscalPeriod,
		fiscalyr.institParentChild institParentChild, --is the institution a Parent or Child 
		fiscalyr.COASParentChild COASParentChild,     --is the COAS a Parent or Child account
		ROW_NUMBER() OVER (
            PARTITION BY
                genledgerENT.chartOfAccountsId,
                genledgerENT.fiscalYear2Char,
		        genledgerENT.fiscalPeriodCode,
		        genledgerENT.accountingString
            ORDER BY
                genledgerENT.recordActivityDate DESC
		) AS genledgerRn
    from FiscalYearMCR fiscalyr
		left join GeneralLedgerReporting genledgerENT on genledgerENT.chartOfAccountsId = fiscalyr.chartOfAccountsId
			and genledgerENT.fiscalYear2Char = fiscalyr.fiscalYear2Char  
			and genledgerENT.fiscalPeriodCode = fiscalyr.fiscalPeriodCode
		--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
			and ((genledgerENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) 
				and genledgerENT.recordActivityDate <= fiscalyr.asOfDate)
					or genledgerENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
			and genledgerENT.isIPEDSReportable = 1
  )
where genledgerRn = 1
),

OperatingLedgerMCR AS (
select *
from (
    select oppledgerENT.*,
		fiscalyr.fiscalPeriod fiscalPeriod,
	    fiscalyr.institParentChild institParentChild,--is the institution a Parent or Child
	    fiscalyr.COASParentChild COASParentChild,    --is the COAS a Parent or Child account
		ROW_NUMBER() OVER (
            PARTITION BY
                oppledgerENT.chartOfAccountsId,
                oppledgerENT.fiscalYear2Char,
		        oppledgerENT.fiscalPeriodCode,
		        oppledgerENT.accountingString
            ORDER BY
                oppledgerENT.recordActivityDate DESC
		) AS oppledgerRn
    from FiscalYearMCR fiscalyr
		left join OperatingLedgerReporting oppledgerENT on oppledgerENT.chartOfAccountsId = fiscalyr.chartOfAccountsId
			and oppledgerENT.fiscalYear2Char = fiscalyr.fiscalYear2Char  
			and oppledgerENT.fiscalPeriodCode = fiscalyr.fiscalPeriodCode
		--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
			and ((oppledgerENT.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) 
				and oppledgerENT.recordActivityDate <= fiscalyr.asOfDate)
					or oppledgerENT.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
			and oppledgerENT.isIPEDSReportable = 1
  )
where oppledgerRn = 1
)
 
/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs 

PARTS: 
    Part 9: General Information
    Part A: Statement of Financial Position
    Part B: Summary of Changes in Net Assets
    Part C: Scholarships and Fellowships
    Part D: Revenues and Investment Return
    Part E: Expenses by Functional and Natural Classification
    Part H: Endowment Assets 

*****/

-- Part 9
-- General Information
--jh 20200226 Removed join of ClientConfigMCR since config values are in COASPerFYAsOfDate

select DISTINCT 
	'9' part,
	0 sort,
	CAST(MONTH(NVL(fiscalyr.startDate, fiscalyr.priorAsOfDate))  as BIGINT) field1,
	CAST(YEAR(NVL(fiscalyr.startDate, fiscalyr.priorAsOfDate)) as BIGINT) field2,
	CAST(MONTH(NVL(fiscalyr.endDate, fiscalyr.asOfDate)) as BIGINT) field3,
	CAST(YEAR(NVL(fiscalyr.endDate, fiscalyr.asOfDate)) as BIGINT) field4,
	fiscalyr.finGPFSAuditOpinion field5, --1=Unqualified, 2=Qualified, 3=Don't know
--jh 20200217 removed finReportingModel
	fiscalyr.finAthleticExpenses field6, --1=Auxiliary enterprises 2=Student services 3=Does not participate in intercollegiate athletics 4=Other (specify in caveats box below)
--jh 20200217 added finPellTransactions
	fiscalyr.finPellTransactions field7 --P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants, no record or null default = 'P'
from FiscalYearMCR fiscalyr

union

--jh 20200226 Modified all sections to pull from OperatingLedgerMCR or genledger and added parent/child logic

-- Part A
-- Statement of Financial Position

/* This part is intended to report the assets, liabilities, and net assets.
   Data should be consistent with the Statement of Financial Position in the GPFS.
*/

-- Line 01 - Long-term investments -
/*  Enter the END-of-year market value for all assets held for long-term investment. Long-term investments should
    be distinguished from temporary investments based on the intention of the organization regarding the term of
    the investment rather than the nature of the investment itself. Thus, cash and cash equivalents which are
    held until appropriate long-term investments are identified should be treated as long-term investments.
    Similarly, cash equivalents strategically invested and reinvested for long-term purposes should be treated
    as long-term investments.  GASB A04 Formula CV = [A05-A31] 
*/

select 'A',
	1,
	'1',  -- Line 01, Long-Term Investments
	null,
	CAST((NVL(ABS(ROUND(SUM(CASE when (genledger.assetCapitalLand = 'Y'
											or genledger.assetCapitalInfrastructure = 'Y'
											or genledger.assetCapitalBuildings = 'Y'
											or genledger.assetCapitalEquipment = 'Y'
											or genledger.assetCapitalConstruction = 'Y'
											or genledger.assetCapitalIntangibleAsset = 'Y'
											or genledger.assetCapitalOther = 'Y'
											or genledger.assetNoncurrentOther = 'Y') 
								  then genledger.endBalance ELSE 0 END))), 0) --  GASB A5
        -  -- Depreciable capital assets, net of depreciation.
        NVL(ABS(ROUND(SUM(CASE when (genledger.assetCapitalLand = 'Y'
											or genledger.assetCapitalInfrastructure = 'Y'
											or genledger.assetCapitalBuildings = 'Y'
											or genledger.assetCapitalEquipment = 'Y'
											or genledger.assetCapitalConstruction = 'Y'
											or genledger.assetCapitalIntangibleAsset = 'Y'
											or genledger.assetCapitalOther = 'Y') 
								then genledger.endBalance ELSE 0 END)
		- SUM(CASE when genledger.accumDepreciation = 'Y' then genledger.endBalance ELSE 0 END))), 0) -- GASB A31
	) as BIGINT),  -- FASB A01 -- Formula CV = [A05-A31]
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and ((genledger.assetCapitalLand = 'Y' 
		or genledger.assetCapitalInfrastructure = 'Y'  
		or genledger.assetCapitalBuildings = 'Y' 
		or genledger.assetCapitalEquipment = 'Y'  
		or genledger.assetCapitalConstruction = 'Y' 
		or genledger.assetCapitalIntangibleAsset = 'Y' 
		or genledger.assetCapitalOther = 'Y' 
		or genledger.assetNoncurrentOther = 'Y')
		or (genledger.assetCapitalLand = 'Y' 
			or genledger.assetCapitalInfrastructure = 'Y' 
			or genledger.assetCapitalBuildings = 'Y'  
			or genledger.assetCapitalEquipment = 'Y' 
			or genledger.assetCapitalConstruction = 'Y' 
			or genledger.assetCapitalIntangibleAsset = 'Y' 
			or genledger.assetCapitalOther = 'Y')
		or (genledger.accumDepreciation = 'Y'))
    
union

-- Line 19 - Property, plant, and equipment, net of accumulated depreciation
/*  Includes END-of-year market value for categories such as land, buildings, improvements other than buildings, equipment, 
    and library books, combined and net of accumulated depreciation. (FARM para. 415)
    FASB A19 = GASB A31
*/

select 'A',
	2,
	'19', --Property, plant, and equipment, net of accumulated depreciation
	null,
	CAST((NVL(ABS(ROUND(SUM(CASE when (genledger.assetCapitalLand = 'Y'
											or genledger.assetCapitalInfrastructure = 'Y'
											or genledger.assetCapitalBuildings = 'Y'
											or genledger.assetCapitalEquipment = 'Y'
											or genledger.assetCapitalConstruction = 'Y'
											or genledger.assetCapitalIntangibleAsset = 'Y'
											or genledger.assetCapitalOther = 'Y') 
								  then genledger.endBalance ELSE 0 END)
			- SUM(CASE when genledger.accumDepreciation = 'Y' then genledger.endBalance ELSE 0 END))), 0)) as BIGINT),  -- FASB A19 / -- GASB A31
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and ((genledger.assetCapitalLand = 'Y' 
		or genledger.assetCapitalInfrastructure = 'Y'  
		or genledger.assetCapitalBuildings = 'Y' 
		or genledger.assetCapitalEquipment = 'Y' 
		or genledger.assetCapitalConstruction = 'Y' 
		or genledger.assetCapitalIntangibleAsset = 'Y' 
		or genledger.assetCapitalOther = 'Y')
		   or (genledger.accumDepreciation = 'Y'))

union

 -- Line 20 - Intangible assets, net of accumulated amortization - 
 /* Report all assets consisting of certain nonmaterial rights and benefits of an institution, such as patents, copyrights,
    trademarks and goodwill. The amount reported should be reduced by total accumulated amortization. (FARM para. 409)
	FASB A20 = GASB A33
*/

select 'A',
	3,
	'20', --Intangible assets, net of accumulated amortization
	null,
	CAST((NVL(ABS(ROUND(SUM(CASE when genledger.assetCapitalIntangibleAsset = 'Y' then genledger.endBalance ELSE 0 END)
		- SUM(CASE when genledger.accumAmmortization = 'Y' then genledger.endBalance ELSE 0 END))), 0)) as BIGINT),  -- FASB A20/GASB A33
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and (genledger.assetCapitalIntangibleAsset = 'Y'
		or genledger.accumAmmortization = 'Y')

union

-- Line 02 - Total assets - 
/* Enter the amount from your GPFS which is the SUM of:
    a) Cash, cash equivalents, and temporary investments;
    b) Receivables (net of allowance for uncollectible amounts);
    c) Inventories, prepaid expenses, and deferred charges;
    d) Amounts held by trustees for construction and debt service;
    e) Long-term investments;
    f) Plant, property, and equipment; and,
    g) Other assets 
    FASB 02 = GASB A06 | CV=(A01+A05)
*/

select 'A',
	4,
	'2', --Total assets
	null,
	CAST((NVL(ABS(ROUND(SUM(CASE when genledger.assetCurrent = 'Y' then genledger.endBalance ELSE 0 END))), 0) -- GASB A1
		+ NVL(ABS(ROUND(SUM(CASE when (genledger.assetCapitalLand = 'Y'
											or genledger.assetCapitalInfrastructure = 'Y'
											or genledger.assetCapitalBuildings = 'Y'
											or genledger.assetCapitalEquipment = 'Y'
											or genledger.assetCapitalConstruction = 'Y'
											or genledger.assetCapitalIntangibleAsset = 'Y'
											or genledger.assetCapitalOther = 'Y'
											or genledger.assetNoncurrentOther = 'Y') 
								  then genledger.endBalance ELSE 0 END))), 0) -- GASB A5
	) as BIGINT),  -- FASB A02 / GASB (A01+A05)
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and (genledger.assetCurrent = 'Y'
		or (genledger.assetCapitalLand = 'Y'
			or genledger.assetCapitalInfrastructure = 'Y'  
			or genledger.assetCapitalBuildings = 'Y'  
			or genledger.assetCapitalEquipment = 'Y' 
			or genledger.assetCapitalConstruction = 'Y' 
			or genledger.assetCapitalIntangibleAsset = 'Y' 
			or genledger.assetCapitalOther = 'Y'
			or genledger.assetNoncurrentOther = 'Y'))
    
union

-- Line 03 - Total liabilities - 
/* Enter the amount from your GPFS which is the SUM of:
    a) Accounts payable;
    b) Deferred revenues and refundable advances;
    c) Post-retirement and post-employment obligations;
    d) Other accrued liabilities;
    e) Annuity and life income obligations and other amounts held for the benefit of others;
    f) Bonds, notes, and capital leases payable and other long-term debt, including current portion;
    g) Government grants refundable under student loan programs; and,
    h) Other liabilities. 
    FASB A03 = GASB A10
*/

select 'A',
	5,
	'3', --Total liabilities
	null,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),  -- A03
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and (genledger.liabCurrentLongtermDebt = 'Y'
		or genledger.liabCurrentOther = 'Y'
		or genledger.liabNoncurrentLongtermDebt = 'Y'
		or genledger.liabNoncurrentOther = 'Y')

    
union


-- Line 03a - Debt related to property, plant and equipment - 
/*  Includes amounts for all long-term debt obligations including bonds payable, mortgages payable, capital leases payable,
    and long-term notes payable. (FARM para. 420.3, 423) If the current portion of long-term debt is separately reported 
    in the GPFS, include that amount. 
	FASB A03a = GASB A10
*/

select 'A',
	6,
	'3', -- Debt related to property, plant and equipment
	'a',
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),  -- A03a
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.liabNoncurrentLongtermDebt = 'Y'

union

-- Line 04 - Unrestricted net assets  
/* Enter the amount of unrestricted (designated and undesignated) net assets.
    Unrestricted net assets are amounts that are available for the general purposes of the
    institution without restriction. Include amounts specifically designated by the governing board,
    such as those designated as quasi-endowments, for building additions and replacement,
    for debt service, and for loan programs. In addition, include the unrestricted portion of
    net investment in plant, property, and equipment less related debt. This amount is computed
    as the amount of plant, property, and equipment, net of accumulated depreciation,
    reduced by any bonds, mortgages, notes, capital leases, or other borrowings that are clearly
    attributable to the acquisition, construction, or improvement of those assets 
	FASB A04 = GASB A17    CV=[A18-(A14+A15+A16)]
*/

select 'A',
	7,
	'4',    -- Unrestricted net assets
	null,
	CAST(((((NVL(ABS(ROUND(SUM(CASE when (genledger.assetCurrent = 'Y'
											or genledger.assetCapitalLand = 'Y'
											or genledger.assetCapitalInfrastructure = 'Y'
											or genledger.assetCapitalBuildings = 'Y'
											or genledger.assetCapitalEquipment = 'Y'
											or genledger.assetCapitalConstruction = 'Y'
											or genledger.assetCapitalIntangibleAsset = 'Y'
											or genledger.assetCapitalOther = 'Y'
											or genledger.assetNoncurrentOther = 'Y'
											or genledger.deferredOutflow = 'Y') 
									 then genledger.endBalance ELSE 0 END))), 0) ))
		- (NVL(ABS(ROUND(SUM(CASE when (genledger.liabCurrentLongtermDebt = 'Y' 
											or genledger.liabCurrentOther = 'Y'
											or genledger.liabNoncurrentLongtermDebt = 'Y' 
											or genledger.liabNoncurrentOther = 'Y')
								   then genledger.endBalance ELSE 0 END))), 0)
		+ NVL(ABS(ROUND(SUM(CASE when genledger.deferredInflow = 'Y' then genledger.endBalance ELSE 0 END))), 0)))
		- (((NVL(ABS(ROUND(SUM(CASE when (genledger.assetCapitalLand = 'Y'
											or genledger.assetCapitalInfrastructure = 'Y'
											or genledger.assetCapitalBuildings = 'Y'
											or genledger.assetCapitalEquipment = 'Y'
											or genledger.assetCapitalConstruction = 'Y'
											or genledger.assetCapitalIntangibleAsset = 'Y'
											or genledger.assetCapitalOther = 'Y') 
									  then genledger.endBalance ELSE 0 END)
		- SUM(CASE when genledger.accumDepreciation = 'Y' or genledger.isCapitalRelatedDebt = 1 then genledger.endBalance ELSE 0 END))), 0))
		+ (NVL(ABS(ROUND(SUM(CASE when genledger.accountType = 'Asset' and genledger.isRestrictedExpendOrTemp = 1 
								   then genledger.endBalance ELSE 0 END))), 0))
		+ (NVL(ABS(ROUND(SUM(CASE when genledger.accountType = 'Asset' and genledger.isRestrictedNonExpendOrPerm = 1 
								   then genledger.endBalance ELSE 0 END))), 0))))) 
	as BIGINT),  --FASB A04/GASB A17
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'

-- Line 05  - Total Restricted Net Assets
-- ( Do not include in import file. Will be calculated ) 

union

-- Line 05a - Permanently restricted net assets - 
/* Report the portion of net assets required by the donor or grantor to be held in perpetuity. 
   FASB A05a / GASB A15	
*/

select 'A',
	9,
	'5',  --  Permanently restricted net assets
	'a',
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),  -- A05a
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset'
	and genledger.isRestrictedExpendOrTemp = 1

union

-- Line 05b - Temporarily restricted net assets - 
/* Report net assets that are subject to a donor's or grantor's restriction are restricted net assets.
   Include long-term but temporarily restricted net assets, such as term endowments, and net assets 
   held subject to trust agreements if those agreements permit expenditure of the resources at 
   a future date. (FARM para. 450.3) 
*/

select 'A',
	10,
	'5',   -- Temporarily restricted net assets
	'b',
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),  -- FASB A05b / GASB A16
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset'
	and genledger.isRestrictedNonExpendOrPerm = 1

union

-- Line 11 - Land and land improvements -
/* Provide END of year values for land and land improvements as a reconciliation of beginning of the year values with additions to and
   retirements of land and land improvements to obtain END of year values. Use your underlying institutional records. 
   FASB A11 / GASB A21 
*/

select 'A',
	11,
	'11',    -- Land and land improvements
	null,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),  -- FASB A11 / GASB A21
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.assetCapitalLand = 'Y'


union

-- Line 12 - Buildings  
/*  End of year values for buildings represent a reconciliation of beginning of the year values with additions to and retirements 
    of building values to obtain END of year values. Capitalized leasehold improvements should be included on this line if the 
    improvements are to leased facilities.       
    FASB A12 / GASB A23 
*/

select 'A',
	13,
	'12',    -- Buildings
	null,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), -- FASB A12 / GASB A23
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.assetCapitalBuildings = 'Y'

union

-- Line 13 - Equipment, including art and library collections
/*  End of year values for equipment represent a reconciliation of beginning of the year values with additions to and retirements 
    of equipment values to obtain END of year values. Capitalized leasehold improvements should be included on this line if the
	improvements are to leased equipment. 
	FASB A13 / GASB A32
*/

select 'A',
	14,
	'13',    -- Equipment, including art and library collections
	null,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), -- FASB A13 / GASB A32
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.assetCapitalEquipment = 'Y'

union

-- Line 15 -  Construction in progress - 
/*  Report capital assets under construction and not yet placed into service.			 
    FASB A15 / GASB A27 
*/

select 'A',
	15,
	'15',   -- Construction in Progress
	null,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), -- FASB A15 / GASB A27
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.assetCapitalConstruction = 'Y'

union

-- Line 16 - Other - 
/*  Report all other amounts for capital assets not reported in lines 11-15. 
    FASB A16 / GASB A34
*/

select 'A',
	16,
	'16',    --  Other
	null,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), -- FASB A16 / GASB A34
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.assetCapitalOther = 'Y'


union

-- Line 18 - Accumulated depreciation  
/*  Report all depreciation amounts, including depreciation on assets that may not be included on any of the above lines. 
    FASB A18 / GASB A28
*/

select 'A',
	18,
	'18',	--Accumulated depreciation
	null,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), -- FASB A18 / GASB A28
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accumDepreciation = 'Y'

union

-- PART B   
-- Summary of Changes in Net Assets 
/*  This part is intended to report a summary of changes in net assets and to determine that all amounts being reported on the 
    Statement of Financial Position (Part A), Revenues and Investment Return (Part D), and Expenses by Functional and 
    Natural Classification (Part E) are in agreement.
*/

-- Line 01 - Total revenues and investment return - 
/*  Enter total revenues and investment return. The amount should represent all revenues reported for the fiscal period and should agree
	with the revenues recognized in the institution's GPFS. If your institution divides its statement of activities into operating and
	nonoperating sections, selected revenues in the nonoperating section must be added to the operating revenue subtotal.
	FASB B01 / GASB B01 
*/

select 'B',
	20,
	'1',   --  Total revenues and investment return
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- B01	Total Revenue
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue'
	    and (oppledger.revRealizedCapitalGains != 'Y' 
            and oppledger.revRealizedOtherGains != 'Y' 
            and oppledger.revExtraordGains != 'Y'))
	and oppledger.institParentChild = 'P'
	
union

-- Line 02 - Total expenses -
/*  Enter total expenses. The amount should represent total expenses recognized in the institution's GPFS.
    If your institution divides its statement of activities into operating and nonoperating sections, selected 
    expenses in the nonoperating section must be added to the operating expense subtotal. 
    Please enter the amount of expenses as a positive number which will then be treated as a negative number in 
    further computations as indicated by the parentheses.  
*/

select 'B',
	21,
	'2',   --  Total expenses
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- B02
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
       and (oppledger.expFedIncomeTax != 'Y'  
         and oppledger.expExtraordLosses != 'Y' ))
	and oppledger.institParentChild = 'P'
	
union

-- Line 04 - Change in net assets

select 'B',
	23 ,
	'4',   --  Change in net assets
	CAST(((NVL(ABS(ROUND(SUM(CASE when oppledger.accountType = 'Revenue' then oppledger.endBalance ELSE 0 END))), 0))
		- (NVL(ABS(ROUND(SUM(CASE when oppledger.accountType = 'Expense' then oppledger.endBalance ELSE 0 END))), 0))) as BIGINT), -- FASB B04 / GASB D03
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue' 
       or oppledger.accountType = 'Expense')
	and oppledger.institParentChild = 'P'
	
union

 select 'B',
	24,
	'5',   --  Net assets, beginning of year
	CAST((NVL(ABS(ROUND(SUM(CASE when (genledger.assetCurrent = 'Y'           --A1 Current Assets
								or genledger.assetCapitalLand = 'Y'     --A14 Net assets invested in capital assets
								or genledger.assetCapitalInfrastructure = 'Y'
								or genledger.assetCapitalBuildings = 'Y'
								or genledger.assetCapitalEquipment = 'Y'
								or genledger.assetCapitalConstruction = 'Y'
								or genledger.assetCapitalIntangibleAsset = 'Y'
								or genledger.assetCapitalOther = 'Y'
								or genledger.assetNoncurrentOther = 'Y'   --A5 Noncurrent Assets
								or genledger.deferredOutflow = 'Y')       --A19 Deferred Outflow
								then genledger.beginBalance ELSE 0 END))), 0)
		- NVL(ABS(ROUND(SUM(CASE when (genledger.liabCurrentLongtermDebt = 'Y'
								or genledger.liabCurrentOther = 'Y'      --A9 Current Liabilities
								or genledger.liabNoncurrentLongtermDebt = 'Y'
								or genledger.liabNoncurrentOther = 'Y'   --A12 Noncurrent Liabilities
								or genledger.deferredInflow = 'Y'        --A20 Deferred Inflow
								or genledger.accumDepreciation = 'Y'     --A31 Depreciation on capital assets
								or genledger.accumAmmortization = 'Y')   --P33 Accumulated amortization on intangible assets
								then genledger.beginBalance ELSE 0 END)
				)), 0)) as BIGINT), -- FASB B05 / GASB D4
	null,
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year Begin'
	and (genledger.assetCurrent = 'Y' 
        or genledger.assetCapitalLand = 'Y'   
        or genledger.assetCapitalInfrastructure = 'Y' 
        or genledger.assetCapitalBuildings = 'Y' 
        or genledger.assetCapitalEquipment = 'Y' 
        or genledger.assetCapitalConstruction = 'Y' 
        or genledger.assetCapitalIntangibleAsset = 'Y' 
        or genledger.assetCapitalOther = 'Y' 
        or genledger.assetNoncurrentOther = 'Y' 
        or genledger.deferredOutflow = 'Y'
        or genledger.liabCurrentLongtermDebt = 'Y' 
        or genledger.liabCurrentOther = 'Y' 
        or genledger.liabNoncurrentLongtermDebt = 'Y'  
        or genledger.liabNoncurrentOther = 'Y'  
        or genledger.deferredInflow = 'Y'  
        or genledger.accumDepreciation = 'Y'  
        or genledger.accumAmmortization = 'Y')
	and genledger.institParentChild = 'P'
	
union

-- PART C  
-- Scholarships and Fellowships
-- Note:  FASB (Part C) & GASB (Part E) Mostly matched  

select 'C',
	27,
	'1',   -- Pell grants (federal)
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C01
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
  and oppledger.institParentChild = oppledger.COASParentChild
  and oppledger.expFAPellGrant = 'Y'

union

-- Line 02 -- Other federal grants (Do NOT include FDSL amounts)

select 'C',
	28,
	'2',   -- Other federal grants (Do NOT include FDSL amounts)
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C02
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFANonPellFedGrants = 'Y'
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'C',
	29,
	'3',   -- Grants by state government
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C03
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFAStateGrants = 'Y'
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'C',
	30,
	'4',   -- Grants by local government
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C04
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFALocalGrants = 'Y'
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'C',
	31,
	'5',   -- Institutional grants (restricted)
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C01
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFAInstitGrantsRestr = 'Y'
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'C',
	32,
	'6',   -- Institutional grants (unrestricted)
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C06
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFAInstitGrantsUnrestr = 'Y'
	and oppledger.institParentChild = oppledger.COASParentChild

-- PART C, Line 07 - Total Revenue   (Do Not include in Import/Export File )

union

select 'C',
	34,
	'8',   -- Discounts and Allowances applied to tuition and fees
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C08
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.discAllowTuitionFees = 'Y'
	and oppledger.institParentChild = oppledger.COASParentChild

union

 select 'C',
	35,
	'9',   -- Discounts and Allowances applied to auxiliary enterprise revenues
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.discAllowAuxEnterprise = 'Y'
	and oppledger.institParentChild = oppledger.COASParentChild

/* 	PART C, Line 10 -- Total Discounts and Allowances
	(Do not include in Export/Import File
*/

union

-- Part D 
-- Revenues by Source
/*  FASB Totals(Col1) Should Match GASB Part B01
	FASB Column 1 Should be the SUM of Columns 2-4 as it is the total amount broken out by 
    -- UnRestricted,  RestrictedTemp and RestrictedPerm 
*/
-- jdh PartD should be ParentInstitution only
select 'D',
	37,
	'1', -- Tuition and fees (net of allowance reported in Part C, line 08) D01
	2,   --isUnrestrictedFASB
	CAST((NVL(ABS(ROUND(SUM(CASE when oppledger.revTuitionAndFees = 'Y' then oppledger.endBalance ELSE 0 END)
					  - SUM(CASE when oppledger.discAllowTuitionFees = 'Y' then oppledger.endBalance ELSE 0 END))), 0)) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and ((oppledger.revTuitionAndFees = 'Y'  
	 or oppledger.discAllowTuitionFees = 'Y' )
      	and oppledger.isUnrestrictedFASB = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	38,
	'1',   -- Tuition and fees (net of allowance reported in Part C, line 08) D01
	3,     -- isRestrictedTempFASB
	CAST((NVL(ABS(ROUND(SUM(CASE when oppledger.revTuitionAndFees = 'Y' then oppledger.endBalance ELSE 0 END)
					- SUM(CASE when oppledger.discAllowTuitionFees = 'Y' then oppledger.endBalance ELSE 0 END))), 0)) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and ((oppledger.revTuitionAndFees = 'Y' 
	 or oppledger.discAllowTuitionFees = 'Y')
   	and oppledger.isRestrictedTempFASB = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	39,
	'1',    -- Tuition and fees (net of allowance reported in Part C, line 08) D01
	4,      -- isRestrictedPermFASB
	CAST((NVL(ABS(ROUND(SUM(CASE when oppledger.revTuitionAndFees = 'Y' then oppledger.endBalance ELSE 0 END)
					  - SUM(CASE when oppledger.discAllowTuitionFees = 'Y' then oppledger.endBalance ELSE 0 END))), 0)) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and ((oppledger.revTuitionAndFees = 'Y'
          or oppledger.discAllowTuitionFees = 'Y')
        and oppledger.isRestrictedPermFASB = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	40,
	'2',	--Federal appropriations | D02
	2, 		--isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revFedApproprations = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	41,
	'2',   -- Federal appropriations | D02
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revFedApproprations = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

-- Line 02 - Federal appropriations

select 'D',
	42,
	'2',   -- Federal appropriations | D02
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.revFedApproprations = 'Y'
    and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	43,
	'3',   -- State appropriations  | D03
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateApproprations = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	44,
	'3',   -- State appropriations  | D03
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateApproprations = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	45,
	'3',   -- State appropriations  | D03
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateApproprations = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	46,
	'4',   -- Local appropriations | D04
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalApproprations = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	47,
	'4',   -- Local appropriations | D04
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalApproprations = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	48,
	'4',   -- Local appropriations | D04
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalApproprations = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	49,
	'5',   -- Federal grants and contracts (Do not include FDSL) | D05
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revFedGrantsContractsOper = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	50,
	'5',   -- Federal grants and contracts (Do not include FDSL) | D05
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revFedGrantsContractsOper = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	51,
	'5',    -- Federal grants and contracts (Do not include FDSL) | D05
	4, 		--isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revFedGrantsContractsOper = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	52,
	'6',    -- State grants and contracts | D06
	2, 		--isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateGrantsContractsOper = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	53,
	'6',    -- State grants and contracts | D06
	3,      -- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateGrantsContractsOper = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	54,
	'6',    -- State grants and contracts | D06
	4,      -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateGrantsContractsOper = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	55,
	'7',   -- Local government grants and contracts | D07
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalGrantsContractsOper = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	56,
	'7',   -- Local government grants and contracts | D07
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalGrantsContractsOper = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	57,
	'7',   -- Local government grants and contracts | D07
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalGrantsContractsOper = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

-- Line 08 - Private Gifts, Grants and Contracts
-- PART D, Line 8 Private Gifts and Contracts  are [Calculated CV D08 = (D08a + D08b)] 
-- ( Do not include in export )  

-- Line 08a - Private gifts
/*  Enter revenues from private (non-governmental) entities including revenues received from gift or 
    contribution nonexchange transactions (including contributed services) except those from affiliated entities, 
    which are entered on line 09. Includes bequests, promises to give (pledges), gifts from an affiliated 
    organization or a component unit not blended or consolidated, and income from funds held in irrevocable 
    trusts or distributable at the direction of the trustees of the trusts. Includes any contributed services 
    recognized (recorded) by the institution.
*/

-- jdh Part D should report on Parent institution data only 

select 'D',
	58,
	'8a',   -- Private gifts | D08a  -- revPrivGifts
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGifts = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union
 
select 'D',
	59,
	'8a',   -- Private gifts | D08a  -- revPrivGifts
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGifts = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	60,
	'8a',   -- Private gifts | D08a  -- revPrivGifts
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGifts = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	61,
	'8b',   -- Private grants & Contracts | D08b revPrivGrantsContractsOper
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGrantsContractsOper = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	62,
	'8b',   -- Private grants & Contracts | D08b revPrivGrantsContractsOper
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGrantsContractsOper = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	63,
	'8b',   -- Private grants & Contracts | D08b revPrivGrantsContractsOper
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGrantsContractsOper = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	64,
	'9',   -- Contributions from affiliated entities | D09 revAffiliatedOrgnGifts
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revAffiliatedOrgnGifts = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	65,
	'9',   -- Contributions from affiliated entities | D09 revAffiliatedOrgnGifts
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revAffiliatedOrgnGifts = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	66,
	'9',   -- Contributions from affiliated entities | D09 revAffiliatedOrgnGifts
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revAffiliatedOrgnGifts = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

-- Other Revenue

union

select 'D',
	67,
	'10',   -- Investment return | D10  revInvestmentIncome
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revInvestmentIncome = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	68,
	'10',   -- Investment return | D10  revInvestmentIncome
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revInvestmentIncome = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	69,
	'10',   -- Investment return | D10  revInvestmentIncome
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revInvestmentIncome = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	70,
	'11',   -- Sales and services of educational activities | D11 revEducActivSalesServices
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revEducActivSalesServices = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	73,
	'12' ,   -- Sales and services of auxilliary enterprises | D12
	2, --isUnrestrictedFASB
	CAST((NVL(ABS(ROUND(SUM(CASE when oppledger.revAuxEnterprSalesServices = 'Y' then oppledger.endBalance ELSE 0 END)
						- SUM(CASE when oppledger.discAllowAuxEnterprise = 'Y' then oppledger.endBalance ELSE 0 END))), 0)) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and ((oppledger.revAuxEnterprSalesServices = 'Y'
      		or oppledger.discAllowAuxEnterprise = 'Y')
		and oppledger.isUnrestrictedFASB = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	76,
	'13',   -- Hospital revenue | D13
	2, --isUnrestrictedFASB
	CAST((NVL(ABS(ROUND(SUM(CASE when oppledger.revHospitalSalesServices = 'Y' then oppledger.endBalance ELSE 0 END)
					  - SUM(CASE when oppledger.discAllowPatientContract = 'Y' then oppledger.endBalance ELSE 0 END))), 0)) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and ((oppledger.revHospitalSalesServices = 'Y'
	   		or oppledger.discAllowPatientContract = 'Y')
		and oppledger.isUnrestrictedFASB = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	79,
	'14',   -- Independent operations revenue | D14 revIndependentOperations
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revIndependentOperations = 'Y'
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	80,
	'14',   -- Independent operations revenue | D14 revIndependentOperations
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revIndependentOperations = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	81,
	'14',   -- Independent operations revenue | D14 revIndependentOperations
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revIndependentOperations = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

-- Line 15 Other Revenue CV= [D16-(D01+...+D14)]
-- (Do not include in import file. Will be calculated as sums of columns) 

-- Line 16 - Total revenues and investment return -
/* This amount is carried forward from Part B, line 01.
   This amount should include ARRA revenues received by the institution, if any. 
*/

select 'D',
	82,
	'16',    -- Total revenues and investment return | D16
	1, -- Total
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue'
		and (oppledger.revRealizedCapitalGains != 'Y'
			and oppledger.revRealizedOtherGains != 'Y'
			and oppledger.revExtraordGains != 'Y'
			and oppledger.revOwnerEquityAdjustment != 'Y'
			and oppledger.revSumOfChangesAdjustment != 'Y'))
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	82,
	'16',    -- Total revenues and investment return | D16
	2, --isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue'
	  	and (oppledger.revRealizedCapitalGains != 'Y'
	  		and oppledger.revRealizedOtherGains != 'Y'
	  		and oppledger.revExtraordGains != 'Y'
	  		and oppledger.revOwnerEquityAdjustment != 'Y'
	  		and oppledger.revSumOfChangesAdjustment != 'Y'))
	and oppledger.isUnrestrictedFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	83,
	'16',    -- Total revenues and investment return | D16
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue'
	  	and (oppledger.revRealizedCapitalGains != 'Y'
        	and oppledger.revRealizedOtherGains != 'Y'
        	and oppledger.revExtraordGains != 'Y'
        	and oppledger.revOwnerEquityAdjustment != 'Y'
        	and oppledger.revSumOfChangesAdjustment != 'Y'))
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	84,
	'16',    -- Total revenues and investment return | D16
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue'
		and (oppledger.revRealizedCapitalGains != 'Y'
	  		and oppledger.revRealizedOtherGains != 'Y'
	  		and oppledger.revExtraordGains != 'Y'
	  		and oppledger.revOwnerEquityAdjustment != 'Y'
	  		and oppledger.revSumOfChangesAdjustment != 'Y'))
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

-- Line 17 - Net assets released from restriction - 
/*  Enter all revenues resulting from the reclassification of temporarily restricted assets
    or permanently restricted assets. 
*/

select 'D',
	86,
	'17',    --    | D17  revReleasedAssets
	3, --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revReleasedAssets = 'Y'
	and oppledger.isRestrictedTempFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'D',
	87,
	'17',    --    | D17  revReleasedAssets
	4, --isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revReleasedAssets = 'Y'
	and oppledger.isRestrictedPermFASB = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

/* 	Line 18  D18 - Net total revenues, after assets released from restriction
            Do not include in export - Calculated by Form.
	Line 19  D19 - 	12-MONTH Student FTE from E12
            Do not include in export - Calculated by Form.
	Line 20  D20 - Total revenues and investment return per student FTE CV=[D16/D19]
            Do not include in export - Calculated by Form.
*/

-- Part E-1 
-- Expenses by Functional Classification
/*  Report Total Operating AND Nonoperating Expenses in this section 
    Part E is intended to report expenses by function. All expenses recognized in the GPFS should be reported 
    using the expense functions provided on lines 01-12. These functional categories are consistent with 
    Chapter 4 (Accounting for Independent Colleges and Universities) of the NACUBO FARM.
*/
-- jdh Part E should report on Parent institution only 

select 'E',
	88,
	'1',   -- Instruction | E1,1
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --   E1,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isInstruction = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	89,
	'1',   -- Instruction | E1,1
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --   E1,2
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense' 
	and oppledger.expSalariesWages = 'Y'  
	and oppledger.isInstruction = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	90,
	'2',   -- Research | E1,2
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --  E2,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense' 
	and oppledger.isResearch = 1
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	91,
	'2',   -- Research | E1,2
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E2,2
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
    	and oppledger.expSalariesWages = 'Y' 
    	and oppledger.isResearch = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	92,
	'3',   -- Public service | E1,3
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E3,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
    	and oppledger.isPublicService = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	93,
	'3',   -- Public service | E1,3
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E3,2
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
    	and oppledger.expSalariesWages = 'Y' 
    	and oppledger.isPublicService = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	94,
	'4',   -- Academic support | E1,4
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E4,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
    	and oppledger.isAcademicSupport = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	95,
	'4',   -- Academic support | E1,4
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E4,2
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
        and oppledger.expSalariesWages = 'Y' 
        and oppledger.isAcademicSupport = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	96,
	'5',   -- Student services | E1,5
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E5,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
		and oppledger.isStudentServices = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	97,
	'5',   -- Student services | E1,5
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E5,2
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
        and oppledger.expSalariesWages = 'Y' 
        and oppledger.isStudentServices = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	98,
	'6',   -- Institutional support | E1,6
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E6,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
        and oppledger.isInstitutionalSupport = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	99,
	'6',   -- Institutional support | E1,6
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E6,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
        and oppledger.isInstitutionalSupport = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	100,
	'7',   -- Auxiliary enterprises | E1,7
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E7,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
        and oppledger.isAuxiliaryEnterprises  = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	101,
	'7',   -- Auxiliary enterprises | E1,7
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E7,2
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
        and oppledger.expSalariesWages = 'Y' 
        and oppledger.isAuxiliaryEnterprises = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	102,
	'8',   -- Net grant aid to students | E1,8
	1,
	CAST((NVL(ABS(ROUND(SUM(CASE when (oppledger.expFAPellGrant = 'Y'           --Pell grants
									or oppledger.expFANonPellFedGrants = 'Y'    --Other federal grants
									or oppledger.expFAStateGrants = 'Y'         --Grants by state government
									or oppledger.expFALocalGrants = 'Y'         --Grants by local government
									or oppledger.expFAInstitGrantsRestr = 'Y'   --Institutional grants from restricted resources
									or oppledger.expFAInstitGrantsUnrestr = 'Y' --Institutional grants from unrestricted resources
								) then oppledger.endBalance ELSE 0 END)
				- SUM(CASE when (oppledger.discAllowTuitionFees = 'Y'   --Discounts and allowances applied to tuition and fees
							  or oppledger.discAllowAuxEnterprise = 'Y' --Discounts and allowances applied to sales and services of auxiliary enterprises
								) then oppledger.endBalance ELSE 0 END))), 0)) as BIGINT),
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and ((oppledger.expFAPellGrant = 'Y' 
       or oppledger.expFANonPellFedGrants = 'Y' 
       or oppledger.expFAStateGrants = 'Y' 
       or oppledger.expFALocalGrants = 'Y' 
       or oppledger.expFAInstitGrantsRestr = 'Y' 
       or oppledger.expFAInstitGrantsUnrestr = 'Y')
	 or (oppledger.discAllowTuitionFees = 'Y'
       or oppledger.discAllowAuxEnterprise = 'Y'))
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	103,
	'9',   -- Hospital Services | E1,9
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E9,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
		and oppledger.isHospitalServices  = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	104,
	'9',   -- Hospital Services | E1,9
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E9,2
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
    	and oppledger.expSalariesWages = 'Y' 
    	and oppledger.isHospitalServices = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	105,
	'10',   -- Independent operations | E-10
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E10,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
     	and oppledger.isIndependentOperations = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	106,
	'10',   -- Independent operations | E-10
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E10,2
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
    	and oppledger.expSalariesWages = 'Y' 
    	and oppledger.isIndependentOperations = 1)
	and oppledger.institParentChild = oppledger.COASParentChild

-- Part E,  Line E12   Calculated CV = [E13-(E01+...+E10)]
-- (Do not include in import file. Will be calculated )

union

select 'E',
	107,
	'13',   -- Total expenses and Deductions | E-13
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E13,1
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
        and (expFedIncomeTax != 'Y'
           	and expExtraordLosses != 'Y'))
	and oppledger.institParentChild = oppledger.COASParentChild

union

select 'E',
	108,
	'13',   -- Total expenses and Deductions | E-13
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E13,2
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense' 
      	and oppledger.expSalariesWages = 'Y')
	and oppledger.institParentChild = oppledger.COASParentChild

union

-- Part E-2 - Expenses by Natural Classification 
-- jdh Part E2 should report on Parent Institution only 

-- Part 'E', Line 71   '13-2' ,  E13,2 Salaries and Wages(from Part E-1, line 13 column 2)
-- (Do not include in import file. Will be calculated )
 
-- Line E13-3  - Benefits
-- Enter the total amount of benefits expenses incurred

select 'E',
	109,
	'13',   --  E13-3 Benefits
	3,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E13-3 Benefits
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expBenefits = 'Y'
	and oppledger.institParentChild = oppledger.COASParentChild

union

-- Line E13-4 Operation and Maintenance of Plant
/* This amount is used to show the distribution of operation and maintenance of plant expenses.
   Enter in this column the allocated amount of operation and maintenance of plant expenses for all 
   functions listed on lines 01-12 in Part E-1. 
*/

select 'E',
	110,
	'13',   --  E13-4 Operation and Maintenance of Plant
	4,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E13-4,
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.expCapitalConstruction = 'Y' 
      	or oppledger.expCapitalEquipPurch = 'Y' 
      	or oppledger.expCapitalLandPurchOther = 'Y')
	and oppledger.institParentChild = oppledger.COASParentChild

union

-- Line E13-5 Depreciation
-- Enter the total amount of depreciation incurred.

select 'E',
	111,
	'13',   --  E13-5 Depreciation
	5,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E13-5 Depreciation
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expDepreciation = 'Y'
	and oppledger.institParentChild = oppledger.COASParentChild

union

-- Line E13-6 Interest
-- Enter in the total amount of interest incurred on debt.

select 'E',
	112,
	'13',   --  E13-6 Interest
	6,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E13-6 Interest
	null,
	null,
	null,
	null
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expInterest = 'Y'
	and oppledger.institParentChild = oppledger.COASParentChild

/* 	Line 13-1 Total Expenses and Deductions (from Part E-1, Line 13)
              (Do not include in import file. Will be calculated )
	Line 14-1 12-MONTH Student FTE (from E12 survey)
              (Do not include in import file. Will be calculated )
	Line 15-1 Total expenses and deductions per student FTE   CV=[E13/E14]
*/

union

-- PART H   
-- Value of Endowment Assets
/* Details of Endowment Assets, Part H (positional file only) 
   This part is intended to report details about endowments.
   This part appears only for institutions answering "Yes" to the general information question regarding endowment assets.
   Report the amounts of gross investments of endowment, term endowment, and funds functioning as endowment for the 
   institution and any of its foundations and other affiliated organizations. DO NOT reduce investments by liabilities 
   for Part H
*/

-- Line 01 - Value of endowment assets at the beginning of the fiscal year
/* If the market value of some investments is not available, use whatever value was assigned by the institution 
   in reporting market values in the annual financial report.
*/

--jh 20200218 added conditional for IPEDSClientConfig.finEndowmentAssets

select 'H',
	113,
	'1', --Value of endowment assets at the beginning of the fiscal year
	CAST(case when (select clientconfig.finEndowmentAssets
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.beginBalance))), 0) 
		 else 0
	end as BIGINT),
	null,
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year Begin'
	and genledger.accountType = 'Asset' 
    and genledger.isEndowment = 1
	and genledger.institParentChild = genledger.COASParentChild

union

select 'H',
	114,        
	'2', --Value of endowment assets at the END of the fiscal year
	CAST(case when (select clientconfig.finEndowmentAssets
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	null,
	null,
	null,
	null,
	null
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and (genledger.accountType = 'Asset' 
        and genledger.isEndowment = 1)
	and genledger.institParentChild = genledger.COASParentChild
        
-- order by 2 
