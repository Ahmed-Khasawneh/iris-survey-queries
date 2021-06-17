/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Spring Collection
FILE NAME:      Finance v3 (F3A) 
FILE DESC:      Finance for Degree-Granting Private For-Profit Institutions Using FASB 
AUTHOR:         Janet Hanicak
CREATED:        20191220

SECTIONS:
    Reporting Dates 
    Most Recent Records 
    Survey Formatting

SUMMARY OF CHANGES

Date(yyyymmdd)      Author              Tag              Comments
-----------------   ----------------    --------------  --------------------------------------------------------------------------------
20200622			akhasawneh			ak 20200622		 Modify Finance report query with standardized view naming/aliasing convention (PF-1532) -Run time 5m 4s
20200412			jhanicak			jh 20204012		 Added dummy date option for recordActivityDate in most current record queries PF-1374
														 Added DefaultValues query and rewrote other queries to use PF-1418
														 Removed all 'prior' queries - not needed
20200303            jd.hysler           jdh 2020-03-04   PF-1297 Modify Finance all versions to use latest records from finance entities 
                                                         - Removed FYPerAsOfDate/FYPerPriorAsOfDate
                                                         - Removed BothCOASPerFYAsOfDate/BothCOASPerFYPriorAsOfDate
                                                         - Move IPEDSClientConfig values into COASPerFYAsOfDate/COASPerFYPriorAsOfDate
                                                         - Added GL cte for most recent record views for GeneralLedgerReporting
                                                         ? Added GL_Prior cte for most recent record views for GeneralLedgerReporting for prior fiscal year
                                                         - Added oppledger cte for most recent record views for OperatingLedgerReporting
                                                         ? Added OL_Prior cte for most recent record views for OperatingLedgerReporting for prior fiscal year
                                                         - Removed cross join with ClientConfigMCR since values are now already in COASPerFYAsOfDate
                                                         - Changed PART Queries to use new in-line genledger/oppledger Structure
                                                                --  1-Changed GeneralLedgerReporting to genledger inline view  
                                                                --  2-Removed the join to COAS       
                                                                --  3-Removed genledger.isIpedsReportable = 1 
                                                                --  4-move fiscalPeriod from join to where clause
                                                                --  5-added Parent/Child idenfier 
20200217            jhanicak            jh 20200217     PF-1254 Added default values for IPEDSReportingPeriod and IPEDSClientConfig
20200103            jd.hysler                           Move original code to template 
20191230            jd.hysler           jdh 20191230    Made Part A Line 12 match Line 1b                

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
	'F3A' surveyId,
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
	'F3A' surveyId,
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
	repValues.finBusinessStructure finBusinessStructure,
	repValues.finTaxExpensePaid finTaxExpensePaid,
	repValues.finParentOrChildInstitution finParentOrChildInstitution
from (
	select NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.currentSection) THEN repPeriodENT.asOfDate END, defvalues.asOfDate) asOfDate,
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
		defvalues.finBusinessStructure finBusinessStructure,
		defvalues.finTaxExpensePaid finTaxExpensePaid,
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
	repValues.finBusinessStructure,
	repValues.finTaxExpensePaid,
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
	defvalues.finBusinessStructure finBusinessStructure,
	defvalues.finTaxExpensePaid finTaxExpensePaid,
	defvalues.finParentOrChildInstitution finParentOrChildInstitution
from DefaultValues defvalues
where defvalues.surveyYear not in (select surveyYear
                                    from DefaultValues defvalues1
										cross join IPEDSReportingPeriod repPeriodENT
                                    where repPeriodENT.surveyCollectionYear = defvalues1.surveyYear
										and repPeriodENT.surveyId = defvalues.surveyId)
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
--finTaxExpensePaid - v3, v6
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
	finBusinessStructure finBusinessStructure,
	finTaxExpensePaid finTaxExpensePaid,
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
		NVL(clientConfigENT.finBusinessStructure, repperiod.finBusinessStructure) finBusinessStructure,
		NVL(clientConfigENT.finTaxExpensePaid, repperiod.finTaxExpensePaid) finTaxExpensePaid,
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
	repperiod.finBusinessStructure finBusinessStructure,
	repperiod.finTaxExpensePaid finTaxExpensePaid,
	repperiod.finParentOrChildInstitution finParentOrChildInstitution
from ReportingPeriodMCR repperiod
where repperiod.surveyYear not in (select clientconfigENT.surveyCollectionYear
								   from IPEDSClientConfig clientconfigENT
									inner join ReportingPeriodMCR repperiod1 
									on repperiod1.surveyYear = clientconfigENT.surveyCollectionYear)
),

 --jdh 2020-03-04 Removed FYPerAsOfDate/FYPerPriorAsOfDate

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

--jdh 2020-03-04 Modified COASPerFYAsOfDate to include IPEDSClientConfig values
--and most recent records based on COASPerAsOfDate and FiscalYear

--jh 20200412 Added DefaultValues query and rewrote other queries to use PF-1418

FiscalYearMCR AS (
select FYData.asOfDate asOfDate,
	FYData.priorAsOfDate priorAsOfDate,
	CASE FYData.finGPFSAuditOpinion
		 when 'Q' then 1
		 when 'UQ' then 2
		 when 'U' then 3 END finGPFSAuditOpinion, --1=Unqualified, 2=Qualified, 3=Don't know
	CASE FYData.finPellTransactions
		 when 'P' then 1
		 when 'F' then 2
		 when 'N' then 3 END finPellTransactions, --P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants 
	CASE FYData.finBusinessStructure
		 when 'SP' then 1
		 when 'P' then 2
		 when 'C' then 3 
		 when 'SC' then 4 
		 when 'LLC' then 5 END finBusinessStructure,  --SP = Sole Proprietorship, P = Partnership, C = C Corp, SC = S Corp, LLC = LLC
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
from (
	select fiscalyearENT.*,
		clientconfig.asOfDate asOfDate,
		clientconfig.priorAsOfDate priorAsOfDate,
		clientconfig.finGPFSAuditOpinion finGPFSAuditOpinion,
		clientconfig.finPellTransactions finPellTransactions,
		clientconfig.finBusinessStructure finBusinessStructure,
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

 
--jdh 2020-03-04 Added genledger most recent record views for GeneralLedgerReporting
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

--jdh 2020-03-04 Added oppledger cte for most recent record views for OperatingLedgerReporting

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
where oppledgerRN = 1
)
  
/*****
BEGIN SECTION - Survey Formatting
The select query below contains union statements to match each part of the survey specs 

PARTS: 
    Part 9: General Information
    Part F: Income Tax Expenses
    Part A: Balance Sheet Information
    Part B: Summary of Changes in Equity
    Part C: Scholarships and Fellowships
    Part D: Revenues and Investment Return
    Part E: Expenses by Functional and Natural Classification

*****/

-- Part 9
-- General Information

--jdh 2020-03-04  Moved Case Statements from part 9 into COASPerFYAsOfDate cte
--jh 20200226 Removed join of ClientConfigMCR since config values are in COASPerFYAsOfDate

select DISTINCT '9' part,
	0 sort,
	CAST(MONTH(NVL(fiscalyr.startDate, fiscalyr.priorAsOfDate))  as BIGINT) field1,
	CAST(YEAR(NVL(fiscalyr.startDate, fiscalyr.priorAsOfDate)) as BIGINT) field2,
	CAST(MONTH(NVL(fiscalyr.endDate, fiscalyr.asOfDate)) as BIGINT) field3,
	CAST(YEAR(NVL(fiscalyr.endDate, fiscalyr.asOfDate)) as BIGINT) field4,
	fiscalyr.finGPFSAuditOpinion  field5, --1=Unqualified, 2=Qualified, 3=Don't know
	fiscalyr.finPellTransactions  field6, --P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants 
	fiscalyr.finBusinessStructure field7  --SP = Sole Proprietorship, P = Partnership, C = C Corp, SC = S Corp, LLC = LLC 
from FiscalYearMCR fiscalyr

union

-- Part F  
-- Income Tax Expenses 

/* This section is only applicable to those institutions that have either a C Corporation or a Limited 
   Liability Company (LLC) business structure. The institution should follow the Basic Principles for 
   Income Tax Accounting, as outlined in FASB SFAS 109.   
*/

-- Line 01 - Federal - Report the SUM of the current Federal tax expense (or benefit) and deferred Federal tax expense (or benefit)

select 'F', 
	1, 
	'1', -- Federal - Report the SUM of the current Federal tax expense (or benefit) and deferred Federal tax expense (or benefit)
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.institParentChild = 'P'
	and oppledger.expFedIncomeTax  = 'Y'
	
union 

select 'F',  
	2, 
	'2', -- State and Local - Report the SUM of the current State and Local tax expense (or benefit) and deferred State and Local tax expense (or benefit).
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- F2,1
	NULL,   -- F2,2
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.institParentChild = 'P'
		and oppledger.expStateLocalIncomeTax = 'Y'
 
union

select 'F' ,  
	3 , 
	'3', -- Line 3 , --03 - Tax Structure designate who paid the reported tax expenses for your institution
	case when clientconfig.finTaxExpensePaid = 'M' then 1
		when clientconfig.finTaxExpensePaid = 'N' then 2
		when clientconfig.finTaxExpensePaid = 'R' then 3
	end, --finTaxExpensePaidFASB
	NULL,
	NULL,
	NULL,      
	NULL, 
	NULL 
--jh 20200412 Removed oppledger inline view and removed subquery in select field by moving ClientConfigMCR to from
from ClientConfigMCR clientconfig

union	

-- Part A 
-- Balance Sheet Information         
-- This part is intended to report the assets, liabilities, and net assets. 

-- Line 01 - Total Assets:
/* Enter the amount from your GPFS which is the SUM of:
    a) Cash, cash equivalents, and temporary investments;
    b) Receivables, net (net of allowance for uncollectible amounts);
    c) Inventories, prepaid expenses, and deferred charges;
    d) Amounts held by trustees for construction and debt service;
    e) Long-term investments;
    f) Plant, property, and equipment; and,
    g) Other assets 	
*/

select 'A',  
	4 , 
	'1', -- Line 1 , --01 - Total Assets 
	NULL,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,      
	NULL,
	NULL  
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and (genledger.assetCurrent = 'Y' 
      or genledger.assetCapitalLand = 'Y'
      or genledger.assetCapitalInfrastructure = 'Y'
      or genledger.assetCapitalBuildings = 'Y'
      or genledger.assetCapitalEquipment = 'Y'
      or genledger.assetCapitalConstruction = 'Y'
      or genledger.assetCapitalIntangibleAsset = 'Y'
      or genledger.assetCapitalOther = 'Y'
      or genledger.assetNoncurrentOther = 'Y'
      or genledger.deferredOutflow = 'Y')

union 

-- Line 01a -  Long-term investments - 
/* Enter the END-of-year market value for all assets held for long-term investments. 
   Long-term investments should be distinguished from temporary investments based on the intention of 
   the organization regarding the term of the investment rather than the nature of the investment itself.
*/

select 'A',  
	5 , 
	'1', --01a - Long-term investments (GASB non-current besides capital)
	'a',
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,      
	NULL,
	NULL    
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.assetNoncurrentOther = 'Y'

union 

select 'A',  
	6, 
	'1', --01b - Property, plant, and equipment, net of accumulated depreciation
	'b',
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (genledger.assetCapitalLand = 'Y'
								  or genledger.assetCapitalInfrastructure = 'Y'
								  or genledger.assetCapitalBuildings = 'Y'
								  or genledger.assetCapitalEquipment = 'Y'
								  or genledger.assetCapitalConstruction = 'Y'
								  or genledger.assetCapitalIntangibleAsset = 'Y'
								  or genledger.assetCapitalOther = 'Y') THEN genledger.endBalance ELSE 0 END) 
					- SUM(CASE WHEN genledger.accumDepreciation = 'Y' THEN genledger.endBalance ELSE 0 END))), 0)as BIGINT), 
	NULL,
	NULL,      
	NULL,
	NULL   
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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

select 'A',  
	7, 
	'1', --01c - Intangible assets, net of accumulated amortization
	'c',
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN genledger.assetCapitalIntangibleAsset = 'Y' THEN genledger.endBalance ELSE 0 END)
					- SUM(CASE WHEN genledger.accumAmmortization = 'Y' THEN genledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,      
	NULL,
	NULL    
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and (genledger.assetCapitalIntangibleAsset = 'Y'
		or genledger.accumAmmortization = 'Y')

union 

select 'A',  
	8, 
	'2', --02  - Total liabilities 
	NULL,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,      
	NULL,
	NULL    
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and (genledger.liabCurrentLongtermDebt = 'Y'
        or genledger.liabCurrentOther = 'Y'
        or genledger.liabNoncurrentLongtermDebt = 'Y'   
        or genledger.liabNoncurrentOther = 'Y'
        or genledger.deferredInflow = 'Y')

union 

select 'A',  
	8, 
	'2', --02a - Debt related to property, plant, and equipment (long-term debt)
	'a',
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,      
	NULL,
	NULL    
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and (genledger.liabCurrentLongtermDebt = 'Y'
		or genledger.liabNoncurrentLongtermDebt = 'Y')

union

select 'A',  
	12, 
	'5', --05 - Land and land improvements
	NULL,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), 
	NULL,
	NULL,      
	NULL,
	NULL    
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.assetCapitalLand = 'Y'

union 

select 'A',  
	13, 
	'6', -- 06 - Buildings
	NULL,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), 
	NULL,
	NULL,      
	NULL,
	NULL    
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.assetCapitalBuildings = 'Y'

union

select 'A',  
	14, 
	'7', --07 - Equipment, including art and library collections
	NULL,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), 
	NULL,
	NULL,      
	NULL,
	NULL   
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.assetCapitalEquipment = 'Y'

union

select 'A',  
	15, 
	'8', --08 - Construction in Progress
	NULL,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), 
	NULL,
	NULL,      
	NULL,
	NULL    
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.assetCapitalConstruction = 'Y'

union

-- Line 09 - Other - 
-- Report all other amounts for capital assets not reported in lines 05-08.

select 'A',  
	16, 
	'9', --09 - Other
	NULL,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), 
	NULL,
	NULL,      
	NULL,
	NULL    
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and (genledger.assetCapitalOther = 'Y' 
		or genledger.assetCapitalInfrastructure = 'Y')

-- Line 10 is Calculated  (Do not Include in import File ) 

union

-- Line 11 -  Accumulated depreciation
/* Report all depreciation amounts, including depreciation on assets that may not be included 
   on any of the above lines.
*/

select 'A',  
	17, 
	'11', --  Accumulated depreciation
	NULL,
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), 
	NULL,
	NULL,      
	NULL,
	NULL    
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
	and genledger.accumDepreciation = 'Y'

union

-- Line 12 - Property, Plant, and Equipment, net of accumulated depreciation (from A1b) 
/* Note:
   the spec says this value is taken from A1b, but still requests that A12 be reported
*/

select 'A',  
	18, 
	'12', --12 - Property, Plant, and Equipment, net of accumulated depreciation (from A1b) 
	NULL,
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (genledger.assetCapitalLand = 'Y'
							  or genledger.assetCapitalInfrastructure = 'Y'
							  or genledger.assetCapitalBuildings = 'Y'
							  or genledger.assetCapitalEquipment = 'Y'
							  or genledger.assetCapitalConstruction = 'Y'
							  or genledger.assetCapitalIntangibleAsset = 'Y'
							  or genledger.assetCapitalOther = 'Y') THEN genledger.endBalance ELSE 0 END) 
					  - SUM(CASE WHEN genledger.accumDepreciation = 'Y' THEN genledger.endBalance ELSE 0 END))), 0)as BIGINT),
	NULL,
	NULL,      
	NULL,
	NULL    
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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

-- Part B - 
-- Summary of Changes in Equity    
/* This part is intended to report a summary of changes in equity and to determine that all 
   amounts being reported on the Balance Sheet Information (Part A), Revenues and Other 
   Additions (Part D), and Expenses by Function (Part E) are in agreement. 
*/

-- Line 01 - Total revenues and investment return
/* Enter all revenues that agree with the revenues recognized in your GPFS. 
   The amount provided here is important because it will be carried forward to Part D.
*/
select 'B', 
	20, 
	'1',   --  Total revenues and investment return
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance ))), 0) as BIGINT), -- B01	Total Revenue
	NULL, 
	NULL, 
	NULL, 
	NULL,
	NULL   
from OperatingLedgerMCR oppledger   
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.institParentChild = 'P'
    and  oppledger.accountType = 'Revenue'
    and (oppledger.revRealizedCapitalGains != 'Y' 
	    and oppledger.revRealizedOtherGains != 'Y' 
	    and oppledger.revExtraordGains != 'Y')

union 

-- Line 02 - Total expenses - 
/* Enter all expenses that agree with the expenses recognized in your GPFS. 
   The amount provided here is important because it will be carried forward to Part E.
*/ 
 
select 'B', 
	21, 
	'2',   --  Total expenses 
	CAST(NVL(ABS(ROUND(SUM( oppledger.endBalance))), 0) as BIGINT), -- B02
	NULL, 
	NULL,
	NULL, 
	NULL,
	NULL   
from OperatingLedgerMCR oppledger   
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.institParentChild = 'P'
	and ((oppledger.accountType = 'Expense' 
     	and (oppledger.expFedIncomeTax != 'Y' 
     	and oppledger.expExtraordLosses != 'Y')))

union

--revisit 

select 'B',  
	23 , 
	'4', -- Line 4 , --04 - Net income					
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Revenue' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT) 
	  - CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Expense' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), -- FASB B04 / GASB D03
	NULL, 
	NULL, 
	NULL, 
	NULL,
	NULL   
from OperatingLedgerMCR oppledger   
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.institParentChild = 'P'
	and (oppledger.accountType = 'Revenue'
		or oppledger.accountType = 'Expense')
 
union 

-- Line 05 - Other changes in equity - 
/* Enter the SUM of these amounts: 
   1. investments by owners
   2. distributions to owners  
   3. unrealized gains (losses) on securities and other 
   4. comprehensive income
   5. and other additions to (deductions from) owners- equity. 
*/ 

select 'B', 
	24, 
	'5', --  05 - Other changes in equity 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), 
	NULL, 
	NULL, 
	NULL, 
	NULL,
	NULL    
from OperatingLedgerMCR oppledger   
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.institParentChild = 'P'
    and (oppledger.revOwnerEquityAdjustment = 'Y'
      	or oppledger.expOwnerEquityAdjustment = 'Y' 
	  	or oppledger.revUnrealizedGains = 'Y' )

union 

select 'B', 
	25, 
	'6', --  06 - Equity, beginning of year   
	(CAST(NVL(ABS(ROUND(SUM(CASE WHEN (genledger.assetCurrent = 'Y'           --A1 Current Assets
								or genledger.assetCapitalLand = 'Y'     --A14 Net assets invested in capital assets
								or genledger.assetCapitalInfrastructure = 'Y'
								or genledger.assetCapitalBuildings = 'Y'
								or genledger.assetCapitalEquipment = 'Y'
								or genledger.assetCapitalConstruction = 'Y'
								or genledger.assetCapitalIntangibleAsset = 'Y'
								or genledger.assetCapitalOther = 'Y'
								or genledger.assetNoncurrentOther = 'Y'   --A5 Noncurrent Assets
								or genledger.deferredOutflow = 'Y')       --A19 Deferred Outflow
								THEN genledger.beginBalance ELSE 0 END))), 0) as BIGINT))
	 - (CAST(NVL(ABS(ROUND(SUM(CASE WHEN (genledger.liabCurrentLongtermDebt = 'Y'
								or genledger.liabCurrentOther = 'Y'      --A9 Current Liabilities
								or genledger.liabNoncurrentLongtermDebt = 'Y'
								or genledger.liabNoncurrentOther = 'Y'   --A12 Noncurrent Liabilities
								or genledger.deferredInflow = 'Y'        --A20 Deferred Inflow
								or genledger.accumDepreciation = 'Y'     --A31 Depreciation on capital assets
								or genledger.accumAmmortization = 'Y')   --P33 Accumulated amortization on intangible assets
								THEN genledger.beginBalance ELSE 0 END)  )), 0) as BIGINT)), -- FASB B05 / GASB D4
	NULL, 
	NULL, 
	NULL, 
	NULL,
	NULL   
from GeneralLedgerMCR genledger   
where genledger.fiscalPeriod = 'Year Begin'
	and genledger.institParentChild = 'P'
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

-- Line 07 - Adjustments to beginning net equity 

/* (Do not include in import file. Will be generated; 
   Line 8 minus SUM of (lines 4-6))
*/

-- Line 08 - Equity, END of year will be transferred from Part A, line 3. Do not include in import file.

union 

-- Part C
-- Scholarships and Fellowships 

/* This section collects information about the sources of revenue that support (1) Scholarship 
	 and Fellowship expense and (2) discounts applied to tuition and fees and auxiliary enterprises.    
*/ 

-- Line 01 Pell grants (Federal)
select 'C',  
	27, 
	'1',   -- Pell grants (Federal)
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C01					   
	NULL,  
	NULL, 
	NULL,
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFAPellGrant = 'Y'

union

select 'C',  
	28, 
	'2',   -- Other federal grants (Do NOT include FDSL amounts)
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT ), -- FASB C02				   
	NULL, 
	NULL,
	NULL,  
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFANonPellFedGrants = 'Y'
 
union
 
select 'C',  
	29, 
	'3',   -- Grants by state government
	'a', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C03a				   
	NULL, 
	NULL,
	NULL,  
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFAStateGrants = 'Y'

union 
 
select 'C',  
	30, 
	'3',   -- Grants by local government
	'b', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C04					   
	NULL, 
	NULL,
	NULL,  
	NULL 
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expFALocalGrants = 'Y'
  
union 

select 'C',  
	31, 
	'4',   -- Institutional grants 
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C01				   
	NULL, 
	NULL,
	NULL,  
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.expFAInstitGrantsRestr = 'Y'
	  	or oppledger.expFAInstitGrantsUnrestr = 'Y')

/* Line 05: Total revenue that funds scholarship and fellowships
   (Calculated (Do not include in import file )
   C,5 = CV=[C01+...+C04]
*/ 

union

select 'C',  
	34, 
	'6',   -- Discounts and Allowances applied to tuition and fees
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- FASB C08					   
	NULL, 
	NULL,
	NULL,  
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.discAllowTuitionFees = 'Y'

union
  
select 'C',  
	35, 
	'7',   -- Discounts and Allowances applied to auxiliary enterprise revenues 
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), 				   
	NULL, 
	NULL,
	NULL,  
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.discAllowAuxEnterprise = 'Y'
 
/* LINE 08 --  08: Total discounts & allowances
   (Calculated - Do not include in import file )
   C,8 = CV=[C06+C07]
*/  

union 

-- Part D 
-- Revenues by Source 
  
/* This part is intended to report revenues by source.  The revenues and investment return reported in 
   this part should agree with the revenues reported in the institution-s GPFS.   
   Exclude from revenues (and expenses) internal changes and credits. Internal changes and credits 
   include charges between parent and subsidiary only if the two are consolidated in the amounts 
   reported in the IPEDS survey.
*/

select 'D', 
	37, 
	'1', -- Line 1 ,  --  Tuition and fees (net of amount reported in Part C, line 06)
	NULL,
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revTuitionandFees = 'Y' THEN oppledger.endBalance ELSE 0 END) 
					- SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revTuitionandFees = 'Y' 
		or  oppledger.discAllowTuitionFees = 'Y')
 
union 
 
-- Government Appropriations, Grants and Contracts 
-- Line 02a - Federal appropriations

select 'D', 
	38, 
	'2', --  Federal appropriations
	'a',
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revFedApproprations = 'Y'
            
union 

-- Line 2b - Federal grants and contracts (Do not include FDSL)

select 'D', 
	39, 
	'2', --  Federal grants and contracts (Do not include FDSL)
	'b',
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
    and oppledger.revFedGrantsContractsOper = 'Y'

union  		

-- Line 3a - State appropriations

select 'D', 
	40, 
	'3', --  State appropriations
	'a', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revStateApproprations = 'Y'   

union 

-- Line 3b  State grants and contracts

select 'D', 
	41, 
	'3', --  State grants and contracts
	'b',
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revStateGrantsContractsOper = 'Y'

union 

-- Line 3c  Local government appropriations

select 'D', 
	42  , 
	'3' , --  Local government appropriations
	'c', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revLocalApproprations = 'Y'

union 

-- Line 3d - Local government grants and contracts

select 'D', 
	43, 
	'3', --  Local government grants and contracts
	'd', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revLocalGrantsContractsOper = 'Y'

union  

 -- Line 4 - Private gifts,  grants and contracts

select 'D', 
	44, 
	'4', --  Private gifts,  grants and contracts
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revPrivGifts = 'Y' 
		or oppledger.revPrivGrantsContractsOper = 'Y' 
		or oppledger.revPrivGrantsContractsNOper = 'Y')

union  

-- Line 5 - Investment income and investment gains (losses) included in net income

select 'D',  
	45, 
	'5', --  Investment income and investment gains (losses) included in net income
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revInvestmentIncome = 'Y'

union 

select 'D',
	46, 
	'6', -- Line 6 ,   --  Sales and services of educational activities
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revEducActivSalesServices = 'Y'
	
union  

select 'D',
	47 , 
	'7', -- Line 7 ,   --  Sales and services of auxiliary enterprises (net of amount reported in Part C, line 07)
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revAuxEnterprSalesServices = 'Y' THEN oppledger.endBalance ELSE 0 END) 
					 - SUM(CASE WHEN oppledger.discAllowAuxEnterprise = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revAuxEnterprSalesServices = 'Y'
		or oppledger.discAllowAuxEnterprise = 'Y')

union 
 
select 'D', 
	48, 
	'12', -- Line 12 ,  --  Hospital revenue
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revHospitalSalesServices = 'Y' THEN oppledger.endBalance ELSE 0 END) 
					- SUM(CASE WHEN oppledger.discAllowPatientContract = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revHospitalSalesServices = 'Y'
		or oppledger.discAllowPatientContract = 'Y')
 
/* Line 08  - Other Revenue  CV=[D09-(D01+...+D07+D12)]
   Calculated (Do not include in import file )  
*/ 
   
union 

-- Line 09 - Total revenues and investment return
 
select 'D',
	50, 
	'9',  --  Total revenues and investment return
	NULL, 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL, 
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.accountType = 'Revenue'
   		and (oppledger.revRealizedCapitalGains != 'Y' 
      		and oppledger.revRealizedOtherGains != 'Y' 
      		and oppledger.revExtraordGains != 'Y'
      		and oppledger.revOwnerEquityAdjustment != 'Y' 
      		and oppledger.revSumOfChangesAdjustment != 'Y'))
 
/* Line 10 - 12-MONTH Student FTE from E12 Carried over from E12  
   (Calculated - Do not include in import file )  
    Line 11 - 	Total revenues and investment return per student FTE CV=[D09/D10]
   (Calculated - Do not include in import file ) 
*/ 

union  

-- PART E-1  
-- Expenses by Functional Classification

/* Report Total Operating and Nonoperating Expenses in this section   
   Part E is intended to report expenses by function. All expenses recognized in the GPFS should be reported 
   using the expense functions provided on lines 01-06, 10, and 11. These functional categories are consistent 
   with Chapter 5 (Accounting for Independent Colleges and Universities) of the NACUBO FARM. (FARM para 504)

   Although for-profit institutions are not required to report expenses by functions in their GPFS, please 
   report expenses by functional categories using your underlying accounting records. 
   Expenses should be assigned to functional categories by direct identification with a function, wherever 
   possible. When direct assignment to functional categories is not possible, an allocation is appropriate.
*/ 

-- Line 01 - Instruction

select 'E',
	53, 
	'1',   -- Instruction | E1,1  
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --   E1,1
	NULL, 
	NULL, 
	NULL, 
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isInstruction = 1

union 

select 'E',
	54, 
	'1',   -- Instruction | E1,1 
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --   E1,2
	NULL, 
	NULL, 
	NULL, 
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.expSalariesWages = 'Y'  
	and oppledger.isInstruction = 1

union

select 'E',
	55, 
	'2a',   -- Research | E1,2a  
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --  E2,1
	NULL, 
	NULL, 
	NULL, 
	NULL 
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isResearch = 1

union

select 'E',
	56, 
	'2a',   -- Research | E1,2a   
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E2,2
	NULL, 
	NULL, 
	NULL, 
	NULL 
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.expSalariesWages = 'Y' 
	and oppledger.isResearch = 1 

union 

select 'E',
	57, 
	'2b',   -- Public service | E1,2b 
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E3,1
	NULL, 
	NULL, 
	NULL, 
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isPublicService = 1

union 

select 'E',
	58, 
	'2b',   -- Public service | E1,2b 
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E3,2
	NULL, 
	NULL, 
	NULL, 
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.expSalariesWages = 'Y' 
	and oppledger.isPublicService = 1

union 

select 'E',
	59, 
	'3a',   -- Academic support | E1,3a 
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E4,1
	NULL, 
	NULL, 
	NULL, 
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isAcademicSupport = 1
 
union 

select 'E',
	60, 
	'3a',   -- Academic support | E1,3a 
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E4,2
	NULL, 
	NULL, 
	NULL, 
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.expSalariesWages = 'Y' 
	and oppledger.isAcademicSupport = 1

union 

select 'E',
	61, 
	'3b',   -- Student services | E1,3b 
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E5,1
	NULL, 
	NULL, 
	NULL, 
	NULL 
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isStudentServices = 1
	
union 

select 'E',
	62, 
	'3b',   -- Student services | E1,3b 
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E5,2
	NULL, 
	NULL, 
	NULL, 
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.expSalariesWages = 'Y' 
	and oppledger.isStudentServices = 1
	
union 

select 'E',
	63, 
	'3c',   -- Institutional support | E3c,6 
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E6,1
	NULL, 
	NULL, 
	NULL, 
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isInstitutionalSupport = 1
	
union 

select 'E',
	64, 
	'3c',   -- Institutional support | E3c,6 
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E6,1
	NULL, 
	NULL, 
	NULL, 
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.expSalariesWages = 'Y' 
	and oppledger.isInstitutionalSupport = 1
	
union 

select 'E', 
	100, 
	'4',   -- Auxiliary enterprises | E1,4 
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --E7,1
	NULL, 
	NULL, 
	NULL, 
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
    and oppledger.accountType = 'Expense' 
    and oppledger.isAuxiliaryEnterprises  = 1

union 

select 'E', 
	101, 
	'4',   -- Auxiliary enterprises | E1,4 
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E7,2
	NULL, 
	NULL, 
	NULL, 
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.expSalariesWages = 'Y' 
	and oppledger.isAuxiliaryEnterprises = 1
 
union

select 'E', 
	102, 
	'5',   -- Net grant aid to students | E1,5  
	1,
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.expFAPellGrant = 'Y'          --Pell grants
								  or oppledger.expFANonPellFedGrants = 'Y'    --Other federal grants
								  or oppledger.expFAStateGrants = 'Y'         --Grants by state government
								  or oppledger.expFALocalGrants = 'Y'         --Grants by local government
								  or oppledger.expFAInstitGrantsRestr = 'Y'   --Institutional grants from restricted resources
								  or oppledger.expFAInstitGrantsUnrestr = 'Y' --Institutional grants from unrestricted resources
								) THEN oppledger.endBalance ELSE 0 END)
		-             SUM(CASE WHEN (oppledger.discAllowTuitionFees = 'Y'      --Discounts and allowances applied to tuition and fees
								  or oppledger.discAllowAuxEnterprise = 'Y'    --Discounts and allowances applied to sales and services of auxiliary enterprises
							   ) THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), 
	NULL, 
	NULL, 
	NULL, 
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.expFAPellGrant = 'Y'            
       	or oppledger.expFANonPellFedGrants = 'Y'  
       	or oppledger.expFAStateGrants = 'Y'       
       	or oppledger.expFALocalGrants = 'Y'    
       	or oppledger.expFAInstitGrantsRestr = 'Y'   
       	or oppledger.expFAInstitGrantsUnrestr = 'Y'   
	    or oppledger.discAllowTuitionFees = 'Y'  
       	or oppledger.discAllowAuxEnterprise = 'Y')

union 

-- Line 10 - Hospital services (as applicable) - 

/* Enter all expenses associated with the operation of a hospital reported as a component of an 
  institution of higher education. Include nursing expenses, other professional services, administrative services, 
  fiscal services, and charges for operation and maintenance of plant. (FARM para. 703.12) 
  Hospitals or medical centers reporting educational program activities conducted independent of an 
  institution of higher education (not as a component of a reporting institution of higher education) 
  should not complete this line.
*/ 

select 'E', 
	103, 
	'10',   -- Hospital Services | E1,10 
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E9,1
	NULL, 
	NULL,
	NULL,
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isHospitalServices  = 1 
 
union

select 'E', 
	104, 
	'10',   -- Hospital Services | E1,10 
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E9,2
	NULL, 
	NULL, 
	NULL, 
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
    and oppledger.accountType = 'Expense' 
    and oppledger.expSalariesWages = 'Y' 
    and oppledger.isHospitalServices = 1

-- Line 06 - Other Expenses and Deduction  CV=[E07-(E01+...+E10)]
-- Calculated (Do not include in import file )  
	
union 

-- Line 07 - Column 1 - Total

select 'E', 
	107, 
	'7',   -- Total expenses and Deductions | E-7
	1,     -- Column 1 - Total
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E13,1 
	NULL,
	NULL,
	NULL,
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and (expFedIncomeTax != 'Y'
    	and expExtraordLosses != 'Y' )
 
union

-- Line 07 - Column 2 - Salaries and wages

select 'E', 
	108, 
	'7',   -- Total expenses and Deductions | E-7 
	2,	   -- Column 2 - Salaries and wages
	CAST(NVL(ABS(ROUND(SUM( oppledger.endBalance ))), 0) as BIGINT), -- E13,2
	NULL,
	NULL,
	NULL,
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.expSalariesWages = 'Y'
 
union

-- Line 07 - Column 3 - Benefits

select 'E', 
	109, 
	'7',   -- Total expenses and Deductions | E-7
	3,     -- Column 3 - Benefits
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E13-3 Benefits
	NULL,
	NULL, 
	NULL,
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expBenefits = 'Y'
 
union 

-- Line 07,4 - Operation and Maintenance of Plant

select 'E', 
	110, 
	'7',   -- Total expenses and Deductions | E-7 
	4, 	   -- Column 4, Operation and Maintenance of Plant
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), -- E7-4,
	NULL,
	NULL, 
	NULL,
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.expCapitalConstruction = 'Y'
     	or oppledger.expCapitalEquipPurch = 'Y'
     	or oppledger.expCapitalLandPurchOther = 'Y')
 
union 

-- Line 07,5 Depreciation 

select 'E', 
	111, 
	'7',   -- Total expenses and Deductions | E-7 
	5,      --Column 5 - Depreciation
	CAST(NVL(ABS(ROUND(SUM( oppledger.endBalance ))), 0) as BIGINT), -- E13-5 Depreciation
	NULL,
	NULL, 
	NULL,
	NULL 
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expDepreciation = 'Y' 
  
union  

-- Line 07,6 Interest 

select 'E', 
	112, 
	'7',   -- Total expenses and Deductions | E-7 
	6,      --6 Interest 
	CAST(NVL(ABS(ROUND(SUM( oppledger.endBalance ))), 0) as BIGINT), -- E13-6 Interest
	NULL,
	NULL,
	NULL, 
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End' 
    and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.expInterest = 'Y' 
 
/* Line 7,7 - Other Natural Expenses and Deductions 
     (Do not include in import file. 
		 Will be generated as follows: line 7 minus the SUM of (lines 7-2 through 7-6))
		 12-MONTH Student FTE from E12
Line 8,1 - Total amount 
     (Do not include in import file. Will be generated)
		 Total expenses per student FTE
Line 9,1 - Total amount 
      (Do not include in import file. Will be generated as the ratio of lines E7 over E8) 
*/  

--Order by 2 asc 
