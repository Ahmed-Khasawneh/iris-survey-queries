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
20200412			jhanicak			jh 20204012		 Added dummy date option for recordActivityDate in most current record queries PF-1374
														 Added DefaultValues query and rewrote other queries to use PF-1418
														 Removed all 'prior' queries - not needed
20200303            jhysler             jdh 2020-03-03  PF-1297 Modified all sections to pull from OL or GL and added parent/child logic 
20200226            jhanicak            jh 20200226     PF-1257
                                                        Added most recent record views for GeneralLedgerReporting and OperatorLedgerReporting
                                                        Removed FYPerAsOfDate/FYPerPriorAsOfDate
                                                        Modified COASPerFYAsOfDate/COASPerFYPriorAsOfDate to include IPEDSClientConfig values
                                                        and most recent records based on COASPerAsOfDate/COASPerPriorAsOfDate and FiscalYear
                                                        Removed join of ConfigPerAsOfDate since config values are in COASPerFYAsOfDate
                                                        Modified all sections to pull from OL or GL and added parent/child logic
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
ReportingDates AS
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
    (select nvl(CASE WHEN upper(reportPeriod.surveySection) = upper(defaultValues.currentSection) THEN reportPeriod.asOfDate END, defaultValues.asOfDate) asOfDate,
			nvl(CASE WHEN upper(reportPeriod.surveySection) = upper(defaultValues.priorSection) THEN reportPeriod.asOfDate END, defaultValues.priorAsOfDate) priorAsOfDate,
        reportPeriod.surveyCollectionYear surveyYear,
            reportPeriod.surveyId surveyId,
            defaultValues.currentSection currentSection,
		    defaultValues.priorSection priorSection,
            defaultValues.finGPFSAuditOpinion finGPFSAuditOpinion,
	        defaultValues.finAthleticExpenses finAthleticExpenses,
	        defaultValues.finEndowmentAssets finEndowmentAssets,
	        defaultValues.finPensionBenefits finPensionBenefits,
	        defaultValues.finPellTransactions finPellTransactions,
	        defaultValues.finParentOrChildInstitution finParentOrChildInstitution,
        ROW_NUMBER() OVER (
            PARTITION BY
                reportPeriod.surveyCollectionYear,
                                reportPeriod.surveyId,
                reportPeriod.surveySection
            ORDER BY
                reportPeriod.recordActivityDate DESC
        ) AS reportPeriodRn
                    from DefaultValues defaultValues
                        cross join IPEDSReportingPeriod reportPeriod
                    where reportPeriod.surveyCollectionYear = defaultValues.surveyYear
                            and reportPeriod.surveyId = defaultValues.surveyId
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

select defaultValues.surveyYear surveyYear,
    defaultValues.surveyId surveyId,
    defaultValues.asOfDate asOfDate,
    defaultValues.priorAsOfDate priorAsOfDate,
    defaultValues.currentSection currentSection,
	defaultValues.priorSection priorSection,
    defaultValues.finGPFSAuditOpinion finGPFSAuditOpinion,
	defaultValues.finAthleticExpenses finAthleticExpenses,
	defaultValues.finEndowmentAssets finEndowmentAssets,
	defaultValues.finPensionBenefits finPensionBenefits, 
	defaultValues.finPellTransactions finPellTransactions,
	defaultValues.finParentOrChildInstitution finParentOrChildInstitution
from DefaultValues defaultValues
where defaultValues.surveyYear not in (select surveyYear
                                    from DefaultValues defValues
                                        cross join IPEDSReportingPeriod reportPeriod
                                    where reportPeriod.surveyCollectionYear = defValues.surveyYear
                                        and reportPeriod.surveyId = defValues.surveyId)
),

/*****
BEGIN SECTION - Most Recent Records
The views below pull the most recent records based on activity date and other fields, as required
*****/

--jh 20200412 Added DefaultValues query and rewrote other queries to use PF-1418

ConfigPerAsOfDate AS
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
    select ReportingDates.surveyYear surveyCollectionYear,
	            ReportingDates.asOfDate asOfDate,
                ReportingDates.priorAsOfDate priorAsOfDate,
                nvl(config.finGPFSAuditOpinion, ReportingDates.finGPFSAuditOpinion) finGPFSAuditOpinion,
                nvl(config.finAthleticExpenses, ReportingDates.finAthleticExpenses) finAthleticExpenses,
                nvl(config.finEndowmentAssets, ReportingDates.finEndowmentAssets) finEndowmentAssets,
                nvl(config.finPensionBenefits, ReportingDates.finPensionBenefits) finPensionBenefits,
                nvl(config.finPellTransactions, ReportingDates.finPellTransactions) finPellTransactions,
                nvl(config.finParentOrChildInstitution, ReportingDates.finParentOrChildInstitution) finParentOrChildInstitution,
                ROW_NUMBER() OVER (
                        PARTITION BY
                                config.surveyCollectionYear
                        ORDER BY
                                config.recordActivityDate DESC
                ) AS configRn
    from IPEDSClientConfig config
       inner join ReportingDates ReportingDates on ReportingDates.surveyYear = config.surveyCollectionYear
      )
where configRn = 1

union

select ReportingDates.surveyYear,
      ReportingDates.asOfDate asOfDate,
      ReportingDates.priorAsOfDate priorAsOfDate,
	  ReportingDates.finGPFSAuditOpinion finGPFSAuditOpinion,
	  ReportingDates.finAthleticExpenses finAthleticExpenses,
	  ReportingDates.finEndowmentAssets finEndowmentAssets,
	  ReportingDates.finPensionBenefits finPensionBenefits, 
	  ReportingDates.finPellTransactions finPellTransactions,
	  ReportingDates.finParentOrChildInstitution finParentOrChildInstitution
from ReportingDates ReportingDates
where ReportingDates.surveyYear not in (select config.surveyCollectionYear
                        from IPEDSClientConfig config
		                	inner join ReportingDates ReportingDates on ReportingDates.surveyYear = config.surveyCollectionYear)
),

COASPerAsOfDate AS (
select *
from (
    select COAS.*,
      ROW_NUMBER() OVER (
        PARTITION BY
          COAS.chartOfAccountsId
        ORDER BY
          COAS.startDate DESC,
          COAS.recordActivityDate DESC
      ) AS COASRn
    from ConfigPerAsOfDate config
         inner join ChartOfAccounts COAS ON COAS.startDate <= config.asOfDate
--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
         and ((COAS.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) 
		            and COAS.recordActivityDate <= config.asOfDate)
			    or COAS.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
         and (COAS.endDate IS NULL or COAS.endDate >= config.asOfDate)
         and COAS.statusCode = 'Active'  
         and COAS.isIPEDSReportable = 1
  )
where COASRn = 1
),

--jh 20200412 Added DefaultValues query and rewrote other queries to use PF-1418

COASPerFYAsOfDate AS (
select FYData.asOfDate asOfDate,
        FYData.priorAsOfDate priorAsOfDate,

        CASE FYData.finGPFSAuditOpinion
                    when 'Q' then 1
                    when 'UQ' then 2
                    when 'U' then 3 END finGPFSAuditOpinion, --1=Unqualified, 2=Qualified, 3=Don't know
        CASE FYData.finAthleticExpenses 
                    WHEN 'A' THEN 1
                    WHEN 'S' THEN 2 
                    WHEN 'N' THEN 3 
                    WHEN 'O' THEN 4 END finAthleticExpenses, --1=Auxiliary enterprises 2=Student services 3=Does not participate in intercollegiate athletics 4=Other (specify in caveats box below)
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
        case when COAS.isParent = 1 then 'P'
             when COAS.isChild = 1 then 'C' end COASParentChild
from (select FY.*,
                config.asOfDate asOfDate,
                config.priorAsOfDate priorAsOfDate,
                config.finGPFSAuditOpinion finGPFSAuditOpinion,                
                config.finAthleticExpenses finAthleticExpenses,
				config.finPellTransactions finPellTransactions,
                config.finParentOrChildInstitution finParentOrChildInstitution,
		ROW_NUMBER() OVER (
			PARTITION BY
				FY.chartOfAccountsId,
				        FY.fiscalYear4Char,
				        FY.fiscalPeriodCode
			ORDER BY
				FY.fiscalYear4Char DESC,
				FY.fiscalPeriodCode DESC,
				FY.recordActivityDate DESC
		) AS FYRn
	    from ConfigPerAsOfDate config
		    left join FiscalYear FY on FY.startDate <= config.asOfDate
           and FY.fiscalPeriod in ('Year Begin', 'Year End')
                --and FY.fiscalPeriod in ('1st Month', '12th Month') --test only
--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
                and ((FY.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) 
		            and FY.recordActivityDate <= config.asOfDate)
			    or FY.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
		        and (FY.endDate IS NULL or FY.endDate >= config.asOfDate)
                and FY.isIPEDSReportable = 1
        ) FYData 
            left join COASPerAsOfDate COAS on FYData.chartOfAccountsId = COAS.chartOfAccountsId	
where FYData.FYRn = 1
),

--jh 20200226 Added most recent record views for GeneralLedgerReporting

GL AS (
select *
from (
    select GL.*,
		COAS.fiscalPeriod fiscalPeriod,
		COAS.institParentChild institParentChild, --is the institution a Parent or Child 
		COAS.COASParentChild COASParentChild,     --is the COAS a Parent or Child account
		ROW_NUMBER() OVER (
            PARTITION BY
                GL.chartOfAccountsId,
                GL.fiscalYear2Char,
		        GL.fiscalPeriodCode,
		        GL.accountingString
            ORDER BY
                GL.recordActivityDate DESC
		    ) AS GLRn
    from COASPerFYAsOfDate COAS
		left join GeneralLedgerReporting GL on GL.chartOfAccountsId = COAS.chartOfAccountsId
				and GL.fiscalYear2Char = COAS.fiscalYear2Char  
				and GL.fiscalPeriodCode = COAS.fiscalPeriodCode
--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
				and ((GL.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) 
				    and GL.recordActivityDate <= COAS.asOfDate)
				   or GL.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
				and GL.isIPEDSReportable = 1
  )
where GLRn = 1
),

OL AS (
select *
from (
    select OL.*,
		COAS.fiscalPeriod fiscalPeriod,
	    COAS.institParentChild institParentChild,--is the institution a Parent or Child
	    COAS.COASParentChild COASParentChild,    --is the COAS a Parent or Child account
		ROW_NUMBER() OVER (
            PARTITION BY
                OL.chartOfAccountsId,
                OL.fiscalYear2Char,
		        OL.fiscalPeriodCode,
		        OL.accountingString
            ORDER BY
                OL.recordActivityDate DESC
		    ) AS OLRn
    from COASPerFYAsOfDate COAS
		left join OperatingLedgerReporting OL on OL.chartOfAccountsId = COAS.chartOfAccountsId
				and OL.fiscalYear2Char = COAS.fiscalYear2Char  
				and OL.fiscalPeriodCode = COAS.fiscalPeriodCode
--jh 20204012 Added dummy date option for recordActivityDate in most current record queries PF-1374
			and ((OL.recordActivityDate != CAST('9999-09-09' AS TIMESTAMP) 
				    and OL.recordActivityDate <= COAS.asOfDate)
				   or OL.recordActivityDate = CAST('9999-09-09' AS TIMESTAMP))
				and OL.isIPEDSReportable = 1
  )
where OLRn = 1
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
--jh 20200226 Removed join of ConfigPerAsOfDate since config values are in COASPerFYAsOfDate

select DISTINCT '9' part,
                0 sort,
                CAST(MONTH(nvl(COAS.startDate, COAS.priorAsOfDate))  as BIGINT) field1,
                CAST(YEAR(nvl(COAS.startDate, COAS.priorAsOfDate)) as BIGINT) field2,
                CAST(MONTH(nvl(COAS.endDate, COAS.asOfDate)) as BIGINT) field3,
                CAST(YEAR(nvl(COAS.endDate, COAS.asOfDate)) as BIGINT) field4,
                COAS.finGPFSAuditOpinion field5, --1=Unqualified, 2=Qualified, 3=Don't know
--jh 20200217 removed finReportingModel
                COAS.finAthleticExpenses field6, --1=Auxiliary enterprises 2=Student services 3=Does not participate in intercollegiate athletics 4=Other (specify in caveats box below)
--jh 20200217 added finPellTransactions
				COAS.finPellTransactions field7 --P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants, no record or null default = 'P'
from COASPerFYAsOfDate COAS

union

--jh 20200226 Modified all sections to pull from OL or GL and added parent/child logic

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
        CAST((NVL(ABS(ROUND(SUM(CASE when (GL.assetCapitalLand = 'Y'
                                or GL.assetCapitalInfrastructure = 'Y'
                                or GL.assetCapitalBuildings = 'Y'
                                or GL.assetCapitalEquipment = 'Y'
                                or GL.assetCapitalConstruction = 'Y'
                                or GL.assetCapitalIntangibleAsset = 'Y'
                                or GL.assetCapitalOther = 'Y'
                                or GL.assetNoncurrentOther = 'Y') then GL.endBalance ELSE 0 END))), 0) --  GASB A5
        -  -- Depreciable capital assets, net of depreciation.
        NVL(ABS(ROUND(SUM(CASE when (GL.assetCapitalLand = 'Y'
                                or GL.assetCapitalInfrastructure = 'Y'
                                or GL.assetCapitalBuildings = 'Y'
                                or GL.assetCapitalEquipment = 'Y'
                                or GL.assetCapitalConstruction = 'Y'
                                or GL.assetCapitalIntangibleAsset = 'Y'
                                or GL.assetCapitalOther = 'Y') then GL.endBalance ELSE 0 END)
                    - SUM(CASE when GL.accumDepreciation = 'Y' then GL.endBalance ELSE 0 END))), 0) -- GASB A31
        ) as BIGINT),  -- FASB A01 -- Formula CV = [A05-A31]
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
	and ((GL.assetCapitalLand = 'Y' 
			or GL.assetCapitalInfrastructure = 'Y'  
			or GL.assetCapitalBuildings = 'Y' 
			or GL.assetCapitalEquipment = 'Y'  
			or GL.assetCapitalConstruction = 'Y' 
			or GL.assetCapitalIntangibleAsset = 'Y' 
			or GL.assetCapitalOther = 'Y' 
			or GL.assetNoncurrentOther = 'Y')
	  	or (GL.assetCapitalLand = 'Y' 
			or GL.assetCapitalInfrastructure = 'Y' 
			or GL.assetCapitalBuildings = 'Y'  
			or GL.assetCapitalEquipment = 'Y' 
			or GL.assetCapitalConstruction = 'Y' 
			or GL.assetCapitalIntangibleAsset = 'Y' 
			or GL.assetCapitalOther = 'Y')
	  	or (GL.accumDepreciation = 'Y'))
    
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
        CAST((NVL(ABS(ROUND(SUM(CASE when (GL.assetCapitalLand = 'Y'
                                or GL.assetCapitalInfrastructure = 'Y'
                                or GL.assetCapitalBuildings = 'Y'
                                or GL.assetCapitalEquipment = 'Y'
                                or GL.assetCapitalConstruction = 'Y'
                                or GL.assetCapitalIntangibleAsset = 'Y'
                                or GL.assetCapitalOther = 'Y') then GL.endBalance ELSE 0 END)
                          - SUM(CASE when GL.accumDepreciation = 'Y' then GL.endBalance ELSE 0 END))), 0)) as BIGINT),  -- FASB A19 / -- GASB A31
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and GL.institParentChild = 'P'
	and ((GL.assetCapitalLand = 'Y' 
       or GL.assetCapitalInfrastructure = 'Y'  
       or GL.assetCapitalBuildings = 'Y' 
       or GL.assetCapitalEquipment = 'Y' 
       or GL.assetCapitalConstruction = 'Y' 
       or GL.assetCapitalIntangibleAsset = 'Y' 
       or GL.assetCapitalOther = 'Y')
	   or (GL.accumDepreciation = 'Y'))

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
        CAST((NVL(ABS(ROUND(SUM(CASE when GL.assetCapitalIntangibleAsset = 'Y' then GL.endBalance ELSE 0 END)
                          - SUM(CASE when GL.accumAmmortization = 'Y' then GL.endBalance ELSE 0 END))), 0)) as BIGINT),  -- FASB A20/GASB A33
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and GL.institParentChild = 'P'
	and (GL.assetCapitalIntangibleAsset = 'Y'
	  or GL.accumAmmortization = 'Y')

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
        CAST((NVL(ABS(ROUND(SUM(CASE when GL.assetCurrent = 'Y' then GL.endBalance ELSE 0 END))), 0) -- GASB A1
            + NVL(ABS(ROUND(SUM(CASE when (GL.assetCapitalLand = 'Y'
                                    or GL.assetCapitalInfrastructure = 'Y'
                                    or GL.assetCapitalBuildings = 'Y'
                                    or GL.assetCapitalEquipment = 'Y'
                                    or GL.assetCapitalConstruction = 'Y'
                                    or GL.assetCapitalIntangibleAsset = 'Y'
                                    or GL.assetCapitalOther = 'Y'
                                    or GL.assetNoncurrentOther = 'Y') then GL.endBalance ELSE 0 END))), 0) -- GASB A5
             ) as BIGINT),  -- FASB A02 / GASB (A01+A05)
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
	and (GL.assetCurrent = 'Y'
	   or (GL.assetCapitalLand = 'Y'
       or GL.assetCapitalInfrastructure = 'Y'  
       or GL.assetCapitalBuildings = 'Y'  
       or GL.assetCapitalEquipment = 'Y' 
       or GL.assetCapitalConstruction = 'Y' 
       or GL.assetCapitalIntangibleAsset = 'Y' 
       or GL.assetCapitalOther = 'Y'
       or GL.assetNoncurrentOther = 'Y'))
    
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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),  -- A03
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
	and (GL.liabCurrentLongtermDebt = 'Y'
      or GL.liabCurrentOther = 'Y'
      or GL.liabNoncurrentLongtermDebt = 'Y'
      or GL.liabNoncurrentOther = 'Y')

    
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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),  -- A03a
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and GL.institParentChild = 'P'
	and GL.liabNoncurrentLongtermDebt = 'Y'

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
        CAST(((((NVL(ABS(ROUND(SUM(CASE when (GL.assetCurrent = 'Y'
                                or GL.assetCapitalLand = 'Y'
                                or GL.assetCapitalInfrastructure = 'Y'
                                or GL.assetCapitalBuildings = 'Y'
                                or GL.assetCapitalEquipment = 'Y'
                                or GL.assetCapitalConstruction = 'Y'
                                or GL.assetCapitalIntangibleAsset = 'Y'
                                or GL.assetCapitalOther = 'Y'
                                or GL.assetNoncurrentOther = 'Y'
                                or GL.deferredOutflow = 'Y') then GL.endBalance ELSE 0 END))), 0) ))
                - (NVL(ABS(ROUND(SUM(CASE when (GL.liabCurrentLongtermDebt = 'Y' or GL.liabCurrentOther = 'Y'
                                    or GL.liabNoncurrentLongtermDebt = 'Y' or GL.liabNoncurrentOther = 'Y')
                                        then GL.endBalance ELSE 0 END))), 0)
                + NVL(ABS(ROUND(SUM(CASE when GL.deferredInflow = 'Y' then GL.endBalance ELSE 0 END))), 0)))
                - (((NVL(ABS(ROUND(SUM(CASE when (GL.assetCapitalLand = 'Y'
                                or GL.assetCapitalInfrastructure = 'Y'
                                or GL.assetCapitalBuildings = 'Y'
                                or GL.assetCapitalEquipment = 'Y'
                                or GL.assetCapitalConstruction = 'Y'
                                or GL.assetCapitalIntangibleAsset = 'Y'
                                or GL.assetCapitalOther = 'Y') then GL.endBalance ELSE 0 END)
                    - SUM(CASE when GL.accumDepreciation = 'Y' or GL.isCapitalRelatedDebt = 1 then GL.endBalance ELSE 0 END))), 0))
                + (NVL(ABS(ROUND(SUM(CASE when GL.accountType = 'Asset' 
                               and GL.isRestrictedExpendOrTemp = 1 then GL.endBalance ELSE 0 END))), 0))
                + (NVL(ABS(ROUND(SUM(CASE when GL.accountType = 'Asset' 
                               and GL.isRestrictedNonExpendOrPerm = 1 then GL.endBalance ELSE 0 END))), 0))))) as BIGINT),  --FASB A04/GASB A17
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and GL.institParentChild = 'P'

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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),  -- A05a
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
	and GL.accountType = 'Asset'
	and GL.isRestrictedExpendOrTemp = 1

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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),  -- FASB A05b / GASB A16
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
	and GL.accountType = 'Asset'
	and GL.isRestrictedNonExpendOrPerm = 1

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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),  -- FASB A11 / GASB A21
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and GL.institParentChild = 'P'
	and GL.assetCapitalLand = 'Y'


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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), -- FASB A12 / GASB A23
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and GL.institParentChild = 'P'
	and GL.assetCapitalBuildings = 'Y'

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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), -- FASB A13 / GASB A32
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and GL.institParentChild = 'P'
	and GL.assetCapitalEquipment = 'Y'

union

-- Line 15 -  Construction in progress - 
/*  Report capital assets under construction and not yet placed into service.			 
    FASB A15 / GASB A27 
*/

select 'A',
        15,
        '15',   -- Construction in Progress
        null,
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), -- FASB A15 / GASB A27
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and GL.institParentChild = 'P'
	and GL.assetCapitalConstruction = 'Y'

union

-- Line 16 - Other - 
/*  Report all other amounts for capital assets not reported in lines 11-15. 
    FASB A16 / GASB A34
*/

select 'A',
        16,
        '16',    --  Other
        null,
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), -- FASB A16 / GASB A34
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and GL.institParentChild = 'P'
	and GL.assetCapitalOther = 'Y'


union

-- Line 18 - Accumulated depreciation  
/*  Report all depreciation amounts, including depreciation on assets that may not be included on any of the above lines. 
    FASB A18 / GASB A28
*/

select 'A',
        18,
        '18',	--Accumulated depreciation
        null,
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), -- FASB A18 / GASB A28
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and GL.institParentChild = 'P'
	and GL.accumDepreciation = 'Y'

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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- B01	Total Revenue
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Revenue'
	    and (OL.revRealizedCapitalGains != 'Y' 
            and OL.revRealizedOtherGains != 'Y' 
            and OL.revExtraordGains != 'Y'))
	and OL.institParentChild = 'P'
	
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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- B02
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
       and (OL.expFedIncomeTax != 'Y'  
         and OL.expExtraordLosses != 'Y' ))
	and OL.institParentChild = 'P'
	
union

-- Line 04 - Change in net assets

select 'B',
        23 ,
        '4',   --  Change in net assets
        CAST(((NVL(ABS(ROUND(SUM(CASE when OL.accountType = 'Revenue' then OL.endBalance ELSE 0 END))), 0))
            - (NVL(ABS(ROUND(SUM(CASE when OL.accountType = 'Expense' then OL.endBalance ELSE 0 END))), 0))) as BIGINT), -- FASB B04 / GASB D03
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Revenue' 
       or OL.accountType = 'Expense')
	and OL.institParentChild = 'P'
	
union

 select 'B',
        24,
        '5',   --  Net assets, beginning of year
        CAST((NVL(ABS(ROUND(SUM(CASE when (GL.assetCurrent = 'Y'           --A1 Current Assets
                                    or GL.assetCapitalLand = 'Y'     --A14 Net assets invested in capital assets
                                    or GL.assetCapitalInfrastructure = 'Y'
                                    or GL.assetCapitalBuildings = 'Y'
                                    or GL.assetCapitalEquipment = 'Y'
                                    or GL.assetCapitalConstruction = 'Y'
                                    or GL.assetCapitalIntangibleAsset = 'Y'
                                    or GL.assetCapitalOther = 'Y'
                                    or GL.assetNoncurrentOther = 'Y'   --A5 Noncurrent Assets
                                    or GL.deferredOutflow = 'Y')       --A19 Deferred Outflow
                                    then GL.beginBalance ELSE 0 END))), 0)
            - NVL(ABS(ROUND(SUM(CASE when (GL.liabCurrentLongtermDebt = 'Y'
                                    or GL.liabCurrentOther = 'Y'      --A9 Current Liabilities
                                    or GL.liabNoncurrentLongtermDebt = 'Y'
                                    or GL.liabNoncurrentOther = 'Y'   --A12 Noncurrent Liabilities
                                    or GL.deferredInflow = 'Y'        --A20 Deferred Inflow
                                    or GL.accumDepreciation = 'Y'     --A31 Depreciation on capital assets
                                    or GL.accumAmmortization = 'Y')   --P33 Accumulated amortization on intangible assets
                                    then GL.beginBalance ELSE 0 END)
                    )), 0)) as BIGINT), -- FASB B05 / GASB D4
        null,
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year Begin'
	and (GL.assetCurrent = 'Y' 
        or GL.assetCapitalLand = 'Y'   
        or GL.assetCapitalInfrastructure = 'Y' 
        or GL.assetCapitalBuildings = 'Y' 
        or GL.assetCapitalEquipment = 'Y' 
        or GL.assetCapitalConstruction = 'Y' 
        or GL.assetCapitalIntangibleAsset = 'Y' 
        or GL.assetCapitalOther = 'Y' 
        or GL.assetNoncurrentOther = 'Y' 
        or GL.deferredOutflow = 'Y'
        or GL.liabCurrentLongtermDebt = 'Y' 
        or GL.liabCurrentOther = 'Y' 
        or GL.liabNoncurrentLongtermDebt = 'Y'  
        or GL.liabNoncurrentOther = 'Y'  
        or GL.deferredInflow = 'Y'  
        or GL.accumDepreciation = 'Y'  
        or GL.accumAmmortization = 'Y')
	and GL.institParentChild = 'P'
	
union

-- PART C  
-- Scholarships and Fellowships
-- Note:  FASB (Part C) & GASB (Part E) Mostly matched  

select 'C',
        27,
        '1',   -- Pell grants (federal)
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C01
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.expFAPellGrant = 'Y'

union

-- Line 02 -- Other federal grants (Do NOT include FDSL amounts)

select 'C',
        28,
        '2',   -- Other federal grants (Do NOT include FDSL amounts)
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C02
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.expFANonPellFedGrants = 'Y'
	and OL.institParentChild = OL.COASParentChild

union

select 'C',
        29,
        '3',   -- Grants by state government
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C03
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.expFAStateGrants = 'Y'
	and OL.institParentChild = OL.COASParentChild

union

select 'C',
        30,
        '4',   -- Grants by local government
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C04
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.expFALocalGrants = 'Y'
	and OL.institParentChild = OL.COASParentChild

union

select 'C',
        31,
        '5',   -- Institutional grants (restricted)
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C01
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.expFAInstitGrantsRestr = 'Y'
	and OL.institParentChild = OL.COASParentChild

union

select 'C',
        32,
        '6',   -- Institutional grants (unrestricted)
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C06
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.expFAInstitGrantsUnrestr = 'Y'
	and OL.institParentChild = OL.COASParentChild

-- PART C, Line 07 - Total Revenue   (Do Not include in Import/Export File )

union

select 'C',
        34,
        '8',   -- Discounts and Allowances applied to tuition and fees
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C08
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.discAllowTuitionFees = 'Y'
	and OL.institParentChild = OL.COASParentChild

union

 select 'C',
        35,
        '9',   -- Discounts and Allowances applied to auxiliary enterprise revenues
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.discAllowAuxEnterprise = 'Y'
	and OL.institParentChild = OL.COASParentChild

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
        CAST((NVL(ABS(ROUND(SUM(CASE when OL.revTuitionAndFees = 'Y' then OL.endBalance ELSE 0 END)
                          - SUM(CASE when OL.discAllowTuitionFees = 'Y' then OL.endBalance ELSE 0 END))), 0)) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and ((OL.revTuitionAndFees = 'Y'  
	 or OL.discAllowTuitionFees = 'Y' )
      	and OL.isUnrestrictedFASB = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        38,
        '1',   -- Tuition and fees (net of allowance reported in Part C, line 08) D01
        3,     -- isRestrictedTempFASB
        CAST((NVL(ABS(ROUND(SUM(CASE when OL.revTuitionAndFees = 'Y' then OL.endBalance ELSE 0 END)
                        - SUM(CASE when OL.discAllowTuitionFees = 'Y' then OL.endBalance ELSE 0 END))), 0)) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and ((OL.revTuitionAndFees = 'Y' 
	 or OL.discAllowTuitionFees = 'Y')
   	and OL.isRestrictedTempFASB = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        39,
        '1',    -- Tuition and fees (net of allowance reported in Part C, line 08) D01
        4,      -- isRestrictedPermFASB
        CAST((NVL(ABS(ROUND(SUM(CASE when OL.revTuitionAndFees = 'Y' then OL.endBalance ELSE 0 END)
                          - SUM(CASE when OL.discAllowTuitionFees = 'Y' then OL.endBalance ELSE 0 END))), 0)) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and ((OL.revTuitionAndFees = 'Y'
          or OL.discAllowTuitionFees = 'Y')
        and OL.isRestrictedPermFASB = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        40,
        '2',	--Federal appropriations | D02
        2, 		--isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revFedApproprations = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        41,
        '2',   -- Federal appropriations | D02
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revFedApproprations = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

-- Line 02 - Federal appropriations

select 'D',
        42,
        '2',   -- Federal appropriations | D02
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
    and OL.revFedApproprations = 'Y'
    and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        43,
        '3',   -- State appropriations  | D03
        2, --isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revStateApproprations = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        44,
        '3',   -- State appropriations  | D03
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revStateApproprations = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        45,
        '3',   -- State appropriations  | D03
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revStateApproprations = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        46,
        '4',   -- Local appropriations | D04
        2, --isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revLocalApproprations = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        47,
        '4',   -- Local appropriations | D04
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revLocalApproprations = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        48,
        '4',   -- Local appropriations | D04
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revLocalApproprations = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        49,
        '5',   -- Federal grants and contracts (Do not include FDSL) | D05
        2, --isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revFedGrantsContractsOper = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        50,
        '5',   -- Federal grants and contracts (Do not include FDSL) | D05
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revFedGrantsContractsOper = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        51,
        '5',    -- Federal grants and contracts (Do not include FDSL) | D05
        4, 		--isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revFedGrantsContractsOper = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        52,
        '6',    -- State grants and contracts | D06
        2, 		--isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revStateGrantsContractsOper = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        53,
        '6',    -- State grants and contracts | D06
        3,      -- isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revStateGrantsContractsOper = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        54,
        '6',    -- State grants and contracts | D06
        4,      -- isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revStateGrantsContractsOper = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        55,
        '7',   -- Local government grants and contracts | D07
        2, --isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revLocalGrantsContractsOper = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        56,
        '7',   -- Local government grants and contracts | D07
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revLocalGrantsContractsOper = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        57,
        '7',   -- Local government grants and contracts | D07
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revLocalGrantsContractsOper = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revPrivGifts = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union
 
select 'D',
        59,
        '8a',   -- Private gifts | D08a  -- revPrivGifts
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revPrivGifts = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        60,
        '8a',   -- Private gifts | D08a  -- revPrivGifts
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revPrivGifts = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        61,
        '8b',   -- Private grants & Contracts | D08b revPrivGrantsContractsOper
        2, --isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revPrivGrantsContractsOper = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        62,
        '8b',   -- Private grants & Contracts | D08b revPrivGrantsContractsOper
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revPrivGrantsContractsOper = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        63,
        '8b',   -- Private grants & Contracts | D08b revPrivGrantsContractsOper
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revPrivGrantsContractsOper = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        64,
        '9',   -- Contributions from affiliated entities | D09 revAffiliatedOrgnGifts
        2, --isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revAffiliatedOrgnGifts = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        65,
        '9',   -- Contributions from affiliated entities | D09 revAffiliatedOrgnGifts
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revAffiliatedOrgnGifts = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        66,
        '9',   -- Contributions from affiliated entities | D09 revAffiliatedOrgnGifts
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revAffiliatedOrgnGifts = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

-- Other Revenue

union

select 'D',
        67,
        '10',   -- Investment return | D10  revInvestmentIncome
        2, --isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revInvestmentIncome = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        68,
        '10',   -- Investment return | D10  revInvestmentIncome
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revInvestmentIncome = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        69,
        '10',   -- Investment return | D10  revInvestmentIncome
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revInvestmentIncome = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        70,
        '11',   -- Sales and services of educational activities | D11 revEducActivSalesServices
        2, --isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revEducActivSalesServices = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        73,
        '12' ,   -- Sales and services of auxilliary enterprises | D12
        2, --isUnrestrictedFASB
        CAST((NVL(ABS(ROUND(SUM(CASE when OL.revAuxEnterprSalesServices = 'Y' then OL.endBalance ELSE 0 END)
                            - SUM(CASE when OL.discAllowAuxEnterprise = 'Y' then OL.endBalance ELSE 0 END))), 0)) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and ((OL.revAuxEnterprSalesServices = 'Y'
      		or OL.discAllowAuxEnterprise = 'Y')
		and OL.isUnrestrictedFASB = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        76,
        '13',   -- Hospital revenue | D13
        2, --isUnrestrictedFASB
        CAST((NVL(ABS(ROUND(SUM(CASE when OL.revHospitalSalesServices = 'Y' then OL.endBalance ELSE 0 END)
                          - SUM(CASE when OL.discAllowPatientContract = 'Y' then OL.endBalance ELSE 0 END))), 0)) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and ((OL.revHospitalSalesServices = 'Y'
	   		or OL.discAllowPatientContract = 'Y')
		and OL.isUnrestrictedFASB = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        79,
        '14',   -- Independent operations revenue | D14 revIndependentOperations
        2, --isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revIndependentOperations = 'Y'
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        80,
        '14',   -- Independent operations revenue | D14 revIndependentOperations
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revIndependentOperations = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        81,
        '14',   -- Independent operations revenue | D14 revIndependentOperations
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revIndependentOperations = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Revenue'
		and (OL.revRealizedCapitalGains != 'Y'
			and OL.revRealizedOtherGains != 'Y'
			and OL.revExtraordGains != 'Y'
			and OL.revOwnerEquityAdjustment != 'Y'
			and OL.revSumOfChangesAdjustment != 'Y'))
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        82,
        '16',    -- Total revenues and investment return | D16
        2, --isUnrestrictedFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Revenue'
	  	and (OL.revRealizedCapitalGains != 'Y'
	  		and OL.revRealizedOtherGains != 'Y'
	  		and OL.revExtraordGains != 'Y'
	  		and OL.revOwnerEquityAdjustment != 'Y'
	  		and OL.revSumOfChangesAdjustment != 'Y'))
	and OL.isUnrestrictedFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        83,
        '16',    -- Total revenues and investment return | D16
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Revenue'
	  	and (OL.revRealizedCapitalGains != 'Y'
        	and OL.revRealizedOtherGains != 'Y'
        	and OL.revExtraordGains != 'Y'
        	and OL.revOwnerEquityAdjustment != 'Y'
        	and OL.revSumOfChangesAdjustment != 'Y'))
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        84,
        '16',    -- Total revenues and investment return | D16
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Revenue'
		and (OL.revRealizedCapitalGains != 'Y'
	  		and OL.revRealizedOtherGains != 'Y'
	  		and OL.revExtraordGains != 'Y'
	  		and OL.revOwnerEquityAdjustment != 'Y'
	  		and OL.revSumOfChangesAdjustment != 'Y'))
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

-- Line 17 - Net assets released from restriction - 
/*  Enter all revenues resulting from the reclassification of temporarily restricted assets
    or permanently restricted assets. 
*/

select 'D',
        86,
        '17',    --    | D17  revReleasedAssets
        3, --isRestrictedTempFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revReleasedAssets = 'Y'
	and OL.isRestrictedTempFASB = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'D',
        87,
        '17',    --    | D17  revReleasedAssets
        4, --isRestrictedPermFASB
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.revReleasedAssets = 'Y'
	and OL.isRestrictedPermFASB = 1
	and OL.institParentChild = OL.COASParentChild

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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --   E1,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.accountType = 'Expense'
	and OL.isInstruction = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        89,
        '1',   -- Instruction | E1,1
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --   E1,2
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.accountType = 'Expense' 
	and OL.expSalariesWages = 'Y'  
	and OL.isInstruction = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        90,
        '2',   -- Research | E1,2
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --  E2,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.accountType = 'Expense' 
	and OL.isResearch = 1
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        91,
        '2',   -- Research | E1,2
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E2,2
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
    	and OL.expSalariesWages = 'Y' 
    	and OL.isResearch = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        92,
        '3',   -- Public service | E1,3
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E3,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
    	and OL.isPublicService = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        93,
        '3',   -- Public service | E1,3
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E3,2
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
    	and OL.expSalariesWages = 'Y' 
    	and OL.isPublicService = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        94,
        '4',   -- Academic support | E1,4
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E4,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
    	and OL.isAcademicSupport = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        95,
        '4',   -- Academic support | E1,4
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E4,2
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
        and OL.expSalariesWages = 'Y' 
        and OL.isAcademicSupport = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        96,
        '5',   -- Student services | E1,5
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E5,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
		and OL.isStudentServices = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        97,
        '5',   -- Student services | E1,5
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E5,2
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
        and OL.expSalariesWages = 'Y' 
        and OL.isStudentServices = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        98,
        '6',   -- Institutional support | E1,6
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E6,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
        and OL.isInstitutionalSupport = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        99,
        '6',   -- Institutional support | E1,6
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E6,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
        and OL.isInstitutionalSupport = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        100,
        '7',   -- Auxiliary enterprises | E1,7
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E7,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
        and OL.isAuxiliaryEnterprises  = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        101,
        '7',   -- Auxiliary enterprises | E1,7
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E7,2
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
        and OL.expSalariesWages = 'Y' 
        and OL.isAuxiliaryEnterprises = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        102,
        '8',   -- Net grant aid to students | E1,8
        1,
        CAST((NVL(ABS(ROUND(SUM(CASE when (OL.expFAPellGrant = 'Y'           --Pell grants
                                        or OL.expFANonPellFedGrants = 'Y'    --Other federal grants
                                        or OL.expFAStateGrants = 'Y'         --Grants by state government
                                        or OL.expFALocalGrants = 'Y'         --Grants by local government
                                        or OL.expFAInstitGrantsRestr = 'Y'   --Institutional grants from restricted resources
                                        or OL.expFAInstitGrantsUnrestr = 'Y' --Institutional grants from unrestricted resources
                                    ) then OL.endBalance ELSE 0 END)
                    - SUM(CASE when (OL.discAllowTuitionFees = 'Y'   --Discounts and allowances applied to tuition and fees
                                  or OL.discAllowAuxEnterprise = 'Y' --Discounts and allowances applied to sales and services of auxiliary enterprises
                                    ) then OL.endBalance ELSE 0 END))), 0)) as BIGINT),
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and ((OL.expFAPellGrant = 'Y' 
       or OL.expFANonPellFedGrants = 'Y' 
       or OL.expFAStateGrants = 'Y' 
       or OL.expFALocalGrants = 'Y' 
       or OL.expFAInstitGrantsRestr = 'Y' 
       or OL.expFAInstitGrantsUnrestr = 'Y')
	 or (OL.discAllowTuitionFees = 'Y'
       or OL.discAllowAuxEnterprise = 'Y'))
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        103,
        '9',   -- Hospital Services | E1,9
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E9,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
		and OL.isHospitalServices  = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        104,
        '9',   -- Hospital Services | E1,9
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E9,2
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
    	and OL.expSalariesWages = 'Y' 
    	and OL.isHospitalServices = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        105,
        '10',   -- Independent operations | E-10
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E10,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
     	and OL.isIndependentOperations = 1)
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        106,
        '10',   -- Independent operations | E-10
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E10,2
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
    	and OL.expSalariesWages = 'Y' 
    	and OL.isIndependentOperations = 1)
	and OL.institParentChild = OL.COASParentChild

-- Part E,  Line E12   Calculated CV = [E13-(E01+...+E10)]
-- (Do not include in import file. Will be calculated )

union

select 'E',
        107,
        '13',   -- Total expenses and Deductions | E-13
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E13,1
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
        and (expFedIncomeTax != 'Y'
           	and expExtraordLosses != 'Y'))
	and OL.institParentChild = OL.COASParentChild

union

select 'E',
        108,
        '13',   -- Total expenses and Deductions | E-13
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E13,2
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense' 
      	and OL.expSalariesWages = 'Y')
	and OL.institParentChild = OL.COASParentChild

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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E13-3 Benefits
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.expBenefits = 'Y'
	and OL.institParentChild = OL.COASParentChild

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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E13-4,
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and (OL.expCapitalConstruction = 'Y' 
      	or OL.expCapitalEquipPurch = 'Y' 
      	or OL.expCapitalLandPurchOther = 'Y')
	and OL.institParentChild = OL.COASParentChild

union

-- Line E13-5 Depreciation
-- Enter the total amount of depreciation incurred.

select 'E',
        111,
        '13',   --  E13-5 Depreciation
        5,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E13-5 Depreciation
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.expDepreciation = 'Y'
	and OL.institParentChild = OL.COASParentChild

union

-- Line E13-6 Interest
-- Enter in the total amount of interest incurred on debt.

select 'E',
        112,
        '13',   --  E13-6 Interest
        6,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E13-6 Interest
        null,
        null,
        null,
        null
from OL OL
where OL.fiscalPeriod = 'Year End'
	and OL.expInterest = 'Y'
	and OL.institParentChild = OL.COASParentChild

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
        CAST(case when (select config.finEndowmentAssets
						from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(GL.beginBalance))), 0) 
			 else 0
		end as BIGINT),
        null,
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year Begin'
	and GL.accountType = 'Asset' 
    and GL.isEndowment = 1
	and GL.institParentChild = GL.COASParentChild

union

select 'H',
        114,        
        '2', --Value of endowment assets at the END of the fiscal year
        CAST(case when (select config.finEndowmentAssets
						from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(GL.endBalance))), 0) 
			 else 0
		end as BIGINT),
        null,
        null,
        null,
        null,
        null
from GL GL
where GL.fiscalPeriod = 'Year End'
	and (GL.accountType = 'Asset' 
        and GL.isEndowment = 1)
	and GL.institParentChild = GL.COASParentChild
        
-- order by 2 
