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
20200412			jhanicak			jh 20204012		 Added dummy date option for recordActivityDate in most current record queries PF-1374
														 Added DefaultValues query and rewrote other queries to use PF-1418
														 Removed all 'prior' queries - not needed
20200303            jd.hysler           jdh 2020-03-04   PF-1297 Modify Finance all versions to use latest records from finance entities 
                                                         - Removed FYPerAsOfDate/FYPerPriorAsOfDate
                                                         - Removed BothCOASPerFYAsOfDate/BothCOASPerFYPriorAsOfDate
                                                         - Move IPEDSClientConfig values into COASPerFYAsOfDate/COASPerFYPriorAsOfDate
                                                         - Added GL cte for most recent record views for GeneralLedgerReporting
                                                         ? Added GL_Prior cte for most recent record views for GeneralLedgerReporting for prior fiscal year
                                                         - Added OL cte for most recent record views for OperatingLedgerReporting
                                                         ? Added OL_Prior cte for most recent record views for OperatingLedgerReporting for prior fiscal year
                                                         - Removed cross join with ConfigPerAsOfDate since values are now already in COASPerFYAsOfDate
                                                         - Changed PART Queries to use new in-line GL/OL Structure
                                                                --  1-Changed GeneralLedgerReporting to GL inline view  
                                                                --  2-Removed the join to COAS       
                                                                --  3-Removed GL.isIpedsReportable = 1 
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
	   repValues.finBusinessStructure finBusinessStructure,
	   repValues.finTaxExpensePaid finTaxExpensePaid,
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
			defaultValues.finBusinessStructure finBusinessStructure,
			defaultValues.finTaxExpensePaid finTaxExpensePaid,
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
		repValues.finBusinessStructure,
		repValues.finTaxExpensePaid,
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
	defaultValues.finBusinessStructure finBusinessStructure,
	defaultValues.finTaxExpensePaid finTaxExpensePaid,
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
    select ReportingDates.surveyYear surveyCollectionYear,
	            ReportingDates.asOfDate asOfDate,
                ReportingDates.priorAsOfDate priorAsOfDate,
                nvl(config.finGPFSAuditOpinion, ReportingDates.finGPFSAuditOpinion) finGPFSAuditOpinion,
                nvl(config.finAthleticExpenses, ReportingDates.finAthleticExpenses) finAthleticExpenses,
                nvl(config.finEndowmentAssets, ReportingDates.finEndowmentAssets) finEndowmentAssets,
                nvl(config.finPensionBenefits, ReportingDates.finPensionBenefits) finPensionBenefits,
                nvl(config.finPellTransactions, ReportingDates.finPellTransactions) finPellTransactions,
                nvl(config.finBusinessStructure, ReportingDates.finBusinessStructure) finBusinessStructure,
                nvl(config.finTaxExpensePaid, ReportingDates.finTaxExpensePaid) finTaxExpensePaid,
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
	  ReportingDates.finBusinessStructure finBusinessStructure,
	  ReportingDates.finTaxExpensePaid finTaxExpensePaid,
	  ReportingDates.finParentOrChildInstitution finParentOrChildInstitution
from ReportingDates ReportingDates
where ReportingDates.surveyYear not in (select config.surveyCollectionYear
                        from IPEDSClientConfig config
		                	inner join ReportingDates ReportingDates on ReportingDates.surveyYear = config.surveyCollectionYear)
),

 --jdh 2020-03-04 Removed FYPerAsOfDate/FYPerPriorAsOfDate

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

--jdh 2020-03-04 Modified COASPerFYAsOfDate to include IPEDSClientConfig values
--and most recent records based on COASPerAsOfDate and FiscalYear

--jh 20200412 Added DefaultValues query and rewrote other queries to use PF-1418

COASPerFYAsOfDate AS (
select FYData.asOfDate asOfDate,
        FYData.priorAsOfDate priorAsOfDate,
        CASE FYData.finGPFSAuditOpinion
             WHEN 'Q' THEN 1
             WHEN 'UQ' THEN 2
             WHEN 'U' THEN 3 END finGPFSAuditOpinion, --1=Unqualified, 2=Qualified, 3=Don't know
        CASE FYData.finPellTransactions
             WHEN 'P' THEN 1
             WHEN 'F' THEN 2
             WHEN 'N' THEN 3 END finPellTransactions, --P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants 
        CASE FYData.finBusinessStructure
             WHEN 'SP' THEN 1
             WHEN 'P' THEN 2
             WHEN 'C' THEN 3 
             WHEN 'SC' THEN 4 
             WHEN 'LLC' THEN 5 END finBusinessStructure,  --SP = Sole Proprietorship, P = Partnership, C = C Corp, SC = S Corp, LLC = LLC
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
				config.finPellTransactions finPellTransactions,				                
                config.finBusinessStructure finBusinessStructure,
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

 
--jdh 2020-03-04 Added GL most recent record views for GeneralLedgerReporting
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

--jdh 2020-03-04 Added OL cte for most recent record views for OperatingLedgerReporting

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
--jh 20200226 Removed join of ConfigPerAsOfDate since config values are in COASPerFYAsOfDate

select DISTINCT '9' part,
                 0 sort,
                 CAST(MONTH(nvl(COAS.startDate, COAS.priorAsOfDate))  as BIGINT) field1,
                 CAST(YEAR(nvl(COAS.startDate, COAS.priorAsOfDate)) as BIGINT) field2,
                 CAST(MONTH(nvl(COAS.endDate, COAS.asOfDate)) as BIGINT) field3,
                 CAST(YEAR(nvl(COAS.endDate, COAS.asOfDate)) as BIGINT) field4,
                 COAS.finGPFSAuditOpinion  field5, --1=Unqualified, 2=Qualified, 3=Don't know
                  COAS.finPellTransactions  field6, --P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants 
                  COAS.finBusinessStructure field7  --SP = Sole Proprietorship, P = Partnership, C = C Corp, SC = S Corp, LLC = LLC 
from COASPerFYAsOfDate COAS 

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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
    and OL.institParentChild = 'P'
	and OL.expFedIncomeTax  = 'Y'
	
union 

select 'F',  
        2, 
        '2', -- State and Local - Report the SUM of the current State and Local tax expense (or benefit) and deferred State and Local tax expense (or benefit).
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- F2,1
        NULL,   -- F2,2
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
    and OL.institParentChild = 'P'
 	  and OL.expStateLocalIncomeTax = 'Y'
 
union

select 'F' ,  
        3 , 
        '3', -- Line 3 , --03 - Tax Structure designate who paid the reported tax expenses for your institution
        case when config.finTaxExpensePaid = 'M' then 1
			when config.finTaxExpensePaid = 'N' then 2
			when config.finTaxExpensePaid = 'R' then 3
		end, --finTaxExpensePaidFASB
        NULL,
        NULL,
        NULL,      
        NULL, 
        NULL 
--jh 20200412 Removed OL inline view and removed subquery in select field by moving ConfigPerAsOfDate to from
from ConfigPerAsOfDate config

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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,      
        NULL,
        NULL  
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and (GL.assetCurrent = 'Y' 
      or GL.assetCapitalLand = 'Y'
      or GL.assetCapitalInfrastructure = 'Y'
      or GL.assetCapitalBuildings = 'Y'
      or GL.assetCapitalEquipment = 'Y'
      or GL.assetCapitalConstruction = 'Y'
      or GL.assetCapitalIntangibleAsset = 'Y'
      or GL.assetCapitalOther = 'Y'
      or GL.assetNoncurrentOther = 'Y'
      or GL.deferredOutflow = 'Y')

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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,      
        NULL,
        NULL    
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.assetNoncurrentOther = 'Y'

union 

select 'A',  
        6, 
        '1', --01b - Property, plant, and equipment, net of accumulated depreciation
        'b',
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (GL.assetCapitalLand = 'Y'
                                      or GL.assetCapitalInfrastructure = 'Y'
                                      or GL.assetCapitalBuildings = 'Y'
                                      or GL.assetCapitalEquipment = 'Y'
                                      or GL.assetCapitalConstruction = 'Y'
                                      or GL.assetCapitalIntangibleAsset = 'Y'
                                      or GL.assetCapitalOther = 'Y') THEN GL.endBalance ELSE 0 END) 
                        - SUM(CASE WHEN GL.accumDepreciation = 'Y' THEN GL.endBalance ELSE 0 END))), 0)as BIGINT), 
        NULL,
        NULL,      
        NULL,
        NULL   
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
and (GL.assetCapitalLand = 'Y'
  or GL.assetCapitalInfrastructure = 'Y'
  or GL.assetCapitalBuildings = 'Y'
  or GL.assetCapitalEquipment = 'Y'
  or GL.assetCapitalConstruction = 'Y'
  or GL.assetCapitalIntangibleAsset = 'Y'
  or GL.assetCapitalOther = 'Y'
  or GL.accumDepreciation = 'Y')

union 

select 'A',  
        7, 
        '1', --01c - Intangible assets, net of accumulated amortization
        'c',
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN GL.assetCapitalIntangibleAsset = 'Y' THEN GL.endBalance ELSE 0 END)
                        - SUM(CASE WHEN GL.accumAmmortization = 'Y' THEN GL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL,
        NULL,      
        NULL,
        NULL    
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and (GL.assetCapitalIntangibleAsset = 'Y'
      or GL.accumAmmortization = 'Y')

union 

select 'A',  
       8, 
       '2', --02  - Total liabilities 
       NULL,
       CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
       NULL,
       NULL,      
       NULL,
       NULL    
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and (GL.liabCurrentLongtermDebt = 'Y'
        or GL.liabCurrentOther = 'Y'
        or GL.liabNoncurrentLongtermDebt = 'Y'   
        or GL.liabNoncurrentOther = 'Y'
        or GL.deferredInflow = 'Y')

union 

select 'A',  
        8, 
        '2', --02a - Debt related to property, plant, and equipment (long-term debt)
        'a',
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,      
        NULL,
        NULL    
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and (GL.liabCurrentLongtermDebt = 'Y'
      or GL.liabNoncurrentLongtermDebt = 'Y')

union

select 'A',  
        12, 
        '5', --05 - Land and land improvements
        NULL,
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), 
        NULL,
        NULL,      
        NULL,
        NULL    
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.assetCapitalLand = 'Y'

union 

select 'A',  
        13, 
        '6', -- 06 - Buildings
        NULL,
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), 
        NULL,
        NULL,      
        NULL,
        NULL    
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.assetCapitalBuildings = 'Y'

union

select 'A',  
        14, 
        '7', --07 - Equipment, including art and library collections
        NULL,
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), 
        NULL,
        NULL,      
        NULL,
        NULL   
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.assetCapitalEquipment = 'Y'

union

select 'A',  
        15, 
        '8', --08 - Construction in Progress
        NULL,
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), 
        NULL,
        NULL,      
        NULL,
        NULL    
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.assetCapitalConstruction = 'Y'

union

-- Line 09 - Other - 
-- Report all other amounts for capital assets not reported in lines 05-08.

select 'A',  
        16, 
        '9', --09 - Other
        NULL,
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), 
        NULL,
        NULL,      
        NULL,
        NULL    
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and (GL.assetCapitalOther = 'Y' 
      or GL.assetCapitalInfrastructure = 'Y')

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
		CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), 
		NULL,
		NULL,      
		NULL,
		NULL    
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
	and GL.accumDepreciation = 'Y'

union

-- Line 12 - Property, Plant, and Equipment, net of accumulated depreciation (from A1b) 
/* Note:
   the spec says this value is taken from A1b, but still requests that A12 be reported
*/

select 'A',  
        18, 
        '12', --12 - Property, Plant, and Equipment, net of accumulated depreciation (from A1b) 
        NULL,
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (GL.assetCapitalLand = 'Y'
                                  or GL.assetCapitalInfrastructure = 'Y'
                                  or GL.assetCapitalBuildings = 'Y'
                                  or GL.assetCapitalEquipment = 'Y'
                                  or GL.assetCapitalConstruction = 'Y'
                                  or GL.assetCapitalIntangibleAsset = 'Y'
                                  or GL.assetCapitalOther = 'Y') THEN GL.endBalance ELSE 0 END) 
                          - SUM(CASE WHEN GL.accumDepreciation = 'Y' THEN GL.endBalance ELSE 0 END))), 0)as BIGINT),
        NULL,
        NULL,      
        NULL,
        NULL    
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
and (GL.assetCapitalLand = 'Y'
  or GL.assetCapitalInfrastructure = 'Y'
  or GL.assetCapitalBuildings = 'Y'
  or GL.assetCapitalEquipment = 'Y'
  or GL.assetCapitalConstruction = 'Y'
  or GL.assetCapitalIntangibleAsset = 'Y'
  or GL.assetCapitalOther = 'Y'
  or GL.accumDepreciation = 'Y')

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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance ))), 0) as BIGINT), -- B01	Total Revenue
        NULL, 
        NULL, 
        NULL, 
        NULL,
        NULL   
from OL OL   
where OL.fiscalPeriod = 'Year End'
    and OL.institParentChild = 'P'
    and  OL.accountType = 'Revenue'
    and (OL.revRealizedCapitalGains != 'Y' 
	    and OL.revRealizedOtherGains != 'Y' 
	    and OL.revExtraordGains != 'Y')

union 

-- Line 02 - Total expenses - 
/* Enter all expenses that agree with the expenses recognized in your GPFS. 
   The amount provided here is important because it will be carried forward to Part E.
*/ 
 
select 'B', 
      21, 
      '2',   --  Total expenses 
      CAST(NVL(ABS(ROUND(SUM( OL.endBalance))), 0) as BIGINT), -- B02
      NULL, 
      NULL,
      NULL, 
      NULL,
      NULL   
from OL OL   
where OL.fiscalPeriod = 'Year End'
    and OL.institParentChild = 'P'
	and ((OL.accountType = 'Expense' 
     	and (OL.expFedIncomeTax != 'Y' 
     	and OL.expExtraordLosses != 'Y')))

union

--revisit 

select 'B',  
        23 , 
        '4', -- Line 4 , --04 - Net income					
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.accountType = 'Revenue' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT) 
          - CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.accountType = 'Expense' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), -- FASB B04 / GASB D03
        NULL, 
        NULL, 
        NULL, 
        NULL,
        NULL   
from OL OL   
where OL.fiscalPeriod = 'Year End'
    and OL.institParentChild = 'P'
	and (OL.accountType = 'Revenue'
	  or OL.accountType = 'Expense')
 
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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), 
        NULL, 
        NULL, 
        NULL, 
        NULL,
        NULL    
from OL OL   
where OL.fiscalPeriod = 'Year End'
    and OL.institParentChild = 'P'
    and (OL.revOwnerEquityAdjustment = 'Y'
      	or OL.expOwnerEquityAdjustment = 'Y' 
	  	or OL.revUnrealizedGains = 'Y' )

union 

select 'B', 
        25, 
        '6', --  06 - Equity, beginning of year   
        (CAST(NVL(ABS(ROUND(SUM(CASE WHEN (GL.assetCurrent = 'Y'           --A1 Current Assets
                                    or GL.assetCapitalLand = 'Y'     --A14 Net assets invested in capital assets
                                    or GL.assetCapitalInfrastructure = 'Y'
                                    or GL.assetCapitalBuildings = 'Y'
                                    or GL.assetCapitalEquipment = 'Y'
                                    or GL.assetCapitalConstruction = 'Y'
                                    or GL.assetCapitalIntangibleAsset = 'Y'
                                    or GL.assetCapitalOther = 'Y'
                                    or GL.assetNoncurrentOther = 'Y'   --A5 Noncurrent Assets
                                    or GL.deferredOutflow = 'Y')       --A19 Deferred Outflow
                                    THEN GL.beginBalance ELSE 0 END))), 0) as BIGINT))
         - (CAST(NVL(ABS(ROUND(SUM(CASE WHEN (GL.liabCurrentLongtermDebt = 'Y'
                                    or GL.liabCurrentOther = 'Y'      --A9 Current Liabilities
                                    or GL.liabNoncurrentLongtermDebt = 'Y'
                                    or GL.liabNoncurrentOther = 'Y'   --A12 Noncurrent Liabilities
                                    or GL.deferredInflow = 'Y'        --A20 Deferred Inflow
                                    or GL.accumDepreciation = 'Y'     --A31 Depreciation on capital assets
                                    or GL.accumAmmortization = 'Y')   --P33 Accumulated amortization on intangible assets
                                    THEN GL.beginBalance ELSE 0 END)  )), 0) as BIGINT)), -- FASB B05 / GASB D4
        NULL, 
        NULL, 
        NULL, 
        NULL,
        NULL   
from GL GL   
where GL.fiscalPeriod = 'Year Begin'
    and GL.institParentChild = 'P'
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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C01					   
        NULL,  
        NULL, 
        NULL,
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
   and OL.institParentChild = OL.COASParentChild
   and OL.expFAPellGrant = 'Y'

union

select 'C',  
        28, 
        '2',   -- Other federal grants (Do NOT include FDSL amounts)
        NULL, 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT ), -- FASB C02				   
        NULL, 
        NULL,
        NULL,  
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
   and OL.institParentChild = OL.COASParentChild
   and OL.expFANonPellFedGrants = 'Y'
 
union
 
select 'C',  
        29, 
        '3',   -- Grants by state government
        'a', 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C03a				   
        NULL, 
        NULL,
        NULL,  
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.expFAStateGrants = 'Y'

union 
 
select 'C',  
        30, 
        '3',   -- Grants by local government
        'b', 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C04					   
        NULL, 
        NULL,
        NULL,  
        NULL 
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.expFALocalGrants = 'Y'
  
union 

select 'C',  
        31, 
        '4',   -- Institutional grants 
        NULL, 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C01				   
        NULL, 
        NULL,
        NULL,  
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and (OL.expFAInstitGrantsRestr = 'Y'
	  	or OL.expFAInstitGrantsUnrestr = 'Y')

/* Line 05: Total revenue that funds scholarship and fellowships
   (Calculated (Do not include in import file )
   C,5 = CV=[C01+...+C04]
*/ 

union

select 'C',  
        34, 
        '6',   -- Discounts and Allowances applied to tuition and fees
        NULL, 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- FASB C08					   
        NULL, 
        NULL,
        NULL,  
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.discAllowTuitionFees = 'Y'

union
  
select 'C',  
        35, 
        '7',   -- Discounts and Allowances applied to auxiliary enterprise revenues 
        NULL, 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), 				   
        NULL, 
        NULL,
        NULL,  
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
  and OL.discAllowAuxEnterprise = 'Y'
 
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
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.revTuitionandFees = 'Y' THEN OL.endBalance ELSE 0 END) 
                        - SUM(CASE WHEN OL.discAllowTuitionFees = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and (OL.revTuitionandFees = 'Y' 
		or  OL.discAllowTuitionFees = 'Y')
 
union 
 
-- Government Appropriations, Grants and Contracts 
-- Line 02a - Federal appropriations

select 'D', 
        38, 
        '2', --  Federal appropriations
        'a',
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.revFedApproprations = 'Y'
            
union 

-- Line 2b - Federal grants and contracts (Do not include FDSL)

select 'D', 
        39, 
        '2', --  Federal grants and contracts (Do not include FDSL)
        'b',
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
    and OL.revFedGrantsContractsOper = 'Y'

union  		

-- Line 3a - State appropriations

select 'D', 
        40, 
        '3', --  State appropriations
        'a', 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.revStateApproprations = 'Y'   

union 

-- Line 3b  State grants and contracts

select 'D', 
        41, 
        '3', --  State grants and contracts
        'b',
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.revStateGrantsContractsOper = 'Y'

union 

-- Line 3c  Local government appropriations

select 'D', 
        42  , 
        '3' , --  Local government appropriations
        'c', 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.revLocalApproprations = 'Y'

union 

-- Line 3d - Local government grants and contracts

select 'D', 
        43, 
        '3', --  Local government grants and contracts
        'd', 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
          NULL, 
          NULL,
          NULL,
          NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.revLocalGrantsContractsOper = 'Y'

union  

 -- Line 4 - Private gifts,  grants and contracts

select 'D', 
        44, 
        '4', --  Private gifts,  grants and contracts
        NULL, 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and (OL.revPrivGifts = 'Y' 
	  or OL.revPrivGrantsContractsOper = 'Y' 
	  or OL.revPrivGrantsContractsNOper = 'Y')

union  

-- Line 5 - Investment income and investment gains (losses) included in net income

select 'D',  
        45, 
        '5', --  Investment income and investment gains (losses) included in net income
        NULL, 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.revInvestmentIncome = 'Y'

union 

select 'D',
        46, 
        '6', -- Line 6 ,   --  Sales and services of educational activities
        NULL, 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
  and OL.revEducActivSalesServices = 'Y'
	
union  

select 'D',
        47 , 
        '7', -- Line 7 ,   --  Sales and services of auxiliary enterprises (net of amount reported in Part C, line 07)
        NULL, 
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.revAuxEnterprSalesServices = 'Y' THEN OL.endBalance ELSE 0 END) 
                         - SUM(CASE WHEN OL.discAllowAuxEnterprise = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and (OL.revAuxEnterprSalesServices = 'Y'
	  or OL.discAllowAuxEnterprise = 'Y')

union 
 
select 'D', 
        48, 
        '12', -- Line 12 ,  --  Hospital revenue
        NULL, 
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.revHospitalSalesServices = 'Y' THEN OL.endBalance ELSE 0 END) 
                        - SUM(CASE WHEN OL.discAllowPatientContract = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and (OL.revHospitalSalesServices = 'Y'
	  	or OL.discAllowPatientContract = 'Y')
 
/* Line 08  - Other Revenue  CV=[D09-(D01+...+D07+D12)]
   Calculated (Do not include in import file )  
*/ 
   
union 

-- Line 09 - Total revenues and investment return
 
select 'D',
        50, 
        '9',  --  Total revenues and investment return
        NULL, 
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL, 
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and (OL.accountType = 'Revenue'
   		and (OL.revRealizedCapitalGains != 'Y' 
      		and OL.revRealizedOtherGains != 'Y' 
      		and OL.revExtraordGains != 'Y'
      		and OL.revOwnerEquityAdjustment != 'Y' 
      		and OL.revSumOfChangesAdjustment != 'Y'))
 
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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --   E1,1
        NULL, 
        NULL, 
        NULL, 
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.isInstruction = 1

union 

select 'E',
        54, 
        '1',   -- Instruction | E1,1 
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --   E1,2
        NULL, 
        NULL, 
        NULL, 
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.expSalariesWages = 'Y'  
	and OL.isInstruction = 1

union

select 'E',
        55, 
        '2a',   -- Research | E1,2a  
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --  E2,1
        NULL, 
        NULL, 
        NULL, 
        NULL 
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.isResearch = 1

union

select 'E',
        56, 
        '2a',   -- Research | E1,2a   
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E2,2
        NULL, 
        NULL, 
        NULL, 
        NULL 
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.expSalariesWages = 'Y' 
	and OL.isResearch = 1 

union 

select 'E',
        57, 
        '2b',   -- Public service | E1,2b 
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E3,1
        NULL, 
        NULL, 
        NULL, 
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.isPublicService = 1

union 

select 'E',
        58, 
        '2b',   -- Public service | E1,2b 
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E3,2
        NULL, 
        NULL, 
        NULL, 
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.expSalariesWages = 'Y' 
	and OL.isPublicService = 1

union 

select 'E',
        59, 
        '3a',   -- Academic support | E1,3a 
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E4,1
        NULL, 
        NULL, 
        NULL, 
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.isAcademicSupport = 1
 
union 

select 'E',
        60, 
        '3a',   -- Academic support | E1,3a 
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E4,2
        NULL, 
        NULL, 
        NULL, 
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.expSalariesWages = 'Y' 
	and OL.isAcademicSupport = 1

union 

select 'E',
        61, 
        '3b',   -- Student services | E1,3b 
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E5,1
        NULL, 
        NULL, 
        NULL, 
        NULL 
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.isStudentServices = 1
	
union 

select 'E',
        62, 
        '3b',   -- Student services | E1,3b 
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E5,2
        NULL, 
        NULL, 
        NULL, 
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.expSalariesWages = 'Y' 
	and OL.isStudentServices = 1
	
union 

select 'E',
        63, 
        '3c',   -- Institutional support | E3c,6 
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E6,1
        NULL, 
        NULL, 
        NULL, 
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.isInstitutionalSupport = 1
	
union 

select 'E',
        64, 
        '3c',   -- Institutional support | E3c,6 
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E6,1
        NULL, 
        NULL, 
        NULL, 
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.expSalariesWages = 'Y' 
	and OL.isInstitutionalSupport = 1
	
union 

select 'E', 
        100, 
        '4',   -- Auxiliary enterprises | E1,4 
        1,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --E7,1
        NULL, 
        NULL, 
        NULL, 
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
    and OL.accountType = 'Expense' 
    and OL.isAuxiliaryEnterprises  = 1

union 

select 'E', 
        101, 
        '4',   -- Auxiliary enterprises | E1,4 
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E7,2
        NULL, 
        NULL, 
        NULL, 
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.expSalariesWages = 'Y' 
	and OL.isAuxiliaryEnterprises = 1
 
union

select 'E', 
        102, 
        '5',   -- Net grant aid to students | E1,5  
        1,
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.expFAPellGrant = 'Y'          --Pell grants
                                      or OL.expFANonPellFedGrants = 'Y'    --Other federal grants
                                      or OL.expFAStateGrants = 'Y'         --Grants by state government
                                      or OL.expFALocalGrants = 'Y'         --Grants by local government
                                      or OL.expFAInstitGrantsRestr = 'Y'   --Institutional grants from restricted resources
                                      or OL.expFAInstitGrantsUnrestr = 'Y' --Institutional grants from unrestricted resources
                                    ) THEN OL.endBalance ELSE 0 END)
            -             SUM(CASE WHEN (OL.discAllowTuitionFees = 'Y'      --Discounts and allowances applied to tuition and fees
                                      or OL.discAllowAuxEnterprise = 'Y'    --Discounts and allowances applied to sales and services of auxiliary enterprises
                                   ) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), 
        NULL, 
        NULL, 
        NULL, 
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and (OL.expFAPellGrant = 'Y'            
       	or OL.expFANonPellFedGrants = 'Y'  
       	or OL.expFAStateGrants = 'Y'       
       	or OL.expFALocalGrants = 'Y'    
       	or OL.expFAInstitGrantsRestr = 'Y'   
       	or OL.expFAInstitGrantsUnrestr = 'Y'   
	    or OL.discAllowTuitionFees = 'Y'  
       	or OL.discAllowAuxEnterprise = 'Y')

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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E9,1
        NULL, 
        NULL,
        NULL,
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.isHospitalServices  = 1 
 
union

select 'E', 
        104, 
        '10',   -- Hospital Services | E1,10 
        2,
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E9,2
        NULL, 
        NULL, 
        NULL, 
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
    and OL.accountType = 'Expense' 
    and OL.expSalariesWages = 'Y' 
    and OL.isHospitalServices = 1

-- Line 06 - Other Expenses and Deduction  CV=[E07-(E01+...+E10)]
-- Calculated (Do not include in import file )  
	
union 

-- Line 07 - Column 1 - Total

select 'E', 
        107, 
        '7',   -- Total expenses and Deductions | E-7
        1,     -- Column 1 - Total
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E13,1 
        NULL,
        NULL,
        NULL,
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and (expFedIncomeTax != 'Y'
    	and expExtraordLosses != 'Y' )
 
union

-- Line 07 - Column 2 - Salaries and wages

select 'E', 
        108, 
        '7',   -- Total expenses and Deductions | E-7 
        2,	   -- Column 2 - Salaries and wages
        CAST(NVL(ABS(ROUND(SUM( OL.endBalance ))), 0) as BIGINT), -- E13,2
        NULL,
        NULL,
        NULL,
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.accountType = 'Expense' 
	and OL.expSalariesWages = 'Y'
 
union

-- Line 07 - Column 3 - Benefits

select 'E', 
        109, 
        '7',   -- Total expenses and Deductions | E-7
        3,     -- Column 3 - Benefits
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E13-3 Benefits
        NULL,
        NULL, 
        NULL,
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.expBenefits = 'Y'
 
union 

-- Line 07,4 - Operation and Maintenance of Plant

select 'E', 
        110, 
        '7',   -- Total expenses and Deductions | E-7 
        4, 	   -- Column 4, Operation and Maintenance of Plant
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), -- E7-4,
        NULL,
        NULL, 
        NULL,
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and (OL.expCapitalConstruction = 'Y'
     	or OL.expCapitalEquipPurch = 'Y'
     	or OL.expCapitalLandPurchOther = 'Y')
 
union 

-- Line 07,5 Depreciation 

select 'E', 
        111, 
        '7',   -- Total expenses and Deductions | E-7 
        5,      --Column 5 - Depreciation
        CAST(NVL(ABS(ROUND(SUM( OL.endBalance ))), 0) as BIGINT), -- E13-5 Depreciation
        NULL,
        NULL, 
        NULL,
        NULL 
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.expDepreciation = 'Y' 
  
union  

-- Line 07,6 Interest 

select 'E', 
        112, 
        '7',   -- Total expenses and Deductions | E-7 
        6,      --6 Interest 
        CAST(NVL(ABS(ROUND(SUM( OL.endBalance ))), 0) as BIGINT), -- E13-6 Interest
        NULL,
        NULL,
        NULL, 
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = OL.COASParentChild
	and OL.expInterest = 'Y' 
 
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
