/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Spring Collection
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
                                                         - Removed BothCOASPerFYAsOfDate/BothCOASPerFYPriorAsOfDate
                                                         - Move IPEDSClientConfig values into COASPerFYAsOfDate/COASPerFYPriorAsOfDate
                                                         - Added GL cte for most recent record views for GeneralLedgerReporting
                                                         - Added GL_Prior cte for most recent record views for GeneralLedgerReporting for prior fiscal year
                                                         - Added OL cte for most recent record views for OperatingLedgerReporting
                                                         - Added OL_Prior cte for most recent record views for OperatingLedgerReporting for prior fiscal year
                                                         - Removed cross join with ConfigPerAsOfDate since values are now already in COASPerFYAsOfDate
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
        'F1B' surveyId,
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
        'F1B' surveyId,
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
	   repValues.finParentOrChildInstitution finParentOrChildInstitution,
	   repValues.finReportingModel finReportingModel,
	   repValues.finAthleticExpenses finAthleticExpenses,
	   repValues.finPensionBenefits finPensionBenefits,
	   repValues.finEndowmentAssets finEndowmentAssets
from 
    (select nvl(CASE WHEN upper(reportPeriod.surveySection) = upper(defaultValues.currentSection) THEN reportPeriod.asOfDate END, defaultValues.asOfDate) asOfDate,
			nvl(CASE WHEN upper(reportPeriod.surveySection) = upper(defaultValues.priorSection) THEN reportPeriod.asOfDate END, defaultValues.priorAsOfDate) priorAsOfDate,
            reportPeriod.surveyCollectionYear surveyYear,
            reportPeriod.surveyId surveyId,
            defaultValues.currentSection currentSection,
		    defaultValues.priorSection priorSection,
            defaultValues.finGPFSAuditOpinion finGPFSAuditOpinion,
	        defaultValues.finParentOrChildInstitution finParentOrChildInstitution,
	        defaultValues.finReportingModel finReportingModel,
	        defaultValues.finAthleticExpenses finAthleticExpenses,
	        defaultValues.finPensionBenefits finPensionBenefits,
	        defaultValues.finEndowmentAssets finEndowmentAssets,
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
	    repValues.finParentOrChildInstitution,
	    repValues.finReportingModel,
	    repValues.finAthleticExpenses,
	    repValues.finPensionBenefits,
	    repValues.finEndowmentAssets

union

select defaultValues.surveyYear surveyYear,
    defaultValues.surveyId surveyId,
    defaultValues.asOfDate asOfDate,
    defaultValues.priorAsOfDate priorAsOfDate,
    defaultValues.currentSection currentSection,
	defaultValues.priorSection priorSection,
    defaultValues.finGPFSAuditOpinion finGPFSAuditOpinion,
	defaultValues.finParentOrChildInstitution finParentOrChildInstitution,
	defaultValues.finReportingModel finReportingModel,
	defaultValues.finAthleticExpenses finAthleticExpenses,
	defaultValues.finPensionBenefits finPensionBenefits,
	defaultValues.finEndowmentAssets finEndowmentAssets
from DefaultValues defaultValues
where defaultValues.surveyYear not in (select surveyYear
                                    from DefaultValues defValues
                                        cross join IPEDSReportingPeriod reportPeriod
                                    where reportPeriod.surveyCollectionYear = defValues.surveyYear
                                        and reportPeriod.surveyId = defValues.surveyId)
),

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
		finReportingModel finReportingModel,
	    finParentOrChildInstitution finParentOrChildInstitution
from ( 
	select ReportingDates.surveyYear surveyCollectionYear,
	            ReportingDates.asOfDate asOfDate,
                ReportingDates.priorAsOfDate priorAsOfDate,
                nvl(config.finGPFSAuditOpinion, ReportingDates.finGPFSAuditOpinion) finGPFSAuditOpinion,
				nvl(config.finAthleticExpenses, ReportingDates.finAthleticExpenses) finAthleticExpenses,
                nvl(config.finEndowmentAssets, ReportingDates.finEndowmentAssets) finEndowmentAssets,
                nvl(config.finPensionBenefits, ReportingDates.finPensionBenefits) finPensionBenefits,
                nvl(config.finReportingModel, ReportingDates.finReportingModel) finReportingModel,
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
	  ReportingDates.finReportingModel finReportingModel,
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
		CASE FYData.finReportingModel 
                    WHEN 'B' THEN 1
                    WHEN 'G' THEN 2
                    WHEN 'GB' THEN 3 END finReportingModel, --1=Business Type Activities 2=Governmental Activities 3=Governmental Activities with Business-Type Activities
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
                config.finReportingModel finReportingModel,
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

--jdh 2020-03-04 Removed cross join with ConfigPerAsOfDate since values are now already in COASPerFYAsOfDate
--swapped CASE Statements for COAS fields used for Section 9 (General Information)

select DISTINCT '9' part,
                1 sort,
                CAST(MONTH(nvl(COAS.startDate, COAS.priorAsOfDate))  as BIGINT) field1,
                CAST(YEAR(nvl(COAS.startDate, COAS.priorAsOfDate)) as BIGINT) field2,
                CAST(MONTH(nvl(COAS.endDate, COAS.asOfDate)) as BIGINT) field3,
                CAST(YEAR(nvl(COAS.endDate, COAS.asOfDate)) as BIGINT) field4,
                COAS.finGPFSAuditOpinion field5,  --1=Unqualified, 2=Qualified, 3=Don't know
                COAS.finReportingModel   field6,  --1=Business Type Activities 2=Governmental Activities 3=Governmental Activities with Business-Type Activities
                COAS.finAthleticExpenses field7   --1=Auxiliary enterprises 2=Student services 3=Does not participate in intercollegiate athletics 4=Other (specify in caveats box below)
from COASPerFYAsOfDate COAS 

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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
  and GL.assetCurrent = 'Y'
 
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
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (GL.assetCapitalLand = 'Y'
                                or GL.assetCapitalInfrastructure = 'Y'
                                or GL.assetCapitalBuildings = 'Y'
                                or GL.assetCapitalEquipment = 'Y'
                                or GL.assetCapitalConstruction = 'Y'
                                or GL.assetCapitalIntangibleAsset = 'Y'
                                or GL.assetCapitalOther = 'Y') THEN GL.endBalance ELSE 0 END) 
            - SUM(CASE WHEN GL.accumDepreciation = 'Y' THEN GL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL,
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

-- Line 05 - Total noncurrent assets
-- Report the total of all noncurrent assets as reported in the institution-s GPFS.  

select 'A',
        4,
        '5', --Total noncurrent assets
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
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
        or GL.assetNoncurrentOther = 'Y') 

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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.deferredOutflow = 'Y' 

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
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.liabCurrentLongtermDebt = 'Y' 
	
union

-- Line 09 -Total current liabilitie
-- Report the total of all current liabilities as reported in the institution-s GPFS. 
 
select 'A',
        7,
        '9', --Total current liabilities
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and (GL.liabCurrentLongtermDebt = 'Y' 
		or GL.liabCurrentOther = 'Y') 

union

-- Line 10 - Long-term debt

select 'A',
        8,
        '10', --Long-term debt
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.liabNoncurrentLongtermDebt = 'Y' 

union

select 'A',
        9,
        '12', --Total noncurrent liabilities
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and (GL.liabNoncurrentLongtermDebt = 'Y' 
		or GL.liabNoncurrentOther = 'Y') 

union

select 'A',
        10,
        '20', --Deferred inflows of resources
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.deferredInflow = 'Y' 
	
union

select 'A',
        11,
        '14', --Net assets invested in capital assets, net of related debt
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (GL.assetCapitalLand = 'Y'
                                or GL.assetCapitalInfrastructure = 'Y'
                                or GL.assetCapitalBuildings = 'Y'
                                or GL.assetCapitalEquipment = 'Y'
                                or GL.assetCapitalConstruction = 'Y'
                                or GL.assetCapitalIntangibleAsset = 'Y'
                                or GL.assetCapitalOther = 'Y') THEN GL.endBalance ELSE 0 END)
                    - SUM(CASE WHEN GL.accumDepreciation = 'Y' or GL.isCapitalRelatedDebt = 1 THEN GL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL,
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
      or GL.accumDepreciation = 'Y' 
      or GL.isCapitalRelatedDebt = 1) 

union

select 'A',
        12,
        '15', --Restricted expendable net assets
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.accountType = 'Asset' 
    and GL.isRestrictedExpendOrTemp = 1 
	
union

select 'A',
        13,
        '16', --Restricted non-expendable net assets
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
    and GL.institParentChild = 'P'
    and GL.accountType = 'Asset' 
    and GL.isRestrictedNonExpendOrPerm = 1 
 
union

-- Part P  
-- Capital Assets
/*  This part represents page 2 of Part A in the online forms.
    While the form shows Part A Page 2 as a component of Part A, The specs have us listing as Part P 
*/

-- Line 21 - Land and land improvements
-- Report land and other land improvements, such as athletic fields, golf courses, lakes, etc.

select 'P',
        14,
        '21', --Land and land improvements
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL    
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = GL.COASParentChild
  and GL.assetCapitalLand = 'Y'

union

-- Line 22 - Infrastructure
-- Report infrastructure assets such as roads, bridges, drainage systems, water and sewer systems, etc.

select 'P',
        15,
        '22', --Infrastructure
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL    
where fiscalPeriod = 'Year End'
   and GL.institParentChild = GL.COASParentChild
   and GL.assetCapitalInfrastructure = 'Y'

union

-- Line 23 - Buildings
/*  Report structures built for occupancy or use, such as for classrooms, research, administrative offices, storage, etc.
    Include built-in fixtures and equipment that are essentially part of the permanent structure.
*/

select 'P',
        16,
        '23', --Buildings
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL    
where fiscalPeriod = 'Year End'
   and GL.institParentChild = GL.COASParentChild
   and GL.assetCapitalBuildings = 'Y'

union

-- Line 32 - Equipment, including art and library collections - 
/*  Report moveable tangible property such as research equipment, vehicles, office equipment, library collections 
    (capitalized amount of books, films, tapes, and other materials maintained in library collections intended for use by patrons), 
    and capitalized art collections.
*/

select 'P',
        17,
        '32', --Equipment
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL    
where fiscalPeriod = 'Year End'
   and GL.institParentChild = GL.COASParentChild
   and GL.assetCapitalEquipment = 'Y'

union

-- Line 27 - Construction in progress - 
-- Report capital assets under construction and not yet placed into service.

select 'P',
        18,
        '27', --Construction in Progress
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL    
where fiscalPeriod = 'Year End'
   and GL.institParentChild = GL.COASParentChild
   and GL.assetCapitalConstruction = 'Y'

union

-- Line 28 - Accumulated depreciation - 
-- Report all depreciation amounts, including depreciation on assets that may not be included on any of the above lines.

select 'P',
        19,
        '28', --Accumulated depreciation
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL    
where fiscalPeriod = 'Year End'
   and GL.institParentChild = GL.COASParentChild
   and GL.accumDepreciation = 'Y'

union

-- Line 33 - Intangible assets, net of accumulated amortization - 
/*  Report all assets consisting of certain nonmaterial rights and benefits of an institution, such as patents, copyrights, trademarks and goodwill. 
    The amount report should be reduced by total accumulated amortization.
*/

select 'P',
        20,
        '33', --Intangible assets, net of accumulated amortization
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN GL.assetCapitalIntangibleAsset = 'Y' THEN GL.endBalance ELSE 0 END) 
                    - SUM(CASE WHEN GL.accumAmmortization = 'Y' THEN GL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL    
where fiscalPeriod = 'Year End'
   and GL.institParentChild = GL.COASParentChild
   and (GL.assetCapitalIntangibleAsset = 'Y'
     or GL.accumAmmortization = 'Y')

union

-- Line 34 - Other capital assets  
-- Report all other amounts for capital assets not reported in lines 21 through 28, and lines 32 and 33.

select 'P',
        21,
        '34', --Other capital assets
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL    
where fiscalPeriod = 'Year End'
   and GL.institParentChild = GL.COASParentChild
   and GL.assetCapitalOther = 'Y'
     
union

-- Part D  
-- Summary of Changes in Net Assets
/*  This part is intended to report a summary of changes in net position and to determine that all amounts being reported on the 
    Statement of Financial Position (Part A), Revenues and Other Additions (Part B), and Expenses and Other Deductions (Part B) are in agreement. 
*/

-- Line 01 - Total revenues & other additions  
/*  Enter total revenues and other additions. The amount should represent all revenues reported for the fiscal period and should agree 
    with the revenues recognized in the institution's GPFS and should match the figure reported in Part B, line 25.
*/

select 'D',
        22,
        '1', --Total revenues and other additions
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL  
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = 'P'
    and OL.accountType = 'Revenue'

union

select 'D',
        23,
        '2', --Total expenses and deductions
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL  
where OL.fiscalPeriod = 'Year End' 
    and OL.institParentChild = 'P'
    and OL.accountType = 'Expense'
	
union

select 'D',
        24,
        '4', --Net assets beginning of year
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN GL.accountType = 'Asset' THEN GL.beginBalance ELSE 0 END))), 0) as BIGINT) 
        - CAST(NVL(ABS(ROUND(SUM(CASE WHEN GL.accountType = 'Liability' THEN GL.beginBalance ELSE 0 END))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year Begin' 
    and GL.institParentChild = 'P'
    and (GL.accountType = 'Asset' 
	  or GL.accountType = 'Liability')
      
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
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
   and OL.institParentChild = OL.COASParentChild
   and OL.expFAPellGrant = 'Y'
	
union

select 'E',
        26,
        '2', --Other Federal grants
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
   and OL.institParentChild = OL.COASParentChild
    and OL.expFANonPellFedGrants = 'Y'
	
union

select 'E',
        27,
        '3', --Grants by state government
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
   and OL.institParentChild = OL.COASParentChild
    and OL.expFAStateGrants = 'Y'
    
union

select 'E',
        28,
        '4', --Grants by local government
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
   and OL.institParentChild = OL.COASParentChild
   and OL.expFALocalGrants = 'Y'
	
union

select 'E',
        29,
        '5', --Institutional grants from restricted resources
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild

union

-- Line 07 - Total revenue that funds scholarships and fellowships 
/*  Report the total revenue used to fund scholarships and fellowships from sources in lines 01 to 06. 
    Check this amount with the corresponding amount on their GPFS or underlying records. If these amounts differ materially, 
    the data provider is advised to check the other amounts provided on this screen for data entry errors. 
*/

select 'E',
        30,
        '7', -- Total revenue that funds scholarships and fellowships
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT)  ,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
   and OL.institParentChild = OL.COASParentChild
   and (OL.expFAPellGrant = 'Y'             --E1 Pell grants
      or OL.expFANonPellFedGrants = 'Y'     --E2 Other federal grants
      or OL.expFAStateGrants = 'Y'          --E3 Grants by state government
      or OL.expFALocalGrants = 'Y'          --E4 Grants by local government
      or OL.expFAInstitGrantsRestr = 'Y'    --E5 Institutional grants from restricted resources
      or OL.expFAInstitGrantsUnrestr = 'Y') --E6 Institutional grants from unrestricted resources

union

select 'E',
        31,
        '8', --Discounts and allowances applied to tuition and fees
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT)  ,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
   and OL.institParentChild = OL.COASParentChild
   and OL.discAllowTuitionFees = 'Y'

union

select 'E',
        32,
        '9', --Discounts and allowances applied to sales and services of auxiliary enterprises
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.discAllowAuxEnterprise = 'Y'
 
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
        33,
        '1', --Tuition and fees (after deducting discounts and allowances)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.revTuitionAndFees = 'Y' THEN OL.endBalance ELSE 0 END) 
                         - SUM(CASE WHEN OL.discAllowTuitionFees = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.revTuitionAndFees = 'Y' 
    or OL.discAllowTuitionFees = 'Y')

union

select 'B',
        34,
        '2', --Federal operating grants and contracts
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revFedGrantsContractsOper = 'Y'

union

select 'B',
        35,
        '3', --State operating grants and contracts
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revStateGrantsContractsOper = 'Y'

union

select 'B',
        36,
        '4a', --Local government operating grants and contracts
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revLocalGrantsContractsOper = 'Y'

union

select 'B',
        37,
        '4b', --Private operating grants and contracts
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revPrivGrantsContractsOper = 'Y'

union

select 'B',
        38,
        '5', --Sales and services of auxiliary enterprises (after deducting discounts and allowances)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.revAuxEnterprSalesServices = 'Y' THEN OL.endBalance ELSE 0 END) 
                         - SUM(CASE WHEN OL.discAllowAuxEnterprise = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.revAuxEnterprSalesServices = 'Y'
    or OL.discAllowAuxEnterprise = 'Y' )

union

select 'B',
        39,
        '6', --Sales and services of hospitals (after deducting patient contractual allowances)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.revHospitalSalesServices = 'Y' THEN OL.endBalance ELSE 0 END) 
                        - SUM(CASE WHEN OL.discAllowPatientContract = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.revHospitalSalesServices = 'Y'
	  or OL.discAllowPatientContract = 'Y')

union

select 'B',
        40,
        '26', --Sales & services of educational activities
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revEducActivSalesServices = 'Y'

union

select 'B',
        41,
        '7', --Independent operations
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revIndependentOperations = 'Y'

union

-- Line 09 - Total Operating Revenues - 
-- Report total operating revenues from your GPFS.

select 'B',
        42,
        '9', --Total operating revenues
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.revTuitionAndFees = 'Y'            --B1 Tuition and fees (after deducting discounts and allowances)
                                    or OL.revFedGrantsContractsOper = 'Y'       --B2 Federal operating grants and contracts
                                    or OL.revStateGrantsContractsOper = 'Y'     --B3 State operating grants and contracts
                                    or OL.revLocalGrantsContractsOper = 'Y'     --B4a Local government operating grants and contracts
                                    or OL.revPrivGrantsContractsOper = 'Y'      --B5b Private operating grants and contracts
                                    or OL.revAuxEnterprSalesServices = 'Y'      --B5 Sales and services of auxiliary enterprises (after deducting discounts and allowances)
                                    or OL.revHospitalSalesServices = 'Y'        --B6 Sales and services of hospitals (after deducting patient contractual allowances)
                                    or OL.revEducActivSalesServices = 'Y'       --B26 Sales & services of educational activities
                                    or OL.revOtherSalesServices = 'Y'
                                    or OL.revIndependentOperations = 'Y'        --B7 Independent operations
                                    or OL.revOtherOper = 'Y'                    --B8 Other sources - operating
                                        ) THEN OL.endBalance ELSE 0 END) -
                        SUM(CASE WHEN (OL.discAllowTuitionFees = 'Y'            --B1 Tuition and fees discounts and allowances
                                    or OL.discAllowAuxEnterprise = 'Y'          --B5 auxiliary enterprises discounts and allowances
                                    or OL.discAllowPatientContract = 'Y'        --B6 patient contractual allowances
                                        ) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.revTuitionAndFees = 'Y'  
        or OL.revFedGrantsContractsOper = 'Y'  
        or OL.revStateGrantsContractsOper = 'Y'  
        or OL.revLocalGrantsContractsOper = 'Y'  
        or OL.revPrivGrantsContractsOper = 'Y'  
        or OL.revAuxEnterprSalesServices = 'Y'  
        or OL.revHospitalSalesServices = 'Y' 
        or OL.revEducActivSalesServices = 'Y'  
        or OL.revOtherSalesServices = 'Y' 
        or OL.revIndependentOperations = 'Y'  
        or OL.revOtherOper = 'Y' 
        or OL.discAllowTuitionFees = 'Y' 
        or OL.discAllowAuxEnterprise = 'Y' 
        or OL.discAllowPatientContract = 'Y')

union

select 'B',
        43,
        '10', --Federal appropriations
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revFedApproprations = 'Y'

union

select 'B',
        44,
        '11', --State appropriations
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT)  ,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revStateApproprations = 'Y'

union

select 'B',
        45,
        '12', --Local appropriations, education district taxes, and similar support
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.revLocalApproprations = 'Y' 
      or OL.revLocalTaxApproprations = 'Y')
   
union

select 'B',
        46,
        '13', --Federal nonoperating grants
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revFedGrantsContractsNOper = 'Y'

union

select 'B',
        47,
        '14', --State nonoperating grants
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revStateGrantsContractsNOper = 'Y'

union

select 'B',
        48,
        '15', --Local government nonoperating grants
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revLocalGrantsContractsNOper = 'Y'

union

select 'B',
        49,
        '16', --Gifts, including contributions from affiliated organizations
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.revPrivGifts = 'Y' 
        or OL.revAffiliatedOrgnGifts = 'Y')

union

select 'B',
        50,
        '17', --Investment income
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revInvestmentIncome = 'Y'

union

select 'B',
        51,
        '19', --Total nonoperating revenues
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.revFedApproprations = 'Y'               --B10 Federal appropriations
        or OL.revStateApproprations = 'Y'         --B11 State appropriations
        or OL.revLocalApproprations = 'Y'         --B12 Local appropriations, education district taxes, and similar support
        or OL.revLocalTaxApproprations = 'Y'      --B12 Local appropriations, education district taxes, and similar support
        or OL.revFedGrantsContractsNOper = 'Y'    --B13 Federal nonoperating grants
        or OL.revStateGrantsContractsNOper = 'Y'  --B14 State nonoperating grants
        or OL.revLocalGrantsContractsNOper = 'Y'  --B15 Local government nonoperating grants
        or OL.revPrivGifts = 'Y'                  --B16 Gifts, including contributions from affiliated organizations
        or OL.revAffiliatedOrgnGifts = 'Y'        --B16 Gifts, including contributions from affiliated organizations
        or OL.revInvestmentIncome = 'Y'           --B17 Investment income
        or OL.revOtherNOper = 'Y'                 --Other sources - nonoperating
        or OL.revPrivGrantsContractsNOper = 'Y')  --Other - Private nonoperating grants

union

select 'B',
        52,
        '20', --Capital appropriations
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.revStateCapitalAppropriations = 'Y' 
        or OL.revLocalCapitalAppropriations = 'Y')

union

select 'B',
        53,
        '21', --Capital grants and gifts
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT)  ,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revCapitalGrantsGifts = 'Y'

union

select 'B',
        54,
        '22', --Additions to permanent endowments
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revAddToPermEndowments = 'Y'

union

select 'B',
        55,
        '25', --Total all revenues and other additions
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.accountType = 'Revenue' THEN OL.endBalance ELSE 0 END) -
                           SUM(CASE WHEN OL.accountType = 'Revenue Discount' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.accountType = 'Revenue'
        or OL.accountType = 'Revenue Discount')
 
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
        56,
        '1', --Instruction
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT)  , --Total amount (1-7,11-13,19)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19))
        NULL,
        NULL,
        NULL,
        NULL
from OL OL
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.accountType = 'Expense'

union

select 'C',
        57,
        '2', --Research
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
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

select 'C',
        58,
        '3', --Public service
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT)  , --Total amount (1-7,11-13,19)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
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

select 'C',
        59,
        '5', --Academic support
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
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

select 'C',
        60,
        '6', --Student services
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
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

select 'C',
        61,
        '7', --Institutional support
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y'  THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
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

select 'C',
        62,
        '11', --Auxiliary enterprises
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
        NULL,
        NULL,
        NULL,
        NULL
from OL OL
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.accountType = 'Expense' 
  and OL.isAuxiliaryEnterprises = 1

union

select 'C',
        63,
        '12', --Hospital services
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Salaries and wages (1-7,11-13,19)
        NULL,
        NULL,
        NULL,
        NULL
from OL OL
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.accountType = 'Expense' 
  and OL.isHospitalServices = 1

union

select 'C',
        64,
        '13', --Independent operations
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y'  THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
        NULL,
        NULL,
        NULL,
        NULL
from OL OL
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.accountType = 'Expense' 
  and OL.isIndependentOperations = 1

union

select 'C',
        65,
        '19', --Total expenses and deductions
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.accountType = 'Expense' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Total amount (1-7,11-13,19)
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.accountType = 'Expense' and OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19) 19_2
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expBenefits = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Benefits 19_3
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintBenefits = 'Y' or OL.expOperMaintOther = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Oper and Maint of Plant 19_4
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expDepreciation = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Depreciation 19_5
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expInterest = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)   --Interest 19
from OL OL
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.accountType = 'Expense'
    or OL.expSalariesWages = 'Y'
    or OL.expBenefits = 'Y'
    or OL.expOperMaintSalariesWages = 'Y'
    or OL.expOperMaintBenefits = 'Y'
    or OL.expOperMaintOther = 'Y'
    or OL.expDepreciation = 'Y'
    or OL.expInterest = 'Y')	  
 
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
        66,
        '1', --Pension expense
        CAST(case when (select config.finPensionBenefits
                        from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(OL.endBalance))), 0) 
			else 0
		end as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = 'P'
  and OL.accountType = 'Expense' 
  and OL.isPensionGASB = 1

union

select 'M',
        67,
        '2', --Net Pension liability
        CAST(case when (select config.finPensionBenefits
			from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(GL.endBalance))), 0) 
			else 0
		        end as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
  and GL.accountType = 'Liability' 
  and GL.isPensionGASB = 1

union

select 'M',
        68,
        '3', --Deferred inflows (an acquisition of net assets) related to pension
        CAST(case when (select config.finPensionBenefits
						from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(GL.endBalance))), 0) 
			 else 0
		end as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
  and GL.accountType = 'Liability' 
  and GL.deferredInflow = 'Y' 
  and GL.isPensionGASB = 1

union

select 'M',
        69,
        '4', --Deferred outflows(a consumption of net assets) related to pension
        CAST(case when (select config.finPensionBenefits
						from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(GL.endBalance))), 0) 
			 else 0
		end as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
  and GL.accountType = 'Asset' 
  and GL.deferredOutflow = 'Y' 
  and GL.isPensionGASB = 1
    
union

select 'M',
        70,
        '5', --OPEB Expense
        CAST(case when (select config.finPensionBenefits
						from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(OL.endBalance))), 0) 
			 else 0
		end as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = 'P'
  and OL.accountType = 'Expense' 
  and OL.isOPEBRelatedGASB = 1 

union

select 'M',
        71,
        '6', --Net OPEB Liability
        CAST(case when (select config.finPensionBenefits
						from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(GL.endBalance))), 0) 
			 else 0
		end as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
  and GL.accountType = 'Liability' 
  and GL.isOPEBRelatedGASB = 1

union

select 'M',
        72,
        '7', --Deferred inflow related to OPEB
        CAST(case when (select config.finPensionBenefits
						from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(GL.endBalance))), 0) 
			 else 0
		end as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
  and GL.accountType = 'Liability' 
  and GL.deferredInflow = 'Y' 
  and GL.isOPEBRelatedGASB = 1

union

select 'M',
        73,
        '8', --Deferred outflow related to OPEB
        CAST(case when (select config.finPensionBenefits
						from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(GL.endBalance))), 0) 
			 else 0
		end as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
  and GL.accountType = 'Asset' 
  and GL.deferredOutflow = 'Y' 
  and GL.isOPEBRelatedGASB = 1

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
        74,
        '1', --Value of endowment assets at the beginning of the fiscal year
		CAST(case when (select config.finEndowmentAssets
				from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(GL.beginBalance))), 0) 
			        else 0
		                end as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL
where GL.fiscalPeriod = 'Year Begin'
  and GL.institParentChild = GL.COASParentChild
  and GL.accountType = 'Asset' 
  and GL.isEndowment = 1

union

select 'H',
        75,
        '2', --Value of endowment assets at the END of the fiscal year
        CAST(case when (select config.finEndowmentAssets
			from ConfigPerAsOfDate config) = 'Y' then NVL(ABS(ROUND(SUM(GL.endBalance))), 0) 
			 else 0
		end as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = GL.COASParentChild
  and GL.accountType = 'Asset' 
  and GL.isEndowment = 1
	 
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
        76,
        '2', --Sales and services
        NULL, --Total amount J2,1 --(Do not send in file, Calculated )
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and (OL.revEducActivSalesServices = 'Y' or OL.revOtherSalesServices = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations  
        NULL, --Auxiliary enterprises J2,3
        NULL, --Hospitals J2,4
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN  OL.isAgricultureOrExperiment = 1 and OL.revOtherSalesServices = 'Y' THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and ((OL.isAgricultureOrExperiment = 0 
	 and OL.isAuxiliaryEnterprises = 0 
	 and OL.isHospitalServices = 0) 
  or (OL.isAgricultureOrExperiment = 1 
	 and OL.revOtherSalesServices = 'Y'))

union

-- Line 3 - Federal grants and contracts (excluding Pell)
/*  Include both operating and non-operating grants, but exclude Pell and other student grants
	and any Federal loans received on behalf of the students. Include all other direct Federal
	grants, including research grants, in the appropriate column.
*/

select 'J',
        77,
        '3', --Federal grants and contracts (excluding Pell)
        NULL, --Total amount  -- (Do not include in file) 
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations 
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAuxiliaryEnterprises = 1 and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y'))) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  ,   --Hospitals  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (((OL.isAgricultureOrExperiment = 0 
                and OL.isAuxiliaryEnterprises = 0 
                and OL.isHospitalServices = 0) 
        and (OL.revFedGrantsContractsOper = 'Y' 
                or OL.revFedGrantsContractsNOper = 'Y'))
   or ((OL.isAuxiliaryEnterprises = 1 
                and (OL.revFedGrantsContractsOper = 'Y' 
                        or OL.revFedGrantsContractsNOper = 'Y')))
        or (OL.isHospitalServices = 1 
                and (OL.revFedGrantsContractsOper = 'Y' 
                        or OL.revFedGrantsContractsNOper = 'Y')))

union

-- Line 04 - State appropriations, current and capital
-- Include all operating and non-operating appropriations, as well as all current and capital appropriations. 

select 'J',
        78,
        '4', --State appropriations, current and capital
        NULL, --Total amount  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and (OL.revStateApproprations = 'Y' or OL.revStateCapitalAppropriations = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations 2-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and (OL.revStateApproprations = 'Y' or OL.revStateCapitalAppropriations = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises 3-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and (OL.revStateApproprations = 'Y' or OL.revStateCapitalAppropriations = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Hospitals 3-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and (OL.revStateApproprations = 'Y' or OL.revStateCapitalAppropriations = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 2-7
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (((OL.isAgricultureOrExperiment = 0 
			and OL.isAuxiliaryEnterprises = 0 
			and OL.isHospitalServices = 0) 
		and (OL.revStateApproprations = 'Y' 
				or OL.revStateCapitalAppropriations = 'Y'))
  or (OL.isAuxiliaryEnterprises = 1 
			and (OL.revStateApproprations = 'Y' 
				or OL.revStateCapitalAppropriations = 'Y'))
	  or (OL.isHospitalServices = 1 
			and (OL.revStateApproprations = 'Y' 
				or OL.revStateCapitalAppropriations = 'Y'))
	  or (OL.isAgricultureOrExperiment = 1 
			and (OL.revStateApproprations = 'Y' 
				or OL.revStateCapitalAppropriations = 'Y')))

union

-- Line 5 - State grants and contracts
-- Include state grants and contracts, both operating and non-operating, in the proper column. Do not include state student grant aid.*/

select 'J',
        79,
        '5', --State grants and contracts
        NULL, --Total amount  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0  and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations 2-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises 3-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Hospitals 3-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Agriculture extension/experiment services 2-7
        NULL   
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (((OL.isAgricultureOrExperiment = 0 
                        and OL.isAuxiliaryEnterprises = 0 
                        and OL.isHospitalServices = 0) 
                and (OL.revStateGrantsContractsOper = 'Y' 
                        or OL.revStateGrantsContractsNOper = 'Y'))
        or (OL.isAuxiliaryEnterprises = 1 
                and (OL.revStateGrantsContractsOper = 'Y' 
                        or OL.revStateGrantsContractsNOper = 'Y'))
        or (OL.isHospitalServices = 1 
                and (OL.revStateGrantsContractsOper = 'Y' 
                        or OL.revStateGrantsContractsNOper = 'Y'))
        or (OL.isAgricultureOrExperiment = 1 
                and (OL.revStateGrantsContractsOper = 'Y' 
                        or OL.revStateGrantsContractsNOper = 'Y')))

union

-- Line 06 - Local appropriations, current and capital
/*  Include local government appropriations in the appropriate column, regardless of whether appropriations were for 
    current or capital. This generally applies only to local institutions of higher education.
*/

select 'J',
        80,
        '6', --Local appropriations, current and capital
        NULL, --Total amount   -- (Do not include in file) 
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises 3-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Hospitals 3-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 2-7
        NULL  
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (((OL.isAgricultureOrExperiment = 0 
        and OL.isAuxiliaryEnterprises = 0 
        and OL.isHospitalServices = 0) 
        and (OL.revLocalApproprations = 'Y' 
                        or OL.revLocalCapitalAppropriations = 'Y'))
        or (OL.isAuxiliaryEnterprises = 1 
                and (OL.revLocalApproprations = 'Y' 
                        or OL.revLocalCapitalAppropriations = 'Y'))
        or (OL.isHospitalServices = 1 
                and (OL.revLocalApproprations = 'Y' 
                        or OL.revLocalCapitalAppropriations = 'Y'))
        or (OL.isAgricultureOrExperiment = 1 
                and (OL.revLocalApproprations = 'Y' 
                        or OL.revLocalCapitalAppropriations = 'Y')))

union

-- Line 07 - Local grants and contracts
/*  Include state grants and contracts, both operating and non-operating, in the proper column. 
    Do not include state student grant aid.
*/

select 'J',
        81,
        '7', --Local grants and contracts
        NULL, --Total amount   -- (Do not include in file) 
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Education and general/independent operations 2-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises 3-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Hospitals 3-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 2-7
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (((OL.isAgricultureOrExperiment = 0 
				and OL.isAuxiliaryEnterprises = 0 	
				and OL.isHospitalServices = 0) 
			and (OL.revLocalGrantsContractsOper = 'Y' 
				or OL.revLocalGrantsContractsNOper = 'Y'))
		or (OL.isAuxiliaryEnterprises = 1 
			and (OL.revLocalGrantsContractsOper = 'Y' 
				or OL.revLocalGrantsContractsNOper = 'Y'))
        or (OL.isHospitalServices = 1 
			and (OL.revLocalGrantsContractsOper = 'Y' 
				or OL.revLocalGrantsContractsNOper = 'Y'))
        or (OL.isAgricultureOrExperiment = 1 
			and (OL.revLocalGrantsContractsOper = 'Y' 
				or OL.revLocalGrantsContractsNOper = 'Y')))
	
union

-- Line 08 - Receipts from property and non-property taxes - Total all funds
/*  This item applies only to local institutions of higher education. Include in column 1 any revenue from locally imposed property taxes or
    other taxes levied by the local higher education district. Include all funds - current, restricted, unrestricted and debt service.
    Exclude taxes levied by another government and transferred to the local higher education district by the levying government. 
*/

select 'J',
        82,
        '8', --Receipts from property and non-property taxes - Total all funds
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT)  , --Total amount 8-12
        NULL,
        NULL,
        NULL,
        NULL, 
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and OL.revPropAndNonPropTaxes = 'Y'

union

-- Line 09 - Gifts and private grants, NOT including capital grants 
/*  Include grants from private organizations and individuals here. Include additions to
    permanent endowments if they are gifts. Exclude gifts to component units and capital contributions.   
*/
 
select 'J',
        83,
        '9', --Gifts and private grants, NOT including capital grants
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT)  , --Total amount 8-12
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild
  and (OL.revPrivGrantsContractsOper = 'Y' 
    or OL.revPrivGrantsContractsNOper = 'Y' 
    or OL.revPrivGifts = 'Y' 
    or OL.revAffiliatedOrgnGifts = 'Y')

union

-- Line 10 - Interest earnings
-- Report the total interest earned in column 1. Include all funds and endowments.  

select 'J',
        84,
        '10', --Interest earnings
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --Total amount 8-12
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild 
  and OL.revInterestEarnings = 'Y'

union

-- Line 11 - Dividend earnings
/*  Dividends should be reported separately if available. Report only the total, in column 1,
    from all funds including endowments but excluding dividends of any component units. Note: if
    dividends are not separately available, please report include with Interest earnings in J10, column 1. 
*/

select 'J',
        85,
        '11', --Dividend earnings
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT)  , --Total amount 8-12
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild  
  and OL.revDividendEarnings = 'Y'

union

-- Line 12 - Realized capital gains
/*  Report only the total earnings. Do not include unrealized gains.
    Also, include all other miscellaneous revenue. Use column 1 only. 
*/

select 'J',
        86,
        '12', --Realized capital gains
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT), --Total amount 8-12
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild 
  and OL.revRealizedCapitalGains = 'Y'

union

-- Part K   
-- Expenditures for Bureau of the Census 
 
-- Line 02 - Employee benefits
/* 	Report the employee benefits for staff associated with Education and General, Auxiliary Enterprises,
	Hospitals, and for Agricultural extension/experiment services, if applicable. 
*/

select 'K',
        87,
        '2', --Employee benefits, total
        NULL, --Total amount 8
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  K24, --Hospitals 
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services  
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild 
  and (((OL.isAgricultureOrExperiment = 0 
                and OL.isAuxiliaryEnterprises = 0 
                and OL.isHospitalServices = 0)
                        and (OL.expBenefits = 'Y' 
                                or OL.expOperMaintBenefits = 'Y'))
        or (OL.isAuxiliaryEnterprises = 1 
                and (OL.expBenefits = 'Y' 
                        or OL.expOperMaintBenefits = 'Y'))
        or (OL.isHospitalServices = 1 
                and (OL.expBenefits = 'Y' 
                        or OL.expOperMaintBenefits = 'Y'))
        or (OL.isAgricultureOrExperiment = 1 
                and (OL.expBenefits = 'Y' 
                        or OL.expOperMaintBenefits = 'Y')))

union

-- Line 03 - Payment to state retirement funds
/*  Applies to state institutions only. Include amounts paid to retirement systems operated by
    your state government only. Include employer contributions only. Exclude employee contributions withheld. 
*/

select 'K',
        88,
        '3', --Payment to state retirement funds
        NULL, --Total amount  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations 2-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Auxiliary enterprises  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Hospitals  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Agriculture extension/experiment services  
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild 
  and (((OL.isAgricultureOrExperiment = 0 
			and OL.isAuxiliaryEnterprises = 0 
			and OL.isHospitalServices = 0) 
				and OL.accountType = 'Expense' 
				and OL.isStateRetireFundGASB = 1)
		or (OL.isAuxiliaryEnterprises = 1 
			and OL.accountType = 'Expense' 
			and OL.isStateRetireFundGASB = 1)
		or (OL.isHospitalServices = 1 
			and OL.accountType = 'Expense' 
			and OL.isStateRetireFundGASB = 1)
		or (OL.isAgricultureOrExperiment = 1 
			and OL.accountType = 'Expense' 
			and OL.isStateRetireFundGASB = 1))

union

-- Line 04 - Current expenditures including salaries
/*  Report all current expenditures including salaries, employee benefits, supplies, materials, contracts and professional services, 
    utilities, travel, and insurance.  Exclude scholarships and fellowships, capital outlay, interest(report on line 8), 
    employer contributions to state retirement systems (applies to state institutions only) and depreciation. 
*/

select 'K',
        89,
        '4', --Current expenditures including salaries
        NULL, --Total amount 8
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') and OL.isStateRetireFundGASB = 1)) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Education and general/independent operations 2-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') and OL.isStateRetireFundGASB = 1)) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises 2-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') and OL.isStateRetireFundGASB = 1)) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Hospitals 2-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') and OL.isStateRetireFundGASB = 1)) THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 2-7
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild 
	and (((OL.isAgricultureOrExperiment = 0 
			and OL.isAuxiliaryEnterprises = 0 
			and OL.isHospitalServices = 0) 
				and ((OL.expSalariesWages = 'Y' 
					or OL.expOperMaintSalariesWages = 'Y' 
					or OL.expOperMaintOther = 'Y' 
					or OL.expOther = 'Y') 
				and OL.isStateRetireFundGASB = 1))
		or (OL.isAuxiliaryEnterprises = 1 
			and ((OL.expSalariesWages = 'Y' 
				or OL.expOperMaintSalariesWages = 'Y' 
				or OL.expOperMaintOther = 'Y' 
				or OL.expOther = 'Y') 
			 and OL.isStateRetireFundGASB = 1))
	   or (OL.isHospitalServices = 1 
			and ((OL.expSalariesWages = 'Y' 
				or OL.expOperMaintSalariesWages = 'Y' 
				or OL.expOperMaintOther = 'Y' 
				or OL.expOther = 'Y') 
			 and OL.isStateRetireFundGASB = 1))
	   or (OL.isAgricultureOrExperiment = 1 
			and ((OL.expSalariesWages = 'Y' 
				or OL.expOperMaintSalariesWages = 'Y' 
				or OL.expOperMaintOther = 'Y' 
				or OL.expOther = 'Y') 
			 and OL.isStateRetireFundGASB = 1)))

union

-- Line 05 - Capital outlay, construction
/*  Construction from all funds (plant, capital, or bond funds) includes expenditure for the construction of new structures and other
    permanent improvements, additions replacements, and major alterations. Report in proper column according to function. 
*/

select 'K',
        90,
        '5', --Capital outlay, construction
        NULL, --Total amount -- K5,1    -- (Do not include in file) 
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and OL.expCapitalConstruction = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and OL.expCapitalConstruction = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and OL.expCapitalConstruction = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Hospitals  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and OL.expCapitalConstruction = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Agriculture extension/experiment services  
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild 
	and (((OL.isAgricultureOrExperiment = 0 
				and OL.isAuxiliaryEnterprises = 0 	
				and OL.isHospitalServices = 0) 
			and OL.expCapitalConstruction = 'Y')
		or (OL.isAuxiliaryEnterprises = 1 
			and OL.expCapitalConstruction = 'Y')
		or (OL.isHospitalServices = 1 
			and OL.expCapitalConstruction = 'Y')
		or (OL.isAgricultureOrExperiment = 1 
			and OL.expCapitalConstruction = 'Y'))

union

--  Line 06 - Capital outlay, equipment purchases
--  Equipment purchases from all funds (plant, capital, or bond funds). 
 
select 'K',
        91,
        '6',  --Capital outlay, equipment purchases
        NULL, --Total amount   -- K6,1    -- (Do not include in file) 
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and OL.expCapitalEquipPurch = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Education and general/independent operations 2-7
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and OL.expCapitalEquipPurch = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Auxiliary enterprises  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and OL.expCapitalEquipPurch = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Hospitals  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and OL.expCapitalEquipPurch = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Agriculture extension/experiment services  
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild 
	and (((OL.isAgricultureOrExperiment = 0 
			and OL.isAuxiliaryEnterprises = 0 
			and OL.isHospitalServices = 0) 
		 and OL.expCapitalEquipPurch = 'Y')
		or (OL.isAuxiliaryEnterprises = 1 
			and OL.expCapitalEquipPurch = 'Y')
		or (OL.isHospitalServices = 1 
			and OL.expCapitalEquipPurch = 'Y')
		or (OL.isAgricultureOrExperiment = 1 
			and OL.expCapitalEquipPurch = 'Y'))

union

-- Line 07 - Capital outlay, land purchases
/*  from all funds (plant, capital, or bond funds), include the cost of land and existing structures, as well as the purchase of rights-of-way.
    Include all capital outlay other than Construction if not specified elsewhere. 
*/

select 'K',
        92,
        '7',  --Capital outlay, land purchases
        NULL, --Total amount  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) and OL.expCapitalLandPurchOther = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Education and general/independent operations  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and OL.expCapitalLandPurchOther = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Auxiliary enterprises  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and OL.expCapitalLandPurchOther = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT)  , --Hospitals  
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and OL.expCapitalLandPurchOther = 'Y') THEN OL.endBalance ELSE 0 END))), 0) as BIGINT), --Agriculture extension/experiment services 
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild 
  and (((OL.isAgricultureOrExperiment = 0 
                and OL.isAuxiliaryEnterprises = 0 
                and OL.isHospitalServices = 0) 
                and OL.expCapitalLandPurchOther = 'Y')
        or (OL.isAuxiliaryEnterprises = 1 
                and OL.expCapitalLandPurchOther = 'Y')
        or (OL.isHospitalServices = 1 
                and OL.expCapitalLandPurchOther = 'Y')
        or (OL.isAgricultureOrExperiment = 1 
                and OL.expCapitalLandPurchOther = 'Y'))

union

-- Line 08 - Interest paid on revenue debt only. 
/*  Includes interest on debt issued by the institution, such as that which is repayable from pledged earnings, 
    charges or gees (e.g. dormitory, stadium, or student union revenue bonds). Report only the total, 
    in column 1. Excludes interest expenditure of the parent state or local government on debt issued on behalf 
    of the institution and backed by that parent government. Also excludes interest on debt issued by a state 
    dormitory or housing finance agency on behalf of the institution. 
*/

select 'K',
        93,
        '8', --Interest on debt outstanding, all funds and activities
        CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) as BIGINT)  , --Total amount
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.institParentChild = OL.COASParentChild 
	and OL.expInterest = 'Y'

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
        94,
        '1', --Long-term debt outstanding at beginning of fiscal year
        CAST(NVL(ABS(ROUND(SUM(GL.beginBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year Begin'
  and GL.institParentChild = 'P'
--jh 20200107 added parenthesis for or clause
  and (GL.liabCurrentLongtermDebt = 'Y' 
    or GL.liabNoncurrentLongtermDebt = 'Y')

union

-- Line 02 - Long-term debt issued during fiscal year  

--jdh 2020-03-30 Janet & JD discussed: 
--  Decision was made to identify Long-Term debt acquired as 
--  Any long-term Accounts that did not exist or had a balance of 0 at the begining of FY
--  and had a balance greater than 0 at the end of the FY. 

select 'L',
        95,
        '2', --Long-term debt issued during fiscal year
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT), -- Amount of LongTermDebt acquired
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL 
  Left Join GL GL_Begin on GL.accountingString = GL_Begin.accountingString
                        and GL_Begin.fiscalPeriod = 'Year Begin'
where GL.fiscalPeriod = 'Year End' 
  and GL.institParentChild = 'P'  
  and (GL.liabCurrentLongtermDebt = 'Y' 
    or GL.liabNoncurrentLongtermDebt = 'Y')
  and  GL.endBalance > 0
  and (GL_Begin.chartOfAccountsId is null 
     or NVL(GL_Begin.beginBalance,0) <= 0)
union

-- Line 03 - Long-term debt retired during fiscal year 

--jdh 2020-03-30 Janet & JD discussed: 
--  Decision was made to identify Long-Term debt retired as 
--  Any long-term Accounts that had a balance greater than 0 at the begining of FY
--  and had a balance of 0 at the end of the FY.  

select 'L',
        96,
        '3', --Long-term debt retired during fiscal year
        CAST(NVL(ABS(ROUND(SUM(GL.beginBalance))), 0) as BIGINT), -- Amount of LongTermDebt retired
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL 
Left Join GL GL_End on GL.accountingString = GL_End.accountingString
                        and GL_End.fiscalPeriod = 'Year End'
where GL.fiscalPeriod = 'Year Begin' 
  and GL.institParentChild = 'P'  
  and (GL.liabCurrentLongtermDebt = 'Y' 
    or GL.liabNoncurrentLongtermDebt = 'Y')
  and  GL.beginBalance > 0
    and (GL_End.chartOfAccountsId is null 
     or nvl(GL_End.endBalance,0) <= 0)

union

-- Line 04 - Long-term debt outstanding at END of fiscal year

select 'L',
        97,
        '4', --Long-term debt outstanding at END of fiscal year
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
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

select 'L',
        98,
        '5', --Short-term debt outstanding at beginning of fiscal year
        CAST(NVL(ABS(ROUND(SUM(GL.beginBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year Begin'
  and GL.institParentChild = 'P'
--jh 20200107 added parenthesis for or clause
 and (GL.liabCurrentOther = 'Y' 
   or GL.liabNoncurrentOther = 'Y') 

union

-- Line 06 - Short-term debt outstanding at END of fiscal year

select 'L',
        99,
        '6', --Short-term debt outstanding at END of fiscal year
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT)  ,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
--jh 20200107 added parenthesis for or clause
    and (GL.liabCurrentOther = 'Y' 
     or GL.liabNoncurrentOther = 'Y')

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
        100,
        '7', --Total cash and security assets held at END of fiscal year in sinking or debt service funds
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
  and GL.accountType = 'Asset'
  and GL.isSinkingOrDebtServFundGASB = 1 
  and GL.isCashOrSecurityAssetGASB = 1

union

-- Line 08 - Total cash and security assets held at END of fiscal year in bond funds

select 'L',
        101,
        '8', --Total cash and security assets held at END of fiscal year in bond funds
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
    and GL.accountType = 'Asset' 
    and GL.isBondFundGASB = 1 
    and GL.isCashOrSecurityAssetGASB = 1

union

-- Line 09 - Total cash and security assets held at END of fiscal year in all other funds

select 'L',
        102,
        '9', --Total cash and security assets held at END of fiscal year in all other funds
        CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) as BIGINT),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
from GL GL  
where GL.fiscalPeriod = 'Year End'
  and GL.institParentChild = 'P'
  and GL.accountType = 'Asset' 
  and GL.isNonBondFundGASB = 1 
  and GL.isCashOrSecurityAssetGASB = 1

--order by 2
