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
ReportingPeriodMCR AS
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
    (select NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.currentSection) THEN repPeriodENT.asOfDate END, defvalues.asOfDate) asOfDate,
		NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.priorSection) THEN repPeriodENT.asOfDate END, defvalues.priorAsOfDate) priorAsOfDate,
		repPeriodENT.surveyCollectionYear surveyYear,
		repPeriodENT.surveyId surveyId,
		defvalues.currentSection currentSection,
		defvalues.priorSection priorSection,
		defvalues.finGPFSAuditOpinion finGPFSAuditOpinion,
		defvalues.finParentOrChildInstitution finParentOrChildInstitution,
		defvalues.finReportingModel finReportingModel,
		defvalues.finAthleticExpenses finAthleticExpenses,
		defvalues.finPensionBenefits finPensionBenefits,
		defvalues.finEndowmentAssets finEndowmentAssets,
		ROW_NUMBER() OVER (
			PARTITION BY
				repPeriodENT.surveyCollectionYear,
				repPeriodENT.surveyId,
				repPeriodENT.surveySection
			ORDER BY
				repPeriodENT.recordActivityDate DESC
		) AS repperiodRn
	from DefaultValues defvalues
		cross join IPEDSReportingPeriod repPeriodENT
	where repPeriodENT.surveyCollectionYear = defvalues.surveyYear
			and repPeriodENT.surveyId = defvalues.surveyId
	) repValues 
where repValues.repperiodRn = 1
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

select defvalues.surveyYear surveyYear,
    defvalues.surveyId surveyId,
    defvalues.asOfDate asOfDate,
    defvalues.priorAsOfDate priorAsOfDate,
    defvalues.currentSection currentSection,
	defvalues.priorSection priorSection,
    defvalues.finGPFSAuditOpinion finGPFSAuditOpinion,
	defvalues.finParentOrChildInstitution finParentOrChildInstitution,
	defvalues.finReportingModel finReportingModel,
	defvalues.finAthleticExpenses finAthleticExpenses,
	defvalues.finPensionBenefits finPensionBenefits,
	defvalues.finEndowmentAssets finEndowmentAssets
from DefaultValues defvalues
where defvalues.surveyYear not in (select surveyYear
                                    from DefaultValues defvalues1
                                        cross join IPEDSReportingPeriod repPeriodENT
                                    where repPeriodENT.surveyCollectionYear = defvalues1.surveyYear
                                        and repPeriodENT.surveyId = defvalues1.surveyId)
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
	finReportingModel finReportingModel,
	finParentOrChildInstitution finParentOrChildInstitution
from ( 
	select repperiod.surveyYear surveyCollectionYear,
		repperiod.asOfDate asOfDate,
		repperiod.priorAsOfDate priorAsOfDate,
		NVL(clientConfigENT.finGPFSAuditOpinion, repperiod.finGPFSAuditOpinion) finGPFSAuditOpinion,
		NVL(clientConfigENT.finAthleticExpenses, repperiod.finAthleticExpenses) finAthleticExpenses,
		NVL(clientConfigENT.finEndowmentAssets, repperiod.finEndowmentAssets) finEndowmentAssets,
		NVL(clientConfigENT.finPensionBenefits, repperiod.finPensionBenefits) finPensionBenefits,
		NVL(clientConfigENT.finReportingModel, repperiod.finReportingModel) finReportingModel,
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
	repperiod.finReportingModel finReportingModel,
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
where coasRn = 1
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
    case when coas.isParent = 1 then 'P'
		when coas.isChild = 1 then 'C' end COASParentChild
from (
	select fiscalyearENT.*,
		clientconfig.asOfDate asOfDate,
		clientconfig.priorAsOfDate priorAsOfDate,
		clientconfig.finGPFSAuditOpinion finGPFSAuditOpinion,
		clientconfig.finAthleticExpenses finAthleticExpenses,
		clientconfig.finReportingModel finReportingModel,
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
		) AS fiscalyearRn
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
where FYData.fiscalyearRn = 1
),

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
		) AS OLRn
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
	CAST(MONTH(NVL(coas.startDate, coas.priorAsOfDate))  as BIGINT) field1,
	CAST(YEAR(NVL(coas.startDate, coas.priorAsOfDate)) as BIGINT) field2,
	CAST(MONTH(NVL(coas.endDate, coas.asOfDate)) as BIGINT) field3,
	CAST(YEAR(NVL(coas.endDate, coas.asOfDate)) as BIGINT) field4,
	coas.finGPFSAuditOpinion field5,  --1=Unqualified, 2=Qualified, 3=Don't know
	coas.finReportingModel   field6,  --1=Business Type Activities 2=Governmental Activities 3=Governmental Activities with Business-Type Activities
	coas.finAthleticExpenses field7   --1=Auxiliary enterprises 2=Student services 3=Does not participate in intercollegiate athletics 4=Other (specify in caveats box below)
from FiscalYearMCR coas 

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
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
    and genledger.institParentChild = 'P'
    and genledger.accountType = 'Asset' 
    and genledger.isRestrictedNonExpendOrPerm = 1 
 
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
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriod = 'Year End'
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
	NULL
from GeneralLedgerMCR genledger    
where genledger.fiscalPeriod = 'Year End'
   and genledger.institParentChild = genledger.COASParentChild
   and genledger.assetCapitalOther = 'Y'
     
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
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger  
where oppledger.fiscalPeriod = 'Year End' 
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
        NULL
from OperatingLedgerMCR oppledger  
where oppledger.fiscalPeriod = 'Year End' 
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
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year Begin' 
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	33,
	'1', --Tuition and fees (after deducting discounts and allowances)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revTuitionAndFees = 'Y' THEN oppledger.endBalance ELSE 0 END) 
					 - SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revTuitionAndFees = 'Y' 
		or oppledger.discAllowTuitionFees = 'Y')

union

select 'B',
	34,
	'2', --Federal operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revFedGrantsContractsOper = 'Y'

union

select 'B',
	35,
	'3', --State operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revStateGrantsContractsOper = 'Y'

union

select 'B',
	36,
	'4a', --Local government operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revLocalGrantsContractsOper = 'Y'

union

select 'B',
	37,
	'4b', --Private operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revPrivGrantsContractsOper = 'Y'

union

select 'B',
	38,
	'5', --Sales and services of auxiliary enterprises (after deducting discounts and allowances)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revAuxEnterprSalesServices = 'Y' THEN oppledger.endBalance ELSE 0 END) 
					 - SUM(CASE WHEN oppledger.discAllowAuxEnterprise = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
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

select 'B',
	39,
	'6', --Sales and services of hospitals (after deducting patient contractual allowances)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revHospitalSalesServices = 'Y' THEN oppledger.endBalance ELSE 0 END) 
					- SUM(CASE WHEN oppledger.discAllowPatientContract = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revHospitalSalesServices = 'Y'
		or oppledger.discAllowPatientContract = 'Y')

union

select 'B',
	40,
	'26', --Sales & services of educational activities
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revEducActivSalesServices = 'Y'

union

select 'B',
	41,
	'7', --Independent operations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revIndependentOperations = 'Y'

union

-- Line 09 - Total Operating Revenues - 
-- Report total operating revenues from your GPFS.

select 'B',
	42,
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	43,
	'10', --Federal appropriations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revFedApproprations = 'Y'

union

select 'B',
	44,
	'11', --State appropriations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revStateApproprations = 'Y'

union

select 'B',
	45,
	'12', --Local appropriations, education district taxes, and similar support
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revLocalApproprations = 'Y' 
		or oppledger.revLocalTaxApproprations = 'Y')
   
union

select 'B',
	46,
	'13', --Federal nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revFedGrantsContractsNOper = 'Y'

union

select 'B',
	47,
	'14', --State nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revStateGrantsContractsNOper = 'Y'

union

select 'B',
	48,
	'15', --Local government nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revLocalGrantsContractsNOper = 'Y'

union

select 'B',
	49,
	'16', --Gifts, including contributions from affiliated organizations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revPrivGifts = 'Y' 
		or oppledger.revAffiliatedOrgnGifts = 'Y')

union

select 'B',
	50,
	'17', --Investment income
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revInvestmentIncome = 'Y'

union

select 'B',
	51,
	'19', --Total nonoperating revenues
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	52,
	'20', --Capital appropriations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revStateCapitalAppropriations = 'Y' 
		or oppledger.revLocalCapitalAppropriations = 'Y')

union

select 'B',
	53,
	'21', --Capital grants and gifts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revCapitalGrantsGifts = 'Y'

union

select 'B',
	54,
	'22', --Additions to permanent endowments
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revAddToPermEndowments = 'Y'

union

select 'B',
	55,
	'25', --Total all revenues and other additions
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Revenue' THEN oppledger.endBalance ELSE 0 END) -
					   SUM(CASE WHEN oppledger.accountType = 'Revenue Discount' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	56,
	'1', --Instruction
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19))
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense'

union

select 'C',
	57,
	'2', --Research
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
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

select 'C',
	58,
	'3', --Public service
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
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

select 'C',
	59,
	'5', --Academic support
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
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

select 'C',
	60,
	'6', --Student services
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
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

select 'C',
	61,
	'7', --Institutional support
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y'  THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
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

select 'C',
	62,
	'11', --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isAuxiliaryEnterprises = 1

union

select 'C',
	63,
	'12', --Hospital services
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)  , --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isHospitalServices = 1

union

select 'C',
	64,
	'13', --Independent operations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y'  THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.accountType = 'Expense' 
	and oppledger.isIndependentOperations = 1

union

select 'C',
	65,
	'19', --Total expenses and deductions
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Expense' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Expense' and oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Salaries and wages (1-7,11-13,19) 19_2
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expBenefits = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Benefits 19_3
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expOperMaintSalariesWages = 'Y' or oppledger.expOperMaintBenefits = 'Y' or oppledger.expOperMaintOther = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Oper and Maint of Plant 19_4
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expDepreciation = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT), --Depreciation 19_5
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expInterest = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) as BIGINT)   --Interest 19
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
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
	66,
	'1', --Pension expense
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) 
		else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = 'P'
	and oppledger.accountType = 'Expense' 
	and oppledger.isPensionGASB = 1

union

select 'M',
	67,
	'2', --Net Pension liability
	CAST(case when (select clientconfig.finPensionBenefits
		from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		else 0
			end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL											
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Liability' 
	and genledger.isPensionGASB = 1

union

select 'M',
	68,
	'3', --Deferred inflows (an acquisition of net assets) related to pension
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Liability' 
	and genledger.deferredInflow = 'Y' 
	and genledger.isPensionGASB = 1

union

select 'M',
	69,
	'4', --Deferred outflows(a consumption of net assets) related to pension
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset' 
	and genledger.deferredOutflow = 'Y' 
	and genledger.isPensionGASB = 1
    
union

select 'M',
	70,
	'5', --OPEB Expense
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = 'P'
	and oppledger.accountType = 'Expense' 
	and oppledger.isOPEBRelatedGASB = 1 

union

select 'M',
	71,
	'6', --Net OPEB Liability
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Liability' 
	and genledger.isOPEBRelatedGASB = 1

union

select 'M',
	72,
	'7', --Deferred inflow related to OPEB
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Liability' 
	and genledger.deferredInflow = 'Y' 
	and genledger.isOPEBRelatedGASB = 1

union

select 'M',
	73,
	'8', --Deferred outflow related to OPEB
	CAST(case when (select clientconfig.finPensionBenefits
					from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
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
	74,
	'1', --Value of endowment assets at the beginning of the fiscal year
	CAST(case when (select clientconfig.finEndowmentAssets
			from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.beginBalance))), 0) 
				else 0
					end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year Begin'
	and genledger.institParentChild = genledger.COASParentChild
	and genledger.accountType = 'Asset' 
	and genledger.isEndowment = 1

union

select 'H',
	75,
	'2', --Value of endowment assets at the END of the fiscal year
	CAST(case when (select clientconfig.finEndowmentAssets
		from ClientConfigMCR clientconfig) = 'Y' then NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) 
		 else 0
	end as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = genledger.COASParentChild
	and genledger.accountType = 'Asset' 
	and genledger.isEndowment = 1
	 
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
        NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	77,
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	78,
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	79,
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
	NULL   
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	80,
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
	NULL  
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	81,
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	82,
	'8', --Receipts from property and non-property taxes - Total all funds
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL, 
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and oppledger.revPropAndNonPropTaxes = 'Y'

union

-- Line 09 - Gifts and private grants, NOT including capital grants 
/*  Include grants from private organizations and individuals here. Include additions to
    permanent endowments if they are gifts. Exclude gifts to component units and capital contributions.   
*/
 
select 'J',
	83,
	'9', --Gifts and private grants, NOT including capital grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild
	and (oppledger.revPrivGrantsContractsOper = 'Y' 
		or oppledger.revPrivGrantsContractsNOper = 'Y' 
		or oppledger.revPrivGifts = 'Y' 
		or oppledger.revAffiliatedOrgnGifts = 'Y')

union

-- Line 10 - Interest earnings
-- Report the total interest earned in column 1. Include all funds and endowments.  

select 'J',
	84,
	'10', --Interest earnings
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild 
	and oppledger.revInterestEarnings = 'Y'

union

-- Line 11 - Dividend earnings
/*  Dividends should be reported separately if available. Report only the total, in column 1,
    from all funds including endowments but excluding dividends of any component units. Note: if
    dividends are not separately available, please report include with Interest earnings in J10, column 1. 
*/

select 'J',
	85,
	'11', --Dividend earnings
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.institParentChild = oppledger.COASParentChild  
	and oppledger.revDividendEarnings = 'Y'

union

-- Line 12 - Realized capital gains
/*  Report only the total earnings. Do not include unrealized gains.
    Also, include all other miscellaneous revenue. Use column 1 only. 
*/

select 'J',
	86,
	'12', --Realized capital gains
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT), --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	87,
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	88,
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	89,
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	90,
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	91,
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
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
        92,
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
        NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	93,
	'8', --Interest on debt outstanding, all funds and activities
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) as BIGINT)  , --Total amount
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	94,
	'1', --Long-term debt outstanding at beginning of fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.beginBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year Begin'
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
	95,
	'2', --Long-term debt issued during fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT), -- Amount of LongTermDebt acquired
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
	left Join GeneralLedgerMCR genledger1 on genledger.accountingString = genledger1.accountingString
		and genledger1.fiscalPeriod = 'Year Begin'
where genledger.fiscalPeriod = 'Year End' 
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
	96,
	'3', --Long-term debt retired during fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.beginBalance))), 0) as BIGINT), -- Amount of LongTermDebt retired
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
left Join GeneralLedgerMCR genledger1 on genledger.accountingString = genledger1.accountingString
	and genledger1.fiscalPeriod = 'Year End'
where genledger.fiscalPeriod = 'Year Begin' 
	and genledger.institParentChild = 'P'  
	and (genledger.liabCurrentLongtermDebt = 'Y' 
		or genledger.liabNoncurrentLongtermDebt = 'Y')
	and  genledger.beginBalance > 0
    and (genledger1.chartOfAccountsId is null 
		or NVL(genledger1.endBalance,0) <= 0)

union

-- Line 04 - Long-term debt outstanding at END of fiscal year

select 'L',
	97,
	'4', --Long-term debt outstanding at END of fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
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

select 'L',
	98,
	'5', --Short-term debt outstanding at beginning of fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.beginBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year Begin'
	and genledger.institParentChild = 'P'
--jh 20200107 added parenthesis for or clause
	and (genledger.liabCurrentOther = 'Y' 
		or genledger.liabNoncurrentOther = 'Y') 

union

-- Line 06 - Short-term debt outstanding at END of fiscal year

select 'L',
	99,
	'6', --Short-term debt outstanding at END of fiscal year
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT)  ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
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
	100,
	'7', --Total cash and security assets held at END of fiscal year in sinking or debt service funds
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset'
	and genledger.isSinkingOrDebtServFundGASB = 1 
	and genledger.isCashOrSecurityAssetGASB = 1

union

-- Line 08 - Total cash and security assets held at END of fiscal year in bond funds

select 'L',
	101,
	'8', --Total cash and security assets held at END of fiscal year in bond funds
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset' 
	and genledger.isBondFundGASB = 1 
	and genledger.isCashOrSecurityAssetGASB = 1

union

-- Line 09 - Total cash and security assets held at END of fiscal year in all other funds

select 'L',
	102,
	'9', --Total cash and security assets held at END of fiscal year in all other funds
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) as BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger  
where genledger.fiscalPeriod = 'Year End'
	and genledger.institParentChild = 'P'
	and genledger.accountType = 'Asset' 
	and genledger.isNonBondFundGASB = 1 
	and genledger.isCashOrSecurityAssetGASB = 1

--order by 2
