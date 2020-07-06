/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Spring Collection
FILE NAME:      Finance v4   (F1C)
FILE DESC:      Finance for non-degree-granting private, not-for-profit institutions and public institutions using FASB Reporting Standards 
AUTHOR:         Janet Hanicak / JD Hysler
CREATED:        20191220

SECTIONS:
     Reporting Dates 
	 Most Recent Records 
     Survey Formatting  
    
SUMMARY OF CHANGES

Date(yyyymmdd)      Author              Tag              Comments
-----------------   ----------------    -------------    --------------------------------------------------------------------------------
20200622			akhasawneh			ak 20200622		 Modify Finance report query with standardized view naming/aliasing convention (PF-1532) -Run time 5m 23s
20200412			jhanicak			jh 20204012		 Added dummy date option for recordActivityDate in most current record queries PF-1374
														 Added DefaultValues query and rewrote other queries to use PF-1418
														 Removed all 'prior' queries - not needed
20200304            jd.hysler           jdh 2020-03-04   PF-1297 Modify Finance all versions to use latest records from finance entities 
                                                         - Removed FYPerAsOfDate/FYPerPriorAsOfDate
                                                         - Removed BothCOASPerFYAsOfDate/BothCOASPerFYPriorAsOfDate
                                                         - Move IPEDSClientConfig values into COASPerFYAsOfDate/COASPerFYPriorAsOfDate
                                                         - Added GL cte for most recent record views for GeneralLedgerReporting
                                                         - Added GL_Prior cte for most recent record views for GeneralLedgerReporting for prior fiscal year
                                                         - Added oppledger cte for most recent record views for OperatingLedgerReporting
                                                         - Added OL_Prior cte for most recent record views for OperatingLedgerReporting for prior fiscal year
                                                         - Removed cross join with ClientConfigMCR since values are now already in COASPerFYAsOfDate
                                                         - Changed PART Queries to use new in-line GL/oppledger Structure
                                                                --  1-Changed GeneralLedgerReporting to GL inline view  
                                                                --  2-Removed the join to COAS       
                                                                --  3-Removed GL.isIpedsReportable = 1 
                                                                --  4-move fiscalPeriod from join to where clause 
20200218	    	jhanicak			jh 20200218		 PF-1254 Added default values for IPEDSReportingPeriod and IPEDSClientConfig
														 Added Config values/conditionals for Parts M
20200108            akhasawneh          		         Move original code to template format
20191220            jhanicak                  			 Initial version

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
	'F1C' surveyId,
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
	'F1C' surveyId,
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
	repValues.finPensionBenefits finPensionBenefits
from (
	select NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.currentSection) THEN repPeriodENT.asOfDate END, defvalues.asOfDate) asOfDate,
		NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.priorSection) THEN repPeriodENT.asOfDate END, defvalues.priorAsOfDate) priorAsOfDate,
		repPeriodENT.surveyCollectionYear surveyYear,
		repPeriodENT.surveyId surveyId,
		defvalues.currentSection currentSection,
		defvalues.priorSection priorSection,
		defvalues.finGPFSAuditOpinion finGPFSAuditOpinion,
		defvalues.finPensionBenefits finPensionBenefits,
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
	repValues.finPensionBenefits

union

select defvalues.surveyYear surveyYear,
    defvalues.surveyId surveyId,
    defvalues.asOfDate asOfDate,
    defvalues.priorAsOfDate priorAsOfDate,
    defvalues.currentSection currentSection,
	defvalues.priorSection priorSection,
    defvalues.finGPFSAuditOpinion finGPFSAuditOpinion,
	defvalues.finPensionBenefits finPensionBenefits
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

ClientConfigMCR AS
--Pulls client-given data for part 9, including:
--finGPFSAuditOpinion - all versions
--finAthleticExpenses - all versions
--finEndowmentAssets - v1, v2 
--finPensionBenefits - all versions
--finReportingModel - v1
--finPellTransactions - v2, v3, v5, v6
--finBusinessStructure - v3, v6
--finTaxExpensePaid - v6
--finParentOrChildInstitution - v1, v2, v3

(
select surveyCollectionYear surveyYear,
	asOfDate asOfDate,
	priorAsOfDate priorAsOfDate,
	finGPFSAuditOpinion finGPFSAuditOpinion,
	finPensionBenefits finPensionBenefits
from ( 
	select repperiod.surveyYear surveyCollectionYear,
		repperiod.asOfDate asOfDate,
		repperiod.priorAsOfDate priorAsOfDate,
		NVL(clientConfigENT.finGPFSAuditOpinion, repperiod.finGPFSAuditOpinion) finGPFSAuditOpinion,
		NVL(clientConfigENT.finPensionBenefits, repperiod.finPensionBenefits) finPensionBenefits,
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
	repperiod.finPensionBenefits finPensionBenefits
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
where coasRn = 1
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

--jdh 2020-03-04 Added GL most recent record views for GeneralLedgerReporting

GeneralLedgerMCR AS (
select *
from (
    select genledgerENT.*,
		fiscalyr.fiscalPeriod fiscalPeriod,
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
    Part C: Scholarships and Fellowships
    Part D: Revenues and Investment Return
    Part E: Expenses by Functional and Natural Classification

*****/

-- 9  
-- General Information 

select DISTINCT '9' part,
	0 sort,
	CAST(MONTH(NVL(fiscalyr.startDate, fiscalyr.priorAsOfDate))  as BIGINT) field1,
	CAST(YEAR(NVL(fiscalyr.startDate, fiscalyr.priorAsOfDate)) as BIGINT) field2,
	CAST(MONTH(NVL(fiscalyr.endDate, fiscalyr.asOfDate)) as BIGINT) field3,
	CAST(YEAR(NVL(fiscalyr.endDate, fiscalyr.asOfDate)) as BIGINT) field4,
	fiscalyr.finGPFSAuditOpinion field5, --1=Unqualified, 2=Qualified, 3=Don't know
	NULL field6,
	NULL field7
from FiscalYearMCR fiscalyr 

union

-- E  
-- Scholarships and Fellowships 
/* Part E is intended to report expenses by function. 
   All expenses recognized in the GPFS should be reported using the expense functions provided on 
   lines 01-06, 10, and 11. These functional categories are consistent with Chapter 5 
   (Accounting for Independent Colleges and Universities) of the NACUBO FARM. (FARM para 504)
   Although for-profit institutions are not required to report expenses by functions in their GPFS, 
   please report expenses by functional categories using your underlying accounting records. 
   Expenses should be assigned to functional categories by direct identification with a function, 
   wherever possible. When direct assignment to functional categories is not possible, an allocation 
   is appropriate. Objective methods of allocating expense are preferable to subjective methods and 
   may be based on financial or nonfinancial data
*/

select 'E',
	1,
	'1', --Pell grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFAPellGrant  = 'Y'

union

select 'E',
	2,
	'2', --Other Federal grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFANonPellFedGrants = 'Y'

union

select 'E',
	3,
	'3'  , --Grants by state government
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFAStateGrants = 'Y'

union

select 'E',
	4,
	'4' , --Grants by local government
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFALocalGrants = 'Y'

union

select 'E',
	5 ,
	'5' , --Institutional grants from restricted resources
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFAInstitGrantsRestr = 'Y'

union

select 'E',
	7 ,
	'7', -- Total revenue that funds scholarships and fellowships
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.expFAPellGrant  = 'Y' 			    --E1 Pell grants
		or oppledger.expFANonPellFedGrants = 'Y'		  --E2 Other federal grants
		or oppledger.expFAStateGrants = 'Y' 			    --E3 Grants by state government
		or oppledger.expFALocalGrants = 'Y'   		    --E4 Grants by local government
		or oppledger.expFAInstitGrantsRestr = 'Y'  	--E5 Institutional grants from restricted resources
		or oppledger.expFAInstitGrantsUnrestr = 'Y' ) 	--E6 Institutional grants from unrestricted resources  

union

select 'E',
	8,
	'8' , --Discounts and allowances applied to tuition and fees
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.discAllowTuitionFees = 'Y'

union

select 'E',
	9 ,
	'9' , -- Discounts and allowances applied to sales and services of auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
   and oppledger.discAllowAuxEnterprise = 'Y'

union

-- B  
-- Revenues and Other Additions 

select 'B',
	12 ,
	'1' , --Tuition and fees (after deducting discounts and allowances)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revTuitionAndFees = 'Y' THEN oppledger.endBalance ELSE 0 END)
		- SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and (oppledger.revTuitionAndFees = 'Y'
	    or oppledger.discAllowTuitionFees = 'Y')

union

select 'B',
	13 ,
	'2'  , --Federal operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revFedGrantsContractsOper = 'Y'

union

select 'B',
	14,
	'3', --State operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateGrantsContractsOper = 'Y'

union

select 'B',
	16,
	'4a' , --Local government operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalGrantsContractsOper = 'Y'

union

select 'B',
	17 ,
	'4b' , --Private operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGrantsContractsOper = 'Y'

union

select 'B',
	18 ,
	'26', --Sales & services of educational activities
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT) ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revEducActivSalesServices = 'Y'

union

--might need to revisit this

select 'B',
	20,
	'9' , --Total operating revenues
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.revTuitionAndFees = 'Y'          			--B1 Tuition and fees (after deducting discounts and allowances)
										or oppledger.revFedGrantsContractsOper = 'Y'      	--B2 Federal operating grants and contracts
										or oppledger.revStateGrantsContractsOper = 'Y'   	--B3 State operating grants and contracts
										or oppledger.revLocalGrantsContractsOper = 'Y'   	--B4a Local government operating grants and contracts
										or oppledger.revPrivGrantsContractsOper = 'Y'    	--B5b Private operating grants and contracts
										or oppledger.revAuxEnterprSalesServices = 'Y'   	--B5 Sales and services of auxiliary enterprises (after deducting discounts and allowances)
										or oppledger.revHospitalSalesServices = 'Y'     	--B6 Sales and services of hospitals (after deducting patient contractual allowances)
										or oppledger.revEducActivSalesServices = 'Y'       --B26 Sales & services of educational activities
										or oppledger.revOtherSalesServices = 'Y'
										or oppledger.revIndependentOperations = 'Y'        --B7 Independent operations
										or oppledger.revOtherOper = 'Y')                   --B8 Other sources - operating
                                THEN oppledger.endBalance ELSE 0 END)
		-   SUM(CASE WHEN (oppledger.discAllowTuitionFees = 'Y'           	   --B1 Tuition and fees discounts and allowances
							or oppledger.discAllowAuxEnterprise = 'Y'         --B5 auxiliary enterprises discounts and allowances
							or oppledger.discAllowPatientContract = 'Y')     --B6 patient contractual allowances
					 THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
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
	21,
	'10' , --Federal appropriations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT) ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.revFedApproprations = 'Y'

union

select 'B',
	22,
	'11' , --State appropriations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateApproprations = 'Y'

union

select 'B',
	23,
	'12' , --Local appropriations, education district taxes, and similar support
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.revLocalApproprations = 'Y' 
		or oppledger.revLocalTaxApproprations = 'Y')

union

select 'B',
	25,
	'13' , --Federal nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
  and oppledger.revFedGrantsContractsNOper = 'Y'

union

select 'B',
	26 ,
	'14' , --State nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
  and oppledger.revStateGrantsContractsNOper = 'Y'

union

select 'B',
	27,
	'15', --Local government nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
  and oppledger.revLocalGrantsContractsNOper = 'Y'

union

select 'B',
	28,
	'16', --Gifts, including contributions from affiliated organizations
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
  and (oppledger.revPrivGifts = 'Y' 
	or oppledger.revAffiliatedOrgnGifts = 'Y')

union

select 'B',
	29,
	'17' , --Investment income
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerReporting oppledger
	inner join FiscalYearMCR fiscalyr ON oppledger.chartOfAccountsId = fiscalyr.chartOfAccountsId
		and oppledger.fiscalYear2Char = fiscalyr.fiscalYear2Char
		and oppledger.fiscalPeriodCode = fiscalyr.fiscalPeriodCode
		and fiscalyr.fiscalPeriod = 'Year End'
where oppledger.isIPEDSReportable = 1
	and oppledger.revInvestmentIncome = 'Y'
	
union

select 'B',
	31,
	'19' , --Total nonoperating revenues
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.revFedApproprations = 'Y'             --B10 Federal appropriations
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
	55,
	'25', --Total all revenues and other additions
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Revenue' THEN oppledger.endBalance ELSE 0 END)
		-  SUM(CASE WHEN oppledger.accountType = 'Revenue Discount' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue'
		or oppledger.accountType = 'Revenue Discount')

union

-- C
-- Expenses by Functional Classification
/* Report Total Operating AND Nonoperating Expenses in this section 
   Functional Classification */

select 'C',
	37,
	'1', -- Instruction
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT) , --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19))
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isInstruction = 1

union

select 'C',
	38,
	'2', --Research
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isResearch = 1

union

select 'C',
	39,
	'3', --Public service
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isPublicService = 1

union

select 'C',
	40,
	'5', --Academic support
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN  oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isAcademicSupport = 1

union

select 'C',
	41,
	'6', --Student services
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isStudentServices = 1

union

select 'C',
	42,
	'7', --Institutional support
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isInstitutionalSupport = 1

union

select 'C',
	45,
	'19', --Total expenses and deductions
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Expense' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.accountType = 'Expense' and oppledger.expSalariesWages = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19) 19_2
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expBenefits = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Benefits 19_3
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expOperMaintSalariesWages = 'Y'
										or oppledger.expOperMaintBenefits = 'Y'
										or oppledger.expOperMaintOther = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Oper and Maint of Plant 19_4
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expDepreciation = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Depreciation 19_5
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expInterest = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT)  --Interest 19
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Expense'
		or  oppledger.expBenefits = 'Y'
		or  oppledger.expOperMaintSalariesWages = 'Y'
		or  oppledger.expOperMaintBenefits = 'Y'
		or  oppledger.expOperMaintOther = 'Y'
		or  oppledger.expDepreciation = 'Y'
		or  oppledger.expInterest = 'Y')
union

--jh 20200218 added conditional for IPEDSClientConfig.finPensionBenefits

-- M
-- Pension and Postemployment Benefits other than Pension Information

-- Line 01 - Pension expense

select 'M',
	55,
	'1' , 
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
	and oppledger.accountType = 'Expense'
	and oppledger.isPensionGASB = 1

union

-- Line 02 - Net Pension liability


select 'M',
	56,
	'2' , 
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
	and genledger.accountType = 'Liability'
	and genledger.isPensionGASB = 1

union

-- Line 03 - Deferred inflows (an acquisition of net assets) related to pension

select 'M',
	57,
	'3' ,
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
	and genledger.accountType = 'Liability'
	and genledger.deferredInflow = 'Y'
	and genledger.isPensionGASB = 1

union

--Line 04 - Deferred outflows(a consumption of net assets) related to pension

select 'M',
	58,
	'4' ,
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
	and genledger.accountType = 'Asset'
	and genledger.deferredOutflow = 'Y'
	and genledger.isPensionGASB = 1

union

-- Line 5 - OPEB Expense

select 'M',
	59,
	'5' ,
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
	and oppledger.accountType = 'Expense'
	and oppledger.isOPEBRelatedGASB = 1

union

-- Line 6 - Net OPEB Liability

select 'M',
	60,
	'6' ,
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
	and genledger.accountType = 'Liability'
	and genledger.isOPEBRelatedGASB = 1

union

-- Line 7 - Deferred inflow related to OPEB

select 'M',
	61,
	'7' ,
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
	and genledger.accountType = 'Liability'
	and genledger.deferredInflow = 'Y'
	and genledger.isOPEBRelatedGASB = 1

union

-- Line 8 - Deferred outflow related to OPEB

select 'M',
	62,
	'8' , 
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
	and genledger.accountType = 'Asset'
	and genledger.deferredOutflow = 'Y'
	and genledger.isOPEBRelatedGASB = 1

-- J 
-- Revenue Data for the Census Bureau
/* Report data for the same fiscal year as reported in parts A through E.
   Report gross amounts but exclude interfund transfers. Include the transactions
   of all funds of your institution.
   These instructions conform to the U. S. Census Bureau-s Government Finance and
   Employment Classification Manual. This manual can be viewed on the Internet at
   http://www2.census.gov/govs/pubs/classification/2006_classification_manual.pdf
*/

union

-- Line 1 -- Tuition and fees    
/* (Do not include in import file. Will be calculated)
   All amounts will be obtained from Parts B and E. The Census Bureau includes tuition and fees 
   from part B and excludes discounts and allowances (applied to tuition) from Part E. */

-- Line 2 - Sales and services

select 'J',
	64,
	'2',   
	NULL,  --    Totals Calculated by IPEDS  -- (Do not include in the file)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 
										and oppledger.isAuxiliaryEnterprises = 0 
										and oppledger.isHospitalServices = 0) 
										and (oppledger.revEducActivSalesServices = 'Y' 
											or oppledger.revOtherSalesServices = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations 2-7
	NULL, -- J2,3 Aux Enterprises
	NULL, -- J2,4 Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and oppledger.revOtherSalesServices = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and (((oppledger.isAgricultureOrExperiment = 0 
		and oppledger.isAuxiliaryEnterprises = 0 
		and oppledger.isHospitalServices = 0) 
		and (oppledger.revEducActivSalesServices = 'Y' 
			or oppledger.revOtherSalesServices = 'Y'))
		or (oppledger.isAgricultureOrExperiment = 1 
			and oppledger.revOtherSalesServices = 'Y'))
			
union

-- Line 03 - Federal grants/contracts (excludes Pell Grants)
/* Include both operating and non-operating grants, but exclude Pell and other student 
   grants and any Federal loans received on behalf of the students. Include all other 
   direct Federal grants, including research grants, in the appropriate column. */

select 'J',
	65,
	'3',
	NULL,  --Totals Calculated by IPEDS -- (Do not include in export file)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and (oppledger.revFedGrantsContractsOper = 'Y' or oppledger.revFedGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 and (oppledger.revFedGrantsContractsOper = 'Y' or oppledger.revFedGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), -- J3,3 Aux Enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 and (oppledger.revFedGrantsContractsOper = 'Y' 
										or oppledger.revFedGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), -- J3,4 Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and (oppledger.revFedGrantsContractsOper = 'Y' or oppledger.revFedGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services 2-7
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
			and (oppledger.revFedGrantsContractsOper = 'Y' or oppledger.revFedGrantsContractsNOper = 'Y'))
		or (oppledger.isAuxiliaryEnterprises = 1 
			and (oppledger.revFedGrantsContractsOper = 'Y' or oppledger.revFedGrantsContractsNOper = 'Y'))
		or (oppledger.isHospitalServices = 1 
			and (oppledger.revFedGrantsContractsOper = 'Y' or oppledger.revFedGrantsContractsNOper = 'Y'))
		or (oppledger.isAgricultureOrExperiment = 1 
			and (oppledger.revFedGrantsContractsOper = 'Y' or oppledger.revFedGrantsContractsNOper = 'Y')))

union

-- Line 04 - State appropriations, current and capital
-- Include all operating and non-operating appropriations, as well as all current and capital appropriations. 

select 'J',
	66,
	'4', 
	NULL, --Total amount  -- (Do not include in export file)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and (oppledger.revStateApproprations = 'Y' or oppledger.revStateCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 and (oppledger.revStateApproprations = 'Y' or oppledger.revStateCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and (oppledger.revStateApproprations = 'Y' or oppledger.revStateCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and (oppledger.revStateApproprations = 'Y' or oppledger.revStateCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (((oppledger.isAgricultureOrExperiment = 0 
		and oppledger.isAuxiliaryEnterprises = 0 
		and oppledger.isHospitalServices = 0) 
		and (oppledger.revStateApproprations = 'Y' 
			or oppledger.revStateCapitalAppropriations = 'Y'))
		or (oppledger.isAuxiliaryEnterprises = 1 
			and (oppledger.revStateApproprations = 'Y' 
				or oppledger.revStateCapitalAppropriations = 'Y'))
		or (oppledger.isHospitalServices = 1 
			and (oppledger.revStateApproprations = 'Y' or oppledger.revStateCapitalAppropriations = 'Y'))
		or (oppledger.isAgricultureOrExperiment = 1 
			and (oppledger.revStateApproprations = 'Y' or oppledger.revStateCapitalAppropriations = 'Y'))
		)

union

-- Line 05 - State grants and contracts
-- Include state grants and contracts, both operating and non-operating, in the proper column. Do not include state student grant aid.*/

select 'J',
	67 ,
	'5', 
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and (oppledger.revStateGrantsContractsOper = 'Y' or oppledger.revStateGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 and (oppledger.revStateGrantsContractsOper = 'Y' or oppledger.revStateGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and (oppledger.revStateGrantsContractsOper = 'Y' or oppledger.revStateGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and (oppledger.revStateGrantsContractsOper = 'Y' or oppledger.revStateGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Agriculture extension/experiment services
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
		and (oppledger.revStateGrantsContractsOper = 'Y' or oppledger.revStateGrantsContractsNOper = 'Y'))
		or (oppledger.isAuxiliaryEnterprises = 1 
			and (oppledger.revStateGrantsContractsOper = 'Y' or oppledger.revStateGrantsContractsNOper = 'Y'))
		or (oppledger.isHospitalServices = 1 
			and (oppledger.revStateGrantsContractsOper = 'Y' or oppledger.revStateGrantsContractsNOper = 'Y'))
		or (oppledger.isAgricultureOrExperiment = 1 
			and (oppledger.revStateGrantsContractsOper = 'Y' or oppledger.revStateGrantsContractsNOper = 'Y')))

union

-- Line 06 - Local appropriations, current and capital
/* Include local government appropriations in the appropriate column, regardless of whether
   appropriations were for current or capital. This generally applies only to local institutions of higher education. */

select 'J',
	68,
	'6', 
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and (oppledger.revLocalApproprations = 'Y' or oppledger.revLocalCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
										and (oppledger.revLocalApproprations = 'Y' or oppledger.revLocalCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and (oppledger.revLocalApproprations = 'Y' or oppledger.revLocalCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and (oppledger.revLocalApproprations = 'Y' or oppledger.revLocalCapitalAppropriations = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
		and (oppledger.revLocalApproprations = 'Y' or oppledger.revLocalCapitalAppropriations = 'Y'))
		or (oppledger.isAuxiliaryEnterprises = 1 and (oppledger.revLocalApproprations = 'Y' or oppledger.revLocalCapitalAppropriations = 'Y'))
		or (oppledger.isHospitalServices = 1 
			and (oppledger.revLocalApproprations = 'Y' or oppledger.revLocalCapitalAppropriations = 'Y'))
		or (oppledger.isAgricultureOrExperiment = 1 
			and (oppledger.revLocalApproprations = 'Y' or oppledger.revLocalCapitalAppropriations = 'Y')))

union

select 'J',
	69 ,
	'7',  --Local grants and contracts
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and (oppledger.revLocalGrantsContractsOper = 'Y' or oppledger.revLocalGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
										and (oppledger.revLocalGrantsContractsOper = 'Y' or oppledger.revLocalGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and (oppledger.revLocalGrantsContractsOper = 'Y' or oppledger.revLocalGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and (oppledger.revLocalGrantsContractsOper = 'Y' or oppledger.revLocalGrantsContractsNOper = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
		and (oppledger.revLocalGrantsContractsOper = 'Y' or oppledger.revLocalGrantsContractsNOper = 'Y'))
		or (oppledger.isAuxiliaryEnterprises = 1 
			and (oppledger.revLocalGrantsContractsOper = 'Y' or oppledger.revLocalGrantsContractsNOper = 'Y'))
		or (oppledger.isHospitalServices = 1 
			and (oppledger.revLocalGrantsContractsOper = 'Y' or oppledger.revLocalGrantsContractsNOper = 'Y'))
		or (oppledger.isAgricultureOrExperiment = 1 
			and (oppledger.revLocalGrantsContractsOper = 'Y' or oppledger.revLocalGrantsContractsNOper = 'Y')))

union

-- Line 08 - Receipts from property and non-property taxes - Total all funds
/* This item applies only to local institutions of higher education. Include in column 1 any revenue 
   from locally imposed property taxes orother taxes levied by the local higher education district. 
   Include all funds - current, restricted, unrestricted and debt service.
   Exclude taxes levied by another government and transferred to the local higher education district 
   by the levying government.  */

select 'J',
	70,
	'8',
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT) , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPropAndNonPropTaxes = 'Y'

union

-- Line 09 - Gifts and private grants, NOT including capital grants
/* Include grants from private organizations and individuals here. Include additions to
   permanent endowments if they are gifts. Exclude gifts to component units 
   and capital contributions. */

select 'J',
	71,
	'9', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT) , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.revPrivGrantsContractsOper = 'Y'
		or oppledger.revPrivGrantsContractsNOper = 'Y'
		or oppledger.revPrivGifts = 'Y'
		or oppledger.revAffiliatedOrgnGifts = 'Y')

union

-- Line 10 - Interest earnings
-- Report the total interest earned in column 1. Include all funds and endowments.  

select 'J',
	72 ,
	'10',
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revInterestEarnings = 'Y'

union

-- Line 11 - Dividend earnings
/* Dividends should be reported separately if available. Report only the total, in column 1,
	 from all funds including endowments but excluding dividends of any component units.
   Note: if dividends are not separately available, please report include with Interest earnings 
   in J10, column 1. */

select 'J',
	73,
	'11', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT) , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revDividendEarnings = 'Y'

union

-- Line 12 - Realized capital gains
/* Report only the total earnings. Do not include unrealized gains.
   Also, include all other miscellaneous revenue. Use column 1 only.*/

select 'J',
	74 ,
	'12', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revRealizedCapitalGains = 'Y'

union

-- K 
-- Expenditures for Bureau of the Census 

-- Line 02 - Employee benefits, total
/* Report the employee benefits for staff associated with Education and General, Auxiliary Enterprises,
	 Hospitals, and for Agricultural extension/experiment services, if applicable.*/

select 'K',
	75,
	'2', 
	NULL, --Total amount 8
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and (oppledger.expBenefits = 'Y' or oppledger.expOperMaintBenefits = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
										and (oppledger.expBenefits = 'Y' or oppledger.expOperMaintBenefits = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
										and (oppledger.expBenefits = 'Y' or oppledger.expOperMaintBenefits = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) K24, --Hospitals 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
										and (oppledger.expBenefits = 'Y' or oppledger.expOperMaintBenefits = 'Y')) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services 2-7
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
		and (oppledger.expBenefits = 'Y' or oppledger.expOperMaintBenefits = 'Y'))
			or (oppledger.isAuxiliaryEnterprises = 1 
				and (oppledger.expBenefits = 'Y' or oppledger.expOperMaintBenefits = 'Y'))
			or (oppledger.isHospitalServices = 1 
				and (oppledger.expBenefits = 'Y' or oppledger.expOperMaintBenefits = 'Y'))
			or (oppledger.isAgricultureOrExperiment = 1 
				and (oppledger.expBenefits = 'Y' or oppledger.expOperMaintBenefits = 'Y')))

union

-- Line 08 - Payment to state retirement funds
/* Applies to state institutions only. Include amounts paid to retirement systems operated
   by your state government only. 
   Include employer contributions only. 
   Exclude employee contributions withheld. */

select 'K',
	76,
	'3', 
	NULL, --Total amount 8
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and oppledger.accountType = 'Expense' and oppledger.isStateRetireFundGASB = 1) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 and oppledger.accountType = 'Expense' and oppledger.isStateRetireFundGASB = 1) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 and oppledger.accountType = 'Expense' and oppledger.isStateRetireFundGASB = 1) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 and oppledger.accountType = 'Expense' and oppledger.isStateRetireFundGASB = 1) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Agriculture extension/experiment services
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
		and oppledger.accountType = 'Expense' and oppledger.isStateRetireFundGASB = 1)
			or (oppledger.isAuxiliaryEnterprises = 1 and oppledger.accountType = 'Expense' and oppledger.isStateRetireFundGASB = 1)
			or (oppledger.isHospitalServices = 1 and oppledger.accountType = 'Expense' and oppledger.isStateRetireFundGASB = 1)
			or (oppledger.isAgricultureOrExperiment = 1 and oppledger.accountType = 'Expense' and oppledger.isStateRetireFundGASB = 1))

union

-- Line 04 - Current expenditures including salaries
/*  Report all current expenditures including salaries, employee benefits, supplies, materials, 
    contracts and professional services, utilities, travel, and insurance.  
    Exclude scholarships and fellowships, capital outlay, interest (report on line 8), 
    employer contributions to state retirement systems (applies to state institutions only) 
    and depreciation. */

select 'K',
	77,
	'4',
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and ((oppledger.expSalariesWages = 'Y' or oppledger.expOperMaintSalariesWages = 'Y' or oppledger.expOperMaintOther = 'Y' or oppledger.expOther = 'Y') 
											and oppledger.isStateRetireFundGASB = 1)) 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Education and general/independent operations
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 
											and ((oppledger.expSalariesWages = 'Y' or oppledger.expOperMaintSalariesWages = 'Y' or oppledger.expOperMaintOther = 'Y' or oppledger.expOther = 'Y') 
												and oppledger.isStateRetireFundGASB = 1)) 
									THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 
											and ((oppledger.expSalariesWages = 'Y' or oppledger.expOperMaintSalariesWages = 'Y' or oppledger.expOperMaintOther = 'Y' or oppledger.expOther = 'Y') 
												and oppledger.isStateRetireFundGASB = 1)) 
									THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Hospitals
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 
											and ((oppledger.expSalariesWages = 'Y' or oppledger.expOperMaintSalariesWages = 'Y' or oppledger.expOperMaintOther = 'Y' or oppledger.expOther = 'Y') 
												and oppledger.isStateRetireFundGASB = 1)) 
									THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
        NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
		and ((oppledger.expSalariesWages = 'Y' or oppledger.expOperMaintSalariesWages = 'Y' or oppledger.expOperMaintOther = 'Y' or oppledger.expOther = 'Y') 
			and oppledger.isStateRetireFundGASB = 1))
	 or (oppledger.isAuxiliaryEnterprises = 1 
		and ((oppledger.expSalariesWages = 'Y' or oppledger.expOperMaintSalariesWages = 'Y' or oppledger.expOperMaintOther = 'Y' or oppledger.expOther = 'Y') 
			and oppledger.isStateRetireFundGASB = 1))
	 or (oppledger.isHospitalServices = 1 
		and ((oppledger.expSalariesWages = 'Y' or oppledger.expOperMaintSalariesWages = 'Y' or oppledger.expOperMaintOther = 'Y' or oppledger.expOther = 'Y') 
			and oppledger.isStateRetireFundGASB = 1))
	 or (oppledger.isAgricultureOrExperiment = 1 
		and ((oppledger.expSalariesWages = 'Y' or oppledger.expOperMaintSalariesWages = 'Y' or oppledger.expOperMaintOther = 'Y' or oppledger.expOther = 'Y') 
			and oppledger.isStateRetireFundGASB = 1)))

union

-- Line 05 - Capital outlay, construction
/* Construction from all funds (plant, capital, or bond funds) includes expenditure for the construction 
   of	new structures and other permanent improvements, additions replacements, and major alterations.
   Report in proper column according to function. */

select 'K',
	78,
	'5', 
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and oppledger.expCapitalConstruction = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 and oppledger.expCapitalConstruction = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 and oppledger.expCapitalConstruction = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Hospitals 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 and oppledger.expCapitalConstruction = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Agriculture extension/experiment services 2-7
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
		and oppledger.expCapitalConstruction = 'Y')
			or (oppledger.isAuxiliaryEnterprises = 1 and oppledger.expCapitalConstruction = 'Y')
			or (oppledger.isHospitalServices = 1 and oppledger.expCapitalConstruction = 'Y')
			or (oppledger.isAgricultureOrExperiment = 1 and oppledger.expCapitalConstruction = 'Y'))

union

-- Line 06 - Capital outlay, equipment purchases
-- Equipment purchases from all funds (plant, capital, or bond funds). 

select 'K',
	79,
	'6',
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and oppledger.expCapitalEquipPurch = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 and oppledger.expCapitalEquipPurch = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 and oppledger.expCapitalEquipPurch = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 and oppledger.expCapitalEquipPurch = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Agriculture extension/experiment services
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
   and (((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
        and oppledger.expCapitalEquipPurch = 'Y')
		or (oppledger.isAuxiliaryEnterprises = 1 and oppledger.expCapitalEquipPurch = 'Y')
		or (oppledger.isHospitalServices = 1 and oppledger.expCapitalEquipPurch = 'Y')
		or (oppledger.isAgricultureOrExperiment = 1 and oppledger.expCapitalEquipPurch = 'Y'))

union

-- Line 07 - Capital outlay, land purchases
/* from all funds (plant, capital, or bond funds), include the cost of land and existing structures,
	 as well as the purchase of rights-of-way. Include all capital outlay other than Construction if
	 not specified elsewhere. */

select 'K',
	80,
	'7',
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
										and oppledger.expCapitalLandPurchOther = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAuxiliaryEnterprises = 1 and oppledger.expCapitalLandPurchOther = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isHospitalServices = 1 and oppledger.expCapitalLandPurchOther = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT) , --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.isAgricultureOrExperiment = 1 and oppledger.expCapitalLandPurchOther = 'Y') 
								THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (((oppledger.isAgricultureOrExperiment = 0 and oppledger.isAuxiliaryEnterprises = 0 and oppledger.isHospitalServices = 0) 
		and oppledger.expCapitalLandPurchOther = 'Y')
			or (oppledger.isAuxiliaryEnterprises = 1 and oppledger.expCapitalLandPurchOther = 'Y')
			or (oppledger.isHospitalServices = 1 and oppledger.expCapitalLandPurchOther = 'Y')
			or (oppledger.isAgricultureOrExperiment = 1 and oppledger.expCapitalLandPurchOther = 'Y'))

union

-- Line 08 - Interest on debt outstanding, all funds and activities
/* Interest paid on revenue debt only. Includes interest on debt issued by the institution, such as that which
   is repayable from pledged earnings, charges or gees (e.g. dormitory, stadium, or student union revenue bonds).
   Report only the total, in column 1. Excludes interest expenditure of the parent state or local government
   on debt issued on behalf of the institution and backed by that parent government. Also excludes interest on
   debt issued by a state dormitory or housing finance agency on behalf of the institution. */

select 'K',
	81,
	'8', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --Total amount
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expInterest = 'Y'

union

-- L
-- Debt and Assets for Census Bureau 

-- Lines 01 through 06 -
/* Include all debt issued in the name of the institution. Long-term debt and short-term debt 
   are distinguished by length of term for repayment, with one year being the boundary. 
   Short-term debt must be interest bearing. Do not include the current portion of long-term 
   debt as short-term debt. Instead include this in the total long-term debt outstanding.*/

-- Line 1 - Long-term debt outstanding at beginning of fiscal year

select 'L',
	82,
	'1' ,
	CAST(NVL(ABS(ROUND(SUM(genledger.beginBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
where genledger.fiscalPeriod = 'Year Begin'
	and (genledger.liabCurrentLongtermDebt = 'Y' 
		or genledger.liabNoncurrentLongtermDebt = 'Y')

union

--this section needs to be revisited

-- Line 2 - Long-term debt issued during fiscal year

select 'L',
	83,
	'2', 
	PriorLongTerm.LongTermDebt - CurrLongTerm.LongTermDebt,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from ( 
	select CAST(NVL(ABS(ROUND(SUM(CASE WHEN genledger.liabCurrentLongtermDebt = 'Y' or genledger.liabNoncurrentLongtermDebt = 'Y' 
                                       THEN genledger.endBalance ELSE 0 END))), 0) AS BIGINT) LongTermDebt
	from GeneralLedgerMCR genledger 
	where genledger.fiscalPeriod = 'Year End') CurrLongTerm
        cross join (
				select CAST(NVL(ABS(ROUND(SUM(CASE WHEN genledger.liabCurrentLongtermDebt = 'Y' or genledger.liabNoncurrentLongtermDebt = 'Y' 
												   THEN genledger.beginBalance ELSE 0 END))), 0) AS BIGINT) LongTermDebt
				from GeneralLedgerMCR genledger 
				where genledger.fiscalPeriod = 'Year Begin') PriorLongTerm

union

--this section needs to be revisited

-- Line 3 - Long-term debt retired during fiscal year

select 'L',
	84,
	'3',
	CurrLongTerm.LongTermDebt - PriorLongTerm.LongTermDebt ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from (
	select CAST(NVL(ABS(ROUND(SUM(CASE WHEN genledger.liabCurrentLongtermDebt = 'Y' or genledger.liabNoncurrentLongtermDebt = 'Y' 
									   THEN genledger.beginBalance ELSE 0 END))), 0) AS BIGINT) LongTermDebt
	from GeneralLedgerMCR genledger 
	where genledger.fiscalPeriod = 'Year Begin') CurrLongTerm
        cross join (
			select CAST(NVL(ABS(ROUND(SUM(CASE WHEN genledger.liabCurrentLongtermDebt = 'Y' or genledger.liabNoncurrentLongtermDebt = 'Y' 
											   THEN genledger.beginBalance ELSE 0 END))), 0) AS BIGINT) LongTermDebt
			from GeneralLedgerMCR genledger 
			where genledger.fiscalPeriod = 'Year Begin') PriorLongTerm

union

-- Line 4 - Long-term debt outstanding at END of fiscal year

select 'L',
	85,
	'4',
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
where fiscalPeriod = 'Year End'
	and (genledger.liabCurrentLongtermDebt = 'Y' 
		or genledger.liabNoncurrentLongtermDebt = 'Y')

union

-- Line 5 - Short-term debt outstanding at beginning of fiscal year

select 'L',
	86,
	'5',
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
where genledger.fiscalPeriod = 'Year Begin'
	and (genledger.liabCurrentOther = 'Y' 
		or genledger.liabNoncurrentOther = 'Y')

union

-- Line 6 - Short-term debt outstanding at END of fiscal year

select 'L',
	87,
	'6',
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
where genledger.fiscalPeriod = 'Year End'
	and (genledger.liabCurrentOther = 'Y' 
		or genledger.liabNoncurrentOther = 'Y')

union 

-- Lines 07, 08, and 09 :
/* Report the total amount of cash and security assets held in each category.   
   Report assets at book value to the extent possible. Includes cash on hand in each type of fund. 
   Sinking funds are those used exclusively	to service debt. Bond funds are those established by your 
   institution to disburse revenue bond proceeds. All other funds might include current, plant, 
   or endowment funds. Exclude the value of fixed assets andexclude any student loan funds 
   established by the Federal government.
*/

-- Line 7 - Total cash and security assets held at END of fiscal year in sinking or debt service funds

select 'L',
	88,
	'7', 
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
where genledger.fiscalPeriod = 'Year End'
	and genledger.accountType = 'Asset'
	and genledger.isSinkingOrDebtServFundGASB = 1
	and genledger.isCashOrSecurityAssetGASB = 1

union

-- Line 8 - Total cash and security assets held at END of fiscal year in bond funds

select 'L',
	89,
	'8', 
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
where genledger.fiscalPeriod = 'Year End'
	and genledger.accountType = 'Asset'
	and genledger.isBondFundGASB = 1
	and genledger.isCashOrSecurityAssetGASB = 1

union

-- Line 9 - Total cash and security assets held at END of fiscal year in all other funds

select 'L',
	90,
	'9', 
	CAST(NVL(ABS(ROUND(SUM(genledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GeneralLedgerMCR genledger 
where genledger.fiscalPeriod = 'Year End'
	and genledger.accountType = 'Asset'
	and genledger.isNonBondFundGASB = 1
	and genledger.isCashOrSecurityAssetGASB = 1
 
--Order by 2
