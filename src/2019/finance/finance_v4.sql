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
20200412			jhanicak			jh 20204012		 Added dummy date option for recordActivityDate in most current record queries PF-1374
														 Added DefaultValues query and rewrote other queries to use PF-1418
														 Removed all 'prior' queries - not needed
20200304            jd.hysler           jdh 2020-03-04   PF-1297 Modify Finance all versions to use latest records from finance entities 
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
ReportingDates AS
(
select repValues.surveyYear surveyYear,
       repValues.surveyId surveyId,
       MAX(repValues.asOfDate) asOfDate,
       MAX(repValues.priorAsOfDate) priorAsOfDate,
       repValues.currentSection currentSection,
	   repValues.priorSection priorSection,
       repValues.finGPFSAuditOpinion finGPFSAuditOpinion,
	   repValues.finPensionBenefits finPensionBenefits
from 
    (select nvl(CASE WHEN upper(reportPeriod.surveySection) = upper(defaultValues.currentSection) THEN reportPeriod.asOfDate END, defaultValues.asOfDate) asOfDate,
			nvl(CASE WHEN upper(reportPeriod.surveySection) = upper(defaultValues.priorSection) THEN reportPeriod.asOfDate END, defaultValues.priorAsOfDate) priorAsOfDate,
            reportPeriod.surveyCollectionYear surveyYear,
            reportPeriod.surveyId surveyId,
            defaultValues.currentSection currentSection,
		    defaultValues.priorSection priorSection,
            defaultValues.finGPFSAuditOpinion finGPFSAuditOpinion,
	        defaultValues.finPensionBenefits finPensionBenefits,
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
	    repValues.finPensionBenefits

union

select defaultValues.surveyYear surveyYear,
    defaultValues.surveyId surveyId,
    defaultValues.asOfDate asOfDate,
    defaultValues.priorAsOfDate priorAsOfDate,
    defaultValues.currentSection currentSection,
	defaultValues.priorSection priorSection,
    defaultValues.finGPFSAuditOpinion finGPFSAuditOpinion,
	defaultValues.finPensionBenefits finPensionBenefits
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

ConfigPerAsOfDate AS
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
	select ReportingDates.surveyYear surveyCollectionYear,
	            ReportingDates.asOfDate asOfDate,
                ReportingDates.priorAsOfDate priorAsOfDate,
                nvl(config.finGPFSAuditOpinion, ReportingDates.finGPFSAuditOpinion) finGPFSAuditOpinion,
				nvl(config.finPensionBenefits, ReportingDates.finPensionBenefits) finPensionBenefits,
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
	  ReportingDates.finPensionBenefits finPensionBenefits
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
    case when COAS.isParent = 1 then 'P'
             when COAS.isChild = 1 then 'C' end COASParentChild
from (select FY.*,
                config.asOfDate asOfDate,
                config.priorAsOfDate priorAsOfDate,
                config.finGPFSAuditOpinion finGPFSAuditOpinion,
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

GL AS (
select *
from (
    select GL.*,
		COAS.fiscalPeriod fiscalPeriod,
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
    Part C: Scholarships and Fellowships
    Part D: Revenues and Investment Return
    Part E: Expenses by Functional and Natural Classification

*****/

-- 9  
-- General Information 

select DISTINCT '9' part,
	            0 sort,
                CAST(MONTH(nvl(COAS.startDate, COAS.priorAsOfDate))  as BIGINT) field1,
                CAST(YEAR(nvl(COAS.startDate, COAS.priorAsOfDate)) as BIGINT) field2,
                CAST(MONTH(nvl(COAS.endDate, COAS.asOfDate)) as BIGINT) field3,
                CAST(YEAR(nvl(COAS.endDate, COAS.asOfDate)) as BIGINT) field4,
	            COAS.finGPFSAuditOpinion field5, --1=Unqualified, 2=Qualified, 3=Don't know
	            NULL field6,
	            NULL field7
from COASPerFYAsOfDate COAS 

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
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.expFAPellGrant  = 'Y'

union

select 'E',
	2,
	'2', --Other Federal grants
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.expFANonPellFedGrants = 'Y'

union

select 'E',
	3,
	'3'  , --Grants by state government
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.expFAStateGrants = 'Y'

union

select 'E',
	4,
	'4' , --Grants by local government
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.expFALocalGrants = 'Y'

union

select 'E',
	5 ,
	'5' , --Institutional grants from restricted resources
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.expFAInstitGrantsRestr = 'Y'

union

select 'E',
	7 ,
	'7', -- Total revenue that funds scholarships and fellowships
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (OL.expFAPellGrant  = 'Y' 			    --E1 Pell grants
		or OL.expFANonPellFedGrants = 'Y'		  --E2 Other federal grants
		or OL.expFAStateGrants = 'Y' 			    --E3 Grants by state government
		or OL.expFALocalGrants = 'Y'   		    --E4 Grants by local government
		or OL.expFAInstitGrantsRestr = 'Y'  	--E5 Institutional grants from restricted resources
		or OL.expFAInstitGrantsUnrestr = 'Y' ) 	--E6 Institutional grants from unrestricted resources  

union

select 'E',
	8,
	'8' , --Discounts and allowances applied to tuition and fees
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
    and OL.discAllowTuitionFees = 'Y'

union

select 'E',
	9 ,
	'9' , -- Discounts and allowances applied to sales and services of auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
   and OL.discAllowAuxEnterprise = 'Y'

union

-- B  
-- Revenues and Other Additions 

select 'B',
	12 ,
	'1' , --Tuition and fees (after deducting discounts and allowances)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.revTuitionAndFees = 'Y' THEN OL.endBalance ELSE 0 END)
		- SUM(CASE WHEN OL.discAllowTuitionFees = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
    and (OL.revTuitionAndFees = 'Y'
	    or OL.discAllowTuitionFees = 'Y')

union

select 'B',
	13 ,
	'2'  , --Federal operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.revFedGrantsContractsOper = 'Y'

union

select 'B',
	14,
	'3', --State operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.revStateGrantsContractsOper = 'Y'

union

select 'B',
	16,
	'4a' , --Local government operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.revLocalGrantsContractsOper = 'Y'

union

select 'B',
	17 ,
	'4b' , --Private operating grants and contracts
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.revPrivGrantsContractsOper = 'Y'

union

select 'B',
	18 ,
	'26', --Sales & services of educational activities
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT) ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.revEducActivSalesServices = 'Y'

union

--might need to revisit this

select 'B',
	20,
	'9' , --Total operating revenues
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.revTuitionAndFees = 'Y'          			--B1 Tuition and fees (after deducting discounts and allowances)
										or OL.revFedGrantsContractsOper = 'Y'      	--B2 Federal operating grants and contracts
										or OL.revStateGrantsContractsOper = 'Y'   	--B3 State operating grants and contracts
										or OL.revLocalGrantsContractsOper = 'Y'   	--B4a Local government operating grants and contracts
										or OL.revPrivGrantsContractsOper = 'Y'    	--B5b Private operating grants and contracts
										or OL.revAuxEnterprSalesServices = 'Y'   	--B5 Sales and services of auxiliary enterprises (after deducting discounts and allowances)
										or OL.revHospitalSalesServices = 'Y'     	--B6 Sales and services of hospitals (after deducting patient contractual allowances)
										or OL.revEducActivSalesServices = 'Y'       --B26 Sales & services of educational activities
										or OL.revOtherSalesServices = 'Y'
										or OL.revIndependentOperations = 'Y'        --B7 Independent operations
										or OL.revOtherOper = 'Y')                   --B8 Other sources - operating
                                THEN OL.endBalance ELSE 0 END)
		-   SUM(CASE WHEN (OL.discAllowTuitionFees = 'Y'           	   --B1 Tuition and fees discounts and allowances
							or OL.discAllowAuxEnterprise = 'Y'         --B5 auxiliary enterprises discounts and allowances
							or OL.discAllowPatientContract = 'Y')     --B6 patient contractual allowances
					 THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
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
	21,
	'10' , --Federal appropriations
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT) ,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
    and OL.revFedApproprations = 'Y'

union

select 'B',
	22,
	'11' , --State appropriations
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.revStateApproprations = 'Y'

union

select 'B',
	23,
	'12' , --Local appropriations, education district taxes, and similar support
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (OL.revLocalApproprations = 'Y' 
		or OL.revLocalTaxApproprations = 'Y')

union

select 'B',
	25,
	'13' , --Federal nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.revFedGrantsContractsNOper = 'Y'

union

select 'B',
	26 ,
	'14' , --State nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.revStateGrantsContractsNOper = 'Y'

union

select 'B',
	27,
	'15', --Local government nonoperating grants
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.revLocalGrantsContractsNOper = 'Y'

union

select 'B',
	28,
	'16', --Gifts, including contributions from affiliated organizations
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and (OL.revPrivGifts = 'Y' 
	or OL.revAffiliatedOrgnGifts = 'Y')

union

select 'B',
	29,
	'17' , --Investment income
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerReporting OL
	inner join COASPerFYAsOfDate COAS ON OL.chartOfAccountsId = COAS.chartOfAccountsId
		and OL.fiscalYear2Char = COAS.fiscalYear2Char
		and OL.fiscalPeriodCode = COAS.fiscalPeriodCode
		and COAS.fiscalPeriod = 'Year End'
where OL.isIPEDSReportable = 1
	and OL.revInvestmentIncome = 'Y'
	
union

select 'B',
	31,
	'19' , --Total nonoperating revenues
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and (OL.revFedApproprations = 'Y'             --B10 Federal appropriations
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
	55,
	'25', --Total all revenues and other additions
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.accountType = 'Revenue' THEN OL.endBalance ELSE 0 END)
		-  SUM(CASE WHEN OL.accountType = 'Revenue Discount' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and (OL.accountType = 'Revenue'
	or OL.accountType = 'Revenue Discount')

union

-- C
-- Expenses by Functional Classification
/* Report Total Operating AND Nonoperating Expenses in this section 
   Functional Classification */

select 'C',
	37,
	'1', -- Instruction
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT) , --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19))
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
  and OL.accountType = 'Expense'
  and OL.isInstruction = 1

union

select 'C',
	38,
	'2', --Research
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.accountType = 'Expense'
	and OL.isResearch = 1

union

select 'C',
	39,
	'3', --Public service
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.accountType = 'Expense'
	and OL.isPublicService = 1

union

select 'C',
	40,
	'5', --Academic support
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN  OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.accountType = 'Expense'
	and OL.isAcademicSupport = 1

union

select 'C',
	41,
	'6', --Student services
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.accountType = 'Expense'
	and OL.isStudentServices = 1

union

select 'C',
	42,
	'7', --Institutional support
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19)
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.accountType = 'Expense'
	and OL.isInstitutionalSupport = 1

union

select 'C',
	45,
	'19', --Total expenses and deductions
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.accountType = 'Expense' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Total amount (1-7,11-13,19)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.accountType = 'Expense' and OL.expSalariesWages = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Salaries and wages (1-7,11-13,19) 19_2
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expBenefits = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Benefits 19_3
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expOperMaintSalariesWages = 'Y'
										or OL.expOperMaintBenefits = 'Y'
										or OL.expOperMaintOther = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Oper and Maint of Plant 19_4
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expDepreciation = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Depreciation 19_5
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN OL.expInterest = 'Y' THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT)  --Interest 19
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (OL.accountType = 'Expense'
		or  OL.expBenefits = 'Y'
		or  OL.expOperMaintSalariesWages = 'Y'
		or  OL.expOperMaintBenefits = 'Y'
		or  OL.expOperMaintOther = 'Y'
		or  OL.expDepreciation = 'Y'
		or  OL.expInterest = 'Y')
union

--jh 20200218 added conditional for IPEDSClientConfig.finPensionBenefits

-- M
-- Pension and Postemployment Benefits other than Pension Information

-- Line 01 - Pension expense

select 'M',
	55,
	'1' , 
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
	and OL.accountType = 'Expense'
	and OL.isPensionGASB = 1

union

-- Line 02 - Net Pension liability


select 'M',
	56,
	'2' , 
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
	and GL.accountType = 'Liability'
	and GL.isPensionGASB = 1

union

-- Line 03 - Deferred inflows (an acquisition of net assets) related to pension

select 'M',
	57,
	'3' ,
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
	and GL.accountType = 'Liability'
	and GL.deferredInflow = 'Y'
	and GL.isPensionGASB = 1

union

--Line 04 - Deferred outflows(a consumption of net assets) related to pension

select 'M',
	58,
	'4' ,
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
	and GL.accountType = 'Asset'
	and GL.deferredOutflow = 'Y'
	and GL.isPensionGASB = 1

union

-- Line 5 - OPEB Expense

select 'M',
	59,
	'5' ,
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
	and OL.accountType = 'Expense'
	and OL.isOPEBRelatedGASB = 1

union

-- Line 6 - Net OPEB Liability

select 'M',
	60,
	'6' ,
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
	and GL.accountType = 'Liability'
	and GL.isOPEBRelatedGASB = 1

union

-- Line 7 - Deferred inflow related to OPEB

select 'M',
	61,
	'7' ,
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
	and GL.accountType = 'Liability'
	and GL.deferredInflow = 'Y'
	and GL.isOPEBRelatedGASB = 1

union

-- Line 8 - Deferred outflow related to OPEB

select 'M',
	62,
	'8' , 
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
	and GL.accountType = 'Asset'
	and GL.deferredOutflow = 'Y'
	and GL.isOPEBRelatedGASB = 1

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
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 
										and OL.isAuxiliaryEnterprises = 0 
										and OL.isHospitalServices = 0) 
										and (OL.revEducActivSalesServices = 'Y' 
											or OL.revOtherSalesServices = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations 2-7
	NULL, -- J2,3 Aux Enterprises
	NULL, -- J2,4 Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 
										and OL.revOtherSalesServices = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
    and (((OL.isAgricultureOrExperiment = 0 
		and OL.isAuxiliaryEnterprises = 0 
		and OL.isHospitalServices = 0) 
		and (OL.revEducActivSalesServices = 'Y' 
			or OL.revOtherSalesServices = 'Y'))
		or (OL.isAgricultureOrExperiment = 1 
			and OL.revOtherSalesServices = 'Y'))
			
union

-- Line 03 - Federal grants/contracts (excludes Pell Grants)
/* Include both operating and non-operating grants, but exclude Pell and other student 
   grants and any Federal loans received on behalf of the students. Include all other 
   direct Federal grants, including research grants, in the appropriate column. */

select 'J',
	65,
	'3',
	NULL,  --Totals Calculated by IPEDS -- (Do not include in export file)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), -- J3,3 Aux Enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and (OL.revFedGrantsContractsOper = 'Y' 
										or OL.revFedGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), -- J3,4 Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 
										and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services 2-7
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
			and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y'))
		or (OL.isAuxiliaryEnterprises = 1 
			and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y'))
		or (OL.isHospitalServices = 1 
			and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y'))
		or (OL.isAgricultureOrExperiment = 1 
			and (OL.revFedGrantsContractsOper = 'Y' or OL.revFedGrantsContractsNOper = 'Y')))

union

-- Line 04 - State appropriations, current and capital
-- Include all operating and non-operating appropriations, as well as all current and capital appropriations. 

select 'J',
	66,
	'4', 
	NULL, --Total amount  -- (Do not include in export file)
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and (OL.revStateApproprations = 'Y' or OL.revStateCapitalAppropriations = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and (OL.revStateApproprations = 'Y' or OL.revStateCapitalAppropriations = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 
										and (OL.revStateApproprations = 'Y' or OL.revStateCapitalAppropriations = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 
										and (OL.revStateApproprations = 'Y' or OL.revStateCapitalAppropriations = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (((OL.isAgricultureOrExperiment = 0 
		and OL.isAuxiliaryEnterprises = 0 
		and OL.isHospitalServices = 0) 
		and (OL.revStateApproprations = 'Y' 
			or OL.revStateCapitalAppropriations = 'Y'))
		or (OL.isAuxiliaryEnterprises = 1 
			and (OL.revStateApproprations = 'Y' 
				or OL.revStateCapitalAppropriations = 'Y'))
		or (OL.isHospitalServices = 1 
			and (OL.revStateApproprations = 'Y' or OL.revStateCapitalAppropriations = 'Y'))
		or (OL.isAgricultureOrExperiment = 1 
			and (OL.revStateApproprations = 'Y' or OL.revStateCapitalAppropriations = 'Y'))
		)

union

-- Line 05 - State grants and contracts
-- Include state grants and contracts, both operating and non-operating, in the proper column. Do not include state student grant aid.*/

select 'J',
	67 ,
	'5', 
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 
										and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 
										and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Agriculture extension/experiment services
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
		and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y'))
		or (OL.isAuxiliaryEnterprises = 1 
			and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y'))
		or (OL.isHospitalServices = 1 
			and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y'))
		or (OL.isAgricultureOrExperiment = 1 
			and (OL.revStateGrantsContractsOper = 'Y' or OL.revStateGrantsContractsNOper = 'Y')))

union

-- Line 06 - Local appropriations, current and capital
/* Include local government appropriations in the appropriate column, regardless of whether
   appropriations were for current or capital. This generally applies only to local institutions of higher education. */

select 'J',
	68,
	'6', 
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 
										and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 
										and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 
										and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
		and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y'))
		or (OL.isAuxiliaryEnterprises = 1 and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y'))
		or (OL.isHospitalServices = 1 
			and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y'))
		or (OL.isAgricultureOrExperiment = 1 
			and (OL.revLocalApproprations = 'Y' or OL.revLocalCapitalAppropriations = 'Y')))

union

select 'J',
	69 ,
	'7',  --Local grants and contracts
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 
										and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 
										and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 
										and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
		and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y'))
		or (OL.isAuxiliaryEnterprises = 1 
			and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y'))
		or (OL.isHospitalServices = 1 
			and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y'))
		or (OL.isAgricultureOrExperiment = 1 
			and (OL.revLocalGrantsContractsOper = 'Y' or OL.revLocalGrantsContractsNOper = 'Y')))

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
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT) , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.revPropAndNonPropTaxes = 'Y'

union

-- Line 09 - Gifts and private grants, NOT including capital grants
/* Include grants from private organizations and individuals here. Include additions to
   permanent endowments if they are gifts. Exclude gifts to component units 
   and capital contributions. */

select 'J',
	71,
	'9', 
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT) , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (OL.revPrivGrantsContractsOper = 'Y'
		or OL.revPrivGrantsContractsNOper = 'Y'
		or OL.revPrivGifts = 'Y'
		or OL.revAffiliatedOrgnGifts = 'Y')

union

-- Line 10 - Interest earnings
-- Report the total interest earned in column 1. Include all funds and endowments.  

select 'J',
	72 ,
	'10',
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT), --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.revInterestEarnings = 'Y'

union

-- Line 11 - Dividend earnings
/* Dividends should be reported separately if available. Report only the total, in column 1,
	 from all funds including endowments but excluding dividends of any component units.
   Note: if dividends are not separately available, please report include with Interest earnings 
   in J10, column 1. */

select 'J',
	73,
	'11', 
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT) , --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.revDividendEarnings = 'Y'

union

-- Line 12 - Realized capital gains
/* Report only the total earnings. Do not include unrealized gains.
   Also, include all other miscellaneous revenue. Use column 1 only.*/

select 'J',
	74 ,
	'12', 
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT), --Total amount 8-12
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.revRealizedCapitalGains = 'Y'

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
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 
										and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 
										and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) K24, --Hospitals 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 
										and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y')) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services 2-7
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
		and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y'))
			or (OL.isAuxiliaryEnterprises = 1 
				and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y'))
			or (OL.isHospitalServices = 1 
				and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y'))
			or (OL.isAgricultureOrExperiment = 1 
				and (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y')))

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
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Agriculture extension/experiment services
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
		and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1)
			or (OL.isAuxiliaryEnterprises = 1 and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1)
			or (OL.isHospitalServices = 1 and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1)
			or (OL.isAgricultureOrExperiment = 1 and OL.accountType = 'Expense' and OL.isStateRetireFundGASB = 1))

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
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') 
											and OL.isStateRetireFundGASB = 1)) 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Education and general/independent operations
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 
											and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') 
												and OL.isStateRetireFundGASB = 1)) 
									THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 
											and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') 
												and OL.isStateRetireFundGASB = 1)) 
									THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Hospitals
        CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 
											and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') 
												and OL.isStateRetireFundGASB = 1)) 
									THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
        NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
		and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') 
			and OL.isStateRetireFundGASB = 1))
	 or (OL.isAuxiliaryEnterprises = 1 
		and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') 
			and OL.isStateRetireFundGASB = 1))
	 or (OL.isHospitalServices = 1 
		and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') 
			and OL.isStateRetireFundGASB = 1))
	 or (OL.isAgricultureOrExperiment = 1 
		and ((OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y' or OL.expOperMaintOther = 'Y' or OL.expOther = 'Y') 
			and OL.isStateRetireFundGASB = 1)))

union

-- Line 05 - Capital outlay, construction
/* Construction from all funds (plant, capital, or bond funds) includes expenditure for the construction 
   of	new structures and other permanent improvements, additions replacements, and major alterations.
   Report in proper column according to function. */

select 'K',
	78,
	'5', 
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and OL.expCapitalConstruction = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and OL.expCapitalConstruction = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and OL.expCapitalConstruction = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Hospitals 2-7
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and OL.expCapitalConstruction = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Agriculture extension/experiment services 2-7
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
		and OL.expCapitalConstruction = 'Y')
			or (OL.isAuxiliaryEnterprises = 1 and OL.expCapitalConstruction = 'Y')
			or (OL.isHospitalServices = 1 and OL.expCapitalConstruction = 'Y')
			or (OL.isAgricultureOrExperiment = 1 and OL.expCapitalConstruction = 'Y'))

union

-- Line 06 - Capital outlay, equipment purchases
-- Equipment purchases from all funds (plant, capital, or bond funds). 

select 'K',
	79,
	'6',
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and OL.expCapitalEquipPurch = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and OL.expCapitalEquipPurch = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and OL.expCapitalEquipPurch = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and OL.expCapitalEquipPurch = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Agriculture extension/experiment services
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
   and (((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
        and OL.expCapitalEquipPurch = 'Y')
		or (OL.isAuxiliaryEnterprises = 1 and OL.expCapitalEquipPurch = 'Y')
		or (OL.isHospitalServices = 1 and OL.expCapitalEquipPurch = 'Y')
		or (OL.isAgricultureOrExperiment = 1 and OL.expCapitalEquipPurch = 'Y'))

union

-- Line 07 - Capital outlay, land purchases
/* from all funds (plant, capital, or bond funds), include the cost of land and existing structures,
	 as well as the purchase of rights-of-way. Include all capital outlay other than Construction if
	 not specified elsewhere. */

select 'K',
	80,
	'7',
	NULL, --Total amount
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN ((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
										and OL.expCapitalLandPurchOther = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Education and general/independent operations
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAuxiliaryEnterprises = 1 and OL.expCapitalLandPurchOther = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Auxiliary enterprises
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isHospitalServices = 1 and OL.expCapitalLandPurchOther = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT) , --Hospitals
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (OL.isAgricultureOrExperiment = 1 and OL.expCapitalLandPurchOther = 'Y') 
								THEN OL.endBalance ELSE 0 END))), 0) AS BIGINT), --Agriculture extension/experiment services
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and (((OL.isAgricultureOrExperiment = 0 and OL.isAuxiliaryEnterprises = 0 and OL.isHospitalServices = 0) 
		and OL.expCapitalLandPurchOther = 'Y')
			or (OL.isAuxiliaryEnterprises = 1 and OL.expCapitalLandPurchOther = 'Y')
			or (OL.isHospitalServices = 1 and OL.expCapitalLandPurchOther = 'Y')
			or (OL.isAgricultureOrExperiment = 1 and OL.expCapitalLandPurchOther = 'Y'))

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
	CAST(NVL(ABS(ROUND(SUM(OL.endBalance))), 0) AS BIGINT), --Total amount
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from OL OL 
where OL.fiscalPeriod = 'Year End'
	and OL.expInterest = 'Y'

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
	CAST(NVL(ABS(ROUND(SUM(GL.beginBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GL GL 
where GL.fiscalPeriod = 'Year Begin'
	and (GL.liabCurrentLongtermDebt = 'Y' 
		or GL.liabNoncurrentLongtermDebt = 'Y')

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
	select CAST(NVL(ABS(ROUND(SUM(CASE WHEN GL.liabCurrentLongtermDebt = 'Y' or GL.liabNoncurrentLongtermDebt = 'Y' 
                                       THEN GL.endBalance ELSE 0 END))), 0) AS BIGINT) LongTermDebt
	from GL GL 
	where GL.fiscalPeriod = 'Year End') CurrLongTerm
        cross join (
				select CAST(NVL(ABS(ROUND(SUM(CASE WHEN GL.liabCurrentLongtermDebt = 'Y' or GL.liabNoncurrentLongtermDebt = 'Y' 
												   THEN GL.beginBalance ELSE 0 END))), 0) AS BIGINT) LongTermDebt
				from GL GL 
				where GL.fiscalPeriod = 'Year Begin') PriorLongTerm

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
	select CAST(NVL(ABS(ROUND(SUM(CASE WHEN GL.liabCurrentLongtermDebt = 'Y' or GL.liabNoncurrentLongtermDebt = 'Y' 
									   THEN GL.beginBalance ELSE 0 END))), 0) AS BIGINT) LongTermDebt
	from GL GL 
	where GL.fiscalPeriod = 'Year Begin') CurrLongTerm
        cross join (
			select CAST(NVL(ABS(ROUND(SUM(CASE WHEN GL.liabCurrentLongtermDebt = 'Y' or GL.liabNoncurrentLongtermDebt = 'Y' 
											   THEN GL.beginBalance ELSE 0 END))), 0) AS BIGINT) LongTermDebt
			from GL GL 
			where GL.fiscalPeriod = 'Year Begin') PriorLongTerm

union

-- Line 4 - Long-term debt outstanding at END of fiscal year

select 'L',
	85,
	'4',
	CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GL GL 
where fiscalPeriod = 'Year End'
	and (GL.liabCurrentLongtermDebt = 'Y' 
		or GL.liabNoncurrentLongtermDebt = 'Y')

union

-- Line 5 - Short-term debt outstanding at beginning of fiscal year

select 'L',
	86,
	'5',
	CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GL GL 
where GL.fiscalPeriod = 'Year Begin'
	and (GL.liabCurrentOther = 'Y' 
		or GL.liabNoncurrentOther = 'Y')

union

-- Line 6 - Short-term debt outstanding at END of fiscal year

select 'L',
	87,
	'6',
	CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GL GL 
where GL.fiscalPeriod = 'Year End'
	and (GL.liabCurrentOther = 'Y' 
		or GL.liabNoncurrentOther = 'Y')

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
	CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GL GL 
where GL.fiscalPeriod = 'Year End'
	and GL.accountType = 'Asset'
	and GL.isSinkingOrDebtServFundGASB = 1
	and GL.isCashOrSecurityAssetGASB = 1

union

-- Line 8 - Total cash and security assets held at END of fiscal year in bond funds

select 'L',
	89,
	'8', 
	CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GL GL 
where GL.fiscalPeriod = 'Year End'
	and GL.accountType = 'Asset'
	and GL.isBondFundGASB = 1
	and GL.isCashOrSecurityAssetGASB = 1

union

-- Line 9 - Total cash and security assets held at END of fiscal year in all other funds

select 'L',
	90,
	'9', 
	CAST(NVL(ABS(ROUND(SUM(GL.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
from GL GL 
where GL.fiscalPeriod = 'Year End'
	and GL.accountType = 'Asset'
	and GL.isNonBondFundGASB = 1
	and GL.isCashOrSecurityAssetGASB = 1
 
--Order by 2
