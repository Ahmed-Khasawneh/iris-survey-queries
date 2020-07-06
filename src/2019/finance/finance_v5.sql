/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Spring Collection
FILE NAME:      Finance v5   (F2C)
FILE DESC:      Finance for non-degree-granting private, not-for-profit institutions and public institutions using FASB Reporting Standards 
AUTHOR:         Janet Hanicak / JD Hysler
CREATED:        20191220

SECTIONS:
    Reporting Dates 
    Most Recent Records 
    Survey Formatting  
    
SUMMARY OF CHANGES

Date(yyyymmdd)      Author              Tag              Comments
-----------------   ----------------    -------------   --------------------------------------------------------------------------------
20200622			akhasawneh			ak 20200622		 Modify Finance report query with standardized view naming/aliasing convention (PF-1532) -Run time 4m 57s
20200412			jhanicak			jh 20204012		 Added dummy date option for recordActivityDate in most current record queries PF-1374
														 Added DefaultValues query and rewrote other queries to use PF-1418
														 Removed all 'prior' queries - not needed
20200303            jd.hysler           jdh 2020-03-03   PF-1297 Modify Finance all versions to use latest records from finance entities 
                                                         - Removed FYPerAsOfDate/FYPerPriorAsOfDate
                                                         - Removed BothCOASPerFYAsOfDate/BothCOASPerFYPriorAsOfDate
                                                         - Move IPEDSClientConfig values into COASPerFYAsOfDate/COASPerFYPriorAsOfDate
                                                         - Added GL cte for most recent record views for GeneralLedgerReporting
                                                         - Added GL_Prior cte for most recent record views for GeneralLedgerReporting for prior fiscal year
                                                         - Added OL cte for most recent record views for OperatingLedgerReporting
                                                         - Added OL_Prior cte for most recent record views for OperatingLedgerReporting for prior fiscal year
                                                         - Removed cross join with ClientConfigMCR since values are now already in COASPerFYAsOfDate
                                                         - Changed PART Queries to use new in-line GL/OL Structure
                                                                --  1-Changed GeneralLedgerReporting to GL inline view  
                                                                --  2-Removed the join to COAS       
                                                                --  3-Removed GL.isIpedsReportable = 1 
                                                                --  4-move fiscalPeriod from join to where clause 
20200218	    	jhanicak			jh 20200218		 PF-1254 Added default values for IPEDSReportingPeriod and IPEDSClientConfig
20200108            akhasawneh                			 Move original code to template format
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
	'F2C' surveyId,
	'Current' currentSection,
	'Prior' priorSection,
	'U' finGPFSAuditOpinion,  --U = unknown/in progress -- all versions
	'A' finAthleticExpenses,  --A = Auxiliary Enterprises -- all versions
	'Y' finEndowmentAssets,  --Y = Yes -- v1, v2
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
	'F2C' surveyId,
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
	repValues.finPellTransactions finPellTransactions
from (
	select NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.currentSection) THEN repPeriodENT.asOfDate END, defvalues.asOfDate) asOfDate,
		NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.priorSection) THEN repPeriodENT.asOfDate END, defvalues.priorAsOfDate) priorAsOfDate,
		repPeriodENT.surveyCollectionYear surveyYear,
		repPeriodENT.surveyId surveyId,
		defvalues.currentSection currentSection,
		defvalues.priorSection priorSection,
		defvalues.finGPFSAuditOpinion finGPFSAuditOpinion,
		defvalues.finPellTransactions finPellTransactions,
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
		repValues.finPellTransactions
		
	union

	select defvalues.surveyYear surveyYear,
		defvalues.surveyId surveyId,
		defvalues.asOfDate asOfDate,
		defvalues.priorAsOfDate priorAsOfDate,
		defvalues.currentSection currentSection,
		defvalues.priorSection priorSection,
		defvalues.finGPFSAuditOpinion finGPFSAuditOpinion, 
		defvalues.finPellTransactions finPellTransactions
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

--jdh 2020-03-04 Parent/Child Institution added in to be consistant between different versions

--jh 20200412 Added DefaultValues query and rewrote other queries to use PF-1418

ClientConfigMCR AS
--Pulls client-given data for part 9, including:
--finGPFSAuditOpinion - all versions
--finAthleticExpenses - all versions
--finEndowmentAssets - v1, v2
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
	finPellTransactions finPellTransactions
from (
    select repperiod.surveyYear surveyCollectionYear,
	   repperiod.asOfDate asOfDate,
	   repperiod.priorAsOfDate priorAsOfDate,
	   NVL(clientConfigENT.finGPFSAuditOpinion, repperiod.finGPFSAuditOpinion) finGPFSAuditOpinion,
	   NVL(clientConfigENT.finPellTransactions, repperiod.finPellTransactions) finPellTransactions,
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
	repperiod.finPellTransactions finPellTransactions
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
	CASE FYData.finPellTransactions
			when 'P' then 1
			when 'F' then 2
			when 'N' then 3 END finPellTransactions,
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
			) AS GLRn
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
where GLRn = 1
),


--jdh 2020-03-04 Added OL cte for most recent record views for OperatingLedgerReporting

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
    Part C: Scholarships and Fellowships
    Part D: Revenues and Investment Return
    Part E: Expenses by Functional and Natural Classification
*****/

-- 9  
-- General Information 

--jdh 2020-03-04  Moved Case Statements from part 9 into COASPerFYAsOfDate cte

select DISTINCT '9' part,
	0 sort,
	CAST(MONTH(NVL(fiscalyr.startDate, fiscalyr.priorAsOfDate))  as BIGINT) field1,
	CAST(YEAR(NVL(fiscalyr.startDate, fiscalyr.priorAsOfDate)) as BIGINT) field2,
	CAST(MONTH(NVL(fiscalyr.endDate, fiscalyr.asOfDate)) as BIGINT) field3,
	CAST(YEAR(NVL(fiscalyr.endDate, fiscalyr.asOfDate)) as BIGINT) field4,
	fiscalyr.finGPFSAuditOpinion field5, --1=Unqualified, 2=Qualified, 3=Don't know
	fiscalyr.finPellTransactions field6 
from FiscalYearMCR fiscalyr 

union

-- C 
-- Scholarships and Fellowships
/* This section collects information about the sources of revenue that support 
   (1) Scholarship and Fellowship expense and 
   (2) discounts applied to tuition and fees and auxiliary enterprises. 

   - For each source on lines 01-06, enter the amount of revenue received from each source for supporting 
   scholarships and fellowships. Scholarships and fellowships include: grants-in-aid, trainee stipends, 
   tuition and fee waivers, and prizes to students. Student grants do not include amounts provided to
   students as payments for teaching or research or as fringe benefits.

   - For lines 08 and 09, identify amounts that are reported in the GPFS as discounts and allowances only. 
   "Discounts and allowance" means the institution displays the financial aid amount as a deduction from 
   tuition and fees or a deduction from auxiliary enterprise revenues in its GPFS. */

-- Line 1 - Pell grants

select 'C',
	1 sort,
	'1'field1, 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT) field2,
	NULL field3,
	NULL field4,
	NULL field5,
	NULL field6
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.expFAPellGrant = 'Y'

union

-- Line 2 - Other federal grants (Do NOT include FDSL amounts)

select 'C',
	2,
	'2', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- FASB C02
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.expFANonPellFedGrants = 'Y'

union

-- Line 3 - Grants by state government

select 'C',
	3,
	'3',
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- F2, Amount
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.expFAStateGrants = 'Y'

union

-- Line 4 - Grants by local government

select 'C',
	4,
	'4',
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.expFAStateGrants = 'Y'
    and oppledger.expFALocalGrants = 'Y'

union

-- Line 5 - Institutional grants from restricted resources

select 'C',
	5,
	'5',
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.expFAInstitGrantsRestr = 'Y'

union

-- Line 6 - Institutional grants from un-restricted resources

select 'C',
	6,
	'6', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.expFAInstitGrantsUnrestr = 'Y'

/* Line 07 - Total revenue that funds scholarship and fellowships
   (Calculated (Do not include in import file )  
   CV=[C01+...+C06]
*/

union

-- Line 8 - Discounts and allowances applied to tuition and fees

select 'C',
	8,
	'8', 
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.discAllowTuitionFees = 'Y'

union

-- Line 9 - Discounts and allowances applied to sales and services of auxiliary enterprises

select 'C',
	9,  -- Sorts
	'9',-- F1
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),  -- F2, Amount
	NULL, --F3
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.discAllowAuxEnterprise = 'Y'

union

-- D
-- Revenues and Investment Return by Source
/* This part is intended to report revenues by source. The revenues and investment return 
   reported in this part should agree with the revenues reported in the institution-s GPFS. 

   Exclude from revenue (and expenses) interfund or intraorganizational charges and credits. 
   Interfund and intraorganizational charges and credits include interdepartmental charges, 
   indirect costs, and reclassifications from temporarily restricted net assets.

   Revenues are reported by restriction (columns) and by source (rows).
   Column 1, Total Amount - Sum of the columns 2 through 4.
   Column 2, Unrestricted - Report revenues that are not subject to limitations by 
             a donor-imposed restriction.
   Column 3, Temporarily Restricted - Report revenues that are subject to limitation 
             by donor specification as to use or the time WHEN use may occur 
             (such as a later period of time or after specified events have occurred).
   Column 4, Permanently Restricted - Report revenues that must be maintained in
             perpetuity due to a donor-imposed restriction.
*/ 

-- Line 1 - Tuition and fees 
/* FASB Totals(Col1) Should Match GASB Part B01
   FASB Column 1 Should be the SUM of Columns 2-4 as it is the total broken out by
   UnRestricted,  RestrictedTemp and RestrictedPerm
   Very Similar to Fin V2 Section D01  */

select 'D',
	11,
	'1',    -- Tuition and fees (net of allowance reported in Part C, line 08) D01
	2,      -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revTuitionAndFees = 'Y' THEN oppledger.endBalance ELSE 0 END)
		- SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and ((oppledger.revTuitionAndFees = 'Y'  
		or oppledger.discAllowTuitionFees = 'Y')
		and oppledger.isUnrestrictedFASB = 1)

union

select 'D',
	12,
	'1',   -- Tuition and fees (net of allowance reported in Part C, line 08) D01
	3,     -- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revTuitionAndFees = 'Y' THEN oppledger.endBalance ELSE 0 END)
		- SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and ((oppledger.revTuitionAndFees = 'Y'  
		or oppledger.discAllowTuitionFees = 'Y')
		and oppledger.isRestrictedTempFASB = 1)

union

select 'D',
	13,
	'1',   -- Tuition and fees (net of allowance reported in Part C, line 08) D01
	4,     -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revTuitionAndFees = 'Y' THEN oppledger.endBalance ELSE 0 END)
		- SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and ((oppledger.revTuitionAndFees = 'Y'
		or oppledger.discAllowTuitionFees = 'Y')
		and oppledger.isRestrictedPermFASB = 1)

union

-- Line 2 - Federal appropriations   

select 'D',
	14,
	'2',  -- Federal appropriations | D02
	2, 	  -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revFedApproprations = 'Y'
	and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	15,
	'2', 	-- Federal appropriations | D02
	3,		--isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.revFedApproprations = 'Y'
    and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	16,
	'2',   -- Federal appropriations | D02
	4,     -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.revFedApproprations = 'Y'
    and oppledger.isRestrictedPermFASB = 1

union

-- Line 3 - State appropriations  

select 'D',
	17,
	'3',  -- State appropriations  | D03
	2,    -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.revStateApproprations = 'Y'
    and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	18,
	'3',   -- State appropriations  | D03
	3,     -- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateApproprations = 'Y'
	and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	19,
	'3',   -- State appropriations  | D03
	4,     -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateApproprations = 'Y'
	and oppledger.isRestrictedPermFASB = 1 

union

-- Line 4 - Local appropriations  

select 'D',
	20,
	'4', -- Local appropriations | D04
	2, 	 -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalApproprations = 'Y'
	and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	21,
	'4',  -- Local appropriations | D04
	3,    -- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
  and oppledger.revLocalApproprations = 'Y'
  and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	22,
	'4', -- Local appropriations | D04
	4, 	 -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalApproprations = 'Y'
	and oppledger.isRestrictedPermFASB = 1 

union

-- Line 5 - Federal grants and contracts (Do not include FDSL) 

select 'D',
	23,
	'5', -- Federal grants and contracts (Do not include FDSL) | D05
	2, 	 -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revFedGrantsContractsOper = 'Y'
	and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	24,
	'5', -- Federal grants and contracts (Do not include FDSL) | D05
	3, 	 -- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revFedGrantsContractsOper = 'Y'
	and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	25,
	'5', -- Federal grants and contracts (Do not include FDSL) | D05
	4, 	 -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revFedGrantsContractsOper = 'Y'
	and oppledger.isRestrictedPermFASB = 1

-- Line 6  - State grants and contracts  

union

select 'D',
	26,
	'6', -- State grants and contracts | D06
	2,   -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateGrantsContractsOper = 'Y'
	and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	27,
	'6', -- State grants and contracts | D06
	3, 	 --isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateGrantsContractsOper = 'Y'
	and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	28,
	'6', -- State grants and contracts | D06
	4,   -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revStateGrantsContractsOper = 'Y'
	and oppledger.isRestrictedPermFASB = 1

-- Line 7 -- Local government grants and contracts  

union

select 'D',
	29,
	'7', -- Local government grants and contracts | D07
	2,   -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalGrantsContractsOper = 'Y'
	and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	30,
	'7', -- Local government grants and contracts | D07
	3,   -- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalGrantsContractsOper = 'Y'
	and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	31,
	'7',  -- Local government grants and contracts | D07
	4,    -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revLocalGrantsContractsOper = 'Y'
	and oppledger.isRestrictedPermFASB = 1

-- Line 8 Private Gifts and Contracts are [Calculated CV D08 = (D08a + D08b)]
-- (Do not include in import file. Will be calculated)

union

-- Line 8a - Private gifts
select 'D',
	32,
	'8a', -- D08a  -- revPrivGifts
	2,    -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGifts = 'Y'
	and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	33,
	'8a', -- D08a  -- revPrivGifts
	3, 	  -- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGifts = 'Y'
	and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	34,
	'8a',   -- D08a  -- revPrivGifts
	4, 		-- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGifts = 'Y'
	and oppledger.isRestrictedPermFASB = 1

union

-- Line 8b - Private grants & Contracts   

select 'D',
	35,
	'8b',   -- Private grants & Contracts | D08b revPrivGrantsContractsOper
	2, 		--isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGrantsContractsOper = 'Y'
	and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	36,
	'8b',   -- Private grants & Contracts | D08b revPrivGrantsContractsOper
	3, 		--isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revPrivGrantsContractsOper = 'Y'
	and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	37,
	'8b',   -- Private grants & Contracts | D08b revPrivGrantsContractsOper
	4, 		-- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.isRestrictedPermFASB = 1

union

-- Line 9 - Contributions from affiliated entities

select 'D',
	38,
	'9', -- Contributions from affiliated entities | D09 revAffiliatedOrgnGifts
	2, 	 -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revAffiliatedOrgnGifts = 'Y'
	and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	39,
	'9', -- Contributions from affiliated entities | D09 revAffiliatedOrgnGifts
	3, 	 -- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revAffiliatedOrgnGifts = 'Y'
	and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	40,
	'9', -- Contributions from affiliated entities | D09 revAffiliatedOrgnGifts
	4, 	 -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revAffiliatedOrgnGifts = 'Y'
	and oppledger.isRestrictedPermFASB = 1 

-- Other Revenue 
-- Line 10 - Investment return - 
/* Enter all investment income (i.e., interest, dividends, rents and royalties), gains and losses 
   (realized and unrealized) from holding investments (regardless of the nature of the investment), 
   student loan interest, and amounts distributed from irrevocable trusts held by others 
   (collectively referred to as "investment return").
*/

union

select 'D',
	41,
	'10', -- Investment return | D10  revInvestmentIncome
	2, 	  -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revInvestmentIncome = 'Y'
	and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	42,
	'10',   -- Investment return | D10  revInvestmentIncome
	3, 		-- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revInvestmentIncome = 'Y'
	and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	43,
	'10',   -- Investment return | D10  revInvestmentIncome
	4, 		-- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revInvestmentIncome = 'Y'
	and oppledger.isRestrictedPermFASB = 1

union

-- Line 11 - Sales and services of educational activities
/* Enter all revenues derived from the sales of goods or services that are incidental to the conduct of 
   instruction, research or public service, and revenues of activities that exist to provide instructional 
   and laboratory experience for students and that incidentally create goods and services that may be sold.
*/

select 'D',
	44,
	'11',   -- Sales and services of educational activities | D11 revEducActivSalesServices
	2, 		-- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revEducActivSalesServices = 'Y'
	and oppledger.isUnrestrictedFASB = 1 

/* Part 'D', Line 15  -- Other Reveue
   (Calculated, Do not include in file )
*/

union

-- Line 16 - Total revenues and investment return
/* This amount is carried forward from Part B, line 01.  This amount should include ARRA
   revenues received by the institution , if any.
 
   I copied Formula from FASB B01, GASB B01 For Totals and THEN added the
   Flags for properties :  Unrestricted, RestrictedTemp, RestrictedPerm 
*/
 
select 'D',
	45,
	'16',  	-- Total revenues and investment return | D16
	1, 		-- Total
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue'
		and (oppledger.revRealizedCapitalGains != 'Y'
			and oppledger.revRealizedOtherGains != 'Y'
			and oppledger.revExtraordGains != 'Y'
			and oppledger.revOwnerEquityAdjustment != 'Y'
			and oppledger.revSumOfChangesAdjustment != 'Y'))
        
union

select 'D',
	46,
	'16', -- Total revenues and investment return | D16
	2,    -- isUnrestrictedFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue'
        and (oppledger.revRealizedCapitalGains != 'Y'
        and oppledger.revRealizedOtherGains != 'Y'
        and oppledger.revExtraordGains != 'Y'
        and oppledger.revOwnerEquityAdjustment != 'Y'
        and oppledger.revSumOfChangesAdjustment != 'Y'))
   and oppledger.isUnrestrictedFASB = 1

union

select 'D',
	47,
	'16', -- Total revenues and investment return | D16
	3,    -- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue'
        and (oppledger.revRealizedCapitalGains != 'Y'
            and oppledger.revRealizedOtherGains != 'Y'
            and oppledger.revExtraordGains != 'Y'
            and oppledger.revOwnerEquityAdjustment != 'Y'
            and oppledger.revSumOfChangesAdjustment != 'Y'))
	and oppledger.isRestrictedTempFASB = 1

union

select 'D',
	48,
	'16', -- Total revenues and investment return | D16
	4, 	  -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and (oppledger.accountType = 'Revenue'
        and (oppledger.revRealizedCapitalGains != 'Y'
            and oppledger.revRealizedOtherGains != 'Y'
            and oppledger.revExtraordGains != 'Y'
            and oppledger.revOwnerEquityAdjustment != 'Y'
            and oppledger.revSumOfChangesAdjustment != 'Y'))
	and oppledger.isRestrictedPermFASB = 1

union

-- Line 17 - Net assets released from restriction - 
/* Enter all revenues resulting from the reclassification of temporarily restricted assets
   or permanently restricted assets.
*/

select 'D',
	49,
	'17', -- D17  revReleasedAssets
	3,    -- isRestrictedTempFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revReleasedAssets = 'Y'
	and oppledger.isRestrictedTempFASB = 1

union

-- Line 17 -- Net assets released from Restriction  

select 'D',
	50,
	'17', -- Line    -- revReleasedAssets
	4,    -- Column  -- isRestrictedPermFASB
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.revReleasedAssets = 'Y'
	and oppledger.isRestrictedPermFASB = 1

/*  
    Line 18  D18 - Net total revenues, after assets released from restriction
             Do not include in export - Calculated by Form.
    Line 19  D19 - 	12-MONTH Student FTE from E12
             Do not include in export - Calculated by Form.
    Line 20  D20 - Total revenues and investment return per student FTE CV=[D16/D19]
             Do not include in export - Calculated by Form. 
*/
 
-- E
-- Expenses by Functional and Natural Classification
/* Report Total Operating AND Nonoperating Expenses in this section Functional Classification 
   Expense by Functional Classification
   Column 1, Total amount - Enter the total expense for each applicable functional category listed on lines 01-08. 
             Total expenses, line 13, should agree with the total expenses reported in your GPFS.
   Column 2, Salaries and wages - This column describes the natural classification of salary and wage expenses 
             incurred in each functional category. For this classification, enter the amount of 
             salary and wage expenses for the function identified in lines 01-08 and 13. 
             Do NOT include Operation and maintenance of plant (O&M) expenses in this category 
             because O&M expenses are reported in a separate natural classification category. 
*/ 

union

select 'E', 
	51,    
	'1',   -- F1| Line -- Instruction | E1,1
	1,     -- F2| Column
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E1,1
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isInstruction = 1

union

-- Line 1 - Instruction

select 'E',
	52,
	'1',   -- Instruction | E1,2
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --   E1,2
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.accountType = 'Expense'
	and oppledger.isInstruction = 1
	and oppledger.expSalariesWages = 'Y'

union

-- Line 2 - Research

select 'E',
	53,
	'2',   -- Research | E1,2
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --  E2,1
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.accountType = 'Expense'
	and oppledger.isResearch = 1

union

select 'E',
	54,
	'2',   -- Research | E1,2
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E2,2
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.expSalariesWages = 'Y'
	and oppledger.isResearch = 1 

union

-- Line 3 - Public service

select 'E',
	55,
	'3',   -- Public service | E1,3
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --E3,1
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.accountType = 'Expense'
	and oppledger.isPublicService = 1

union

select 'E',
	56,
	'3',   -- Public service | E1,3
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E3,2
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
   and oppledger.accountType = 'Expense'
   and oppledger.expSalariesWages = 'Y'
   and oppledger.isPublicService = 1

union

-- Line 4 - Academic support

select 'E',
	57,
	'4',   -- Academic support | E1,4
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --E4,1
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isAcademicSupport = 1

union

select 'E',
	58,
	'4',   -- Academic support | E1,4
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E4,2
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.accountType = 'Expense'
	and oppledger.expSalariesWages = 'Y'
	and oppledger.isAcademicSupport = 1

union

-- Line 5 - Student services

select 'E',
	59,
	'5',   -- Student services | E1,5
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --E5,1
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.accountType = 'Expense'
	and oppledger.isStudentServices = 1

union

select 'E',
	60,
	'5',   -- Student services | E1,5
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E5,2
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.accountType = 'Expense'
	and oppledger.expSalariesWages = 'Y'
	and oppledger.isStudentServices = 1

union

-- Line 6 - Institutional support 

select 'E',
	61,
	'6',   -- Institutional support | E1,6
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --E6,1
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isInstitutionalSupport = 1
	
union

select 'E',
	62,
	'6',   -- Institutional support | E1,6
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), --E6,1
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
	and oppledger.accountType = 'Expense'
	and oppledger.isInstitutionalSupport = 1

union

-- Line 08 - Net grant aid to students (net of tuition and fee allowances) - 
/* Enter on this line ONLY scholarships and fellowships recognized as expenses 
   in your GPFS. 
   Do not include Federal Work Study expenses on this line. */

select 'E',
	63,
	'8',   -- Net grant aid to students | E1,8
	1,
	CAST(NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.expFAPellGrant = 'Y'        			--Pell grants
										or oppledger.expFANonPellFedGrants = 'Y'    	--Other federal grants
										or oppledger.expFAStateGrants = 'Y'         	--Grants by state government
										or oppledger.expFALocalGrants = 'Y'         	--Grants by local government
										or oppledger.expFAInstitGrantsRestr = 'Y'   	--Institutional grants from restricted resources
										or oppledger.expFAInstitGrantsUnrestr = 'Y') 	--Institutional grants from unrestricted resources
								THEN oppledger.endBalance ELSE 0 END)
		- SUM(CASE WHEN (oppledger.discAllowTuitionFees = 'Y'      			--Discounts and allowances applied to tuition and fees
							or oppledger.discAllowAuxEnterprise = 'Y')  		--Discounts and allowances applied to sales and services of auxiliary enterprises
				   THEN oppledger.endBalance ELSE 0 END))), 0) AS BIGINT),
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and (oppledger.expFAPellGrant = 'Y' 
        or oppledger.expFANonPellFedGrants = 'Y' 
        or oppledger.expFAStateGrants = 'Y' 
        or oppledger.expFALocalGrants = 'Y' 
        or oppledger.expFAInstitGrantsRestr = 'Y'  
        or oppledger.expFAInstitGrantsUnrestr = 'Y' 
        or oppledger.discAllowTuitionFees = 'Y' 
        or oppledger.discAllowAuxEnterprise = 'Y')

-- Line 12 Other Functional Expenses and deductions    
/* 	Calculated CV = [E13-(E01+...+E10)]
    (Do not include in import file. Will be calculated ) 
*/

union

-- Line 13-1 - Total expenses and Deductions

select 'E',
	64,
	'13',   -- Total expenses and Deductions | E-13
	1,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E13,1
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and (oppledger.accountType = 'Expense'
		and (expFedIncomeTax != 'Y' and expExtraordLosses != 'Y' ))

union

select 'E',
	65,
	'13',   -- Total expenses and Deductions | E-13
	2,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E13,2
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.accountType = 'Expense'
	and oppledger.expSalariesWages = 'Y'

-- Part E-2 
-- Expenses by Natural Classification 
/* This part is intended to collect expenses by natural classification. 
   Do NOT include Operation and maintenance of plant (O&M) expenses in Salaries and Wages, 
   Benefits, Depreciation, Interest, or Other Natural Expenses because O&M expense is 
   reported in its own separate natural classification category.
*/

-- Line 13 13,2 Salaries and Wages(from Part E-1, line 13 column 2)
-- (Do not include in import file. Will be calculated ) 

union

-- Line 13-3 Benefits

select 'E',
	66,
	'13',   --  E13-3 Benefits
	3,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E13-3 Benefits
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.expBenefits = 'Y'

union

-- Line 13-4 Operation and Maintenance of Plant

select 'E',
	67,
	'13',   --  E13-4 Operation and Maintenance of Plant
	4,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E13-4,
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and (oppledger.expCapitalConstruction = 'Y'
		or oppledger.expCapitalEquipPurch = 'Y'
		or oppledger.expCapitalLandPurchOther = 'Y')

union

-- Line 13-5 Depreciation

select 'E',
	68,
	'13',   --  E13-5 Depreciation
	5,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E13-5 Depreciation
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.expDepreciation = 'Y'

union

--  Line 13-6 - Interest
/* Part E-2,
   Line 13-1 Total Expenses and Deductions (from Part E-1, Line 13)
             (Do not include in import file. Will be calculated )
   Line 14-1 12-MONTH Student FTE (from E12 survey)
             (Do not include in import file. Will be calculated )
   Line 15-1 Total expenses and deductions per student FTE   CV=[E13/E14]
             (Do not include in import file. Will be calculated )
*/

select 'E',
	69,
	'13',   --  E13-6 Interest
	6,
	CAST(NVL(ABS(ROUND(SUM(oppledger.endBalance))), 0) AS BIGINT), -- E13-6 Interest
	NULL,
	NULL,
	NULL
from OperatingLedgerMCR oppledger 
where oppledger.fiscalPeriod = 'Year End'
    and oppledger.expInterest = 'Y'

--order by 2
