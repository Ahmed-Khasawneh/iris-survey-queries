/********************

EVI PRODUCT:    DORIS 2019-20 IPEDS Survey Spring Collection
FILE NAME:      Finance v6  (F3B) 
FILE DESC:      Finance for Non-Degree-Granting Private For-Profit Institutions
AUTHOR:         Janet Hanicak / JD Hysler
CREATED:        20191220

SECTIONS:
    Reporting Dates 
    Most Recent Records 
    Survey Formatting  
    
SUMMARY OF CHANGES

Date(yyyymmdd)      Author              Tag              Comments
-----------------   ----------------    -------------    --------------------------------------------------------------------------------
20200622			akhasawneh			ak 20200622		 Modify Finance report query with standardized view naming/aliasing convention (PF-1532) -Run time 1m 11s
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
20200127	    	jhanicak			jh 20200127		 PF-1254 Added default values for IPEDSReportingPeriod and IPEDSClientConfig 
														 Added WITH queries for all parts to increase performance
														 Modified select statements to call the new WITH queries to increase performance
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
	'F3B' surveyId,
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
	'F3B' surveyId,
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
	repValues.finPellTransactions finPellTransactions,
	repValues.finBusinessStructure finBusinessStructure,
	repValues.finTaxExpensePaid finTaxExpensePaid
from (
	select NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.currentSection) THEN repPeriodENT.asOfDate END, defvalues.asOfDate) asOfDate,
		NVL(CASE WHEN UPPER(repPeriodENT.surveySection) = UPPER(defvalues.priorSection) THEN repPeriodENT.asOfDate END, defvalues.priorAsOfDate) priorAsOfDate,
		repPeriodENT.surveyCollectionYear surveyYear,
		repPeriodENT.surveyId surveyId,
		defvalues.currentSection currentSection,
		defvalues.priorSection priorSection,
		defvalues.finGPFSAuditOpinion finGPFSAuditOpinion,
		defvalues.finPellTransactions finPellTransactions,
		defvalues.finBusinessStructure finBusinessStructure,
		defvalues.finTaxExpensePaid finTaxExpensePaid,
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
	repValues.finPellTransactions,
	repValues.finBusinessStructure,
	repValues.finTaxExpensePaid
union

select defvalues.surveyYear surveyYear,
    defvalues.surveyId surveyId,
    defvalues.asOfDate asOfDate,
    defvalues.priorAsOfDate priorAsOfDate,
    defvalues.currentSection currentSection,
	defvalues.priorSection priorSection,
    defvalues.finGPFSAuditOpinion finGPFSAuditOpinion, 
	defvalues.finPellTransactions finPellTransactions,
	defvalues.finBusinessStructure finBusinessStructure,
	defvalues.finTaxExpensePaid finTaxExpensePaid
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
--finTaxExpensePaid - v3, v6
--finParentOrChildInstitution - v1, v2, v3

(
select surveyCollectionYear surveyYear,
	asOfDate asOfDate,
	priorAsOfDate priorAsOfDate,
	finGPFSAuditOpinion finGPFSAuditOpinion,
	finPellTransactions finPellTransactions,
	finBusinessStructure finBusinessStructure,
	finTaxExpensePaid finTaxExpensePaid
from (
    select repperiod.surveyYear surveyCollectionYear,
		repperiod.asOfDate asOfDate,
		repperiod.priorAsOfDate priorAsOfDate,
		NVL(clientConfigENT.finGPFSAuditOpinion, repperiod.finGPFSAuditOpinion) finGPFSAuditOpinion,
		nvl(clientConfigENT.finPellTransactions, repperiod.finPellTransactions) finPellTransactions,
		nvl(clientConfigENT.finBusinessStructure, repperiod.finBusinessStructure) finBusinessStructure,
		nvl(clientConfigENT.finTaxExpensePaid, repperiod.finTaxExpensePaid) finTaxExpensePaid,
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
		repperiod.finPellTransactions finPellTransactions,
		repperiod.finBusinessStructure finBusinessStructure,
		repperiod.finTaxExpensePaid finTaxExpensePaid
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
		 when 'Q' THEN 1
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
	from ClientConfigMCR clientConfig
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
--jh 20200226 Added most recent record views for GeneralLedgerReporting

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
),
 
--jh 20200127
--Modified previous PartC to increase performance

PartC AS (
select partClassification,
	case when partClassification = '3' then partSection else null end partSection,
	case when partClassification = '1' then partAmount.C1
		when partClassification = '2' then partAmount.C2
		when partClassification = '3' and partSection = 'a' then partAmount.C3a
		when partClassification = '3' and partSection = 'b' then partAmount.C3b
		when partClassification = '4' then partAmount.C4
		when partClassification = '6' then partAmount.C6
		when partClassification = '7' then partAmount.C7 end partClassAmount
from ( 
	select nvl(ABS(ROUND(SUM(CASE WHEN oppledger.expFAPellGrant = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) C1,
		nvl(ABS(ROUND(SUM(CASE WHEN oppledger.expFANonPellFedGrants = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) C2,
		nvl(ABS(ROUND(SUM(CASE WHEN oppledger.expFAStateGrants = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) C3a,
		nvl(ABS(ROUND(SUM(CASE WHEN oppledger.expFALocalGrants = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) C3b,
		nvl(ABS(ROUND(SUM(CASE WHEN (oppledger.expFAInstitGrantsRestr = 'Y' OR oppledger.expFAInstitGrantsUnrestr = 'Y') THEN oppledger.endBalance ELSE 0 END))), 0) C4,
		nvl(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) C6,
		nvl(ABS(ROUND(SUM(CASE WHEN oppledger.discAllowAuxEnterprise = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) C7
	from OperatingLedgerMCR oppledger 
	where oppledger.fiscalPeriod = 'Year End'
		and oppledger.accountType IN ('Expense', 'Revenue Discount')
		and (oppledger.expFAPellGrant = 'Y'
			or oppledger.expFANonPellFedGrants = 'Y'
			or oppledger.expFAStateGrants = 'Y'
			or oppledger.expFALocalGrants = 'Y'
			or oppledger.expFAInstitGrantsRestr = 'Y'
			or oppledger.expFAInstitGrantsUnrestr = 'Y'
			or oppledger.discAllowTuitionFees = 'Y'
			or oppledger.discAllowAuxEnterprise = 'Y')
	) partAmount
	cross join (select * 
				from (VALUES 
						('1'),
						('2'),
						('3'),
						('4'),
						('6'),
						('7')
					) PartClass(partClassification)
				)
	cross join (select * 
				from (VALUES 
						('a'),
						('b')
					) PartSection(partSection)
				)
),

--jh 20200127
--Added PartD as inline view

PartD as (
select partClassification,
	case when partClassification = '2' and partSection in ('a', 'b') then partSection 
		when partClassification = '3' then partSection 
		else null 
	end partSection,
	case when partClassification = '1' then partAmount.D1
		when partClassification = '2' and partSection = 'a' then partAmount.D2a
		when partClassification = '2' and partSection = 'b' then partAmount.D2b
		when partClassification = '3' and partSection = 'a' then partAmount.D3a
		when partClassification = '3' and partSection = 'b' then partAmount.D3b
		when partClassification = '3' and partSection = 'c' then partAmount.D3c
		when partClassification = '3' and partSection = 'd' then partAmount.D3d
		when partClassification = '4' then partAmount.D4
		when partClassification = '5' then partAmount.D5
		when partClassification = '6' then partAmount.D6
		when partClassification = '9' then partAmount.D9 
	end partClassAmount
from ( 
	select NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revTuitionAndFees = 'Y' THEN oppledger.endBalance ELSE 0 END) 
			- SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) D1,
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revFedApproprations = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) D2a,
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revFedGrantsContractsOper = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) D2b,
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revStateApproprations = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) D3a,
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revStateGrantsContractsOper = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) D3b,
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revLocalApproprations = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) D3c,
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revLocalGrantsContractsOper = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) D3d,
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.revPrivGifts = 'Y'
							or oppledger.revPrivGrantsContractsOper = 'Y'
							or oppledger.revPrivGrantsContractsNOper = 'Y') THEN oppledger.endBalance ELSE 0 END))), 0) D4,
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revInvestmentIncome = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) D5,
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.revEducActivSalesServices = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) D6,
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.revRealizedCapitalGains != 'Y'
							and oppledger.revRealizedOtherGains != 'Y'
							and oppledger.revExtraordGains != 'Y'
							and oppledger.revOwnerEquityAdjustment != 'Y'
							and oppledger.revSumOfChangesAdjustment != 'Y') THEN oppledger.endBalance ELSE 0 END))), 0) D9
	from OperatingLedgerMCR oppledger 
	where oppledger.fiscalPeriod = 'Year End'
		and (oppledger.accountType = 'Revenue'
			or oppledger.discAllowTuitionFees = 'Y')
	) partAmount
	cross join (select * 
				from (VALUES 
						('1'),
						('2'),
						('3'),
						('4'),
						('5'),
						('6'),
						('9')
					) PartClass(partClassification)
				)
	cross join (select * 
				from (VALUES 
						('a'),
						('b'),
						('c'),
						('d')
					) PartSection(partSection)
				)
),

--jh 20200127
--Added PartE as inline view

PartE as (
select partClassification,
	case when partClassification in ('1', '2a', '2b', '3a', '3b', '3c') and partSection in ('1', '2') then partSection 
		when partClassification = '5' and partSection = '1' then partSection 
		when partClassification = '7' then partSection 
		else null 
	end partSection,
	case when partClassification = '1' and partSection = '1' then partAmount.E1_1
		when partClassification = '1' and partSection = '2' then partAmount.E1_2
		when partClassification = '2a' and partSection = '1' then partAmount.E2a_1
		when partClassification = '2a' and partSection = '2' then partAmount.E2a_2
		when partClassification = '2b' and partSection = '1' then partAmount.E2b_1
		when partClassification = '2b' and partSection = '2' then partAmount.E2b_2
		when partClassification = '3a' and partSection = '1' then partAmount.E3a_1
		when partClassification = '3a' and partSection = '2' then partAmount.E3a_2
		when partClassification = '3b' and partSection = '1' then partAmount.E3b_1
		when partClassification = '3b' and partSection = '2' then partAmount.E3b_2
		when partClassification = '3c' and partSection = '1' then partAmount.E3c_1
		when partClassification = '3c' and partSection = '2' then partAmount.E3c_2
		when partClassification = '5' and partSection = '1' then partAmount.E5_1
		when partClassification = '7' and partSection = '1' then partAmount.E7_1
		when partClassification = '7' and partSection = '2' then partAmount.E7_2
		when partClassification = '7' and partSection = '3' then partAmount.E7_3
		when partClassification = '7' and partSection = '4' then partAmount.E7_4
		when partClassification = '7' and partSection = '5' then partAmount.E7_5
		when partClassification = '7' and partSection = '6' then partAmount.E7_6
	end partClassAmount
from ( 
	select NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.accountType = 'Expense' 
								and oppledger.isInstruction = 1) THEN oppledger.endBalance ELSE 0 END))), 0) E1_1, -- Instruction
		NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.accountType = 'Expense' 
										and oppledger.isInstruction = 1)
											and (oppledger.expSalariesWages = 'Y'
												or oppledger.expOperMaintSalariesWages = 'Y')) THEN oppledger.endBalance ELSE 0 END))), 0) E1_2, -- Instruction
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.accountType = 'Expense' 
										and oppledger.isResearch = 1) THEN oppledger.endBalance ELSE 0 END))), 0) E2a_1, -- Research
		NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.accountType = 'Expense' 
										and oppledger.isResearch = 1)
										and (oppledger.expSalariesWages = 'Y'
											or oppledger.expOperMaintSalariesWages = 'Y')) THEN oppledger.endBalance ELSE 0 END))), 0) E2a_2, -- Research
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.accountType = 'Expense' 
										and oppledger.isPublicService = 1) THEN oppledger.endBalance ELSE 0 END))), 0) E2b_1, -- Public service
		NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.accountType = 'Expense' 
										and oppledger.isPublicService = 1)
									and (oppledger.expSalariesWages = 'Y'
										or oppledger.expOperMaintSalariesWages = 'Y')) THEN oppledger.endBalance ELSE 0 END))), 0) E2b_2, -- Public service
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.accountType = 'Expense' 
										and oppledger.isAcademicSupport = 1) THEN oppledger.endBalance ELSE 0 END))), 0) E3a_1, -- Academic support
		NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.accountType = 'Expense' 
										and oppledger.isAcademicSupport = 1)
										and (oppledger.expSalariesWages = 'Y'
											or oppledger.expOperMaintSalariesWages = 'Y')) THEN oppledger.endBalance ELSE 0 END))), 0) E3a_2, -- Academic support
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.accountType = 'Expense' 
										and oppledger.isStudentServices = 1) THEN oppledger.endBalance ELSE 0 END))), 0) E3b_1, -- Student services
		NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.accountType = 'Expense' 
										and oppledger.isStudentServices = 1)
										and (oppledger.expSalariesWages = 'Y'
											or oppledger.expOperMaintSalariesWages = 'Y')) THEN oppledger.endBalance ELSE 0 END))), 0) E3b_2, -- Student services
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.accountType = 'Expense' 
										and oppledger.isInstitutionalSupport = 1) THEN oppledger.endBalance ELSE 0 END))), 0) E3c_1, -- Institutional support total 
		NVL(ABS(ROUND(SUM(CASE WHEN ((oppledger.accountType = 'Expense' 
										and oppledger.isInstitutionalSupport = 1)
										and (oppledger.expSalariesWages = 'Y'
											or oppledger.expOperMaintSalariesWages = 'Y')) THEN oppledger.endBalance ELSE 0 END))), 0) E3c_2, -- Institutional support salaries and wages
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.expFAPellGrant = 'Y'            			--Pell grants
											or oppledger.expFANonPellFedGrants = 'Y'		--Other federal grants
											or oppledger.expFAStateGrants = 'Y'			--Grants by state government
											or oppledger.expFALocalGrants = 'Y'			--Grants by local government
											or oppledger.expFAInstitGrantsRestr = 'Y'		--Institutional grants from restricted resources
											or oppledger.expFAInstitGrantsUnrestr = 'Y') 	--Institutional grants from unrestricted resources
									THEN oppledger.endBalance ELSE 0 END)
			- SUM(CASE WHEN oppledger.discAllowTuitionFees = 'Y' THEN oppledger.endBalance  --Discounts and allowances applied to tuition and fees
						ELSE 0 END))), 0) E5_1,
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.expFedIncomeTax != 'Y'
									and oppledger.expExtraordLosses != 'Y') THEN oppledger.endBalance ELSE 0 END))), 0) E7_1, -- Total expenses and deductions
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.expSalariesWages = 'Y' or oppledger.expOperMaintSalariesWages = 'Y') THEN oppledger.endBalance ELSE 0 END))), 0) E7_2, -- Salaries and wages
		NVL(ABS(ROUND(SUM(CASE WHEN (oppledger.expBenefits = 'Y' or oppledger.expOperMaintBenefits = 'Y') THEN oppledger.endBalance ELSE 0 END))), 0) E7_3, -- Benefits
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expOperMaintOther = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) E7_4, -- Operation and Maintenance of Plant
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expDepreciation = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) E7_5, -- Depreciation
		NVL(ABS(ROUND(SUM(CASE WHEN oppledger.expInterest = 'Y' THEN oppledger.endBalance ELSE 0 END))), 0) E7_6 -- Interest
	from OperatingLedgerMCR oppledger 
	where oppledger.fiscalPeriod = 'Year End'
		and (oppledger.accountType = 'Expense'
			or (oppledger.discAllowTuitionFees = 'Y' 
			   or oppledger.discAllowAuxEnterprise = 'Y'
			   or oppledger.discAllowPatientContract = 'Y'))
	) partAmount
	cross join (select * 
				from (VALUES 
						('1'),
						('2a'),
						('2b'),
						('3a'),
						('3b'),
						('3c'),
						('5'),
						('7')
					) PartClass(partClassification)
				)
	cross join (select * 
				from (VALUES 
						('1'),
						('2'),
						('3'),
						('4'),
						('5'),
						('6')
					) PartSection(partSection)
				)
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

--jdh 2020-03-04 Removed cross join with ClientConfigMCR since values are now already in COASPerFYAsOfDate
--swapped CASE Statements for COAS fields used for Section 9 (General Information)

select DISTINCT '9' part,
	CAST(MONTH(NVL(fiscalyr.startDate, fiscalyr.priorAsOfDate))  as BIGINT) field1,
	CAST(YEAR(NVL(fiscalyr.startDate, fiscalyr.priorAsOfDate)) as BIGINT) field2,
	CAST(MONTH(NVL(fiscalyr.endDate, fiscalyr.asOfDate)) as BIGINT) field3,
	CAST(YEAR(NVL(fiscalyr.endDate, fiscalyr.asOfDate)) as BIGINT) field4,
	fiscalyr.finGPFSAuditOpinion  field5, --1=Unqualified, 2=Qualified, 3=Don't know
	fiscalyr.finPellTransactions  field6, --P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants
	fiscalyr.finBusinessStructure field7  --SP = Sole Proprietorship, P = Partnership, C = C Corp, SC = S Corp, LLC = LLC
from FiscalYearMCR fiscalyr 

union

-- C 
-- Scholarships and Fellowships
/* This section collects information about the sources of revenue that support 
   (1) Scholarship and Fellowship expense and 
   (2) discounts applied to tuition and fees and auxiliary enterprises. 
   For each source on lines 01-04, enter the amount of resources received from each source that are used for 
   supporting scholarships and fellowships. Scholarships and fellowships include: grants-in-aid, trainee stipends, 
   tuition and fee waivers, and prizes to students. Scholarships and fellowships do not include amounts provided 
   to students as payments for services including teaching or research or as fringe benefits.

   For lines 06 and 07, identify amounts that are reported in the GPFS as discounts and allowances only. 
   -Discounts and allowance- means the institution displays the financial aid amount as a deduction from tuition 
   and fees or a deduction from auxiliary enterprise revenues in its GPFS.
*/

--jh 20200127
--Modified select statement for Part C to pull from WITH and loop through all values

select 'C',
	PartC.partClassification partClassification,
	PartC.partSection partSection,
	CAST(PartC.partClassAmount AS BIGINT) partAmount,
	null,
	null,
	null,
	null
from PartC PartC
where (PartC.partClassification in ('1', '2', '4', '6', '7') 
		and PartC.partSection is null)
	or (PartC.partClassification = '3' 
		and PartC.partSection in ('a', 'b'))

union

-- D 
-- Revenues and Investment Return by Source
/* All revenue source categories are intended to be consistent with the definitions provided 
   for private institutions according to the NACUBO Financial Accounting and Reporting Manual (FARM).
   Exclude from revenues (and expenses) internal changes and credits. Internal changes and 
   credits include charges between parent and subsidiary only if the two are consolidated 
   in the amounts reported in the IPEDS survey.  */

--jh 20200127
--Modified select statement for Part D to pull from WITH and loop through all values

select distinct 'D',
	PartD.partClassification partClassification,
	PartD.partSection partSection,
	CAST(PartD.partClassAmount AS BIGINT) partAmount,
	null,
	null,
	null,
	null
from PartD PartD
where (PartD.partClassification in ('1', '4', '5', '6', '9') 
		and PartD.partSection is null)
	or (PartD.partClassification = '2' 
		and PartD.partSection in ('a', 'b'))
	or (PartD.partClassification = '3' 
		and PartD.partSection in ('a', 'b', 'c', 'd'))

union
 
-- E
-- Expenses by Functional and Natural Classification
/* Report Total Operating AND Nonoperating Expenses in this section	 Functional Classification 
   Part E is intended to report expenses by function. All expenses recognized in the GPFS
   should be reported using the expense functions provided on lines 01-06, 10, and 11. 
   These functional categories are consistent with Chapter 5 
   (Accounting for Independent Colleges and Universities) of the NACUBO FARM. (FARM para 504)

   The total for expenses on line 07 should agree with the total expenses reported in your GPFS 
   including interest expense and any other non-operating expense.

   Do not include losses or other unusual or nonrecurring items in Part E. Operation and 
   maintenance of plant expenses are no longer reported as a separate functional expense category. 
   Instead these expenses are to be distributed, or allocated, among the other functional 
   expense categories.
*/

--jh 20200127
--Modified select statement for Part E to pull from WITH and loop through all values

select distinct 'E',
	PartE.partClassification partClassification,
	PartE.partSection partSection,
	CAST(PartE.partClassAmount AS BIGINT) partAmount,
	null,
	null,
	null,
	null
from PartE PartE
where (PartE.partClassification in ('1', '2a', '2b', '3a', '3b', '3c') 
		and PartE.partSection in ('1', '2'))
	or (PartE.partClassification = '5' 
		and PartE.partSection = '1')
	or PartE.partClassification = '7'

-- Order by 2  
