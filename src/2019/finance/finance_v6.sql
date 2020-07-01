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
                                                         - Removed cross join with ConfigPerAsOfDate since values are now already in COASPerFYAsOfDate
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

ReportingDates AS
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
from 
    (select nvl(CASE WHEN upper(reportPeriod.surveySection) = upper(defaultValues.currentSection) THEN reportPeriod.asOfDate END, defaultValues.asOfDate) asOfDate,
			nvl(CASE WHEN upper(reportPeriod.surveySection) = upper(defaultValues.priorSection) THEN reportPeriod.asOfDate END, defaultValues.priorAsOfDate) priorAsOfDate,
        reportPeriod.surveyCollectionYear surveyYear,
            reportPeriod.surveyId surveyId,
            defaultValues.currentSection currentSection,
		    defaultValues.priorSection priorSection,
            defaultValues.finGPFSAuditOpinion finGPFSAuditOpinion,
	        defaultValues.finPellTransactions finPellTransactions,
			defaultValues.finBusinessStructure finBusinessStructure,
			defaultValues.finTaxExpensePaid finTaxExpensePaid,
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
	    repValues.finPellTransactions,
		repValues.finBusinessStructure,
		repValues.finTaxExpensePaid
union

select defaultValues.surveyYear surveyYear,
    defaultValues.surveyId surveyId,
    defaultValues.asOfDate asOfDate,
    defaultValues.priorAsOfDate priorAsOfDate,
    defaultValues.currentSection currentSection,
	defaultValues.priorSection priorSection,
    defaultValues.finGPFSAuditOpinion finGPFSAuditOpinion, 
	defaultValues.finPellTransactions finPellTransactions,
	defaultValues.finBusinessStructure finBusinessStructure,
	defaultValues.finTaxExpensePaid finTaxExpensePaid
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
		finPellTransactions finPellTransactions,
		finBusinessStructure finBusinessStructure,
		finTaxExpensePaid finTaxExpensePaid
from (
    select ReportingDates.surveyYear surveyCollectionYear,
	            ReportingDates.asOfDate asOfDate,
                ReportingDates.priorAsOfDate priorAsOfDate,
                nvl(config.finGPFSAuditOpinion, ReportingDates.finGPFSAuditOpinion) finGPFSAuditOpinion,
                nvl(config.finPellTransactions, ReportingDates.finPellTransactions) finPellTransactions,
                nvl(config.finBusinessStructure, ReportingDates.finBusinessStructure) finBusinessStructure,
                nvl(config.finTaxExpensePaid, ReportingDates.finTaxExpensePaid) finTaxExpensePaid,
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
	  ReportingDates.finPellTransactions finPellTransactions,
	  ReportingDates.finBusinessStructure finBusinessStructure,
	  ReportingDates.finTaxExpensePaid finTaxExpensePaid
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
	select nvl(ABS(ROUND(SUM(CASE WHEN OL.expFAPellGrant = 'Y' THEN OL.endBalance ELSE 0 END))), 0) C1,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.expFANonPellFedGrants = 'Y' THEN OL.endBalance ELSE 0 END))), 0) C2,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.expFAStateGrants = 'Y' THEN OL.endBalance ELSE 0 END))), 0) C3a,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.expFALocalGrants = 'Y' THEN OL.endBalance ELSE 0 END))), 0) C3b,
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.expFAInstitGrantsRestr = 'Y' OR OL.expFAInstitGrantsUnrestr = 'Y') THEN OL.endBalance ELSE 0 END))), 0) C4,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.discAllowTuitionFees = 'Y' THEN OL.endBalance ELSE 0 END))), 0) C6,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.discAllowAuxEnterprise = 'Y' THEN OL.endBalance ELSE 0 END))), 0) C7
	from OL OL 
	where OL.fiscalPeriod = 'Year End'
		and OL.accountType IN ('Expense', 'Revenue Discount')
		and (OL.expFAPellGrant = 'Y'
		   or OL.expFANonPellFedGrants = 'Y'
		   or OL.expFAStateGrants = 'Y'
		   or OL.expFALocalGrants = 'Y'
		   or OL.expFAInstitGrantsRestr = 'Y'
		   or OL.expFAInstitGrantsUnrestr = 'Y'
		   or OL.discAllowTuitionFees = 'Y'
		   or OL.discAllowAuxEnterprise = 'Y')
	) partAmount
		cross join ( select * from
						(VALUES 
							('1'),
							('2'),
							('3'),
							('4'),
							('6'),
							('7')
						) PartClass(partClassification)
				)
		cross join ( select * from
						(VALUES 
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
	select nvl(ABS(ROUND(SUM(CASE WHEN OL.revTuitionAndFees = 'Y' THEN OL.endBalance ELSE 0 END) 
		- SUM(CASE WHEN OL.discAllowTuitionFees = 'Y' THEN OL.endBalance ELSE 0 END))), 0) D1,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.revFedApproprations = 'Y' THEN OL.endBalance ELSE 0 END))), 0) D2a,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.revFedGrantsContractsOper = 'Y' THEN OL.endBalance ELSE 0 END))), 0) D2b,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.revStateApproprations = 'Y' THEN OL.endBalance ELSE 0 END))), 0) D3a,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.revStateGrantsContractsOper = 'Y' THEN OL.endBalance ELSE 0 END))), 0) D3b,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.revLocalApproprations = 'Y' THEN OL.endBalance ELSE 0 END))), 0) D3c,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.revLocalGrantsContractsOper = 'Y' THEN OL.endBalance ELSE 0 END))), 0) D3d,
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.revPrivGifts = 'Y'
								or OL.revPrivGrantsContractsOper = 'Y'
								or OL.revPrivGrantsContractsNOper = 'Y') THEN OL.endBalance ELSE 0 END))), 0) D4,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.revInvestmentIncome = 'Y' THEN OL.endBalance ELSE 0 END))), 0) D5,
			nvl(ABS(ROUND(SUM(CASE WHEN OL.revEducActivSalesServices = 'Y' THEN OL.endBalance ELSE 0 END))), 0) D6,
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.revRealizedCapitalGains != 'Y'
								and OL.revRealizedOtherGains != 'Y'
								and OL.revExtraordGains != 'Y'
								and OL.revOwnerEquityAdjustment != 'Y'
								and OL.revSumOfChangesAdjustment != 'Y') THEN OL.endBalance ELSE 0 END))), 0) D9
		from OL OL 
		where OL.fiscalPeriod = 'Year End'
			and (OL.accountType = 'Revenue'
			   or OL.discAllowTuitionFees = 'Y')
		) partAmount
			cross join ( select * from
							(VALUES 
								('1'),
								('2'),
								('3'),
								('4'),
								('5'),
								('6'),
								('9')
							) PartClass(partClassification)
					)
			cross join ( select * from
							(VALUES 
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
	select nvl(ABS(ROUND(SUM(CASE WHEN (OL.accountType = 'Expense' 
									and OL.isInstruction = 1) THEN OL.endBalance ELSE 0 END))), 0) E1_1, -- Instruction
			nvl(ABS(ROUND(SUM(CASE WHEN ((OL.accountType = 'Expense' 
											and OL.isInstruction = 1)
										and (OL.expSalariesWages = 'Y'
											or OL.expOperMaintSalariesWages = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) E1_2, -- Instruction
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.accountType = 'Expense' 
											and OL.isResearch = 1) THEN OL.endBalance ELSE 0 END))), 0) E2a_1, -- Research
			nvl(ABS(ROUND(SUM(CASE WHEN ((OL.accountType = 'Expense' 
											and OL.isResearch = 1)
										and (OL.expSalariesWages = 'Y'
											or OL.expOperMaintSalariesWages = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) E2a_2, -- Research
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.accountType = 'Expense' 
											and OL.isPublicService = 1) THEN OL.endBalance ELSE 0 END))), 0) E2b_1, -- Public service
			nvl(ABS(ROUND(SUM(CASE WHEN ((OL.accountType = 'Expense' 
											and OL.isPublicService = 1)
										and (OL.expSalariesWages = 'Y'
											or OL.expOperMaintSalariesWages = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) E2b_2, -- Public service
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.accountType = 'Expense' 
											and OL.isAcademicSupport = 1) THEN OL.endBalance ELSE 0 END))), 0) E3a_1, -- Academic support
			nvl(ABS(ROUND(SUM(CASE WHEN ((OL.accountType = 'Expense' 
											and OL.isAcademicSupport = 1)
										and (OL.expSalariesWages = 'Y'
											or OL.expOperMaintSalariesWages = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) E3a_2, -- Academic support
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.accountType = 'Expense' 
											and OL.isStudentServices = 1) THEN OL.endBalance ELSE 0 END))), 0) E3b_1, -- Student services
			nvl(ABS(ROUND(SUM(CASE WHEN ((OL.accountType = 'Expense' 
											and OL.isStudentServices = 1)
										and (OL.expSalariesWages = 'Y'
											or OL.expOperMaintSalariesWages = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) E3b_2, -- Student services
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.accountType = 'Expense' 
											and OL.isInstitutionalSupport = 1) THEN OL.endBalance ELSE 0 END))), 0) E3c_1, -- Institutional support total 
			nvl(ABS(ROUND(SUM(CASE WHEN ((OL.accountType = 'Expense' 
											and OL.isInstitutionalSupport = 1)
										and (OL.expSalariesWages = 'Y'
											or OL.expOperMaintSalariesWages = 'Y')) THEN OL.endBalance ELSE 0 END))), 0) E3c_2, -- Institutional support salaries and wages
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.expFAPellGrant = 'Y'            			--Pell grants
												or OL.expFANonPellFedGrants = 'Y'		--Other federal grants
												or OL.expFAStateGrants = 'Y'			--Grants by state government
												or OL.expFALocalGrants = 'Y'			--Grants by local government
												or OL.expFAInstitGrantsRestr = 'Y'		--Institutional grants from restricted resources
												or OL.expFAInstitGrantsUnrestr = 'Y') 	--Institutional grants from unrestricted resources
										THEN OL.endBalance ELSE 0 END)
				- SUM(CASE WHEN OL.discAllowTuitionFees = 'Y' THEN OL.endBalance  --Discounts and allowances applied to tuition and fees
							ELSE 0 END))), 0) E5_1,
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.expFedIncomeTax != 'Y'
										and OL.expExtraordLosses != 'Y') THEN OL.endBalance ELSE 0 END))), 0) E7_1, -- Total expenses and deductions
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.expSalariesWages = 'Y' or OL.expOperMaintSalariesWages = 'Y') THEN OL.endBalance ELSE 0 END))), 0) E7_2, -- Salaries and wages
			nvl(ABS(ROUND(SUM(CASE WHEN (OL.expBenefits = 'Y' or OL.expOperMaintBenefits = 'Y') THEN OL.endBalance ELSE 0 END))), 0) E7_3, -- Benefits
			nvl(ABS(ROUND(SUM(CASE WHEN OL.expOperMaintOther = 'Y' THEN OL.endBalance ELSE 0 END))), 0) E7_4, -- Operation and Maintenance of Plant
			nvl(ABS(ROUND(SUM(CASE WHEN OL.expDepreciation = 'Y' THEN OL.endBalance ELSE 0 END))), 0) E7_5, -- Depreciation
			nvl(ABS(ROUND(SUM(CASE WHEN OL.expInterest = 'Y' THEN OL.endBalance ELSE 0 END))), 0) E7_6 -- Interest
		from OL OL 
		where OL.fiscalPeriod = 'Year End'
			and (OL.accountType = 'Expense'
			   or (OL.discAllowTuitionFees = 'Y' 
			   or OL.discAllowAuxEnterprise = 'Y'
			   or OL.discAllowPatientContract = 'Y'))
		) partAmount
			cross join ( select * from
							(VALUES 
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
			cross join ( select * from
							(VALUES 
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

--jdh 2020-03-04 Removed cross join with ConfigPerAsOfDate since values are now already in COASPerFYAsOfDate
--swapped CASE Statements for COAS fields used for Section 9 (General Information)

select DISTINCT '9' part,
		CAST(MONTH(nvl(COAS.startDate, COAS.priorAsOfDate))  as BIGINT) field1,
        CAST(YEAR(nvl(COAS.startDate, COAS.priorAsOfDate)) as BIGINT) field2,
        CAST(MONTH(nvl(COAS.endDate, COAS.asOfDate)) as BIGINT) field3,
        CAST(YEAR(nvl(COAS.endDate, COAS.asOfDate)) as BIGINT) field4,
		COAS.finGPFSAuditOpinion  field5, --1=Unqualified, 2=Qualified, 3=Don't know
		COAS.finPellTransactions  field6, --P = Pass through, F = Federal Grant Revenue, N = Does not award Pell grants
		COAS.finBusinessStructure field7  --SP = Sole Proprietorship, P = Partnership, C = C Corp, SC = S Corp, LLC = LLC
from COASPerFYAsOfDate COAS 

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
