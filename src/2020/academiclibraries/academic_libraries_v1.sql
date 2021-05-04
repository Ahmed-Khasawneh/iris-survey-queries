/********************

EVI PRODUCT:	DORIS 2020-21 IPEDS Survey  
FILE NAME: 		Academic Libraries (AL1)
FILE DESC:      Academic Libraries: Degree-granting institutions that have library expenses
AUTHOR:         akhasawneh
CREATED:        20210311

SECTIONS:
Reporting Dates/Terms
Libraries
Survey Formatting

Date(yyyymmdd)      Author              Tag             Comments
-----------------   ----------------    -------------   ----------------------------------------------------------------------
20210504	        akhasawneh				             Initial version

********************/ 

/*****
BEGIN SECTION - Reporting Dates/Terms
The views below are used to determine the dates, academic terms, academic year, etc. needed for each survey
*****/

WITH DefaultValues as (

--Assigns all hard-coded values to variables. All date and version adjustments and default values should be modified here.
/*
--Production Default (Begin)
select '2021' surveyYear, 
	'AL1' surveyId,
	'August End' repPeriodTag1,
	'Fiscal Year Lockdown' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
    --Report all data for fiscal year (FY) 2020. Fiscal year 2020 is defined as the most recent 12-month period that ends before October 1, 2020, that corresponds to the institution’s fiscal year.
	CAST('2019-09-30' AS DATE) reportingDateStart,
	CAST('2020-10-01' AS DATE) reportingDateEnd
--Production Default (End)
*/

--Test Default (Begin)
select '1415' surveyYear, 
	'AL1' surveyId,
	'August End' repPeriodTag1,
	'Fiscal Year Lockdown' repPeriodTag2,
	CAST('9999-09-09' as DATE) snapshotDate,  
    --Report all data for fiscal year (FY) 2020. Fiscal year 2020 is defined as the most recent 12-month period that ends before October 1, 2020, that corresponds to the institution’s fiscal year.
	CAST('2013-09-30' AS DATE) reportingDateStart,
	CAST('2014-10-01' AS DATE) reportingDateEnd
--Test Default (End)

),

--No longer needed
/*
ReportingDates as (
    select
        max(asOfDate) asOfDate,
        SurveyYear
    from (
        select
            ReportPeriod.asOfDate asOfDate,
            ReportPeriod.surveyCollectionYear SurveyYear,
            coalesce(row_number() OVER (
                PARTITION BY
                    ReportPeriod.surveyCollectionYear,
                    ReportPeriod.surveySection
                ORDER BY
                    ReportPeriod.recordActivityDate DESC
            ), 1) ReportPeriod_RN
        from IPEDSReportingPeriod ReportPeriod
        where ReportPeriod.surveyCollectionYear = '1920'
        and ReportPeriod.surveyId = 'AL1'
    )
    where ReportPeriod_RN = 1
    group by SurveyYear
),
*/

--Report all data... for the the most recent 12-month period that ends before October 1, YYYY, that corresponds to the institution’s fiscal year.

ChartOfAccountsMCR as (

select * 
from (
    select upper(chartofaccountsENT.chartOfAccountsId) chartOfAccountsId_COA,
--In the case of consortia where individual library members share ALL the same library resources and library budget, a parent/child
--  relationship for reporting Academic Libraries data may be established if certain criteria are met. Parent/child relationships can be
--  established for institutions if: (1) the child institution is in the same institutional control as the parent, and (2) the child
--  institution is not set up to report its own academic libraries expenses or collections data. Once a parent/child relationship has been
--  established, the parent institution will report all data for the child institution. Shared resources are to be reported at the system
--  level. For example, if 20,000 e-book titles were purchased by two institutions in a parent/child relationship to be shared, the parent
--  institution will report 20,000 e-book titles and not 40,000 e-book titles. Institutions wishing to establish a parent/child relationship
--  must contact the Help Desk. See the resource guide for more details on parent/child reporting. 
        upper(chartofaccountsENT.isParent) isParent_COA,
        upper(chartofaccountsENT.isChild) isChild_COA,
        chartofaccountsENT.snapshotDate snapshotDate_COA,
        defvalues.reportingDateEnd reportingDateEnd,
        defvalues.reportingDateStart reportingDateStart,
        coalesce(row_number() OVER (
            PARTITION BY
                chartofaccountsENT.chartOfAccountsId,
                chartofaccountsENT.statusCode
            ORDER BY
                (case when array_contains(chartofaccountsENT.tags, defvalues.repPeriodTag2) then 1 else 2 end) asc, --If client's 'Fiscal Year Lockdown' is in snapshot range use their fiscal end snapshot...
				(case when array_contains(chartofaccountsENT.tags, defvalues.repPeriodTag1) then 1 else 2 end) asc, --...otherwise, use the survey end date snapshot 'August End'
			    chartofaccountsENT.snapshotDate desc,
                chartofaccountsENT.recordActivityDate desc
        ), 1) COA_RN
    from ChartOfAccounts chartofaccountsENT
        cross join DefaultValues defvalues
    where coalesce(chartofaccountsENT.isIPEDSReportable, true) = true
        and to_date(chartofaccountsENT.startDate, 'YYYY-MM-DD') < defvalues.reportingDateEnd
        and (chartofaccountsENT.endDate is null 
            or to_date(chartofaccountsENT.endDate, 'YYYY-MM-DD') >= defvalues.reportingDateStart)
        and chartofaccountsENT.statusCode = 'Active' --We want the most relevant active COA codes, not necessarily the most recent.
    )
where COA_RN = 1
),

FiscalYearMCR as (

select *
from (
    select DISTINCT --FiscalYearENT.*,
        chartofaccounts.chartOfAccountsId_COA chartOfAccountsId_COA,
        chartofaccounts.isParent_COA isParent_COA,
        chartofaccounts.isChild_COA isChild_COA,
        chartofaccounts.snapshotDate_COA snapshotDate_COA,
        chartofaccounts.reportingDateEnd reportingDateEnd,
        chartofaccounts.reportingDateStart reportingDateStart,
        cast(FiscalYearENT.fiscalYear2Char as string) fiscalYear2Char,
        coalesce(row_number() OVER (
            PARTITION BY
                FiscalYearENT.chartOfAccountsId
            ORDER BY
                --report on latest fiscal year before the IPEDS defined date (October 1).
                FiscalYearENT.fiscalYear4Char desc,
                (case when FiscalYearENT.snapshotDate = chartofaccounts.snapshotDate_COA then 1 else 2 end) asc,
			    (case when FiscalYearENT.snapshotDate > chartofaccounts.snapshotDate_COA then FiscalYearENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when FiscalYearENT.snapshotDate < chartofaccounts.snapshotDate_COA then FiscalYearENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                FiscalYearENT.recordActivityDate desc
        ), 1) FY_RN
    from ChartOfAccountsMCR chartofaccounts
        inner join FiscalYear FiscalYearENT on upper(FiscalYearENT.chartOfAccountsId) = chartofaccounts.chartOfAccountsId_COA
    where coalesce(fiscalYearENT.isIPEDSReportable, true) = true
        --and fiscalYearENT.fiscalPeriod in ('Year Begin', 'Year End')
        and fiscalYearENT.fiscalPeriod = 'Year End'
--Report all data... for the the most recent 12-month period that ends before October 1, YYYY, that corresponds to the institution’s fiscal year.
--?? COMMENT OUT FOR TESTING. NO RECORDS EXIST IN SAMPLE DATA
        and to_date(FiscalYearENT.startDate, 'YYYY-MM-DD') < chartofaccounts.reportingDateEnd
        and ((to_date(FiscalYearENT.endDate, 'YYYY-MM-DD') <= chartofaccounts.reportingDateEnd
            and to_date(FiscalYearENT.endDate, 'YYYY-MM-DD') >= chartofaccounts.reportingDateStart)
            or FiscalYearENT.endDate is null)
        
    )
where FY_RN = 1
),

/*****
BEGIN SECTION - Libraries
The views below are used to determine library operating data relating to circulation of the parent and applicable child branches.
*****/

LibraryBranchMCR as (

select *
from (
    select libraryBranchENT.branchId,
        coalesce(libraryBranchENT.isCentralOrMainBranch, false) isCentralOrMainBranch,
        coalesce(row_number() OVER (
            PARTITION BY
                libraryBranchENT.branchId
            ORDER BY
                (case when array_contains(libraryBranchENT.tags, defvalues.repPeriodTag2) then 1 else 2 end) asc, --If client's 'Fiscal Year Lockdown' is in snapshot range use their fiscal end snapshot...
				(case when array_contains(libraryBranchENT.tags, defvalues.repPeriodTag1) then 1 else 2 end) asc, --...otherwise, use the survey end date snapshot 'August End'
			    libraryBranchENT.snapshotDate desc,
                libraryBranchENT.recordActivityDate desc
        ), 1) LB_RN
    from LibraryBranch libraryBranchENT
        cross join DefaultValues defvalues
    where libraryBranchENT.branchStatus = 'Active'
--?? COMMENT OUT FOR TESTING. NO RECORDS EXIST IN SAMPLE DATA
        and ((to_date(libraryBranchENT.recordActivityDate,'YYYY-MM-DD') != CAST('9999-09-09' AS DATE)
            and to_date(libraryBranchENT.recordActivityDate,'YYYY-MM-DD') <= defvalues.reportingDateEnd)
                or to_date(libraryBranchENT.recordActivityDate,'YYYY-MM-DD') = CAST('9999-09-09' AS DATE)) 
        and coalesce(libraryBranchENT.isIPEDSReportable, true) = true
    )
where LB_RN = 1
),

--Aggregated Library data. Not currently supported.
/*
LibraryInventoryMCR as (

select chartOfAccountsId_COA chartOfAccountsId_COA,
    isParent_COA isParent_COA,
    isChild_COA isChild_COA,
    snapshotDate_COA snapshotDate_COA,
    reportingDateEnd reportingDateEnd,
    reportingDateStart reportingDateStart,
    fiscalYear2Char fiscalYear2Char,
    branchId branchId,
    sum(case when itemType = 'Book' and isDigital = 0 then 1 end) bookCountPhysical,
    sum(case when itemType = 'Book' and isDigital = 1 then 1 end) bookCountDigital,
    sum(case when itemType = 'Database' then 1 end) databaseCountDigital,
    sum(case when itemType = 'Media' and isDigital = 0 then 1 end) mediaCountPhysical,
    sum(case when itemType = 'Media' and isDigital = 1 then 1 end) mediaCountDigital,
    sum(case when itemType = 'Serial' and isDigital = 0 then 1 end) serialCountPhysical,
    sum(case when itemType = 'Serial' and isDigital = 1 then 1 end) serialCountDigital
from (
    select --LibraryInventoryENT.*,
        fiscalyear.chartOfAccountsId_COA chartOfAccountsId_COA,
        fiscalyear.isParent_COA isParent_COA,
        fiscalyear.isChild_COA isChild_COA,
        fiscalyear.snapshotDate_COA snapshotDate_COA,
        fiscalyear.reportingDateEnd reportingDateEnd,
        fiscalyear.reportingDateStart reportingDateStart,
        fiscalyear.fiscalYear2Char fiscalYear2Char,
        LibraryInventoryENT.branchId branchId,
        LibraryInventoryENT.itemId itemId,
        LibraryInventoryENT.itemType itemType,
        LibraryInventoryENT.isDigital isDigital,
        coalesce(row_number() OVER (
            PARTITION BY
                fiscalyear.chartOfAccountsId_COA,
                LibraryInventoryENT.branchId,
                LibraryInventoryENT.itemId,
                LibraryInventoryENT.itemType
            ORDER BY
                (case when LibraryInventoryENT.snapshotDate = fiscalyear.snapshotDate_COA then 1 else 2 end) asc,
			    (case when LibraryInventoryENT.snapshotDate > fiscalyear.snapshotDate_COA then LibraryInventoryENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when LibraryInventoryENT.snapshotDate < fiscalyear.snapshotDate_COA then LibraryInventoryENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                LibraryInventoryENT.recordActivityDate desc
        ), 0) LCS_RN
    from FiscalYearMCR fiscalyear
        left join LibraryInventory LibraryInventoryENT 
            on LibraryInventoryENT.fiscalYear2Char = fiscalyear.fiscalYear2Char
            --and upper(LibraryInventoryENT.branchId) = librarycollections.branchId
            and coalesce(LibraryInventoryENT.isIPEDSReportable, true) = true
            and to_date(LibraryInventoryENT.inCirculationDate, 'YYYY-MM-DD') <= fiscalyear.reportingDateEnd
            and (LibraryInventoryENT.outOfCirculationDate is null
                or to_date(LibraryInventoryENT.outOfCirculationDate, 'YYYY-MM-DD') > fiscalyear.reportingDateEnd)
    )
where LCS_RN = 1
group by chartOfAccountsId_COA,
    isParent_COA,
    isChild_COA,
    snapshotDate_COA,
    reportingDateEnd,
    reportingDateStart,
    fiscalYear2Char,
    branchId
),
*/

--
LibraryCollectionStatisticMCR as (
--Include data for the main or central academic library and all branch and independent libraries that were open all or part of the fiscal year
--   2020. Branch and independent libraries are defined as auxiliary library service outlets with quarters separate from the central library
--   that houses the basic collection. The central library administers the branches. Libraries on branch campuses that have separate IPEDS
--   unit identification numbers are reported as separate libraries. 

select *
from (
    select --LibraryCollectionStatisticENT.*,
        fiscalyear.chartOfAccountsId_COA chartOfAccountsId_COA,
        fiscalyear.isParent_COA isParent_COA,
        fiscalyear.isChild_COA isChild_COA,
        fiscalyear.snapshotDate_COA snapshotDate_COA,
        fiscalyear.reportingDateEnd reportingDateEnd,
        fiscalyear.reportingDateStart reportingDateStart,
        fiscalyear.fiscalYear2Char fiscalYear2Char,
        upper(LibraryCollectionStatisticENT.branchId) branchId,
        branch.isCentralOrMainBranch isCentralOrMainBranch,
        LibraryCollectionStatisticENT.bookCountPhysical bookCountPhysical,
        LibraryCollectionStatisticENT.bookCountDigital bookCountDigital,
        LibraryCollectionStatisticENT.databaseCountDigital databaseCountDigital,
        LibraryCollectionStatisticENT.mediaCountPhysical mediaCountPhysical,
        LibraryCollectionStatisticENT.mediaCountDigital mediaCountDigital,
        LibraryCollectionStatisticENT.serialCountPhysical serialCountPhysical,
        LibraryCollectionStatisticENT.serialCountDigital serialCountDigital,
        coalesce(row_number() OVER (
            PARTITION BY
                fiscalyear.chartOfAccountsId_COA,
                LibraryCollectionStatisticENT.branchId
            ORDER BY
                (case when LibraryCollectionStatisticENT.snapshotDate = fiscalyear.snapshotDate_COA then 1 else 2 end) asc,
			    (case when LibraryCollectionStatisticENT.snapshotDate > fiscalyear.snapshotDate_COA then LibraryCollectionStatisticENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when LibraryCollectionStatisticENT.snapshotDate < fiscalyear.snapshotDate_COA then LibraryCollectionStatisticENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                LibraryCollectionStatisticENT.recordActivityDate desc
        ), 1) LCS_RN
    --from LibraryInventoryMCR inventory
    from LibraryBranchMCR branch
        inner join LibraryCollectionStatistic LibraryCollectionStatisticENT 
            on LibraryCollectionStatisticENT.branchId = branch.branchId
            and coalesce(LibraryCollectionStatisticENT.isIPEDSReportable, true) = true
        inner join FiscalYearMCR fiscalyear
            on cast(LibraryCollectionStatisticENT.fiscalYear2Char as string) = fiscalyear.fiscalYear2Char
    )
where LCS_RN = 1
),

LibraryCirculationStatisticMCR as (

select *
from (
    select --LibraryCirculationStatisticENT.*,
        fiscalyear.chartOfAccountsId_COA chartOfAccountsId_COA,
        fiscalyear.isParent_COA isParent_COA,
        fiscalyear.isChild_COA isChild_COA,
        fiscalyear.snapshotDate_COA snapshotDate_COA,
        fiscalyear.reportingDateEnd reportingDateEnd,
        fiscalyear.reportingDateStart reportingDateStart,
        fiscalyear.fiscalYear2Char fiscalYear2Char,
        upper(LibraryCirculationStatisticENT.branchId) branchId,
        branch.isCentralOrMainBranch isCentralOrMainBranch,
        LibraryCirculationStatisticENT.circulationCountPhysical circulationCountPhysical,
        LibraryCirculationStatisticENT.circulationCountDigital circulationCountDigital,
        coalesce(row_number() OVER (
            PARTITION BY
                fiscalyear.chartOfAccountsId_COA,
                LibraryCirculationStatisticENT.branchId
            ORDER BY
                (case when LibraryCirculationStatisticENT.snapshotDate = fiscalyear.snapshotDate_COA then 1 else 2 end) asc,
			    (case when LibraryCirculationStatisticENT.snapshotDate > fiscalyear.snapshotDate_COA then LibraryCirculationStatisticENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when LibraryCirculationStatisticENT.snapshotDate < fiscalyear.snapshotDate_COA then LibraryCirculationStatisticENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                LibraryCirculationStatisticENT.recordActivityDate desc
        ), 1) LCS_RN
    from LibraryBranchMCR branch
        inner join LibraryCirculationStatistic LibraryCirculationStatisticENT 
            on LibraryCirculationStatisticENT.branchId = branch.branchId
            and coalesce(LibraryCirculationStatisticENT.isIPEDSReportable, true) = true
        inner join FiscalYearMCR fiscalyear
            on cast(LibraryCirculationStatisticENT.fiscalYear2Char as string) = fiscalyear.fiscalYear2Char

    )
where LCS_RN = 1
),

InterlibraryLoanStatisticMCR as (

select *
from (
    select --InterlibraryLoanStatisticENT.*,
        fiscalyear.chartOfAccountsId_COA chartOfAccountsId_COA,
        fiscalyear.isParent_COA isParent_COA,
        fiscalyear.isChild_COA isChild_COA,
        fiscalyear.snapshotDate_COA snapshotDate_COA,
        fiscalyear.reportingDateEnd reportingDateEnd,
        fiscalyear.reportingDateStart reportingDateStart,
        fiscalyear.fiscalYear2Char fiscalYear2Char,
        upper(InterlibraryLoanStatisticENT.branchId) branchId,
        branch.isCentralOrMainBranch isCentralOrMainBranch,
        InterlibraryLoanStatisticENT.loansDocumentsProvided loansDocumentsProvided,
        InterlibraryLoanStatisticENT.loansDocumentsReceived loansDocumentsReceived,
        coalesce(row_number() OVER (
            PARTITION BY
                fiscalyear.chartOfAccountsId_COA,
                InterlibraryLoanStatisticENT.branchId
            ORDER BY
                (case when InterlibraryLoanStatisticENT.snapshotDate = fiscalyear.snapshotDate_COA then 1 else 2 end) asc,
			    (case when InterlibraryLoanStatisticENT.snapshotDate > fiscalyear.snapshotDate_COA then InterlibraryLoanStatisticENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                (case when InterlibraryLoanStatisticENT.snapshotDate < fiscalyear.snapshotDate_COA then InterlibraryLoanStatisticENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                InterlibraryLoanStatisticENT.recordActivityDate desc
        ),1) ILL_RN
    from LibraryBranchMCR branch
        inner join InterlibraryLoanStatistic InterlibraryLoanStatisticENT
            on InterlibraryLoanStatisticENT.branchId = branch.branchId
            and coalesce(InterlibraryLoanStatisticENT.isIPEDSReportable, true) = true
        inner join FiscalYearMCR fiscalyear
            on cast(InterlibraryLoanStatisticENT.fiscalYear2Char as string) = fiscalyear.fiscalYear2Char
    )
where ILL_RN = 1
),

LibraryExpensesMCR as (
--Library expenses reported only if library expenses >= $100,000

select chartOfAccountsId_COA chartOfAccountsId_COA,
    isParent_COA isParent_COA,
    isChild_COA isChild_COA,
    snapshotDate_COA snapshotDate_COA,
    reportingDateEnd reportingDateEnd,
    reportingDateStart reportingDateStart,
    fiscalYear2Char fiscalYear2Char,
    branchId branchId,
    isCentralOrMainBranch isCentralOrMainBranch,
    round(sum(libFringeBenefits), 2) LibFringeBenefits,
    round(sum(LibOneTimeMaterials), 2) LibOneTimeMaterials,
    round(sum(LibOtherMaterialsServices), 2) LibOtherMaterialsServices,
    round(sum(LibOtherOpsMaintenance), 2) LibOtherOpsMaintenance,
    round(sum(LibPreservationOps), 2) LibPreservationOps,
    round(sum(LibServicesSubscriptions), 2) LibServicesSubscriptions,
    round(sum(libSalariesAndWagesLibrarians) 
            + sum(libSalariesAndWagesProfessionals) 
            + sum(libSalariesAndWagesOther) 
            + sum(libSalariesAndWagesStudents), 2) libSalariesAndWages,
    round(sum(libSalariesAndWagesLibrarians), 2) libSalariesAndWagesLibrarians,
    round(sum(libSalariesAndWagesProfessionals), 2) libSalariesAndWagesProfessionals,
    round(sum(libSalariesAndWagesOther), 2) libSalariesAndWagesOther,
    round(sum(libSalariesAndWagesStudents), 2) libSalariesAndWagesStudents
from (
    select fiscalyear.chartOfAccountsId_COA chartOfAccountsId_COA,
        fiscalyear.isParent_COA isParent_COA,
        fiscalyear.isChild_COA isChild_COA,
        fiscalyear.snapshotDate_COA snapshotDate_COA,
        fiscalyear.reportingDateEnd reportingDateEnd,
        fiscalyear.reportingDateStart reportingDateStart,
        fiscalyear.fiscalYear2Char fiscalYear2Char,
        upper(LibraryExpensesENT.branchId) branchId,
        branch.isCentralOrMainBranch isCentralOrMainBranch,
        LibraryExpensesENT.libFringeBenefits LibFringeBenefits,
        LibraryExpensesENT.LibOneTimeMaterials LibOneTimeMaterials,
        LibraryExpensesENT.LibOtherMaterialsServices LibOtherMaterialsServices,
        LibraryExpensesENT.LibOtherOpsMaintenance LibOtherOpsMaintenance,
        LibraryExpensesENT.LibPreservationOps LibPreservationOps,
        LibraryExpensesENT.LibServicesSubscriptions LibServicesSubscriptions,
        LibraryExpensesENT.libSalariesAndWagesLibrarians libSalariesAndWagesLibrarians, 
        LibraryExpensesENT.libSalariesAndWagesProfessionals libSalariesAndWagesProfessionals, 
        LibraryExpensesENT.libSalariesAndWagesOther libSalariesAndWagesOther,
        LibraryExpensesENT.libSalariesAndWagesStudents libSalariesAndWagesStudents,
        coalesce(row_number() OVER (
            PARTITION BY
                fiscalyear.chartOfAccountsId_COA,
                LibraryExpensesENT.branchId
            ORDER BY
                LibraryExpensesENT.snapshotDate desc,
                LibraryExpensesENT.recordActivityDate desc
        ),1) LE_RN
    from LibraryBranchMCR branch
        inner join LibraryExpenses LibraryExpensesENT 
            on cast(LibraryExpensesENT.branchId as string) = cast(branch.branchId as string)
            and coalesce(LibraryExpensesENT.isIPEDSReportable, true) = true
        inner join FiscalYearMCR fiscalyear
            on cast(LibraryExpensesENT.fiscalYear2Char as string) = cast(fiscalyear.fiscalYear2Char as string)
    )
where LE_RN = 1
group by chartOfAccountsId_COA,
    isParent_COA,
    isChild_COA,
    snapshotDate_COA,
    reportingDateEnd,
    reportingDateStart,
    fiscalYear2Char,
    branchId,
    isCentralOrMainBranch
)

/*****
BEGIN SECTION - Formatting Views
The views below are used to ensure that records exist for all IPEDS expected values even if the query result set doesn't contain records that meet all value conditions.
*****/

--Part A: Library Collections/Circulation
select
    'A' part,
    1 field1, -- Library collections type 1=Physical, 2=Digital/Electronic
    round(sum(bookCount), 2) field2, -- books 0 to 999999999999, -2 or blank = not-applicable
    null field3, -- Databases (Applicable only to Digital/Electronic) 0 to 999999999999, -2 or blank = not-applicable
    round(sum(mediaCount), 2) field4, -- Media 0 to 999999999999, -2 or blank = not-applicable
    round(sum(serialCount), 2) field5, -- Serials 0 to 999999999999, -2 or blank = not-applicable
    round(sum(circulationCountPhysical), 2) field6, -- Circulation 0 to 999999999999, -2 or blank = not-applicable
    null field7,
    null field8
from (
    select
        collection.bookCountPhysical bookCount,
        null databaseCount,
        collection.mediaCountPhysical mediaCount,
        collection.serialCountPhysical serialCount,
        circulation.circulationCountPhysical circulationCountPhysical
    from LibraryBranchMCR branch
        left join LibraryCollectionStatisticMCR collection
            on collection.branchId = branch.branchId
--            and collection.isParent_COA = 'Y'
        left join LibraryCirculationStatisticMCR circulation
            on circulation.branchId = branch.branchId
--            and circulation.isParent_COA = 'Y'
--    where branch.isCentralOrMainBranch = true
    )

union

select
    'A' part,
    2 field1, -- Library collections type 1=Physical, 2=Digital/Electronic
    round(sum(bookCount), 2) field2, -- books 0 to 999999999999, -2 or blank = not-applicable
    round(sum(databaseCount), 2) field3, -- Databases (Applicable only to Digital/Electronic) 0 to 999999999999, -2 or blank = not-applicable
    round(sum(mediaCount), 2) field4, -- Media 0 to 999999999999, -2 or blank = not-applicable
    round(sum(serialCount), 2) field5, -- Serials 0 to 999999999999, -2 or blank = not-applicable
    round(sum(circulationCountDigital), 2) field6, -- Circulation 0 to 999999999999, -2 or blank = not-applicable
    null field7,
    null field8
from (
    select
        collection.bookCountDigital bookCount,
        collection.databaseCountDigital databaseCount,
        collection.mediaCountDigital mediaCount,
        collection.serialCountDigital serialCount,
        circulation.circulationCountDigital circulationCountDigital
    from LibraryBranchMCR branch
        left join LibraryCollectionStatisticMCR collection
            on collection.branchId = branch.branchId
--            and collection.isParent_COA = 'Y'
        left join LibraryCirculationStatisticMCR circulation
            on circulation.branchId = branch.branchId
--            and circulation.isParent_COA = 'Y'
--    where branch.isCentralOrMainBranch = true
    )
    
union

--Part B: Expenses (Library expenses >= $100,000)
--If annual total library expenses are less than $100,000, the institution will submit Section I of the AL component. If annual total library expenses are equal to or greater than $100,000, 
--the institution will report Section I and additional expenses and interlibrary services information in Section II of the AL component.  

select *
from (
select 'B' part, --part
    coalesce(sum(case when isChild_COA = 'Y' then 1 end), 0) field1, --The number of branch and independent libraries (exclude the main or central library) -0 to 999999
    sum(libSalariesAndWages) field2, --Total salaries and wages -0 to 999999999999, -2 or blank = not-applicable
    sum(LibFringeBenefits) field3, --Fringe benefits (if paid by the library budget) -0 to 999999999999, -2 or blank = not-applicable
    sum(LibOneTimeMaterials) field4, --Materials/services cost - One-time purchases of books, serial backfiles, and other materials -0 to 999999999999, -2 or blank = not-applicable
    sum(LibServicesSubscriptions) field5, --Materials/services cost - Ongoing commitments to subscriptions -0 to 999999999999, -2 or blank = not-applicable
    sum(LibOtherMaterialsServices) field6, --Materials/services cost - Other materials/service cost -0 to 999999999999, -2 or blank = not-applicable
    sum(LibPreservationOps) field7, --Operations and maintenance expenses - Preservation services -0 to 999999999999, -2 or blank = not-applicable
    sum(LibOtherOpsMaintenance) field8 --Operations and maintenance expenses - All other operations and maintenance expenses -0 to 999999999999, -2 or blank = not-applicable
from LibraryExpensesMCR expenses
--where expenses.isParent_COA = 'Y' --Report on all branches
--    and expenses.isCentralOrMainBranch = true
) 

       
union

--Part C: Interlibrary Loan Services and Library Staff
--Report FTEs supported from the library budget. However, if known, if significant, and if specifically for library business, include FTEs funded from sources outside of the library budget. 
--For example, for staffing counts, you may include full counts for federal work-study students working for the library, but do not include counts for maintenance and custodial staff.

select 'C' part,
    sum(loansDocumentsProvided) field1, --Total interlibrary loans and documents provided to other libraries -0 to 999999999999, -2 or blank = not-applicable
    sum(loansDocumentsReceived) field2, --Total interlibrary loans and documents received	0 to 999999999999, -2 or blank = not-applicable
    sum(libSalariesAndWagesLibrarians) field3,  --Librarians - Number of FTEs	0 to 999999.00, -2 or blank = not-applicable
    sum(libSalariesAndWagesProfessionals) field4,  --Other Professional Staff - Number of FTEs	0 to 999999.00, -2 or blank = not-applicable
    sum(libSalariesAndWagesOther) field5, --All Other Paid Staff (Except Student Assistants) - Number of FTEs	0 to 999999.00, -2 or blank = not-applicable
    sum(libSalariesAndWagesStudents) field6, --Student Assistants - Number of FTEs	0 to 999999.00, -2 or blank = not-applicable
    null field7,
    null field8
from (
    select loan.loansDocumentsProvided loansDocumentsProvided,
        loan.loansDocumentsReceived loansDocumentsReceived,
        expenses.libSalariesAndWagesLibrarians libSalariesAndWagesLibrarians,  --Librarians - Number of FTEs	0 to 999999.00, -2 or blank = not-applicable
        expenses.libSalariesAndWagesProfessionals libSalariesAndWagesProfessionals,  --Other Professional Staff - Number of FTEs	0 to 999999.00, -2 or blank = not-applicable
        expenses.libSalariesAndWagesOther libSalariesAndWagesOther, --All Other Paid Staff (Except Student Assistants) - Number of FTEs	0 to 999999.00, -2 or blank = not-applicable
        expenses.libSalariesAndWagesStudents libSalariesAndWagesStudents --Student Assistants - Number of FTEs	0 to 999999.00, -2 or blank = not-applicable
    from LibraryBranchMCR branch
        left join LibraryExpensesMCR expenses 
            on expenses.branchId = branch.branchId
--            and expenses.isParent_COA = 'Y'
        left join InterlibraryLoanStatisticMCR loan
            on loan.branchId = branch.branchId
--            and loan.isParent_COA = 'Y'
--    where branch.isCentralOrMainBranch = true
    )
