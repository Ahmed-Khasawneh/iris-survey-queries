with ReportingDates as (
    select
        max(asOfDate) asOfDate,
        SurveyYear
    from (
        select
            ReportPeriod.asOfDate asOfDate,
            ReportPeriod.surveyCollectionYear SurveyYear,
            row_number() OVER (
                PARTITION BY
                    ReportPeriod.surveyCollectionYear,
                    ReportPeriod.surveySection
                ORDER BY
                    ReportPeriod.recordActivityDate DESC
            ) ReportPeriod_RN
        from IPEDSReportingPeriod ReportPeriod
        where ReportPeriod.surveyCollectionYear = '1920'
        and ReportPeriod.surveyId = 'AL1'
    )
    where ReportPeriod_RN = 1
    group by SurveyYear
),
FYPerAsOfDate as (
    select
        FiscalYear.*
    from FiscalYear FiscalYear
    where FiscalYear.fiscalYear4Char = ( -- grab the matching records since we need more than just the year 4 char
        select
            max(fiscalYear4Char)
        from (
            select
                FY.fiscalYear4Char fiscalYear4Char,
                row_number() OVER (
                    PARTITION BY
                        FY.fiscalYear4Char
                    ORDER BY
                        FY.recordActivityDate DESC
                ) FY_RN
            from ReportingDates ReportingDates
              inner join FiscalYear FY on FY.endDate <= ReportingDates.asOfDate
                   --and FY.recordActivityDate <= ReportingDates.AsOfDate --this line should not be commented out with client data
                   and FY.isIPEDSReportable = 1 --true
        )
        WHERE FY_RN = 1
    )
    and FiscalYear.fiscalPeriod in ('Year Begin', 'Year End')
    and FiscalYear.isIPEDSReportable = 1
    limit 1 -- limit one since we only want one fiscal year
),
LibraryCollectionStatisticFiscalYear as (
    select * from LibraryCollectionStatistic LCS
    where fiscalYear2Char = (select fiscalYear2Char from FYPerAsOfDate)
),
LibraryInventoryByFiscalYear as (
    select LI.* from (
        select
            *,
            row_number() OVER (
                partition by
                    LI.branchId,
                    LI.itemId
                order by
                    LI.recordActivityDate DESC
            ) LibraryInventory_RN
        from LibraryInventory LI
    ) LI
    inner join FYPerAsOfDate FiscalYear on FiscalYear.fiscalYear2Char = LI.fiscalYear2Char
    where LI.LibraryInventory_RN = 1
        and LI.isIPEDSReportable = 1
        and (LI.outOfCirculationDate is null or LI.outOfCirculationDate >= FiscalYear.startDate)
),
LibraryCirculationStatisticByFiscalYear as (
    select * from LibraryCirculationStatistic
    where fiscalYear2Char = (select fiscalYear2Char from FYPerAsOfDate)
),
LibraryItemTransactionByFiscalYear as (
    select
        LIT.*,
        LI.isDigital isDigital
    from (
        select
            *,
            row_number() OVER (
                partition by
                    LIT.itemId
                order by
                    LIT.recordActivityDate DESC
            ) AS LibraryItemTransaction_RN
        from LibraryItemTransaction LIT
    ) LIT
    inner join FYPerAsOfDate FiscalYear on FiscalYear.fiscalYear2Char = LIT.fiscalYear2Char
    left join LibraryInventoryByFiscalYear LI on LI.itemId = LIT.itemId
    where LIT.LibraryItemTransaction_RN = 1
        and LIT.isIPEDSReportable = 1
        and (LIT.checkoutDate >= FiscalYear.startDate and LIT.checkoutDate <= FiscalYear.endDate)
),
InterlibraryLoanStatisticByFiscalYear as (
    select * from InterlibraryLoanStatistic
    where fiscalYear2Char = (select fiscalYear2Char from FYPerAsOfDate)
),
LibraryExpensesByFiscalYear as (
    select * from LibraryExpenses
    where fiscalYear2Char = (select fiscalYear2Char from FYPerAsOfDate)
)


select
    'A' part,
    1 field1, -- Library collections type 1=Physical, 2=Digital/Electronic
    cast(nvl(sum(bookCountPhysical), 0) as BIGINT) field2, -- books 0 to 999999999999, -2 or blank = not-applicable
    -2 field3, -- Databases (Applicable only to Digital/Electronic) 0 to 999999999999, -2 or blank = not-applicable
    cast(nvl(sum(mediaCountPhysical), 0) as BIGINT) field4, -- Media 0 to 999999999999, -2 or blank = not-applicable
    cast(nvl(sum(serialCountPhysical), 0) as BIGINT) field5, -- Serials 0 to 999999999999, -2 or blank = not-applicable
    cast(nvl(sum(circulationCountPhysical), 0) as BIGINT) field6, -- Circulation 0 to 999999999999, -2 or blank = not-applicable
    null field7,
    null field8
from (
    select
        LCS.bookCountPhysical bookCountPhysical,
        LCS.mediaCountPhysical mediaCountPhysical,
        LCS.serialCountPhysical serialCountPhysical,
        (select sum(circulationCountPhysical)
            from LibraryCirculationStatisticByFiscalYear LCS2) circulationCountPhysical
    from LibraryCollectionStatisticFiscalYear LCS

    union

    select
        sum(case when LI.itemType = 'Book' then 1 else 0 end) bookCountPhysical,
        sum(case when LI.itemType = 'Media' then 1 else 0 end) mediaCountPhysical,
        sum(case when LI.itemType = 'Serial' then 1 else 0 end) serialCountPhysical,
        (select count(*) from LibraryItemTransactionByFiscalYear
            where isDigital = 0) circulationCountPhysical
    from LibraryInventoryByFiscalYear LI
    where LI.isDigital = 0
        and (select count(*) from LibraryCollectionStatisticFiscalYear) = 0 -- no records if collection statistic exists
        and (select count(*) from LibraryCirculationStatisticByFiscalYear) = 0 -- no records if circulation statistic exists
)

union

select
    'A' part,
    2 field1, -- Library collections type 1=Physical, 2=Digital/Electronic
    cast(nvl(sum(bookCountDigital), 0) as BIGINT) field2, -- books 0 to 999999999999, -2 or blank = not-applicable
    cast(nvl(sum(databaseCountDigital), 0) as BIGINT) field3, -- Databases (Applicable only to Digital/Electronic) 0 to 999999999999, -2 or blank = not-applicable
    cast(nvl(sum(mediaCountDigital), 0) as BIGINT) field4, -- Media 0 to 999999999999, -2 or blank = not-applicable
    cast(nvl(sum(serialCountDigital), 0) as BIGINT) field5, -- Serials 0 to 999999999999, -2 or blank = not-applicable
    cast(nvl(sum(circulationCountDigital), 0) as BIGINT) field6, -- Circulation 0 to 999999999999, -2 or blank = not-applicable
    null field7,
    null field8
from (
    select
        LCS.bookCountDigital bookCountDigital,
        LCS.databaseCountDigital databaseCountDigital,
        LCS.mediaCountDigital mediaCountDigital,
        LCS.serialCountDigital serialCountDigital,
        (select sum(circulationCountDigital)
            from LibraryCirculationStatisticByFiscalYear LCS2) circulationCountDigital
    from LibraryCollectionStatisticFiscalYear LCS

    union

    select
        sum(case when LI.itemType = 'Book' then 1 else 0 end) bookCountDigital,
        sum(case when LI.itemType = 'Database' then 1 else 0 end) databaseCountDigital,
        sum(case when LI.itemType = 'Media' then 1 else 0 end) mediaCountDigital,
        sum(case when LI.itemType = 'Serial' then 1 else 0 end) serialCountDigital,
        (select count(*) from LibraryItemTransactionByFiscalYear
            where isDigital = 1) circulationCountDigital
    from LibraryInventoryByFiscalYear LI
    where LI.isDigital = 1
        and (select count(*) from LibraryCollectionStatisticFiscalYear) = 0 -- no records if collection statistic exists
        and (select count(*) from LibraryCirculationStatisticByFiscalYear) = 0 -- no records if circulation statistic exists
)

union

select
    'B' part,
    (select count(*) from LibraryBranch where branchStatus = 'Active' and isCentralOrMainBranch = false) field1, -- The number of branch and independent libraries (exclude the main or central library)
    cast(nvl(round(sum(case when LibSalariesAndWages is not null then beginBalance - endBalance else 0 end), 0), 0) as BIGINT) field2, --Total salaries and wages
    cast(nvl(round(sum(case when LibFringeBenefits is not null then beginBalance - endBalance else 0 end), 0), 0) as BIGINT) field3, -- Fringe benefits (if paid by the library budget)
    cast(nvl(round(sum(case when LibOneTimeMaterials is not null then beginBalance - endBalance else 0 end), 0), 0) as BIGINT) field4, -- Materials/services cost - One-time purchases of books, serial backfiles, and other materials
    cast(nvl(round(sum(case when LibServicesSubscriptions is not null then beginBalance - endBalance else 0 end), 0), 0) as BIGINT) field5, -- Materials/services cost - Ongoing commitments to subscriptions
    cast(nvl(round(sum(case when LibOtherMaterialsServices is not null then beginBalance - endBalance else 0 end), 0), 0) as BIGINT) field6, -- Materials/services cost - Other materials/service cost
    cast(nvl(round(sum(case when LibPreservationOps is not null then beginBalance - endBalance else 0 end), 0), 0) as BIGINT) field7, -- Operations and maintenance expenses - Preservation services
    cast(nvl(round(sum(case when LibOtherOpsMaintenance is not null then beginBalance - endBalance else 0 end), 0), 0) as BIGINT) field8 -- Operations and maintenance expenses - All other operations and maintenance expenses
from LibraryExpensesByFiscalYear

union

select
    'C' part,
    cast(nvl(sum(ILS.loansDocumentsProvided), 0) as BIGINT) field1, -- Total interlibrary loans and documents provided to other libraries 0 to 999999999999, -2 or blank = not-applicable
    cast(nvl(sum(ILS.loansDocumentsReceived), 0) as BIGINT) field2, -- Total interlibrary loans and documents received 0 to 999999999999, -2 or blank = not-applicable
    null field3,
    null field4, 
    null field5,
    null field6,
    null field7,
    null field8
from InterlibraryLoanStatisticByFiscalYear ILS
