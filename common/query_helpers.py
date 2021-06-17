def func_ipedsReportingPeriod(surveyYear = "2021", surveyId = 'EF1', repPeriodTag1 = '', repPeriodTag2 = '', repPeriodTag3 = '', repPeriodTag4 = '', repPeriodTag5 = '', dmVersion = ''):

    if surveyYear != '':
        surveyYear = f"and surveyCollectionYear = '{surveyYear}'"
        
    if surveyId != '':

        surveyId = f" and upper(surveyId) = upper('{surveyId}')"
    
    if repPeriodTag1 != '':
        repPeriodTag1 = f"when array_contains(tags, '{repPeriodTag1}') then 1"

    if repPeriodTag2 != '':
        repPeriodTag2 = f"when array_contains(tags, '{repPeriodTag2}') then 2"

    if repPeriodTag3 != '':
        repPeriodTag3 = f"when array_contains(tags, '{repPeriodTag3}') then 3"

    if repPeriodTag4 != '':
        repPeriodTag4 = f"when array_contains(tags, '{repPeriodTag4}') then 4"
        
    if repPeriodTag5 != '':
        repPeriodTag5 = f"when array_contains(tags, '{repPeriodTag5}') then 5"
        
    str_ipedsReportingPeriod = f"""
        select *
        from (
			select partOfTermCode partOfTermCode, --1
				to_date(recordActivityDate, 'YYYY-MM-DD') recordActivityDate, --'9999-09-09'
				surveyCollectionYear  surveyCollectionYear, --2021
				upper(surveyId) surveyId,
				upper(surveyName) surveyName,
				upper(surveySection) surveySection,
				upper(termCode) termCode,
				snapshotDate snapshotDate,
				tags tags,
				row_number() over (	
					partition by 
						surveyCollectionYear,
						surveyId,
						surveySection, 
						termCode,
						partOfTermCode	
					order by 
						(case when 1=2 then 0
							{repPeriodTag1} --"when array_contains(tags, '{repPeriodTag1}') then 1"
							{repPeriodTag2} --"when array_contains(tags, '{repPeriodTag2}') then 2"
							{repPeriodTag3} --"when array_contains(tags, '{repPeriodTag3}') then 3"
							{repPeriodTag4} --"when array_contains(tags, '{repPeriodTag4}') then 4"
							{repPeriodTag5} --"when array_contains(tags, '{repPeriodTag5}') then 5"
							 else 6 end) asc,
						snapshotDate desc,
						coalesce(recordActivityDate, CAST('9999-09-09' as DATE)) desc
				) reportPeriodRn	
			from IPEDSReportingPeriod 
			where 1=1
				{surveyId} --"upper(surveyId) = upper({surveyId})"
				{surveyYear} --"and surveyCollectionYear = '{surveyYear}'"
				and termCode is not null
				--and partOfTermCode is not null
        )
        where reportPeriodRn = 1
    """
    return str_ipedsReportingPeriod

def func_ipedsClientConfig(surveyYear = "2021", snapshotDate = "9999-09-09", dmVersion = ''):

    if surveyYear != '':
        surveyYear = f"and surveyCollectionYear = '{surveyYear}'"
        
    if snapshotDate != '':
        snapshotDate = f"to_date('{snapshotDate}', 'YYYY/MM/DD HH:MI:SS')"
    
    str_ipedsClientConfig = f"""
		select * 
		from (
			select upper(acadOrProgReporter) acadOrProgReporter, --0 'A'
				upper(admAdmissionTestScores) admAdmissionTestScores, --1 'R'
				upper(admCollegePrepProgram) admCollegePrepProgram, --2 'R'
				upper(admDemoOfCompetency) admDemoOfCompetency, --3 'R'
				upper(admOtherTestScores)admOtherTestScores,  --4 'R'
				upper(admRecommendation) admRecommendation, --5 'R'
				upper(admSecSchoolGPA) admSecSchoolGPA, --6 'R'
				upper(admSecSchoolRank) admSecSchoolRank, --7 'R'
				upper(admSecSchoolRecord) admSecSchoolRecord, --8 'R'
				upper(admTOEFL) admTOEFL, --9 'R'
				upper(admUseForBothSubmitted)admUseForBothSubmitted, --10 'B'
				upper(admUseForMultiOfSame) admUseForMultiOfSame, --11 'H'
				upper(admUseTestScores) admUseTestScores, --12 'B'
				upper(compGradDateOrTerm) compGradDateOrTerm, --13 'D'
				upper(eviReserved1) eviReserved1, --14 ''
				upper(eviReserved2) eviReserved2, --15 ''
				upper(eviReserved3) eviReserved3, --16 ''
				upper(eviReserved4) eviReserved4, --17 ''
				upper(eviReserved5)  eviReserved5, --18 ''
				upper(feIncludeOptSurveyData) feIncludeOptSurveyData, --19 'Y'
				upper(finAthleticExpenses) finAthleticExpenses, --20 'A'
				upper(finBusinessStructure) finBusinessStructure, --21 'LLC'
				upper(finEndowmentAssets) finEndowmentAssets, --22 'Y'
				upper(finGPFSAuditOpinion) finGPFSAuditOpinion, --23 'U'
				upper(finParentOrChildInstitution) finParentOrChildInstitution, --24 'P'
				upper(finPellTransactions) finPellTransactions, --25 'P'
				upper(finPensionBenefits) finPensionBenefits, --26 'Y'
				upper(finReportingModel) finReportingModel, --27 'B'
				upper(finTaxExpensePaid) finTaxExpensePaid, --28 'B'
				upper(fourYrOrLessInstitution) fourYrOrLessInstitution, --29 'F'
				upper(genderForNonBinary) genderForNonBinary, --30 'F'
				upper(genderForUnknown) genderForUnknown, --31 'F'
				upper(grReportTransferOut) grReportTransferOut, --32 'N'
				upper(hrIncludeSecondarySalary) hrIncludeSecondarySalary, --33 'N'
				upper(icOfferDoctorAwardLevel) icOfferDoctorAwardLevel, --34 'Y'
				upper(icOfferGraduateAwardLevel) icOfferGraduateAwardLevel, --35 'Y'
				upper(icOfferUndergradAwardLevel) icOfferUndergradAwardLevel, --36 'Y'
				upper(includeNonDegreeAsUG) includeNonDegreeAsUG, --37 'Y'
				upper(instructionalActivityType) instructionalActivityType, --38 'CR'
				upper(ncBranchCode) ncBranchCode, --39 '00'
				upper(ncSchoolCode) ncSchoolCode, --40 '000000'
				upper(ncSchoolName) ncSchoolName, --41 'XXXXX'
				upper(publicOrPrivateInstitution) publicOrPrivateInstitution, --42 'U'
				upper(recordActivityDate) recordActivityDate, --43 '9999-09-09'
				upper(sfaGradStudentsOnly) sfaGradStudentsOnly, --44 'N'
				upper(sfaLargestProgCIPC) sfaLargestProgCIPC, --45 null
				upper(sfaReportPriorYear) sfaReportPriorYear, --46 'N'
				upper(sfaReportSecondPriorYear) sfaReportSecondPriorYear, --47 'N'
				upper(surveyCollectionYear) surveyCollectionYear, --48 '2021'
				upper(tmAnnualDPPCreditHoursFTE) tmAnnualDPPCreditHoursFTE, --49 '12'
				snapshotDate, --50
				coalesce(row_number() over (
					partition by
						surveyCollectionYear
					order by
					    (case when to_date(snapshotDate, 'YYYY/MM/DD HH:MI:SS') = {snapshotDate} then 1 else 2 end) asc,
					    (case when to_date(snapshotDate, 'YYYY/MM/DD HH:MI:SS') > {snapshotDate} then to_date(snapshotDate,'YYYY/MM/DD HH:MI:SS') else CAST('9999-09-09' as DATE) end) asc,
                        (case when to_date(snapshotDate, 'YYYY/MM/DD HH:MI:SS') < {snapshotDate} then to_date(snapshotDate,'YYYY/MM/DD HH:MI:SS') else CAST('1900-09-09' as DATE) end) desc,
						coalesce(recordActivityDate, CAST('9999-09-09' as DATE)) desc
				), 1) configRn
        from ipedsClientConfig 
        where 1=1 
        {surveyYear} --" and surveyCollectionYear = '{surveyYear}' "
        )
    where configRn = 1 
    """
    return str_ipedsClientConfig

def func_academicTerm(repPeriodTag1 = '', repPeriodTag2 = '', repPeriodTag3 = '', repPeriodTag4 = '', repPeriodTag5 = '', dmVersion = ''):
    
    if repPeriodTag1 != '':
        repPeriodTag1 = f"when array_contains(tags, '{repPeriodTag1}') then 1"

    if repPeriodTag2 != '':
        repPeriodTag2 = f"when array_contains(tags, '{repPeriodTag2}') then 2"

    if repPeriodTag3 != '':
        repPeriodTag3 = f"when array_contains(tags, '{repPeriodTag3}') then 3"

    if repPeriodTag4 != '':
        repPeriodTag4 = f"when array_contains(tags, '{repPeriodTag4}') then 4"
        
    if repPeriodTag5 != '':
        repPeriodTag5 = f"when array_contains(tags, '{repPeriodTag5}') then 5"
        
    str_academicTerm = f"""
		select *
		from (
			select DISTINCT 
				acadTerm.academicYear academicYear, 
				acadTerm.censusDate censusDate, 
				acadTerm.endDate endDate, 
				acadTerm.financialAidYear financialAidYear,
				acadTerm.isIPEDSReportable isIPEDSReportable,
				upper(acadTerm.partOfTermCode) partOfTermCode, 
				acadTerm.partOfTermCodeDescription partOfTermCodeDescription,
				acadTerm.recordActivityDate recordActivityDate,
		        coalesce(acadTerm.requiredFTCreditHoursGR, 9) requiredFTCreditHoursGR,
	            coalesce(acadTerm.requiredFTCreditHoursUG, 12) requiredFTCreditHoursUG,
	            coalesce(acadTerm.requiredFTClockHoursUG, 24) requiredFTClockHoursUG,
	            coalesce(acadterm.requiredFTCreditHoursUG/coalesce(acadterm.requiredFTClockHoursUG, acadterm.requiredFTCreditHoursUG), 1) equivCRHRFactor,
				acadTerm.startDate startDate, 
				acadTerm.termClassification termClassification, 
				upper(acadTerm.termCode) termCode, 
				acadTerm.termCodeDescription termCodeDescription,
				acadTerm.termType termType,
				termOrder.termOrder termOrder,
				termOrder.maxCensus maxCensus,
				termOrder.minStart minStart,
				termOrder.maxEnd maxEnd,
				row_number() over (
					partition by 
						acadTerm.snapshotDate,
						acadTerm.termCode,
						acadTerm.partOfTermCode
					order by
						(case when 1=2 then 0
						    {repPeriodTag1} --"when array_contains(tags, '{repPeriodTag1}') then 1"
						    {repPeriodTag2} --"when array_contains(tags, '{repPeriodTag2}') then 2"
						    {repPeriodTag3} --"when array_contains(tags, '{repPeriodTag3}') then 3"
						    {repPeriodTag4} --"when array_contains(tags, '{repPeriodTag4}') then 4"
						    {repPeriodTag5} --"when array_contains(tags, '{repPeriodTag5}') then 5"
						 else 6 end) asc,
						acadTerm.recordActivityDate desc
				) acadTermRn
			from AcademicTerm acadTerm 
			inner join (
				select termCode termCode, 
					max(termOrder) termOrder,
					max(censusDate) maxCensus,
					min(startDate) minStart,
					max(endDate) maxEnd,
					termType termType
				from (
					select termCode termCode,
						partOfTermCode partOfTermCode,
						termType termType,
						censusDate censusDate,
						startDate startDate,
						endDate endDate,
						row_number() over (
							order by  
								censusDate asc
						) termOrder
					from (
						select DISTINCT 
							acadTerm1.censusDate censusDate, 
							upper(acadTerm1.partOfTermCode) partOfTermCode, 
							upper(acadTerm1.termCode) termCode, 
							acadTerm1.termType termType,
							acadTerm1.startDate startDate,
							acadTerm1.endDate endDate,
							row_number() over (
								partition by 
									acadTerm1.snapshotDate,
									acadTerm1.termCode,
									acadTerm1.partOfTermCode
								order by
									(case when 1=2 then 0
										{repPeriodTag1} --"when array_contains(tags, '{repPeriodTag1}') then 1"
										{repPeriodTag2} --"when array_contains(tags, '{repPeriodTag2}') then 2"
										{repPeriodTag3} --"when array_contains(tags, '{repPeriodTag3}') then 3"
										{repPeriodTag4} --"when array_contains(tags, '{repPeriodTag4}') then 4"
										{repPeriodTag5} --"when array_contains(tags, '{repPeriodTag5}') then 5"
									 else 6 end) asc,
										acadTerm1.recordActivityDate desc
							) acadTermRn
							from AcademicTerm acadTerm1 
							where coalesce(acadTerm1.isIPEDSReportable, true) = true
						)
					where acadTermRn = 1
					)
				group by termCode, termType
				) as termOrder on termOrder.termCode = acadTerm.termCode
			where coalesce(acadTerm.isIPEDSReportable, true) = true
			) 
		where acadTermRn = 1																					
    """
    return str_academicTerm

def func_reportingPeriodRefactor():
        
    str_reportingPeriod = f"""   
		select coalesce(repPerTerms.yearType, 'CY') yearType,
			repPerTerms.surveySection surveySection,
			--repPerTerms.repPeriodTag1 repPeriodTag1,
			--repPerTerms.repPeriodTag2 repPeriodTag2,
			repPerTerms.termCode termCode,
			repPerTerms.partOfTermCode partOfTermCode,
			repPerTerms.financialAidYear financialAidYear,
			repPerTerms.termOrder termOrder,
			repPerTerms.maxCensus maxCensus,
			repPerTerms.snapshotDate snapshotDate,
			repPerTerms.termClassification termClassification,
			repPerTerms.termType termType,
			repPerTerms.startDate startDate,
			repPerTerms.endDate endDate,
			repPerTerms.censusDate censusDate,
			repPerTerms.requiredFTCreditHoursGR,
			repPerTerms.requiredFTCreditHoursUG,
			repPerTerms.requiredFTClockHoursUG,
			repPerTerms.equivCRHRFactor equivCRHRFactor,
			(case when repPerTerms.termClassification = 'Standard Length' then 1
				 when repPerTerms.termClassification is null then (case when repPerTerms.termType in ('Fall', 'Spring') then 1 else 2 end)
				 else 2
			end) fullTermOrder
		from (
			select distinct 'CY' yearType,
				repper.surveySection surveySection,
				repper.termCode termCode,
				repper.partOfTermCode partOfTermCode,
				repper.financialAidYear financialAidYear,
				coalesce(repper.snapshotDate, repper.snapshotDate) snapshotDate,
				--repper.tags tags,
				--repper.repPeriodTag1 repPeriodTag1,
				--repper.repPeriodTag2 repPeriodTag2,
				coalesce(repper.censusDate, repper.censusDate) censusDate,
				repper.termOrder termOrder,
				repper.maxCensus maxCensus,
				repper.termClassification termClassification,
				repper.termType termType,
				repper.startDate startDate,
				repper.endDate endDate,
				repper.requiredFTCreditHoursGR,
				repper.requiredFTCreditHoursUG,
				repper.requiredFTClockHoursUG,
				coalesce(repper.requiredFTCreditHoursUG/
					coalesce(repper.requiredFTClockHoursUG, repper.requiredFTCreditHoursUG), 1) equivCRHRFactor,
				coalesce(row_number() over (
					partition by 
						repper.termCode,
						repper.partOfTermCode
					order by
						(case when repper.snapshotDate <= to_date(date_add(repper.censusdate, 3), 'YYYY-MM-DD') 
									and repper.snapshotDate >= to_date(date_sub(repper.censusDate, 1), 'YYYY-MM-DD') 
									and ((array_contains(repper.tags, 'Fall Census') and repper.termType = 'Fall')
										or (array_contains(repper.tags, 'Spring Census') and repper.termType = 'Spring')
										or (array_contains(repper.tags, 'Pre-Fall Summer Census') and repper.termType = 'Summer')
										or (array_contains(repper.tags, 'Post-Fall Summer Census') and repper.termType = 'Summer')) then 1
							  when repper.snapshotDate <= to_date(date_add(repper.censusdate, 3), 'YYYY-MM-DD') 
									and repper.snapshotDate >= to_date(date_sub(repper.censusDate, 1), 'YYYY-MM-DD') then 2
							 else 3 end) asc,
						(case when repper.snapshotDate > repper.censusDate then repper.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
						(case when repper.snapshotDate < repper.censusDate then repper.snapshotDate else CAST('1900-09-09' as DATE) end) desc
					), 1) acadTermRnReg
			 from global_reportingPeriodOrder repper
				) repPerTerms
		where repPerTerms.acadTermRnReg = 1 
    """
    
    str_reportingPeriodRefactor = f"""
        select rep.*,
            (case when rep.termType = 'Summer' and rep.termClassification != 'Standard Length' then 
                    case when (select max(rep2.termOrder)
                    from ({str_reportingPeriod}) rep2
                    where rep2.termType = 'Summer') < (select max(rep2.termOrder)
                                                        from ({str_reportingPeriod}) rep2
                                                        where rep2.termType = 'Fall') then 'Pre-Fall Summer'
                    else 'Post-Spring Summer' end
                else rep.termType end) termTypeNew,
            potMax.partOfTermCode maxPOT
        from ({str_reportingPeriod}) rep
        inner join (select rep3.termCode,
                        rep3.partOfTermCode,
                       row_number() over (
			                partition by
			                    rep3.termCode
			                order by
				                rep3.censusDate desc,
				                rep3.endDate desc
		                ) potRn
                from ({str_reportingPeriod}) rep3) potMax on rep.termCode = potMax.termCode
                        and potMax.potRn = 1
    """
    
    return str_reportingPeriodRefactor

def func_person(snapshotDate = '', censusDate = '9999-09-09', genderForNonBinary = '', genderForUnknown = ''):
		
    if snapshotDate != '':
        snapshotDate = f"""
            (case when CAST(snapshotDate as TIMESTAMP) = CAST('{snapshotDate}' as TIMESTAMP) then 1 else 2 end) asc,
            (case when CAST(snapshotDate as TIMESTAMP) > CAST('{snapshotDate}' as TIMESTAMP) then CAST(snapshotDate as TIMESTAMP) else CAST('9999-09-09' as TIMESTAMP) end) asc,
            (case when CAST(snapshotDate as TIMESTAMP) < CAST('{snapshotDate}' as TIMESTAMP) then CAST(snapshotDate as TIMESTAMP) else CAST('1900-09-09' as TIMESTAMP) end) desc,
            """
			
    if censusDate != '':
        censusDate = f" CAST('{censusDate}' as TIMESTAMP)"
		
    str_person = f"""
        select *,
            (case when gender = 'Male' then 'M'
		        when gender = 'Female' then 'F' 
			    when gender = 'Non-Binary' then '{genderForNonBinary}'
			    else '{genderForUnknown}'
			end) ipedsGender,
            (case when isUSCitizen = true or ((isInUSOnVisa = true or {censusDate} between visaStartDate and visaEndDate)
								and visaType in ('Employee Resident', 'Other Resident')) then 
				(case when isHispanic = true then '2' 
					when isMultipleRaces = true then '8' 
					when ethnicity != 'Unknown' and ethnicity is not null then
						(case when ethnicity = 'Hispanic or Latino' then '2'
							when ethnicity = 'American Indian or Alaskan Native' then '3'
							when ethnicity = 'Asian' then '4'
							when ethnicity = 'Black or African American' then '5'
							when ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
							when ethnicity = 'Caucasian' then '7'
							else '9' 
						end) 
					else '9' end) -- 'race and ethnicity unknown'
				when ((isInUSOnVisa = true or censusDate between visaStartDate and visaEndDate)
					and visaType in ('Student Non-resident', 'Employee Non-resident', 'Other Non-resident')) then '1' -- 'nonresident alien'
				else '9' -- 'race and ethnicity unknown'
			end) ipedsEthnicity		

        from (
			select address1 address1,
				address2 address2,
				address3 address3,
				birthDate birthDate,
				upper(city) city,
				deathDate deathDate,
				email email,
				ethnicity ethnicity,
				firstName firstName,
				gender gender,
				isConfidential isConfidential,
				isHispanic isHispanic,
				isInUSOnVisa isInUSOnVisa,
				isIPEDSReportable isIPEDSReportable,
				isMilitary isMilitary,
				isMultipleRaces isMultipleRaces,
				isUSCitizen isUSCitizen,
				isVeteran isVeteran,
				lastFourSsn lastFourSsn,
				lastName lastName,
				maritalStatus maritalStatus,
				middleName middleName,
				upper(nation) nation,
				personId personId,
				phone phone,
				recordActivityDate recordActivityDate,
				upper(state) state,
				suffix suffix,
				visaEndDate visaEndDate,
				visaStartDate visaStartDate,
				visaType visaType,
				zipcode zipcode,
				'{genderForNonBinary}' genderForNonBinary,
				'{genderForUnknown}' genderForUnknown,
				{censusDate} censusDate,
				coalesce(row_number() over (
					partition by
						personId
					order by
                        {snapshotDate}
                        recordActivityDate
				), 1) personRn
			from Person 
			where coalesce(isIpedsReportable = true, true)
                and ((coalesce(CAST(recordActivityDate as TIMESTAMP), CAST('9999-09-09' AS TIMESTAMP)) != CAST('9999-09-09' AS TIMESTAMP)  
				    and CAST(recordActivityDate as TIMESTAMP) <= {censusDate})
					    or coalesce(CAST(recordActivityDate as TIMESTAMP), CAST('9999-09-09' AS TIMESTAMP))  = CAST('9999-09-09' AS TIMESTAMP))
			)
		where personRn = 1
        """
    
    return str_person

def func_admission(snapshotDate = "9999-09-09", censusDate = "9999-09-09"):
    
    if snapshotDate != '':
        snapshotDate = f"cast('{snapshotDate}' as TIMESTAMP)"
        
    if censusDate != '':
        censusDate = f"""
                ((coalesce(cast(admissionDecisionActionDate as TIMESTAMP), cast('9999-09-09' as TIMESTAMP)) != cast('9999-09-09' as TIMESTAMP)
							and cast(admissionDecisionActionDate as TIMESTAMP) <= cast('{censusDate}' as TIMESTAMP))
						or (coalesce(cast(admissionDecisionActionDate as TIMESTAMP), cast('9999-09-09' as TIMESTAMP)) = cast('9999-09-09' AS TIMESTAMP)
							and ((coalesce(cast(recordActivityDate as TIMESTAMP), cast('9999-09-09' as TIMESTAMP)) != cast('9999-09-09' as TIMESTAMP)
									and cast(recordActivityDate as TIMESTAMP) <= cast('{censusDate}' as TIMESTAMP))
								or coalesce(cast(recordActivityDate as TIMESTAMP), cast('9999-09-09' as TIMESTAMP)) = cast('9999-09-09' as TIMESTAMP))))
					and cast(applicationDate as TIMESTAMP) <= cast('{censusDate}' as TIMESTAMP) 
			"""
			
    str_admission = f"""
		select *
		from (
			select *,
				(case when admissionDecision in ('Accepted', 'Admitted', 'Admitted, Waitlisted', 'Student Accepted', 'Student Accepted, Deferred') then 1 else 0 end) isAdmitted,
				coalesce(row_number() over (
						partition by
							termCodeApplied,
							personId
						order by 
							admissionDecisionActionDate desc,
							recordActivityDate desc,
							(case when admissionDecision in ('Accepted', 'Admitted', 'Student Accepted') then 1
								  when admissionDecision in ('Admitted, Waitlisted', 'Student Accepted, Deferred') then 2
								  else 3 end) asc
				), 1) admRn
			from ( 
				select admissionDecision,
					admissionDecisionActionDate,
					admissionType,
					applicationDate,
					applicationNumber,
					applicationStatus,
					applicationStatusActionDate,
					isIPEDSReportable,
					personId,
					recordActivityDate,
					secondarySchoolClassRankPct,
					secondarySchoolCompleteDate,
					secondarySchoolGPA,
					secondarySchoolType,
					studentLevel,
					studentType,
					upper(termCodeAdmitted) termCodeAdmitted,
					upper(termCodeApplied) termCodeApplied,
					coalesce(row_number() over (
						partition by
							termCodeApplied,
							termCodeAdmitted,
							personId,
							admissionDecision,
							admissionDecisionActionDate
						order by                    
					        (case when to_date(snapshotDate, 'YYYY/MM/DD HH:MI:SS') = {snapshotDate} then 1 else 2 end) asc,
					        (case when to_date(snapshotDate, 'YYYY/MM/DD HH:MI:SS') > {snapshotDate} then to_date(snapshotDate,'YYYY/MM/DD HH:MI:SS') else CAST('9999-09-09' as DATE) end) asc,
                            (case when to_date(snapshotDate, 'YYYY/MM/DD HH:MI:SS') < {snapshotDate} then to_date(snapshotDate,'YYYY/MM/DD HH:MI:SS') else CAST('1900-09-09' as DATE) end) desc,
							applicationNumber desc,
							applicationStatusActionDate desc,
							coalesce(recordActivityDate, CAST('9999-09-09' as DATE)) desc,
							(case when applicationStatus in ('Complete', 'Decision Made') then 1 else 2 end) asc
					), 1) appRn 
				from Admission 
				where admissionType = 'New Applicant'
					and (studentLevel = 'Undergraduate'
						or studentLevel is null)
					and (studentType = 'First Time'
						or studentType is null)
					and admissionDecision is not null
					and applicationStatus is not null
					and coalesce(isIpedsReportable, true) = true
                    and {censusDate}
				)
			where appRn = 1
			)
		where admRn = 1 

    """
    return str_admission
    
def func_testScore(snapshotDate = "9999-09-09", censusDate = "9999-09-09", admAdmissionTestScores = '', admUseForBothSubmitted = ''):
    
    if snapshotDate != '':
        snapshotDate = f"cast('{snapshotDate}' as TIMESTAMP)"
        
    if censusDate != '':
        censusDate = f"cast('{censusDate}' as TIMESTAMP)"
        
    str_testScore = f"""
			select isIPEDSReportable,
				personId,
				recordActivityDate,
				testDate,
				testScoreType,
				(case when '{config_admUseForMultiOfSame}' = 'A' then avg(testScore) else max(testScore) end) testScore
			from ( 
				select isIPEDSReportable,
				    personId,
				    recordActivityDate,
				    testDate,
				    testScoreType,
					(case when upper('{admAdmissionTestScores}') in ('R', 'C') then
						(case when upper('{admUseForBothSubmitted}') = 'B' and testScoreType in ('SAT Evidence-Based Reading and Writing', 'SAT Math', 'ACT Composite', 'ACT English', 'ACT Math') then testScore
							when upper('{admUseForBothSubmitted}') = 'A' and testScoreType in ('ACT Composite', 'ACT English', 'ACT Math') then testScore
							when upper('{admUseForBothSubmitted}') = 'S' and testScoreType in ('SAT Evidence-Based Reading and Writing', 'SAT Math') then testScore
						end)
					end) testScore,
					coalesce(row_number() over (
						partition by
							personID,
							testScoreType,
							testScore,
							testDate
						order by
							(case when to_date(snapshotDate, 'YYYY/MM/DD HH:MI:SS') = {snapshotDate} then 1 else 2 end) asc,
							(case when to_date(snapshotDate, 'YYYY/MM/DD HH:MI:SS') > {snapshotDate} then to_date(snapshotDate,'YYYY/MM/DD HH:MI:SS') else CAST('9999-09-09' as DATE) end) asc,
							(case when to_date(snapshotDate, 'YYYY/MM/DD HH:MI:SS') < {snapshotDate} then to_date(snapshotDate,'YYYY/MM/DD HH:MI:SS') else CAST('1900-09-09' as DATE) end) desc,
							coalesce(recordActivityDate, CAST('9999-09-09' as DATE)) desc
					), 1) tstRn
				from TestScore
				where cast(testDate as TIMESTAMP) <= {censusDate}
                    and coalesce(isIPEDSReportable, true) = true
                    and ((cast(recordActivityDate as TIMESTAMP) != CAST('9999-09-09' AS TIMESTAMP)
                        and cast(recordActivityDate as TIMESTAMP) <= {censusDate})
                            or cast(recordActivityDate as TIMESTAMP) = CAST('9999-09-09' AS TIMESTAMP))
					)
			where tstRn = 1
			group by isIPEDSReportable,
				personId,
				recordActivityDate,
				testDate,
				testScore,
				testScoreType

    """
    return str_testScore
    
def func_campus(snapshotDate = '', campusValue = '', dataType = ''):
        
    if snapshotDate != '':
        snapshotDateFilter = f"""
            and ((coalesce(CAST(recordActivityDate as TIMESTAMP), CAST('9999-09-09' AS TIMESTAMP)) != CAST('9999-09-09' AS TIMESTAMP)
                    and CAST(recordActivityDate as TIMESTAMP) <= CAST('{snapshotDate}' as TIMESTAMP))
                or coalesce(CAST(recordActivityDate as TIMESTAMP), CAST('9999-09-09' AS TIMESTAMP)) = CAST('9999-09-09' AS TIMESTAMP)) 
            """
    else:
        snapshotDateFilter = f"""
            """
        
    if campusValue != '':
        campusFilter = f"and upper(campus) = '{campusValue}'"
    else: 
        campusFilter = f" "
    
    if dataType != '':
        campusSelect = f"select {dataType}"
    else:
        campusSelect = f"select *"
            
    str_campus = f"""
		{campusSelect}
		from ( 
			select upper(campus) campus,
				campusDescription campusDescription,
				(case when coalesce(isInternational, false) = true then 1 else 0 end) isInternationalMod,
				coalesce(isInternational, false) isInternational,
				snapshotDate snapshotDate, 
				row_number() over (
					partition by
						campus
					order by
						recordActivityDate desc
				) campusRn
			from Campus 
			where coalesce(isIpedsReportable, true) = true
            {snapshotDateFilter}
		    {campusFilter}
			)
		where campusRn = 1
		order by 1
    """
    
    return str_campus
    
def func_cohort(repPeriod='', termOrder='', instructionalActivityType='', genderForNonBinary='', genderForUnknown='', acadOrProgReporter='', surveyType = ''):
  # 12ME, COM, GR, 200GR, ADM, OM, SFA, FE

    if surveyType in ['FE', 'OM', 'GR', '200GR']:
        SAFilter = f"or studyAbroadStatus = 'Study Abroad - Home Institution'"
    else: 
        SAFilter = f" "
            
    if surveyType in ['12ME', 'ADM', 'SFA', 'FE']:
        ESLFilter = f"when isESL = true then 0 --exclude students enrolled in ESL programs exclusively"
    else: 
        ESLFilter = f" "
        
    if surveyType in ['12ME', 'FE']:
        GradFilter = f"""
            when totalThesisCourses > 0 then 1 --Graduate students enrolled for thesis credits, even when zero credits are awarded, as these students are still enrolled and seeking their degree
            when totalResidencyCourses > 0 then 0 --exclude PHD residents or interns
        """
    else: 
        GradFilter = f" "
        
    str_cohort = f"""
            select *,
            (case when totalCECourses = totalCourses then 0 --exclude students enrolled only in continuing ed courses
            when totalIntlCourses = totalCourses then 0 --exclude students exclusively enrolled in any foreign branch campuses
            when totalAuditCourses = totalCourses then 0 --exclude students exclusively auditing classes
                    --exclude students enrolled in Experimental Pell Programs (incarcerated students)
                    when totalRemCourses = totalCourses and isNonDegreeSeeking = false then 1 --include students taking remedial courses if degree-seeking                
                    {ESLFilter}
                    {GradFilter}
            when totalSAHomeCourses > 0 then 1 --include study abroad student where home institution provides resources, even if credit hours = 0
            when totalCreditHrs > 0 then 1
            when totalClockHrs > 0 then 1 
            else 0
          end) ipedsEnrolled
            from (
            select acadTrack.*,
                upper(degprogENT.degreeProgram) DPdegreeProgram,
                upper(degprogENT.degree) degree,
                upper(degprogENT.major) major,
                coalesce(degProgENT.isESL, false) isESL,
                coalesce(row_number() over (
                    partition by
                        acadTrack.personId,
                        acadTrack.yearType,
                        acadTrack.degreeProgram,
                        degprogENT.degreeProgram
                    order by
                        (case when degprogENT.snapshotDate = acadTrack.snapshotDate then 1 else 2 end) asc,
                        (case when degprogENT.snapshotDate > acadTrack.snapshotDate then degprogENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                        (case when degprogENT.snapshotDate < acadTrack.snapshotDate then degprogENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                        termorder.termOrder desc,
                        degprogENT.startDate desc,
                        degprogENT.recordActivityDate desc
                ), 1) degProgRn
            from (
                select *
            from (
            select person.*,
                upper(acadtrackENT.degreeProgram) degreeProgram,
                coalesce(row_number() over (
                    partition by
                        person.personId,
                        acadtrackENT.personId,
                        person.yearType
                    order by
                      (case when acadtrackENT.snapshotDate = person.snapshotDate then 1 else 2 end) asc,
                        (case when acadtrackENT.snapshotDate > person.snapshotDate then acadtrackENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                        (case when acadtrackENT.snapshotDate < person.snapshotDate then acadtrackENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                        coalesce(acadtrackENT.fieldOfStudyPriority, 1) asc,
                        termorder.termOrder desc,
                        acadtrackENT.recordActivityDate desc,
                        acadtrackENT.fieldOfStudyActionDate desc,
                        (case when acadtrackENT.academicTrackStatus = 'In Progress' then 1 else 2 end) asc
                ), 1) acadTrackRn
            from (
            select pers.*,
                (case when pers.gender = 'Male' then 'M'
                    when pers.gender = 'Female' then 'F' 
                    when pers.gender = 'Non-Binary' then pers.genderForNonBinary
                    else pers.genderForUnknown
                end) ipedsGender,
                (case when pers.isUSCitizen = 1 or ((pers.isInUSOnVisa = 1 or pers.censusDate between pers.visaStartDate and pers.visaEndDate)
                                and pers.visaType in ('Employee Resident', 'Other Resident')) then 
                (case when pers.isHispanic = true then '2' 
                    when pers.isMultipleRaces = true then '8' 
                    when pers.ethnicity != 'Unknown' and pers.ethnicity is not null then
                        (case when pers.ethnicity = 'Hispanic or Latino' then '2'
                            when pers.ethnicity = 'American Indian or Alaskan Native' then '3'
                            when pers.ethnicity = 'Asian' then '4'
                            when pers.ethnicity = 'Black or African American' then '5'
                            when pers.ethnicity = 'Native Hawaiian or Other Pacific Islander' then '6'
                            when pers.ethnicity = 'Caucasian' then '7'
                            else '9' 
                        end) 
                    else '9' end) -- 'race and ethnicity unknown'
                when ((pers.isInUSOnVisa = 1 or pers.censusDate between pers.visaStartDate and pers.visaEndDate)
                    and pers.visaType in ('Student Non-resident', 'Employee Non-resident', 'Other Non-resident')) then '1' -- 'nonresident alien'
                else '9' -- 'race and ethnicity unknown'
            end) ipedsEthnicity
            from (
            select yearType,
                surveySection,
                stu.snapshotDate snapshotDate,
                termCode, 
                termOrder,
                termType,
                censusDate,
                financialAidYear,
                requiredFTCreditHoursGR,
                requiredFTCreditHoursUG,
                requiredFTClockHoursUG,
                genderForUnknown,
                genderForNonBinary,			   
                instructionalActivityType,
                stu.personId personId,
                (case when studyAbroadStatus != 'Study Abroad - Home Institution' then isNonDegreeSeeking
              when totalSAHomeCourses > 0 or totalCreditHrs > 0 or totalClockHrs > 0 then 0 
              else isNonDegreeSeeking 
            end) isNonDegreeSeeking,
                (case when studentLevelUGGR = 'UG' then 
                    (case when studentTypeTermType = 'Fall' and studentType = 'Continuing' and preFallStudType is not null then preFallStudType
                          else studentType 
                    end)
                    else studentType 
                end) studentType,
                studentLevel,
                studentLevelUGGR,
                campus,
                (case when studentLevelUGGR = 'UG' and totalCreditHrs is not null and totalClockHrs is not null then
                            (case when instructionalActivityType in ('CR', 'B') then 
                                    (case when totalCreditHrs >= requiredFTCreditHoursUG then 'Full Time' else 'Part Time' end)
                                when instructionalActivityType = 'CL' then 
                                    (case when totalClockHrs >= requiredFTClockHoursUG then 'Full Time' else 'Part Time' end) 
                              else null end)
                        when studentLevelUGGR = 'GR' and totalCreditHrs is not null then
                            (case when totalCreditHrs >= requiredFTCreditHoursGR then 'Full Time' else 'Part Time' end)
                    else null end) timeStatus,
                fullTimePartTimeStatus,
                (case when totalDECourses = totalCourses then 'Exclusive DE'
              when totalDECourses > 0 then 'Some DE'
                else 'None' 
                end) distanceEdInd,
                studyAbroadStatus,
                residency,
                totalCourses,
                totalCreditCourses,
                totalCreditHrs,
                totalClockHrs,
                totalCECourses,
                totalSAHomeCourses, 
                totalESLCourses,
                totalRemCourses,
                totalIntlCourses,
                totalAuditCourses,
                totalThesisCourses,
                totalResidencyCourses,
                totalDECourses,
                UGCreditHours,
                UGClockHours,
                GRCreditHours,
                DPPCreditHours,
                to_date(personENT.birthDate,'YYYY-MM-DD') birthDate,
                personENT.ethnicity ethnicity,
                coalesce(personENT.isHispanic, false) isHispanic,
                coalesce(personENT.isMultipleRaces, false) isMultipleRaces,
                coalesce(personENT.isInUSOnVisa, false) isInUSOnVisa,
                to_date(personENT.visaStartDate,'YYYY-MM-DD') visaStartDate,
                to_date(personENT.visaEndDate,'YYYY-MM-DD') visaEndDate,
                personENT.visaType visaType,
                coalesce(personENT.isUSCitizen, true) isUSCitizen,
                personENT.gender gender,
                upper(personENT.nation) nation,
                upper(personENT.state) state,
                coalesce(row_number() over (
                    partition by
                        stu.yearType,
                        stu.surveySection,
                        stu.termCode,
                        stu.personId,
                        personENT.personId
                    order by
                        (case when to_date(personENT.snapshotDate,'YYYY-MM-DD') = stu.snapshotDate then 1 else 2 end) asc,
                  (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') > stu.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                        (case when to_date(personENT.snapshotDate, 'YYYY-MM-DD') < stu.snapshotDate then to_date(personENT.snapshotDate,'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                        coalesce(personENT.recordActivityDate, CAST('9999-09-09' as DATE)) desc
                ), 1) personRn
            from (    
                select personId,
                    yearType,
                    min(isNonDegreeSeeking) isNonDegreeSeeking,
                    max(studentType) studentType,
                    max(firstFullTerm) termCode,
                    max(studentLevel) studentLevel,
                    (case when max(studentLevel) in ('Masters', 'Doctorate', 'Professional Practice Doctorate') then 'GR'
                        else 'UG' 
                    end) studentLevelUGGR, 
                    max(studentTypeTermType) studentTypeTermType,
                    max(preFallStudType) preFallStudType,
                    max(campus) campus,
                    max(studyAbroadStatus) studyAbroadStatus,
                    max(fullTimePartTimeStatus) fullTimePartTimeStatus,
                    max(residency) residency,
                    max(surveySection) surveySection,
                    max(snapshotDate) snapshotDate, 
                    max(termOrder) termOrder,
                    max(termType) termType,
                    max(censusDate) censusDate,
                    max(financialAidYear) financialAidYear,
                    max(requiredFTCreditHoursGR) requiredFTCreditHoursGR,
                    max(requiredFTCreditHoursUG) requiredFTCreditHoursUG,
                    max(requiredFTClockHoursUG) requiredFTClockHoursUG,
                    max(genderForUnknown) genderForUnknown,
                    max(genderForNonBinary) genderForNonBinary,			   
                    max(instructionalActivityType) instructionalActivityType,
                    sum(totalCourses) totalCourses,
                    sum(totalCreditCourses) totalCreditCourses,
                    sum(totalCreditHrs) totalCreditHrs,
                    sum(totalClockHrs) totalClockHrs,
                    sum(totalCECourses) totalCECourses,
                    sum(totalSAHomeCourses) totalSAHomeCourses, 
                    sum(totalESLCourses) totalESLCourses,
                    sum(totalRemCourses) totalRemCourses,
                    sum(totalIntlCourses) totalIntlCourses,
                    sum(totalAuditCourses) totalAuditCourses,
                    sum(totalThesisCourses) totalThesisCourses,
                    sum(totalResidencyCourses) totalResidencyCourses,
                    sum(totalDECourses) totalDECourses,
                    sum(UGCreditHours) UGCreditHours,
                    sum(UGClockHours) UGClockHours,
                    sum(GRCreditHours) GRCreditHours,
                    sum(DPPCreditHours) DPPCreditHours
    from (
        select personId,
                yearType,
                termCode,
                FFTRn,
                NDSRn,
                (case when isNonDegreeSeeking = true then 1 else 0 end) isNonDegreeSeeking, 
                (case when isNonDegreeSeeking = false then
                    (case when studentLevel != 'Undergraduate' then null
                        when NDSRn = 1 and FFTRn = 1 then studentType
                        when NDSRn = 1 then 'Continuing'
                    end)
                    else null
                end) studentType,
                (case when isNonDegreeSeeking = false then
                    (case when studentLevel != 'Undergraduate' then null
                        when NDSRn = 1 and FFTRn = 1 then termType
                        when NDSRn = 1 then termType
                    end)
                    else null
                end) studentTypeTermType,
                (case when termType = 'Pre-Fall Summer' then studentType else null end) preFallStudType,
                (case when FFTRn = 1 then studentLevel else null end) studentLevel,
                (case when FFTRn = 1 then termCode else null end) firstFullTerm,
                (case when FFTRn = 1 then campus else null end) campus,
                (case when FFTRn = 1 then fullTimePartTimeStatus else null end) fullTimePartTimeStatus,
                (case when FFTRn = 1 then studyAbroadStatus else null end) studyAbroadStatus,
                (case when FFTRn = 1 then residency else null end) residency,
                (case when FFTRn = 1 then surveySection else null end) surveySection,
                (case when FFTRn = 1 then snapshotDate else null end) snapshotDate,
                (case when FFTRn = 1 then termOrder else null end) termOrder,
                (case when FFTRn = 1 then termType else null end) termType,
                (case when FFTRn = 1 then censusDate else null end) censusDate,
                (case when FFTRn = 1 then financialAidYear else null end) financialAidYear,
                (case when FFTRn = 1 then requiredFTCreditHoursGR else null end) requiredFTCreditHoursGR,
                (case when FFTRn = 1 then requiredFTCreditHoursUG else null end) requiredFTCreditHoursUG,
                (case when FFTRn = 1 then requiredFTClockHoursUG else null end) requiredFTClockHoursUG,
                (case when FFTRn = 1 then genderForUnknown else null end) genderForUnknown,
                (case when FFTRn = 1 then genderForNonBinary else null end) genderForNonBinary,
                (case when FFTRn = 1 then instructionalActivityType else null end) instructionalActivityType,
                (case when FFTRn = 1 then totalCourses else null end) totalCourses,
                (case when FFTRn = 1 then totalCreditCourses else null end) totalCreditCourses,
                (case when FFTRn = 1 then totalCreditHrs else null end) totalCreditHrs,
                (case when FFTRn = 1 then totalClockHrs else null end) totalClockHrs,
                (case when FFTRn = 1 then totalCECourses else null end) totalCECourses,
                (case when FFTRn = 1 then totalSAHomeCourses else null end) totalSAHomeCourses, 
                (case when FFTRn = 1 then totalESLCourses else null end) totalESLCourses,
                (case when FFTRn = 1 then totalRemCourses else null end) totalRemCourses,
                (case when FFTRn = 1 then totalIntlCourses else null end) totalIntlCourses,
                (case when FFTRn = 1 then totalAuditCourses else null end) totalAuditCourses,
                (case when FFTRn = 1 then totalThesisCourses else null end) totalThesisCourses,
                (case when FFTRn = 1 then totalResidencyCourses else null end) totalResidencyCourses,
                (case when FFTRn = 1 then totalDECourses else null end) totalDECourses,
                (case when FFTRn = 1 then UGCreditHours else null end) UGCreditHours,
                (case when FFTRn = 1 then UGClockHours else null end) UGClockHours,
                (case when FFTRn = 1 then GRCreditHours else null end) GRCreditHours,
                (case when FFTRn = 1 then DPPCreditHours else null end) DPPCreditHours
        from (
            select yearType,
                surveySection,
                snapshotDate,
                termCode, 
                termOrder,
                termType,
                fullTermOrder,
                censusDate,
                financialAidYear,
                requiredFTCreditHoursGR,
                requiredFTCreditHoursUG,
                requiredFTClockHoursUG,
                genderForUnknown,
                genderForNonBinary,			   
                instructionalActivityType,
                personId,
                isNonDegreeSeeking,
                studentType,
                studentLevel,
                homeCampus campus,
                fullTimePartTimeStatus,
                studyAbroadStatus,
                residency,
                totalCourses,
                totalCreditCourses,
                totalCreditHrs,
                totalClockHrs,
                totalCECourses,
                totalSAHomeCourses, 
                totalESLCourses,
                totalRemCourses,
                totalIntlCourses,
                totalAuditCourses,
                totalThesisCourses,
                totalResidencyCourses,
                totalDECourses,
                UGCreditHours,
                UGClockHours,
                GRCreditHours,
                DPPCreditHours,
                row_number() over (
                            partition by
                                personId,
                                yearType
                        order by isNonDegreeSeeking asc,
                                fullTermOrder asc, --all standard length terms first
                                termOrder asc, --order by term to find first standard length term
                                startDate asc --get record for term with earliest start date (consideration for parts of term only)
                        ) NDSRn,
                row_number() over (
                            partition by
                                personId,
                                yearType
                        order by fullTermOrder asc, --all standard length terms first
                                termOrder asc, --order by term to find first standard length term
                                startDate asc --get record for term with earliest start date (consideration for parts of term only)
                        ) FFTRn
            from (
                select *
                from (
                    select studentENT.personId personId,
                            repperiod.termCode termCode,
                            repperiod.yearType yearType,
                            repperiod.surveySection surveySection,
                            repperiod.snapshotDate snapshotDate, 
                            repperiod.termOrder termOrder,
                            repperiod.termTypeNew termType,
                            repperiod.startDate startDate,
                            repperiod.censusDate censusDate,
                            repperiod.financialAidYear financialAidYear,
                            repperiod.fullTermOrder fullTermOrder,
                            repperiod.requiredFTCreditHoursGR,
                            repperiod.requiredFTCreditHoursUG,
                            repperiod.requiredFTClockHoursUG,
                            '{genderForUnknown}' genderForUnknown,
                            '{genderForNonBinary}' genderForNonBinary,			   
                            '{instructionalActivityType}' instructionalActivityType,
                            coalesce((case when studentENT.studentType = 'High School' then true
                                            when studentENT.studentType = 'Visiting' then true
                                            when studentENT.studentType = 'Unknown' then true
                                            when studentENT.studentLevel = 'Continuing Education' then true
                                            when studentENT.studentLevel = 'Other' then true
                                                when studentENT.studyAbroadStatus = 'Study Abroad - Host Institution' then true
                            else studentENT.isNonDegreeSeeking end), false) isNonDegreeSeeking,
                            studentENT.studentLevel studentLevel,
                            studentENT.studentType studentType,
                            studentENT.residency residency,
                            upper(studentENT.homeCampus) homeCampus,
                            studentENT.fullTimePartTimeStatus,
                            studentENT.studyAbroadStatus,
                            coalesce(row_number() over (
                                partition by
                                repperiod.yearType,
                                --repperiod.surveySection,
                                studentENT.personId,                    
                                studentENT.termCode
                                order by
                                (case when studentENT.snapshotDate = repperiod.snapshotDate then 1 else 2 end) asc,
                                (case when studentENT.snapshotDate > repperiod.snapshotDate then studentENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                                (case when studentENT.snapshotDate < repperiod.snapshotDate then studentENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                                studentENT.recordActivityDate desc
                            ), 1) studRn
                    from {repPeriod} repperiod   
                        inner join Student studentENT on upper(studentENT.termCode) = repperiod.termCode
                            and coalesce(studentENT.isIpedsReportable, true) = true
                            and ((coalesce(to_date(studentENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)  
                            and to_date(studentENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                                or coalesce(to_date(studentENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  = CAST('9999-09-09' AS DATE))
                ) studData
                    left join (
                        select personId regPersonId,
                                        termCode regTermCode,
                                        coalesce(count(courseSectionNumber), 0) totalCourses,
                                        coalesce(sum((case when enrollmentHours >= 0 then 1 else 0 end)), 0) totalCreditCourses,
                                        coalesce(sum((case when isClockHours = false then enrollmentHoursCalc else 0 end)), 0) totalCreditHrs,
                                        coalesce(sum((case when isClockHours = true and courseSectionLevel = 'Undergraduate' then enrollmentHoursCalc else 0 end)), 0) totalClockHrs,
                                        coalesce(sum((case when courseSectionLevel = 'Continuing Education' then 1 else 0 end)), 0) totalCECourses,
                                        coalesce(sum((case when locationType = 'Foreign Country' then 1 else 0 end)), 0) totalSAHomeCourses, 
                                        coalesce(sum((case when isESL = true then 1 else 0 end)), 0) totalESLCourses,
                                        coalesce(sum((case when isRemedial = true then 1 else 0 end)), 0) totalRemCourses,
                                        coalesce(sum((case when isInternational = true then 1 else 0 end)), 0) totalIntlCourses,
                                        coalesce(sum((case when isAudited = true then 1 else 0 end)), 0) totalAuditCourses,
                                        coalesce(sum((case when instructionType = 'Thesis/Capstone' then 1 else 0 end)), 0) totalThesisCourses,
                                        coalesce(sum((case when instructionType in ('Residency', 'Internship', 'Practicum') and courseSectionLevelUGGR = 'DPP' then 1 else 0 end)), 0) totalResidencyCourses,
                                        coalesce(sum((case when distanceEducationType != 'Not distance education' then 1 else 0 end)), 0) totalDECourses,
                                        coalesce(sum((case when instructionalActivityType != 'CL' and courseSectionLevelUGGR = 'UG' then enrollmentHours else 0 end)), 0) UGCreditHours,
                                        coalesce(sum((case when instructionalActivityType = 'CL' and courseSectionLevelUGGR = 'UG' then enrollmentHours else 0 end)), 0) UGClockHours,
                                        coalesce(sum((case when courseSectionLevelUGGR = 'GR' then enrollmentHours else 0 end)), 0) GRCreditHours,
                                        coalesce(sum((case when courseSectionLevelUGGR = 'DPP' then enrollmentHours else 0 end)), 0) DPPCreditHours
                        from (
                            select regData.*,
                                    coalesce(campus.isInternational, false) isInternational,
                                    coalesce(row_number() over (
                                            partition by
                                                regData.yearType,
                                                regData.termCode,
                                                regData.partOfTermCode,
                                                regData.personId,
                                                regData.courseSectionNumber,
                                                regData.courseSectionLevel
                                            order by 
                                                (case when campus.snapshotDate = regData.snapshotDate then 1 else 2 end) asc,
                                                (case when campus.snapshotDate > regData.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                                                (case when campus.snapshotDate < regData.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc
                                        ), 1) regCampRn
                            from (
                            select repperiod.yearType yearType,
                                repperiod.snapshotDate snapshotDate, 
                                repperiod.surveySection surveySection,
                                repperiod.termCode termCode,
                                repperiod.partOfTermCode partOfTermCode,
                                repperiod.censusDate censusDate,
                                repperiod.maxCensus maxCensus,
                                repperiod.termorder termorder,
                                repperiod.requiredFTCreditHoursGR requiredFTCreditHoursGR,
                                repperiod.requiredFTCreditHoursUG requiredFTCreditHoursUG,
                                repperiod.requiredFTClockHoursUG requiredFTClockHoursUG,
                                repperiod.equivCRHRFactor equivCRHRFactor,
                                '{instructionalActivityType}' instructionalActivityType,
                                regENT.personId personId,
                                upper(regENT.courseSectionNumber) courseSectionNumber,
                                coalesce(upper(regENT.courseSectionCampusOverride), course.courseSectionCampus) campus,
                                upper(regENT.courseSectionCampusOverride) courseSectionCampusOverride,
                                coalesce(regENT.isAudited, false) isAudited,
                                coalesce(regENT.isEnrolled, true) isEnrolled,
                                coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel) courseSectionLevel,
                                (case when coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel) in ('Undergraduate', 'Continuing Education', 'Other') then 'UG'
                                    when coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel) in ('Masters', 'Doctorate') then 'GR'
                                    when coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel) = 'Professional Practice Doctorate' then 'DPP' end) courseSectionLevelUGGR,
                                regENT.courseSectionLevelOverride courseSectionLevelOverride,
                                coalesce(regENT.enrollmentHoursOverride, course.courseSectionEnrollmentHours) enrollmentHours,
                                (case when '{instructionalActivityType}' = 'CR' then coalesce(regENT.enrollmentHoursOverride, course.courseSectionEnrollmentHours)
                                        when course.isClockHours = false then coalesce(regENT.enrollmentHoursOverride, course.courseSectionEnrollmentHours)
                                        when course.isClockHours = true and '{instructionalActivityType}' = 'B' then equivCRHRFactor * coalesce(regENT.enrollmentHoursOverride, course.courseSectionEnrollmentHours)
                                    else coalesce(regENT.enrollmentHoursOverride, course.courseSectionEnrollmentHours)
                                end) enrollmentHoursCalc,
                                regENT.enrollmentHoursOverride enrollmentHoursOverride,
                                course.isESL,
                                course.isRemedial,
                                course.isClockHours,
                                course.instructionType,
                                course.locationType,
                                course.distanceEducationType,
                                course.onlineInstructionType,
                                -- GR regData.termStartDateFall termStartDateFall,
                                -- OM repperiod.financialAidYearFall financialAidYear,
                                -- SFA repperiod.financialAidEndDate,
                                -- SFA repperiod.sfaReportPriorYear sfaReportPriorYear,
                                -- SFA repperiod.sfaReportSecondPriorYear sfaReportSecondPriorYear,
                                -- SFA epperiod.caresAct1 caresAct1,
                                -- SFA repperiod.caresAct2 caresAct2,
                                -- 12ME to_date(regENT.snapshotDate, 'YYYY-MM-DD') regENTSSD,
                                -- 12ME repperiod.snapshotDate repSSD,
                                coalesce(row_number() over (
                                    partition by
                                        repperiod.yearType,
                                        repperiod.surveySection,
                                        repperiod.termCode,
                                        repperiod.partOfTermCode,
                                        regENT.personId,
                                        regENT.courseSectionNumber,
                                        coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel)
                                    order by 
                                        (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
                                        (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                                        (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                                        regENT.recordActivityDate desc,
                                        regENT.registrationStatusActionDate desc
                                ), 1) regRn
                            from {repPeriod} repperiod
                                inner join Registration regENT on upper(regENT.termCode) = repperiod.termCode
                                    and coalesce(upper(regENT.partOfTermCode), 1) = repperiod.partOfTermCode
                                    and ((coalesce(to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' AS DATE)
                                                and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
                                            or (coalesce(to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' AS DATE)
                                                and ((coalesce(to_date(regENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                                                        and to_date(regENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                                                    or coalesce(to_date(regENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))))
                                    and coalesce(regENT.isIpedsReportable, true) = true
                                left join (
                                        select *
                                        from (
                                            select coursesectsched.termCode termCode,
                                                        coursesectsched.partOfTermCode partOfTermCode,
                                                        coursesectsched.courseSectionNumber,
                                                        coalesce(coursesectsched.courseSectionLevel, courseENT.courseLevel) courseSectionLevel,
                                                        coursesectsched.isESL,
                                                        coursesectsched.isRemedial,
                                                        coursesectsched.enrollmentHours courseSectionEnrollmentHours,
                                                        coursesectsched.isClockHours,
                                                        coursesectsched.campus courseSectionCampus,
                                                        coursesectsched.instructionType,
                                                        coursesectsched.locationType,
                                                        coursesectsched.distanceEducationType,
                                                        coursesectsched.onlineInstructionType,
                                                        coalesce(row_number() over (
                                                            partition by
                                                                --coursesectsched.yearType,
                                                                --coursesectsched.surveySection,
                                                                coursesectsched.termCode, 
                                                                coursesectsched.partOfTermCode,
                                                                coursesectsched.courseSectionNumber,
                                                                coalesce(coursesectsched.courseSectionLevel, courseENT.courseLevel)--,
                                                                --coursesectsched.subject,
                                                                --courseENT.subject,
                                                                --coursesectsched.courseNumber,
                                                                --courseENT.courseNumber
                                                            order by
                                                                (case when courseENT.snapshotDate = coursesectsched.snapshotDate then 1 else 2 end) asc,
                                                                (case when courseENT.snapshotDate > coursesectsched.snapshotDate then courseENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                                                                (case when courseENT.snapshotDate < coursesectsched.snapshotDate then courseENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                                                                termorder.termOrder desc,
                                                                courseENT.recordActivityDate desc
                                                            ), 1) courseRn
                                            from (
                                                select *
                                                from (
                                                    select coursesect.*,
                                                                upper(coursesectschedENT.campus) campus,
                                                                coursesectschedENT.instructionType,
                                                                coursesectschedENT.locationType,
                                                                coursesectschedENT.distanceEducationType,
                                                                coursesectschedENT.onlineInstructionType,
                                                                coalesce(row_number() over (
                                                                    partition by
                                                                        --coursesect.yearType,
                                                                        --coursesect.surveySection,
                                                                        coursesect.termCode, 
                                                                        coursesect.partOfTermCode,
                                                                        coursesect.courseSectionNumber,
                                                                        coursesect.courseSectionLevel
                                                                    order by
                                                                        (case when coursesectschedENT.snapshotDate = coursesect.snapshotDate then 1 else 2 end) asc,
                                                                        (case when coursesectschedENT.snapshotDate > coursesect.snapshotDate then coursesectschedENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                                                                        (case when coursesectschedENT.snapshotDate < coursesect.snapshotDate then coursesectschedENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                                                                        coursesectschedENT.recordActivityDate desc
                                                                ), 1) courseSectSchedRn
                                                    from (
                                                        select *
                                                        from (
                                                            select --repperiod.yearType yearType,
                                                                    repperiod.snapshotDate snapshotDate, 
                                                                    --repperiod.surveySection surveySection,
                                                                    repperiod.termCode termCode,
                                                                    repperiod.partOfTermCode partOfTermCode,
                                                                    repperiod.censusDate censusDate,
                                                                    repperiod.termorder termorder,
                                                                    upper(coursesectENT.courseSectionNumber) courseSectionNumber,
                                                                    coursesectENT.courseSectionLevel,
                                                                    upper(coursesectENT.subject) subject,
                                                                    upper(coursesectENT.courseNumber) courseNumber,
                                                                    upper(coursesectENT.section) section,
                                                                    upper(coursesectENT.customDataValue) customDataValue,
                                                                    coursesectENT.courseSectionStatus,
                                                                    coalesce(coursesectENT.isESL, false) isESL, 
                                                                    coalesce(coursesectENT.isRemedial, false) isRemedial,
                                                                    upper(coursesectENT.college) college,
                                                                    upper(coursesectENT.division) division,
                                                                    upper(coursesectENT.department) department,
                                                                    coursesectENT.enrollmentHours,
                                                                    coalesce(coursesectENT.isClockHours, false) isClockHours,
                                                                    coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
                                                                    coalesce(row_number() over (
                                                                            partition by
                                                                                --repperiod.yearType,
                                                                                --repperiod.surveySection,
                                                                                repperiod.termCode,
                                                                                repperiod.partOfTermCode,
                                                                                coursesectENT.courseSectionNumber,
                                                                                coursesectENT.courseSectionLevel
                                                                            order by
                                                                                (case when coursesectENT.snapshotDate = repperiod.snapshotDate then 1 else 2 end) asc,
                                                                                (case when coursesectENT.snapshotDate > repperiod.snapshotDate then coursesectENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                                                                                (case when coursesectENT.snapshotDate < repperiod.snapshotDate then coursesectENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                                                                                coursesectENT.recordActivityDate desc
                                                                        ), 1) courseSectionRn
                                                            from {repPeriod} repperiod
                                                                            inner join CourseSection coursesectENT on repperiod.termCode = upper(coursesectENT.termCode)
                                                                                and repperiod.partOfTermCode = coalesce(upper(coursesectENT.partOfTermCode), 1)
                                                                                and coalesce(coursesectENT.isIpedsReportable, true) = true
                                                                                and ((coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                                                                                        and to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') <= repperiod.censusDate)
                                                                                    or coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))  
                                                                        )
                                                            where courseSectionRn = 1
                                                                and ((recordActivityDate != CAST('9999-09-09' AS DATE)
                                                                    and courseSectionStatus = 'Active')
                                                                or recordActivityDate = CAST('9999-09-09' AS DATE))
                                                        ) coursesect
                                                    left join CourseSectionSchedule coursesectschedENT on coursesect.termCode = upper(coursesectschedENT.termCode) 
                                                        and coursesect.partOfTermCode = coalesce(upper(coursesectschedENT.partOfTermCode), 1)
                                                        and coursesect.courseSectionNumber = upper(coursesectschedENT.courseSectionNumber)
                                                        and coalesce(coursesectschedENT.isIpedsReportable, true) = true 
                                                        and ((coalesce(to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                                                                and to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') <= coursesect.censusDate)
                                                            or coalesce(to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
                                                    ) 
                                                    where courseSectSchedRn = 1
                                                ) coursesectsched
                                            left join Course courseENT on coursesectsched.subject = upper(courseENT.subject) 
                                                and coursesectsched.courseNumber = upper(courseENT.courseNumber) 
                                                and coalesce(courseENT.isIpedsReportable, true) = true
                                                and ((coalesce(to_date(courseENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                                                    and to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') <= coursesectsched.censusDate) 
                                                        or coalesce(to_date(courseENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
                                            left join {termOrder} termorder on termorder.termCode = upper(courseENT.termCodeEffective)
                                            and termorder.termOrder <= coursesectsched.termOrder
                                        )
                                        where courseRn = 1
                                    ) course on upper(regENT.courseSectionNumber) = course.courseSectionNumber
                                            and upper(regENT.termCode) = course.termCode
                                            and coalesce(upper(regENT.partOfTermCode), 1) = course.partOfTermCode
                            ) regData
                            left join Campus campus on regData.campus = campus.campus 
                        where regData.regRn = 1
                            and regData.isEnrolled = true                                       
                        )
                        where regCampRn = 1
                        group by personId, termCode
                    ) reg on studData.personId = reg.regPersonId
                        and studData.termCode = reg.regTermCode
                where studData.studRn = 1
            )
        where regPersonId is not null
        {SAFilter}
        )
    )
    group by personId, 
    yearType
    ) stu
        left join Person personENT on stu.personId = personENT.personId
                and coalesce(personENT.isIpedsReportable, true) = true
          and ((coalesce(to_date(personENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)  
            and to_date(personENT.recordActivityDate,'YYYY-MM-DD') <= stu.censusDate)
              or coalesce(to_date(personENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' AS DATE))  = CAST('9999-09-09' AS DATE)) 
    ) pers
    where pers.personRn = 1
    ) person
        left join AcademicTrack acadTrackENT on person.personId = acadTrackENT.personId
                and ((coalesce(to_date(acadTrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                            and to_date(acadTrackENT.fieldOfStudyActionDate,'YYYY-MM-DD') <= person.censusDate)
                        or (coalesce(to_date(acadTrackENT.fieldOfStudyActionDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)
                            and ((coalesce(to_date(acadTrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' as DATE)
                                    and to_date(acadTrackENT.recordActivityDate,'YYYY-MM-DD') <= person.censusDate)
                                or coalesce(to_date(acadTrackENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' as DATE)))) 
                        and acadTrackENT.fieldOfStudyType = 'Major' 
                        and coalesce(acadTrackENT.isIpedsReportable, true) = true
                        --and coalesce(acadTrackENT.isCurrentFieldOfStudy, true) = true
            left join {termOrder} termorder on termorder.termCode = upper(acadtrackENT.termCodeEffective)
                                        and termorder.termOrder <= person.termOrder
    )
    where acadTrackRn = 1
    ) acadTrack
    left join DegreeProgram degprogENT on acadTrack.degreeProgram = upper(degprogENT.degreeProgram)
                        and (degprogENT.startDate is null 
                                or to_date(degprogENT.startDate, 'YYYY-MM-DD') <= acadTrack.censusDate) 
                      and coalesce(degprogENT.isIpedsReportable, true) = true
                      and ((coalesce(to_date(degprogENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                                  and to_date(degprogENT.recordActivityDate,'YYYY-MM-DD') <= acadTrack.censusDate)
                            or coalesce(to_date(degprogENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
            left join {termOrder} termorder on termorder.termCode = upper(degprogENT.termCodeEffective)
            --                       and termorder.termOrder <= person2.termOrder
            )
    where degProgRn = 1
        and isESL = false
    """
    
    return str_cohort
    
def func_courseLevelCounts(repPeriod, termOrder, instructionalActivityType):

    str_courseCounts = f"""

    select personId personId,
            termCode termCode,
            coalesce(count(courseSectionNumber), 0) totalCourses,
            coalesce(sum((case when enrollmentHours >= 0 then 1 else 0 end)), 0) totalCreditCourses,
            coalesce(sum((case when isClockHours = false then enrollmentHoursCalc else 0 end)), 0) totalCreditHrs,
            coalesce(sum((case when isClockHours = true and courseSectionLevel = 'Undergraduate' then enrollmentHoursCalc else 0 end)), 0) totalClockHrs,
            coalesce(sum((case when courseSectionLevel = 'Continuing Education' then 1 else 0 end)), 0) totalCECourses,
            coalesce(sum((case when locationType = 'Foreign Country' then 1 else 0 end)), 0) totalSAHomeCourses, 
            coalesce(sum((case when isESL = true then 1 else 0 end)), 0) totalESLCourses,
            coalesce(sum((case when isRemedial = true then 1 else 0 end)), 0) totalRemCourses,
            coalesce(sum((case when isInternational = true then 1 else 0 end)), 0) totalIntlCourses,
            coalesce(sum((case when isAudited = true then 1 else 0 end)), 0) totalAuditCourses,
            coalesce(sum((case when instructionType = 'Thesis/Capstone' then 1 else 0 end)), 0) totalThesisCourses,
            coalesce(sum((case when instructionType in ('Residency', 'Internship', 'Practicum') and courseSectionLevelUGGR = 'DPP' then 1 else 0 end)), 0) totalResidencyCourses,
            coalesce(sum((case when distanceEducationType != 'Not distance education' then 1 else 0 end)), 0) totalDECourses,
            coalesce(sum((case when instructionalActivityType != 'CL' and courseSectionLevelUGGR = 'UG' then enrollmentHours else 0 end)), 0) UGCreditHours,
            coalesce(sum((case when instructionalActivityType = 'CL' and courseSectionLevelUGGR = 'UG' then enrollmentHours else 0 end)), 0) UGClockHours,
            coalesce(sum((case when courseSectionLevelUGGR = 'GR' then enrollmentHours else 0 end)), 0) GRCreditHours,
            coalesce(sum((case when courseSectionLevelUGGR = 'DPP' then enrollmentHours else 0 end)), 0) DPPCreditHours
    from (
        select regData.*,
                coalesce(campus.isInternational, false) isInternational,
                coalesce(row_number() over (
                        partition by
                            regData.yearType,
                            regData.termCode,
                            regData.partOfTermCode,
                            regData.personId,
                            regData.courseSectionNumber,
                            regData.courseSectionLevel
                        order by 
                            (case when campus.snapshotDate = regData.snapshotDate then 1 else 2 end) asc,
                            (case when campus.snapshotDate > regData.snapshotDate then campus.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                            (case when campus.snapshotDate < regData.snapshotDate then campus.snapshotDate else CAST('1900-09-09' as DATE) end) desc
                    ), 1) regCampRn
        from (
        select repperiod.yearType yearType,
            repperiod.snapshotDate snapshotDate, 
            repperiod.surveySection surveySection,
            repperiod.termCode termCode,
            repperiod.partOfTermCode partOfTermCode,
            repperiod.censusDate censusDate,
            repperiod.maxCensus maxCensus,
            repperiod.termorder termorder,
            repperiod.requiredFTCreditHoursGR requiredFTCreditHoursGR,
            repperiod.requiredFTCreditHoursUG requiredFTCreditHoursUG,
            repperiod.requiredFTClockHoursUG requiredFTClockHoursUG,
            repperiod.equivCRHRFactor equivCRHRFactor,
            '{instructionalActivityType}' instructionalActivityType,
            regENT.personId personId,
            upper(regENT.courseSectionNumber) courseSectionNumber,
            coalesce(upper(regENT.courseSectionCampusOverride), course.courseSectionCampus) campus,
            upper(regENT.courseSectionCampusOverride) courseSectionCampusOverride,
            coalesce(regENT.isAudited, false) isAudited,
            coalesce(regENT.isEnrolled, true) isEnrolled,
            coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel) courseSectionLevel,
            (case when coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel) in ('Undergraduate', 'Continuing Education', 'Other') then 'UG'
                when coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel) in ('Masters', 'Doctorate') then 'GR'
                when coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel) = 'Professional Practice Doctorate' then 'DPP' end) courseSectionLevelUGGR,
            regENT.courseSectionLevelOverride courseSectionLevelOverride,
            coalesce(regENT.enrollmentHoursOverride, course.courseSectionEnrollmentHours) enrollmentHours,
            (case when '{instructionalActivityType}' = 'CR' then coalesce(regENT.enrollmentHoursOverride, course.courseSectionEnrollmentHours)
                    when course.isClockHours = false then coalesce(regENT.enrollmentHoursOverride, course.courseSectionEnrollmentHours)
                    when course.isClockHours = true and '{instructionalActivityType}' = 'B' then equivCRHRFactor * coalesce(regENT.enrollmentHoursOverride, course.courseSectionEnrollmentHours)
                else coalesce(regENT.enrollmentHoursOverride, course.courseSectionEnrollmentHours)
            end) enrollmentHoursCalc,
            regENT.enrollmentHoursOverride enrollmentHoursOverride,
            course.isESL,
            course.isRemedial,
            course.isClockHours,
            course.instructionType,
            course.locationType,
            course.distanceEducationType,
            course.onlineInstructionType,
            -- GR regData.termStartDateFall termStartDateFall,
            -- OM repperiod.financialAidYearFall financialAidYear,
            -- SFA repperiod.financialAidEndDate,
            -- SFA repperiod.sfaReportPriorYear sfaReportPriorYear,
            -- SFA repperiod.sfaReportSecondPriorYear sfaReportSecondPriorYear,
            -- SFA epperiod.caresAct1 caresAct1,
            -- SFA repperiod.caresAct2 caresAct2,
            -- 12ME to_date(regENT.snapshotDate, 'YYYY-MM-DD') regENTSSD,
            -- 12ME repperiod.snapshotDate repSSD,
            coalesce(row_number() over (
                partition by
                    repperiod.yearType,
                    repperiod.surveySection,
                    repperiod.termCode,
                    repperiod.partOfTermCode,
                    regENT.personId,
                    regENT.courseSectionNumber,
                    coalesce(regENT.courseSectionLevelOverride, course.courseSectionLevel)
                order by 
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') = repperiod.snapshotDate then 1 else 2 end) asc,
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') > repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('9999-09-09' as DATE) end) asc,
                    (case when to_date(regENT.snapshotDate, 'YYYY-MM-DD') < repperiod.snapshotDate then to_date(regENT.snapshotDate, 'YYYY-MM-DD') else CAST('1900-09-09' as DATE) end) desc,
                    regENT.recordActivityDate desc,
                    regENT.registrationStatusActionDate desc
            ), 1) regRn
        from {repPeriod} repperiod
            inner join Registration regENT on upper(regENT.termCode) = repperiod.termCode
                and coalesce(upper(regENT.partOfTermCode), 1) = repperiod.partOfTermCode
                and ((coalesce(to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' AS DATE)
                            and to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD') <= repperiod.censusDate)
                        or (coalesce(to_date(regENT.registrationStatusActionDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' AS DATE)
                            and ((coalesce(to_date(regENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                                    and to_date(regENT.recordActivityDate,'YYYY-MM-DD') <= repperiod.censusDate)
                                or coalesce(to_date(regENT.recordActivityDate,'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))))
                and coalesce(regENT.isIpedsReportable, true) = true
            left join (
                    select *
                    from (
                        select coursesectsched.termCode termCode,
                                    coursesectsched.partOfTermCode partOfTermCode,
                                    coursesectsched.courseSectionNumber,
                                    coalesce(coursesectsched.courseSectionLevel, courseENT.courseLevel) courseSectionLevel,
                                    coursesectsched.isESL,
                                    coursesectsched.isRemedial,
                                    coursesectsched.enrollmentHours courseSectionEnrollmentHours,
                                    coursesectsched.isClockHours,
                                    coursesectsched.campus courseSectionCampus,
                                    coursesectsched.instructionType,
                                    coursesectsched.locationType,
                                    coursesectsched.distanceEducationType,
                                    coursesectsched.onlineInstructionType,
                                    coalesce(row_number() over (
                                        partition by
                                            --coursesectsched.yearType,
                                            --coursesectsched.surveySection,
                                            coursesectsched.termCode, 
                                            coursesectsched.partOfTermCode,
                                            coursesectsched.courseSectionNumber,
                                            coalesce(coursesectsched.courseSectionLevel, courseENT.courseLevel)--,
                                            --coursesectsched.subject,
                                            --courseENT.subject,
                                            --coursesectsched.courseNumber,
                                            --courseENT.courseNumber
                                        order by
                                            (case when courseENT.snapshotDate = coursesectsched.snapshotDate then 1 else 2 end) asc,
                                            (case when courseENT.snapshotDate > coursesectsched.snapshotDate then courseENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                                            (case when courseENT.snapshotDate < coursesectsched.snapshotDate then courseENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                                            termorder.termOrder desc,
                                            courseENT.recordActivityDate desc
                                        ), 1) courseRn
                        from (
                            select *
                            from (
                                select coursesect.*,
                                            upper(coursesectschedENT.campus) campus,
                                            coursesectschedENT.instructionType,
                                            coursesectschedENT.locationType,
                                            coursesectschedENT.distanceEducationType,
                                            coursesectschedENT.onlineInstructionType,
                                            coalesce(row_number() over (
                                                partition by
                                                    --coursesect.yearType,
                                                    --coursesect.surveySection,
                                                    coursesect.termCode, 
                                                    coursesect.partOfTermCode,
                                                    coursesect.courseSectionNumber,
                                                    coursesect.courseSectionLevel
                                                order by
                                                    (case when coursesectschedENT.snapshotDate = coursesect.snapshotDate then 1 else 2 end) asc,
                                                    (case when coursesectschedENT.snapshotDate > coursesect.snapshotDate then coursesectschedENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                                                    (case when coursesectschedENT.snapshotDate < coursesect.snapshotDate then coursesectschedENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                                                    coursesectschedENT.recordActivityDate desc
                                            ), 1) courseSectSchedRn
                                from (
                                    select *
                                    from (
                                        select --repperiod.yearType yearType,
                                                repperiod.snapshotDate snapshotDate, 
                                                --repperiod.surveySection surveySection,
                                                repperiod.termCode termCode,
                                                repperiod.partOfTermCode partOfTermCode,
                                                repperiod.censusDate censusDate,
                                                repperiod.termorder termorder,
                                                upper(coursesectENT.courseSectionNumber) courseSectionNumber,
                                                coursesectENT.courseSectionLevel,
                                                upper(coursesectENT.subject) subject,
                                                upper(coursesectENT.courseNumber) courseNumber,
                                                upper(coursesectENT.section) section,
                                                upper(coursesectENT.customDataValue) customDataValue,
                                                coursesectENT.courseSectionStatus,
                                                coalesce(coursesectENT.isESL, false) isESL, 
                                                coalesce(coursesectENT.isRemedial, false) isRemedial,
                                                upper(coursesectENT.college) college,
                                                upper(coursesectENT.division) division,
                                                upper(coursesectENT.department) department,
                                                coursesectENT.enrollmentHours,
                                                coalesce(coursesectENT.isClockHours, false) isClockHours,
                                                coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) recordActivityDate,
                                                coalesce(row_number() over (
                                                        partition by
                                                            --repperiod.yearType,
                                                            --repperiod.surveySection,
                                                            repperiod.termCode,
                                                            repperiod.partOfTermCode,
                                                            coursesectENT.courseSectionNumber,
                                                            coursesectENT.courseSectionLevel
                                                        order by
                                                            (case when coursesectENT.snapshotDate = repperiod.snapshotDate then 1 else 2 end) asc,
                                                            (case when coursesectENT.snapshotDate > repperiod.snapshotDate then coursesectENT.snapshotDate else CAST('9999-09-09' as DATE) end) asc,
                                                            (case when coursesectENT.snapshotDate < repperiod.snapshotDate then coursesectENT.snapshotDate else CAST('1900-09-09' as DATE) end) desc,
                                                            coursesectENT.recordActivityDate desc
                                                    ), 1) courseSectionRn
                                        from {repPeriod} repperiod
                                                        inner join CourseSection coursesectENT on repperiod.termCode = upper(coursesectENT.termCode)
                                                            and repperiod.partOfTermCode = coalesce(upper(coursesectENT.partOfTermCode), 1)
                                                            and coalesce(coursesectENT.isIpedsReportable, true) = true
                                                            and ((coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                                                                    and to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD') <= repperiod.censusDate)
                                                                or coalesce(to_date(coursesectENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE))  
                                                    )
                                        where courseSectionRn = 1
                                            and ((recordActivityDate != CAST('9999-09-09' AS DATE)
                                                and courseSectionStatus = 'Active')
                                            or recordActivityDate = CAST('9999-09-09' AS DATE))
                                    ) coursesect
                                left join CourseSectionSchedule coursesectschedENT on coursesect.termCode = upper(coursesectschedENT.termCode) 
                                    and coursesect.partOfTermCode = coalesce(upper(coursesectschedENT.partOfTermCode), 1)
                                    and coursesect.courseSectionNumber = upper(coursesectschedENT.courseSectionNumber)
                                    and coalesce(coursesectschedENT.isIpedsReportable, true) = true 
                                    and ((coalesce(to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) != CAST('9999-09-09' AS DATE)
                                            and to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD') <= coursesect.censusDate)
                                        or coalesce(to_date(coursesectschedENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' AS DATE)) = CAST('9999-09-09' AS DATE)) 
                                ) 
                                where courseSectSchedRn = 1
                            ) coursesectsched
                        left join Course courseENT on coursesectsched.subject = upper(courseENT.subject) 
                            and coursesectsched.courseNumber = upper(courseENT.courseNumber) 
                            and coalesce(courseENT.isIpedsReportable, true) = true
                            and ((coalesce(to_date(courseENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) != CAST('9999-09-09' as DATE)
                                and to_date(courseENT.recordActivityDate, 'YYYY-MM-DD') <= coursesectsched.censusDate) 
                                    or coalesce(to_date(courseENT.recordActivityDate, 'YYYY-MM-DD'), CAST('9999-09-09' as DATE)) = CAST('9999-09-09' as DATE))
                        left join {termOrder} termorder on termorder.termCode = upper(courseENT.termCodeEffective)
                              and termorder.termOrder <= coursesectsched.termOrder
                    )
                    where courseRn = 1
                ) course on upper(regENT.courseSectionNumber) = course.courseSectionNumber
                        and upper(regENT.termCode) = course.termCode
                        and coalesce(upper(regENT.partOfTermCode), 1) = course.partOfTermCode
        ) regData
        left join Campus campus on regData.campus = campus.campus 
        where regData.regRn = 1
            and regData.isEnrolled = true                                       
    )
    where regCampRn = 1
    group by personId, termCode

    """
    
    return str_courseCounts    
