capture program drop assignDoctors_oldMu
program assignDoctors_oldMu
    cap log close
    log using "${out}/logs/assignDoctors_oldMu.log", text replace
    syntax, pct(str) days_dead(int) doc_type(str)

    set seed 1082769842

    set sortseed 1884140967
     
     
    import delimited "${data}/npidata_pfile_20050523-20240908.csv", stringcols(1) clear 

    keep if entitytypecode == 1

    egen cleanTitle = sieve(providercredentialtext), keep(a)
    replace cleanTitle = strupper(cleanTitle)

    keep if cleanTitle == "MD" | cleanTitle == "DO" | cleanTitle == "PHDMD" | cleanTitle == "MDPHD" ///
    | cleanTitle == "MDMS" | cleanTitle == "MDMBA" | cleanTitle == "MDMPH" | cleanTitle == "MDFACS" ///
    | cleanTitle == "MDMSC" | cleanTitle == "MDFACC" | cleanTitle == "MDFAAP" | cleanTitle == "MDFACP" ///
    | cleanTitle == "MDFACOG" | cleanTitle == "MDJD" | cleanTitle == "MDMHS" | cleanTitle == "MDMSPH" ///
    | cleanTitle == "MDFRCSC" | cleanTitle == "MDMHA" | cleanTitle == "MDPHDMPH" ///
    | cleanTitle == "MSMD" | cleanTitle == "MDDO" | cleanTitle == "MDFRCPC" | cleanTitle == "MDCM" ///
    | cleanTitle == "MDMPHMS" | cleanTitle == "MDFCCP" | cleanTitle == "MDFACEP" ///
    | cleanTitle == "MDINC" | cleanTitle == "MDMPHMBA" | cleanTitle == "MDMED" ///
    | cleanTitle == "MDFAAFP" | cleanTitle == "MDMSCR" | cleanTitle == "MDMBAMPH" ///
    | cleanTitle == "MDMPHTM" | cleanTitle == "MDMPHIL" | cleanTitle == "MDLLC" ///
    | cleanTitle == "MDMSCFRCSC" | cleanTitle == "MDMSCI" | cleanTitle == "DOMD" ///
    | cleanTitle == "MDMMSC" | cleanTitle == "BSMD" | cleanTitle == "MDFRCS" ///
    | cleanTitle == "MDFAAD" | cleanTitle == "MDPHDFRCSC" | cleanTitle == "MDCMD"

    keep npi cleanTitle

    gduplicates drop

    gisid npi

    save "${data}/npiTitles_tempdata", replace



    use `"${data}/npiMortality`pct'_OP_fulldata.dta"', clear
    append using `"${data}/npiMortality`pct'_IP_fulldata.dta"', keep(npi from_dt)
        if ( "`pct'" == "100" ) {
            disp "There's no 100% carrier file; doing 20%"
            append using `"${data}/npiMortality20_CAR_fulldata.dta"'
        }
        else append using `"${data}/npiMortality`pct'_CAR_fulldata.dta"'
        
        count if mi(fe_sex, fe_race, fe_age, mDual, claim_year)
        sum if mi(fe_sex, fe_race, fe_age, mDual, claim_year)
        drop if mi(fe_sex, fe_race, fe_age, mDual, claim_year)
    
    rename claim_year year

    describe

    duplicates drop

    di("tab year 1")
    tab year
    
    * Merge NPI with their specialty
    if ( "`pct'" == "100" ) {
            disp "There's no 100% carrier file; doing 20%"
            merge m:1 npi year using `"${data}/carrier_specialty_20pct.dta"', nogen keep(match)
        }
    else merge m:1 npi year using `"${data}/carrier_specialty_`pct'pct.dta"', nogen keep(match)
    
    encode specialty, gen(fe_specialty)

    di("tab year 2")
    tab year

    * Only keep PCP-bene combinations
    if "`doc_type'" == "PCP"{
        keep if specialty == "IM"
    }
    else if "`doc_type'" == "Specialist"{
        keep if specialty != "IM"
    }
    else{
        display("Not valid doc type, keeping all NPIs.")
    }

    di("Merging in PCPs with no subspecialties")
    merge m:1 npi using "${data}/npi_pcp_only_list.dta", generate(PCP_only_merge)
    keep if PCP_only_merge == 3
    drop PCP_only_merge

    di("Keeping only PCPs with MD or DO")
    merge m:1 npi using "${data}/npiTitles_tempdata", generate(titles_merge)
    keep if titles_merge == 3
    drop titles_merge

    di("tab year 3")
    tab year

    * Merge beneficiary with the chronic condition indicators
    di("Merging benes with comorbidities")
    count
    merge m:1 bene_id year using `"${data}/comorbiditiesDraft_`pct'pct.dta"', nogen keep(match)
    di("Post merge")
    count

    di("tab year 4")
    
    
    
    * Generate year and month variables
    * gen year = year(from_dt)
    gen month = month(from_dt)
    gen MDate = ym(year, month)
    
    * Generate number of times a bene visited an npi
    bysort bene_code year npi : gen total_visits_per_npi = _N

    gen long daysToDeath = sdod - from_dt
    drop if daysToDeath < -1
    gen byte dead`days_dead' = daysToDeath <= `days_dead'

    * Merge zip code to hsa
    rename zip_cd zip
    merge m:1 zip year using `"${data}/pcp/hsazipstate.dta"', keep(3) keepusing(zip year hsanum) nogen
    rename zip zip_cd
    rename hsanum hsa

    di("tab year 5")
    * Merge in monthly departure data
    merge m:1 npi MDate hsa using `"${data}/pcp_departures_wc_`pct'pct_sortOfDepartures.dta"', ///
        generate(departMerge) keepusing(potentialDeparture ///
        departDate fewBenes)
    tab departMerge
    listsome npi hsa MDate if departMerge == 1, random
    keep if departMerge == 3

    di("tab year 6")
    tab year

    describe

    ***********************************
    * Generate periods of observation *
    ***********************************
    egen bene_npi_month = tag(bene_code npi hsa MDate)
    bysort bene_code : egen numDepartures_ph = total(potentialDeparture) ///
        if bene_npi_month == 1
    bysort bene_code : egen numDepartures = max(numDepartures_ph)
    drop numDepartures_ph

    egen distinct_depart_dates = tag(bene_code departDate)
    bysort bene_code : egen distinct_depart_num = total(distinct_depart_dates)

    gen time_period = .
    gen departureMDate = .
    gen end_period = .
    gen begin_period = .
    gen appointmentCount = 1

    di("tab year 7")
    tab year

    * Algorithm for zero departures
    di("No Departures")
    replace time_period = year if distinct_depart_num == 0

    * Algorithm for one departure
    di("One departure")
    replace departureMDate = departDate if distinct_depart_num == 1
    bysort bene_code : egen max_departureMDate = max(departureMDate) if distinct_depart_num == 1
    replace departureMDate = max_departureMDate if distinct_depart_num == 1
    drop max_departureMDate



    * Algorithm for multiple departures
    di("Two departures")
    replace departureMDate = departDate if distinct_depart_num > 1
    bysort bene_code npi hsa : egen potmax_departureMDate = max(departureMDate) if distinct_depart_num > 1
    gen max_beginMDate = potmax_departureMDate - 11

    bysort bene_code npi hsa : egen departVisits = total(appointmentCount) if inrange(MDate, max_beginMDate, potmax_departureMDate) & distinct_depart_num > 1
    bysort bene_code : egen mostVisits = max(departVisits)
    gen isMostVisited = 1 if mostVisits == departVisits & !mi(mostVisits, departVisits)
    * Tie breaker if there are more than one docs with most visits
    gen randTie = rnormal() if isMostVisited == 1
    bysort bene_code : egen tieBreaker = max(randTie)
    replace potmax_departureMDate = . if departVisits != mostVisits & distinct_depart_num > 1
    di("Tiebreaker works?")
    replace potmax_departureMDate = . if randTie != tieBreaker & distinct_depart_num > 1 & isMostVisited == 1
    bysort bene_code : egen max_departureMDate = max(potmax_departureMDate) if distinct_depart_num > 1
    replace departureMDate = max_departureMDate if distinct_depart_num > 1

    gen death_month = month(sdod)
    gen death_year = year(sdod)
    gen death_MDate = ym(death_year, death_month)

    gen dead_period = .
    gen time_period_start = .
    gen time_period_end = .

    * Defining periods for departures
    forval i = -16/-1{
        di(`i')
        replace end_period = departureMDate - 1 + 12*(`i' + 1)
        replace begin_period = end_period - 11
        replace time_period = `i' if inrange(MDate, begin_period, end_period) & !mi(begin_period, end_period)
        replace dead_period = `i' if inrange(death_MDate, begin_period, end_period) & !mi(begin_period, end_period, death_MDate)
        replace time_period_start = begin_period if inrange(MDate, begin_period, end_period) & !mi(begin_period, end_period)
        replace time_period_end = end_period if inrange(MDate, begin_period, end_period) & !mi(begin_period, end_period)
    }
    forval i = 0/16{
        di(`i')
        replace begin_period = departureMDate + 12*(`i')
        replace end_period = begin_period + 11
        replace time_period = `i' if inrange(MDate, begin_period, end_period) & !mi(begin_period, end_period)
        replace dead_period = `i' if inrange(death_MDate, begin_period, end_period) & !mi(begin_period, end_period, death_MDate)
        replace time_period_start = begin_period if inrange(MDate, begin_period, end_period) & !mi(begin_period, end_period)
        replace time_period_end = end_period if inrange(MDate, begin_period, end_period) & !mi(begin_period, end_period)
    }

    replace dead_period = year if year == death_year & numDepartures == 0

    count if time_period > dead_period
    drop if time_period > dead_period

    drop dead`days_dead'
    gen dead365 = dead_period == time_period

    listsome dead_period time_period, random
    listsome dead_period time_period if dead365 == 1, random

    levelsof time_period
    levelsof dead_period

    tab time_period
    tab dead_period

    di("listsome death_MDate")
    listsome death_MDate, random

    drop if mi(time_period)

    bysort npi hsa time_period : egen fewBenesFlag = max(fewBenes)

    /*
    gen depart = 0
    replace depart = 1 if inrange(departureMDate, time_period_start, time_period_end) & !mi(time_period_start, time_period_end, departureMDate)
    */
    

    save `"${data}/patientAssignment_`pct'pct_oldMu.dta"', replace

    drop total_visits_per_npi
    bysort bene_code time_period npi hsa : gen total_visits_per_npi = _N

    
    
    * Find max Charlson score in time_period by bene-npi combination (check in on this)
    bysort npi bene_code time_period: egen max_charlson = max(CharlsonScore)
    replace CharlsonScore = max_charlson

    * Keep only modal NPI
    bysort bene_code time_period : egen pcp_rank = rank(-total_visits_per_npi), unique

    * Debugging code to determine how many ties there are in pcp rank
    egen bene_npi_tag = tag(bene_code npi time_period)
    bysort bene_code time_period : egen trackRank = ///
        rank(-total_visits_per_npi) if bene_npi_tag == 1, track
    replace trackRank = . if trackRank != 1
    bysort bene_code time_period : egen numModalDocs = total(trackRank)

    di("Stats on modal doctor ties")
    sum numModalDocs
    count if numModalDocs > 1

    bysort bene_code time_period npi hsa (pcp_rank) : gen pcp_rank_tie = pcp_rank[1]
    replace pcp_rank = pcp_rank_tie
    keep if pcp_rank == 1
    
    * Keep earliest appointment in time_period for modal npi
    bysort npi hsa bene_code time_period: egen min_date = min(from_dt)
    keep if from_dt == min_date

    
    * Some instances in which bene-npi combination had more than one appointment
    * in a day. We only keep one of these
    bysort npi hsa bene_code from_dt : gen claim_dup = cond(_N == 1, 0, _n)
    drop if claim_dup > 1
    
    gisid npi hsa time_period bene_code
    
    save `"${data}/patientAssignment_`pct'pct_oldMu_assignments.dta"', replace

    format MDate departDate time_period_start time_period_end %tm
    replace time_period = departureMDate + (12 * time_period) if !mi(departureMDate)
    gen tempMonth = 1 if mi(departureMDate)
    replace time_period = ym(time_period, tempMonth) if mi(departureMDate)
    format time_period %tm

    save `"${data}/patientAssignment_`pct'pct_oldMu_assignments_fixed_periods.dta"', replace
    
        

    use `"${data}/patientAssignment_`pct'pct_oldMu_assignments.dta"', clear

    merge m:1 npi using "${data}/full_doc_subspec.dta", generate(subspec_merge) keep(1 3)

    drop if spec_ind5 == 1 | spec_ind6 == 1

    drop subspec_merge

    assert !mi(time_period)

    * Create min year variable
    bysort bene_code time_period: egen min_year = min(year)

    * generate mortality variable for period
    * gisid bene_code time_period dead`days_dead'


    di("Missing zip codes")
    count if mi(zip_cd)
    assert !mi(zip_cd)

    di("mdesc 5")
    mdesc



    gisid bene_code time_period


    
    
    
    * Merge with median household by zipcode data
    merge m:1 zip_cd using `"${data}/IPUMS_median_hh_income_clean.dta"', nogen keep(3)
    replace median_hh_income = median_hh_income / 10000

    
    drop if mi(median_hh_income)

    gen bene_wgt = 1

    destring fe_race, replace
    destring fe_sex, replace

    egen npi_group = group(npi hsa)

    xtset npi_group

    cap which reghdfe
    if _rc {
        ssc install reghdfe
    }

    gen round_age = floor(demo_age)
    sum CharlsonScore, detail
    levelsof CharlsonScore

    memory

    describe

    compress

    sum round_age, detail
    local pct99age = r(p99)

    drop if round_age > 122

    di("Count of observations before regression")
    count

    mdesc


    * Find modal county for every doctor
    merge m:1 year bene_code using `"${data}/denom`pct'_cohorts.dta"', keepusing(state county) nogen keep(3)

    gisid bene_code time_period

    disp("Beginning of modal county")
    egen state_county = group(state county)
    disp("after group")
    bysort npi_group state_county : gen numObs = _N
    bysort npi_group  : egen maxNumObs = max(numObs)
    disp("Before tag")
    egen modeTag = tag(npi_group state_county) if numObs == maxNumObs
    bysort npi_group : egen totalModes = total(modeTag)
    disp("After total modes")
    gen rand_norm = rnormal() if modeTag == 1
    bysort npi_group : egen highest_rand_norm = max(rand_norm)
    gen modal_county = state_county if rand_norm == highest_rand_norm
    bysort npi_group : egen replace_mode = max(modal_county)
    replace modal_county = replace_mode

    disp("Beginning of modal hsa")
    bysort npi_group hsa : gen numObs_hsa = _N
    bysort npi_group  : egen maxNumObs_hsa = max(numObs_hsa)
    disp("Before tag")
    egen modeTag_hsa = tag(npi_group hsa) if numObs_hsa == maxNumObs_hsa
    bysort npi_group : egen totalModes_hsa = total(modeTag_hsa)
    disp("After total modes")
    gen rand_norm_hsa = rnormal() if modeTag_hsa == 1
    bysort npi_group : egen highest_rand_norm_hsa = max(rand_norm_hsa)
    gen modal_hsa = hsa if rand_norm_hsa == highest_rand_norm_hsa
    bysort npi_group : egen replace_mode_hsa = max(modal_hsa)
    replace modal_hsa = replace_mode_hsa

    preserve
        collapse (first) `varlist' dead`days_dead', by(bene_code time_period npi hsa)
        gisid bene_code time_period
        save "${data}/pcp_assignment_oldMu_`pct'pct.dta", replace
    restore
    
    count

    bysort npi_group time_period: gen nbene = _N
    bysort npi_group : gen total_benes = _N

    gen age_month = month(dofm(time_period))
    gen age_year = year(dofm(time_period))
    gen dec_age = ((mdy(age_month, 1, age_year) - sdob) / 365.25)
    drop age_month

    gen mid_tp = time_period + 6
    gen charlson_era = 3
    replace charlson_era = 1 if mid_tp <= ym(2008, 12)
    replace charlson_era = 2 if mid_tp >= ym(2015, 10)
    drop mid_tp

    **********************************************************************
    * VAM: quantile-bin loop (from doc 2)
    **********************************************************************

    local numQuantiles 3

    xtile dec_num_benes = total_benes, nq(`numQuantiles')
    forval d = 1/`numQuantiles' {
        gen d_`d' = dec_num_benes == `d'
    }

    forval i = 1/`numQuantiles' {

        di("Bin d_`i'")
        sum total_benes if d_`i' == 1, detail

        reghdfe dead365 i.fe_sex i.fe_race dec_age i.mDual median_hh_income i.charlson_era#i.CharlsonScore ///
            i.cc_* if d_`i' == 1, absorb(rawu_d`i' = npi_group year hsa) resid keepsingleton

        predict pred_dead365_d`i' if d_`i' == 1, xb
        predict rawfe_d`i' if d_`i' == 1, d
        replace rawfe_d`i' = rawfe_d`i' - rawu_d`i' if d_`i' == 1
        replace pred_dead365_d`i' = pred_dead365_d`i' + rawfe_d`i' if d_`i' == 1

        gen dtilde_d`i' = dead365 - pred_dead365_d`i' if d_`i' == 1
        gegen dtilde_hsa_d`i' = mean(dtilde_d`i') if d_`i' == 1, by(hsa)
        replace dtilde_d`i' = dtilde_d`i' - dtilde_hsa_d`i' + _b[_cons] if d_`i' == 1

        xtreg dtilde_d`i' if d_`i' == 1, mle
        estimates save "xtreg_docvam_patientBin`i'", replace

        local sigma_alpha = e(sigma_u)
        local sigma_eps   = e(sigma_e)
        local mu_hat      = _b[_cons]

        gen noise_std_dev_d`i' = e(sigma_e)

        tempvar nplan_d`i'
        gegen `nplan_d`i'' = count(1), by(npi_group)

        predict u_`days_dead'_d`i', u
        gegen what_d`i'     = mean(u_`days_dead'_d`i')
        gegen what_hsa_d`i' = mean(u_`days_dead'_d`i'), by(hsa)
        gen lambda_d`i' = `sigma_alpha'^2 / (`sigma_alpha'^2 + `sigma_eps'^2 / `nplan_d`i'')
        gen u_hsa_`days_dead'_d`i' = (u_`days_dead'_d`i' - what_hsa_d`i') * lambda_d`i' + what_hsa_d`i'

        gen u_`days_dead'_jackknife_d`i' = (u_`days_dead'_d`i' - dtilde_d`i' / `nplan_d`i'') * ///
            `nplan_d`i'' / (`nplan_d`i'' - 1)
        replace u_`days_dead'_jackknife_d`i' = what_d`i' if `nplan_d`i'' == 1
        replace u_`days_dead'_jackknife_d`i' = (u_`days_dead'_jackknife_d`i' - what_d`i') * ///
            lambda_d`i' + what_d`i' if `nplan_d`i'' > 1
        assert !mi(u_`days_dead'_jackknife_d`i') if !mi(u_`days_dead'_d`i')

        gen u_hsa_`days_dead'_jackknife_d`i' = (u_`days_dead'_d`i' - dtilde_d`i' / `nplan_d`i'') * ///
            `nplan_d`i'' / (`nplan_d`i'' - 1)
        replace u_hsa_`days_dead'_jackknife_d`i' = what_hsa_d`i' if `nplan_d`i'' == 1
        replace u_hsa_`days_dead'_jackknife_d`i' = (u_`days_dead'_jackknife_d`i' - what_hsa_d`i') * ///
            lambda_d`i' + what_hsa_d`i' if `nplan_d`i'' > 1
        assert !mi(u_hsa_`days_dead'_jackknife_d`i') if !mi(u_`days_dead'_d`i')

        gen u_`days_dead'_unshrunk_d`i' = u_`days_dead'_d`i'

        replace u_`days_dead'_d`i' = u_`days_dead'_d`i' * lambda_d`i'

        gstats sum lambda_d`i' u_`days_dead'_d`i'

        sum rawu_d`i'
        local unshrunk_mean = r(mean)
        sum u_`days_dead'_d`i'
        local shrunk_mean = r(mean)
        sum u_hsa_`days_dead'_d`i'
        local hsa_shrunk_mean = r(mean)
        sum u_`days_dead'_jackknife_d`i'
        local jackknife_mean = r(mean)
        sum u_hsa_`days_dead'_jackknife_d`i'
        local hsa_jackknife_mean = r(mean)

        gen demeaned_rawu_d`i'           = rawu_d`i' - `unshrunk_mean'
        gen demeaned_u_d`i'              = u_`days_dead'_d`i' - `shrunk_mean'
        gen demeaned_u_hsa_d`i'          = u_hsa_`days_dead'_d`i' - `hsa_shrunk_mean'
        gen demeaned_jackknife_u_d`i'    = u_`days_dead'_jackknife_d`i' - `jackknife_mean'
        gen demeaned_jackknife_hsa_u_d`i' = u_hsa_`days_dead'_jackknife_d`i' - `hsa_jackknife_mean'

        sum demeaned_u_hsa_d`i' u_hsa_`days_dead'_d`i'
    }

    * Stitch bin-specific variables into single variables matching doc 3 names
    local genVars pred_dead`days_dead' rawu dtilde u_`days_dead' lambda ///
        demeaned_u demeaned_rawu noise_std_dev u_hsa_`days_dead' demeaned_u_hsa ///
        demeaned_jackknife_u demeaned_jackknife_hsa_u u_`days_dead'_jackknife ///
        u_hsa_`days_dead'_jackknife u_`days_dead'_unshrunk

    * Map bin variable names to the unified names expected downstream
    local srcVars pred_dead365 rawu dtilde u_`days_dead' lambda ///
        demeaned_u demeaned_rawu noise_std_dev u_hsa_`days_dead' demeaned_u_hsa ///
        demeaned_jackknife_u demeaned_jackknife_hsa_u u_`days_dead'_jackknife ///
        u_hsa_`days_dead'_jackknife u_`days_dead'_unshrunk

    local n : word count `genVars'
    forval k = 1/`n' {
        local dest : word `k' of `genVars'
        local src  : word `k' of `srcVars'
        gen `dest' = .
        forval i = 1/`numQuantiles' {
            replace `dest' = `src'_d`i' if d_`i' == 1
        }
        assert !mi(`dest')
    }

    * Also carry forward what_hsa for collapse (used in doc 3 collapse varlist)
    gen what_hsa = .
    forval i = 1/`numQuantiles' {
        replace what_hsa = what_hsa_d`i' if d_`i' == 1
    }

    * Retain doc-3 extras not in the bin loop: one_patient and _10/_25/_50 variants
    * These require nplan which is bin-specific; reconstruct from bin tempvars
    gen one_patient = .
    forval i = 1/`numQuantiles' {
        tempvar nplan_check
        gegen `nplan_check' = count(1) if d_`i' == 1, by(npi_group)
        replace one_patient = (`nplan_check' == 1) if d_`i' == 1
    }

    gen demeaned_jackknife_hsa_u_shrunk = .
    gen demeaned_jackknife_hsa_u_10     = .
    gen demeaned_jackknife_hsa_u_25     = .
    gen demeaned_jackknife_hsa_u_50     = .

    forval i = 1/`numQuantiles' {
        tempvar nplan_i
        gegen `nplan_i' = count(1) if d_`i' == 1, by(npi_group)

        * shrunk hsa jackknife
        tempvar ujk_shrunk_i
        gen `ujk_shrunk_i' = (u_`days_dead'_d`i' / lambda_d`i' - dtilde_d`i' / `nplan_i') * ///
            `nplan_i' / (`nplan_i' - 1) if d_`i' == 1
        replace `ujk_shrunk_i' = ((`ujk_shrunk_i' - what_hsa_d`i') * lambda_d`i' + what_hsa_d`i') ///
            if d_`i' == 1 & `nplan_i' > 1
        replace `ujk_shrunk_i' = what_hsa_d`i' if d_`i' == 1 & `nplan_i' == 1

        sum u_hsa_`days_dead'_jackknife_d`i'
        local hsa_jackknife_mean_i = r(mean)
        replace demeaned_jackknife_hsa_u_shrunk = `ujk_shrunk_i' - `hsa_jackknife_mean_i' ///
            if d_`i' == 1

        * _10/_25/_50 threshold variants
        sum u_hsa_`days_dead'_jackknife_d`i'
        local hjk_mean = r(mean)

        replace demeaned_jackknife_hsa_u_10 = ///
            (u_`days_dead'_d`i' / lambda_d`i' - dtilde_d`i' / `nplan_i') * ///
            `nplan_i' / (`nplan_i' - 1) - `hjk_mean' ///
            if d_`i' == 1 & `nplan_i' >= 10

        replace demeaned_jackknife_hsa_u_25 = ///
            (u_`days_dead'_d`i' / lambda_d`i' - dtilde_d`i' / `nplan_i') * ///
            `nplan_i' / (`nplan_i' - 1) - `hjk_mean' ///
            if d_`i' == 1 & `nplan_i' >= 25

        replace demeaned_jackknife_hsa_u_50 = ///
            (u_`days_dead'_d`i' / lambda_d`i' - dtilde_d`i' / `nplan_i') * ///
            `nplan_i' / (`nplan_i' - 1) - `hjk_mean' ///
            if d_`i' == 1 & `nplan_i' >= 50
    }

    **********************************************************************
    * Everything below is identical to doc 3
    **********************************************************************
    
    * Because we expanded by 5 carrier data, need true nbene variable to distinguish how many patients
    bysort npi_group time_period : egen true_nbene = total(bene_wgt)
    
    count
    count if mi(time_period, bene_code)

    gisid time_period bene_code

    bysort npi_group : egen avg_median_hh_income = mean(median_hh_income)

    * Cleaning up data (Some benes assigned doctors before doctor starts in same period)
    drop if mi(npi)
    
    *****************************************
    *Change on depart definition:
    *****************************************

    merge m:1 npi hsa using `"${data}/depart_date_list_`pct'sortOfDepartures.dta"', generate(depart_merge)

    tab depart_merge
    keep if depart_merge == 1 | depart_merge == 3
    drop depart_merge

    gen depart = 0
    replace depart = 1 if inrange(departDate-1, time_period_start, time_period_end) & !mi(departDate, time_period_start, time_period_end)

    tab depart

    display("Before collapse")
    sum demeaned_rawu

    merge m:1 bene_code using `"${data}/tempfile_bene_codexw_`pct'"', keepusing(race sex) keep(3) nogen

    levelsof race, local(race_levels)
    replace sex = sex - 1

    foreach race_number of local race_levels{
        gen race_ind`race_number' = race == `race_number'
    }

    gisid bene_code time_period

    save `"${data}/tm_dvam_bcollapse_period_oldMu_`pct'pct.dta"', replace

    bysort npi hsa: egen mean_mort_total = mean(dead`days_dead')
    
    preserve
        collapse (mean) dead`days_dead', by(bene_code time_period)
        save `"${data}/tm_dvam_bcollapse_period_benelevel_oldMu_`pct'pct.dta"', replace
    restore

    * I think npi_group is missing if beneficiary was not seen in the data for the entire period.
    * CHECK THIS
    count if mi(npi_group)
    drop if mi(npi_group)

    rename total_benes total_benes_mu

    collapse (mean) what_hsa dead`days_dead' pred_dead`days_dead' rawu dtilde u_`days_dead' lambda ///
        demeaned_u demeaned_rawu noise_std_dev median_hh_income avg_median_hh_income mDual ///
        fe_sex round_age CharlsonScore race_ind* sex nbene true_nbene u_hsa_`days_dead' demeaned_u_hsa ///
        demeaned_jackknife_u demeaned_jackknife_hsa_u demeaned_jackknife_hsa_u_shrunk u_`days_dead'_jackknife u_hsa_`days_dead'_jackknife u_`days_dead'_unshrunk ///
        one_patient demeaned_jackknife_hsa_u_10 demeaned_jackknife_hsa_u_25 demeaned_jackknife_hsa_u_50, ///
        by(npi_group npi total_benes_mu modal_county hsa npi mean_mort_total)

    gisid npi_group

    gisid npi hsa
        
    bysort npi_group : egen sum_nbene = total(nbene)

    label var npi_group                   "Either at_npi (OP) or rfr_npi (carrier)"
    label var dead`days_dead'       "Death within `days_dead' days of last NPI encounter that year"
    label var nbene                 "Number of beneficiary encounters that year in the regression"
    label var rawu                  "Unshrunk doctor mortality effects"
    label var pred_dead`days_dead'  "Predicted Mortality"
    label var dtilde                "residual mortality"
    label var u_`days_dead'         "Shrunk doctor effects"
    label var lambda                "Shrinkage coefficient"
    label var true_nbene            "Number beneficiary encounters that year controlling for expansion of carrier sample"
    label var rawu                  "Demeaned unshrunk doctor mortality effects"
    label var demeaned_u            "Demeaned shrunk doctor effects"
    label var total_benes_mu        "Total patient-years for each doctor"
    label var noise_std_dev         "Noise standard deviation"
    label var u_hsa_`days_dead'     "Hsa-shrunk ME"
    label var demeaned_u_hsa        "Demeaned HRR-shrunk ME"
        
    display("After collapse")
    sum demeaned_rawu [fw = total_benes_mu]

    save `"${data}/tm_dvam_`days_dead'_`doc_type'_oldMu_`pct'pct.dta"', replace
        drop if total_benes_mu < 11
    save `"${data}/tm_dvam_`days_dead'_`doc_type'_oldMu_`pct'pct_share.dta"', replace
    
log close
end

capture program drop makeAmandaFile_oldMu
program makeAmandaFile_oldMu
    cap log close
    log using "${out}/logs/makeAmandaFile_oldMu.log", text replace
    syntax, pct(str)

    set seed 958819242

    use `"${data}/pcp_departures_wc_`pct'pct_sortOfDepartures.dta"', clear

    keep npi hsa departDate
    drop if mi(departDate)

    gen everDeparted = 1

    keep npi hsa everDeparted

    gduplicates drop

    gisid npi hsa

    rename npi lag_npi
    rename hsa lag_hsa

    tempfile everDepartedDocs

    save `everDepartedDocs'

    use npi sex using "${data}/doc_info_export.dta", clear
    *drop name_first name_last
    gduplicates drop
    bysort npi : gen numObs = _N
    drop if numObs > 1
    gisid npi
    gen doc_female = sex == "F"
    drop numObs sex
    tempfile contemp_sex
    save `contemp_sex'
    rename npi lag_npi
    rename doc_female lag_doc_female
    tempfile lag_sex
    save `lag_sex'

    use `"${data}/npiMortality`pct'_OP_fulldata.dta"', clear
    append using `"${data}/npiMortality`pct'_IP_fulldata.dta"'
        if ( "`pct'" == "100" ) {
            disp "There's no 100% carrier file; doing 20%"
            append using `"${data}/npiMortality20_CAR_fulldata.dta"'
        }
        else append using `"${data}/npiMortality`pct'_CAR_fulldata.dta"'

    keep npi from_dt

    sort npi from_dt

    gen year = year(from_dt)
    gen month = month(from_dt)
    gen MDate = ym(year, month)

    bysort npi (from_dt) : gen obsNum = _n

    keep if obsNum == 1


    gisid npi

    rename year minYear

    keep npi MDate minYear
    rename npi lag_npi
    rename MDate docMinDate

    tempfile docMinDates
    save `docMinDates'



    
    tempfile pcpMort
    tempfile pcpPct
    *tempfile pcpVAM
    tempfile pcpDepart

    /*
    * Make merge file to get pcpVAM
    use npi hsa demeaned_jackknife_hsa_u using `"${data}/tm_dvam_365_PCP_oldMu_`pct'pct.dta"', clear
    gisid npi hsa
    rename demeaned_jackknife_hsa_u mu_hat
    keep mu_hat npi hsa
    save `pcpVAM', replace
    
    */
    
    * CHANGE THIS:
    use `"${data}/tm_dvam_bcollapse_period_oldMu_`pct'pct.dta"', clear
    rename total_benes total_benes_mu
    
    di("Count 1")
    count
    gunique npi hsa

    format MDate departDate time_period_start time_period_end %tm
    replace time_period = departureMDate + (12 * time_period) if !mi(departureMDate)
    gen tempMonth = 1 if mi(departureMDate)
    replace time_period = ym(time_period, tempMonth) if mi(departureMDate)
    format time_period %tm


    gisid bene_code time_period

    sum depart

    count
    count if mi(time_period)

    describe modal_*
    drop modal_county
    *drop modal_*

    merge m:1 npi hsa using "${data}/tm_dvam_365_PCP_oldMu_`pct'pct.dta", keep(match) keepusing(demeaned_jackknife_hsa_u one_patient)
    rename demeaned_jackknife_hsa_u mu_hat
    gen zip_3 = substr(zip_cd, 1, 3)
    gen zip_5 = substr(zip_cd, 1, 5)

    di("Count 2")
    count
    gunique npi hsa

    drop state_county total_benes_mu nbene pred_dead365 modal_hsa
    drop numObs maxNumObs modeTag totalModes rand_norm highest_rand_norm replace_mode
    
    cap drop __*

    * Find modal 3-digit-zip
    encode zip_3, gen(zip_3_code)
    
    sort npi zip_3_code time_period
    
    bysort npi time_period zip_3_code : gen numObs = _N
    bysort npi time_period : egen maxNumObs = max(numObs)
    egen modeTag = tag(npi zip_3_code time_period) if numObs == maxNumObs
    bysort npi time_period : egen totalModes = total(modeTag)
    gen rand_norm = rnormal() if modeTag == 1
    bysort npi time_period : egen highest_rand_norm = max(rand_norm)
    gen modal_zip3 = zip_3_code if rand_norm == highest_rand_norm
    bysort npi time_period : egen replace_mode = max(modal_zip3)
    replace modal_zip3 = replace_mode

    /* NL - Find modal 5-digit-zip
    encode zip_5, gen(zip_5_code)
    bysort npi time_period zip_5_code : gen numObs = _N
    bysort npi time_period : egen maxNumObs = max(numObs)
    egen modeTag = tag(npi zip_5_code time_period) if numObs == maxNumObs
    bysort npi time_period : egen totalModes = total(modeTag)
    gen rand_norm = rnormal() if modeTag == 1
    bysort npi time_period : egen highest_rand_norm = max(rand_norm)
    gen modal_zip5 = zip_5_code if rand_norm == highest_rand_norm
    bysort npi time_period : egen replace_mode = max(modal_zip5)
    replace modal_zip5 = replace_mode
    */
    drop numObs maxNumObs modeTag totalModes rand_norm highest_rand_norm replace_mode
    
    * Find modal county
    egen state_county = group(state county)
    sort npi state_county time_period
    bysort npi time_period state_county : gen numObs = _N
    bysort npi time_period : egen maxNumObs = max(numObs)
    egen modeTag = tag(npi state_county time_period) if numObs == maxNumObs
    bysort npi time_period : egen totalModes = total(modeTag)
    gen rand_norm = rnormal() if modeTag == 1
    bysort npi time_period : egen highest_rand_norm = max(rand_norm)
    gen modal_county = state_county if rand_norm == highest_rand_norm
    bysort npi time_period : egen replace_mode = max(modal_county)
    replace modal_county = replace_mode

    drop numObs maxNumObs modeTag totalModes rand_norm highest_rand_norm replace_mode

    * Find modal hsa
    bysort npi time_period hsa : gen numObs = _N
    sort npi hsa time_period
    bysort npi time_period : egen maxNumObs = max(numObs)
    egen modeTag = tag(npi hsa time_period) if numObs == maxNumObs
    bysort npi time_period : egen totalModes = total(modeTag)
    gen rand_norm = rnormal() if modeTag == 1
    bysort npi time_period : egen highest_rand_norm = max(rand_norm)
    gen modal_hsa = hsa if rand_norm == highest_rand_norm
    bysort npi time_period : egen replace_mode = max(modal_hsa)
    replace modal_hsa = replace_mode

    drop numObs maxNumObs modeTag totalModes rand_norm highest_rand_norm replace_mode

    * Modal county test:
    egen county_tag = tag(npi time_period modal_county)
    bysort npi time_period : egen numCounties = total(county_tag)
    assert numCounties == 1

    drop county_tag numCounties

    preserve
    gen lag_npi = npi
    gen lag_mu = mu_hat

    bysort lag_npi : egen total_mort = total(dead365)
    bysort lag_npi : gen total_benes = _N
    
    assert total_mort <= total_benes

    gen mort_rate = total_mort / total_benes       

    gcollapse (mean) lag_mu mort_rate total_mort total_benes, by(lag_npi)

    gisid lag_npi

    gunique lag_npi

    save `pcpMort'
    restore

    preserve
        *save `"${data}/pcpPct_beforework_toDelete.dta"', replace
	keep bene_code time_period npi zip_3 zip_3_code modal_zip3 year modal_county
	
        gisid bene_code time_period
        gen lag_npi = npi
        gen lag_county = modal_county
        gen lag_zip_3 = zip_3
        gen lag_zip_3_code = zip_3_code
        gen lag_modal_zip3 = modal_zip3

        bysort lag_zip_3 time_period : gen zip_3_benes_lag = _N
        bysort lag_zip_3 time_period lag_npi : gen npi_zip_3_benes_lag = _N

        gen pctZip_3 = npi_zip_3_benes_lag/zip_3_benes_lag

        listsome zip_3_benes_lag npi_zip_3_benes_lag pctZip_3 if pctZip_3 > 1

        assert pctZip_3 <= 1

        replace pctZip_3 = pctZip_3 * 100


        gen modalPlaceholder = pctZip_3 if lag_zip_3_code == lag_modal_zip3


        bysort lag_npi time_period : egen modal_pctZip_3 = max(modalPlaceholder)

        assert(!mi(modal_pctZip_3))


        rename year last_year
        rename time_period last_time_period

        keep last_time_period lag_npi modal_pctZip_3 lag_modal_zip3
        duplicates drop

        gisid lag_npi last_time_period

        save `pcpPct'
        *save `"${data}/pcpPct_afterwork_toDelete.dta"', replace
    restore


    * Gen number of beneficiaries
    bysort npi time_period : gen nbene = _N

    bysort npi time_period : egen one_month_mort = mean(dead365)


    gisid bene_code time_period

    gunique npi

    

    /*
    reghdfe one_year_mort i.fe_sex i.fe_race i.round_age i.mDual median_hh_income i.CharlsonScore ///
        , noabsorb resid keepsingleton

    predict pred_dead365, xb
    */
    *gen resid = one_year_mort - pred_dead365

    gen time = time_period

    gen departDate_clean = departDate
    

    gen last_year = year - 1

    drop depart

    gen depart = 0
    replace depart = 1 if inrange(departDate-13, time_period_start, time_period_end) & !mi(departDate, time_period_start, time_period_end)


    gen willDrop = 0

    gen last_time_period = time_period - 12

    sort bene_code time_period

    save "${data}/willDrop_debug_`pct'pct.dta", replace

    di("Will drop")
    by bene_code (time_period) : replace willDrop = 1 if ///
        depart[_n-1] == 1 & time_period[_n-1] == last_time_period

    gen two_period_test = willDrop == 1

    replace willDrop = 1 if ///
        inrange(departDate-1, time_period_start, time_period_end) ///
        & !mi(departDate, time_period_start, time_period_end)

    di("Dropped obs")
    by bene_code (time_period) : gen droppedObs = willDrop[_n-1] if ///
        time_period[_n-1] == last_time_period

    by bene_code (time_period) : gen modify_period = two_period_test[_n-1] if ///
        time_period[_n-1] == last_time_period

    preserve

        keep bene_code time_period last_time_period depart departDate time_period_start time_period_end willDrop npi hsa droppedObs two_period_test modify_period

        gen tp_act= time_period

        save "${data}/last_tp_debug_`pct'pct.dta", replace

    restore

    

    drop if willDrop == 1

    di("Count 3")
    count
    gunique npi hsa


    replace last_time_period = time_period - 24 if droppedObs == 1 & modify_period == 1

    * Note: expenditure data only for 20% of beneficiaries 
    * 100% file is the 20% expenditure
    merge 1:1 bene_id time_period using `"${data}/bene_util_period_`pct'pct.dta"', gen(exp_merge) ///
    	keep(1 3) keepusing(exp_car_period exp_ip_period exp_op_period exp_total_period)

    gisid bene_code time_period
    gisid bene_code last_time_period

    egen hsa_period = group(hsa time_period)

    bysort npi time_period : gen nbene_period = _N

    sort bene_code time_period
    by bene_code (time_period) : gen lag_npi = npi[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_county = modal_county[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_modal_zip3 = modal_zip3[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_zip = zip_cd[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_zip_3 = zip_5[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_zip_3_code = zip_3_code[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_modal_hsa = modal_hsa[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_mDual = mDual[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_Charlson = CharlsonScore[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_hsa = hsa[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_benes = nbene_period[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_terminate = depart[_n-1] if ///
        time_period[_n-1] == last_time_period
    by bene_code (time_period) : gen lag_dead = dead365[_n-1] if ///
        time_period[_n-1] == last_time_period


    drop if mi(lag_npi)
    di("After drop")
    count

    di("Count 4")
    count
    gunique npi hsa


    * Debugging code
    gisid lag_npi bene_code time_period
    

    sum mu_hat, detail

    local median_mu = r(p50)

    gen dum_med = 0
    replace dum_med = 1 if mu_hat > `median_mu'

    merge m:1 lag_npi using `pcpMort', keepusing(lag_mu mort_rate total_mort total_benes) keep(1 3) nogen

    * DO I NEED TO FIX THIS?
    merge m:1 lag_npi last_time_period using `pcpPct', gen(pcpPct_merge)
    keep if pcpPct_merge == 1 | pcpPct_merge == 3


    gisid bene_code time_period


    
    gen instrument = lag_mu * lag_terminate

    


    bysort lag_npi time_period : egen mean_test = mean(dead365)

    xtile benes_dec = lag_benes, nq(10)

    * Merge in subspecialty
    merge m:1 lag_npi using "${data}/full_lag_doc_subspec.dta", generate(subspec_merge) keep(1 3)

    di("Count 5")
    count
    gunique npi hsa
    
    /*
    * Assign CMS diagnosis code era based on midpoint of the 12-month time period
    gen mid_tp = time_period + 6
    gen charlson_era = 3                                // default: Jan 2009 - Sep 2015
    replace charlson_era = 1 if mid_tp <= ym(2008, 12) // Era 1: Jan 2006 - Dec 2008
    replace charlson_era = 2 if mid_tp >= ym(2015, 10) // Era 2: Oct 2015 - Dec 2019
    drop mid_tp

    
    gen age_month = month(dofm(time_period))
    gen age_year = year(dofm(time_period))
    gen dec_age = ((mdy(age_month, 1, age_year) - sdob) / 365.25)
    */
    reghdfe dead365 i.fe_sex i.fe_race dec_age i.mDual median_hh_income i.charlson_era#i.CharlsonScore ///
        i.cc_* ///
        , ///
        absorb(hsa) resid keepsingleton


    predict pred_dead365, xbd

    sort bene_code time_period
    by bene_code (time_period) : gen lag_pred_dead = pred_dead365[_n-1] if ///
        time_period[_n-1] == last_time_period

    count

    *merge m:1 lag_npi year using `"${data}/doc_tenure_prelim.dta"', keepusing(doc_tenure min_year) keep(1 3) gen(tenure_merge)
    merge m:1 lag_npi using `docMinDates', keep(1 3) gen(tenure_merge)
    gen doc_tenure = floor((time_period - docMinDate) / 12)

    listsome lag_npi year if tenure_merge == 1

    keep if tenure_merge == 3

    *bysort lag_npi time_period : egen tenure_ph = min(doc_tenure)
    *replace doc_tenure = tenure_ph

    preserve
    use "${data}/npiTopMedCrosswalk.dta", clear
    merge m:1 medicalschoolname using "${data}/int_med_school_crosswalk_filled.dta", keepusing(d_international) keep(1 3)
    save "${data}/npiTopMedCrosswalk_modified.dta", replace
    restore

    count
    preserve
    import excel "${data}/medschool_tuition_fees.xlsx", sheet(modified) firstrow clear
    duplicates drop MedicalSchoolName OwnershipType, force
    ren MedicalSchoolName medicalschoolname
    replace medicalschoolname = upper(medicalschoolname)
    merge 1:m  medicalschoolname using "${data}/npiTopMedCrosswalk_modified.dta", gen(medschool_match)
    keep if medschool_match == 3
    gen Med_Private = (OwnershipType == "Private")
    replace Med_Private = . if OwnershipType == "Historic"
    tempfile npi_medschool
    save `npi_medschool'
    restore 
    
    merge m:1 lag_npi using `npi_medschool', keepusing(top25Med isMD Med_Private d_international) keep(1 3) gen(lag_docQualMerge)
    ren top25Med lag_top25Med
    ren isMD lag_isMD
    ren Med_Private lag_Med_Private
    ren d_international lag_d_international
    merge m:1 npi using `npi_medschool', keepusing(top25Med isMD Med_Private d_international) keep(1 3) gen(contemp_docQualMerge)
    merge m:1 lag_npi using `lag_sex', keep(1 3) gen(lag_sex)
    merge m:1 npi using `contemp_sex', keep(1 3) gen(contemp_sex)
    count
    sum top25Med isMD Med_Private

    di("Count 6")
    count
    gunique npi hsa
    

    reghdfe dead365 modal_pctZip_3 i.doc_tenure top25Med isMD, noabsorb resid keepsingleton
    predict pred_mu, xb

    reghdfe pred_mu mu_hat, noabsorb resid keepsingleton
    predict resid, residuals

    reghdfe modal_pctZip_3 mu_hat, noabsorb resid keepsingleton
    predict resid_docShare, residuals

    reghdfe dead365 i.doc_tenure, noabsorb resid keepsingleton
    predict pred_mu_tenure, xb

    reghdfe pred_mu_tenure mu_hat, noabsorb resid keepsingleton
    predict resid_docTenure, residuals

    reghdfe top25Med mu_hat, noabsorb resid keepsingleton
    predict resid_schoolQual, residuals

    reghdfe isMD mu_hat, noabsorb resid keepsingleton
    predict resid_credential, residuals

    xtile decile = lag_mu if lag_mu != ., nq(10)

    bysort lag_npi time_period : gen nbene_lag = _N
    bysort lag_county time_period : gen nbene_lag_county = _N

    bysort lag_npi time_period lag_hsa : gen nbene_hsa_lag = _N

    bysort lag_npi time_period : gen doc_share = nbene_lag / nbene_lag_county
    

    save `"${data}/microdata_test_period_oldMu_`pct'pct.dta"', replace

    merge m:1 lag_npi using "${data}/lag_npi_birthyear.dta", gen(birthday_match)
    keep if birthday_match == 3
    gen lag_npi_age = year - birth_year

    *TO DO: Resolve multiple lag_zip_3 issue for Amanda file

    di("Count 7")
    count
    gunique npi hsa

    drop lag_modal_zip3
    bysort lag_npi time_period : egen lag_modal_zip3 = mode(lag_zip_3), minmode

    merge m:1 lag_npi lag_hsa using `everDepartedDocs', nogen keep(1 3)

    preserve
    use "${data}/npi_birthyear.dta", clear
    ren birth_year contemp_birth_year
    tempfile contemp_npi_birthyear
    save `contemp_npi_birthyear'
    restore
    
    merge m:1 npi using `contemp_npi_birthyear', gen(contemp_birthday_match) keep(1 3)

    di("Count 7")
    count
    gunique npi hsa

    gen npi_age = year - contemp_birth_year


    drop lag_modal_zip3
    bysort lag_npi time_period : egen lag_modal_zip3 = mode(lag_zip_3), minmode

    drop d_1 d_2 d_3
    xtile dec_lag_mu=lag_mu if everDeparted == 1, nq(10)
    forval d=1/10 {
        gen d_`d' = dec_lag_mu==`d'
        gen t_d_`d' = lag_terminate*d_`d'
    }

    drop d_1

    *merge Bleemer file for time-invariant doctor chars
    * V_Race, V_Party, Voter, V_HasSpouse, V_SpouseDoctor, School_Rank_Research, School_Rank_PrimCare, V_logHomeValue
    * Race 
    
    preserve
    import delimited using "${data}/Bleemer_DoctorFile.csv", clear
    ren npi lag_npi
    tostring lag_npi, replace 
    local provider_chars v_homevalue v_hhinc school_rank_research school_rank_primcare school_mcat school_studtofac dox_res_ranking1 dox_res_ranking2 dox_res_ranking3 dox_review
    
    foreach var in `provider_chars' {
    	replace `var' = "" if `var' == "NA"
    	destring `var', replace
    }
    
    * political
    gen lag_doc_Republican = (v_party == "Republican") if v_party != "NA"
    gen lag_doc_Democrat = (v_party == "Democratic") if v_party != "NA"
    gen lag_doc_reg_voter = (voter == "TRUE") if voter != "NA"
    
    * demographic 
    gen lag_doc_SouthAsian = (v_race_detailed == "Afghan" | v_race_detailed == "Bandladeshi" | v_race_detailed == "Indian/Hindu" | v_race_detailed == "Nepalese" | v_race_detailed == "Pakistani" | v_race_detailed == "Sri Lankan") if v_race_detailed != "NA"
    gen lag_doc_SoutheastAsian = (v_race_detailed == "Filipino" | v_race_detailed == "Indonesian" | v_race_detailed == "Thai" | v_race_detailed == "Khmer" | v_race_detailed == "Laotian" | v_race_detailed == "Myanmar (Burmese)") if v_race_detailed != "NA"
    gen lag_doc_WestAsian = (v_race_detailed == "Arab" | v_race_detailed == "Armenian" | v_race_detailed == "Azerb" | v_race_detailed == "Persian" | v_race_detailed == "Turkish" | v_race_detailed == "Chechnian" | v_race_detailed == "Georgian" | v_race_detailed == "Kurdish") if v_race_detailed != "NA"
    gen lag_doc_EastAsian = (v_race_detailed == "Japanese" | v_race_detailed == "Tibetan" | v_race_detailed == "Chinese" | v_race_detailed == "Korean" | v_race_detailed == "Mongolian") if v_race_detailed != "NA"
    gen lag_doc_White = (v_race == "White" | v_race_detailed == "Bosnian Muslim") if v_race_detailed != "NA"
    gen lag_doc_Black = (v_race_detailed == "African or Af-Am" | v_race_detailed == "Likely Af-Am (Modeled)") if v_race_detailed != "NA"
    gen lag_doc_Hispanic = (v_race == "Hispanic") if v_race_detailed != "NA"
    
    egen p75_wealth = pctile(v_homevalue), p(75)
    gen lag_doc_highWealth = (v_homevalue > p75_wealth) if v_homevalue != .
    drop p75_wealth
    
    egen p75_income = pctile(v_hhinc), p(75)
    gen lag_doc_highIncome = (v_hhinc > p75_income) if v_hhinc != .
    drop p75_income
    
    * training 
    ren school_rank_research lag_SchoolRank_Research 
    ren school_rank_primcare lag_SchoolRank_PrimCare 
    gen lag_Top10_Research = (lag_SchoolRank_Research <= 10) if lag_SchoolRank_Research != .
    gen lag_Top10_PrimCare = (lag_SchoolRank_PrimCare <= 10) if lag_SchoolRank_PrimCare != .
    gen lag_has_PrimCare_Rank = (has_primcare_ranking == "TRUE") if has_primcare_ranking != "NA"
    ren school_mcat lag_School_MCAT
    	replace lag_School_MCAT = . if lag_School_MCAT == .
    ren school_studtofac lag_School_StudtoFac
    	replace lag_School_StudtoFac = . if lag_School_StudtoFac == .
    destring dox_res_ranking1 dox_res_ranking2 dox_res_ranking3, replace force 
    egen lag_Avg_Residency_Rank = rowmean(dox_res_ranking1 dox_res_ranking2 dox_res_ranking3)
    egen lag_Best_Residency_Rank = rowmin(dox_res_ranking1 dox_res_ranking2 dox_res_ranking3)
    gen lag_Top25_Residency = (lag_Best_Residency_Rank <= 25) if lag_Best_Residency_Rank != .
    gen lag_Multiple_Residency = ((dox_res_ranking1 != . & dox_res_ranking2 != .) | (dox_res_ranking1 != . & dox_res_ranking3 != .) | (dox_res_ranking2 != . & dox_res_ranking3 != .))
    gen lag_doc_Fellowship = (dox_fell_hospital1 != "NA" | dox_fell_hospital2 != "NA")
    
    * family
    gen lag_has_spouse = (v_hasspouse == "TRUE") if v_hasspouse != "NA"
    gen lag_spouse_doc = (v_spousedoctor == "TRUE") if v_spousedoctor != "NA"
    
    * online reputation
    ren dox_review lag_avg_reputation 
    gen lag_has_review = (dox_hasreview == "TRUE") if dox_hasreview != "NA"
    gen lag_has_dox_profile = (has_dox == "TRUE") if has_dox != "NA"
    
    keep lag_npi lag_doc_Republican lag_doc_Democrat lag_doc_reg_voter lag_doc_SouthAsian lag_doc_SoutheastAsian lag_doc_WestAsian lag_doc_EastAsian lag_doc_White lag_doc_Black lag_doc_Hispanic lag_doc_highWealth lag_doc_highIncome lag_SchoolRank_Research lag_SchoolRank_PrimCare lag_Top10_Research lag_Top10_PrimCare lag_has_PrimCare_Rank lag_School_MCAT lag_School_StudtoFac lag_Avg_Residency_Rank lag_Best_Residency_Rank lag_Top25_Residency lag_Multiple_Residency lag_doc_Fellowship lag_has_spouse lag_spouse_doc lag_avg_reputation lag_has_review lag_has_dox_profile
    
    tempfile lag_bleemer_chars_df
    save `lag_bleemer_chars_df' 
    restore 
    merge m:1 lag_npi using `lag_bleemer_chars_df', gen(lag_bleemer_match)
    drop if lag_bleemer_match == 2
    
    preserve 
    import delimited using "${data}/Bleemer_DoctorFile.csv", clear
    tostring npi, replace 
    local provider_chars v_homevalue v_hhinc school_rank_research school_rank_primcare school_mcat school_studtofac dox_res_ranking1 dox_res_ranking2 dox_res_ranking3 dox_review
    
    foreach var in `provider_chars' {
    	replace `var' = "" if `var' == "NA"
    	destring `var', replace
    }
    
    * political
    gen doc_Republican = (v_party == "Republican") if v_party != "NA"
    gen doc_Democrat = (v_party == "Democratic") if v_party != "NA"
    gen doc_reg_voter = (voter == "TRUE")  if voter != "NA"
    
    * demographic 
    gen doc_SouthAsian = (v_race_detailed == "Afghan" | v_race_detailed == "Bandladeshi" | v_race_detailed == "Indian/Hindu" | v_race_detailed == "Nepalese" | v_race_detailed == "Pakistani" | v_race_detailed == "Sri Lankan")  if v_race_detailed != "NA"
    gen doc_SoutheastAsian = (v_race_detailed == "Filipino" | v_race_detailed == "Indonesian" | v_race_detailed == "Thai" | v_race_detailed == "Khmer" | v_race_detailed == "Laotian" | v_race_detailed == "Myanmar (Burmese)") if v_race_detailed != "NA"
    gen doc_WestAsian = (v_race_detailed == "Arab" | v_race_detailed == "Armenian" | v_race_detailed == "Azerb" | v_race_detailed == "Persian" | v_race_detailed == "Turkish" | v_race_detailed == "Chechnian" | v_race_detailed == "Georgian" | v_race_detailed == "Kurdish") if v_race_detailed != "NA"
    gen doc_EastAsian = (v_race_detailed == "Japanese" | v_race_detailed == "Tibetan" | v_race_detailed == "Chinese" | v_race_detailed == "Korean" | v_race_detailed == "Mongolian")  if v_race_detailed != "NA"
    gen doc_White = (v_race == "White" | v_race_detailed == "Bosnian Muslim") if v_race_detailed != "NA"
    gen doc_Black = (v_race_detailed == "African or Af-Am" | v_race_detailed == "Likely Af-Am (Modeled)") if v_race_detailed != "NA"
    gen doc_Hispanic = (v_race == "Hispanic") if v_race_detailed != "NA"
    
    egen p75_wealth = pctile(v_homevalue), p(75)
    gen doc_highWealth = (v_homevalue > p75_wealth) if v_homevalue != .
    drop p75_wealth
    
    egen p75_income = pctile(v_hhinc), p(75)
    gen doc_highIncome = (v_hhinc > p75_income) if v_hhinc != .
    drop p75_income
      
    * training 
    ren school_rank_research SchoolRank_Research 
    ren school_rank_primcare SchoolRank_PrimCare 
    gen Top10_Research = (SchoolRank_Research <= 10) if SchoolRank_Research != .
    gen Top10_PrimCare = (SchoolRank_PrimCare <= 10) if SchoolRank_PrimCare != .
    gen has_PrimCare_Rank = (has_primcare_ranking == "TRUE") if has_primcare_ranking != "NA"
    ren school_mcat School_MCAT
    	replace School_MCAT = . if School_MCAT == .
    ren school_studtofac School_StudtoFac
    	replace School_StudtoFac = . if School_StudtoFac == .
    destring dox_res_ranking1 dox_res_ranking2 dox_res_ranking3, replace force 
    egen Avg_Residency_Rank = rowmean(dox_res_ranking1 dox_res_ranking2 dox_res_ranking3)
    egen Best_Residency_Rank = rowmin(dox_res_ranking1 dox_res_ranking2 dox_res_ranking3)
    gen Top25_Residency = (Best_Residency_Rank <= 25) if Best_Residency_Rank != .
    gen Multiple_Residency = ((dox_res_ranking1 != . & dox_res_ranking2 != .) | (dox_res_ranking1 != . & dox_res_ranking3 != .) | (dox_res_ranking2 != . & dox_res_ranking3 != .))
    gen doc_Fellowship = (dox_fell_hospital1 != "NA" | dox_fell_hospital2 != "NA")
    
    * family
    gen has_spouse = (v_hasspouse == "TRUE") if v_hasspouse != "NA"
    gen spouse_doc = (v_spousedoctor == "TRUE") if v_spousedoctor != "NA"
    
    * online reputation
    ren dox_review avg_reputation 
    gen has_review = (dox_hasreview == "TRUE") if dox_hasreview != "NA"
    gen has_dox_profile = (has_dox == "TRUE") if has_dox != "NA"
    
    keep npi doc_Republican doc_Democrat doc_reg_voter doc_SouthAsian doc_SoutheastAsian doc_WestAsian doc_EastAsian doc_White doc_Black doc_Hispanic doc_highWealth doc_highIncome SchoolRank_Research SchoolRank_PrimCare Top10_Research Top10_PrimCare has_PrimCare_Rank School_MCAT School_StudtoFac Avg_Residency_Rank Best_Residency_Rank Top25_Residency Multiple_Residency doc_Fellowship has_spouse spouse_doc avg_reputation has_review has_dox_profile
    
    tempfile bleemer_chars_df
    save `bleemer_chars_df' 
    
    restore 
    merge m:1 npi using `bleemer_chars_df', gen(bleemer_match)
    drop if bleemer_match == 2

    * merge ever acquired or ever employed by payor 
    preserve 
    use "${data}/npi_tin_crosswalk.dta", clear 
    ren npi lag_npi
    ren ever_acquired_at_time lag_ever_acquired_at_time
    ren ever_payor_employed lag_ever_payor_employed
    tempfile lag_npi_tin
    save `lag_npi_tin'

    restore 
    merge m:1 npi using "${data}/npi_tin_crosswalk.dta", gen(npi_tin_merge) keep(1 3)
    
    merge m:1 lag_npi using `lag_npi_tin', gen(lag_npi_tin_merge) keep(1 3)

    save `"${data}/physician_exit_microdata_hsa_period_oldMu_`pct'pct.dta"', replace

    preserve
    use "${data}/partd_pcps_dos_measures_2013_2019.dta", clear
    keep npi year total_drug_cost total_claims total_beneficiaries opioid_prescribing_rate drug_cost_per_bene
    tostring npi, replace
    tempfile partd
    save `partd'
    restore 


    preserve
    collapse (first) exp_car_period exp_ip_period exp_op_period exp_total_period exp_merge, by(npi year)
    merge m:1 npi year using `partd', gen(partd_exp_merge) keep(1 3)
    save `"${data}/josh_partd_merge_diagnostics.dta"', replace
    restore
    
    gcollapse (mean) dead365 mu_hat one_patient pred_dead* resid resid_docShare resid_docTenure ///
        resid_schoolQual resid_credential lag_mu instrument decile round_age fe_sex doc_female top25Med isMD npi_age Med_Private ///
        fe_race mDual race_ind* modal_pctZip_3 cc_* lag_mDual lag_Charlson spec_ind* lag_npi_age ///
        doc_Republican doc_Democrat doc_reg_voter doc_SouthAsian doc_SoutheastAsian doc_WestAsian doc_EastAsian doc_White doc_Black doc_Hispanic ///
        doc_highWealth doc_highIncome SchoolRank_Research SchoolRank_PrimCare Top10_Research Top10_PrimCare has_PrimCare_Rank School_MCAT School_StudtoFac ///
        Avg_Residency_Rank Best_Residency_Rank Top25_Residency Multiple_Residency doc_Fellowship has_spouse spouse_doc ///
        avg_reputation has_review has_dox_profile lag_doc_Republican lag_doc_Democrat lag_doc_reg_voter lag_doc_SouthAsian lag_doc_SoutheastAsian lag_doc_WestAsian ///
        lag_doc_EastAsian lag_doc_White lag_doc_Black lag_doc_Hispanic lag_doc_highWealth lag_doc_highIncome lag_SchoolRank_Research lag_SchoolRank_PrimCare ///
        lag_Top10_Research lag_Top10_PrimCare lag_has_PrimCare_Rank lag_School_MCAT lag_School_StudtoFac lag_Avg_Residency_Rank lag_Best_Residency_Rank ///
        lag_Top25_Residency lag_Multiple_Residency lag_doc_Fellowship lag_has_spouse lag_spouse_doc lag_avg_reputation lag_has_review lag_has_dox_profile exp* ///
        lag_ever_acquired_at_time lag_ever_payor_employed ever_acquired_at_time ever_payor_employed d_international lag_d_international, ///
        by(lag_npi time_period last_time_period lag_terminate nbene_hsa_lag lag_hsa lag_modal_zip3 minYear birth_year lag_doc_female lag_top25Med lag_isMD lag_Med_Private)

        * lag_modal_hsa


    save `"${data}/amanda_debug_oldMu_`pct'pct.dta"', replace


    gisid lag_npi time_period lag_hsa

    sort lag_npi time_period

    
    bysort decile : egen obs_lag_mort = mean(lag_mu)

    *egen county_year = concat(lag_county year), punct(-)
    *egen county_MDate = concat(lag_county MDate), punct(-)

    *gisid lag_npi county_MDate

    save `"${data}/figure4_phys_exit_period_oldMu_`pct'pct.dta"', replace
    

    tabulate decile, generate(deci_ind)

    *merge m:1 lag_npi year using `"${data}/doc_tenure_prelim.dta"', keepusing(doc_tenure min_year) keep(3) nogen

    *reghdfe dead365 modal_pctZip_3 i.doc_tenure [aw = nbene_lag], noabsorb resid keepsingleton
    *predict resid, residuals
    *predict pred_dead_docQual, xb
    *predict pred_dead365, xb

    save `"${data}/physician_exit_amanda_file_hsa_period_oldMu_`pct'pct.dta"', replace

    log close
end

capture program drop runFigureIV_oldMu
program runFigureIV_oldMu
    syntax, pct(str)

    set seed 274306315

    
    use `"${data}/pcp_departures_wc_`pct'pct_sortOfDepartures.dta"', clear

    keep npi hsa departDate
    drop if mi(departDate)

    gen everDeparted = 1

    keep npi hsa everDeparted

    gduplicates drop

    gisid npi hsa

    rename npi lag_npi
    rename hsa lag_hsa

    tempfile everDepartedDocs

    save `everDepartedDocs'


    
    use "${data}/npi_time_period_clinics_20pct.dta", clear
    rename npi lag_npi
    rename modalClinic lag_clinic
    rename time_period last_time_period
    tempfile clinicData
    save `clinicData'

    
    use `"${data}/microdata_test_period_oldMu_`pct'pct.dta"', clear

    merge m:1 lag_npi using "${data}/lag_npi_birthyear.dta", gen(birthday_match)

    keep if birthday_match == 3

    gen lag_npi_age = year - birth_year


    glevelsof decile

    egen lag_hsa_period = group(lag_hsa time_period)
    egen lag_zip_period = group(lag_zip time_period)

    di("Encode vars")
    encode lag_zip, gen(lag_zip_encode)

    di("Calculate total benes")
    bysort lag_hsa time_period : egen total_benes_hsa = total(nbene_hsa_lag)

    gen docShare_hsa = nbene_hsa_lag/total_benes_hsa


    merge m:1 lag_npi lag_hsa using `everDepartedDocs', generate(everDepMerge)

    keep if everDepMerge == 3

    merge m:1 lag_npi last_time_period using `clinicData', generate (clinicMerge)

    keep if clinicMerge == 3

    xtile dec_lag_mu=lag_mu [aw = nbene_hsa_lag], nq(10)
    forval d=1/10 {
        gen d_`d' = dec_lag_mu==`d'
        gen t_d_`d' = lag_terminate*d_`d'
    }

    drop d_1

    di("Flexible lag_mu")

    di("First Stage")
    reghdfe mu_hat instrument d_* docShare_hsa lag_npi_age, absorb(cc_* lag_clinic lag_zip_period lag_terminate#time_period lag_terminate#lag_zip_encode) vce(cluster lag_hsa) keepsingleton

    di("Balance")
    reghdfe pred_dead365 instrument d_* docShare_hsa lag_npi_age, absorb(cc_* lag_clinic lag_zip_period lag_terminate#time_period lag_terminate#lag_zip_encode) vce(cluster lag_hsa) keepsingleton


    di("Fallback")

    reghdfe resid instrument d_* docShare_hsa lag_npi_age, absorb(cc_* lag_clinic lag_zip_period lag_terminate#time_period lag_terminate#lag_zip_encode) vce(cluster lag_hsa) keepsingleton


    di("Reduced Form")
    reghdfe dead365 instrument d_* docShare_hsa lag_npi_age, absorb(cc_* lag_clinic lag_zip_period lag_terminate#time_period lag_terminate#lag_zip_encode) vce(cluster lag_hsa) keepsingleton

    di("IV")
    ivreghdfe dead365 (mu_hat = instrument) d_* docShare_hsa lag_npi_age, absorb(cc_* lag_clinic lag_zip_period lag_terminate#time_period lag_terminate#lag_zip_encode) cluster(lag_hsa) keepsingleton
    

    * FIGURE 4

    use `"${data}/microdata_test_period_oldMu_`pct'pct.dta"', clear

    merge m:1 lag_npi using "${data}/lag_npi_birthyear.dta", gen(birthday_match)

    keep if birthday_match == 3

    gen lag_npi_age = year - birth_year

    rename lag_terminate terminate
    rename mu_hat mu


    glevelsof decile

    egen lag_hsa_period = group(lag_hsa time_period)
    egen lag_zip_period = group(lag_zip time_period)

    di("Encode vars")
    encode lag_zip, gen(lag_zip_encode)

    di("Calculate total benes")
    bysort lag_hsa time_period : egen total_benes_hsa = total(nbene_hsa_lag)

    gen docShare_hsa = nbene_hsa_lag/total_benes_hsa


    merge m:1 lag_npi lag_hsa using `everDepartedDocs', generate(everDepMerge)

    keep if everDepMerge == 3

    merge m:1 lag_npi last_time_period using `clinicData', generate (clinicMerge)

    keep if clinicMerge == 3

    xtile dec_lag_mu=lag_mu, nq(10)
    forval d=1/10 {
        gen d_`d' = dec_lag_mu==`d'
        gen t_d_`d' = terminate*d_`d'
    }

    drop d_1

    
    foreach var of varlist mu lag_mu pred_dead365 resid dead365{
        reghdfe `var' d_* t_d_* docShare_hsa lag_npi_age ///
            , absorb(lag_clinic lag_zip3_period terminate#time_period terminate#zip3_code) keepsingleton

        gen `var'_dis0 = 0 if _n<=10
        gen `var'_dis1 = 0 if _n<=10


        
        forval d=1/10 { /* store non-terminated outcome means and that + termination effects */
            di("var `var' decile `d'")
            summ `var' if terminate==0 & dec_lag_mu==`d'
            replace `var'_dis0 = r(mean) if _n==`d'
            replace `var'_dis1 = r(mean) + _b[t_d_`d'] if _n==`d'
        }

        

        qui summ `var'
        local `var'_mn=r(mean) /* used below for re-normalization */
    }

    keep *_dis0 *_dis1
    drop if mu_dis0 == .
    duplicates drop


    describe, fullnames

    * pred_dead365_fe_sex pred_dead365_fe_race ///
    * pred_dead365_round_age pred_dead365_mDual ///
    * pred_dead365_CharlsonScore pred_dead365_hh_income ///
    * pred_dead365_benes_dec

    foreach var1 in mu lag_mu pred_dead365 ///
        resid dead365{ /* fill in omitted group + renormalize lines to have the same overall outcome mean */
        foreach var2 in `var1'_dis0 `var1'_dis1 {
            di("var2: `var2'")
            summ `var2'
            replace `var2' = `var2' - r(mean) + ``var1'_mn'
        }
    }

    describe

    save `"${data}/amandaPlotData_oldMu_`pct'pct.dta"', replace
    


    use `"${data}/amandaPlotData_oldMu_`pct'pct.dta"', clear
    
    local legend label(1 "Nonterminated PCPs") label(3 "Terminated PCPs")
    local legend `legend' order(1 3) cols(1) ring(1) position(1) bmargin(small)
    local xtitle "Lagged Observational Mortality"

    * Panel A
    scatter mu_dis0 lag_mu_dis0, color(blue) msymbol(circle) ///
        || lfit mu_dis0 lag_mu_dis0, color(blue) lwidth(0.5) ///
        || scatter mu_dis1 lag_mu_dis1, color(orange) msymbol(circle) ///
        || lfit mu_dis1 lag_mu_dis1, color(orange) lwidth(0.5) ///
        xtitle(`xtitle') ///
        ytitle("Observational Mortality") legend(`legend') ///
        title("A. First Stage", color(black) pos(12) ) ylabel(-.04 (.02) .02)
    graph export `"${out}/pcp/figure4_phys_exit_panA_`pct'pct_oldMu.png"', width(800) height(600) replace


    *PANEL B
    scatter pred_dead365_dis0 lag_mu_dis0, color(blue) msymbol(circle) ///
        || lfit pred_dead365_dis0 lag_mu_dis0, color(blue) lwidth(0.5) ///
        || scatter pred_dead365_dis1 lag_mu_dis1, color(orange) msymbol(circle) ///
        || lfit pred_dead365_dis1 lag_mu_dis1, color(orange) lwidth(0.5) ///
        xtitle(`xtitle') ///
        ytitle("Predicted Mortality") legend(`legend') ///
        title("B. Balance", color(black) pos(12) ) ylabel(0 (.02) .06)
    graph export `"${out}/pcp/figure4_phys_exit_panB_`pct'pct_oldMu.png"', width(800) height(600) replace


    *PANEL C
    scatter resid_dis0 lag_mu_dis0, color(blue) msymbol(circle) ///
        || lfit resid_dis0 lag_mu_dis0, color(blue) lwidth(0.5) ///
        || scatter resid_dis1 lag_mu_dis1, color(orange) msymbol(circle) ///
        || lfit resid_dis1 lag_mu_dis1, color(orange) lwidth(0.5) ///
        xtitle(`xtitle') ///
        ytitle("Predicted Forecast Residual") legend(`legend') ///
        title("C. Fallback", color(black) pos(12) ) ylabel(-.04 (.02) .02)
    graph export `"${out}/pcp/figure4_phys_exit_panC_`pct'pct_oldMu.png"', width(800) height(600) replace


    scatter dead365_dis0 lag_mu_dis0, color(blue) msymbol(circle) ///
        || lfit dead365_dis0 lag_mu_dis0, color(blue) lwidth(0.5) ///
        || scatter dead365_dis1 lag_mu_dis1, color(orange) msymbol(circle) ///
        || lfit dead365_dis1 lag_mu_dis1, color(orange) lwidth(0.5) ///
        xtitle(`xtitle') ///
        ytitle("One-Year Mortality") legend(`legend') ///
        title("D. Reduced Form", color(black) pos(12) ) ylabel(-.02 (.02) .06)
    graph export `"${out}/pcp/figure4_phys_exit_panD_`pct'pct_oldMu.png"', width(800) height(600) replace



end


