set maxvar 120000
set max_memory 2000g
capture log close _all

set trace off
* Options
set more off
set linesize 128
set scheme burd4

* Programs and global
qui do 00common/00preamble.do nodep
qui do 00common/00material.do

log using "${out}/logs/pcp_char_reg.log", replace

capture program drop attachYpos
program attachYpos
    syntax, spec(str) pos_list(string) allvars(string)
    use "${out}/temp/coef_`spec'.dta", clear
    gen ypos = .
    local i = 1
    foreach var of local allvars {
        local p : word `i' of `pos_list'
        replace ypos = `p' if varname == "`var'"
        local ++i
    }
    gen lower        = b - 1.96 * se
    gen upper        = b + 1.96 * se
    gen crosses_zero = (lower < 0 & upper > 0)
end

capture program drop runPCP_char_reg
program runPCP_char_reg
    syntax, pct(str)
    set seed 10101

    cap mkdir "${out}/temp"
    foreach spec in restricted full lnexp franken iv {
        cap erase "${out}/temp/coef_`spec'.dta"
        preserve
            clear
            set obs 0
            gen str32 varname = ""
            gen double b      = .
            gen double se     = .
            save "${out}/temp/coef_`spec'.dta", replace
        restore
    }

    use `"${data}/pcp_departures_wc_`pct'pct_sortOfDepartures.dta"', clear
    keep npi hsa departDate
    drop if mi(departDate)
    gen everDeparted = 1
    keep npi hsa everDeparted
    gduplicates drop
    gisid npi hsa
    ren npi lag_npi
    ren hsa lag_hsa
    tempfile everDepartedDocs
    save `everDepartedDocs'

    use "${data}/npi_time_period_clinics_20pct.dta", clear
    ren npi lag_npi
    ren modalClinic lag_clinic
    ren time_period last_time_period
    tempfile clinicData
    save `clinicData'

    use `"${data}/physician_exit_microdata_hsa_period_oldMu_100pct.dta"', clear

    egen lag_hsa_period  = group(lag_hsa time_period)
    egen lag_zip_period  = group(lag_hsa time_period)
    egen lag_zip3_period = group(lag_modal_zip3 time_period)

    encode lag_zip,   gen(lag_zip_encode)
    encode lag_zip_3, gen(lag_zip3_encode)

    bysort lag_hsa time_period: gen total_benes_hsa = _N
    gen docShare_hsa = nbene_hsa_lag / total_benes_hsa

    merge m:1 lag_npi lag_hsa         using `everDepartedDocs', gen(everDepMerge) keep(1 3)
    merge m:1 lag_npi last_time_period using `clinicData',       gen(clinicMerge)  keep(1 3)

    xtile dec_lag_mu = lag_mu if everDeparted == 1, nq(10)
    forval d = 1/10 {
        gen d_`d'   = dec_lag_mu == `d'
        gen t_d_`d' = lag_terminate * d_`d'
    }
    drop d_1

    xtile mu_pctile = mu_hat, nq(20)
    gen top5  = (mu_pctile == 20)
    gen lnexp = log(exp_total_period + 1)
    gen npi_age_bucket     = floor(lag_npi_age / 10) * 10
    gen lag_npi_age_bucket = floor(lag_npi_age / 10) * 10  // lag_ prefix for instrument loop
    
    foreach var in ever_payor_employed ever_acquired_at_time has_dox_profile ///
        has_review avg_reputation spouse_doc has_spouse ///
        doc_highIncome doc_highWealth doc_reg_voter doc_Democrat doc_Republican ///
        doc_Fellowship Multiple_Residency Top25_Residency ///
        School_StudtoFac School_MCAT has_PrimCare_Rank Top10_PrimCare Top10_Research ///
        Med_Private top25Med isMD ///
        doc_Hispanic doc_Black doc_White doc_EastAsian doc_WestAsian ///
        doc_SoutheastAsian doc_SouthAsian npi_age_bucket doc_female {
        gen instrument_`var' = lag_terminate * lag_`var'
    }

    tempfile microdata
    save `microdata'

    * Regression loop 

    foreach var in ever_payor_employed ever_acquired_at_time has_dox_profile ///
        has_review avg_reputation spouse_doc has_spouse ///
        doc_highIncome doc_highWealth doc_reg_voter doc_Democrat doc_Republican ///
        doc_Fellowship Multiple_Residency Top25_Residency ///
        School_StudtoFac School_MCAT has_PrimCare_Rank Top10_PrimCare Top10_Research ///
        Med_Private top25Med isMD ///
        doc_Hispanic doc_Black doc_White doc_EastAsian doc_WestAsian ///
        doc_SoutheastAsian doc_SouthAsian npi_age_bucket doc_female {

        di as txt _n ">>> `var'"
/*
        * 1. Bivariate OLS — Bleemer trimmed sample
        reghdfe mu_hat `var' [aweight = total_benes] if total_benes >= 11, absorb(hsa) vce(cluster npi) keepsingleton
        local b_r  = _b[`var']
        local se_r = _se[`var']
        clear
        set obs 1
        gen str32 varname = "`var'"
        gen double b  = `b_r'
        gen double se = `se_r'
        append using "${out}/temp/coef_restricted.dta"
        save "${out}/temp/coef_restricted.dta", replace
        use `microdata'
*/
        * 2. Bivariate OLS — full sample
        reghdfe mu_hat `var' [aweight = total_benes], absorb(hsa) vce(cluster npi) keepsingleton
        local b_f  = _b[`var']
        local se_f = _se[`var']
        clear
        set obs 1
        gen str32 varname = "`var'"
        gen double b  = `b_f'
        gen double se = `se_f'
        append using "${out}/temp/coef_full.dta"
        save "${out}/temp/coef_full.dta", replace
        use `microdata'
/*
        * 3. Bivariate OLS — lnexp outcome
        reghdfe lnexp `var' [aweight = total_benes], absorb(hsa) vce(cluster npi) keepsingleton
        local b_l  = _b[`var']
        local se_l = _se[`var']
        clear
        set obs 1
        gen str32 varname = "`var'"
        gen double b  = `b_l'
        gen double se = `se_l'
        append using "${out}/temp/coef_lnexp.dta"
        save "${out}/temp/coef_lnexp.dta", replace
        use `microdata'
*/
        * 4. Franken OLS
        reghdfe mu_hat `var' d_* docShare_hsa lag_npi_age [aweight = total_benes], absorb(lag_clinic lag_zip3_encode lag_terminate#time_period lag_terminate#lag_zip3_encode) vce(cluster npi) keepsingleton
        local b_fk  = _b[`var']
        local se_fk = _se[`var']
        clear
        set obs 1
        gen str32 varname = "`var'"
        gen double b  = `b_fk'
        gen double se = `se_fk'
        append using "${out}/temp/coef_franken.dta"
        save "${out}/temp/coef_franken.dta", replace
        use `microdata'

        * 5. IV
        ivreghdfe dead365 (`var' = instrument_`var') d_* docShare_hsa lag_npi_age [aweight = total_benes], absorb(lag_clinic lag_zip3_encode lag_terminate#time_period lag_terminate#lag_zip3_encode) vce(cluster lag_hsa) keepsingleton
        local b_iv  = _b[`var']
        local se_iv = _se[`var']
        clear
        set obs 1
        gen str32 varname = "`var'"
        gen double b      = `b_iv'
        gen double se     = `se_iv'
        append using "${out}/temp/coef_iv.dta"
        save "${out}/temp/coef_iv.dta", replace
        use `microdata'
    }

    clear
    local pos_list 1 2 3 4 5 6 7 8 9 10 11 12 ///
               15 16 17 ///
               20 21 22 23 24 25 26 27 ///
               30 31 32 33 34 35 36 37 38

    local allvars ever_payor_employed ever_acquired_at_time has_dox_profile ///
        has_review avg_reputation spouse_doc has_spouse ///
        doc_highIncome doc_highWealth doc_reg_voter doc_Democrat doc_Republican ///
        doc_Fellowship Multiple_Residency Top25_Residency ///
        School_StudtoFac School_MCAT has_PrimCare_Rank Top10_PrimCare Top10_Research ///
        Med_Private top25Med isMD ///
        doc_Hispanic doc_Black doc_White doc_EastAsian doc_WestAsian ///
        doc_SoutheastAsian doc_SouthAsian npi_age_bucket doc_female

    local ylabels ylabel( ///
        39 "{bf:Race & Demographics}" ///
        38 "Female"                   ///
        37 "Age"                      ///
        36 "South Asian"              ///
        35 "Southeast Asian"          ///
        34 "West Asian"               ///
        33 "East Asian"               ///
        32 "White"                    ///
        31 "Black"                    ///
        30 "Hispanic"                 ///
        28 "{bf:Med School}"          ///
        27 "is MD"                    ///
        26 "Top-25 Med School"        ///
        25 "Private Med School"       ///
        24 "Top-10 Research"          ///
        23 "Top-10 Primcare"          ///
        22 "Has Primcare Rank"        ///
        21 "School MCAT"              ///
        20 "Student-Faculty Ratio"    ///
        18 "{bf:Training}"            ///
        17 "Top-25 Residency"         ///
        16 "Multiple Residency"       ///
        15 "Had Fellowship"           ///
        13 "{bf:Other}"               ///
        12 "Republican"               ///
        11 "Democrat"                 ///
        10 "Registered Voter"         ///
        9 "High Wealth"              ///
        8 "High Income"              ///
        7 "Has Spouse"               ///
        6 "Spouse is Doctor"         ///
        5 "Avg Online Reputation"    ///
        4 "Has Review"               ///
        3 "Has Dox Profile"          ///
        2 "Ever Acquired"            ///
        1 "Ever Employed by Payor"   ///
        , angle(0) labsize(vsmall) notick nogrid)

    local ylines yline(1 2 3 4 5 6 7 8 9 10 11 12 15 16 17 20 21 22 23 24 25 26 27 30 31 32 33 34 35 36 37 38, ///
        lwidth(vthin) lcolor(gs12) lpattern(solid))

    local gopts graphregion(color(white) margin(zero)) plotregion(color(white) margin(zero)) ///
        ysize(9) scheme(s1mono)

    * PANEL 1 — Bivariate OLS 
    attachYpos, spec(full) pos_list(`pos_list') allvars(`allvars')
    twoway ///
        (rcap lower upper ypos if crosses_zero == 1, horizontal lwidth(thin) lcolor(gs6)) ///
        (rcap lower upper ypos if crosses_zero == 0, horizontal lwidth(thin) lcolor(gs6))  ///
        (scatter ypos b if crosses_zero == 1, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)) ///
        (scatter ypos b if crosses_zero == 0, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)), ///
        `ylabels' ///
        xlabel(-0.01 0 0.01, labsize(vsmall) format(%5.3f)) ///
        xscale(range(-0.012 0.012)) ///
        yscale(range(0 40)) ///
        xline(0, lwidth(vthin) lcolor(gs8) lpattern(dash)) ///
        `ylines' ///
        title("Bivariate OLS", size(small) box fcolor(gs12) lcolor(gs8) lwidth(thin) bexpand) ///
        ytitle("") xtitle("") legend(off) ///
        `gopts' xsize(11) fxsize(45)
    graph save "${out}/temp/p_bivariate.gph", replace

    * PANEL 2 — Franken OLS
    attachYpos, spec(franken) pos_list(`pos_list') allvars(`allvars')
    twoway ///
        (rcap lower upper ypos if crosses_zero == 1, horizontal lwidth(thin) lcolor(gs6)) ///
        (rcap lower upper ypos if crosses_zero == 0, horizontal lwidth(thin) lcolor(gs6))  ///
        (scatter ypos b if crosses_zero == 1, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)) ///
        (scatter ypos b if crosses_zero == 0, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)), ///
        xlabel(-0.01 0 0.01, labsize(vsmall) format(%5.3f)) ///
        xscale(range(-0.012 0.012)) ///
        yscale(range(0 40)) ylabel(none) ytick(none) ymtick(none) ///
        xline(0, lwidth(vthin) lcolor(gs8) lpattern(dash)) ///
        `ylines' ///
        title("OLS with Controls", size(small) box fcolor(gs12) lcolor(gs8) lwidth(thin) bexpand) ///
        ytitle("") xtitle("") legend(off) ///
        `gopts' xsize(4) fxsize(27)
    graph save "${out}/temp/p_franken.gph", replace

    * PANEL 3 — IV 
    attachYpos, spec(iv) pos_list(`pos_list') allvars(`allvars')
    twoway ///
        (rcap lower upper ypos if crosses_zero == 1, horizontal lwidth(thin) lcolor(gs6)) ///
        (rcap lower upper ypos if crosses_zero == 0, horizontal lwidth(thin) lcolor(gs6))  ///
        (scatter ypos b if crosses_zero == 1, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)) ///
        (scatter ypos b if crosses_zero == 0, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)), ///
        xlabel(-0.50 0 0.50, labsize(vsmall) format(%4.2f)) ///
        xscale(range(-0.52 0.52)) ///
        yscale(range(0 40)) ylabel(none) ytick(none) ymtick(none) ///
        xline(0, lwidth(vthin) lcolor(gs8) lpattern(dash)) ///
        `ylines' ///
        title("IV", size(small) box fcolor(gs12) lcolor(gs8) lwidth(thin) bexpand) ///
        ytitle("") xtitle("") legend(off) ///
        `gopts' xsize(4) fxsize(27)
    graph save "${out}/temp/p_iv.gph", replace

    graph combine ///
        "${out}/temp/p_bivariate.gph" ///
        "${out}/temp/p_franken.gph" ///
        "${out}/temp/p_iv.gph", ///
        cols(3) imargin(0 0 0 0) ///
        graphregion(color(white)) ///
        ysize(9) xsize(10) ///
        name(combined_char_reg, replace)

    cap mkdir "${out}/figures"
    graph export "${out}/figures/pcp_char_reg_combined.png", replace


end

cap program drop main
program main
    syntax, [CAPture NOIsily]
    foreach pct in 100 {
        runPCP_char_reg, pct(`pct')
    }
end

main, cap noi

log close
