set maxvar 120000
set max_memory 2000g
capture log close _all

set trace off

* Options 
set more off
set varabbrev off
set linesize 128
set scheme burd4

* Programs and globals
qui do "00common/00preamble.do" nodep
qui do "00common/00material.do"

log using `"${out}/logs/pcp_char_lasso.log"', replace

capture program drop runPCP_char_lasso
program runPCP_char_lasso
    syntax, pct(str)

    set seed 10101

    use `"${data}/physician_exit_amanda_file_hsa_period_oldMu_100pct.dta"', clear

    gen npi_age_bucket = floor(npi_age / 10) * 10

    local char_vars_full has_dox_profile has_review avg_reputation spouse_doc has_spouse doc_highIncome doc_highWealth doc_reg_voter doc_Democrat doc_Republican doc_Fellowship Multiple_Residency Top25_Residency School_StudtoFac School_MCAT has_PrimCare_Rank Top10_PrimCare Top10_Research Med_Private top25Med isMD doc_Hispanic doc_Black doc_White doc_EastAsian doc_WestAsian doc_SoutheastAsian doc_SouthAsian npi_age doc_female ever_acquired_at_time ever_payor_employed
    local char_vars has_dox_profile has_review has_spouse doc_highIncome doc_highWealth doc_reg_voter doc_Democrat doc_Republican doc_Fellowship Multiple_Residency Med_Private top25Med isMD doc_Hispanic doc_Black doc_White doc_EastAsian doc_WestAsian doc_SoutheastAsian doc_SouthAsian npi_age_bucket doc_female ever_acquired_at_time ever_payor_employed

    * generate missing indicators FIRST, then replace with mean
    local miss_vars
    foreach v of varlist `char_vars' {
        qui count if mi(`v')
        if r(N) > 0 {
            gen miss_`v' = mi(`v')
            qui sum `v'
            replace `v' = r(mean) if mi(`v')
            local miss_vars `miss_vars' miss_`v'
        }
    }

    drop if mi(mu_hat)

    save `"${data}/amanda_file_lasso.dta"', replace

    * lasso with cv on full set including missing indicators
    lasso linear mu_hat `char_vars' `miss_vars', selection(cv) rseed(42)
    local lasso_kept `e(allvars_sel)'
    lassocoef

    di "LASSO kept variables: `lasso_kept'"

    * print only variables dropped by lasso
    di "LASSO dropped variables:"
    foreach v of varlist `char_vars' `miss_vars' {
        local kept = 0
        foreach kv of local lasso_kept {
            if "`v'" == "`kv'" local kept = 1
        }
        if `kept' == 0 di "  `v'"
    }

    if "`lasso_kept'" == "" {
        di("LASSO selected no variables")
        exit
    }

    global lasso_kept_global `lasso_kept'

    * Build microdata sample

    use `"${data}/pcp_departures_wc_100pct_sortOfDepartures.dta"', clear

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

    use `"${data}/npi_time_period_clinics_20pct.dta"', clear
    ren npi lag_npi
    ren modalClinic lag_clinic
    ren time_period last_time_period
    tempfile clinicData
    save `clinicData'

    use `"${data}/physician_exit_microdata_hsa_period_oldMu_100pct.dta"', clear
    egen lag_hsa_period  = group(lag_hsa time_period)
    egen lag_zip_period  = group(lag_zip time_period)
    egen lag_zip3_period = group(lag_modal_zip3 time_period)

    di("Encode vars")
    encode lag_zip,        gen(lag_zip_encode)
    encode lag_modal_zip3, gen(lag_zip3_encode)

    di("Calculate total benes")
    bysort lag_hsa time_period: egen total_benes_hsa = total(nbene_hsa_lag)
    gen docShare_hsa = nbene_hsa_lag / total_benes_hsa

    merge m:1 lag_npi lag_hsa          using `everDepartedDocs', gen(everDepMerge) keep(1 3)
    merge m:1 lag_npi last_time_period using `clinicData',        gen(clinicMerge)  keep(1 3)

    gen npi_age_bucket     = floor(npi_age     / 10) * 10
    gen lag_npi_age_bucket = floor(lag_npi_age / 10) * 10

    * missing indicators + mean-imputation on microdata
    foreach v of varlist `char_vars' {
        qui count if mi(`v')
        if r(N) > 0 {
            capture confirm variable miss_`v'
            if _rc != 0 gen miss_`v' = mi(`v')
            qui sum `v'
            replace `v' = r(mean) if mi(`v')
        }
    }

    * ensure all lasso-selected miss_ vars exist in microdata
    foreach v of varlist `char_vars' {
        capture confirm variable miss_`v'
        if _rc != 0 gen miss_`v' = 0
    }

    drop if mi(mu_hat)

    * Post-LASSO OLS to get coefficients and construct mu_char -------------

    reghdfe mu_hat ${lasso_kept_global} [aweight = total_benes], absorb(hsa) vce(cluster npi)

    * save coefficient dataset for figures
    preserve
        local n : word count `char_vars'
        clear
        set obs `n'
        gen varname = ""
        gen b       = .
        gen se      = .
        local i = 1
        foreach v of local char_vars {
            replace varname = "`v'" in `i'
            capture local coef = _b[`v']
            if _rc == 0 {
                replace b  = _b[`v']  in `i'
                replace se = _se[`v'] in `i'
            }
            else {
                replace b  = . in `i'
                replace se = . in `i'
            }
            local ++i
        }
        cap mkdir "${out}/temp"
        save "${out}/temp/coef_post_lasso.dta", replace
    restore

    * construct baseline age to make provider characteristics timeless
    sort npi time_period
    bysort npi (time_period): gen npi_age_baseline = npi_age[1]
    tempvar npi_age_orig
    gen `npi_age_orig' = npi_age
    replace npi_age = npi_age_baseline

    * define mu_char = characteristic-predicted mu
    predict mu_char, xb
    replace npi_age = `npi_age_orig'

    * Construct lag_mu_char

    preserve
        gcollapse (mean) mu_char [aweight = total_benes], by(npi)
        ren npi     lag_npi
        ren mu_char lag_mu_char
        keep lag_npi lag_mu_char
        tempfile lag_mu_char_file
        save `lag_mu_char_file'
    restore

    merge m:1 lag_npi using `lag_mu_char_file', keep(1 3) nogen
    gen instrument_char = lag_terminate * lag_mu_char

    * Decile dummies based on lag_mu_char

    xtile dec_lag_mu_char = lag_mu_char [aw = total_benes], nq(10)
    forval d = 1/10 {
        gen dc_`d'   = dec_lag_mu_char == `d'
        gen t_dc_`d' = lag_terminate * dc_`d'
    }
    drop dc_1

    * Main regressions

    * First stage
    reghdfe mu_char instrument_char lag_terminate dc_* t_dc_* docShare_hsa lag_npi_age_bucket ///
        [aweight = total_benes], ///
        absorb(hsa lag_clinic lag_zip3_encode ///
               lag_terminate#time_period lag_terminate#lag_zip3_encode) ///
        vce(cluster npi)
    estimates store first_stage

    * Reduced form
    reghdfe dead365 instrument_char lag_terminate dc_* t_dc_* docShare_hsa lag_npi_age_bucket ///
        [aweight = total_benes], ///
        absorb(hsa lag_clinic lag_zip3_encode ///
               lag_terminate#time_period lag_terminate#lag_zip3_encode) ///
        vce(cluster npi)
    estimates store reduced_form
/*
    * IV
    ivreghdfe dead365 (mu_char = instrument_char) lag_terminate dc_* t_dc_* docShare_hsa lag_npi_age_bucket ///
        [aweight = total_benes], ///
        absorb(hsa lag_clinic lag_zip3_encode ///
               lag_terminate#time_period lag_terminate#lag_zip3_encode) ///
        vce(cluster npi)
    estimates store iv_muchar
*/

    * Figure 4 analog: mu_char figures with demeaning

    tempfile pre_figure_data
    save `pre_figure_data'
    save "${data}/physician_exit_microdata_mu_char.dta", replace

    local legend label(1 "Non-Terminated Plans") label(3 "Terminated Plans")
    local legend `legend' order(1 3) cols(1) ring(1) position(1) bmargin(small)
    local xtitle "Lagged Characteristic-Predicted Quality"

    foreach var of varlist mu_char dead365 {
        reghdfe `var' lag_terminate dc_* t_dc_* docShare_hsa lag_npi_age_bucket ///
            [aweight = total_benes], ///
            absorb(hsa lag_clinic lag_zip3_encode ///
                   lag_terminate#time_period lag_terminate#lag_zip3_encode) ///
            vce(cluster npi)

        gen `var'_dis0 = 0 if _n <= 10
        gen `var'_dis1 = 0 if _n <= 10

        forval d = 1/10 {
            summ `var' [aw = total_benes] if lag_terminate == 0 & dec_lag_mu_char == `d'
            replace `var'_dis0 = r(mean) if _n == `d'
            local tcoef = 0
            capture local tcoef = _b[t_dc_`d']
            replace `var'_dis1 = r(mean) + `tcoef' if _n == `d'
        }

        qui summ `var' [aw = total_benes]
        local `var'_mn = r(mean)
    }

    * x-axis: lag_mu_char decile means by termination status
    gen lag_mu_char_dis0 = 0 if _n <= 10
    gen lag_mu_char_dis1 = 0 if _n <= 10
    forval d = 1/10 {
        summ lag_mu_char [aw = total_benes] if lag_terminate == 0 & dec_lag_mu_char == `d'
        replace lag_mu_char_dis0 = r(mean) if _n == `d'
        summ lag_mu_char [aw = total_benes] if lag_terminate == 1 & dec_lag_mu_char == `d'
        replace lag_mu_char_dis1 = r(mean) if _n == `d'
    }
    qui summ lag_mu_char [aw = total_benes]
    local lag_mu_char_mn = r(mean)

    keep *_dis0 *_dis1
    drop if mu_char_dis0 == .
    duplicates drop

    * re-normalize all lines to share overall weighted mean (demeaning step)
    foreach var1 in mu_char dead365 lag_mu_char {
        foreach var2 in `var1'_dis0 `var1'_dis1 {
            summ `var2'
            replace `var2' = `var2' - r(mean) + ``var1'_mn'
        }
    }

    * Save plot data so figures can be reproduced without rerunning everything
    cap mkdir "${out}/temp"
    save "${out}/temp/fig4_plot_data.dta", replace

    * Panel A: first stage
    scatter mu_char_dis0 lag_mu_char_dis0, color(blue) msymbol(circle) ///
        || lfit mu_char_dis0 lag_mu_char_dis0, color(blue) lwidth(0.5) ///
        || scatter mu_char_dis1 lag_mu_char_dis1, color(orange) msymbol(circle) ///
        || lfit mu_char_dis1 lag_mu_char_dis1, color(orange) lwidth(0.5) ///
        xtitle(`xtitle') ytitle("Characteristic-Predicted Quality") ///
        legend(`legend') title("A. First Stage", color(black) pos(12))
    graph export "${out}/fig4_muchar_panA.png", width(800) height(600) replace

    * Panel D: reduced form
    scatter dead365_dis0 lag_mu_char_dis0, color(blue) msymbol(circle) ///
        || lfit dead365_dis0 lag_mu_char_dis0, color(blue) lwidth(0.5) ///
        || scatter dead365_dis1 lag_mu_char_dis1, color(orange) msymbol(circle) ///
        || lfit dead365_dis1 lag_mu_char_dis1, color(orange) lwidth(0.5) ///
        xtitle(`xtitle') ytitle("One-Year Mortality") ///
        legend(`legend') title("D. Reduced Form", color(black) pos(12))
    graph export "${out}/fig4_muchar_panD.png", width(800) height(600) replace

    * reload full data
    use `pre_figure_data', clear

end

cap program drop main
program main
    syntax, [CAPture NOIsily]
        foreach pct in 100 {
            runPCP_char_lasso, pct(`pct')
        }
end

main, cap noi
log close