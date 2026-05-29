* ===========================================================================
* pcp_char_desc_regressions.do
* dead365 ~ mu_char by departure status
* ===========================================================================

set maxvar 120000, permanently
set more off
set varabbrev off
set linesize 128
set scheme burd4

qui do "00common/00preamble.do" nodep
qui do "00common/00material.do"

cap log close _all
log using `"${out}/logs/pcp_char_desc_regressions.log"', replace
set seed 10101

local char_vars has_dox_profile has_review has_spouse doc_highIncome       ///
    doc_highWealth doc_reg_voter doc_Democrat doc_Republican doc_Fellowship  ///
    Multiple_Residency Med_Private top25Med isMD doc_Hispanic doc_Black      ///
    doc_White doc_EastAsian doc_WestAsian doc_SoutheastAsian doc_SouthAsian  ///
    npi_age_bucket doc_female ever_acquired_at_time ever_payor_employed

cap program drop mean_impute
program define mean_impute
    local varlist `0'
    foreach v of local varlist {
        qui count if mi(`v')
        if r(N) > 0 {
            cap gen miss_`v' = mi(`v')
            qui sum `v'
            replace `v' = r(mean) if mi(`v')
        }
        else {
            cap gen miss_`v' = 0
        }
    }
end

* LASSO on physician-exit file
use `"${data}/physician_exit_amanda_file_hsa_period_oldMu_100pct.dta"', clear
gen npi_age_bucket = floor(npi_age / 10) * 10
mean_impute `char_vars'
drop if mi(mu_hat)

save `"${data}/amanda_file_lasso.dta"', replace

local miss_vars
foreach v of local char_vars {
    cap confirm variable miss_`v'
    if !_rc {
        local miss_vars `miss_vars' miss_`v'
    }
}

lasso linear mu_hat `char_vars' `miss_vars', selection(cv) rseed(42)
if `"`e(allvars_sel)'"' == "" {
    di "LASSO selected no variables"
    exit
}
global lasso_kept `e(allvars_sel)'

use `"${data}/pcp_departures_wc_100pct_sortOfDepartures.dta"', clear
keep npi hsa departDate
drop if mi(departDate)
gduplicates drop npi hsa, force
gen everDeparted = 1
ren (npi hsa) (lag_npi lag_hsa)
tempfile everDeparted
save `everDeparted'

use `"${data}/npi_time_period_clinics_20pct.dta"', clear
ren (npi modalClinic time_period) (lag_npi lag_clinic last_time_period)
tempfile clinicData
save `clinicData'

use `"${data}/physician_exit_microdata_hsa_period_oldMu_100pct.dta"', clear

egen lag_hsa_period  = group(lag_hsa time_period)
egen lag_zip_period  = group(lag_zip time_period)
egen lag_zip3_period = group(lag_modal_zip3 time_period)

encode lag_zip,        gen(lag_zip_encode)
encode lag_modal_zip3, gen(lag_zip3_encode)

bysort lag_hsa time_period: egen total_benes_hsa = total(nbene_hsa_lag)
gen docShare_hsa = nbene_hsa_lag / total_benes_hsa

merge m:1 lag_npi lag_hsa         using `everDeparted', gen(_m1) keep(1 3)
merge m:1 lag_npi last_time_period using `clinicData',  gen(_m2) keep(1 3)

gen npi_age_bucket     = floor(npi_age     / 10) * 10
gen lag_npi_age_bucket = floor(lag_npi_age / 10) * 10

mean_impute `char_vars'
drop if mi(mu_hat)

* Post-LASSO OLS 
reghdfe mu_hat ${lasso_kept} [aweight = total_benes], absorb(hsa) vce(cluster npi)

bysort npi (time_period): gen npi_age_baseline = npi_age[1]
tempvar npi_age_orig
gen `npi_age_orig' = npi_age
replace npi_age    = npi_age_baseline
predict mu_char, xb
replace npi_age    = `npi_age_orig'
drop npi_age_baseline

preserve
    gcollapse (mean) mu_char [aweight = total_benes], by(npi)
    ren (npi mu_char) (lag_npi lag_mu_char)
    tempfile lag_mu_char_file
    save `lag_mu_char_file'
restore
merge m:1 lag_npi using `lag_mu_char_file', keep(1 3) nogen

gen instrument_char = lag_terminate * lag_mu_char

xtile dec_lag_mu_char = lag_mu_char [aw = total_benes], nq(10)
forval d = 2/10 {
    gen dc_`d'   = dec_lag_mu_char == `d'
    gen t_dc_`d' = lag_terminate   * dc_`d'
}

gen byte is_departing = (everDeparted == 1)

local controls dc_* t_dc_* docShare_hsa lag_npi_age_bucket
local abs_full absorb(hsa lag_clinic lag_zip3_encode                   ///
                      lag_terminate#time_period                         ///
                      lag_terminate#lag_zip3_encode)

di as text _n "Weighted means of dead365 / mu_char by departure status"
di as text %22s "" "mean(dead365)   mean(mu_char)         N"

qui summ dead365 [aw = total_benes] if is_departing == 0
local mn_d0 = r(mean)
local nn0   = r(N)
qui summ mu_char [aw = total_benes] if is_departing == 0
di as text "  Non-departing  " as result %10.4f `mn_d0' "   " %10.4f r(mean) "   " %12.0fc `nn0'

qui summ dead365 [aw = total_benes] if is_departing == 1
local mn_d1 = r(mean)
local nn1   = r(N)
qui summ mu_char [aw = total_benes] if is_departing == 1
di as text "  Departing      " as result %10.4f `mn_d1' "   " %10.4f r(mean) "   " %12.0fc `nn1'

di as text _n "dead365 ~ mu_char by departure status"
di as text %28s "" "coef(mu_char)        SE              N"

* Non-departing, no controls
reghdfe dead365 mu_char [aw = total_benes] if is_departing == 0, ///
    absorb(hsa) vce(cluster npi)
di as text "Non-dep, no controls:   " ///
    as result %9.4f _b[mu_char] "  (" %7.4f _se[mu_char] ")  " %12.0fc e(N)

* Non-departing, full controls
reghdfe dead365 mu_char `controls' [aw = total_benes] if is_departing == 0, ///
    `abs_full' vce(cluster npi)
di as text "Non-dep, controls:      " ///
    as result %9.4f _b[mu_char] "  (" %7.4f _se[mu_char] ")  " %12.0fc e(N)

* Departing, no controls
reghdfe dead365 mu_char [aw = total_benes] if is_departing == 1, ///
    absorb(hsa) vce(cluster npi)
di as text "Departing, no controls: " ///
    as result %9.4f _b[mu_char] "  (" %7.4f _se[mu_char] ")  " %12.0fc e(N)

* Departing, full controls
reghdfe dead365 mu_char `controls' [aw = total_benes] if is_departing == 1, ///
    `abs_full' vce(cluster npi)
di as text "Departing, controls:    " ///
    as result %9.4f _b[mu_char] "  (" %7.4f _se[mu_char] ")  " %12.0fc e(N)

log close