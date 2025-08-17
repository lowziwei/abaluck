//Find fractions to clarify on screening
* initialize Stata
clear all
set more off
version 18.0
set max_memory 32g
set seed 20250108

global dropboxSCTO "/Users/ninalow/Library/CloudStorage/Dropbox/08_ChatGPT/07_Survey Data/Round 3"
global personaldropbox "/Users/ninalow/Library/CloudStorage/Dropbox"
global startdate "clock("30jan2025", "DMY")"

cd "$dropboxSCTO"

use "$dropboxSCTO/01_Data/Merged data/tests_patient_flow_long_wmissingSCTO_NL_Aug13.dta.dta", clear

forvalues i = 1/3 {
    preserve
    keep assessment_id_num  
    duplicates drop
    gen soap_sequence = 1.5
    gen test_merge_num = `i'
    tempfile new_rows_`i'
    save `new_rows_`i''
    restore
    
    append using `new_rows_`i''
}

sort assessment_id_num test_merge_num soap_sequence

*--- Merge LLM added test xlsx----
preserve
import excel "$personaldropbox/LLM_unncessary_test.xlsx", sheet("gpt_added") firstrow clear

* Get ID variables (everything except the Ravi_check columns)
ds
local allvars `r(varlist)'
local lastvars "Ravi_check_malaria Ravi_check_pcv Ravi_check_uti"
local idvars : list allvars - lastvars

expand 3  

bysort `idvars': gen test_merge_num = _n

gen test_name = ""
replace test_name = "Malaria" if test_merge_num == 1 & Ravi_check_malaria == 1
replace test_name = "PCV" if test_merge_num == 2 & Ravi_check_pcv == 1  
replace test_name = "UTI" if test_merge_num == 3 & Ravi_check_uti == 1

replace test_name = "" if test_name == ""
keep assessment_id_num test_merge_num test_name soap_sequence Notes

tempfile excel_data
save `excel_data'

restore

drop _merge
merge m:1 assessment_id_num test_merge_num soap_sequence test_name using `excel_data'
sort assessment_id_num test_merge_num soap_sequence

bysort assessment_id_num test_merge_num: egen test_conclusion_clean = mode(test_conclusion), maxmode
bysort assessment_id_num test_merge_num: egen test_conclusion_ever_clean = mode(test_conclusion_ever), maxmode
bysort assessment_id_num test_merge_num: egen any_requested = max(soap_sequence != 6 & !missing(test_name))
bysort assessment_id_num test_merge_num: egen any_RCHEW_requested = max(!missing(test_name))
bysort assessment_id_num test_merge_num: egen unassisted_requested = max(soap_sequence == 1 & !missing(test_name))
bysort assessment_id_num test_merge_num: egen assisted_requested = max(soap_sequence == 2 & !missing(test_name))
bysort assessment_id_num test_merge_num: egen screening_requested = max(soap_sequence == 6 & !missing(test_name))

sort assessment_id_num test_merge_num soap_sequence

collapse (first) test_conclusion_clean test_conclusion_ever_clean any_requested any_RCHEW_requested unassisted_requested assisted_requested screening_requested age_eligible symp_eligible, by(assessment_id_num test_merge_num)

local test_names "mal pcv uri"
local test_nums "1 2 3"

*-------------------------------------------------------------------------------------------------------------------
*A) Total counts (what I think Lisa has already done)
*-------------------------------------------------------------------------------------------------------------------

*i) Age eligibility counts
local i = 1
foreach name in `test_names' {
    quietly count if test_merge_num == `i' & age_eligible == 0
    local age_ineligible_`name'_count = r(N)
    di "`name' age ineligible: `age_ineligible_`name'_count'"
    
    quietly count if test_merge_num == `i' & age_eligible == 1
    local age_eligible_`name'_count = r(N)
    di "`name' age eligible: `age_eligible_`name'_count'"
    local i = `i' + 1
}

*ii) Symptom eligibility counts
local i = 1
foreach name in `test_names' {
    quietly count if test_merge_num == `i' & symp_eligible == 0
    local symp_ineligible_`name'_count = r(N)
    di "`name' symptom ineligible: `symp_ineligible_`name'_count'"
    
    quietly count if test_merge_num == `i' & symp_eligible == 1
    local symp_eligible_`name'_count = r(N)
    di "`name' symptom eligible: `symp_eligible_`name'_count'"
    local i = `i' + 1
}

*iii) Eligible for testing given age and symptoms
local i = 1
foreach name in `test_names' {
    quietly count if test_merge_num == `i' & age_eligible == 1 & symp_eligible == 1
    local both_eligible_`name'_count = r(N)
    di "`name' age+symptom eligible: `both_eligible_`name'_count'"
    local i = `i' + 1
}

*iv) Eligible for testing given age and symptoms and we have non-missing results
gen nonmissing_result = 0
replace nonmissing_result = 1 if test_conclusion_clean == "Normal" | test_conclusion_clean == "Abnormal"

local i = 1
foreach name in `test_names' {
    quietly count if test_merge_num == `i' & age_eligible == 1 & symp_eligible == 1 & nonmissing_result == 1
    local eligible_nonmiss_`name'_count = r(N)
    di "`name' eligible with nonmissing: `eligible_nonmiss_`name'_count'"
    local i = `i' + 1
}

*-------------------------------------------------------------------------------------------------------------------
* Create and export Table 1
*-------------------------------------------------------------------------------------------------------------------

* Create matrix for Table 1
matrix table1 = J(6, 4, .)
matrix rownames table1 = "Age_Inelig" "Age_Elig" "Symp_Inelig" "Symp_Elig" "Both_Elig" "Both_Elig_NonMiss" 
matrix colnames table1 = "Malaria" "Anemia" "UTI" "Total"

* Row 1: Age Ineligible
matrix table1[1,1] = `age_ineligible_mal_count'
matrix table1[1,2] = `age_ineligible_pcv_count'
matrix table1[1,3] = `age_ineligible_uri_count'
matrix table1[1,4] = `age_ineligible_mal_count' + `age_ineligible_pcv_count' + `age_ineligible_uri_count'

* Row 2: Age Eligible  
matrix table1[2,1] = `age_eligible_mal_count'
matrix table1[2,2] = `age_eligible_pcv_count'
matrix table1[2,3] = `age_eligible_uri_count'
matrix table1[2,4] = `age_eligible_mal_count' + `age_eligible_pcv_count' + `age_eligible_uri_count'

* Row 3: Symptom Ineligible
matrix table1[3,1] = `symp_ineligible_mal_count'
matrix table1[3,2] = `symp_ineligible_pcv_count'
matrix table1[3,3] = `symp_ineligible_uri_count'
matrix table1[3,4] = `symp_ineligible_mal_count' + `symp_ineligible_pcv_count' + `symp_ineligible_uri_count'

* Row 4: Symptom Eligible
matrix table1[4,1] = `symp_eligible_mal_count'
matrix table1[4,2] = `symp_eligible_pcv_count'
matrix table1[4,3] = `symp_eligible_uri_count'
matrix table1[4,4] = `symp_eligible_mal_count' + `symp_eligible_pcv_count' + `symp_eligible_uri_count'

* Row 5: Age + Symptom Eligible
matrix table1[5,1] = `both_eligible_mal_count'
matrix table1[5,2] = `both_eligible_pcv_count'
matrix table1[5,3] = `both_eligible_uri_count'
matrix table1[5,4] = `both_eligible_mal_count' + `both_eligible_pcv_count' + `both_eligible_uri_count'

* Row 6: Age + Symptom Eligible with Nonmissing Results
matrix table1[6,1] = `eligible_nonmiss_mal_count'
matrix table1[6,2] = `eligible_nonmiss_pcv_count'
matrix table1[6,3] = `eligible_nonmiss_uri_count'
matrix table1[6,4] = `eligible_nonmiss_mal_count' + `eligible_nonmiss_pcv_count' + `eligible_nonmiss_uri_count'

* Display and export Table 1
matrix list table1
putexcel set "Table1_Eligible_Patients.xlsx", replace
putexcel A1 = matrix(table1), names
di "Table 1 exported to: Table1_Eligible_Patients.xlsx"

*-------------------------------------------------------------------------------------------------------------------
*B.1) GPT/CHEW/MO Requests (excluding research team screening)
*-------------------------------------------------------------------------------------------------------------------

* Calculate denominators
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & any_requested == 1
    local any_req_`name'_count = r(N)
    di "GPT/CHEW/MO `name' requests: `any_req_`name'_count'"
    local i = `i' + 1
}

* Calculate counts by eligibility
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & any_requested == 1 & age_eligible == 0
    local age_inelig_req_`name'_count = r(N)
    
    qui count if test_merge_num == `i' & any_requested == 1 & symp_eligible == 0
    local symp_inelig_req_`name'_count = r(N)
    
    qui count if test_merge_num == `i' & any_requested == 1 & age_eligible == 1 & symp_eligible == 1
    local both_elig_req_`name'_count = r(N)
    
    qui count if test_merge_num == `i' & any_requested == 1 & age_eligible == 1 & symp_eligible == 1 & nonmissing_result == 1
    local elig_nonmiss_req_`name'_count = r(N)
    
    local i = `i' + 1
}

* Create and export Table 2.1
matrix table2_1 = J(6, 4, .)
matrix rownames table2_1 = "Age_Inelig" "Age_Elig" "Symp_Inelig" "Symp_Elig" "Both_Elig" "Both_Elig_NonMiss"
matrix colnames table2_1 = "Malaria" "Anemia" "UTI" "Total"

* Calculate totals
local total_any_req = `any_req_mal_count' + `any_req_pcv_count' + `any_req_uri_count'
local total_age_inelig_req = `age_inelig_req_mal_count' + `age_inelig_req_pcv_count' + `age_inelig_req_uri_count'
local total_symp_inelig_req = `symp_inelig_req_mal_count' + `symp_inelig_req_pcv_count' + `symp_inelig_req_uri_count'
local total_both_elig_req = `both_elig_req_mal_count' + `both_elig_req_pcv_count' + `both_elig_req_uri_count'
local total_elig_nonmiss_req = `elig_nonmiss_req_mal_count' + `elig_nonmiss_req_pcv_count' + `elig_nonmiss_req_uri_count'

* Fill matrix
matrix table2_1[1,1] = `age_inelig_req_mal_count'
matrix table2_1[1,2] = `age_inelig_req_pcv_count'
matrix table2_1[1,3] = `age_inelig_req_uri_count'
matrix table2_1[1,4] = `total_age_inelig_req'

matrix table2_1[2,1] = (`any_req_mal_count' - `age_inelig_req_mal_count')
matrix table2_1[2,2] = (`any_req_pcv_count' - `age_inelig_req_pcv_count')
matrix table2_1[2,3] = (`any_req_uri_count' - `age_inelig_req_uri_count')
matrix table2_1[2,4] = (`total_any_req' - `total_age_inelig_req')

matrix table2_1[3,1] = `symp_inelig_req_mal_count'
matrix table2_1[3,2] = `symp_inelig_req_pcv_count'
matrix table2_1[3,3] = `symp_inelig_req_uri_count'
matrix table2_1[3,4] = `total_symp_inelig_req'

matrix table2_1[4,1] = (`any_req_mal_count' - `symp_inelig_req_mal_count')
matrix table2_1[4,2] = (`any_req_pcv_count' - `symp_inelig_req_pcv_count')
matrix table2_1[4,3] = (`any_req_uri_count' - `symp_inelig_req_uri_count')
matrix table2_1[4,4] = (`total_any_req' - `total_symp_inelig_req')

matrix table2_1[5,1] = `both_elig_req_mal_count'
matrix table2_1[5,2] = `both_elig_req_pcv_count'
matrix table2_1[5,3] = `both_elig_req_uri_count'
matrix table2_1[5,4] = `total_both_elig_req'

matrix table2_1[6,1] = `elig_nonmiss_req_mal_count'
matrix table2_1[6,2] = `elig_nonmiss_req_pcv_count'
matrix table2_1[6,3] = `elig_nonmiss_req_uri_count'
matrix table2_1[6,4] = `total_elig_nonmiss_req'

* Display and export Table 2.1
matrix list table2_1
putexcel set "Table2_1_GPT_CHEW_MO_Requests.xlsx", replace
putexcel A1 = matrix(table2_1), names
di "Table 2.1 exported to: Table2_1_GPT_CHEW_MO_Requests.xlsx"

*-------------------------------------------------------------------------------------------------------------------
*B.2) All Requests (including research team screening)
*-------------------------------------------------------------------------------------------------------------------

* Calculate denominators
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & any_RCHEW_requested == 1
    local any_RC_req_`name'_count = r(N)
    di "All `name' requests (including research team): `any_RC_req_`name'_count'"
    local i = `i' + 1
}

* Calculate counts by eligibility
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & any_RCHEW_requested == 1 & age_eligible == 0
    local age_inelig_RC_req_`name'_count = r(N)
    
    qui count if test_merge_num == `i' & any_RCHEW_requested == 1 & symp_eligible == 0
    local symp_inelig_RC_req_`name'_count = r(N)
    
    qui count if test_merge_num == `i' & any_RCHEW_requested == 1 & age_eligible == 1 & symp_eligible == 1
    local both_elig_RC_req_`name'_count = r(N)
    
    qui count if test_merge_num == `i' & any_RCHEW_requested == 1 & age_eligible == 1 & symp_eligible == 1 & nonmissing_result == 1
    local elig_nonmiss_RC_req_`name'_count = r(N)
    
    local i = `i' + 1
}

* Create and export Table 2.2
matrix table2_2 = J(6, 4, .)
matrix rownames table2_2 = "Age_Inelig" "Age_Elig" "Symp_Inelig" "Symp_Elig" "Both_Elig" "Both_Elig_NonMiss"
matrix colnames table2_2 = "Malaria" "Anemia" "UTI" "Total"

* Calculate totals
local total_any_RC_req = `any_RC_req_mal_count' + `any_RC_req_pcv_count' + `any_RC_req_uri_count'
local total_age_inelig_RC_req = `age_inelig_RC_req_mal_count' + `age_inelig_RC_req_pcv_count' + `age_inelig_RC_req_uri_count'
local total_symp_inelig_RC_req = `symp_inelig_RC_req_mal_count' + `symp_inelig_RC_req_pcv_count' + `symp_inelig_RC_req_uri_count'
local total_both_elig_RC_req = `both_elig_RC_req_mal_count' + `both_elig_RC_req_pcv_count' + `both_elig_RC_req_uri_count'
local total_elig_nonmiss_RC_req = `elig_nonmiss_RC_req_mal_count' + `elig_nonmiss_RC_req_pcv_count' + `elig_nonmiss_RC_req_uri_count'

* Fill matrix
matrix table2_2[1,1] = `age_inelig_RC_req_mal_count'
matrix table2_2[1,2] = `age_inelig_RC_req_pcv_count'
matrix table2_2[1,3] = `age_inelig_RC_req_uri_count'
matrix table2_2[1,4] = `total_age_inelig_RC_req'

matrix table2_2[2,1] = (`any_RC_req_mal_count' - `age_inelig_RC_req_mal_count')
matrix table2_2[2,2] = (`any_RC_req_pcv_count' - `age_inelig_RC_req_pcv_count')
matrix table2_2[2,3] = (`any_RC_req_uri_count' - `age_inelig_RC_req_uri_count')
matrix table2_2[2,4] = (`total_any_RC_req' - `total_age_inelig_RC_req')

matrix table2_2[3,1] = `symp_inelig_RC_req_mal_count'
matrix table2_2[3,2] = `symp_inelig_RC_req_pcv_count'
matrix table2_2[3,3] = `symp_inelig_RC_req_uri_count'
matrix table2_2[3,4] = `total_symp_inelig_RC_req'

matrix table2_2[4,1] = (`any_RC_req_mal_count' - `symp_inelig_RC_req_mal_count')
matrix table2_2[4,2] = (`any_RC_req_pcv_count' - `symp_inelig_RC_req_pcv_count')
matrix table2_2[4,3] = (`any_RC_req_uri_count' - `symp_inelig_RC_req_uri_count')
matrix table2_2[4,4] = (`total_any_RC_req' - `total_symp_inelig_RC_req')

matrix table2_2[5,1] = `both_elig_RC_req_mal_count'
matrix table2_2[5,2] = `both_elig_RC_req_pcv_count'
matrix table2_2[5,3] = `both_elig_RC_req_uri_count'
matrix table2_2[5,4] = `total_both_elig_RC_req'

matrix table2_2[6,1] = `elig_nonmiss_RC_req_mal_count'
matrix table2_2[6,2] = `elig_nonmiss_RC_req_pcv_count'
matrix table2_2[6,3] = `elig_nonmiss_RC_req_uri_count'
matrix table2_2[6,4] = `total_elig_nonmiss_RC_req'

* Display and export Table 2.2
matrix list table2_2
putexcel set "Table2_2_All_Requests_Including_Research_Team.xlsx", replace
putexcel A1 = matrix(table2_2), names
di "Table 2.2 exported to: Table2_2_All_Requests_Including_Research_Team.xlsx"

*-------------------------------------------------------------------------------------------------------------------
*C) Of all tests asked for by a CHEW in the unassisted note and are X, of what fraction did we do a screening test?
*-------------------------------------------------------------------------------------------------------------------

*i) X = Ineligible for testing given age
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & unassisted_requested == 1 & age_eligible == 0
    local unassist_age_inelig_den_`name' = r(N)
    qui count if test_merge_num == `i' & unassisted_requested == 1 & age_eligible == 0 & screening_requested == 1
    local unassist_age_inelig_num_`name' = r(N)
    if `unassist_age_inelig_den_`name'' > 0 {
        local unassist_age_inelig_prop_`name' = `unassist_age_inelig_num_`name'' / `unassist_age_inelig_den_`name''
    }
    else {
        local unassist_age_inelig_prop_`name' = .
    }
    di "fraction of `name' unassisted requests (age ineligible) that got screening:" %9.5f `unassist_age_inelig_prop_`name''
    local i = `i' + 1
}

*ii) X = Ineligible for testing given symptoms
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & unassisted_requested == 1 & symp_eligible == 0
    local unassist_symp_inelig_den_`name' = r(N)
    qui count if test_merge_num == `i' & unassisted_requested == 1 & symp_eligible == 0 & screening_requested == 1
    local unassist_symp_inelig_num_`name' = r(N)
    if `unassist_symp_inelig_den_`name'' > 0 {
        local unassist_symp_inelig_prop_`name' = `unassist_symp_inelig_num_`name'' / `unassist_symp_inelig_den_`name''
    }
    else {
        local unassist_symp_inelig_prop_`name' = .
    }
    di "fraction of `name' unassisted requests (symp ineligible) that got screening:" %9.5f `unassist_symp_inelig_prop_`name''
    local i = `i' + 1
}

*iii) X = Eligible for testing given age and symptoms
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & unassisted_requested == 1 & age_eligible == 1 & symp_eligible == 1 
    local unassist_elig_den_`name' = r(N)
    qui count if test_merge_num == `i' & unassisted_requested == 1 & age_eligible == 1 & symp_eligible == 1 & screening_requested == 1
    local unassist_elig_num_`name' = r(N)
    if `unassist_elig_den_`name'' > 0 {
        local unassist_elig_prop_`name' = `unassist_elig_num_`name'' / `unassist_elig_den_`name''
    }
    else {
        local unassist_elig_prop_`name' = .
    }
    di "fraction of `name' unassisted requests (eligible) that got screening:" %9.5f `unassist_elig_prop_`name''
    local i = `i' + 1
}

*iv) X = Eligible for testing given age and symptoms and we have non-missing results
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & unassisted_requested == 1 & age_eligible == 1 & symp_eligible == 1 & nonmissing_result == 1
    local unassist_elig_nonmiss_den_`name' = r(N)
    qui count if test_merge_num == `i' & unassisted_requested == 1 & age_eligible == 1 & symp_eligible == 1 & screening_requested == 1 & nonmissing_result == 1
    local unassist_elig_nonmiss_num_`name' = r(N)
    if `unassist_elig_nonmiss_den_`name'' > 0 {
        local unassist_elig_nonmiss_prop_`name' = `unassist_elig_nonmiss_num_`name'' / `unassist_elig_nonmiss_den_`name''
    }
    else {
        local unassist_elig_nonmiss_prop_`name' = .
    }
    di "fraction of `name' unassisted requests (eligible+nonmissing) that got screening:" %9.5f `unassist_elig_nonmiss_prop_`name''
    local i = `i' + 1
}

*-------------------------------------------------------------------------------------------------------------------
*D) Of all tests asked for by a CHEW in the assisted note and are X, of what fraction did we do a screening test?
*-------------------------------------------------------------------------------------------------------------------

*i) X = Ineligible for testing given age
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & assisted_requested == 1 & age_eligible == 0
    local assist_age_inelig_den_`name' = r(N)
    qui count if test_merge_num == `i' & assisted_requested == 1 & age_eligible == 0 & screening_requested == 1
    local assist_age_inelig_num_`name' = r(N)
    if `assist_age_inelig_den_`name'' > 0 {
        local assist_age_inelig_prop_`name' = `assist_age_inelig_num_`name'' / `assist_age_inelig_den_`name''
    }
    else {
        local assist_age_inelig_prop_`name' = .
    }
    di "fraction of `name' assisted requests (age ineligible) that got screening:" %9.5f `assist_age_inelig_prop_`name''
    local i = `i' + 1
}

*ii) X = Ineligible for testing given symptoms
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & assisted_requested == 1 & symp_eligible == 0
    local assist_symp_inelig_den_`name' = r(N)
    qui count if test_merge_num == `i' & assisted_requested == 1 & symp_eligible == 0 & screening_requested == 1
    local assist_symp_inelig_num_`name' = r(N)
    if `assist_symp_inelig_den_`name'' > 0 {
        local assist_symp_inelig_prop_`name' = `assist_symp_inelig_num_`name'' / `assist_symp_inelig_den_`name''
    }
    else {
        local assist_symp_inelig_prop_`name' = .
    }
    di "fraction of `name' assisted requests (symp ineligible) that got screening:" %9.5f `assist_symp_inelig_prop_`name''
    local i = `i' + 1
}

*iii) X = Eligible for testing given age and symptoms
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & assisted_requested == 1 & age_eligible == 1 & symp_eligible == 1 
    local assist_elig_den_`name' = r(N)
    qui count if test_merge_num == `i' & assisted_requested == 1 & age_eligible == 1 & symp_eligible == 1 & screening_requested == 1
    local assist_elig_num_`name' = r(N)
    if `assist_elig_den_`name'' > 0 {
        local assist_elig_prop_`name' = `assist_elig_num_`name'' / `assist_elig_den_`name''
    }
    else {
        local assist_elig_prop_`name' = .
    }
    di "fraction of `name' assisted requests (eligible) that got screening:" %9.5f `assist_elig_prop_`name''
    local i = `i' + 1
}

*iv) X = Eligible for testing given age and symptoms and we have non-missing results
local i = 1
foreach name in `test_names' {
    qui count if test_merge_num == `i' & assisted_requested == 1 & age_eligible == 1 & symp_eligible == 1 & nonmissing_result == 1
    local assist_elig_nonmiss_den_`name' = r(N)
    qui count if test_merge_num == `i' & assisted_requested == 1 & age_eligible == 1 & symp_eligible == 1 & screening_requested == 1 & nonmissing_result == 1
    local assist_elig_nonmiss_num_`name' = r(N)
    if `assist_elig_nonmiss_den_`name'' > 0 {
        local assist_elig_nonmiss_prop_`name' = `assist_elig_nonmiss_num_`name'' / `assist_elig_nonmiss_den_`name''
    }
    else {
        local assist_elig_nonmiss_prop_`name' = .
    }
    di "fraction of `name' assisted requests (eligible+nonmissing) that got screening:" %9.5f `assist_elig_nonmiss_prop_`name''
    local i = `i' + 1
}

*----------------------------------------------------------------------------------------------------------
* Create comparative tables for each condition
*----------------------------------------------------------------------------------------------------------

* Table 3A: Malaria - Screening conditional on CHEW Requests
matrix table3a_malaria = J(4, 4, .)
matrix rownames table3a_malaria = "Age_Inelig" "Symp_Inelig" "Both_Elig" "Both_Elig_NonMiss" 
matrix colnames table3a_malaria = "Unassist_Req" "Unassist_Pct" "Assist_Req" "Assist_Pct"

* Fill Malaria table
* Row 1: Age Ineligible
matrix table3a_malaria[1,1] = `unassist_age_inelig_den_mal'
matrix table3a_malaria[1,2] = `unassist_age_inelig_prop_mal' * 100
matrix table3a_malaria[1,3] = `assist_age_inelig_den_mal'
matrix table3a_malaria[1,4] = `assist_age_inelig_prop_mal' * 100

* Row 2: Symptom Ineligible
matrix table3a_malaria[2,1] = `unassist_symp_inelig_den_mal'
matrix table3a_malaria[2,2] = `unassist_symp_inelig_prop_mal' * 100
matrix table3a_malaria[2,3] = `assist_symp_inelig_den_mal'
matrix table3a_malaria[2,4] = `assist_symp_inelig_prop_mal' * 100

* Row 3: Both Eligible
matrix table3a_malaria[3,1] = `unassist_elig_den_mal'
matrix table3a_malaria[3,2] = `unassist_elig_prop_mal' * 100
matrix table3a_malaria[3,3] = `assist_elig_den_mal'
matrix table3a_malaria[3,4] = `assist_elig_prop_mal' * 100

* Row 4: Both Eligible with Nonmissing Results
matrix table3a_malaria[4,1] = `unassist_elig_nonmiss_den_mal'
matrix table3a_malaria[4,2] = `unassist_elig_nonmiss_prop_mal' * 100
matrix table3a_malaria[4,3] = `assist_elig_nonmiss_den_mal'
matrix table3a_malaria[4,4] = `assist_elig_nonmiss_prop_mal' * 100

* Table 3B: PCV - Screening conditional on CHEW Requests
matrix table3b_pcv = J(4, 4, .)
matrix rownames table3b_pcv = "Age_Inelig" "Symp_Inelig" "Both_Elig" "Both_Elig_NonMiss"
matrix colnames table3b_pcv = "Unassist_Req" "Unassist_Pct" "Assist_Req" "Assist_Pct"

* Fill PCV table
matrix table3b_pcv[1,1] = `unassist_age_inelig_den_pcv'
matrix table3b_pcv[1,2] = `unassist_age_inelig_prop_pcv' * 100
matrix table3b_pcv[1,3] = `assist_age_inelig_den_pcv'
matrix table3b_pcv[1,4] = `assist_age_inelig_prop_pcv' * 100

matrix table3b_pcv[2,1] = `unassist_symp_inelig_den_pcv'
matrix table3b_pcv[2,2] = `unassist_symp_inelig_prop_pcv' * 100
matrix table3b_pcv[2,3] = `assist_symp_inelig_den_pcv'
matrix table3b_pcv[2,4] = `assist_symp_inelig_prop_pcv' * 100

matrix table3b_pcv[3,1] = `unassist_elig_den_pcv'
matrix table3b_pcv[3,2] = `unassist_elig_prop_pcv' * 100
matrix table3b_pcv[3,3] = `assist_elig_den_pcv'
matrix table3b_pcv[3,4] = `assist_elig_prop_pcv' * 100

matrix table3b_pcv[4,1] = `unassist_elig_nonmiss_den_pcv'
matrix table3b_pcv[4,2] = `unassist_elig_nonmiss_prop_pcv' * 100
matrix table3b_pcv[4,3] = `assist_elig_nonmiss_den_pcv'
matrix table3b_pcv[4,4] = `assist_elig_nonmiss_prop_pcv' * 100


* Table 3C: UTI - Screening conditional on CHEW Requests
matrix table3c_uti = J(4, 4, .)
matrix rownames table3c_uti = "Age_Inelig" "Symp_Inelig" "Both_Elig" "Both_Elig_NonMiss"
matrix colnames table3c_uti = "Unassist_Req" "Unassist_Pct" "Assist_Req" "Assist_Pct"

* Fill UTI table
matrix table3c_uti[1,1] = `unassist_age_inelig_den_uri'
matrix table3c_uti[1,2] = `unassist_age_inelig_prop_uri' * 100
matrix table3c_uti[1,3] = `assist_age_inelig_den_uri'
matrix table3c_uti[1,4] = `assist_age_inelig_prop_uri' * 100

matrix table3c_uti[2,1] = `unassist_symp_inelig_den_uri'
matrix table3c_uti[2,2] = `unassist_symp_inelig_prop_uri' * 100
matrix table3c_uti[2,3] = `assist_symp_inelig_den_uri'
matrix table3c_uti[2,4] = `assist_symp_inelig_prop_uri' * 100

matrix table3c_uti[3,1] = `unassist_elig_den_uri'
matrix table3c_uti[3,2] = `unassist_elig_prop_uri' * 100
matrix table3c_uti[3,3] = `assist_elig_den_uri'
matrix table3c_uti[3,4] = `assist_elig_prop_uri' * 100

matrix table3c_uti[4,1] = `unassist_elig_nonmiss_den_uri'
matrix table3c_uti[4,2] = `unassist_elig_nonmiss_prop_uri' * 100
matrix table3c_uti[4,3] = `assist_elig_nonmiss_den_uri'
matrix table3c_uti[4,4] = `assist_elig_nonmiss_prop_uri' * 100


* Display tables
di "=== MALARIA TABLE ==="
matrix list table3a_malaria

di "=== PCV/ANEMIA TABLE ==="
matrix list table3b_pcv

di "=== UTI TABLE ==="
matrix list table3c_uti

* Export to Excel with separate sheets
putexcel set "Table3_CHEW_Screening_Comparison.xlsx", replace

putexcel set "Table3_CHEW_Screening_Comparison.xlsx", modify sheet("Malaria")
putexcel A1 = "Screening conditional on CHEW Requests (Malaria)"
putexcel A2 = matrix(table3a_malaria), names

putexcel set "Table3_CHEW_Screening_Comparison.xlsx", modify sheet("PCV")
putexcel A1 = "Screening conditional on CHEW Requests (PCV)"
putexcel A2 = matrix(table3b_pcv), names

putexcel set "Table3_CHEW_Screening_Comparison.xlsx", modify sheet("UTI")
putexcel A1 = "Screening conditional on CHEW Requests (UTI)"
putexcel A2 = matrix(table3c_uti), names

di "All tables exported to: Table3_CHEW_Screening_Comparison.xlsx"
