*Check number of cases which AI was added a test to unique list of cases for MD review

*preliminaries
if c(username) == "ninalow" {
    global path "/Users/ninalow/Desktop/Abaluck/GPT4Health"
    global data "$path/raw_data" 
	global output "$path/output"
	global output2 "$path/output2"
}
cd "$path"
use "final_merged_data_long.dta", clear
keep if sample_rerated_25feb == 1
cd "$output"

preserve
import delimited "MD_initial_review_assignment.csv", clear
* Keep only the assessment_id column and remove duplicates
keep assessment_id
duplicates drop
* Save as temporary file
tempfile valid_ids
save `valid_ids'
restore

merge m:1 assessment_id using `valid_ids', keep(match) nogenerate

save "filtered_final_merged_data_long.dta", replace
export delimited "unique_final_merged_data_long.csv", replace

*See whether LLM added target condition tests
keep if soap_sequence == 1

*Check AI Notes
gen ai_malaria_test = 0
replace ai_malaria_test = 1 if regexm(ai_note, "(?i)malaria") & (regexm(ai_note, "(?i)test") | regexm(ai_note, "(?i)rdt") | regexm(ai_note, "(?i)smear"))
replace ai_malaria_test = 1 if regexm(ai_note, "(?i)malaria rdt") | regexm(ai_note, "(?i)malaria rapid")
replace ai_malaria_test = 1 if regexm(ai_note, "(?i)malaria smear") | regexm(ai_note, "(?i)blood smear")
replace ai_malaria_test = 1 if regexm(ai_note, "(?i)parasite") & regexm(ai_note, "(?i)test")

gen ai_pcv_test = 0
replace ai_pcv_test = 1 if regexm(ai_note, "(?i)pcv") | regexm(ai_note, "(?i)packed cell volume")
replace ai_pcv_test = 1 if regexm(ai_note, "(?i)hemoglobin") | regexm(ai_note, "(?i)haemoglobin")
replace ai_pcv_test = 1 if regexm(ai_note, "(?i)hematocrit") | regexm(ai_note, "(?i)haematocrit")
replace ai_pcv_test = 1 if regexm(ai_note, "(?i)full blood count") | regexm(ai_note, "(?i)fbc")
replace ai_pcv_test = 1 if regexm(ai_note, "(?i)complete blood count") | regexm(ai_note, "(?i)cbc")

gen ai_uti_test = 0
replace ai_uti_test = 1 if regexm(ai_note, "(?i)urine") & (regexm(ai_note, "(?i)test") | regexm(ai_note, "(?i)analysis") | regexm(ai_note, "(?i)culture"))
replace ai_uti_test = 1 if regexm(ai_note, "(?i)urinalysis") | regexm(ai_note, "(?i)dipstick")
replace ai_uti_test = 1 if regexm(ai_note, "(?i)urine culture")

*Check Unassisted SOAP Notes
gen soap_malaria_test = 0
replace soap_malaria_test = 1 if regexm(soap_note, "(?i)malaria") & (regexm(soap_note, "(?i)test") | regexm(soap_note, "(?i)rdt") | regexm(soap_note, "(?i)smear"))
replace soap_malaria_test = 1 if regexm(soap_note, "(?i)malaria rdt") | regexm(soap_note, "(?i)malaria rapid")
replace soap_malaria_test = 1 if regexm(soap_note, "(?i)parasite") & regexm(soap_note, "(?i)test")

gen soap_pcv_test = 0
replace soap_pcv_test = 1 if regexm(soap_note, "(?i)pcv") | regexm(soap_note, "(?i)packed cell volume")
replace soap_pcv_test = 1 if regexm(soap_note, "(?i)hemoglobin") | regexm(soap_note, "(?i)haemoglobin")
replace soap_pcv_test = 1 if regexm(soap_note, "(?i)hematocrit") | regexm(soap_note, "(?i)haematocrit")
replace soap_pcv_test = 1 if regexm(soap_note, "(?i)full blood count") | regexm(soap_note, "(?i)fbc")
replace soap_pcv_test = 1 if regexm(soap_note, "(?i)complete blood count") | regexm(soap_note, "(?i)cbc")

gen soap_uti_test = 0
replace soap_uti_test = 1 if regexm(soap_note, "(?i)urine") & (regexm(soap_note, "(?i)test") | regexm(soap_note, "(?i)analysis"))
replace soap_uti_test = 1 if regexm(soap_note, "(?i)urinalysis") | regexm(soap_note, "(?i)dipstick")
replace soap_uti_test = 1 if regexm(soap_note, "(?i)urine culture")

* "AI added" = 1 if AI mentioned the test AND it wasn't in the original SOAP note
gen ai_added_malaria = (ai_malaria_test == 1 & soap_malaria_test == 0)
gen ai_added_pcv = (ai_pcv_test == 1 & soap_pcv_test == 0)
gen ai_added_uti = (ai_uti_test == 1 & soap_uti_test == 0)

count
local total_n = r(N)
count if ai_malaria_test == 1
local malaria_ai = r(N)
count if ai_added_malaria == 1  
local malaria_added = r(N)
count if ai_pcv_test == 1
local pcv_ai = r(N)
count if ai_added_pcv == 1
local pcv_added = r(N)
count if ai_uti_test == 1
local uti_ai = r(N)
count if ai_added_uti == 1
local uti_added = r(N)

preserve
clear
set obs 6
gen test_type = ""
gen category = ""
gen count = .
gen total = .
gen percentage = .

replace test_type = "Malaria" in 1
replace category = "AI Mentioned" in 1
replace count = `malaria_ai' in 1
replace total = `total_n' in 1
replace percentage = round((`malaria_ai'/`total_n')*100, 0.1) in 1

replace test_type = "Malaria" in 2
replace category = "AI Added New" in 2
replace count = `malaria_added' in 2
replace total = `total_n' in 2
replace percentage = round((`malaria_added'/`total_n')*100, 0.1) in 2

replace test_type = "PCV/Anemia" in 3
replace category = "AI Mentioned" in 3
replace count = `pcv_ai' in 3
replace total = `total_n' in 3
replace percentage = round((`pcv_ai'/`total_n')*100, 0.1) in 3

replace test_type = "PCV/Anemia" in 4
replace category = "AI Added New" in 4
replace count = `pcv_added' in 4
replace total = `total_n' in 4
replace percentage = round((`pcv_added'/`total_n')*100, 0.1) in 4

replace test_type = "UTI" in 5
replace category = "AI Mentioned" in 5
replace count = `uti_ai' in 5
replace total = `total_n' in 5
replace percentage = round((`uti_ai'/`total_n')*100, 0.1) in 5

replace test_type = "UTI" in 6
replace category = "AI Added New" in 6
replace count = `uti_added' in 6
replace total = `total_n' in 6
replace percentage = round((`uti_added'/`total_n')*100, 0.1) in 6

export excel "summary_test_frequencies.xlsx", replace firstrow(variables)

restore

keep assessment_id assessment_index soap_type ai_note soap_note ai_malaria_test ai_pcv_test ai_uti_test soap_malaria_test soap_pcv_test soap_uti_test ai_added_malaria ai_added_pcv ai_added_uti
export excel "LLM_test_analysis.xlsx", replace firstrow(variables)
