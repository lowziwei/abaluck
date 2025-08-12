** Check number of observations dropped in EMR Data/Final_merged_data_long.dta due to server shut down between Apr 10 - May 5
** Retrieved  patient_flow_data data dump from Lisa for Apr 10 - Apr 24
//Append missing SCTO patient flow (Apr 10 - 25)
*preliminaries
if c(username) == "ninalow" {
    global path "/Users/ninalow/Desktop/Abaluck/GPT4Health"
    global data "$path/raw_data" 
	global output "$path/output"
}
cd "$data"

use "final_merged_data_long.dta", clear
keep if sample_rerated_25feb == 1 

bysort assessment_id: egen keep_case = max(soap_note_date >= tc(9apr2025 16:23:58) & soap_note_date <= tc(6may2025 23:59:59) & !missing(soap_note_date))
keep if keep_case == 1
drop keep_case

sort assessment_id soap_note_date
by assessment_id: keep if _n == 1

* Keep earliest assessment_id by submissiondate for each assessment_id
sort assessment_id soap_note_date
by assessment_id: keep if _n == 1


* Import comparison dataset and prepare for merge
preserve
import excel using "Patient flow module - Pilot 3_WIDE_newApr29.xlsx", firstrow clear
ren SubmissionDate submissiondate
keep submissiondate assessment_id
ren submissiondate submissiondate_miss
sort assessment_id submissiondate_miss 
by assessment_id: keep if _n == 1
gen temp_match = 1
tempfile comparison_data
save `comparison_data'
restore

order soap_note_date, last
* Merge and create match indicator
merge 1:1 assessment_id using `comparison_data'
gen match = 1 if _merge == 3
replace match =0 if match == .

* Summary
tab match, missing //22 from Apr29 data dump not in EMR or SCTO data

*Check pattern for those 22 instances
preserve
use "final_merged_data_seq_wide.dta", clear 
keep if assessment_id == "BMGF587842" | assessment_id == "BMGF590208" | assessment_id == "BMGF587912" | assessment_id == "BMGF587784" | assessment_id == "BMGF586979" | assessment_id == "BMGF588651" | assessment_id == "BMGF587675" | assessment_id == "BMGF587458" | assessment_id == "BMGF589864" | assessment_id == "BMGF587892" | assessment_id == "BMGF587438" | assessment_id == "BMGF587891" | assessment_id == "BMGF588048" | assessment_id == "BMGF588852" | assessment_id == "BMGF588079" | assessment_id == "BMGF587497" | assessment_id == "BMGF586998" | assessment_id == "BMGF588049" | assessment_id == "BMGF590172" | assessment_id == "BMGF588826" | assessment_id == "BMGF586978" | assessment_id == "BMGF590167"


sort assessment_id 
by assessment_id: keep if _n == 1
tab assessment_id 
restore

** all 22 instances have missing values for all QALY rating columns

*Keep only consenting individuals 
*Drop individuals where match !=1

ren match consent 
drop _merge
tempfile consent_data
save `consent_data'

use "final_merged_data_long.dta", clear
merge m:1 assessment_id using `consent_data', keepusing(consent)

drop if consent == 0

save "new_final_merged_data_long.dta", replace



