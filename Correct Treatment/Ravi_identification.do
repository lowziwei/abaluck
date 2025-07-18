*Coding for correct treatment 

*Create unique list of assessments for MD initial review
*preliminaries
if c(username) == "ninalow" {
    global path "/Users/ninalow/Desktop/Abaluck/GPT4Health"
    global data "$path/raw_data" 
	global output "$path/output"
}
cd "$data"
use "prescription_long.dta", clear

*Filter for only CHEW notes
keep if soap_sequence <=2 

*To reshape patient data from multiple rows to one row per patient with concatenated clinical indications and prescription medications

gen presc_medication_combined = presc_medication
replace presc_medication_combined = presc_medication + " CONDITION " + presc_test_condition if !missing(presc_test_condition) & presc_test_condition != ""

*Collapse the data by patient, concatenating the values with commas
sort patient_id
duplicates drop patient_id clinical_indication, force

* Use bysort to create concatenated strings manually
bysort patient_id: gen clinical_indication_concat = clinical_indication[1]
bysort patient_id: gen presc_medication_concat = presc_medication_combined[1]

* Loop through observations to build concatenated strings
bysort patient_id: replace clinical_indication_concat = clinical_indication_concat + ", " + clinical_indication if _n > 1
bysort patient_id: replace presc_medication_concat = presc_medication_concat + ", " + presc_medication_combined if _n > 1

* Keep only the last observation for each patient (which has the complete concatenated string)
bysort patient_id: keep if _n == _N

sort patient_id

keep patient_id assessment_id encounter_id soap_sequence clinical_indication_concat presc_medication_concat
export excel "identify_correct_treat.xlsx", replace firstrow(variables)


