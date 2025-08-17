*Coding for correct treatment 

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
sort assessment_id soap_sequence encounter_id clinical_indication

*Remove duplicates during concat
bysort encounter_id clinical_indication: gen dup_flag = _n

*Keep only the first occurrence of each clinical indication within assessment_id soap_sequence
*Set clinical_indication to blank for duplicates
replace clinical_indication = "" if dup_flag > 1

*To reshape patient data from multiple rows to one row per patient with concatenated clinical indications and prescription medications
gen clinical_ind_combined = clinical_indication
gen presc_medication_combined = presc_medication
gen presc_test_condition_combined = presc_test_condition
*If presc_medication + presc_test_conditions
*replace presc_medication_combined = presc_medication + " CONDITION " + presc_test_condition if !missing(presc_test_condition) & presc_test_condition != ""


*Collapse the data by patient, concatenating the values with commas
sort encounter_id
list encounter_id clinical_indication if encounter_id == "EN574789"

* Remove duplicate clinical indications within each patient
* duplicates drop encounter_id clinical_indication, force

* Use bysort to create concatenated strings manually
bysort encounter_id: gen clinical_indication_concat = clinical_indication[1]
bysort encounter_id: gen presc_medication_concat = presc_medication[1]
bysort encounter_id: gen presc_test_condition_concat = presc_test_condition[1]

* Loop through observations to build concatenated strings
bysort encounter_id: replace clinical_indication_concat = clinical_indication_concat[_n-1] + ", " + clinical_indication if _n > 1

bysort encounter_id: replace presc_medication_concat = presc_medication_concat[_n-1] + ", " + presc_medication if _n > 1

bysort encounter_id: replace presc_test_condition_concat = presc_test_condition_concat[_n-1] + ", " + presc_test_condition if _n > 1


* Clean up leading and trailing commas
replace clinical_indication_concat = strtrim(clinical_indication_concat)
replace clinical_indication_concat = regexr(clinical_indication_concat, "^,+ *", "")
replace clinical_indication_concat = regexr(clinical_indication_concat, " *,+$", "")
replace clinical_indication_concat = regexr(clinical_indication_concat, "^, ", "")
replace clinical_indication_concat = regexr(clinical_indication_concat, ",$", "")
replace clinical_indication_concat = regexr(clinical_indication_concat, ", , ,", ",")
replace clinical_indication_concat = regexr(clinical_indication_concat, ", ,", ",")

* Do the same for the other variables
replace presc_medication_concat = strtrim(presc_medication_concat)
replace presc_medication_concat = regexr(presc_medication_concat, "^,+ *", "")
replace presc_medication_concat = regexr(presc_medication_concat, " *,+$", "")

replace presc_test_condition_concat = strtrim(presc_test_condition_concat)
replace presc_test_condition_concat = regexr(presc_test_condition_concat, "^,+ *", "")
replace presc_test_condition_concat = regexr(presc_test_condition_concat, " *,+$", "")
replace presc_test_condition_concat = regexr(presc_test_condition_concat, "^, ", "")
replace presc_test_condition_concat = regexr(presc_test_condition_concat, ",$", "")
replace presc_test_condition_concat = regexr(presc_test_condition_concat, ", , , , ,", "")
replace presc_test_condition_concat = regexr(presc_test_condition_concat, ", , ,", ",")
replace presc_test_condition_concat = regexr(presc_test_condition_concat, ", ,", ",")

* Keep only the last observation for each patient (which has the complete concatenated string)
bysort encounter_id: keep if _n == _N

sort assessment_id
keep patient_id assessment_id encounter_id soap_sequence clinical_indication_concat presc_medication_concat presc_test_condition_concat
export excel "identify_correct_treat4.xlsx", replace firstrow(variables)
save "reshaped_patient_data.dta", replace

**For Ravi - list of unique clinical indications and clinical indication + test condition combination:
* SHEET 1: Unique clinical indications and their frequency
preserve
contract clinical_indication_concat, freq(frequency)
gsort -frequency clinical_indication_concat  // Sort by frequency descending

gen suggest_malaria = 0
gen suggest_anemia = 0
gen suggest_uti = 0

replace suggest_malaria = 1 if regexm(clinical_indication_concat, "(?i)malaria")
replace suggest_anemia = 1 if regexm(clinical_indication_concat, "(?i)anaemia")
replace suggest_uti = 1 if regexm(clinical_indication_concat, "(?i)urinary")
replace suggest_uti = 1 if regexm(clinical_indication_concat, "(?i)Dysuria")
replace suggest_uti = 1 if regexm(clinical_indication_concat, "(?i)urine")


export excel using "clinical_indications_frequency.xlsx", replace firstrow(variables)
restore

* SHEET 2: Frequency of clinical_indication_concat + presc_medication_concat combinations
preserve
contract clinical_indication_concat presc_test_condition_concat, freq(frequency)
gsort -frequency clinical_indication_concat presc_test_condition_concat  // Sort by frequency descending

gen suggest_malaria = 0
gen suggest_anemia = 0
gen suggest_uti = 0

replace suggest_malaria = 1 if regexm(clinical_indication_concat, "(?i)malaria")
replace suggest_malaria = 1 if regexm(presc_test_condition_concat, "(?i)malaria.*positive")
replace suggest_malaria = 1 if regexm(presc_test_condition_concat, "(?i)malaria.*postive")
replace suggest_malaria = 1 if regexm(presc_test_condition_concat, "(?i)malari.*positive")
replace suggest_malaria = 1 if regexm(presc_test_condition_concat, "(?i)malaria")
replace suggest_anemia = 1 if regexm(clinical_indication_concat, "(?i)anaemia")
replace suggest_anemia = 1 if regexm(presc_test_condition_concat, "(?i)p\.c\.v\..*low")
replace suggest_anemia = 1 if regexm(presc_test_condition_concat, "(?i)pcv.*low")
replace suggest_anemia = 1 if regexm(presc_test_condition_concat, "(?i)pcv.*positive")
replace suggest_anemia = 1 if regexm(presc_test_condition_concat, "(?i)pack cell volume.*low")
replace suggest_anemia = 1 if regexm(presc_test_condition_concat, "(?i)hemoglobin.*count")
replace suggest_anemia = 1 if regexm(presc_test_condition_concat, "(?i)pcv")
replace suggest_anemia = 1 if regexm(presc_test_condition_concat, "(?i)packed cell volume")
replace suggest_uti = 1 if regexm(clinical_indication_concat, "(?i)urinary")
replace suggest_uti = 1 if regexm(clinical_indication_concat, "(?i)Dysuria")
replace suggest_uti = 1 if regexm(clinical_indication_concat, "(?i)urine")
replace suggest_uti = 1 if regexm(presc_test_condition_concat, "(?i)urine analysis.*positive")
replace suggest_uti = 1 if regexm(presc_test_condition_concat, "(?i)urinary tract infection.*positive")


export excel using "clinical_test_condition_combinations.xlsx", replace firstrow(variables)
restore

***export list of clinical indications that suggest "intend to treat target conditions"


*Step 2: Find combinations of drug + dosage where intent_treat = 1
cd "$data"
import delimited "prescription_long_enhanced.csv", clear bindquote(strict)
*Filter for only CHEW notes
replace soap_sequence = "" if soap_sequence == "NA"
destring soap_sequence, replace
keep if soap_sequence <= 2

* Clean prescription_line - remove everything after ||
replace prescription_line = regexr(prescription_line, "\|\|.*", "")
replace prescription_line = strtrim(prescription_line)

sort assessment_id soap_sequence encounter_id clinical_indication
keep assessment_id soap_sequence encounter_id clinical_indication presc_medication presc_test_condition prescription_line

* Merge in demographics data - check ALL soap_sequences for anthropometry
preserve
use "tests_patient_flow_long.dta", clear
keep assessment_id soap_sequence emr_patient_gender emr_patient_age soap_note

* Find which soap_sequences have anthropometry data
gen has_height_data = regexm(soap_note, "Height[[:space:]]*\([0-9]+\.?[0-9]*[[:space:]]*cm\)") | regexm(soap_note, "Ht[[:space:]]*-[[:space:]]*[0-9]+\.?[0-9]*[[:space:]]*cm")
gen has_weight_data = regexm(soap_note, "Weight[[:space:]]*\([0-9]+\.?[0-9]*[[:space:]]*kg\)") | regexm(soap_note, "Wt[[:space:]]*-[[:space:]]*[0-9]+\.?[0-9]*[[:space:]]*kg")
gen has_bmi_data = regexm(soap_note, "BMI[[:space:]]*\([0-9]+\.?[0-9]*[[:space:]]*kg/m2\)")
gen anthro_score = has_height_data + has_weight_data + has_bmi_data

gen has_gender = (emr_patient_gender != "" & emr_patient_gender != ".")
gen has_age = !missing(emr_patient_age)  
gen has_soap = (soap_note != "" & soap_note != ".")

* Sort to prioritize records with anthropometry data first
gsort assessment_id -anthro_score -has_height_data -has_gender -has_age -has_soap soap_sequence

* Keep the first record per assessment_id (prioritizing anthropometry data)
bysort assessment_id: keep if _n == 1

* Clean up helper variables
drop has_height_data has_weight_data has_bmi_data anthro_score has_gender has_age has_soap

* Keep only what we need
keep assessment_id emr_patient_gender emr_patient_age soap_note

tempfile demographics
save `demographics'
restore

merge m:1 assessment_id using `demographics'
drop if _merge == 2
drop _merge

* Extract anthropometric data - handle both formats
gen height_cm = .
gen weight_kg = .
gen bmi = .

* Extract Height - handle both "Height (102 cm)" and "Ht - 102cm"
replace height_cm = real(regexs(1)) if regexm(soap_note, "Height[[:space:]]*\(([0-9]+\.?[0-9]*)[[:space:]]*cm\)")
replace height_cm = real(regexs(1)) if regexm(soap_note, "Ht[[:space:]]*-[[:space:]]*([0-9]+\.?[0-9]*)[[:space:]]*cm") & missing(height_cm)

* Extract Weight - handle both "Weight (13 kg)" and "Wt - 13kg"  
replace weight_kg = real(regexs(1)) if regexm(soap_note, "Weight[[:space:]]*\(([0-9]+\.?[0-9]*)[[:space:]]*kg\)")
replace weight_kg = real(regexs(1)) if regexm(soap_note, "Wt[[:space:]]*-[[:space:]]*([0-9]+\.?[0-9]*)[[:space:]]*kg") & missing(weight_kg)

* Extract BMI - standard format only
replace bmi = real(regexs(1)) if regexm(soap_note, "BMI[[:space:]]*\(([0-9]+\.?[0-9]*)[[:space:]]*kg/m2\)")

count if !missing(height_cm)
count if !missing(weight_kg)
count if !missing(bmi)

* Create intent treatment variables
cap drop intent_treat_malaria intent_treat_anemia intent_treat_uti
gen intent_treat_malaria = 0
gen intent_treat_anemia = 0
gen intent_treat_uti = 0

replace intent_treat_malaria = 1 if regexm(clinical_indication, "(?i)malaria")
replace intent_treat_malaria = 1 if regexm(presc_test_condition, "(?i)malaria.*positive")
replace intent_treat_malaria = 1 if regexm(presc_test_condition, "(?i)malaria.*postive")
replace intent_treat_malaria = 1 if regexm(presc_test_condition, "(?i)malari.*positive")
replace intent_treat_malaria = 1 if regexm(presc_test_condition, "(?i)malaria")
replace intent_treat_malaria = 1 if regexm(presc_medication, "(?i)Arthemether")
replace intent_treat_malaria = 1 if regexm(presc_medication, "(?i)Arthemeter")
replace intent_treat_malaria = 1 if regexm(presc_medication, "(?i)Artemether")
replace intent_treat_malaria = 1 if regexm(presc_medication, "(?i)Coartem")

replace intent_treat_anemia = 1 if regexm(clinical_indication, "(?i)anaemia")
replace intent_treat_anemia = 1 if regexm(presc_test_condition, "(?i)p\.c\.v\..*low")
replace intent_treat_anemia = 1 if regexm(presc_test_condition, "(?i)pcv.*low")
replace intent_treat_anemia = 1 if regexm(presc_test_condition, "(?i)pcv.*positive")
replace intent_treat_anemia = 1 if regexm(presc_test_condition, "(?i)pack cell volume.*low")
replace intent_treat_anemia = 1 if regexm(presc_test_condition, "(?i)hemoglobin.*count")
replace intent_treat_anemia = 1 if regexm(presc_test_condition, "(?i)pcv")
replace intent_treat_anemia = 1 if regexm(presc_test_condition, "(?i)packed cell volume")
replace intent_treat_anemia = 1 if regexm(presc_medication, "(?i)iron")

replace intent_treat_uti = 1 if regexm(clinical_indication, "(?i)urinary")
replace intent_treat_uti = 1 if regexm(clinical_indication, "(?i)pyelonephritis")
replace intent_treat_uti = 1 if regexm(clinical_indication, "(?i)Dysuria")
replace intent_treat_uti = 1 if regexm(clinical_indication, "(?i)urine")
replace intent_treat_uti = 1 if regexm(presc_test_condition, "(?i)urine analysis.*positive")
replace intent_treat_uti = 1 if regexm(presc_test_condition, "(?i)urinary tract infection.*positive")

sort height_cm assessment_id
rename emr_patient_age age
rename emr_patient_gender gender


* Sheet 1: Malaria treatments
preserve
keep if intent_treat_malaria == 1
gen correct_drug_malaria = .
replace correct_drug_malaria = 1 if regexm(presc_medication, "(?i)Arthemether")
replace correct_drug_malaria = 1 if regexm(presc_medication, "(?i)Arthemeter")
replace correct_drug_malaria = 1 if regexm(presc_medication, "(?i)Artemether")
replace correct_drug_malaria = 1 if regexm(presc_medication, "(?i)Coartem")
gen correct_dosage_malaria = . 

gen d_18Older = 0
replace d_18Older = 1 if age >=18

* Collapse with count and indicators
gen counter = 1
collapse (sum) n_records=counter (mean) intent_treat_malaria d_18Older correct_drug_malaria correct_dosage_malaria, by(presc_medication prescription_line)

export excel using "check_drug_dosage.xlsx", sheet("malaria") replace firstrow(variables)
restore

* Sheet 2: Anemia treatments  
preserve
keep if intent_treat_anemia == 1
gen correct_drug_anemia = .
replace correct_drug_anemia = 1 if regexm(presc_medication, "(?i)iron")
gen correct_dosage_anemia = . 

gen d_16OrOlder = 0
replace d_16OrOlder = 1 if age >=16
gen weight = 0 // weight only matters if 16 or below
replace weight = weight_kg if d_16OrOlder == 0
replace weight = . if d_16OrOlder == 1
sort presc_medication prescription_line d_16OrOlder

* Collapse with count and indicators
gen counter = 1 
collapse (sum) n_records=counter (mean) intent_treat_anemia d_16OrOlder correct_drug_anemia correct_dosage_anemia, by(presc_medication prescription_line weight)

export excel using "check_drug_dosage.xlsx", sheet("anemia") sheetmodify firstrow(variables)
restore

* Sheet 3: UTI treatments
preserve
keep if intent_treat_uti == 1
gen correct_drug_uti = .
gen correct_dosage_uti = . 

gen d_18Older = 0
replace d_18Older = 1 if age >=18

* Collapse with count and indicators
gen counter = 1
collapse (sum) n_records=counter (mean) intent_treat_uti d_18Older correct_drug_uti correct_dosage_uti, by(presc_medication prescription_line)

export excel using "check_drug_dosage.xlsx", sheet("uti") sheetmodify firstrow(variables)
restore



