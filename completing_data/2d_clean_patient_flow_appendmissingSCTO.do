//Append missing SCTO patient flow (Apr 10 - 25)
*preliminaries
if c(username) == "ninalow" {
    global path "/Users/ninalow/Desktop/Abaluck/GPT4Health"
    global data "$path/raw_data" 
	global output "$path/output"
}
cd "$data"

*Load reference file (unique assessment_ids)
use "patient_flow_wide_complete.dta", clear

*Notice missing submissiondate between 08mar2025 and 06may2025 not inclusive 

*Use Lisa's Apr 28 SCTO Data Dump
import delimited "Patient flow module - Pilot 3_WIDE - Apr28save.csv", clear

// labels
label define yes_no 1 "Yes" 0 "No"
label define agree_disagree_5 1 "Strongly disagree" 2 "Disagree" 3 "Neither agree nor disagree" 4 "Agree" 5 "Strongly Agree"

label variable key "SurveyCTO unique submission ID"
label variable submissiondate "SurveyCTO Date/time submitted"
label variable formdef_version "SurveyCTO Form version used on device"
label var duration "Survey duration (seconds)"
	
label variable enumid "Please select your name (first and last name):"
label values enumid

label variable first_consent "Confirm the respondent has agreed to answer questions."
label values first_consent yes_no

* Consent variables
label variable first_consent "Confirm the respondent has agreed to answer questions."
label values first_consent yes_no

label variable consent_adults "Did the patient consent to participate?"
label values consent_adults yes_no
label variable consent_parents "Did the patient consent to participate?"
label values consent_parents yes_no
label variable consent_adolescents "Did the minor provide assent consent to participate?"
label values consent_adolescents yes_no
label variable consent_children "Did the minor provide assent consent to participate?"
label values consent_children yes_no

* eligibility criteria 	 
label var eligibility_criteria_1 "An adult is able to give consent"
label var eligibility_criteria_2 "At clinic for an outpatient consultation"
label var eligibility_criteria_3 "Patient does not require emergency care"
label var eligibility_criteria_4 "Not here for a checkup"
label var eligibility_criteria_5 "Not a trauma patient "
label var eligibility_criteria_6 "Not a scheduled procedure or a birth"	
	
label variable patient_age_group "Enumerator: Select the age group the patient belongs to."
label define patient_age_group 1 "Adult (18 years old or more)" 2 "Adolescent (13 to 17 years old)" 3 "Child (7 to 12 years old)" 4 "Child (0 to 6 years old)"
label values patient_age_group patient_age_group

label variable patient_gender "Enumerator: Select the patient's gender"
label define patient_gender 0 "Male" 1 "Female"
label values patient_gender patient_gender

* confirmations 
label var confirm_patient_ready "Please confirm: has the patient seen the CHEW and is preparing to see the MO?"
label values confirm_patient_ready yes_no
label var soap_note_confirm_chew1 "Please, confirm with the CHEW they correctly submitted all necessary SOAP notes (A and B)."
label values soap_note_confirm_chew1 yes_no
label var soap_note_confirm_mo "Please, confirm with the MO they have generated the discharge SOAP note"
label values soap_note_confirm_mo yes_no

* ids
label variable enumid "Please select your name (first and last name):"
cap label variable enu_id_1 "Confirm Enumerator ID"
label variable enu_id_2 "Confirm Enumerator ID"
label variable enu_id_3 "Confirm Enumerator ID"
label variable enumname "Enumerator name"
cap label variable confirm_patient_id_mo "Please confirm the EMR patient ID."
label variable reschewid "Research CHEW in charge of supplemental testing today:"
label variable reschewid_confirm "Research CHEW: please enter your assigned ID."

label variable assessment_id "Assessment ID"
label variable confirm_assessment_id_mo "Enumerator: Please confirm the patient's EMR assessment ID from the MO's EMR screen is [assessment_id]"
label values confirm_assessment_id_mo yes_no

label variable soap_note_confirm_mo "Please confirm that you clicked the Generate SOAP note button for the updated as"
label values soap_note_confirm_mo yes_no

label variable chew_referral "In your normal operations, would you refer this patient to an Attending Doctor?"
label values chew_referral yes_no

label variable res_chew_note2 "confirm the patient agreed to answer a few additional questions:"
label values res_chew_note2 yes_no

* chew feedback
label variable chew_feedback_1 "In my opinion, the LLM feedback helped me improve the written documentation of this patient´s case."
label values chew_feedback_1 agree_disagree_5

label variable chew_feedback_2 "In my opinion, the LLM feedback helped me provide better healthcare for this patient."
note chew_feedback_2: "In my opinion, the LLM feedback helped me provide better healthcare for the patient."
label values chew_feedback_2 agree_disagree_5

label variable chew_feedback_3 "In my opinion, the LLM feedback for this patient contained a medical error."	
label define chew_feedback_3 1 "Yes, LLM feedback contained a serious error." 2 "Yes, LLM feedback contained a minor error." 3 "No error"
label values chew_feedback_3 chew_feedback_3
label variable chew_feedback_3_desc "Please briefly describe the error"

label variable comment_chew "Take notes on any problems the CHEW encountered with the data collection procedure."
note comment_chew: "Enumerator: Please verify that the CHEW generated both SOAP notes. Take notes on any problems the CHEW encountered with the data collection procedure."

* mo consultation 
label var patient_severity "MO: Please summarize: How severe is this patient's condition or illness?"
label define patient_sev 1 "Severe (treatment is important to prevent serious complications)" 2 "Moderate (requires treatmenr or may not improve symptoms)" 3 "Mild (simple treatments may or may not improve symptoms)"
label values patient_severity patient_sev

* test criteria 
label var test_criteria_1 "Fever in the last 24 hours"
label var test_criteria_2 "Pain while urinating"
label var test_criteria_3 "Burning feeling while urinating"
label var test_criteria_4 "Abnormal discharge from the genital area"
label var test_criteria_5 "Blood in urine"
label var test_criteria_6 "Frequent urge to urinate"
label var test_criteria_7 "Feeling tired or with low energy for >= 4 weeks"
label var test_criteria_8 "Feeling like your heart is racing for >= 4 weeks"
label var test_criteria_9 "Signs of bleeing for >= 4 weeks"
label var test_criteria_10 "Feeling dizzy"
label var test_criteria_11 "Difficulty breathing"
label var test_criteria_12 "Currently pregnant"
label var test_criteria_13 "Delivered a baby within the past six weeks"
label var test_criteria__222 "Don't know/Don't want to answer"

label var test_first_choice_1 "Malaria RDT"
label var test_first_choice_2 "Urine dipstick analysis"
label var test_first_choice_3 "PCV (anemia)"
label var test_first_choice__222 "None"

label var test_consent_adults_1 "Malaria RDT"
label var test_consent_adults_2 "Urine dipstick analysis"
label var test_consent_adults_3 "PCV (anemia)"
label var test_consent_adults__222 "None"
label var test_consent_minors_1 "Malaria RDT"
label var test_consent_minors_2 "Urine dipstick analysis"
cap label var test_consent_minors__222 "None"

label var test_request_adults_1 "Confirm ordered: Malaria RDT"
label var test_request_adults_2 "Confirm ordered: Urine dipstick analysis"
label var test_request_adults_3 "Confirm ordered: PCV (anemia)"
cap label var test_request_adults__222 "None"
label var test_request_parental_1 "Confirm ordered: Malaria RDT"
cap label var test_request_parental_2 "Confirm orderd: Urine dipstick analysis"
cap label var test_request_parental__222 "Confirm ordered: None"


// calculate fields 
label var conda "Any patient"
label var condb "Female and over 7 years old patient"
label var condc "Adult patient"
label var condd "Adult and female patient"
label var condnone "None"

label var calc_cond ""

label var join_connd "All tests for conditions"

label var calc_none "No test"
rename  calc_none test_symptoms_none
label var sum_fields "Total number of tests"

label var did_testing_happen "Was testing carried out?"

// consolidate minor and adult consent and test requests
forvalues i=1/2{
 gen test_request_`i' = test_request_adults_`i'
 replace test_request_`i' = 1 if test_request_parental_`i'==1
 drop test_request_adults_`i' test_request_parental_`i'
	// confirmed: minor consent obtained in each relevant case
 gen test_consent_`i'=test_consent_adults_`i'
}
gen   test_consent_3 = test_consent_adults_3
gen test_request_3 = test_request_adults_3
forvalues i=1/3 {
	gen test_ordered_`i' = 1 if (test_first_choice_`i'==1 | test_request_`i'==1)
}

gen test_demo_elig_1 = 1 
label var test_demo_elig_1 "Demog. eligible for mal test"
gen test_demo_elig_2=0
replace test_demo_elig_2=1 if condb==2
label var test_demo_elig_2 "Demog. eligible for urine test"
gen test_demo_elig_3=0
replace test_demo_elig_3=1 if condc==3
label var test_demo_elig_3 "Demog. eligible for PCV test"



// reorder testing variables

order submissiondate - calc_cond test_criteria_1 test_criteria_2 test_criteria_3 test_criteria_4 test_criteria_5 test_criteria_6 test_criteria_7 test_criteria_8 test_criteria_9 test_criteria_10 test_criteria_11 test_criteria_12 test_criteria_13 test_criteria__222 symptom_to_test_count index_1 symptom_index_1 conde_1 test_demo_elig_1 calc_mal test_first_choice_1 test_consent_1 test_request_1  test_ordered_1 index_2 symptom_index_2 conde_2 test_demo_elig_2 calc_uri test_first_choice_2 test_consent_2  test_request_2 test_ordered_2 index_3 symptom_index_3 conde_3 test_demo_elig_3 calc_pcv test_first_choice_3 test_consent_3 test_request_3 test_ordered_3

local i=1
foreach var in mal uri pcv {
	rename calc_`var' test_symptoms_`i'
	replace test_symptoms_`i' =1 if test_symptoms_`i'==`i'
	rename test_first_choice_`i' test_MO_ordered_`i'
	replace test_symptoms_`i'=0 if test_symptoms_`i'==.
	replace test_MO_ordered_`i' = 0 if test_MO_ordered_`i'==. & test_symptoms_`i'==1
		replace test_consent_`i'=0 if test_consent_`i'==.  & test_symptoms_`i'==1 &test_MO_ordered_`i'==0
	replace test_request_`i' =0 if test_request_`i'==.  & test_consent_`i'==1
	replace test_ordered_`i'=0 if test_symptoms_`i'==1 & test_ordered_`i'==.

	label var test_symptoms_`i' "Symptoms eligible for `var' test"
	label var test_MO_ordered_`i' "Confirm: MO ordered"
	label var test_consent_`i' "Consent obtained"
	label var test_request_`i' "Confirm: RCHEW ordered"
	label var test_ordered_`i' "Test was ordered"
	
	local i = `i'+1
} 


// check duplicates
duplicates tag assessment_id, gen(dup)

list assessment_id patient_age_group patient_gender submissiondate if dup > 0
//BMGF587675 - same characteristics
drop if assessment_id == "BMGF587675"

drop dup




// reshape to long format, by patient x test 
keep assessment_id patient_age_group patient_gender join_connd test_symptoms_? test_demo_elig_? test_MO_ordered_* test_consent_? test_request_? test_ordered_* calc_cond

rename calc_cond calculate_eligibility

reshape long test_symptoms_ test_demo_elig_ test_MO_ordered_ test_consent_ test_request_ test_ordered_ , i(assessment_id) j(test_merge_num) 


	label var test_symptoms_ "Symptoms eligible for test (survey based)"
	label var test_MO_ordered_ "Confirm: MO ordered (svy based)"
	label var test_consent_ "Consent obtained"
	label var test_request_ "Confirm: RCHEW ordered (svy based)"
	label var test_ordered_ "Test was ordered (svy based)"
	label var test_demo_elig_ "Demog. eligible for test (svy based)"


destring assessment_id, gen(assessment_id_num) ignore("BMGF")
label define test_lab 1 "Malaria" 2 "Urine analysis" 3 "PCV"
label values test_merge_num test_lab

drop calculate_eligibility join_connd

rename test_symptoms_ test_symp_elig_sv
rename test_MO_ordered_ test_MO_ordered_sv
rename test_consent_ test_consent
rename test_request_ test_RCHEW_ordered_sv
rename test_ordered_ test_ordered_sv
rename test_demo_elig_ test_demo_elig_sv

drop patient_gender patient_age_group

cd "$output"

save "patient_flow_tests_long.dta", replace


