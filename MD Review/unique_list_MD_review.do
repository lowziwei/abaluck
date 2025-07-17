*Create unique list of assessments for MD initial review
*preliminaries
if c(username) == "ninalow" {
    global path "/Users/ninalow/Desktop/Abaluck/GPT4Health"
    global data "$path/raw_data" 
	global output "$path/output"
}
cd "$data"
use "final_merged_data_long.dta", clear

*Check purpose of assessment_id
preserve
bysort assessment_id assessment_index: keep if _n == 1
bysort assessment_id: egen has_index_2 = max(assessment_index == 2)
keep if has_index_2 == 1

* Check that we have both index 1 and 2 for each assessment_id
bysort assessment_id: gen count_indices = _N
keep if count_indices == 2  

sort assessment_id assessment_index
keep assessment_id provider_id submissiondate assessment_index
reshape wide provider_id submissiondate, i(assessment_id) j(assessment_index)


rename provider_id1 provider
rename submissiondate1 submission_index_1  
rename submissiondate2 submission_index_2

keep assessment_id provider submission_index_1 submission_index_2

cd "$output"
export delimited using "index_2_variations.csv", replace

restore

*Only look at post Feb 25 data due to randomization issue with blinding.
keep if sample_rerated_25feb ==1

*0--------------
*Descriptive - Distribution of QALY ratings 
gen assisted = (d_assisted_note == 1)
	lab var assisted "Assisted"
	label define ai_note_lab 0 "Unassisted" 1 "Assisted"
label values assisted ai_note_lab

*Calculate combined winsorizing percentiles at 99
summarize QALY_hours, detail
local p1 = r(p1)
local p99 = r(p99) 

gen QALY_cmb_winsor = QALY_hours
replace QALY_cmb_winsor = `p1' if QALY_hours < `p1'
replace QALY_cmb_winsor = `p99' if QALY_hours > `p99'

*calculate outliers
summarize QALY_hours if assisted == 0, detail
local q1_unassisted = r(p25)
local q3_unassisted = r(p75) 
local iqr_unassisted = `q3_unassisted' - `q1_unassisted'
local lower_fence_unassisted = `q1_unassisted' - 1.5*`iqr_unassisted'
local upper_fence_unassisted = `q3_unassisted' + 1.5*`iqr_unassisted'

*Get 1st and 99th percentiles for winsorization
local p1_unassisted = r(p1)
local p99_unassisted = r(p99)

summarize QALY_hours if assisted == 1, detail
local q1_assisted = r(p25)
local q3_assisted = r(p75) 
local iqr_assisted = `q3_assisted' - `q1_assisted'
local lower_fence_assisted = `q1_assisted' - 1.5*`iqr_assisted'
local upper_fence_assisted = `q3_assisted' + 1.5*`iqr_assisted'

*Get 1st and 99th percentiles for winsorization
local p1_assisted = r(p1)
local p99_assisted = r(p99)

gen outlier = ((assisted == 0 & (QALY_hours < `lower_fence_unassisted' | QALY_hours > `upper_fence_unassisted')) | ///
               (assisted == 1 & (QALY_hours < `lower_fence_assisted' | QALY_hours > `upper_fence_assisted')))

 *Count outliers for each group
count if outlier == 1 & assisted == 0 //146 
count if outlier == 1 & assisted == 1  //147	

*Winsorize at 1st and 99th percentiles by group
gen QALY_hours_winsorized = QALY_hours

replace QALY_hours_winsorized = `p1_unassisted' if assisted == 0 & QALY_hours < `p1_unassisted'
replace QALY_hours_winsorized = `p99_unassisted' if assisted == 0 & QALY_hours > `p99_unassisted'
replace QALY_hours_winsorized = `p1_assisted' if assisted == 1 & QALY_hours < `p1_assisted'
replace QALY_hours_winsorized = `p99_assisted' if assisted == 1 & QALY_hours > `p99_assisted'
			   
count if QALY_hours != QALY_hours_winsorized & assisted == 0 //2
count if QALY_hours != QALY_hours_winsorized & assisted == 1 //1




*Two way comparision for all data vs. winsorize combined 99 percentile 
*Unassisted
* Panel 1: Before (all unassisted data)
histogram QALY_hours if assisted == 0, ///
    frequency fcolor(blue%30) lcolor(blue) ///
    title("Before Winsorize at 99 Percentile") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(unassisted_before_winsor, replace)

* Panel 2: After (unassisted without outliers)  
histogram QALY_cmb_winsor if assisted == 0, ///
    frequency fcolor(red%30) lcolor(red) ///
    title("After Winsorize at 99 Percentile") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(unassisted_after_winsor, replace)

*Combine unassisted graphs
graph combine unassisted_before_winsor unassisted_after_winsor, ///
    title("Unassisted QALY Hours") 
	cd "$output"
	graph export "unassisted_qaly_cmb_winsor_hist.png", replace
	
*ASSISTED
* Panel 1: Before (all assisted data)
histogram QALY_hours if assisted == 1, ///
    frequency fcolor(blue%30) lcolor(blue) ///
    title("Before Winsorize at 99 Percentile") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(assisted_before_winsor, replace)

* Panel 2: After (assisted without outliers)
histogram QALY_cmb_winsor if assisted == 1, ///
    frequency fcolor(red%30) lcolor(red) ///
    title("After Winsorize at 99 Percentile") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(assisted_after_winsor, replace)

*Combine assisted graphs
graph combine assisted_before_winsor assisted_after_winsor, ///
    title("Assisted QALY Hours")	
	cd "$output"
graph export "assisted_qaly_cmb_winsor_hist.png", replace


*Two way comparision for all data vs. winsorize 99 percentile 
*Unassisted
* Panel 1: Before (all unassisted data)
histogram QALY_hours if assisted == 0, ///
    frequency fcolor(blue%30) lcolor(blue) ///
    title("Before Winsorize at 99 Percentile") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(unassisted_before_winsor, replace)

* Panel 2: After (unassisted without outliers)  
histogram QALY_hours_winsorized if assisted == 0, ///
    frequency fcolor(red%30) lcolor(red) ///
    title("After Winsorize at 99 Percentile") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(unassisted_after_winsor, replace)

*Combine unassisted graphs
graph combine unassisted_before_winsor unassisted_after_winsor, ///
    title("Unassisted QALY Hours") 
	cd "$output"
	graph export "unassisted_qaly_winsor_hist.png", replace
	
*ASSISTED
* Panel 1: Before (all assisted data)
histogram QALY_hours if assisted == 1, ///
    frequency fcolor(blue%30) lcolor(blue) ///
    title("Before Winsorize at 99 Percentile") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(assisted_before_winsor, replace)

* Panel 2: After (assisted without outliers)
histogram QALY_hours_winsorized if assisted == 1, ///
    frequency fcolor(red%30) lcolor(red) ///
    title("After Winsorize at 99 Percentile") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(assisted_after_winsor, replace)

*Combine assisted graphs
graph combine assisted_before_winsor assisted_after_winsor, ///
    title("Assisted QALY Hours")	
	cd "$output"
graph export "assisted_qaly_winsor_hist.png", replace


*Two way comparision for all data vs. excluding outliers			   
*Unassisted
* Panel 1: Before (all unassisted data)
histogram QALY_hours if assisted == 0, ///
    frequency fcolor(blue%30) lcolor(blue) ///
    title("Before Outlier Removal") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(unassisted_before, replace)

* Panel 2: After (unassisted without outliers)  
histogram QALY_hours if assisted == 0 & outlier == 0, ///
    frequency fcolor(red%30) lcolor(red) ///
    title("After Outlier Removal") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(unassisted_after, replace)

*Combine unassisted graphs
graph combine unassisted_before unassisted_after, ///
    title("Unassisted QALY Hours: Before vs After Outlier Removal") 
	cd "$output"
	graph export "unassisted_qaly_hist.png", replace
	
*ASSISTED
* Panel 1: Before (all assisted data)
histogram QALY_hours if assisted == 1, ///
    frequency fcolor(blue%30) lcolor(blue) ///
    title("Before Outlier Removal") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(assisted_before, replace)

* Panel 2: After (assisted without outliers)
histogram QALY_hours if assisted == 1 & outlier == 0, ///
    frequency fcolor(red%30) lcolor(red) ///
    title("After Outlier Removal") ///
    xtitle("QALY Hours") ytitle("Frequency") ///
    name(assisted_after, replace)

*Combine assisted graphs
graph combine assisted_before assisted_after, ///
    title("Assisted QALY Hours: Before vs After Outlier Removal")	
	cd "$output"
graph export "assisted_qaly_hist.png", replace
	
*Output summary table - Unassisted 
matrix drop _all

quietly {
    * Complete data 
    summarize QALY_hours if assisted == 0, detail
    matrix unassisted_complete = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
    
    * No outliers 
    summarize QALY_hours if assisted == 0 & outlier == 0, detail
    matrix unassisted_no_outliers = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
    
    * Winsorized
    summarize QALY_hours_winsorized if assisted == 0, detail
    matrix unassisted_winsorized = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
	
	* Combined Winsorized
    summarize QALY_cmb_winsor if assisted == 0, detail
    matrix unassisted_cmb_winsorized = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
}

	
* Combine into single matrix
matrix unassisted_summary = unassisted_complete \ unassisted_no_outliers \ unassisted_winsorized \ unassisted_cmb_winsorized

* Set row and column names
matrix rownames unassisted_summary = "Complete" "No_Outliers" "Winsorized (Within)" "Winsorize (Across)"
matrix colnames unassisted_summary = "Mean" "SD" "p1" "p5" "p25" "p50" "p75" "p95" "p99" "Min" "Max" "N"

cd "$output"
esttab matrix(unassisted_summary) using "unassisted_summary.csv", replace csv ///
    title("Unassisted QALY Hours Summary")	
	
*Output summary table - Assisted 
matrix drop _all

quietly {
    * Complete data 
    summarize QALY_hours if assisted == 1, detail
    matrix assisted_complete = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
    
    * No outliers 
    summarize QALY_hours if assisted == 1 & outlier == 0, detail
    matrix assisted_no_outliers = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
    
    * Winsorized 
    summarize QALY_hours_winsorized if assisted == 1, detail
    matrix assisted_winsorized = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
	
	* Winsorized 
    summarize QALY_cmb_winsor if assisted == 1, detail
    matrix assisted_cmb_winsorized = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
}

* Combine into single matrix
matrix assisted_summary = assisted_complete \ assisted_no_outliers \ assisted_winsorized \ assisted_cmb_winsorized

* Set row and column names
matrix rownames assisted_summary = "Complete" "No_Outliers" "Winsorized (Within)" "Winsorized (Across)"
matrix colnames assisted_summary = "Mean" "SD" "p1" "p5" "p25" "p50" "p75" "p95" "p99" "Min" "Max" "N"

cd "$output"
esttab matrix(assisted_summary) using "assisted_summary.csv", replace csv ///
    title("Assisted QALY Hours Summary")	
	
*Output summary table - combined
quietly {
    * Complete data 
    summarize QALY_hours, detail
    matrix complete = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
}

* Combine into single matrix
matrix complete_table_summary = complete 

* Set row and column names
matrix rownames complete_table_summary = "Complete" 
matrix colnames complete_table_summary = "Mean" "SD" "p1" "p5" "p25" "p50" "p75" "p95" "p99" "Min" "Max" "N"

cd "$output"
esttab matrix(complete_table_summary) using "complete_table_summary.csv", replace csv ///
    title("Complete Summary")		
	

	
*1--------------
*Find 5% of unassisted notes with worst QALY ratings

preserve
keep if assisted == 0
qui summarize QALY_hours if assisted == 0, detail
keep if QALY_hours >= r(p95) 
keep assessment_id assessment_index soap_type QALY_hours assisted harm_risk soap_severe_error
cd "$output"
export delimited using "unassisted_worst_5pct_QALY_complete.csv", replace

restore

*Same but remove outliers
preserve
keep if assisted == 0 & outlier == 0
qui summarize QALY_hours, detail
keep if QALY_hours >= r(p95) 
keep assessment_id assessment_index soap_type QALY_hours assisted harm_risk soap_severe_error
cd "$output"
export delimited using "unassisted_worst_5pct_QALY_noOutliers.csv", replace

restore

*Same but after winsorize
preserve
keep if assisted == 0
qui summarize QALY_hours_winsorized, detail
keep if QALY_hours_winsorized >= r(p95) 
keep assessment_id assessment_index soap_type QALY_hours_winsorized assisted harm_risk soap_severe_error
cd "$output"
export delimited using "unassisted_worst_5pct_QALY_winsor.csv", replace

restore

*1.1------------
*Find fixed sample size - Top 5% with random tie breaking for boundary cases
*Complete
preserve
keep if assisted == 0
qui count
local total_n = r(N)
local target_n = ceil(`total_n' * 0.05)  // At least 5% of observations (rounds up)

* Sort by QALY_hours (descending) with random tie-breaking
set seed 12345  
gen random_tie_breaker = runiform()
gsort -QALY_hours -random_tie_breaker

* Keep exactly top 5%
keep if _n <= `target_n'

di "Complete data: Selected `target_n' out of `total_n' unassisted cases (exactly 5%)"

keep assessment_id assessment_index soap_type QALY_hours assisted harm_risk soap_severe_error
cd "$output"
export delimited using "unassisted_worst_5pct_fix_QALY_complete.csv", replace
restore

*Remove Outliers
preserve
keep if assisted == 0 & outlier == 0
qui count
local total_n = r(N)
local target_n = ceil(`total_n' * 0.05)

* Sort with random tie-breaking
set seed 12345
gen random_tie_breaker = runiform()
gsort -QALY_hours -random_tie_breaker

* Keep exactly top 5%
keep if _n <= `target_n'

di "No outliers: Selected `target_n' out of `total_n' unassisted cases (exactly 5%)"

keep assessment_id assessment_index soap_type QALY_hours assisted harm_risk soap_severe_error
cd "$output"
export delimited using "unassisted_worst_5pct_fix_QALY_noOutliers.csv", replace
restore

*Winsorzied at 99 percentile 
preserve
keep if assisted == 0
qui count
local total_n = r(N)
local target_n = ceil(`total_n' * 0.05)

* Sort by winsorized QALY_hours with random tie-breaking
set seed 12345
gen random_tie_breaker = runiform()
gsort -QALY_hours_winsorized -random_tie_breaker

* Keep exactly top 5%
keep if _n <= `target_n'

di "Winsorized: Selected `target_n' out of `total_n' unassisted cases (exactly 5%)"

keep assessment_id assessment_index soap_type QALY_hours_winsorized assisted harm_risk soap_severe_error
cd "$output"
export delimited using "unassisted_worst_5pct_fix_QALY_winsor.csv", replace
restore

*2--------------
*Find 5% of notes with biggest benefit from LLM
gen QALY_hours_unassisted = .
gen QALY_hours_assisted = .
bysort assessment_id: replace QALY_hours_unassisted = QALY_hours if assisted == 0
bysort assessment_id: replace QALY_hours_assisted = QALY_hours if assisted == 1

bysort assessment_id: egen temp_unassisted = max(QALY_hours_unassisted)
bysort assessment_id: egen temp_assisted = max(QALY_hours_assisted)

replace QALY_hours_unassisted = temp_unassisted
replace QALY_hours_assisted = temp_assisted
drop temp_unassisted temp_assisted

gen QALY_difference = QALY_hours_unassisted - QALY_hours_assisted // More positive => LLM made larger positive difference

preserve
bysort assessment_id assessment_index: keep if _n == 1

*Create outlier
summarize QALY_difference, detail
local q1_diff = r(p25)
local q3_diff = r(p75) 
local iqr_diff = `q3_diff' - `q1_diff'
local lower_fence_diff = `q1_diff' - 1.5*`iqr_diff'
local upper_fence_diff = `q3_diff' + 1.5*`iqr_diff'

*Get 1st and 99th percentiles for winsorization
local p1_diff = r(p1)
local p99_diff = r(p99)

gen outlier_diff = (QALY_difference < `lower_fence_diff') | (QALY_difference > `upper_fence_diff')

*Count outliers
count if outlier_diff == 1 //157

tabstat QALY_difference if outlier_diff == 0, statistics(p5 p10 p25 p50 p75 p90 p95 mean sd n) //no variation if removed outliers - all values are 0

	
histogram QALY_difference, ///
	frequency ///
	fcolor(blue%30) lcolor(blue) ///
	title("QALY Difference: Unassisted - Assisted") ///
	subtitle("Positive values = LLM helps (reduces QALY hours)") ///
	xtitle("QALY Difference")
	 cd "$output"
graph export "qaly_hours_diff_hist.png", replace

count if QALY_difference > 0 //74 cases -> LLM positive VA
count if QALY_difference < 0  //83 cases -> LLM negative VA

*Winsorize at 1st and 99th percentiles by group
gen QALY_diff_winsor = QALY_difference
replace QALY_diff_winsor = `p1_diff' if QALY_difference < `p1_diff'
replace QALY_diff_winsor = `p99_diff' if QALY_difference > `p99_diff'
			   
count if QALY_difference != QALY_diff_winsor //12


*Two way comparision for all data vs. winsorize 99 percentile 
*Unassisted
* Panel 1: Before (all unassisted data)
histogram QALY_difference, ///
    frequency fcolor(blue%30) lcolor(blue) ///
    title("Before Winsorize at 99 Percentile") ///
    xtitle("QALY Hours Difference (Unassisted - Assisted)") ytitle("Frequency") ///
    name(QALY_diff_before_winsor, replace)

* Panel 2: After (unassisted without outliers)  
histogram QALY_diff_winsor, ///
    frequency fcolor(red%30) lcolor(red) ///
    title("After Winsorize at 99 Percentile") ///
    xtitle("QALY Hours Difference (Unassisted - Assisted)") ytitle("Frequency") ///
    name(QALY_diff_after_winsor, replace)

*Combine unassisted graphs
graph combine QALY_diff_before_winsor QALY_diff_after_winsor, ///
    title("QALY Hours Difference (Unassisted - Assisted)") ///
	subtitle("Positive values = LLM makes positive impact")
	cd "$output"
	graph export "qaly_diff_hist.png", replace
	
	
*Create summary table
matrix drop _all

quietly {
    * Complete data 
    summarize QALY_difference, detail
    matrix QALY_diff_complete = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
    
    * No outliers 
    summarize QALY_difference if outlier_diff == 0, detail
    matrix QALY_diff_no_outliers = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
    
    * Winsorized 
    summarize QALY_diff_winsor, detail
    matrix QALY_diff_winsorized = r(mean), r(sd), r(p1), r(p5), r(p25), r(p50), r(p75), r(p95), r(p99), r(min), r(max), r(N)
}

* Combine into single matrix
matrix QALY_diff_summary = QALY_diff_complete \ QALY_diff_no_outliers \ QALY_diff_winsorized

* Set row and column names
matrix rownames QALY_diff_summary = "Complete" "No_Outliers" "Winsorized"
matrix colnames QALY_diff_summary = "Mean" "SD" "p1" "p5" "p25" "p50" "p75" "p95" "p99" "Min" "Max" "N"

cd "$output"
esttab matrix(QALY_diff_summary) using "QALY_diff_summary.csv", replace csv ///
    title("QALY Hours Difference (Unassisted - Assisted) Summary")


summarize QALY_diff_winsor, detail
keep if QALY_diff_winsor >= r(p95) 
keep assessment_id assessment_index QALY_hours_assisted QALY_hours_unassisted QALY_difference QALY_diff_winsor
export delimited using "biggest_benefit_5pct_QALY_winsor.csv", replace
restore

preserve
bysort assessment_id assessment_index: keep if _n == 1
summarize QALY_difference, detail
keep if QALY_difference >= r(p95) 
keep assessment_id assessment_index QALY_hours_assisted QALY_hours_unassisted QALY_difference
export delimited using "biggest_benefit_5pct_QALY_complete.csv", replace
restore

*2.2------------
*Find fixed sample size - Top 5% with random tie breaking for boundary cases
*Complete
preserve
bysort assessment_id assessment_index: keep if _n == 1
qui count
local total_n = r(N)
local target_n = ceil(`total_n' * 0.05)  // At least 5% of observations (rounds up)

* Sort by QALY_hours (descending) with random tie-breaking
set seed 12345  
gen random_tie_breaker = runiform()
gsort -QALY_difference -random_tie_breaker

* Keep exactly top 5%
keep if _n <= `target_n'

di "Complete data: Selected `target_n' out of `total_n' unassisted cases (exactly 5%)"

keep assessment_id assessment_index QALY_hours_assisted QALY_hours_unassisted QALY_difference
cd "$output"
export delimited using "biggest_benefit_5pct_fix_QALY_complete.csv", replace
restore

*Winsorzied at 99 percentile 
preserve
bysort assessment_id assessment_index: keep if _n == 1
*Create outlier
summarize QALY_difference, detail
local p1_diff = r(p1)
local p99_diff = r(p99)

gen QALY_diff_winsor = QALY_difference
replace QALY_diff_winsor = `p1_diff' if QALY_difference < `p1_diff'
replace QALY_diff_winsor = `p99_diff' if QALY_difference > `p99_diff'

qui count
local total_n = r(N)
local target_n = ceil(`total_n' * 0.05)

* Sort by winsorized QALY_hours with random tie-breaking
set seed 12345
gen random_tie_breaker = runiform()
gsort -QALY_diff_winsor -random_tie_breaker

* Keep exactly top 5%
keep if _n <= `target_n'

di "Winsorized: Selected `target_n' out of `total_n' unassisted cases (exactly 5%)"

keep assessment_id assessment_index QALY_hours_assisted QALY_hours_unassisted QALY_difference QALY_diff_winsor
cd "$output"
export delimited using "biggest_benefit_5pct_fix_QALY_winsor.csv", replace
restore


*3--------------
*Find 5% of notes with biggest degradation from LLM

preserve 

bysort assessment_id assessment_index: keep if _n == 1
tabstat QALY_difference, statistics(p5 p10 p25 p50 p75 p90 p95 mean sd n)
summarize QALY_difference, detail
keep if QALY_difference <= r(p5) 
keep assessment_id assessment_index QALY_hours_assisted QALY_hours_unassisted QALY_difference
export delimited using "biggest_degradation_5pct_QALY_complete.csv", replace

restore

*Winsorzied at 99 percentile 
preserve
bysort assessment_id assessment_index: keep if _n == 1
*Create outlier
summarize QALY_difference, detail
local p1_diff = r(p1)
local p99_diff = r(p99)

gen QALY_diff_winsor = QALY_difference
replace QALY_diff_winsor = `p1_diff' if QALY_difference < `p1_diff'
replace QALY_diff_winsor = `p99_diff' if QALY_difference > `p99_diff'

summarize QALY_diff_winsor, detail
keep if QALY_difference <= r(p5) 
keep assessment_id assessment_index QALY_hours_assisted QALY_hours_unassisted QALY_difference QALY_diff_winsor
export delimited using "biggest_degradation_5pct_QALY_winsor.csv", replace
restore


*3.2------------
*Complete
preserve
bysort assessment_id assessment_index: keep if _n == 1
qui count
local total_n = r(N)
local target_n = ceil(`total_n' * 0.05)  // At least 5% of observations (rounds up)

* Sort by QALY_hours (descending) with random tie-breaking
set seed 12345  
gen random_tie_breaker = runiform()
gsort QALY_difference random_tie_breaker

* Keep exactly top 5%
keep if _n <= `target_n'

di "Complete data: Selected `target_n' out of `total_n' unassisted cases (exactly 5%)"

keep assessment_id assessment_index QALY_hours_assisted QALY_hours_unassisted QALY_difference
cd "$output"
export delimited using "biggest_degradation_5pct_fix_QALY_complete.csv", replace
restore


*Winsorzied at 99 percentile 
preserve
bysort assessment_id assessment_index: keep if _n == 1
*Create outlier
summarize QALY_difference, detail
local p1_diff = r(p1)
local p99_diff = r(p99)

gen QALY_diff_winsor = QALY_difference
replace QALY_diff_winsor = `p1_diff' if QALY_difference < `p1_diff'
replace QALY_diff_winsor = `p99_diff' if QALY_difference > `p99_diff'

qui count
local total_n = r(N)
local target_n = ceil(`total_n' * 0.05)

* Sort by winsorized QALY_hours with random tie-breaking
set seed 12345
gen random_tie_breaker = runiform()
gsort QALY_diff_winsor random_tie_breaker

* Keep exactly top 5%
keep if _n <= `target_n'

di "Winsorized: Selected `target_n' out of `total_n' unassisted cases (exactly 5%)"

keep assessment_id assessment_index QALY_hours_assisted QALY_hours_unassisted QALY_difference QALY_diff_winsor
cd "$output"
export delimited using "biggest_degradation_5pct_fix_QALY_winsor.csv", replace
restore



*Find fixed sample size - Top 5% with random tie breaking for boundary cases


*4-------------
*Create unique lists of cases for MD Initial Review

cd "$output"

import delimited "unassisted_worst_5pct_fix_QALY_complete.csv", clear
keep assessment_id assessment_index
gen unassisted_worst = 1
tempfile file1
save `file1'

import delimited "biggest_benefit_5pct_fix_QALY_complete.csv", clear
keep assessment_id assessment_index
gen biggest_benefit = 1
tempfile file2
save `file2'

import delimited "biggest_degradation_5pct_fix_QALY_complete.csv", clear
keep assessment_id assessment_index
gen biggest_degradation = 1
tempfile file3
save `file3'

use `file1', clear
merge 1:1 assessment_id assessment_index using `file2', nogen
merge 1:1 assessment_id assessment_index using `file3', nogen

replace unassisted_worst = 0 if missing(unassisted_worst)
replace biggest_benefit = 0 if missing(biggest_benefit)
replace biggest_degradation = 0 if missing(biggest_degradation)


gen sources = ""
replace sources = sources + "unassisted_worst" if unassisted_worst == 1
replace sources = sources + ", biggest_benefit" if biggest_benefit == 1 & sources != ""
replace sources = sources + "biggest_benefit" if biggest_benefit == 1 & sources == ""
replace sources = sources + ", biggest_degradation" if biggest_degradation == 1 & sources != ""
replace sources = sources + "biggest_degradation" if biggest_degradation == 1 & sources == ""


export delimited using "unique_assessment_ids_with_sources.csv", replace
save "unique_assessment_ids_with_sources.dta", replace

*Overlap analysis
gen overlap_count = unassisted_worst + biggest_benefit + biggest_degradation

gen overlap_category = ""
replace overlap_category = "Unassisted Worst Only" if unassisted_worst == 1 & biggest_benefit == 0 & biggest_degradation == 0
replace overlap_category = "Most Benefit Only" if unassisted_worst == 0 & biggest_benefit == 1 & biggest_degradation == 0
replace overlap_category = "Most Degradation Only" if unassisted_worst == 0 & biggest_benefit == 0 & biggest_degradation == 1
replace overlap_category = "Unassisted Worst + Most Benefit" if unassisted_worst == 1 & biggest_benefit == 1 & biggest_degradation == 0
replace overlap_category = "Unassisted Worst + Most Degradation" if unassisted_worst == 1 & biggest_benefit == 0 & biggest_degradation == 1
replace overlap_category = "Most Benefit + Most Degradation" if unassisted_worst == 0 & biggest_benefit == 1 & biggest_degradation == 1
replace overlap_category = "All Three Groups" if unassisted_worst == 1 & biggest_benefit == 1 & biggest_degradation == 1

* Create summary table
preserve
contract overlap_category, freq(count) percent(percentage)
gsort -count

* Format percentage
replace percentage = round(percentage, 0.0001)

* Clean up and export
rename overlap_category Source_Group_Combination
rename count Number_of_Assessments
rename percentage Percentage

export delimited using "source_group_overlap_analysis.csv", replace

list, clean noobs
restore











