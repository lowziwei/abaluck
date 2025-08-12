*Randomize list of assessment ID for MD initial review
set seed 2025710

*preliminaries
if c(username) == "ninalow" {
    global path "/Users/ninalow/Desktop/Abaluck/GPT4Health"
    global data "$path/raw_data" 
	global output "$path/output"
}
cd "$output"

*Randomize assessment_id to 2 MDs

use "unique_assessment_ids_with_sources.dta", clear 

scalar temp_obs =_N

preserve 
clear
set obs `=temp_obs'
scalar cases_per_pair = floor(temp_obs/ 3)
scalar remainder = mod(temp_obs, 3)


gen pair_cycle = .

replace pair_cycle = 1 in 1/`=cases_per_pair' //sets observations from 1 to cases per pair to cycle1
replace pair_cycle = 2 in `=cases_per_pair+1'/`=cases_per_pair*2'
replace pair_cycle = 3 in `=cases_per_pair*2+1'/`=cases_per_pair*3'

* Handle remainder cases 
if remainder > 0 {
    
    replace pair_cycle = ceil(runiform() * 3) if missing(pair_cycle) //For each missing pair_cycle, randomly assign 1, 2, or 3
}


*Shuffle Pair Cycles
gen random_order = runiform()
sort random_order

gen assignment_id = _n

*Save temporary file
tempfile assignments
save "assignments.dta", replace

restore 
*Assign MD pairs based on Pair Cycles with 3 MDs:
*1 - (MD1, MD2)
*2 - (MD1, MD3)
*3 - (MD2, MD3)

gen assignment_id = _n
merge 1:1 assignment_id using "assignments.dta", nogen

gen md1 = cond(pair_cycle == 1, 1, cond(pair_cycle == 2, 1, 2))
gen md2 = cond(pair_cycle == 1, 2, cond(pair_cycle == 2, 3, 3))

keep assessment_id assessment_index md1 md2

export delimited using "MD_initial_review_assignment.csv", replace

preserve

gen id = _n

reshape long md, i(id) j(md_num)

drop id md_num
sort md

export delimited using "MD_initial_review_assignment_long.csv", replace
restore

*------------------------------
* Create balance table dataset
preserve

* Count assignments for each MD
count if md1 == 1 | md2 == 1
local md1_count = r(N)
count if md1 == 2 | md2 == 2  
local md2_count = r(N)
count if md1 == 3 | md2 == 3
local md3_count = r(N)

clear
set obs 3

gen MD = ""
gen Cases_Assigned = .
gen Percentage = .

replace MD = "MD 1" in 1
replace Cases_Assigned = `md1_count' in 1
replace Percentage = round(`md1_count' / (`md1_count' + `md2_count' + `md3_count') * 100, 0.0001) in 1

replace MD = "MD 2" in 2  
replace Cases_Assigned = `md2_count' in 2
replace Percentage = round(`md2_count' / (`md1_count' + `md2_count' + `md3_count') * 100, 0.0001) in 2

replace MD = "MD 3" in 3
replace Cases_Assigned = `md3_count' in 3
replace Percentage = round(`md3_count' / (`md1_count' + `md2_count' + `md3_count') * 100, 0.0001) in 3

* Export MD balance table
export delimited using "md_assignment_balance_table.csv", replace

list, clean noobs

restore
