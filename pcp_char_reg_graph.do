qui do 00common/00preamble.do nodep
qui do 00common/00material.do

capture program drop attachYpos
program attachYpos
    syntax, spec(str) pos_list(string) allvars(string)
    use "${out}/temp/coef_`spec'.dta", clear
    gen ypos = .
    local i = 1
    foreach var of local allvars {
        local p : word `i' of `pos_list'
        replace ypos = `p' if varname == "`var'"
        local ++i
    }
    gen lower        = b - 1.96 * se
    gen upper        = b + 1.96 * se
    gen crosses_zero = (lower < 0 & upper > 0)
end

* VARIABLE LISTS & POSITIONS BY BLOCK

* Block: Race & Demographics 
local vars_race doc_Hispanic doc_Black doc_White doc_EastAsian doc_WestAsian ///
    doc_SoutheastAsian doc_SouthAsian npi_age_bucket doc_female

local pos_race 0.5 1.0 1.5 2.0 2.5 3.0 3.5 4.0 4.5

local ylabels_race ylabel( ///
    0.5 "Hispanic"        ///
    1.0 "Black"           ///
    1.5 "White"           ///
    2.0 "East Asian"      ///
    2.5 "West Asian"      ///
    3.0 "Southeast Asian" ///
    3.5 "South Asian"     ///
    4.0 "Age"             ///
    4.5 "Female"          ///
    , angle(0) labsize(vsmall) notick nogrid)

local ylines_race yline(0.5 1.0 1.5 2.0 2.5 3.0 3.5 4.0 4.5, ///
    lwidth(vthin) lcolor(gs12) lpattern(solid))

* Block: Med School + Training
local vars_medtrain doc_Fellowship Multiple_Residency Top25_Residency ///
    School_StudtoFac School_MCAT has_PrimCare_Rank Top10_PrimCare Top10_Research ///
    Med_Private top25Med isMD

local pos_medtrain 0.5 1.0 1.5 2.0 2.5 3.0 3.5 4.0 4.5 5.0 5.5

local ylabels_medtrain ylabel( ///
    0.5 "Had Fellowship"        ///
    1.0 "Multiple Residency"    ///
    1.5 "Top-25 Residency"      ///
    2.0 "Student-Faculty Ratio" ///
    2.5 "School MCAT"           ///
    3.0 "Has Primcare Rank"     ///
    3.5 "Top-10 Primcare"       ///
    4.0 "Top-10 Research"       ///
    4.5 "Private Med School"    ///
    5.0 "Top-25 Med School"     ///
    5.5 "is MD"                 ///
    , angle(0) labsize(vsmall) notick nogrid)

local ylines_medtrain yline(0.5 1.0 1.5 2.0 2.5 3.0 3.5 4.0 4.5 5.0 5.5, ///
    lwidth(vthin) lcolor(gs12) lpattern(solid))

* Block: Other 
local vars_other ever_payor_employed ever_acquired_at_time has_dox_profile ///
    has_review avg_reputation spouse_doc has_spouse ///
    doc_highIncome doc_highWealth doc_reg_voter doc_Democrat doc_Republican

local pos_other 0.0 0.5 1.0 1.5 2.0 2.5 3.0 3.5 4.0 4.5 5.0 5.5

local ylabels_other ylabel( ///
    0.0 "Ever Employed by Payor" ///
    0.5 "Ever Acquired"          ///
    1.0 "Has Dox Profile"        ///
    1.5 "Has Review"             ///
    2.0 "Avg Online Reputation"  ///
    2.5 "Spouse is Doctor"       ///
    3.0 "Has Spouse"             ///
    3.5 "High Income"            ///
    4.0 "High Wealth"            ///
    4.5 "Registered Voter"       ///
    5.0 "Democrat"               ///
    5.5 "Republican"             ///
    , angle(0) labsize(vsmall) notick nogrid)

local ylines_other yline(0.0 0.5 1.0 1.5 2.0 2.5 3.0 3.5 4.0 4.5 5.0 5.5, ///
    lwidth(vthin) lcolor(gs12) lpattern(solid))

* Full char_vars only
local allvars_full ever_payor_employed ever_acquired_at_time ///
    has_dox_profile has_review has_spouse ///
    doc_highIncome doc_highWealth doc_reg_voter doc_Democrat doc_Republican ///
    doc_Fellowship Multiple_Residency ///
    Med_Private top25Med isMD ///
    doc_Hispanic doc_Black doc_White doc_EastAsian doc_WestAsian ///
    doc_SoutheastAsian doc_SouthAsian npi_age_bucket doc_female

local pos_full 1 2 3 4 5 6 7 8 9 10 ///
               13 14 ///
               17 18 19 ///
               22 23 24 25 26 27 28 29 30

local ylabels_full ylabel( ///
    31 "{bf:Race & Demographics}" ///
    30 "Female"                   ///
    29 "Age"                      ///
    28 "South Asian"              ///
    27 "Southeast Asian"          ///
    26 "West Asian"               ///
    25 "East Asian"               ///
    24 "White"                    ///
    23 "Black"                    ///
    22 "Hispanic"                 ///
    20 "{bf:Med School}"          ///
    19 "is MD"                    ///
    18 "Top-25 Med School"        ///
    17 "Private Med School"       ///
    15 "{bf:Training}"            ///
    14 "Multiple Residency"       ///
    13 "Had Fellowship"           ///
    11 "{bf:Other}"               ///
    10 "Republican"               ///
     9 "Democrat"                 ///
     8 "Registered Voter"         ///
     7 "High Wealth"              ///
     6 "High Income"              ///
     5 "Has Spouse"               ///
     4 "Has Review"               ///
     3 "Has Dox Profile"          ///
     2 "Ever Acquired"            ///
     1 "Ever Employed by Payor"   ///
    , angle(0) labsize(vsmall) notick nogrid)

local ylines_full yline(1 2 3 4 5 6 7 8 9 10 13 14 17 18 19 22 23 24 25 26 27 28 29 30, ///
    lwidth(vthin) lcolor(gs12) lpattern(solid))

local xopts_block xlabel(-0.01 -0.005 0 0.005 0.01, labsize(vsmall) format(%5.3f)) ///
    xscale(range(-0.012 0.012)) ///
    xline(0, lwidth(vthin) lcolor(gs8) lpattern(dash))

local gopts_block graphregion(color(white) margin(l=4 r=4 t=2 b=2)) ///
    plotregion(color(white) margin(zero)) scheme(s1mono)

local gopts graphregion(color(white) margin(zero)) ///
    plotregion(color(white) margin(zero)) scheme(s1mono)

cap mkdir "${out}/figures"

* GRAPH 1 — Bivariate OLS: Race & Demographics

attachYpos, spec(full) pos_list(`pos_race') allvars(`vars_race')
twoway ///
    (rcap lower upper ypos if crosses_zero == 1, horizontal lwidth(thin) lcolor(gs6)) ///
    (rcap lower upper ypos if crosses_zero == 0, horizontal lwidth(thin) lcolor(gs6)) ///
    (scatter ypos b if crosses_zero == 1, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)) ///
    (scatter ypos b if crosses_zero == 0, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)), ///
    `ylabels_race' ///
    `xopts_block' ///
    yscale(range(0.25 4.75)) ///
    `ylines_race' ///
    title("Bivariate OLS: Race & Demographics", size(small) box fcolor(gs12) lcolor(gs8) lwidth(thin) bexpand) ///
    ytitle("") xtitle("") legend(off) ///
    `gopts_block' ysize(1) xsize(1.5)
graph export "${out}/figures/pcp_char_lasso_race.png", width(1200) replace

* GRAPH 2 — Bivariate OLS: Med School & Training

attachYpos, spec(full) pos_list(`pos_medtrain') allvars(`vars_medtrain')
twoway ///
    (rcap lower upper ypos if crosses_zero == 1, horizontal lwidth(thin) lcolor(gs6)) ///
    (rcap lower upper ypos if crosses_zero == 0, horizontal lwidth(thin) lcolor(gs6)) ///
    (scatter ypos b if crosses_zero == 1, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)) ///
    (scatter ypos b if crosses_zero == 0, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)), ///
    `ylabels_medtrain' ///
    `xopts_block' ///
    yscale(range(0.25 5.75)) ///
    `ylines_medtrain' ///
    title("Bivariate OLS: Med School & Training", size(small) box fcolor(gs12) lcolor(gs8) lwidth(thin) bexpand) ///
    ytitle("") xtitle("") legend(off) ///
    `gopts_block' ysize(2.2) xsize(3.5)
graph export "${out}/figures/pcp_char_lasso_medtrain.png", width(1200) replace

* GRAPH 3 — Bivariate OLS: Other

attachYpos, spec(full) pos_list(`pos_other') allvars(`vars_other')
twoway ///
    (rcap lower upper ypos if crosses_zero == 1, horizontal lwidth(thin) lcolor(gs6)) ///
    (rcap lower upper ypos if crosses_zero == 0, horizontal lwidth(thin) lcolor(gs6)) ///
    (scatter ypos b if crosses_zero == 1, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)) ///
    (scatter ypos b if crosses_zero == 0, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)), ///
    `ylabels_other' ///
    `xopts_block' ///
    yscale(range(-0.25 5.75)) ///
    `ylines_other' ///
    title("Bivariate OLS: Other", size(small) box fcolor(gs12) lcolor(gs8) lwidth(thin) bexpand) ///
    ytitle("") xtitle("") legend(off) ///
    `gopts_block' ysize(2.2) xsize(3.5)
graph export "${out}/figures/pcp_char_lasso_other.png", width(1200) replace

* GRAPH 4 — Bivariate OLS vs. Post-LASSO OLS (char_vars only)

* Panel A: Bivariate OLS
attachYpos, spec(full) pos_list(`pos_full') allvars(`allvars_full')
twoway ///
    (rcap lower upper ypos if crosses_zero == 1, horizontal lwidth(thin) lcolor(gs6)) ///
    (rcap lower upper ypos if crosses_zero == 0, horizontal lwidth(thin) lcolor(gs6)) ///
    (scatter ypos b if crosses_zero == 1, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)) ///
    (scatter ypos b if crosses_zero == 0, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)), ///
    `ylabels_full' ///
    xlabel(-0.01 0 0.01, labsize(vsmall) format(%5.3f)) ///
    xscale(range(-0.012 0.012)) ///
    yscale(range(0 32)) ///
    xline(0, lwidth(vthin) lcolor(gs8) lpattern(dash)) ///
    `ylines_full' ///
    title("Bivariate OLS", size(small) box fcolor(gs12) lcolor(gs8) lwidth(thin) bexpand) ///
    ytitle("") xtitle("") legend(off) ///
    `gopts' ysize(9) xsize(11) fxsize(57)
graph save "${out}/temp/p4_lasso_bivariate.gph", replace

* Panel B: Post-LASSO OLS
attachYpos, spec(post_lasso) pos_list(`pos_full') allvars(`allvars_full')
twoway ///
    (rcap lower upper ypos if crosses_zero == 1, horizontal lwidth(thin) lcolor(gs6)) ///
    (rcap lower upper ypos if crosses_zero == 0, horizontal lwidth(thin) lcolor(gs6)) ///
    (scatter ypos b if crosses_zero == 1, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)) ///
    (scatter ypos b if crosses_zero == 0, msymbol(circle_hollow) msize(vsmall) mcolor(gs6)), ///
    xlabel(-0.01 0 0.01, labsize(vsmall) format(%5.3f)) ///
    xscale(range(-0.012 0.012)) ///
    yscale(range(0 32)) ylabel(none) ytick(none) ymtick(none) ///
    xline(0, lwidth(vthin) lcolor(gs8) lpattern(dash)) ///
    `ylines_full' ///
    title("Post-LASSO OLS", size(small) box fcolor(gs12) lcolor(gs8) lwidth(thin) bexpand) ///
    ytitle("") xtitle("") legend(off) ///
    `gopts' ysize(9) xsize(5) fxsize(43)
graph save "${out}/temp/p4_post_lasso.gph", replace

* Combine
graph combine ///
    "${out}/temp/p4_lasso_bivariate.gph" ///
    "${out}/temp/p4_post_lasso.gph", ///
    cols(2) imargin(0 0 0 0) ///
    graphregion(color(white)) ///
    ysize(9) xsize(10) ///
    name(combined_lasso, replace)

graph export "${out}/figures/pcp_char_lasso_combined.png", replace