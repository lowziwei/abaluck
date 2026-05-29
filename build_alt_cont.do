*******************************************************************************
* Preamble ********************************************************************
*******************************************************************************
* Title:    build_alt.do
* Blame:    Tedi Totojani <teditotojani1@gmail.com> & Nina Low <ninalow.zi.wei@gmail.com>
* Purpose:  <insert purpose here>
* Note:     <insert notes here>

*******************************************************************************
* Programs ********************************************************************
*******************************************************************************

capture program drop build_alt
program build_alt
    syntax, thresh(real) lbreadth(real) ubreadth(real)

    * impute plan descendant for terminated plans with only contract descendant by choosing the highest rank pbp of the most enrolled network for the contract descendant
    ***************************************************************************
    use "${data}/ntwk/matched-network-descendants_2025-11-03.dta", clear
    
    *impute plan descendant id for terminated plans with plan descendants for subsequent years
    gen same_plan_desc_id = descendant_id if descendant_type == "same plan" 
    bysort plan_id fips_code (same_plan_desc_id): replace same_plan_desc_id = same_plan_desc_id[_N]
    replace descendant_id = same_plan_desc if descendant_type == "contract" & !missing(same_plan_desc_id)
    replace descendant_type = "same plan" if descendant_type == "contract" & !missing(same_plan_desc_id)
    
    keep if descendant_type == "contract"
    keep descendant_id
    ren descendant_id contract
    duplicates drop contract, force
    
    tempfile terminated_contract_only
    save `terminated_contract_only'

    use "${data}/ntwk/ma-genacute-2021.dta", clear

    gen contract = substr(external_plan_id, 1, 5)
    gen pbp = substr(external_plan_id, 7, 3)
    merge m:1 contract using `terminated_contract_only'
    
    * note there exists ~15% of terminated plans with only contract ids with no network info
    * only keep instances where we can find network_id
    keep if _merge == 3

    preserve 
    use "${data}/ntwk/2021enrollment.dta", clear 
    gen pbp = string(planid, "%03.0f")
    ren contractnumber contract
    keep contract pbp enrollment month
    replace enrollment = "" if enrollment == "*"
    destring enrollment, replace 
    collapse (sum) enrollment, by (contract pbp)

    tempfile enrollment
    save `enrollment'
    restore 

    merge m:1 contract pbp using `enrollment', gen(enrol_merge)

    keep if enrol_merge == 3
    sort network_id contract pbp
    *obs are uniquly identified by plan-provider
    collapse (first) network_name enrollment, by(network_id contract pbp)
    sort contract 

    *impute contract by highest enrollment network
    egen network_enroll = sum(enrollment), by(network_id contract)
    gen neg_enrollment = -enrollment
    gen neg_network_enroll = -network_enroll
    sort contract pbp
    bysort contract (neg_network_enroll): gen network_rank = _n

    keep if network_rank ==1 
    gen plan_desc = contract + "-" + pbp 
    ren contract descendant_id 
    keep descendant_id plan_desc
    
    merge 1:m descendant_id using "${data}/ntwk/matched-network-descendants_2025-11-03.dta", gen(_merge)
    replace descendant_id = plan_desc if descendant_type == "contract" & !missing(plan_desc)
    drop plan_desc _merge
    
    * matched descendants differentiated on network_id and other irrelevant,un-used network details
    duplicates drop year plan_id fips_code, force 
    
    * map to current plan - Tedi's original code modified
    ***************************************************************************
    drop if descendant_type == "none"
    drop if fips_code == ""
 
    keep year plan_id fips_code descendant_id descendant_type
    destring year, force replace
    rename plan_id plan
    rename descendant_id plan_desc
    rename fips_code bene_fips

    tempfile map_desc
    save `map_desc'

    * map to network_id
    ***************************************************************************
    use "${data}/ntwk/ma-genacute-2021.dta", clear
    gen plan = substr(external_plan_id, 1, 9)
    tostring network_id, replace
    keep network_id plan

    gduplicates drop
    gduplicates drop plan, force
    rename plan plan_desc

    tempfile ntwks
    save `ntwks'


    * map to in-network status
    ***************************************************************************
    /* (network_id, provider) -> in-network status */

    import delimited "${data}/ntwk/national-pooled-hospital-networks_2019-2022.csv", clear
    keep network_id prvnumgrp
    rename prvnumgrp prvdrnum
    *drop if prvdrnum == ""
    tostring prvdrnum, replace
    tostring network_id, replace
    gduplicates drop

    tempfile map_ntwk_status
    save `map_ntwk_status'

    * get provider zips
    ***********************************************************************
    /* (prvdrnum) -> prvdr_zip */

    use "${data}/ntwk/aha-2009-to-2022_v2024.1.0.dta", clear
    gen str6 prvdrnum = string(provider_id, "%06.0f")
    gen prvdr_zip = substr(mloczip_2019, 1, 5)

    keep prvdrnum prvdr_zip
    drop if prvdr_zip == ""

    tempfile prvdr_zips
    save `prvdr_zips'


    ***************************************************************************
    * GET CLAIMS
    ***************************************************************************
    local start = 2011
    local end   = 2019

    local files
    forvalues year = `start'/`end' {
        di "`year'"

        * load data
        use "${data}/ntwk/medpar`year'_100pct.dta", clear

        * data cleaning
        drop plan
        gen plan = cntrct + "-" + pbpid         /* regenerate plan var */
        drop if pbpid == ""                     /* drop if pbpid is empty */
        merge m:1 plan using "${data}/ntwk/snp_plans.dta", keep(1 3) gen(_snp)
                                                /* merge in SNP plan info */
        gen ssacounty = state_cd + cnty_cd
        gen bene_ssa = ssacounty
                                                /* get bene ssa counties */
        merge m:1 ssacounty using "${data}/ssa_fips/ssa_fips_st_cty2019.dta", nogen keep(3)
        gen bene_fips = fipscounty
                                                /* get bene fips counties*/
        gen fips_code = bene_fips
        merge m:1 fips_code using "${data}/ntwk/localized_breadth_John.dta", nogen keep(3)
                                                /* get localized breadth */
        drop ssacounty fipscounty fips_code     
                                                /* drop redudant stuff */
        gen zipcode = bene_zip
        merge m:1 zipcode using "${data}/ntwk/zip_coords.dta", keep(3) gen(_bene_coord) keepusing(intptlat intptlong)
        rename intptlat bene_lat
        rename intptlong bene_long
        drop zipcode
                                                /* get bene coordinates */

        if `year' < 2019 {
            merge m:1 year plan bene_fips using `map_desc', nogen keep(3) keepusing(plan_desc)
        }
                                                /* merge in plan descendant */

        *replace plan = plan_desc if year != 2019
        *merge m:1 plan using `ntwks', nogen keep(3) keepusing(network_id)
                                                /* merge in network_id */

        * subsetting
        keep if mMA == 1                        /* keep modal MA benes only */
        keep if er_amt == 0                     /* keep non-emergencies only */
        *keep if snp_plan == "No"                /* keep non-SNP plans only */

        /*
        keep if mean_denominator >= 2
        keep if mean_breadth >= `lbreadth' & mean_breadth <= `ubreadth'
        */

        * finalize
        tempfile `year'
        save "``year''"
        local files `files' "``year''"

    }

    clear
    append using `files'

    replace plan_desc = plan if year == 2019
    merge m:1 plan_desc using `ntwks', nogen keep(3) keepusing(network_id)
                                                /* merge in network_id */

    tempfile clms
    save `clms'

    ***************************************************************************
    * GET THE CHOICE SETS
    ***************************************************************************

    * provider condition #1: must observe hospital every year
    ***************************************************************************
    use `clms', clear
    keep prvdrnum year
    gduplicates drop

    gen stub = 1
    reshape wide stub, i(prvdrnum) j(year)
    egen cnt = rowtotal(stub*)
    keep if cnt == 9

    keep prvdrnum
    tempfile prvdr_obs_every_year
    save `prvdr_obs_every_year'

    * provider condition #2: >50 unique visits for each bene_fips
    ***************************************************************************
    use `clms', clear
    egen tag = tag(bene_id bene_fips prvdrnum)
    collapse (sum) tag, by(bene_fips prvdrnum)
    keep if tag >= `thresh'

    tempfile prvdr_unique_visits
    save `prvdr_unique_visits'

    * get final choice set
    ***************************************************************************
    use "${data}/ntwk/choice_sets_long.dta", clear

    merge m:1 prvdrnum using `prvdr_obs_every_year', nogen keep(3)
        /* apply condition #1: must observe hospital every year */
    merge 1:1 bene_fips prvdrnum using `prvdr_unique_visits', nogen keep(3)
    drop tag
        /* apply condition #2: >50 unique visits for each bene_fips */

    merge m:1 prvdrnum using `prvdr_zips', nogen keep(3) keepusing(prvdr_zip)
    gen zipcode = prvdr_zip
    merge m:1 zipcode using "${data}/ntwk/zip_coords.dta", nogen keep(3) keepusing(intptlat intptlong)
    rename intptlat prvdr_lat
    rename intptlong prvdr_long
    drop zipcode
        /* apply condition #3: valid zip & coordinates */
    
    gen tocnt = 1
    egen choice_cnt = sum(tocnt), by(bene_fips)
    drop if choice_cnt == 1
    drop tocnt choice_cnt
        /* apply condition #4: drop bene_fips with only one choice */

    gisid bene_fips prvdrnum
    sort bene_fips prvdrnum
    order bene_fips prvdrnum

    tempfile choices_long
    save `choices_long'


    ***************************************************************************
    * GET THE CHOICES
    ***********************************************************************
    use `clms', clear
    merge m:1 bene_fips prvdrnum using `choices_long', nogen keep(3) keepusing()

    keep bene_id year prvdrnum
    gduplicates drop

    tempfile choices_all
    save `choices_all'

    use `clms', clear
    merge m:1 bene_fips prvdrnum using `choices_long', nogen keep(3) keepusing()

    bysort bene_id year (admsndt): gen rank = _n
    keep if rank == 1
    keep bene_id year prvdrnum
    gisid bene_id year

    tempfile choices_first
    save `choices_first'


    ***************************************************************************
    * CONSTRUCT ANALYSIS SAMPLE
    ***********************************************************************
    
    * benes to keep
    use `clms', clear
    merge m:1 bene_fips prvdrnum using `choices_long', nogen keep(3)

    keep bene_id year admsndt plan cntrct pbpid network_id bene_zip bene_ssa bene_fips bene_lat bene_long
    bysort bene_id year (admsndt): gen rank = _n
    keep if rank == 1
    drop rank
    
    tempfile bene_years
    save `bene_years'

    keep bene_id
    gduplicates drop

    tempfile benes
    save `benes'

    * get past visits
    use "${data}/ntwk/medpar_visits_100pct.dta", clear
    merge m:1 bene_id using `benes', nogen keep(3) keepusing()

    tempfile relevant_visits
    save `relevant_visits'

    keep bene_id prvdrnum
    gduplicates drop
    expand 20
    bysort bene_id prvdrnum: gen year = 1999 + _n
    merge 1:1 bene_id prvdrnum year using `relevant_visits', gen(_visits) keep(1 3)
    gen visit = (_visits == 3)

    bysort bene_id prvdrnum (year): gen visited = sum(visit[_n-1])
    replace visited = 1 if visited > 1

    keep if year >= 2011
    keep bene_id prvdrnum year visited
    gisid bene_id prvdrnum year

    tempfile past_visits
    save `past_visits'

    * put it all together
    use `choices_long', clear

    bysort bene_fips: gen rank = _n
    reshape wide prvdrnum prvdr_zip prvdr_lat prvdr_long, i(bene_fips) j(rank)

    merge 1:m bene_fips using `bene_years', nogen keep(3)
    greshape long prvdrnum prvdr_zip prvdr_lat prvdr_long, i(bene_id year) j(j) fast
    drop if missing(prvdrnum)
    drop j

    * merge in choices
    merge 1:1 bene_id year prvdrnum using `choices_first', keep(1 3) gen(_choices_first) keepusing()
    gen chosen_first = (_choices_first == 3)

    merge 1:1 bene_id year prvdrnum using `choices_all', keep(1 3) gen(_choices_all) keepusing()
    gen chosen_all = (_choices_all == 3)

    drop _*

    * get distance
    geodist bene_lat bene_long prvdr_lat prvdr_long, gen(dist) miles
    gen dist_sq = dist^2

    * merge in network indicator
    merge m:1 network_id prvdrnum using `map_ntwk_status', keep(1 3) gen(_vericred)
    gen in_ntwk = 0
    replace in_ntwk = 1 if _vericred == 3
    label var in_ntwk "In-network"

    * merge in past visits
    merge 1:1 bene_id prvdrnum year using `past_visits', nogen keep(1 3)
    replace visited = 0 if visited == .

    * get provider fixed effects
    merge m:1 bene_fips prvdrnum using `prvdr_unique_visits', nogen keep(1 3)
    egen prvdr_fe = group(bene_fips prvdrnum)
    gen neg_tag = -tag 
    bysort bene_id year (neg_tag): gen size_rank = _n
    replace prvdr_fe = 9999 if size_rank == 1

    * finalize
    gen beneXyear = bene_id + string(year) 
    sort bene_id year prvdrnum
    order bene_id year prvdrnum plan

end
