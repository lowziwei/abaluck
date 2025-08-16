#### GENERAL
* data_check.py > check 1% and 100 rows of any tables in MarketScan

#### TASK: MATCH PRESCRIPTION-PHYSICIAN
* subset.py > search years where semaglutide NDCNUM exists 
* refill_check.py > check truly new prescriptions in table D (temporarily just for 2018)
* npi_overlap_check.py > check npi overlap with provid to gen unique phys_id variable for prescription-physician matching 
* gen_phys_id.py >  1. delete provid where there are multiple npis for a provid, 2. delete provid when there are multiple provid for a npi, 3. gen a phys_id variable where its the same as npi but uses provid value when not available. 

###### 1pp Scripts
* sample_merge8.py > import from truly new prescriptions and merge with +/-30 day window for possible NPIs. Exports frequency of unique phys id count and for each prescription events, number of unique phys id associated.
* histogram_uniq_phys.py > creates two histograms illustrating distribution of unique phys id
* 
