#### GENERAL
* data_check.py > check 1% and 100 rows of any tables in MarketScan

#### TASK - MATCH PRESCRIPTION <> PHYSICIAN
* subset.py > search years where semaglutide NDCNUM exists 
* refill_check.py > check truly new prescriptions in table D (temporarily just for 2018)
* npi_overlap_check.py > check npi overlap with provid to gen unique phys_id variable for prescription <> physician matching 

