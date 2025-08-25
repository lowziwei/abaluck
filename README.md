#### GENERAL
* data_check.py > check 1% and 100 rows of any tables in MarketScan

#### TASK: MATCH PRESCRIPTION-PHYSICIAN
* subset.py > search years where semaglutide NDCNUM exists 
* refill_check.py > check truly new prescriptions in table D (temporarily just for 2018)
* npi_overlap_check.py > check npi overlap with provid to gen unique phys_id variable for prescription-physician matching 
* gen_phys_id.py >  1. delete provid where there are multiple npis for a provid, 2. delete provid when there are multiple provid for a npi, 3. gen a phys_id variable where its the same as npi but uses provid value when not available. 

###### 1pp Scripts
* sample_merge8.py > import from truly new prescriptions and merge with +/-30 day window for possible NPIs. Exports frequency of unique phys id count and for each prescription events, number of unique phys id associated.
* histogram_uniq_phys.py > creates a histogram illustrating distribution of unique phys id
* events_combined.py > subsetted dataset that shows all prescription events with unique_phys_id == 1 and the associated phys_id
* match_phys_id_ndcnum.py > associate ndcnum from file d with output of events_combined.py and collapse based on enrolid, svcdate, phys_id, d_semaglutide
* gen_phys_ndcnum_df.py > for each phys_id, svcdate pair find total prescription, total patient, fraction of prescriptions that are semaglutide, fraction of patients receiving semaglutide
* ccae_adoption.py > find top 100 NPIs and find total prescription and average fraction of patients per month

##### dataframes
* prescription_events_YYYY_PP_with_ndcnum.parquet > pairs per prescription event that has unique_phys_id count == 1, with phys_id and ndcnum info, and a dummy for semaglutide
* phys_id_ndcnum.parquet > for each phys_id, svcdate pair, show total patients, total prescriptions, fraction of prescriptions that are semaglutide, fraction of patients receiving semaglutide
* physician_monthly_semaglutide_analysis.parquet > same as phys_id_ndcnum.parquet but collapsed monthly

