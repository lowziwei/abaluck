
* **refill_check.py** > This script identifies truly new drug initiations by checking REFILL=0 prescriptions against historical records (2014-present), flagging each based on prior drug history: FLAG=NULL indicates no prior history, FLAG=0 indicates same-year duplicates, and negative flags indicate years since last prescription. 
* **sample_merge9.py** > This script identifies truly new prescriptions (FLAG = NULL) and matches each to the closest physician visit(s) in time, calculating unique physician counts and temporal distances, then outputs histogram summaries and detailed prescription event files for downstream analysis.
* **merge_ndcnum.py** > This script filters prescription events to single-physician visits within 60 days, enriches them with NDC drug codes, and flags semaglutide prescriptions, outputting prescription_events_YYYY_##_with_ndcnum.parquet files.
* 
