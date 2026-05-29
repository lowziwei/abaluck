Most relevant files are define k-band.py and episode-file-derivation.py 

* define_k-band.py: Builds clinical episodes from MarketScan Medicare outpatient claims by linking diagnosis-matched visits within a rolling 100-day window, restricted to Level-1 (5-digit) CPT codes merged against per-year RVU files. Prices each episode in Work/MP RVUs and dollars, flags CPT composition and 99213/99214 "k-band" thresholds, and outputs claim-level and episode-level datasets plus a cross-year summary.

* episode-file-derivation.py 
