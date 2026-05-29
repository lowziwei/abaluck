Episode construction:
* define_k-band.py: Builds clinical episodes from MarketScan Medicare outpatient claims by linking diagnosis-matched visits within a rolling 100-day window, restricted to Level-1 (5-digit) CPT codes merged against per-year RVU files. Prices each episode in Work/MP RVUs and dollars, flags CPT composition and 99213/99214 "k-band" thresholds, and outputs claim-level and episode-level datasets plus a cross-year summary.

* episode-file-derivation.py: Pools the per-year episode files into a single 2014–2018 table, deriving work/MP and full RVU measures, dollar equivalents, and COB-inclusive/exclusive spending totals for analysis.

CRRA stuff: 
