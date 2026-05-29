Episode construction:
* define_k-band.py: Builds clinical episodes from MarketScan Medicare outpatient claims by linking diagnosis-matched visits within a rolling 100-day window, restricted to Level-1 (5-digit) CPT codes merged against per-year RVU files. Prices each episode in Work/MP RVUs and dollars, flags CPT composition and 99213/99214 "k-band" thresholds, and outputs claim-level and episode-level datasets plus a cross-year summary.

* episode-file-derivation.py: Pools the per-year episode files into a single 2014–2018 table, deriving work/MP and full RVU measures, dollar equivalents, and COB-inclusive/exclusive spending totals for analysis.

CRRA stuff: 
* physician-episode-df-construction.py: Builds physician–patient and physician–patient–episode expenditure datasets (A and B) from MarketScan Medicare claims for a fixed 50k-NPI sample, with parallel chunked pulls and an A-vs-B reconciliation check.
* physician-episode-df-construction_deid.py: Orchestrates the full pipeline end to end: builds Datasets A/B (Stage 1), renumbers episode IDs sequentially within each physician–patient pair across years (Stage 2), and de-identifies NPIs for export (Stage 3), selectable via --stage.
