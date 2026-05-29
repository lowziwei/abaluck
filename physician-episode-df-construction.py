import duckdb
import pandas as pd
import numpy as np
import os
import time
import psutil
import gc
from multiprocessing import Pool, cpu_count
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# SETTINGS
# =============================================================================
DATASET_TYPE   = "MEDICARE_SET_A"
DATABASE       = "MDCR"
TABLE_CODE     = "O"
YEARS          = [str(y) for y in range(2015, 2025)]  # 2015-2024 (2014: NPI entirely null)

# Episode definition
DX_DIGITS        = 3
TIME_WINDOW_DAYS = 100

# Sampling: 50k NPIs sampled ONCE from union across all years, fixed for all years
NPI_SAMPLE_SIZE  = 50000   # Set to None for all NPIs

# Chunking: number of NPIs per parallel worker
NPI_CHUNK_SIZE   = 500

# Parallel workers
N_PROCESSES = min(cpu_count() - 1, 4)

# RVU file path — XX replaced with 2-digit year suffix
RVU_BASE_PATH = '/home/zl749/rvu_PPRRVUXX_partD.csv'

# Output directories
home_dir   = os.path.expanduser('~')
output_dir = os.path.join(home_dir, 'episode_outputs')
temp_dir   = os.path.join(home_dir, 'duckdb_temp')

os.makedirs(output_dir, exist_ok=True)
os.makedirs(temp_dir, exist_ok=True)

np.random.seed(42)

# Place of service classification for PE RVU actualization
FACILITY_POS     = {2, 19, 21, 22, 23, 24, 26, 31, 34, 41, 42, 51, 52, 53, 56, 61}
NON_FACILITY_POS = {1, 3, 4, 11, 12, 13, 14, 15, 16, 17, 20, 25, 32, 33, 49,
                    50, 54, 55, 57, 60, 62, 65, 71, 72, 81, 99}

print("=" * 100)
print("EPISODE EXPENDITURE ANALYSIS — PHYSICIAN-PATIENT LEVEL")
print("=" * 100)
print(f"Years: {YEARS[0]}–{YEARS[-1]} | Processes: {N_PROCESSES}")
print(f"NPI sample: {NPI_SAMPLE_SIZE:,} drawn ONCE from union across all years" if NPI_SAMPLE_SIZE
      else "NPI sample: ALL NPIs")
print(f"Episode definition: {DX_DIGITS}-digit DX codes, {TIME_WINDOW_DAYS}-day window")
print(f"Revenue  = COB + NETPAY + COINS + COPAY + DEDUCT  (+ components saved separately)")
print(f"m        = (WORK_RVU + MP_RVU) * 98               (+ components saved separately)")
print("=" * 100)


# =============================================================================
# STEP 0 — SAMPLE NPIs ONCE ACROSS ALL YEARS
# Uses DuckDB UNION ALL across all parquets to get the full NPI universe,
# then draws one fixed random sample used for every year.
# =============================================================================

def sample_npis_once():
    """
    Query DISTINCT NPI from the union of all year parquets in one DuckDB call.
    Returns a numpy array of NPI strings — the fixed sample used for all years.
    """
    print("\n" + "=" * 100)
    print("SAMPLING NPIs — union across all years")
    print("=" * 100)

    file_paths = []
    for year in YEARS:
        fp = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_update_{year}.parquet"
        if os.path.exists(fp):
            file_paths.append(fp)
        else:
            print(f"  WARNING: {fp} not found — excluded from NPI union")

    if not file_paths:
        raise FileNotFoundError("No parquet files found for any year.")

    union_sql = " UNION ALL ".join(
        f"SELECT CAST(NPI AS VARCHAR) AS NPI FROM '{fp}' WHERE NPI IS NOT NULL AND DATATYP = 3"
        for fp in file_paths
    )

    conn = duckdb.connect()
    conn.execute(f"SET temp_directory='{temp_dir}'")
    conn.execute("SET memory_limit='8GB'")
    conn.execute(f"SET threads={max(1, cpu_count() - 1)}")

    print(f"  Querying DISTINCT NPI across {len(file_paths)} year files...")
    t0 = time.time()

    all_npis = conn.execute(f"""
        SELECT DISTINCT NPI
        FROM ({union_sql})
        WHERE NPI IS NOT NULL AND TRIM(NPI) != ''
    """).df()['NPI'].str.strip().values

    conn.close()

    print(f"  Found {len(all_npis):,} unique NPIs across all years ({time.time()-t0:.1f}s)")

    if NPI_SAMPLE_SIZE and len(all_npis) > NPI_SAMPLE_SIZE:
        np.random.seed(42)
        sampled = np.random.choice(all_npis, size=NPI_SAMPLE_SIZE, replace=False)
        print(f"  Sampled {NPI_SAMPLE_SIZE:,} NPIs (seed=42, fixed for ALL years)")
    else:
        sampled = all_npis
        print(f"  Using all {len(sampled):,} NPIs (no sampling needed)")

    return sampled


# =============================================================================
# RVU LOADING — pandas read_csv, matching reference code approach
# Renames HCPS -> PROC1 for merging with MDCR PROC1 column.
# =============================================================================

def load_rvu_data(year):
    """
    Load RVU CSV for a specific year into a pandas DataFrame.
    Follows the same pattern as the reference code:
      - read_csv with latin-1 encoding
      - rename HCPS -> PROC1
      - keep only needed columns
      - deduplicate on PROC1
    Returns None if file not found.
    """
    year_suffix = str(year)[2:]
    rvu_path = RVU_BASE_PATH.replace('XX', year_suffix)

    try:
        rvu_df = pd.read_csv(rvu_path, encoding='latin-1')

        # Rename HCPS -> PROC1 to match MDCR column name
        if 'HCPS' in rvu_df.columns:
            rvu_df = rvu_df.rename(columns={'HCPS': 'PROC1'})

        if 'PROC1' not in rvu_df.columns:
            raise ValueError(
                f"Neither HCPS nor PROC1 found in RVU columns: {rvu_df.columns.tolist()}"
            )

        rvu_df['PROC1'] = rvu_df['PROC1'].astype(str).str.strip()

        # Keep only the columns we need
        keep_cols = ['PROC1', 'WORK_RVU', 'MP_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU']
        keep_cols = [c for c in keep_cols if c in rvu_df.columns]
        rvu_df = rvu_df[keep_cols].copy()

        # Convert RVU values to numeric
        for col in keep_cols:
            if col != 'PROC1':
                rvu_df[col] = pd.to_numeric(rvu_df[col], errors='coerce').fillna(0)

        # Deduplicate — keep first occurrence per procedure code
        rvu_df = rvu_df.drop_duplicates(subset='PROC1', keep='first')

        print(f"  [{year}] RVU data: {len(rvu_df):,} unique procedure codes")
        return rvu_df

    except FileNotFoundError:
        print(f"  [{year}] WARNING: RVU file not found: {rvu_path}")
        return None
    except Exception as e:
        print(f"  [{year}] ERROR loading RVU data: {e}")
        return None


# =============================================================================
# EPISODE ASSIGNMENT — (NPI, ENROLID) level
# Runs on the full concatenated dataframe after all chunks return.
# =============================================================================

def define_episodes_npi_patient(df, dx_digits=3, time_window_days=100):
    """
    Assign episode IDs grouped by (NPI, ENROLID).

    For each physician-patient pair, claims are sorted by SVCDATE and grouped
    into episodes using a rolling time window + diagnosis overlap rule:
      - A new episode starts when no active episode shares a diagnosis code
        with the current claim, or all active episodes have expired
        (> time_window_days since last claim in that episode).
      - Claims with no diagnosis codes (DX1-DX4 all null) cannot be matched
        to any episode: episode_id=0, is_residual=1.
      - Real episodes get a globally unique positive integer episode_id
        within the year (year offset added in process_year to avoid
        cross-year collisions when years are stacked into a panel).

    Returns df with new columns: episode_id (int), is_residual (int 0/1).
    """
    if len(df) == 0:
        df['episode_id'] = 0
        df['is_residual'] = 1
        return df

    df = df.sort_values(['NPI', 'ENROLID', 'SVCDATE']).reset_index(drop=True)

    # Clean and truncate diagnosis codes
    for col in ['DX1', 'DX2', 'DX3', 'DX4']:
        df[col] = df[col].astype(str).replace(
            ['', 'nan', 'None', 'Non', 'none', ' ', 'nat', 'NaN', 'NaT'], np.nan
        )
        df[col] = df[col].str[:dx_digits].replace(['', 'nan', 'Non', ' ', 'NaN'], np.nan)

    df['episode_id'] = 0
    df['is_residual'] = 0
    current_episode = 0

    for (npi, enrolid), group_idx in df.groupby(['NPI', 'ENROLID'], sort=False).groups.items():
        indices   = list(group_idx)
        first_idx = indices[0]
        first_dx  = set(df.loc[first_idx, ['DX1', 'DX2', 'DX3', 'DX4']].dropna().values)

        if len(first_dx) == 0:
            df.loc[first_idx, ['episode_id', 'is_residual']] = [0, 1]
        else:
            current_episode += 1
            df.loc[first_idx, ['episode_id', 'is_residual']] = [current_episode, 0]

        active_episodes = {}
        if first_dx:
            active_episodes[current_episode] = {
                'date': df.loc[first_idx, 'SVCDATE'],
                'dx_codes': first_dx.copy()
            }

        for idx in indices[1:]:
            visit_date = df.loc[idx, 'SVCDATE']
            visit_dx   = set(df.loc[idx, ['DX1', 'DX2', 'DX3', 'DX4']].dropna().values)

            if len(visit_dx) == 0:
                df.loc[idx, ['episode_id', 'is_residual']] = [0, 1]
                continue

            # Expire episodes outside time window
            for ep_id in [e for e, d in active_episodes.items()
                          if (visit_date - d['date']).days > time_window_days]:
                del active_episodes[ep_id]

            # Match to active episode by diagnosis overlap
            matched = next(
                (ep_id for ep_id, ep_data in active_episodes.items()
                 if visit_dx & ep_data['dx_codes']),
                None
            )

            if matched is not None:
                df.loc[idx, ['episode_id', 'is_residual']] = [matched, 0]
                active_episodes[matched]['date'] = visit_date
                active_episodes[matched]['dx_codes'].update(visit_dx)
            else:
                current_episode += 1
                df.loc[idx, ['episode_id', 'is_residual']] = [current_episode, 0]
                active_episodes[current_episode] = {
                    'date': visit_date, 'dx_codes': visit_dx.copy()
                }

    return df


# =============================================================================
# CHUNK PROCESSING (PARALLEL WORKERS)
# Each worker:
#   1. Uses DuckDB to pull claims for its NPI chunk from the parquet
#   2. Does type conversion in pandas
#   3. Merges RVU data via pandas LEFT JOIN (matching reference code approach)
#   4. Computes revenue, m_estimate, PE_RVU_actualized in pandas
#   5. Returns clean claim-level df — episode assignment deferred to main process
# =============================================================================

def process_npi_chunk(args):
    """
    Pull and preprocess claims for one NPI chunk.
    DuckDB used only for the fast parquet pull.
    All transformations (RVU merge, revenue, m_estimate) done in pandas,
    matching the reference code approach.
    """
    npi_chunk, year, chunk_idx, total_chunks, file_path, rvu_df = args

    try:
        # --- Pull claims via DuckDB ---
        conn = duckdb.connect()
        conn.execute(f"SET temp_directory='{temp_dir}'")
        conn.execute("SET memory_limit='3GB'")
        conn.execute("SET threads=2")

        year_int = int(year)
        medadv_clause = (
            "AND (MEDADV = 0 OR MEDADV IS NULL)"
            if year_int >= 2020 else ""
        )

        npi_list = ','.join(f"'{str(n)}'" for n in npi_chunk)

        chunk_df = conn.execute(f"""
            SELECT
                CAST(NPI     AS VARCHAR) AS NPI,
                CAST(ENROLID AS VARCHAR) AS ENROLID,
                SVCDATE,
                DX1, DX2, DX3, DX4,
                CAST(PROC1   AS VARCHAR) AS PROC1,
                STDPLAC,
                COALESCE(CAST(COB    AS DOUBLE), 0) AS COB,
                COALESCE(CAST(NETPAY AS DOUBLE), 0) AS NETPAY,
                COALESCE(CAST(COINS  AS DOUBLE), 0) AS COINS,
                COALESCE(CAST(COPAY  AS DOUBLE), 0) AS COPAY,
                COALESCE(CAST(DEDUCT AS DOUBLE), 0) AS DEDUCT
            FROM '{file_path}'
            WHERE CAST(NPI AS VARCHAR) IN ({npi_list})
              AND SVCDATE IS NOT NULL
              AND NPI     IS NOT NULL
              AND DATATYP = 3
              {medadv_clause}
        """).df()

        conn.close()

        if len(chunk_df) == 0:
            return None, chunk_idx

        # --- Type conversions ---
        chunk_df['SVCDATE'] = pd.to_datetime(chunk_df['SVCDATE'], errors='coerce')
        chunk_df = chunk_df.dropna(subset=['SVCDATE'])
        chunk_df['NPI']     = chunk_df['NPI'].str.strip()
        chunk_df['ENROLID'] = chunk_df['ENROLID'].str.strip()
        chunk_df['PROC1']   = chunk_df['PROC1'].str.strip()

        # --- Revenue composite ---
        chunk_df['revenue'] = (
            chunk_df['COB']    + chunk_df['NETPAY'] +
            chunk_df['COINS']  + chunk_df['COPAY']  + chunk_df['DEDUCT']
        )

        # --- RVU merge via pandas LEFT JOIN ---
        # Matches reference code: HCPS already renamed to PROC1 in load_rvu_data()
        # LEFT join keeps all claims; unmatched get RVU = 0 (contribute revenue, not m)
        if rvu_df is not None:
            chunk_df = chunk_df.merge(rvu_df, on='PROC1', how='left')
            for col in ['WORK_RVU', 'MP_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU']:
                if col in chunk_df.columns:
                    chunk_df[col] = chunk_df[col].fillna(0)
        else:
            for col in ['WORK_RVU', 'MP_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU']:
                chunk_df[col] = 0.0

        # --- m_estimate = (WORK_RVU + MP_RVU) * 98 ---
        chunk_df['m_estimate'] = (chunk_df['WORK_RVU'] + chunk_df['MP_RVU']) * 98

        # --- PE_RVU_actualized based on STDPLAC ---
        chunk_df['PE_RVU_actualized'] = np.nan
        if 'STDPLAC' in chunk_df.columns:
            fac_mask     = chunk_df['STDPLAC'].isin(FACILITY_POS)
            nonfac_mask  = chunk_df['STDPLAC'].isin(NON_FACILITY_POS)
            neither_mask = ~fac_mask & ~nonfac_mask & chunk_df['STDPLAC'].notna()

            chunk_df.loc[fac_mask,     'PE_RVU_actualized'] = chunk_df.loc[fac_mask,     'FACILITY_PE_RVU']
            chunk_df.loc[nonfac_mask,  'PE_RVU_actualized'] = chunk_df.loc[nonfac_mask,  'NON-FAC_PE_RVU']
            chunk_df.loc[neither_mask, 'PE_RVU_actualized'] = (
                chunk_df.loc[neither_mask, 'FACILITY_PE_RVU'] +
                chunk_df.loc[neither_mask, 'NON-FAC_PE_RVU']
            ) / 2.0
        chunk_df['PE_RVU_actualized'] = chunk_df['PE_RVU_actualized'].fillna(0)

        return chunk_df, chunk_idx

    except Exception as e:
        print(f"[{year}] Error in chunk {chunk_idx}: {e}")
        import traceback
        traceback.print_exc()
        return None, chunk_idx


# =============================================================================
# YEAR-LEVEL PROCESSING
# =============================================================================

def process_year(year, sampled_npis):
    """
    Full pipeline for one year given a fixed set of sampled NPIs:
      1. Load RVU data for this year via pandas (reference code approach).
      2. Pull + preprocess claims in parallel NPI chunks.
      3. Concatenate all chunks.
      4. Episode assignment on full df grouped by (NPI, ENROLID).
         Episode IDs prefixed with year*1e8 to avoid cross-year collisions.
      5. Aggregate to Dataset A (NPI, ENROLID) and Dataset B (NPI, ENROLID, episode)
         using DuckDB for fast groupby.
      6. Reconciliation check: sum(B) == A for every (NPI, ENROLID).
      7. Save both datasets as CSV.
    """
    start_time = time.time()
    process    = psutil.Process()
    print(f"\n[{year}] Starting | Memory: {process.memory_info().rss / 1e9:.2f} GB")

    file_path = (
        f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_update_{year}.parquet"
    )
    if not os.path.exists(file_path):
        print(f"[{year}] ERROR: File not found — skipping")
        return

    # Load RVU data once for this year — passed to all workers
    rvu_df = load_rvu_data(year)

    # -------------------------------------------------------------------------
    # STEP 1: Pull claims in parallel NPI chunks
    # -------------------------------------------------------------------------
    chunks = [
        sampled_npis[i:i + NPI_CHUNK_SIZE]
        for i in range(0, len(sampled_npis), NPI_CHUNK_SIZE)
    ]
    total_chunks = len(chunks)
    print(f"[{year}] {len(sampled_npis):,} NPIs → {total_chunks:,} chunks | "
          f"{N_PROCESSES} workers")

    chunk_args = [
        (chunk, year, idx + 1, total_chunks, file_path, rvu_df)
        for idx, chunk in enumerate(chunks)
    ]

    all_claims = []
    with Pool(processes=N_PROCESSES) as pool:
        for claim_df, chunk_idx in pool.imap(process_npi_chunk, chunk_args):
            if claim_df is not None and len(claim_df) > 0:
                all_claims.append(claim_df)

            if chunk_idx % 25 == 0:
                mem     = process.memory_info().rss / 1e9
                elapsed = time.time() - start_time
                rate    = chunk_idx / elapsed * 60 if elapsed > 0 else 0
                eta     = (total_chunks - chunk_idx) / rate if rate > 0 else 0
                n_so_far = sum(len(d) for d in all_claims)
                print(f"[{year}] {chunk_idx}/{total_chunks} | "
                      f"claims: {n_so_far:,} | "
                      f"mem: {mem:.1f}GB | {rate:.1f} chunks/min | ETA: {eta:.1f}m")

    if not all_claims:
        print(f"[{year}] No claims collected — skipping")
        return

    claims_df = pd.concat(all_claims, ignore_index=True)
    del all_claims
    gc.collect()

    print(f"\n[{year}] Claims:   {len(claims_df):,}")
    print(f"[{year}] NPIs:     {claims_df['NPI'].nunique():,}")
    print(f"[{year}] Patients: {claims_df['ENROLID'].nunique():,}")

    # -------------------------------------------------------------------------
    # STEP 2: Episode assignment on full combined dataframe
    #
    # Runs here (not in workers) so every (NPI, ENROLID) pair has all its
    # claims present and sorted together before episode logic runs.
    #
    # Episode IDs made year-specific: year * 100_000_000 + counter.
    # e.g. episode 47 in 2017 → 201700000047, distinct from 201800000047.
    # Residual claims (no DX codes) stay at episode_id = 0.
    # -------------------------------------------------------------------------
    print(f"\n[{year}] Episode assignment...")
    t0 = time.time()
    claims_df = define_episodes_npi_patient(claims_df, DX_DIGITS, TIME_WINDOW_DAYS)

    year_offset = int(year) * 100_000_000
    nonresidual = claims_df['is_residual'] == 0
    claims_df.loc[nonresidual, 'episode_id'] = (
        claims_df.loc[nonresidual, 'episode_id'] + year_offset
    )

    n_residual = claims_df['is_residual'].sum()
    n_episodes = claims_df.loc[nonresidual, 'episode_id'].nunique()
    print(f"[{year}] Done in {(time.time()-t0)/60:.1f} min | "
          f"episodes: {n_episodes:,} | residual claims: {n_residual:,} "
          f"({n_residual/len(claims_df)*100:.1f}%)")

    # -------------------------------------------------------------------------
    # STEP 3: Aggregate using DuckDB (faster than pandas groupby at this scale)
    # -------------------------------------------------------------------------
    conn = duckdb.connect()
    conn.register('claims', claims_df)

    agg_expr = """
        SUM(revenue)           AS revenue,
        SUM(COB)               AS COB,
        SUM(NETPAY)            AS NETPAY,
        SUM(COINS)             AS COINS,
        SUM(COPAY)             AS COPAY,
        SUM(DEDUCT)            AS DEDUCT,
        SUM(m_estimate)        AS m_estimate,
        SUM(WORK_RVU)          AS WORK_RVU,
        SUM(MP_RVU)            AS MP_RVU,
        SUM(PE_RVU_actualized) AS PE_RVU_actualized,
        SUM(FACILITY_PE_RVU)   AS FACILITY_PE_RVU,
        SUM("NON-FAC_PE_RVU")  AS NONFAC_PE_RVU,
        COUNT(*)               AS n_claims
    """

    dataset_a = conn.execute(f"""
        SELECT NPI, ENROLID, {agg_expr}
        FROM claims
        GROUP BY NPI, ENROLID
    """).df()
    dataset_a['year'] = int(year)

    dataset_b = conn.execute(f"""
        SELECT NPI, ENROLID, episode_id, is_residual, {agg_expr}
        FROM claims
        GROUP BY NPI, ENROLID, episode_id, is_residual
    """).df()
    dataset_b['year'] = int(year)

    conn.unregister('claims')
    conn.close()

    print(f"\n[{year}] Dataset A: {len(dataset_a):,} (NPI, patient) pairs")
    print(f"  Total revenue:    ${dataset_a['revenue'].sum():,.2f}")
    print(f"  Mean revenue:     ${dataset_a['revenue'].mean():,.2f}")
    print(f"  Total m_estimate: {dataset_a['m_estimate'].sum():,.2f}")
    print(f"\n[{year}] Dataset B: {len(dataset_b):,} (NPI, patient, episode) rows")
    print(f"  Real episodes: {(dataset_b['is_residual']==0).sum():,}")
    print(f"  Residual rows: {(dataset_b['is_residual']==1).sum():,}")

    # -------------------------------------------------------------------------
    # STEP 4: Reconciliation check
    # -------------------------------------------------------------------------
    conn2 = duckdb.connect()
    conn2.register('a', dataset_a)
    conn2.register('b', dataset_b)

    reconcile = conn2.execute("""
        SELECT
            MAX(ABS(a.revenue    - b_sum.rev_b)) AS max_rev_diff,
            MAX(ABS(a.m_estimate - b_sum.m_b))   AS max_m_diff,
            SUM(CASE WHEN b_sum.rev_b IS NULL THEN 1 ELSE 0 END) AS n_missing_in_b
        FROM a
        LEFT JOIN (
            SELECT NPI, ENROLID,
                   SUM(revenue)    AS rev_b,
                   SUM(m_estimate) AS m_b
            FROM b GROUP BY NPI, ENROLID
        ) b_sum USING (NPI, ENROLID)
    """).df()

    conn2.close()

    max_rev_diff = reconcile['max_rev_diff'].iloc[0]
    max_m_diff   = reconcile['max_m_diff'].iloc[0]
    n_missing    = reconcile['n_missing_in_b'].iloc[0]

    print(f"\n[{year}] RECONCILIATION CHECK:")
    print(f"  Max revenue diff:    ${max_rev_diff:.6f}")
    print(f"  Max m_estimate diff: {max_m_diff:.6f}")
    print(f"  Missing in B:        {n_missing}")
    if max_rev_diff < 0.01 and max_m_diff < 0.01 and n_missing == 0:
        print(f"  ✓ A and B reconcile perfectly")
    else:
        print(f"  ✗ WARNING: discrepancy detected")

    # -------------------------------------------------------------------------
    # STEP 5: Save
    # -------------------------------------------------------------------------
    npi_suffix = f"_npi{NPI_SAMPLE_SIZE}" if NPI_SAMPLE_SIZE else "_all_npis"
    path_a = os.path.join(output_dir, f'dataset_A_physician_patient_{year}{npi_suffix}.csv')
    path_b = os.path.join(output_dir, f'dataset_B_physician_patient_episode_{year}{npi_suffix}.csv')

    dataset_a.to_csv(path_a, index=False)
    dataset_b.to_csv(path_b, index=False)

    elapsed = (time.time() - start_time) / 60
    print(f"\n[{year}] ✓ COMPLETE in {elapsed:.1f} min")
    print(f"  A: {path_a} ({os.path.getsize(path_a)/1e6:.1f} MB)")
    print(f"  B: {path_b} ({os.path.getsize(path_b)/1e6:.1f} MB)")

    del claims_df, dataset_a, dataset_b
    gc.collect()


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    overall_start = time.time()

    # Sample NPIs once from the union of all years — same set for every year
    sampled_npis = sample_npis_once()

    # Save the NPI list for reproducibility
    npi_suffix    = f"_npi{NPI_SAMPLE_SIZE}" if NPI_SAMPLE_SIZE else "_all_npis"
    npi_list_path = os.path.join(output_dir, f'sampled_npis{npi_suffix}.csv')
    pd.DataFrame({'NPI': sampled_npis}).to_csv(npi_list_path, index=False)
    print(f"\nSampled NPI list saved: {npi_list_path}")

    # Process each year with the fixed NPI sample
    for year in YEARS:
        print(f"\n{'='*100}")
        print(f"PROCESSING YEAR: {year}")
        print(f"{'='*100}")
        try:
            process_year(year, sampled_npis)
        except Exception as e:
            print(f"\n[{year}] ✗ Fatal error: {e}")
            import traceback
            traceback.print_exc()
            continue

    overall_elapsed = (time.time() - overall_start) / 3600
    print(f"\n{'='*100}")
    print(f"ALL YEARS COMPLETE — {overall_elapsed:.2f} hours total")
    print(f"Output directory: {output_dir}")
    print(f"\nFiles generated:")
    print(f"  sampled_npis{npi_suffix}.csv  ← fixed NPI sample (same across all years)")
    print(f"  dataset_A_physician_patient_YYYY{npi_suffix}.csv")
    print(f"    One row per (NPI, ENROLID, year)")
    print(f"    Columns: NPI, ENROLID, year,")
    print(f"             revenue, COB, NETPAY, COINS, COPAY, DEDUCT,")
    print(f"             m_estimate, WORK_RVU, MP_RVU,")
    print(f"             PE_RVU_actualized, FACILITY_PE_RVU, NONFAC_PE_RVU, n_claims")
    print(f"  dataset_B_physician_patient_episode_YYYY{npi_suffix}.csv")
    print(f"    One row per (NPI, ENROLID, episode_id, year)")
    print(f"    Same columns as A plus: episode_id, is_residual")
    print(f"    episode_id = year*1e8 + counter (unique across years); 0 = residual")
    print(f"\nsum(Dataset B over episodes) == Dataset A for every (NPI, ENROLID, year)")
    print("=" * 100)
