"""
MASTER PIPELINE
===============
Runs the full physician-patient episode analysis pipeline in order:

  Stage 1: Pull claims, assign episodes, build Datasets A and B (per year)
  Stage 2: Reassign episode IDs to be sequential within each (NPI, ENROLID)
            pair across all years — runs on both identified and de-identified
            files if de-identified files already exist
  Stage 3: De-identify NPI → anon_physician_id for export

Usage:
  python master_pipeline.py --stage all          # run everything
  python master_pipeline.py --stage 1            # claims + episodes only
  python master_pipeline.py --stage 2            # episode ID reassignment only
  python master_pipeline.py --stage 3            # de-identification only
  python master_pipeline.py --stage 2,3          # reassignment + de-id

If de-identified files already exist when stage 2 runs, the reassignment
is applied to both identified and de-identified files automatically.
"""

import argparse
import os
import sys
import time
import pandas as pd
import numpy as np
import duckdb
import psutil
import gc
from multiprocessing import Pool, cpu_count
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# SHARED SETTINGS
# =============================================================================
DATASET_TYPE     = "MEDICARE_SET_A"
DATABASE         = "MDCR"
TABLE_CODE       = "O"
YEARS            = [str(y) for y in range(2015, 2025)]
DX_DIGITS        = 3
TIME_WINDOW_DAYS = 100
NPI_SAMPLE_SIZE  = 50000
NPI_CHUNK_SIZE   = 500
N_PROCESSES      = min(cpu_count() - 1, 4)
RVU_BASE_PATH    = '/home/zl749/rvu_PPRRVUXX_partD.csv'

home_dir        = os.path.expanduser('~')
output_dir      = os.path.join(home_dir, 'episode_outputs')
export_dir      = os.path.join(home_dir, 'episode_outputs_deidentified')
crosswalk_dir   = os.path.join(home_dir, 'secure_crosswalks')
temp_dir        = os.path.join(home_dir, 'duckdb_temp')

for d in [output_dir, export_dir, crosswalk_dir, temp_dir]:
    os.makedirs(d, exist_ok=True)

npi_suffix = f"_npi{NPI_SAMPLE_SIZE}"

FACILITY_POS     = {2, 19, 21, 22, 23, 24, 26, 31, 34, 41, 42, 51, 52, 53, 56, 61}
NON_FACILITY_POS = {1, 3, 4, 11, 12, 13, 14, 15, 16, 17, 20, 25, 32, 33, 49,
                    50, 54, 55, 57, 60, 62, 65, 71, 72, 81, 99}

np.random.seed(42)


# =============================================================================
# STAGE 1 HELPERS
# =============================================================================

def sample_npis_once():
    print("\n" + "=" * 80)
    print("STAGE 1 — Sampling NPIs from union of all years")
    print("=" * 80)

    file_paths = [
        f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_update_{y}.parquet"
        for y in YEARS
        if os.path.exists(
            f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_update_{y}.parquet"
        )
    ]

    if not file_paths:
        raise FileNotFoundError("No parquet files found.")

    union_sql = " UNION ALL ".join(
        f"SELECT CAST(NPI AS VARCHAR) AS NPI FROM '{fp}' WHERE NPI IS NOT NULL AND DATATYP = 3"
        for fp in file_paths
    )

    conn = duckdb.connect()
    conn.execute(f"SET temp_directory='{temp_dir}'")
    conn.execute("SET memory_limit='8GB'")
    conn.execute(f"SET threads={max(1, cpu_count()-1)}")

    print(f"  Querying DISTINCT NPI across {len(file_paths)} files...")
    t0 = time.time()
    all_npis = conn.execute(f"""
        SELECT DISTINCT NPI FROM ({union_sql})
        WHERE NPI IS NOT NULL AND TRIM(NPI) != ''
    """).df()['NPI'].str.strip().values
    conn.close()

    print(f"  Found {len(all_npis):,} unique NPIs ({time.time()-t0:.1f}s)")

    if NPI_SAMPLE_SIZE and len(all_npis) > NPI_SAMPLE_SIZE:
        np.random.seed(42)
        sampled = np.random.choice(all_npis, size=NPI_SAMPLE_SIZE, replace=False)
        print(f"  Sampled {NPI_SAMPLE_SIZE:,} NPIs (seed=42)")
    else:
        sampled = all_npis
        print(f"  Using all {len(sampled):,} NPIs")

    # Save NPI list
    npi_list_path = os.path.join(output_dir, f'sampled_npis{npi_suffix}.csv')
    pd.DataFrame({'NPI': sampled}).to_csv(npi_list_path, index=False)
    print(f"  NPI list saved: {npi_list_path}")
    return sampled


def load_rvu_data(year):
    year_suffix = str(year)[2:]
    rvu_path = RVU_BASE_PATH.replace('XX', year_suffix)
    try:
        rvu_df = pd.read_csv(rvu_path, encoding='latin-1')
        if 'HCPS' in rvu_df.columns:
            rvu_df = rvu_df.rename(columns={'HCPS': 'PROC1'})
        if 'PROC1' not in rvu_df.columns:
            raise ValueError(f"No PROC1/HCPS column found in RVU file")
        rvu_df['PROC1'] = rvu_df['PROC1'].astype(str).str.strip()
        keep = ['PROC1', 'WORK_RVU', 'MP_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU']
        keep = [c for c in keep if c in rvu_df.columns]
        rvu_df = rvu_df[keep].copy()
        for col in keep:
            if col != 'PROC1':
                rvu_df[col] = pd.to_numeric(rvu_df[col], errors='coerce').fillna(0)
        rvu_df = rvu_df.drop_duplicates(subset='PROC1', keep='first')
        print(f"  [{year}] RVU: {len(rvu_df):,} procedure codes")
        return rvu_df
    except FileNotFoundError:
        print(f"  [{year}] WARNING: RVU file not found")
        return None
    except Exception as e:
        print(f"  [{year}] ERROR loading RVU: {e}")
        return None


def define_episodes_npi_patient(df, dx_digits=3, time_window_days=100):
    if len(df) == 0:
        df['episode_id'] = 0
        df['is_residual'] = 1
        return df

    df = df.sort_values(['NPI', 'ENROLID', 'SVCDATE']).reset_index(drop=True)

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
        first_dx  = set(df.loc[first_idx, ['DX1','DX2','DX3','DX4']].dropna().values)

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
            visit_dx   = set(df.loc[idx, ['DX1','DX2','DX3','DX4']].dropna().values)

            if len(visit_dx) == 0:
                df.loc[idx, ['episode_id', 'is_residual']] = [0, 1]
                continue

            for ep_id in [e for e, d in active_episodes.items()
                          if (visit_date - d['date']).days > time_window_days]:
                del active_episodes[ep_id]

            matched = next(
                (ep_id for ep_id, ep_data in active_episodes.items()
                 if visit_dx & ep_data['dx_codes']), None
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


def process_npi_chunk(args):
    npi_chunk, year, chunk_idx, total_chunks, file_path, rvu_df = args
    try:
        conn = duckdb.connect()
        conn.execute(f"SET temp_directory='{temp_dir}'")
        conn.execute("SET memory_limit='3GB'")
        conn.execute("SET threads=2")

        year_int = int(year)
        medadv_clause = "AND (MEDADV = 0 OR MEDADV IS NULL)" if year_int >= 2020 else ""
        npi_list = ','.join(f"'{str(n)}'" for n in npi_chunk)

        chunk_df = conn.execute(f"""
            SELECT
                CAST(NPI     AS VARCHAR) AS NPI,
                CAST(ENROLID AS VARCHAR) AS ENROLID,
                SVCDATE, DX1, DX2, DX3, DX4,
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

        chunk_df['SVCDATE'] = pd.to_datetime(chunk_df['SVCDATE'], errors='coerce')
        chunk_df = chunk_df.dropna(subset=['SVCDATE'])
        chunk_df['NPI']     = chunk_df['NPI'].str.strip()
        chunk_df['ENROLID'] = chunk_df['ENROLID'].str.strip()
        chunk_df['PROC1']   = chunk_df['PROC1'].str.strip()

        chunk_df['revenue'] = (
            chunk_df['COB'] + chunk_df['NETPAY'] +
            chunk_df['COINS'] + chunk_df['COPAY'] + chunk_df['DEDUCT']
        )

        if rvu_df is not None:
            chunk_df = chunk_df.merge(rvu_df, on='PROC1', how='left')
            for col in ['WORK_RVU', 'MP_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU']:
                if col in chunk_df.columns:
                    chunk_df[col] = chunk_df[col].fillna(0)
        else:
            for col in ['WORK_RVU', 'MP_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU']:
                chunk_df[col] = 0.0

        chunk_df['m_estimate'] = (chunk_df['WORK_RVU'] + chunk_df['MP_RVU']) * 98

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
        import traceback; traceback.print_exc()
        return None, chunk_idx


def process_year(year, sampled_npis):
    start_time = time.time()
    process    = psutil.Process()
    print(f"\n[{year}] Starting | Memory: {process.memory_info().rss/1e9:.2f} GB")

    file_path = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_update_{year}.parquet"
    if not os.path.exists(file_path):
        print(f"[{year}] File not found — skipping")
        return

    rvu_df = load_rvu_data(year)

    chunks = [sampled_npis[i:i+NPI_CHUNK_SIZE] for i in range(0, len(sampled_npis), NPI_CHUNK_SIZE)]
    total_chunks = len(chunks)
    print(f"[{year}] {len(sampled_npis):,} NPIs → {total_chunks:,} chunks | {N_PROCESSES} workers")

    chunk_args = [(chunk, year, idx+1, total_chunks, file_path, rvu_df)
                  for idx, chunk in enumerate(chunks)]

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
                print(f"[{year}] {chunk_idx}/{total_chunks} | claims: {n_so_far:,} | "
                      f"mem: {mem:.1f}GB | {rate:.1f} chunks/min | ETA: {eta:.1f}m")

    if not all_claims:
        print(f"[{year}] No claims — skipping")
        return

    claims_df = pd.concat(all_claims, ignore_index=True)
    del all_claims; gc.collect()

    print(f"\n[{year}] Claims: {len(claims_df):,} | NPIs: {claims_df['NPI'].nunique():,} | "
          f"Patients: {claims_df['ENROLID'].nunique():,}")

    print(f"[{year}] Episode assignment...")
    t0 = time.time()
    claims_df = define_episodes_npi_patient(claims_df, DX_DIGITS, TIME_WINDOW_DAYS)

    # Year offset for global uniqueness within year (reassigned in Stage 2)
    year_offset = int(year) * 100_000_000
    nonresidual = claims_df['is_residual'] == 0
    claims_df.loc[nonresidual, 'episode_id'] = (
        claims_df.loc[nonresidual, 'episode_id'] + year_offset
    )
    n_residual = claims_df['is_residual'].sum()
    n_episodes = claims_df.loc[nonresidual, 'episode_id'].nunique()
    print(f"[{year}] Done in {(time.time()-t0)/60:.1f}m | episodes: {n_episodes:,} | "
          f"residual: {n_residual:,} ({n_residual/len(claims_df)*100:.1f}%)")

    conn = duckdb.connect()
    conn.register('claims', claims_df)

    agg_expr = """
        SUM(revenue) AS revenue, SUM(COB) AS COB, SUM(NETPAY) AS NETPAY,
        SUM(COINS) AS COINS, SUM(COPAY) AS COPAY, SUM(DEDUCT) AS DEDUCT,
        SUM(m_estimate) AS m_estimate, SUM(WORK_RVU) AS WORK_RVU,
        SUM(MP_RVU) AS MP_RVU, SUM(PE_RVU_actualized) AS PE_RVU_actualized,
        SUM(FACILITY_PE_RVU) AS FACILITY_PE_RVU,
        SUM("NON-FAC_PE_RVU") AS NONFAC_PE_RVU, COUNT(*) AS n_claims
    """

    dataset_a = conn.execute(f"SELECT NPI, ENROLID, {agg_expr} FROM claims GROUP BY NPI, ENROLID").df()
    dataset_a['year'] = int(year)

    dataset_b = conn.execute(f"""
        SELECT NPI, ENROLID, episode_id, is_residual, {agg_expr}
        FROM claims GROUP BY NPI, ENROLID, episode_id, is_residual
    """).df()
    dataset_b['year'] = int(year)

    conn.unregister('claims'); conn.close()

    # Reconciliation
    conn2 = duckdb.connect()
    conn2.register('a', dataset_a); conn2.register('b', dataset_b)
    rec = conn2.execute("""
        SELECT MAX(ABS(a.revenue - b_sum.rev_b)) AS max_rev_diff,
               MAX(ABS(a.m_estimate - b_sum.m_b)) AS max_m_diff,
               SUM(CASE WHEN b_sum.rev_b IS NULL THEN 1 ELSE 0 END) AS n_missing
        FROM a LEFT JOIN (
            SELECT NPI, ENROLID, SUM(revenue) AS rev_b, SUM(m_estimate) AS m_b
            FROM b GROUP BY NPI, ENROLID
        ) b_sum USING (NPI, ENROLID)
    """).df()
    conn2.close()

    ok = rec['max_rev_diff'].iloc[0] < 0.01 and rec['max_m_diff'].iloc[0] < 0.01 and rec['n_missing'].iloc[0] == 0
    print(f"[{year}] Reconciliation: {'✓' if ok else '✗ WARNING'} | "
          f"max_rev_diff=${rec['max_rev_diff'].iloc[0]:.6f} | "
          f"max_m_diff={rec['max_m_diff'].iloc[0]:.6f}")

    path_a = os.path.join(output_dir, f'dataset_A_physician_patient_{year}{npi_suffix}.csv')
    path_b = os.path.join(output_dir, f'dataset_B_physician_patient_episode_{year}{npi_suffix}.csv')
    dataset_a.to_csv(path_a, index=False)
    dataset_b.to_csv(path_b, index=False)

    print(f"[{year}] ✓ COMPLETE in {(time.time()-start_time)/60:.1f}m | "
          f"A: {os.path.getsize(path_a)/1e6:.1f}MB | B: {os.path.getsize(path_b)/1e6:.1f}MB")

    del claims_df, dataset_a, dataset_b; gc.collect()


# =============================================================================
# STAGE 2: REASSIGN EPISODE IDs
# Works on both identified and de-identified files if they exist.
# Uses physician_col parameter ('NPI' or 'anon_physician_id') so the same
# logic runs on both file sets.
# =============================================================================

def reassign_episode_ids_for_fileset(physician_col, files_dir, file_suffix=''):
    """
    Reassign sequential episode IDs within each (physician, patient) pair
    across all years. Works on either identified (NPI) or de-identified
    (anon_physician_id) files.
    """
    print(f"\n  Physician column: '{physician_col}' | Directory: {files_dir}")

    # Load all Dataset B files
    dfs = []
    for year in YEARS:
        fname = f'dataset_B_physician_patient_episode_{year}{npi_suffix}{file_suffix}.csv'
        path  = os.path.join(files_dir, fname)
        if not os.path.exists(path):
            print(f"    [{year}] Not found — skipping")
            continue
        df = pd.read_csv(path)
        dfs.append(df)
        print(f"    [{year}] {len(df):,} rows")

    if not dfs:
        print("    No files found — skipping")
        return

    combined = pd.concat(dfs, ignore_index=True)
    del dfs
    print(f"\n    Total rows: {len(combined):,}")
    print(f"    Unique ({physician_col}, ENROLID) pairs: "
          f"{combined.groupby([physician_col, 'ENROLID']).ngroups:,}")

    # Build episode map — one row per unique (physician, ENROLID, year, old episode_id)
    real_mask    = combined['is_residual'] == 0
    episode_map  = (
        combined.loc[real_mask, [physician_col, 'ENROLID', 'year', 'episode_id']]
        .drop_duplicates()
        .sort_values([physician_col, 'ENROLID', 'year', 'episode_id'])
        .reset_index(drop=True)
    )

    # Sequential cumcount within each (physician, ENROLID) pair
    episode_map['new_episode_id'] = (
        episode_map.groupby([physician_col, 'ENROLID']).cumcount() + 1
    )

    print(f"    Episode map: {len(episode_map):,} unique episodes")
    print(f"    Max episodes per pair: "
          f"{episode_map.groupby([physician_col,'ENROLID'])['new_episode_id'].max().max():,}")

    # Merge new IDs back
    combined = combined.merge(
        episode_map[[physician_col, 'ENROLID', 'year', 'episode_id', 'new_episode_id']],
        on=[physician_col, 'ENROLID', 'year', 'episode_id'],
        how='left'
    )
    combined['new_episode_id'] = combined['new_episode_id'].fillna(0).astype(int)
    combined['episode_id']     = combined['new_episode_id']
    combined = combined.drop(columns=['new_episode_id'])

    # Verification
    real     = combined[combined['is_residual'] == 0]
    residual = combined[combined['is_residual'] == 1]
    assert (residual['episode_id'] == 0).all(), "Residual rows have non-zero episode_id!"

    pair_check = (
        real.groupby([physician_col, 'ENROLID'])['episode_id']
        .agg(['min', 'max', 'nunique']).reset_index()
    )
    pair_check['expected'] = pair_check['max'] - pair_check['min'] + 1
    bad = pair_check[pair_check['nunique'] != pair_check['expected']]
    print(f"    ✓ Residuals all 0 | "
          f"{'✓' if len(bad)==0 else f'✗ {len(bad)} pairs non-contiguous'} contiguous | "
          f"min episode_id = {pair_check['min'].min()}")

    # Write back per year
    for year in YEARS:
        fname = f'dataset_B_physician_patient_episode_{year}{npi_suffix}{file_suffix}.csv'
        path  = os.path.join(files_dir, fname)
        if not os.path.exists(path):
            continue

        # Backup original
        backup = path.replace('.csv', '_old_episodeids.csv')
        if not os.path.exists(backup):
            os.rename(path, backup)

        year_df = combined[combined['year'] == int(year)].copy()
        year_df.to_csv(path, index=False)
        print(f"    [{year}] Written: {len(year_df):,} rows ({os.path.getsize(path)/1e6:.1f}MB)")


def run_stage_2():
    print("\n" + "=" * 80)
    print("STAGE 2 — Reassigning episode IDs across years")
    print("=" * 80)

    # Always run on identified files
    print("\n[2a] Identified files (NPI)...")
    reassign_episode_ids_for_fileset(
        physician_col='NPI',
        files_dir=output_dir,
        file_suffix=''
    )

    # Run on de-identified files if they exist
    deid_files = [
        f for f in os.listdir(export_dir)
        if 'dataset_B' in f and '_deid' in f
    ] if os.path.exists(export_dir) else []

    if deid_files:
        print("\n[2b] De-identified files (anon_physician_id) found — reassigning there too...")
        reassign_episode_ids_for_fileset(
            physician_col='anon_physician_id',
            files_dir=export_dir,
            file_suffix='_deid'
        )
    else:
        print("\n[2b] No de-identified files found — skipping de-id reassignment")
        print("     (Run Stage 3 after Stage 2 to create de-identified files with correct IDs)")


# =============================================================================
# STAGE 3: DE-IDENTIFICATION
# =============================================================================

def run_stage_3():
    print("\n" + "=" * 80)
    print("STAGE 3 — De-identification (NPI → anon_physician_id)")
    print("=" * 80)

    # Build NPI crosswalk
    print("\n[3a] Building NPI crosswalk...")
    all_npis = set()
    for year in YEARS:
        path_a = os.path.join(output_dir, f'dataset_A_physician_patient_{year}{npi_suffix}.csv')
        if not os.path.exists(path_a):
            continue
        npis = pd.read_csv(path_a, usecols=['NPI'])['NPI'].astype(str).str.strip().unique()
        all_npis.update(npis)
        print(f"  [{year}] {len(npis):,} NPIs")

    all_npis = sorted(all_npis)
    crosswalk = pd.DataFrame({
        'NPI':              all_npis,
        'anon_physician_id': range(1, len(all_npis) + 1)
    })
    crosswalk_path = os.path.join(crosswalk_dir, f'npi_crosswalk{npi_suffix}.csv')
    crosswalk.to_csv(crosswalk_path, index=False)
    npi_to_anon = dict(zip(crosswalk['NPI'], crosswalk['anon_physician_id']))
    print(f"  Crosswalk: {len(crosswalk):,} NPIs → {crosswalk_path}")
    print(f"  *** DO NOT export this file ***")

    # De-identify and export
    print("\n[3b] De-identifying datasets...")
    for year in YEARS:
        path_a = os.path.join(output_dir, f'dataset_A_physician_patient_{year}{npi_suffix}.csv')
        path_b = os.path.join(output_dir, f'dataset_B_physician_patient_episode_{year}{npi_suffix}.csv')

        if not os.path.exists(path_a) or not os.path.exists(path_b):
            print(f"  [{year}] Missing files — skipping")
            continue

        for path, label in [(path_a, 'A'), (path_b, 'B')]:
            df = pd.read_csv(path)
            df['NPI'] = df['NPI'].astype(str).str.strip()
            df['anon_physician_id'] = df['NPI'].map(npi_to_anon)
            cols = ['anon_physician_id'] + [c for c in df.columns if c not in ['NPI', 'anon_physician_id']]
            df   = df[cols]

            suffix_map = {'A': 'A_physician_patient', 'B': 'B_physician_patient_episode'}
            export_path = os.path.join(
                export_dir,
                f'dataset_{suffix_map[label]}_{year}{npi_suffix}_deid.csv'
            )
            df.to_csv(export_path, index=False)
            print(f"  [{year}] Dataset {label}: {len(df):,} rows → {os.path.getsize(export_path)/1e6:.1f}MB")

    print(f"\n  Exportable files: {export_dir}/")
    print(f"  Crosswalk (secure): {crosswalk_path}")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Master pipeline')
    parser.add_argument('--stage', type=str, default='all',
                        help='Stages to run: all, 1, 2, 3, or comma-separated e.g. 2,3')
    args = parser.parse_args()

    stages = set()
    if args.stage == 'all':
        stages = {1, 2, 3}
    else:
        stages = {int(s.strip()) for s in args.stage.split(',')}

    overall_start = time.time()

    print("=" * 80)
    print("MASTER PIPELINE — PHYSICIAN-PATIENT EPISODE ANALYSIS")
    print("=" * 80)
    print(f"Stages to run: {sorted(stages)}")
    print(f"Years: {YEARS[0]}–{YEARS[-1]}")
    print(f"NPI sample: {NPI_SAMPLE_SIZE:,} (fixed across all years)")

    if 1 in stages:
        sampled_npis = sample_npis_once()
        for year in YEARS:
            print(f"\n{'='*80}\nPROCESSING YEAR: {year}\n{'='*80}")
            try:
                process_year(year, sampled_npis)
            except Exception as e:
                print(f"[{year}] ✗ Fatal error: {e}")
                import traceback; traceback.print_exc()

    if 2 in stages:
        run_stage_2()

    if 3 in stages:
        run_stage_3()

    elapsed = (time.time() - overall_start) / 3600
    print(f"\n{'='*80}")
    print(f"PIPELINE COMPLETE — {elapsed:.2f} hours total")
    print(f"{'='*80}")
    print(f"\nOutputs:")
    print(f"  Identified:     {output_dir}/dataset_[A|B]_*{npi_suffix}.csv")
    print(f"  De-identified:  {export_dir}/dataset_[A|B]_*{npi_suffix}_deid.csv")
    print(f"  NPI crosswalk:  {crosswalk_dir}/npi_crosswalk{npi_suffix}.csv  ← never export")
    print(f"  NPI sample:     {output_dir}/sampled_npis{npi_suffix}.csv")
    print(f"\nRecommended order if running stages separately:")
    print(f"  python master_pipeline.py --stage 1   # pull + episode assignment")
    print(f"  python master_pipeline.py --stage 2   # fix episode IDs across years")
    print(f"  python master_pipeline.py --stage 3   # de-identify for export")
    print("=" * 80)
