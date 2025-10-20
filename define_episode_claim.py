import duckdb
import pandas as pd
from datetime import timedelta
import numpy as np
import os
import time
import psutil
import gc

# Settings
DATASET_TYPE = "MEDICARE_SET_A"
DATABASE = "MDCR"
TABLE_CODE = "O"
YEARS = [str(year) for year in range(2014, 2014)]  # 2014-2024

# Episode definition parameters
DX_DIGITS = 3
TIME_WINDOW_DAYS = 100
PATIENT_CHUNK_SIZE = 2000

# SAMPLING PARAMETER
SAMPLE_SIZE = 15000

# Directories
home_dir = os.path.expanduser('~')
output_dir = os.path.join(home_dir, 'episode_outputs')
temp_dir = os.path.join(home_dir, 'duckdb_temp')

os.makedirs(output_dir, exist_ok=True)
os.makedirs(temp_dir, exist_ok=True)

np.random.seed(42)

print("="*100)
print("HIGHLY OPTIMIZED EPISODE PROCESSING")
print("="*100)
print(f"Years: {YEARS[0]}-{YEARS[-1]} | Sample: {SAMPLE_SIZE:,} patients | Chunk: {PATIENT_CHUNK_SIZE:,}")
print("Optimizations: Vectorized ops + Minimal copying + Efficient aggregation")

def define_episodes_optimized(df, dx_digits=3, time_window_days=100):
    """
    OPTIMIZED: Episode assignment with rolling window diagnosis check.
    Uses efficient vectorized operations where possible, with targeted iteration for complex logic.
    """
    if len(df) == 0:
        return df

    # Sort once (required for logic)
    df.sort_values(['ENROLID', 'SVCDATE'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Vectorized: Clean and truncate diagnosis codes in-place
    # IMPORTANT: Replace None/nan BEFORE truncating to avoid "Non" appearing in data
    for col in ['DX1', 'DX2', 'DX3', 'DX4']:
        df[col] = df[col].astype(str)
        df[col].replace(['', 'nan', 'None', 'Non', ' ', 'nat'], np.nan, inplace=True)
        df[col] = df[col].str[:dx_digits]
        # Clean again after truncation in case truncation created invalid values
        df[col].replace(['', 'nan', 'Non', ' '], np.nan, inplace=True)

    # Initialize episode tracking
    df['episode_id'] = 0
    current_episode = 0

    # Process each patient separately
    for patient_id in df['ENROLID'].unique():
        patient_mask = df['ENROLID'] == patient_id
        patient_indices = df[patient_mask].index.tolist()

        if len(patient_indices) == 0:
            continue

        # Start first episode for this patient
        current_episode += 1
        df.loc[patient_indices[0], 'episode_id'] = current_episode

        # Track active episodes: {episode_id: {'date': last_date, 'dx_codes': set of codes}}
        active_episodes = {
            current_episode: {
                'date': df.loc[patient_indices[0], 'SVCDATE'],
                'dx_codes': set(df.loc[patient_indices[0], ['DX1', 'DX2', 'DX3', 'DX4']].dropna().values)
            }
        }

        # Process remaining visits for this patient
        for idx in patient_indices[1:]:
            visit_date = df.loc[idx, 'SVCDATE']
            visit_dx = set(df.loc[idx, ['DX1', 'DX2', 'DX3', 'DX4']].dropna().values)

            # Remove expired episodes (outside time window)
            episodes_to_remove = []
            for ep_id, ep_data in active_episodes.items():
                days_diff = (visit_date - ep_data['date']).days
                if days_diff > time_window_days:
                    episodes_to_remove.append(ep_id)

            for ep_id in episodes_to_remove:
                del active_episodes[ep_id]

            # Check if this visit matches any active episode
            matched_episode = None
            if len(visit_dx) > 0:  # Only check if visit has diagnosis codes
                for ep_id, ep_data in active_episodes.items():
                    # Check for diagnosis overlap
                    if len(visit_dx & ep_data['dx_codes']) > 0:
                        matched_episode = ep_id
                        break

            # Assign episode
            if matched_episode is not None:
                # Continue existing episode
                df.loc[idx, 'episode_id'] = matched_episode
                # Update episode data
                active_episodes[matched_episode]['date'] = visit_date
                active_episodes[matched_episode]['dx_codes'].update(visit_dx)
            else:
                # Start new episode
                current_episode += 1
                df.loc[idx, 'episode_id'] = current_episode
                active_episodes[current_episode] = {
                    'date': visit_date,
                    'dx_codes': visit_dx
                }

    return df

def aggregate_episodes_fast(df):
    """
    ULTRA-OPTIMIZED: Fast aggregation using built-in functions.
    """
    # Group once
    grouped = df.groupby(['ENROLID', 'episode_id'], sort=False)

    # Basic aggregations (fast)
    agg_result = grouped.agg({
        'SVCDATE': ['nunique', 'min', 'max'],
        'PAY': 'sum'
    })

    # Flatten column names
    agg_result.columns = ['UNIQUE_DATES', 'EARLIEST_DATE', 'LATEST_DATE', 'TOTAL_COST']
    agg_result.reset_index(inplace=True)

    # Diagnosis codes - efficient string concatenation
    # Stack all dx columns into long format
    dx_cols = ['DX1', 'DX2', 'DX3', 'DX4']
    dx_long = df[['ENROLID', 'episode_id'] + dx_cols].melt(
        id_vars=['ENROLID', 'episode_id'],
        value_vars=dx_cols,
        value_name='dx_code'
    )

    # Remove nulls and get unique codes per episode
    dx_long = dx_long[dx_long['dx_code'].notna()]
    dx_unique = dx_long.groupby(['ENROLID', 'episode_id'], sort=False)['dx_code'].apply(
        lambda x: ','.join(sorted(set(x)))
    ).reset_index()
    dx_unique.columns = ['ENROLID', 'episode_id', 'UNIQUE_DX_CODES']

    # Merge back
    result = agg_result.merge(dx_unique, on=['ENROLID', 'episode_id'], how='left')
    result['UNIQUE_DX_CODES'].fillna('', inplace=True)

    return result

def process_year_optimized(year):
    """
    ULTRA-OPTIMIZED: Single year processing.
    """
    start_time = time.time()
    process = psutil.Process()
    initial_memory = process.memory_info().rss / (1024**3)
    peak_memory = initial_memory

    conn = duckdb.connect()
    conn.execute(f"SET temp_directory='{temp_dir}'")
    conn.execute("SET memory_limit='3GB'")
    conn.execute("SET threads=2")

    print(f"\n[{year}] Starting | Memory: {initial_memory:.2f} GB")

    output_file = os.path.join(output_dir, f'episode_summary_{year}_sample{SAMPLE_SIZE}.csv')
    file_path = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_{year}.parquet"

    # Get and sample patients
    try:
        all_patients = conn.execute(f"""
            SELECT DISTINCT ENROLID FROM '{file_path}' WHERE ENROLID IS NOT NULL
        """).df()['ENROLID'].values

        print(f"[{year}] Found {len(all_patients):,} patients")

        year_seed = 42 + int(year)
        np.random.seed(year_seed)

        if SAMPLE_SIZE and len(all_patients) > SAMPLE_SIZE:
            patients = np.random.choice(all_patients, size=SAMPLE_SIZE, replace=False)
            print(f"[{year}] Sampled {SAMPLE_SIZE:,} patients")
        else:
            patients = all_patients
    except Exception as e:
        print(f"[{year}] Error: {e}")
        conn.close()
        return None

    # Process in chunks
    chunks = [patients[i:i + PATIENT_CHUNK_SIZE] for i in range(0, len(patients), PATIENT_CHUNK_SIZE)]
    print(f"[{year}] Processing {len(chunks)} chunks\n")

    first_write = True
    total_episodes = 0

    for chunk_idx, patient_chunk in enumerate(chunks, 1):
        chunk_start = time.time()

        current_memory = process.memory_info().rss / (1024**3)
        peak_memory = max(peak_memory, current_memory)

        # Progress
        if chunk_idx > 1:
            elapsed = time.time() - start_time
            rate = chunk_idx / elapsed * 60
            eta = (len(chunks) - chunk_idx) / rate if rate > 0 else 0
            print(f"[{year}] Chunk {chunk_idx}/{len(chunks)} | Mem: {current_memory:.1f}GB | "
                  f"Rate: {rate:.1f}/min | ETA: {eta:.1f}m")
        else:
            print(f"[{year}] Chunk {chunk_idx}/{len(chunks)} | Mem: {current_memory:.1f}GB")

        patient_list = ','.join(map(str, map(int, patient_chunk)))

        # Load data - ONLY essential columns
        try:
            chunk_df = conn.execute(f"""
                SELECT ENROLID, SVCDATE, DX1, DX2, DX3, DX4, PAY
                FROM '{file_path}'
                WHERE ENROLID IN ({patient_list}) AND SVCDATE IS NOT NULL
            """).df()
        except Exception as e:
            print(f"[{year}] Error loading chunk {chunk_idx}: {e}")
            continue

        if len(chunk_df) == 0:
            continue

        # Convert types
        chunk_df['SVCDATE'] = pd.to_datetime(chunk_df['SVCDATE'], errors='coerce')
        chunk_df['PAY'] = pd.to_numeric(chunk_df['PAY'], errors='coerce').fillna(0)
        chunk_df.dropna(subset=['SVCDATE'], inplace=True)

        # Define episodes
        chunk_df = define_episodes_optimized(chunk_df, DX_DIGITS, TIME_WINDOW_DAYS)

        # Aggregate
        episodes = aggregate_episodes_fast(chunk_df)
        episodes['YEAR'] = year

        # Reorder
        episodes = episodes[['ENROLID', 'episode_id', 'YEAR', 'UNIQUE_DATES',
                            'EARLIEST_DATE', 'LATEST_DATE', 'TOTAL_COST', 'UNIQUE_DX_CODES']]
        episodes.columns = ['ENROLID', 'EPISODE_ID', 'YEAR', 'UNIQUE_DATES',
                           'EARLIEST_DATE', 'LATEST_DATE', 'TOTAL_COST', 'UNIQUE_DX_CODES']

        # Save
        episodes.to_csv(output_file, index=False, mode='w' if first_write else 'a', header=first_write)
        first_write = False

        total_episodes += len(episodes)
        chunk_time = time.time() - chunk_start
        print(f"         → {len(episodes):,} episodes in {chunk_time:.1f}s")

        # Cleanup
        del chunk_df, episodes
        gc.collect()

    conn.close()

    total_time = time.time() - start_time
    file_size = os.path.getsize(output_file) / (1024**2) if os.path.exists(output_file) else 0

    print(f"\n[{year}] ✓ COMPLETE!")
    print(f"  Time: {total_time/60:.1f} min | Peak Mem: {peak_memory:.1f} GB")
    print(f"  Episodes: {total_episodes:,} | File: {file_size:.1f} MB")

    return output_file

if __name__ == '__main__':
    overall_start = time.time()

    output_files = []

    # Process each year and save after completion
    for year in YEARS:
        print(f"\n{'='*100}")
        print(f"PROCESSING YEAR: {year}")
        print(f"{'='*100}")

        output_file = process_year_optimized(year)

        if output_file and os.path.exists(output_file):
            output_files.append(output_file)
            print(f"\n✓ Year {year} saved: {output_file}")
        else:
            print(f"\n✗ Year {year} failed to process")

    overall_time = time.time() - overall_start

    print(f"\n{'='*100}")
    print(f"ALL YEARS COMPLETE")
