import duckdb
import pandas as pd
from datetime import timedelta
import numpy as np
from pathlib import Path
import gc
import os

# Settings
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
TABLE_CODE = "O"
YEARS = ["2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]

# Episode definition parameters
DX_DIGITS = 3
TIME_WINDOW_DAYS = 100
PATIENT_CHUNK_SIZE = 100

# SAMPLING PARAMETER - Set to None to process all patients
SAMPLE_SIZE = 15000  # Randomly sample this many patients per year

# Use home directory for output (you have permissions here)
home_dir = os.path.expanduser('~')
output_file = os.path.join(home_dir, f'episode_summary_{DX_DIGITS}digit_{TIME_WINDOW_DAYS}days_sample{SAMPLE_SIZE}.csv')
temp_dir = os.path.join(home_dir, 'duckdb_temp')

# Create directories
os.makedirs(temp_dir, exist_ok=True)

# Connect to DuckDB and set temp directory
conn = duckdb.connect()
conn.execute(f"SET temp_directory='{temp_dir}'")

# Set random seed for reproducibility
np.random.seed(42)

print("="*100)
print("YEAR-BY-YEAR EPISODE PROCESSING (NO CROSS-YEAR CONTINUITY)")
print("="*100)
print(f"Sample size: {SAMPLE_SIZE:,} patients per year")
print(f"Output file: {output_file}")
print(f"Temp directory: {temp_dir}")
print(f"Episode IDs are unique within each ENROLID (patient)")

def define_episodes(patient_df, dx_digits=3, time_window_days=100):
    """
    Define episodes for a patient based on matching diagnosis codes within time window.
    Returns the dataframe with episode_id added (episode numbers are per-patient).
    """
    if len(patient_df) == 0:
        return patient_df

    patient_df = patient_df.sort_values('SVCDATE').reset_index(drop=True)

    def get_dx_set(row, digits):
        dx_codes = []
        for dx_col in ['DX1', 'DX2', 'DX3', 'DX4']:
            val = row[dx_col]
            if pd.notna(val) and val != '' and val != ' ':
                truncated = str(val)[:digits]
                dx_codes.append(truncated)
        return set(dx_codes)

    patient_df['all_dx'] = patient_df.apply(lambda row: get_dx_set(row, dx_digits), axis=1)
    patient_df['has_dx'] = patient_df['all_dx'].apply(lambda x: len(x) > 0)

    episodes = []
    episode_counter = 1

    for idx, row in patient_df.iterrows():
        current_date = row['SVCDATE']
        current_dx = row['all_dx']
        has_dx = row['has_dx']

        if idx == 0:
            episodes.append(episode_counter)
            continue
        
        lookback_date = current_date - timedelta(days=time_window_days)
        matched_episode = None

        if has_dx:
            # Look back through previous visits to find matching diagnosis
            for prev_idx in range(idx):
                prev_date = patient_df.iloc[prev_idx]['SVCDATE']
                prev_dx = patient_df.iloc[prev_idx]['all_dx']

                if prev_date >= lookback_date:
                    if len(current_dx & prev_dx) > 0:
                        matched_episode = episodes[prev_idx]
                        break
        else:
            # No diagnosis codes - link to most recent visit within window
            for prev_idx in range(idx - 1, -1, -1):
                prev_date = patient_df.iloc[prev_idx]['SVCDATE']

                if prev_date >= lookback_date:
                    matched_episode = episodes[prev_idx]
                    break

        if matched_episode is not None:
            episodes.append(matched_episode)
        else:
            episode_counter += 1
            episodes.append(episode_counter)

    patient_df['episode_id'] = episodes
    patient_df = patient_df.drop(['all_dx', 'has_dx'], axis=1)

    return patient_df

first_write = True

# Process year by year
for year_idx, year in enumerate(YEARS):
    print(f"\n{'='*100}")
    print(f"PROCESSING YEAR {year} ({year_idx + 1}/{len(YEARS)})")
    print(f"{'='*100}")

    file_path = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_{year}.parquet"

    # Step 1: Get unique patients for this year
    try:
        query = f"""
        SELECT DISTINCT ENROLID 
        FROM '{file_path}'
        WHERE ENROLID IS NOT NULL
        """
        all_year_patients = conn.execute(query).df()['ENROLID'].values
        print(f"\nTotal unique patients in {year}: {len(all_year_patients):,}")

        # RANDOMLY SAMPLE PATIENTS
        if SAMPLE_SIZE is not None and len(all_year_patients) > SAMPLE_SIZE:
            year_patients = np.random.choice(all_year_patients, size=SAMPLE_SIZE, replace=False)
            print(f"Randomly sampled {SAMPLE_SIZE:,} patients for processing")
        else:
            year_patients = all_year_patients
            print(f"Processing all {len(year_patients):,} patients (fewer than sample size)")

    except Exception as e:
        print(f"Error accessing {year}: {e}")
        continue

    # Step 2: Process in chunks
    patient_chunks = [year_patients[i:i + PATIENT_CHUNK_SIZE]
                     for i in range(0, len(year_patients), PATIENT_CHUNK_SIZE)]
    print(f"Processing in {len(patient_chunks)} chunks of up to {PATIENT_CHUNK_SIZE} patients")

    for chunk_idx, patient_chunk in enumerate(patient_chunks, 1):
        if chunk_idx % 10 == 0:  # Progress update every 10 chunks
            print(f"  Chunk {chunk_idx}/{len(patient_chunks)}...", end=" ", flush=True)

        patient_list = ', '.join([str(int(p)) for p in patient_chunk])

        # Load data for this chunk from current year
        try:
            query = f"""
            SELECT ENROLID, SVCDATE, DX1, DX2, DX3, DX4, PAY
            FROM '{file_path}'
            WHERE ENROLID IN ({patient_list})
            AND SVCDATE IS NOT NULL
            """
            chunk_df = conn.execute(query).df()

        except Exception as e:
            print(f"Error loading chunk: {e}")
            continue

        if len(chunk_df) == 0:
            continue

        chunk_df['SVCDATE'] = pd.to_datetime(chunk_df['SVCDATE'])
        chunk_df['PAY'] = pd.to_numeric(chunk_df['PAY'], errors='coerce').fillna(0)

        # Process each patient
        chunk_episodes = []

        for patient_id in patient_chunk:
            patient_data = chunk_df[chunk_df['ENROLID'] == patient_id].copy()

            if len(patient_data) == 0:
                continue

            # Define episodes for this patient
            patient_with_episodes = define_episodes(
                patient_data,
                dx_digits=DX_DIGITS,
                time_window_days=TIME_WINDOW_DAYS
            )

            chunk_episodes.append(patient_with_episodes)

        if len(chunk_episodes) == 0:
            continue

        # Combine all patients in chunk
        chunk_with_episodes = pd.concat(chunk_episodes, ignore_index=True)

        # Extract diagnosis codes for each visit
        def get_all_dx_codes(row):
            codes = []
            for dx_col in ['DX1', 'DX2', 'DX3', 'DX4']:
                val = row[dx_col]
                if pd.notna(val) and val != '' and val != ' ':
                    code = str(val)[:DX_DIGITS]
                    codes.append(code)
            return codes

        chunk_with_episodes['dx_codes'] = chunk_with_episodes.apply(get_all_dx_codes, axis=1)

        # Aggregate to episode level
        episode_summary = chunk_with_episodes.groupby(['ENROLID', 'episode_id']).agg({
            'SVCDATE': ['nunique', 'min', 'max'],
            'PAY': 'sum',
            'dx_codes': lambda x: sorted(list(set([code for sublist in x for code in sublist])))
        }).reset_index()

        episode_summary.columns = ['ENROLID', 'EPISODE_ID', 'UNIQUE_DATES',
                                   'EARLIEST_DATE', 'LATEST_DATE', 'TOTAL_COST', 'UNIQUE_DX_CODES']

        # Add year column for reference
        episode_summary['YEAR'] = year

        # Convert diagnosis codes list to comma-separated string
        episode_summary['UNIQUE_DX_CODES'] = episode_summary['UNIQUE_DX_CODES'].apply(
            lambda x: ','.join(x) if len(x) > 0 else ''
        )

        # Reorder columns
        episode_summary = episode_summary[['ENROLID', 'EPISODE_ID', 'YEAR', 'UNIQUE_DATES',
                                          'EARLIEST_DATE', 'LATEST_DATE', 'TOTAL_COST', 'UNIQUE_DX_CODES']]

        # Save to file
        if first_write:
            episode_summary.to_csv(output_file, index=False, mode='w')
            first_write = False
        else:
            episode_summary.to_csv(output_file, index=False, mode='a', header=False)

        # Memory cleanup
        del chunk_df, chunk_episodes, chunk_with_episodes, episode_summary

        # Force garbage collection every 50 chunks
        if chunk_idx % 50 == 0:
            gc.collect()

    print(f"\n  Year {year} complete - memory cleared")
    gc.collect()

print(f"\n{'='*100}")
print("PROCESSING COMPLETE")
print(f"{'='*100}")

# Use DuckDB to read summary statistics without loading entire file into memory
print("\nCalculating summary statistics...")
try:
    summary_stats = conn.execute(f"""
        SELECT 
            COUNT(*) as total_episodes,
            COUNT(DISTINCT ENROLID) as total_patients,
            SUM(TOTAL_COST) as total_cost,
            AVG(UNIQUE_DATES) as avg_dates_per_episode,
            MAX(UNIQUE_DATES) as max_dates_per_episode
        FROM '{output_file}'
    """).df()

    print(f"\nFinal Dataset Summary:")
    print(f"  Total episodes: {summary_stats['total_episodes'].iloc[0]:,}")
    print(f"  Total patients: {summary_stats['total_patients'].iloc[0]:,}")
    print(f"  Total cost: ${summary_stats['total_cost'].iloc[0]:,.2f}")
    print(f"  Avg dates per episode: {summary_stats['avg_dates_per_episode'].iloc[0]:.2f}")
    print(f"  Max dates per episode: {summary_stats['max_dates_per_episode'].iloc[0]:,.0f}")

    # Episodes by year
    print(f"\nEpisodes by year:")
    by_year = conn.execute(f"""
        SELECT YEAR, 
               COUNT(*) as episodes,
               COUNT(DISTINCT ENROLID) as patients,
               AVG(UNIQUE_DATES) as avg_dates
        FROM '{output_file}'
        GROUP BY YEAR
        ORDER BY YEAR
    """).df()
    print(by_year.to_string(index=False))

    # Sample of episodes with most unique dates
    print(f"\nSample of episodes with most unique dates:")
    top_episodes = conn.execute(f"""
        SELECT ENROLID, EPISODE_ID, YEAR, UNIQUE_DATES, EARLIEST_DATE, LATEST_DATE, TOTAL_COST
        FROM '{output_file}'
        ORDER BY UNIQUE_DATES DESC
        LIMIT 10
    """).df()
    print(top_episodes.to_string(index=False))

except Exception as e:
    print(f"Could not calculate summary statistics: {e}")
    print("File saved successfully but summary calculation failed")

print(f"\nOutput saved to: {output_file}")
conn.close()
print("\nAnalysis complete!")
