import duckdb
import pandas as pd
from datetime import timedelta
import numpy as np
from pathlib import Path
import pickle
import shutil

# Settings
DATASET_TYPE = "COMMERCIAL_SET_A"  
DATABASE = "CCAE"                  
TABLE_CODE = "O"                   

# CHANGE THIS: Set to True to continue from 2022, False to start fresh
CONTINUE_MODE = True
RESTART_YEAR = "2022"  # Year to restart from

YEARS = ["2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]

# Episode definition parameters
DX_DIGITS = 3  
TIME_WINDOW_DAYS = 100  
PATIENT_CHUNK_SIZE = 100  

# Connect to DuckDB
conn = duckdb.connect()

print("="*100)
print("EFFICIENT YEAR-BY-YEAR EPISODE PROCESSING")
print("="*100)

def define_episodes(patient_df, dx_digits=3, time_window_days=100, starting_episode_num=1):
    """
    Define episodes for a patient based on matching diagnosis codes within time window.
    Returns the dataframe with episode_id added, starting from starting_episode_num.
    """
    if len(patient_df) == 0:
        return patient_df, starting_episode_num
    
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
    episode_counter = starting_episode_num
    
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
            for prev_idx in range(idx):
                prev_date = patient_df.iloc[prev_idx]['SVCDATE']
                prev_dx = patient_df.iloc[prev_idx]['all_dx']
                
                if prev_date >= lookback_date:
                    if len(current_dx & prev_dx) > 0:
                        matched_episode = episodes[prev_idx]
                        break
        else:
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
    
    return patient_df, episode_counter

output_file = f'episode_summary_{DX_DIGITS}digit_{TIME_WINDOW_DAYS}days.csv'

# Initialize variables
global_episode_id = 1
patient_state = {}
first_write = True

# Handle continuation mode
if CONTINUE_MODE and Path(output_file).exists():
    print(f"\n{'='*100}")
    print("CONTINUATION MODE ENABLED")
    print(f"{'='*100}")
    
    # Backup existing file
    backup_file = output_file.replace('.csv', '_backup.csv')
    shutil.copy2(output_file, backup_file)
    print(f"\nBackup created: {backup_file}")
    
    # Load existing data
    print(f"Loading existing data from {output_file}...")
    existing_data = pd.read_csv(output_file)
    print(f"  Existing episodes: {len(existing_data):,}")
    print(f"  Existing patients: {existing_data['ENROLID'].nunique():,}")
    
    # Find the max episode ID to continue numbering
    global_episode_id = existing_data['EPISODEID'].max() + 1
    print(f"  Max existing EPISODEID: {global_episode_id - 1}")
    print(f"  Next EPISODEID will be: {global_episode_id}")
    
    # Rebuild patient_state from 2021 data (year before restart)
    restart_idx = YEARS.index(RESTART_YEAR)
    if restart_idx > 0:
        prev_year = YEARS[restart_idx - 1]
        print(f"\n  Rebuilding patient state from {prev_year}...")
        
        prev_year_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_{prev_year}.parquet"
        
        try:
            # Get all patients who had visits in the previous year
            prev_year_episodes = existing_data[
                (existing_data['LATEST_DATE'] >= f'{prev_year}-01-01')
            ].copy()
            
            unique_patients = prev_year_episodes['ENROLID'].unique()
            print(f"  Found {len(unique_patients):,} patients with episodes in {prev_year}")
            
            # For each patient, load their last TIME_WINDOW_DAYS of data from prev_year
            patient_chunks = [unique_patients[i:i + PATIENT_CHUNK_SIZE] 
                            for i in range(0, len(unique_patients), PATIENT_CHUNK_SIZE)]
            
            for chunk_idx, patient_chunk in enumerate(patient_chunks, 1):
                print(f"    Chunk {chunk_idx}/{len(patient_chunks)}...", end=" ")
                
                patient_list = ', '.join([str(int(p)) for p in patient_chunk])
                
                query = f"""
                SELECT ENROLID, SVCDATE, DX1, DX2, DX3, DX4, NETPAY
                FROM '{prev_year_file}'
                WHERE ENROLID IN ({patient_list})
                AND SVCDATE IS NOT NULL
                """
                chunk_df = conn.execute(query).df()
                chunk_df['SVCDATE'] = pd.to_datetime(chunk_df['SVCDATE'])
                
                # For each patient, keep only last TIME_WINDOW_DAYS
                year_end = pd.Timestamp(f'{prev_year}-12-31')
                lookback_date = year_end - timedelta(days=TIME_WINDOW_DAYS)
                
                for patient_id in patient_chunk:
                    patient_data = chunk_df[
                        (chunk_df['ENROLID'] == patient_id) & 
                        (chunk_df['SVCDATE'] >= lookback_date)
                    ].copy()
                    
                    if len(patient_data) > 0:
                        # Get the max episode_id for this patient from existing data
                        patient_episodes = existing_data[existing_data['ENROLID'] == patient_id]
                        if len(patient_episodes) > 0:
                            max_episode_num = patient_episodes['EPISODEID'].max()
                            
                            # Assign episode IDs to carryover data (use existing episode IDs where possible)
                            # This is simplified - we'll let define_episodes recreate them
                            patient_state[patient_id] = {
                                'carryover_data': patient_data,
                                'next_episode_num': 1  # Will be properly set during processing
                            }
                
                print(f"{len(patient_state)} patients loaded")
            
            print(f"  Patient state rebuilt with {len(patient_state):,} patients")
            
        except Exception as e:
            print(f"  Warning: Could not rebuild patient state: {e}")
            print(f"  Continuing anyway - cross-year episodes from {prev_year} may not link properly")
    
    # Filter years to process
    YEARS = YEARS[YEARS.index(RESTART_YEAR):]
    print(f"\n  Will process years: {', '.join(YEARS)}")
    
    # Keep existing data, we'll append new years
    first_write = False
    
else:
    print("\nStarting fresh processing")
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
        year_patients = conn.execute(query).df()['ENROLID'].values
        print(f"\nUnique patients in {year}: {len(year_patients):,}")
    except Exception as e:
        print(f"Error accessing {year}: {e}")
        continue
    
    # Step 2: Process in chunks
    patient_chunks = [year_patients[i:i + PATIENT_CHUNK_SIZE] 
                     for i in range(0, len(year_patients), PATIENT_CHUNK_SIZE)]
    print(f"Processing in {len(patient_chunks)} chunks of up to {PATIENT_CHUNK_SIZE} patients")
    
    # Track state for next year
    new_patient_state = {}
    
    for chunk_idx, patient_chunk in enumerate(patient_chunks, 1):
        print(f"\n  Chunk {chunk_idx}/{len(patient_chunks)} ({len(patient_chunk)} patients)...", end=" ")
        
        patient_list = ', '.join([str(int(p)) for p in patient_chunk])
        
        # Load data for this chunk from current year
        try:
            query = f"""
            SELECT ENROLID, SVCDATE, DX1, DX2, DX3, DX4, NETPAY
            FROM '{file_path}'
            WHERE ENROLID IN ({patient_list})
            AND SVCDATE IS NOT NULL
            """
            chunk_df = conn.execute(query).df()
            
        except Exception as e:
            print(f"Error: {e}")
            continue
        
        if len(chunk_df) == 0:
            print("No data")
            continue
        
        chunk_df['SVCDATE'] = pd.to_datetime(chunk_df['SVCDATE'])
        chunk_df['NETPAY'] = pd.to_numeric(chunk_df['NETPAY'], errors='coerce').fillna(0)
        
        print(f"{len(chunk_df):,} visits", end=" ")
        
        # Process each patient
        chunk_episodes = []
        
        for patient_id in patient_chunk:
            patient_data = chunk_df[chunk_df['ENROLID'] == patient_id].copy()
            
            if len(patient_data) == 0:
                continue
            
            # Check if this patient has carryover data from previous year
            starting_episode_num = 1
            if patient_id in patient_state:
                carryover = patient_state[patient_id]['carryover_data']
                starting_episode_num = patient_state[patient_id]['next_episode_num']
                
                # Combine carryover with current year data
                patient_data = pd.concat([carryover, patient_data], ignore_index=True)
                patient_data = patient_data.sort_values('SVCDATE').reset_index(drop=True)
            
            # Define episodes
            patient_with_episodes, max_episode_num = define_episodes(
                patient_data, 
                dx_digits=DX_DIGITS, 
                time_window_days=TIME_WINDOW_DAYS,
                starting_episode_num=starting_episode_num
            )
            
            # Create unique global episode IDs
            unique_episode_ids = patient_with_episodes['episode_id'].unique()
            episode_id_mapping = {old_id: global_episode_id + i 
                                 for i, old_id in enumerate(sorted(unique_episode_ids))}
            
            patient_with_episodes['EPISODEID'] = patient_with_episodes['episode_id'].map(episode_id_mapping)
            
            # Update global counter
            global_episode_id += len(unique_episode_ids)
            
            chunk_episodes.append(patient_with_episodes)
            
            # Store carryover data for next year (last TIME_WINDOW_DAYS of visits)
            year_end = pd.Timestamp(f'{year}-12-31')
            lookback_date = year_end - timedelta(days=TIME_WINDOW_DAYS)
            
            carryover_data = patient_with_episodes[
                patient_with_episodes['SVCDATE'] >= lookback_date
            ].copy()
            
            if len(carryover_data) > 0:
                new_patient_state[patient_id] = {
                    'carryover_data': carryover_data,
                    'next_episode_num': max_episode_num + 1
                }
        
        if len(chunk_episodes) == 0:
            print()
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
        episode_summary = chunk_with_episodes.groupby(['ENROLID', 'EPISODEID']).agg({
            'SVCDATE': ['nunique', 'min', 'max'],
            'NETPAY': 'sum',
            'dx_codes': lambda x: sorted(list(set([code for sublist in x for code in sublist])))
        }).reset_index()
        
        episode_summary.columns = ['ENROLID', 'EPISODEID', 'UNIQUE_DATES', 
                                   'EARLIEST_DATE', 'LATEST_DATE', 'TOTAL_COST', 'UNIQUE_DX_CODES']
        
        # Convert diagnosis codes list to comma-separated string
        episode_summary['UNIQUE_DX_CODES'] = episode_summary['UNIQUE_DX_CODES'].apply(
            lambda x: ','.join(x) if len(x) > 0 else ''
        )
        
        print(f"-> {len(episode_summary):,} episodes")
        
        # Save to file
        if first_write:
            episode_summary.to_csv(output_file, index=False, mode='w')
            first_write = False
        else:
            episode_summary.to_csv(output_file, index=False, mode='a', header=False)
        
        # Clear memory
        del chunk_df, chunk_episodes, chunk_with_episodes, episode_summary
    
    # Update patient state for next year
    patient_state = new_patient_state
    print(f"\n  Patients with carryover to next year: {len(patient_state):,}")

print(f"\n{'='*100}")
print("PROCESSING COMPLETE")
print(f"{'='*100}")

# Read final results for summary statistics
print("\nLoading final results for summary...")
final_results = pd.read_csv(output_file)

print(f"\nFinal Dataset Summary:")
print(f"  Total episodes: {len(final_results):,}")
print(f"  Total patients: {final_results['ENROLID'].nunique():,}")
print(f"  Total cost: ${final_results['TOTAL_COST'].sum():,.2f}")
print(f"\nUnique dates per episode:")
print(final_results['UNIQUE_DATES'].describe())
print(f"\nCost per episode:")
print(final_results['TOTAL_COST'].describe())
print(f"\nSample of episodes with most unique dates:")
print(final_results.nlargest(5, 'UNIQUE_DATES')[['ENROLID', 'EPISODEID', 'UNIQUE_DATES', 
                                                   'EARLIEST_DATE', 'LATEST_DATE', 
                                                   'TOTAL_COST', 'UNIQUE_DX_CODES']])

print(f"\nOutput saved to: {output_file}")
print(f"Columns: {list(final_results.columns)}")

conn.close()
print("\nAnalysis complete!")
