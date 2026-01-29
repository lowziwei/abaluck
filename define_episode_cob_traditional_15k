import duckdb
import pandas as pd
from datetime import timedelta
import numpy as np
import os
import time
import psutil
import gc
import matplotlib.pyplot as plt
from multiprocessing import Pool, cpu_count
import warnings
warnings.filterwarnings('ignore')

# Settings
DATASET_TYPE = "MEDICARE_SET_A"
DATABASE = "MDCR"
TABLE_CODE = "O"
YEARS = [str(year) for year in range(2014, 2025)]  # 2014-2024

# Episode definition parameters
DX_DIGITS = 3
TIME_WINDOW_DAYS = 100
PATIENT_CHUNK_SIZE = 2000

# SAMPLING PARAMETER - Set to None to process all patients
SAMPLE_SIZE = 15000  # Set to None for all patients, or a number like 15000 for sampling

# Parallel processing
N_PROCESSES = min(cpu_count() - 1, 4)  # Leave one core free, max 4 to avoid memory issues

# RVU file path template
RVU_BASE_PATH = '/home/zl749/rvu_PPRRVUXX_partD.csv'

# Directories
home_dir = os.path.expanduser('~')
output_dir = os.path.join(home_dir, 'episode_outputs')
temp_dir = os.path.join(home_dir, 'duckdb_temp')

os.makedirs(output_dir, exist_ok=True)
os.makedirs(temp_dir, exist_ok=True)

# Set random seed for reproducibility
np.random.seed(42)

# Define facility vs non-facility place of service codes
FACILITY_POS = {21, 22, 23, 24, 26, 31, 32, 33, 34, 51, 52, 53, 54, 55, 56, 61}
NON_FACILITY_POS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 
                    19, 20, 25, 27, 28, 35, 41, 42, 49, 50, 57, 60, 62, 65, 71, 72, 
                    81, 95, 98, 99}

print("="*100)
print("EPISODE EXPENDITURE ANALYSIS WITH RVU DATA")
print("="*100)
print(f"Years: {YEARS[0]}-{YEARS[-1]} | Processes: {N_PROCESSES}")
if SAMPLE_SIZE:
    print(f"SAMPLING: {SAMPLE_SIZE:,} patients per year")
else:
    print("SAMPLING: Processing ALL patients (no sampling)")
print(f"Episode Definition: {DX_DIGITS}-digit DX codes, {TIME_WINDOW_DAYS}-day window")
print("="*100)

def load_rvu_data(year):
    """Load RVU data for a specific year"""
    year_suffix = str(year)[2:]  # Get last 2 digits
    rvu_path = RVU_BASE_PATH.replace('XX', year_suffix)
    
    try:
        rvu_df = pd.read_csv(rvu_path)
        
        # Rename HCPS to PROC1 if needed
        if 'HCPS' in rvu_df.columns and 'PROC1' not in rvu_df.columns:
            rvu_df = rvu_df.rename(columns={'HCPS': 'PROC1'})
        
        print(f"  [{year}] Loaded RVU data: {len(rvu_df):,} codes")
        return rvu_df
        
    except FileNotFoundError:
        print(f"  [{year}] WARNING: RVU file not found: {rvu_path}")
        return None
    except Exception as e:
        print(f"  [{year}] ERROR loading RVU data: {e}")
        return None

def define_episodes_optimized(df, dx_digits=3, time_window_days=100):
    """
    OPTIMIZED: Episode assignment with rolling window diagnosis check.
    Uses efficient vectorized operations where possible, with targeted iteration for complex logic.
    """
    if len(df) == 0:
        return df

    # Sort once (required for logic)
    df = df.sort_values(['ENROLID', 'SVCDATE']).reset_index(drop=True)

    # Vectorized: Clean and truncate diagnosis codes in-place
    for col in ['DX1', 'DX2', 'DX3', 'DX4']:
        df[col] = df[col].astype(str)
        df[col] = df[col].replace(['', 'nan', 'None', 'Non', ' ', 'nat', 'NaN', 'NaT'], np.nan)
        df[col] = df[col].str[:dx_digits]
        # Clean again after truncation
        df[col] = df[col].replace(['', 'nan', 'Non', ' ', 'NaN'], np.nan)

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

def process_patient_chunk(args):
    """
    Process a single chunk of patients and return expenditures and RVU data.
    This function is called in parallel.
    """
    patient_chunk, year, chunk_idx, total_chunks, file_path, rvu_df = args
    
    try:
        # Create a separate connection for this process
        conn = duckdb.connect()
        conn.execute(f"SET temp_directory='{temp_dir}'")
        conn.execute("SET memory_limit='2GB'")
        conn.execute("SET threads=1")  # Single thread per process since we're using multiprocessing
        
        patient_list = ','.join(map(str, map(int, patient_chunk)))
        
        # Determine if we need to filter MEDADV
        year_int = int(year)
        filter_medadv = (year_int >= 2020 and year_int <= 2025)
        
        # Build query with conditional MEDADV filter
        if filter_medadv:
            medadv_clause = "AND (MEDADV = 0 OR MEDADV IS NULL)"
        else:
            medadv_clause = ""
        
        # Load data - NOW including payment columns and PROC1, STDPLAC
        chunk_df = conn.execute(f"""
            SELECT ENROLID, SVCDATE, DX1, DX2, DX3, DX4, 
                   COB, COINS, COPAY, DEDUCT, NETPAY, PAY,
                   PROC1, STDPLAC
            FROM '{file_path}'
            WHERE ENROLID IN ({patient_list}) 
              AND SVCDATE IS NOT NULL 
              AND DATATYP = 3
              {medadv_clause}
        """).df()
        
        conn.close()
        
        if len(chunk_df) == 0:
            return None, chunk_idx
        
        # Convert types
        chunk_df['SVCDATE'] = pd.to_datetime(chunk_df['SVCDATE'], errors='coerce')
        
        # Convert payment columns to numeric
        payment_cols = ['COB', 'COINS', 'COPAY', 'DEDUCT', 'NETPAY', 'PAY']
        for col in payment_cols:
            if col in chunk_df.columns:
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce').fillna(0)
        
        chunk_df = chunk_df.dropna(subset=['SVCDATE'])
        
        # Merge with RVU data if available
        if rvu_df is not None and 'PROC1' in chunk_df.columns:
            # Clean PROC1 in chunk_df
            chunk_df['PROC1'] = chunk_df['PROC1'].astype(str).str.strip()
            
            # Merge (inner join - keep only matches)
            pre_merge_len = len(chunk_df)
            chunk_df = chunk_df.merge(rvu_df, on='PROC1', how='inner')
            post_merge_len = len(chunk_df)
            
            if chunk_idx % 50 == 0:  # Only print occasionally to avoid spam
                print(f"  [{year}] Chunk {chunk_idx}: RVU merge kept {post_merge_len:,}/{pre_merge_len:,} records")
            
            # Create PE_RVU_actualized based on STDPLAC
            if 'STDPLAC' in chunk_df.columns:
                chunk_df['PE_RVU_actualized'] = np.nan
                
                # Find PE RVU columns (try different naming conventions)
                pe_facility_col = None
                pe_nonfacility_col = None
                
                for col in chunk_df.columns:
                    col_lower = col.lower()
                    if 'pe' in col_lower and 'facility' in col_lower and 'non' not in col_lower:
                        pe_facility_col = col
                    elif 'pe' in col_lower and ('nonfacility' in col_lower or 'non_facility' in col_lower):
                        pe_nonfacility_col = col
                
                if pe_facility_col and pe_nonfacility_col:
                    # Convert to numeric
                    chunk_df[pe_facility_col] = pd.to_numeric(chunk_df[pe_facility_col], errors='coerce')
                    chunk_df[pe_nonfacility_col] = pd.to_numeric(chunk_df[pe_nonfacility_col], errors='coerce')
                    
                    # Apply facility PE RVU
                    facility_mask = chunk_df['STDPLAC'].isin(FACILITY_POS)
                    chunk_df.loc[facility_mask, 'PE_RVU_actualized'] = chunk_df.loc[facility_mask, pe_facility_col]
                    
                    # Apply non-facility PE RVU
                    nonfacility_mask = chunk_df['STDPLAC'].isin(NON_FACILITY_POS)
                    chunk_df.loc[nonfacility_mask, 'PE_RVU_actualized'] = chunk_df.loc[nonfacility_mask, pe_nonfacility_col]
                    
                    # Apply AVERAGE for NEITHER category (codes not in either list)
                    neither_mask = ~facility_mask & ~nonfacility_mask & chunk_df['STDPLAC'].notna()
                    if neither_mask.sum() > 0:
                        chunk_df.loc[neither_mask, 'PE_RVU_actualized'] = (
                            chunk_df.loc[neither_mask, pe_facility_col] + 
                            chunk_df.loc[neither_mask, pe_nonfacility_col]
                        ) / 2.0
                        
                        if chunk_idx % 50 == 0:
                            print(f"  [{year}] Chunk {chunk_idx}: {neither_mask.sum():,} records using averaged PE RVU (NEITHER category)")
        
        # Define episodes
        chunk_df = define_episodes_optimized(chunk_df, DX_DIGITS, TIME_WINDOW_DAYS)
        
        # Prepare aggregation dictionary
        agg_dict = {}
        
        # Add payment columns
        for col in ['COB', 'COINS', 'COPAY', 'DEDUCT', 'NETPAY', 'PAY']:
            if col in chunk_df.columns:
                agg_dict[col] = 'sum'
        
        # Add RVU columns if they exist
        if 'PE_RVU_actualized' in chunk_df.columns:
            agg_dict['PE_RVU_actualized'] = 'sum'
        
        # Find Work RVU and MP RVU columns
        for col in chunk_df.columns:
            col_lower = col.lower()
            if 'work' in col_lower and 'rvu' in col_lower and col not in agg_dict:
                agg_dict[col] = 'sum'
            elif ('mp' in col_lower or 'malpractice' in col_lower) and 'rvu' in col_lower and col not in agg_dict:
                agg_dict[col] = 'sum'
        
        # Aggregate at episode level
        episode_data = chunk_df.groupby(['ENROLID', 'episode_id'], sort=False).agg(agg_dict).reset_index()
        
        # Clean up
        del chunk_df
        gc.collect()
        
        return episode_data, chunk_idx
        
    except Exception as e:
        print(f"[{year}] Error in chunk {chunk_idx}: {e}")
        import traceback
        traceback.print_exc()
        return None, chunk_idx

def process_year_parallel(year):
    """
    Process a single year using parallel processing.
    Returns DataFrame of all episode data for the year.
    """
    start_time = time.time()
    process = psutil.Process()
    initial_memory = process.memory_info().rss / (1024**3)
    
    print(f"\n[{year}] Starting | Initial Memory: {initial_memory:.2f} GB")
    
    # Load RVU data for this year
    rvu_df = load_rvu_data(year)
    
    file_path = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_update_{year}.parquet"
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"[{year}] ERROR: File not found: {file_path}")
        return None
    
    # Get patients and apply sampling if needed
    try:
        conn = duckdb.connect()
        all_patients = conn.execute(f"""
            SELECT DISTINCT ENROLID FROM '{file_path}' 
            WHERE ENROLID IS NOT NULL AND DATATYP = 3
        """).df()['ENROLID'].values
        conn.close()
        
        print(f"[{year}] Found {len(all_patients):,} total patients")
        
        # Apply sampling
        if SAMPLE_SIZE and len(all_patients) > SAMPLE_SIZE:
            year_seed = 42 + int(year)  # Different seed per year
            np.random.seed(year_seed)
            patients = np.random.choice(all_patients, size=SAMPLE_SIZE, replace=False)
            print(f"[{year}] Sampled {SAMPLE_SIZE:,} patients for processing")
        else:
            patients = all_patients
            print(f"[{year}] Processing all {len(patients):,} patients (no sampling)")
        
    except Exception as e:
        print(f"[{year}] Error loading patient list: {e}")
        return None
    
    # Create chunks
    chunks = [patients[i:i + PATIENT_CHUNK_SIZE] for i in range(0, len(patients), PATIENT_CHUNK_SIZE)]
    total_chunks = len(chunks)
    print(f"[{year}] Processing {total_chunks:,} chunks with {N_PROCESSES} parallel processes")
    
    # Prepare arguments for parallel processing
    chunk_args = [(chunk, year, idx+1, total_chunks, file_path, rvu_df) for idx, chunk in enumerate(chunks)]
    
    # Process chunks in parallel
    all_episode_data = []
    
    with Pool(processes=N_PROCESSES) as pool:
        # Use imap for progress tracking
        results = pool.imap(process_patient_chunk, chunk_args)
        
        for episode_data, chunk_idx in results:
            if episode_data is not None and len(episode_data) > 0:
                all_episode_data.append(episode_data)
            
            # Progress update every 50 chunks
            if chunk_idx % 50 == 0:
                current_memory = process.memory_info().rss / (1024**3)
                elapsed = time.time() - start_time
                rate = chunk_idx / elapsed * 60  # chunks per minute
                eta = (total_chunks - chunk_idx) / rate if rate > 0 else 0
                total_episodes = sum(len(df) for df in all_episode_data)
                print(f"[{year}] Progress: {chunk_idx}/{total_chunks} | "
                      f"Episodes: {total_episodes:,} | "
                      f"Memory: {current_memory:.1f}GB | "
                      f"Rate: {rate:.1f} chunks/min | "
                      f"ETA: {eta:.1f}m")
    
    # Combine all episode data
    if len(all_episode_data) == 0:
        print(f"[{year}] No episode data collected")
        return None
    
    combined_df = pd.concat(all_episode_data, ignore_index=True)
    
    total_time = time.time() - start_time
    final_memory = process.memory_info().rss / (1024**3)
    
    print(f"\n[{year}] ✓ COMPLETE!")
    print(f"  Time: {total_time/60:.1f} minutes")
    print(f"  Total Episodes: {len(combined_df):,}")
    
    # Print summary for all payment columns
    payment_cols = ['COB', 'COINS', 'COPAY', 'DEDUCT', 'NETPAY', 'PAY']
    for col in payment_cols:
        if col in combined_df.columns:
            print(f"  Total {col}: ${combined_df[col].sum():,.2f}")
            print(f"  Mean {col}: ${combined_df[col].mean():,.2f}")
    
    # Print RVU summary if available
    rvu_cols = ['PE_RVU_actualized']
    for col in combined_df.columns:
        col_lower = col.lower()
        if 'work' in col_lower and 'rvu' in col_lower:
            rvu_cols.append(col)
        elif ('mp' in col_lower or 'malpractice' in col_lower) and 'rvu' in col_lower:
            rvu_cols.append(col)
    
    for col in rvu_cols:
        if col in combined_df.columns:
            print(f"  Total {col}: {combined_df[col].sum():,.2f}")
            print(f"  Mean {col}: {combined_df[col].mean():,.2f}")
    
    print(f"  Final Memory: {final_memory:.2f} GB")
    
    return combined_df

def compute_statistics(df, year):
    """
    Compute the requested statistics with high accuracy.
    
    Task 1: Deciles of expenditures per episode (10 numbers)
    Task 2: Minimum expenditures (overall, 1st percentile, 5th percentile)
    
    Uses PAY as the primary expenditure measure.
    """
    if df is None or len(df) == 0:
        print(f"[{year}] WARNING: No data to compute statistics")
        return None
    
    # Use PAY instead of COB
    if 'PAY' not in df.columns:
        print(f"[{year}] ERROR: PAY column not found in data")
        return None
    
    expenditures = df['PAY'].values
    
    print(f"\n[{year}] Computing statistics on {len(expenditures):,} episodes...")
    
    # Task 1: Deciles (10th, 20th, ..., 90th percentiles)
    deciles = np.percentile(expenditures, np.arange(10, 100, 10))
    
    # Task 2: Minimum statistics
    overall_min = np.min(expenditures)
    percentile_1 = np.percentile(expenditures, 1)
    percentile_5 = np.percentile(expenditures, 5)
    
    # Additional useful statistics
    median = np.median(expenditures)
    mean = np.mean(expenditures)
    percentile_95 = np.percentile(expenditures, 95)
    percentile_99 = np.percentile(expenditures, 99)
    
    # Create summary dataframe
    stats_rows = []
    
    # Minimum statistics (Task 2)
    stats_rows.append({'year': year, 'statistic': 'minimum_overall', 'value': overall_min})
    stats_rows.append({'year': year, 'statistic': 'minimum_p1', 'value': percentile_1})
    stats_rows.append({'year': year, 'statistic': 'minimum_p5', 'value': percentile_5})
    
    # Deciles (Task 1)
    for i, decile_value in enumerate(deciles, start=1):
        stats_rows.append({'year': year, 'statistic': f'p{i}0', 'value': decile_value})
    
    # Additional statistics
    stats_rows.append({'year': year, 'statistic': 'mean', 'value': mean})
    stats_rows.append({'year': year, 'statistic': 'median', 'value': median})
    stats_rows.append({'year': year, 'statistic': 'p95', 'value': percentile_95})
    stats_rows.append({'year': year, 'statistic': 'p99', 'value': percentile_99})
    
    stats_df = pd.DataFrame(stats_rows)
    
    # Save to CSV
    stats_file = os.path.join(output_dir, f'expenditure_statistics_{year}.csv')
    stats_df.to_csv(stats_file, index=False)
    
    # Print summary
    print(f"\n[{year}] PAY STATISTICS:")
    print(f"  Minimum (overall): ${overall_min:,.2f}")
    print(f"  Minimum (1st %ile): ${percentile_1:,.2f}")
    print(f"  Minimum (5th %ile): ${percentile_5:,.2f}")
    print(f"  Deciles:")
    for i, val in enumerate(deciles, start=1):
        print(f"    {i}0th percentile: ${val:,.2f}")
    print(f"  Mean: ${mean:,.2f}")
    print(f"  Median: ${median:,.2f}")
    print(f"  95th percentile: ${percentile_95:,.2f}")
    print(f"  Statistics saved to: {stats_file}")
    
    return stats_df

def create_histograms(df, year):
    """
    Create the requested histograms (Task 3A and 3B).
    Uses PAY as the expenditure measure.
    """
    if df is None or len(df) == 0:
        print(f"[{year}] WARNING: No data to create histograms")
        return
    
    if 'PAY' not in df.columns:
        print(f"[{year}] ERROR: PAY column not found in data")
        return
    
    expenditures = df['PAY'].values
    
    print(f"\n[{year}] Creating histograms...")
    
    # Calculate percentiles for filtering
    p95 = np.percentile(expenditures, 95)
    p10 = np.percentile(expenditures, 10)
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # A) Full distribution (0th to 95th percentile)
    filtered_a = expenditures[expenditures <= p95]
    axes[0].hist(filtered_a, bins=100, edgecolor='black', alpha=0.7)
    axes[0].set_title(f'{year}: Full Distribution (0-95th percentile)\nn={len(filtered_a):,} episodes', 
                     fontsize=12, fontweight='bold')
    axes[0].set_xlabel('PAY per Episode ($)', fontsize=11)
    axes[0].set_ylabel('Frequency', fontsize=11)
    axes[0].axvline(np.median(filtered_a), color='red', linestyle='--', linewidth=2, 
                   label=f'Median: ${np.median(filtered_a):,.2f}')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    
    # B) Lower tail (0th to 10th percentile)
    filtered_b = expenditures[expenditures <= p10]
    axes[1].hist(filtered_b, bins=50, edgecolor='black', alpha=0.7, color='orange')
    axes[1].set_title(f'{year}: Lower Tail (0-10th percentile)\nn={len(filtered_b):,} episodes', 
                     fontsize=12, fontweight='bold')
    axes[1].set_xlabel('PAY per Episode ($)', fontsize=11)
    axes[1].set_ylabel('Frequency', fontsize=11)
    
    min_val = np.min(filtered_b)
    near_min = np.sum((filtered_b >= min_val) & (filtered_b <= min_val + 10))
    axes[1].axvline(min_val, color='red', linestyle='--', linewidth=2, 
                   label=f'Min: ${min_val:,.2f}\n{near_min:,} episodes within $10')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    histogram_file = os.path.join(output_dir, f'expenditure_histograms_{year}.png')
    plt.savefig(histogram_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Histograms saved to: {histogram_file}")
    
    # Additional analysis: Check for clustering at minimum
    print(f"\n[{year}] CLUSTERING ANALYSIS (Lower Tail):")
    print(f"  Minimum PAY value: ${min_val:,.2f}")
    print(f"  Episodes within $10 of min: {near_min:,} ({near_min/len(filtered_b)*100:.1f}% of lower tail)")
    print(f"  Episodes within $50 of min: {np.sum((filtered_b >= min_val) & (filtered_b <= min_val + 50)):,}")
    print(f"  Episodes within $100 of min: {np.sum((filtered_b >= min_val) & (filtered_b <= min_val + 100)):,}")

def save_episode_data(df, year):
    """
    Save the episode-level data with RVU information.
    """
    print(f"\n[{year}] Saving episode-level data...")
    
    # Save as parquet for efficiency
    sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
    episode_file = os.path.join(output_dir, f'episodes_{year}{sample_suffix}.parquet')
    df.to_parquet(episode_file, index=False)
    
    file_size = os.path.getsize(episode_file) / (1024**2)
    print(f"  Episode data saved to: {episode_file} ({file_size:.1f} MB)")
    
    return episode_file

if __name__ == '__main__':
    overall_start = time.time()
    
    all_year_stats = []
    episode_files = {}
    
    # Process each year
    for year in YEARS:
        print(f"\n{'='*100}")
        print(f"PROCESSING YEAR: {year}")
        print(f"{'='*100}")
        
        try:
            # Extract episode data for this year (parallel processing)
            episode_df = process_year_parallel(year)
            
            if episode_df is None or len(episode_df) == 0:
                print(f"\n[{year}] ✗ No data processed")
                continue
            
            # Task 1 & 2: Compute statistics
            stats = compute_statistics(episode_df, year)
            if stats is not None:
                all_year_stats.append(stats)
            
            # Task 3: Create histograms
            create_histograms(episode_df, year)
            
            # Save episode data
            ep_file = save_episode_data(episode_df, year)
            episode_files[year] = ep_file
            
            print(f"\n[{year}] ✓ Year complete")
            
            # Clean up to free memory for next year
            del episode_df
            gc.collect()
            
        except Exception as e:
            print(f"\n[{year}] ✗ Error: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Combine statistics across all years
    if len(all_year_stats) > 0:
        print(f"\n{'='*100}")
        print("COMBINING RESULTS ACROSS ALL YEARS")
        print(f"{'='*100}")
        
        combined_stats = pd.concat(all_year_stats, ignore_index=True)
        sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
        combined_file = os.path.join(output_dir, f'expenditure_statistics_all_years{sample_suffix}.csv')
        combined_stats.to_csv(combined_file, index=False)
        
        print(f"\n✓ Combined statistics saved to: {combined_file}")
        
        # Print summary table
        print("\n" + "="*100)
        print("SUMMARY TABLE (Minimum Statistics)")
        print("="*100)
        min_stats = combined_stats[combined_stats['statistic'].isin(['minimum_overall', 'minimum_p1', 'minimum_p5'])]
        pivot = min_stats.pivot(index='year', columns='statistic', values='value')
        print(pivot.to_string())
        
        print("\n" + "="*100)
        print("SUMMARY TABLE (Deciles)")
        print("="*100)
        decile_stats = combined_stats[combined_stats['statistic'].str.match(r'^p\d0$')]
        decile_pivot = decile_stats.pivot(index='year', columns='statistic', values='value')
        col_order = [f'p{i}0' for i in range(1, 10)]
        decile_pivot = decile_pivot[[c for c in col_order if c in decile_pivot.columns]]
        print(decile_pivot.to_string())
    
    overall_time = time.time() - overall_start
    
    print(f"\n{'='*100}")
    print(f"ALL PROCESSING COMPLETE")
    print(f"{'='*100}")
    print(f"Total time: {overall_time/60:.1f} minutes ({overall_time/3600:.1f} hours)")
    print(f"Years processed: {len(all_year_stats)}/{len(YEARS)}")
    if SAMPLE_SIZE:
        print(f"Sample size: {SAMPLE_SIZE:,} patients per year")
    else:
        print("Sample size: ALL patients (no sampling)")
    print(f"Output directory: {output_dir}")
    print(f"\nFiles generated:")
    print(f"  ✓ expenditure_statistics_YYYY.csv (per year)")
    print(f"  ✓ expenditure_statistics_all_years{sample_suffix}.csv (combined)")
    print(f"  ✓ expenditure_histograms_YYYY.png (per year - 2 panels)")
    print(f"  ✓ episodes_YYYY{sample_suffix}.parquet (episode-level data with RVUs)")
    print("="*100)
