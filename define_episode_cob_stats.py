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

# NO SAMPLING - Process all patients
SAMPLE_SIZE = None

# Parallel processing
N_PROCESSES = min(cpu_count() - 1, 4)  # Leave one core free, max 4 to avoid memory issues

# Directories
home_dir = os.path.expanduser('~')
output_dir = os.path.join(home_dir, 'episode_outputs')
temp_dir = os.path.join(home_dir, 'duckdb_temp')

os.makedirs(output_dir, exist_ok=True)
os.makedirs(temp_dir, exist_ok=True)

print("="*100)
print("EPISODE EXPENDITURE ANALYSIS - ALL PATIENTS")
print("="*100)
print(f"Years: {YEARS[0]}-{YEARS[-1]} | Processes: {N_PROCESSES}")
print(f"Episode Definition: {DX_DIGITS}-digit DX codes, {TIME_WINDOW_DAYS}-day window")
print("NOTE: Geographic variables not available in dataset - Tasks 1 & 2 only")
print("="*100)

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
    Process a single chunk of patients and return expenditures.
    This function is called in parallel.
    """
    patient_chunk, year, chunk_idx, total_chunks, file_path = args
    
    try:
        # Create a separate connection for this process
        conn = duckdb.connect()
        conn.execute(f"SET temp_directory='{temp_dir}'")
        conn.execute("SET memory_limit='2GB'")
        conn.execute("SET threads=1")  # Single thread per process since we're using multiprocessing
        
        patient_list = ','.join(map(str, map(int, patient_chunk)))
        
        # Load data - ONLY essential columns
        chunk_df = conn.execute(f"""
            SELECT ENROLID, SVCDATE, DX1, DX2, DX3, DX4, COB
            FROM '{file_path}'
            WHERE ENROLID IN ({patient_list}) 
              AND SVCDATE IS NOT NULL 
              AND DATATYP = 3
              AND COB IS NOT NULL
        """).df()
        
        conn.close()
        
        if len(chunk_df) == 0:
            return np.array([]), chunk_idx
        
        # Convert types
        chunk_df['SVCDATE'] = pd.to_datetime(chunk_df['SVCDATE'], errors='coerce')
        chunk_df['COB'] = pd.to_numeric(chunk_df['COB'], errors='coerce').fillna(0)
        chunk_df = chunk_df.dropna(subset=['SVCDATE'])
        
        # Define episodes
        chunk_df = define_episodes_optimized(chunk_df, DX_DIGITS, TIME_WINDOW_DAYS)
        
        # Sum expenditures per episode
        episode_expenditures = chunk_df.groupby(['ENROLID', 'episode_id'], sort=False)['COB'].sum().values
        
        # Clean up
        del chunk_df
        gc.collect()
        
        return episode_expenditures, chunk_idx
        
    except Exception as e:
        print(f"[{year}] Error in chunk {chunk_idx}: {e}")
        import traceback
        traceback.print_exc()
        return np.array([]), chunk_idx

def process_year_parallel(year):
    """
    Process a single year using parallel processing.
    Returns array of all episode expenditures for the year.
    """
    start_time = time.time()
    process = psutil.Process()
    initial_memory = process.memory_info().rss / (1024**3)
    
    print(f"\n[{year}] Starting | Initial Memory: {initial_memory:.2f} GB")
    
    file_path = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_{year}.parquet"
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"[{year}] ERROR: File not found: {file_path}")
        return None
    
    # Get ALL patients (no sampling)
    try:
        conn = duckdb.connect()
        all_patients = conn.execute(f"""
            SELECT DISTINCT ENROLID FROM '{file_path}' 
            WHERE ENROLID IS NOT NULL AND DATATYP = 3
        """).df()['ENROLID'].values
        conn.close()
        
        print(f"[{year}] Found {len(all_patients):,} patients")
        
    except Exception as e:
        print(f"[{year}] Error loading patient list: {e}")
        return None
    
    # Create chunks
    chunks = [all_patients[i:i + PATIENT_CHUNK_SIZE] for i in range(0, len(all_patients), PATIENT_CHUNK_SIZE)]
    total_chunks = len(chunks)
    print(f"[{year}] Processing {total_chunks:,} chunks with {N_PROCESSES} parallel processes")
    
    # Prepare arguments for parallel processing
    chunk_args = [(chunk, year, idx+1, total_chunks, file_path) for idx, chunk in enumerate(chunks)]
    
    # Process chunks in parallel
    all_expenditures = []
    
    with Pool(processes=N_PROCESSES) as pool:
        # Use imap for progress tracking
        results = pool.imap(process_patient_chunk, chunk_args)
        
        for expenditures, chunk_idx in results:
            if len(expenditures) > 0:
                all_expenditures.extend(expenditures)
            
            # Progress update every 50 chunks
            if chunk_idx % 50 == 0:
                current_memory = process.memory_info().rss / (1024**3)
                elapsed = time.time() - start_time
                rate = chunk_idx / elapsed * 60  # chunks per minute
                eta = (total_chunks - chunk_idx) / rate if rate > 0 else 0
                print(f"[{year}] Progress: {chunk_idx}/{total_chunks} | "
                      f"Episodes: {len(all_expenditures):,} | "
                      f"Memory: {current_memory:.1f}GB | "
                      f"Rate: {rate:.1f} chunks/min | "
                      f"ETA: {eta:.1f}m")
    
    # Convert to numpy array
    all_expenditures = np.array(all_expenditures)
    
    total_time = time.time() - start_time
    final_memory = process.memory_info().rss / (1024**3)
    
    print(f"\n[{year}] ✓ COMPLETE!")
    print(f"  Time: {total_time/60:.1f} minutes")
    print(f"  Total Episodes: {len(all_expenditures):,}")
    print(f"  Total Expenditure: ${np.sum(all_expenditures):,.2f}")
    print(f"  Mean Expenditure: ${np.mean(all_expenditures):,.2f}")
    print(f"  Final Memory: {final_memory:.2f} GB")
    
    return all_expenditures

def compute_statistics(expenditures, year):
    """
    Compute the requested statistics with high accuracy.
    
    Task 1: Deciles of expenditures per episode (10 numbers)
    Task 2: Minimum expenditures (overall, 1st percentile, 5th percentile)
    """
    if len(expenditures) == 0:
        print(f"[{year}] WARNING: No expenditures to compute statistics")
        return None
    
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
    print(f"\n[{year}] EXPENDITURE STATISTICS:")
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

def create_histograms(expenditures, year):
    """
    Create the requested histograms (Task 3A and 3B).
    Task 3C (LA County) cannot be completed without geographic data.
    
    A) The whole distribution, from the 0th percentile to the 95th percentile
    B) Zooming in, show only the 0th percentile to the 10th percentile
    C) LA County - NOT AVAILABLE (no geographic data in dataset)
    """
    if len(expenditures) == 0:
        print(f"[{year}] WARNING: No expenditures to create histograms")
        return
    
    print(f"\n[{year}] Creating histograms...")
    
    # Calculate percentiles for filtering
    p95 = np.percentile(expenditures, 95)
    p10 = np.percentile(expenditures, 10)
    
    fig, axes = plt.subplots(1, 3, figsize=(20, 6))
    
    # A) Full distribution (0th to 95th percentile)
    filtered_a = expenditures[expenditures <= p95]
    axes[0].hist(filtered_a, bins=100, edgecolor='black', alpha=0.7)
    axes[0].set_title(f'{year}: Full Distribution (0-95th percentile)\nn={len(filtered_a):,} episodes', 
                     fontsize=12, fontweight='bold')
    axes[0].set_xlabel('Expenditure per Episode ($)', fontsize=11)
    axes[0].set_ylabel('Frequency', fontsize=11)
    axes[0].axvline(np.median(filtered_a), color='red', linestyle='--', linewidth=2, 
                   label=f'Median: ${np.median(filtered_a):,.2f}')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    
    # B) Lower tail (0th to 10th percentile) - Overall
    filtered_b = expenditures[expenditures <= p10]
    axes[1].hist(filtered_b, bins=50, edgecolor='black', alpha=0.7, color='orange')
    axes[1].set_title(f'{year}: Lower Tail (0-10th percentile)\nn={len(filtered_b):,} episodes', 
                     fontsize=12, fontweight='bold')
    axes[1].set_xlabel('Expenditure per Episode ($)', fontsize=11)
    axes[1].set_ylabel('Frequency', fontsize=11)
    
    # Check for clustering around minimum k
    min_val = np.min(filtered_b)
    # Count episodes within $10 of minimum
    near_min = np.sum((filtered_b >= min_val) & (filtered_b <= min_val + 10))
    axes[1].axvline(min_val, color='red', linestyle='--', linewidth=2, 
                   label=f'Min: ${min_val:,.2f}\n{near_min:,} episodes within $10')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    
    # C) LA County - Not available
    axes[2].text(0.5, 0.5, 
                'LA County Analysis\nNOT AVAILABLE\n\nGeographic variables not included\nin dataset purchase', 
                ha='center', va='center', transform=axes[2].transAxes, 
                fontsize=13, bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8))
    axes[2].set_title(f'{year}: LA County (0-10th percentile)', fontsize=12, fontweight='bold')
    axes[2].set_xlabel('Expenditure per Episode ($)', fontsize=11)
    axes[2].set_ylabel('Frequency', fontsize=11)
    
    plt.tight_layout()
    
    histogram_file = os.path.join(output_dir, f'expenditure_histograms_{year}.png')
    plt.savefig(histogram_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Histograms saved to: {histogram_file}")
    
    # Additional analysis: Check for clustering at minimum (as mentioned in email)
    print(f"\n[{year}] CLUSTERING ANALYSIS (Lower Tail):")
    print(f"  Minimum value: ${min_val:,.2f}")
    print(f"  Episodes within $10 of min: {near_min:,} ({near_min/len(filtered_b)*100:.1f}% of lower tail)")
    print(f"  Episodes within $50 of min: {np.sum((filtered_b >= min_val) & (filtered_b <= min_val + 50)):,}")
    print(f"  Episodes within $100 of min: {np.sum((filtered_b >= min_val) & (filtered_b <= min_val + 100)):,}")

def save_expenditure_data(expenditures, year):
    """
    Save the raw expenditure data for potential further analysis.
    """
    print(f"\n[{year}] Saving raw expenditure data...")
    
    # Save as compressed numpy array for efficiency
    expenditure_file = os.path.join(output_dir, f'expenditures_{year}.npy')
    np.save(expenditure_file, expenditures)
    
    file_size = os.path.getsize(expenditure_file) / (1024**2)
    print(f"  Raw data saved to: {expenditure_file} ({file_size:.1f} MB)")
    
    return expenditure_file

if __name__ == '__main__':
    overall_start = time.time()
    
    all_year_stats = []
    expenditure_files = {}
    
    # Process each year
    for year in YEARS:
        print(f"\n{'='*100}")
        print(f"PROCESSING YEAR: {year}")
        print(f"{'='*100}")
        
        try:
            # Extract expenditures for this year (parallel processing)
            expenditures = process_year_parallel(year)
            
            if expenditures is None or len(expenditures) == 0:
                print(f"\n[{year}] ✗ No data processed")
                continue
            
            # Task 1 & 2: Compute statistics
            stats = compute_statistics(expenditures, year)
            if stats is not None:
                all_year_stats.append(stats)
            
            # Task 3: Create histograms (A and B only)
            create_histograms(expenditures, year)
            
            # Save raw data
            exp_file = save_expenditure_data(expenditures, year)
            expenditure_files[year] = exp_file
            
            print(f"\n[{year}] ✓ Year complete")
            
            # Clean up to free memory for next year
            del expenditures
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
        combined_file = os.path.join(output_dir, 'expenditure_statistics_all_years.csv')
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
        # Sort columns properly
        col_order = [f'p{i}0' for i in range(1, 10)]
        decile_pivot = decile_pivot[[c for c in col_order if c in decile_pivot.columns]]
        print(decile_pivot.to_string())
    
    overall_time = time.time() - overall_start
    
    print(f"\n{'='*100}")
    print(f"ALL PROCESSING COMPLETE")
    print(f"{'='*100}")
    print(f"Total time: {overall_time/60:.1f} minutes ({overall_time/3600:.1f} hours)")
    print(f"Years processed: {len(all_year_stats)}/{len(YEARS)}")
    print(f"Output directory: {output_dir}")
    print(f"\nFiles generated:")
    print(f"  ✓ expenditure_statistics_YYYY.csv (per year)")
    print(f"  ✓ expenditure_statistics_all_years.csv (combined)")
    print(f"  ✓ expenditure_histograms_YYYY.png (per year - panels A & B)")
    print(f"  ✓ expenditures_YYYY.npy (raw data per year)")
    print(f"\nNOTE: Task 3C (LA County histogram) not completed - geographic data not in dataset")
    print("="*100)
