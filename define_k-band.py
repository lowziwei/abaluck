import duckdb
import pandas as pd
import numpy as np
import os
import time
import gc

# Settings
DATASET_TYPE = "MEDICARE_SET_A"
DATABASE = "MDCR"
TABLE_CODE = "O"
YEARS = [str(year) for year in range(2014, 2025)]
SAMPLE_SIZE = 15000
DX_DIGITS = 3
TIME_WINDOW_DAYS = 100

# RVU file path template
RVU_BASE_PATH = '/home/zl749/rvu_PPRRVUXX_partD.csv'

# Directories
home_dir = os.path.expanduser('~')
output_dir = os.path.join(home_dir, 'episode_outputs')
temp_dir = os.path.join(home_dir, 'duckdb_temp')
micro_data_dir = os.path.join(home_dir, 'micro_data')  # NEW: Directory for claim-level data

os.makedirs(output_dir, exist_ok=True)
os.makedirs(temp_dir, exist_ok=True)
os.makedirs(micro_data_dir, exist_ok=True)  # NEW

np.random.seed(42)

# Define facility vs non-facility place of service codes
FACILITY_POS = {2, 19, 21, 22, 23, 24, 26, 31, 34, 41, 42, 51, 52, 53, 56, 61}
NON_FACILITY_POS = {1, 3, 4, 11, 12, 13, 14, 15, 16, 17, 20, 25, 32, 33, 49, 50, 54, 55, 57, 60, 62, 65, 71, 72, 81, 99}

# NEW: Toggle to use saved micro data or regenerate
USE_SAVED_MICRO_DATA = False  # Set to True to load existing micro data, False to regenerate

print("="*100)
print("EPISODE ANALYSIS WITH K-BAND CLASSIFICATION (LEVEL 1 CPT CODES ONLY)")
print("="*100)
print(f"Sample: {SAMPLE_SIZE:,} patients per year")
print(f"Episode definition: {DX_DIGITS}-digit DX codes, {TIME_WINDOW_DAYS}-day window")
print(f"CPT filter: Level 1 only (5-digit numeric codes)")
print(f"Use saved micro data: {USE_SAVED_MICRO_DATA}")
print("="*100)

def classify_encounter_level(proc_code):
    """
    Classify CPT code into levels:
    Level 1: 5 digits (e.g., 99214, 99213, 93306)
    Level 2: 1 letter + 4 digits (e.g., G0463, A4657)
    Level 3: 3-4 digits (e.g., 85025, 80053)
    """
    if pd.isna(proc_code) or proc_code == '':
        return None
    proc_str = str(proc_code).strip()
    
    # Level 1: Exactly 5 digits
    if len(proc_str) == 5 and proc_str.isdigit():
        return 1
    # Level 2: 1 letter + 4 digits
    if len(proc_str) == 5 and proc_str[0].isalpha() and proc_str[1:].isdigit():
        return 2
    # Level 3: 3 or 4 digits
    if len(proc_str) in [3, 4] and proc_str.isdigit():
        return 3
    
    return None

def load_rvu_data(year):
    """Load RVU data for a specific year"""
    year_suffix = str(year)[2:]
    rvu_path = RVU_BASE_PATH.replace('XX', year_suffix)
    
    try:
        rvu_df = pd.read_csv(rvu_path, encoding='latin-1')
        
        if 'HCPS' in rvu_df.columns:
            rvu_df = rvu_df.rename(columns={'HCPS': 'PROC1'})
        
        if 'PROC1' in rvu_df.columns:
            rvu_df['PROC1'] = rvu_df['PROC1'].astype(str).str.strip()
        
        print(f"  [{year}] Loaded RVU data: {len(rvu_df):,} codes")
        return rvu_df
        
    except FileNotFoundError:
        print(f"  [{year}] WARNING: RVU file not found: {rvu_path}")
        return None
    except Exception as e:
        print(f"  [{year}] ERROR loading RVU data: {e}")
        return None

def define_episodes_optimized(df, dx_digits=3, time_window_days=100):
    """Define episodes with rolling window diagnosis check"""
    if len(df) == 0:
        return df

    df = df.sort_values(['ENROLID', 'SVCDATE']).reset_index(drop=True)

    for col in ['DX1', 'DX2', 'DX3', 'DX4']:
        df[col] = df[col].astype(str)
        df[col] = df[col].replace(['', 'nan', 'None', 'Non', ' ', 'nat', 'NaN', 'NaT'], np.nan)
        df[col] = df[col].str[:dx_digits]
        df[col] = df[col].replace(['', 'nan', 'Non', ' ', 'NaN'], np.nan)

    df['episode_id'] = 0
    current_episode = 0

    for patient_id in df['ENROLID'].unique():
        patient_mask = df['ENROLID'] == patient_id
        patient_indices = df[patient_mask].index.tolist()

        if len(patient_indices) == 0:
            continue

        current_episode += 1
        df.loc[patient_indices[0], 'episode_id'] = current_episode

        active_episodes = {
            current_episode: {
                'date': df.loc[patient_indices[0], 'SVCDATE'],
                'dx_codes': set(df.loc[patient_indices[0], ['DX1', 'DX2', 'DX3', 'DX4']].dropna().values)
            }
        }

        for idx in patient_indices[1:]:
            visit_date = df.loc[idx, 'SVCDATE']
            visit_dx = set(df.loc[idx, ['DX1', 'DX2', 'DX3', 'DX4']].dropna().values)

            episodes_to_remove = []
            for ep_id, ep_data in active_episodes.items():
                days_diff = (visit_date - ep_data['date']).days
                if days_diff > time_window_days:
                    episodes_to_remove.append(ep_id)

            for ep_id in episodes_to_remove:
                del active_episodes[ep_id]

            matched_episode = None
            if len(visit_dx) > 0:
                for ep_id, ep_data in active_episodes.items():
                    if len(visit_dx & ep_data['dx_codes']) > 0:
                        matched_episode = ep_id
                        break

            if matched_episode is not None:
                df.loc[idx, 'episode_id'] = matched_episode
                active_episodes[matched_episode]['date'] = visit_date
                active_episodes[matched_episode]['dx_codes'].update(visit_dx)
            else:
                current_episode += 1
                df.loc[idx, 'episode_id'] = current_episode
                active_episodes[current_episode] = {
                    'date': visit_date,
                    'dx_codes': visit_dx
                }

    return df

def load_or_create_micro_data(year):
    """
    Load existing micro data if available and USE_SAVED_MICRO_DATA is True,
    otherwise create new micro data from raw files.
    """
    sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
    micro_file = os.path.join(micro_data_dir, f'micro_level1_{year}{sample_suffix}.parquet')
    
    # Try to load existing micro data
    if USE_SAVED_MICRO_DATA and os.path.exists(micro_file):
        print(f"[{year}] Loading existing micro data from: {micro_file}")
        try:
            df = pd.read_parquet(micro_file)
            print(f"[{year}] Loaded {len(df):,} records from saved micro data")
            return df
        except Exception as e:
            print(f"[{year}] Error loading saved micro data: {e}")
            print(f"[{year}] Will regenerate micro data...")
    
    # Create new micro data
    print(f"[{year}] Creating micro data from raw files...")
    
    # Load RVU data
    rvu_df = load_rvu_data(year)
    if rvu_df is None:
        print(f"[{year}] Cannot create micro data without RVU file")
        return None
    
    file_path = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_update_{year}.parquet"
    
    if not os.path.exists(file_path):
        print(f"[{year}] Data file not found: {file_path}")
        return None
    
    # Get patients and sample
    conn = duckdb.connect()
    all_patients = conn.execute(f"""
        SELECT DISTINCT ENROLID FROM '{file_path}' 
        WHERE ENROLID IS NOT NULL AND DATATYP = 3
    """).df()['ENROLID'].values
    conn.close()
    
    print(f"[{year}] Found {len(all_patients):,} total patients")
    
    if SAMPLE_SIZE and len(all_patients) > SAMPLE_SIZE:
        year_seed = 42 + int(year)
        np.random.seed(year_seed)
        patients = np.random.choice(all_patients, size=SAMPLE_SIZE, replace=False)
        print(f"[{year}] Sampled {SAMPLE_SIZE:,} patients")
    else:
        patients = all_patients
    
    patient_list = ','.join(map(str, map(int, patients)))
    
    # Load data
    conn = duckdb.connect()
    conn.execute(f"SET temp_directory='{temp_dir}'")
    
    year_int = int(year)
    medadv_clause = "AND (MEDADV = 0 OR MEDADV IS NULL)" if year_int >= 2020 else ""
    
    print(f"[{year}] Loading raw data...")
    df = conn.execute(f"""
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
    
    print(f"[{year}] Loaded {len(df):,} raw records")
    
    # Convert types
    df['SVCDATE'] = pd.to_datetime(df['SVCDATE'], errors='coerce')
    df = df.dropna(subset=['SVCDATE'])
    
    payment_cols = ['COB', 'COINS', 'COPAY', 'DEDUCT', 'NETPAY', 'PAY']
    for col in payment_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Merge with RVU data (INNER JOIN)
    df['PROC1'] = df['PROC1'].astype(str).str.strip()
    
    pre_merge_len = len(df)
    df = df.merge(rvu_df, on='PROC1', how='inner')
    post_merge_len = len(df)
    
    print(f"  [{year}] RVU merge kept {post_merge_len:,}/{pre_merge_len:,} records")
    
    # Classify encounter levels
    df['encounter_level'] = df['PROC1'].apply(classify_encounter_level)
    
    # Count by level before filtering
    level_counts = df['encounter_level'].value_counts().sort_index()
    print(f"  [{year}] Encounter level distribution:")
    for level, count in level_counts.items():
        if level is not None:
            print(f"    Level {level}: {count:,} records")
    
    # Filter to Level 1 only
    pre_level1_len = len(df)
    df = df[df['encounter_level'] == 1].copy()
    post_level1_len = len(df)
    
    print(f"  [{year}] Level 1 filter kept {post_level1_len:,}/{pre_level1_len:,} records")
    print(f"  [{year}] Excluded: {pre_level1_len - post_level1_len:,} Level 2/3 records")
    
    if len(df) == 0:
        print(f"  [{year}] WARNING: No Level 1 records remaining")
        return None
    
    # Create PE_RVU_actualized based on STDPLAC
    df['PE_RVU_actualized'] = np.nan
    
    pe_facility_col = 'FACILITY_PE_RVU'
    pe_nonfacility_col = 'NON-FAC_PE_RVU'
    
    if pe_facility_col in df.columns and pe_nonfacility_col in df.columns:
        df[pe_facility_col] = pd.to_numeric(df[pe_facility_col], errors='coerce')
        df[pe_nonfacility_col] = pd.to_numeric(df[pe_nonfacility_col], errors='coerce')
        
        # Apply facility PE RVU
        facility_mask = df['STDPLAC'].isin(FACILITY_POS)
        df.loc[facility_mask, 'PE_RVU_actualized'] = df.loc[facility_mask, pe_facility_col]
        
        # Apply non-facility PE RVU
        nonfacility_mask = df['STDPLAC'].isin(NON_FACILITY_POS)
        df.loc[nonfacility_mask, 'PE_RVU_actualized'] = df.loc[nonfacility_mask, pe_nonfacility_col]
        
        # Apply AVERAGE for NEITHER category
        neither_mask = ~facility_mask & ~nonfacility_mask & df['STDPLAC'].notna()
        if neither_mask.sum() > 0:
            df.loc[neither_mask, 'PE_RVU_actualized'] = (
                df.loc[neither_mask, pe_facility_col] + 
                df.loc[neither_mask, pe_nonfacility_col]
            ) / 2.0
            print(f"  [{year}] {neither_mask.sum():,} records using averaged PE RVU")
    
    # Save micro data for future use
    print(f"[{year}] Saving micro data to: {micro_file}")
    df.to_parquet(micro_file, index=False, compression='snappy')
    file_size = os.path.getsize(micro_file) / (1024**2)
    print(f"  [{year}] Saved {file_size:.1f} MB")
    
    return df

def identify_99214_only_episodes(df, year):
    """
    Identify episodes that contain only a single 99214 claim.
    Called BEFORE aggregation to episode level.
    
    Returns:
    - df with 'only_99214' column added
    - k value (Work RVU + MP RVU) calculated from 99214-only episodes
    """
    # For each episode, count total claims and 99214 claims
    episode_summary = df.groupby(['ENROLID', 'episode_id']).agg({
        'PROC1': ['count', lambda x: (x == '99214').sum(), 'nunique']
    }).reset_index()
    
    episode_summary.columns = ['ENROLID', 'episode_id', 'total_claims', 'n_99214', 'n_unique_codes']
    
    # Episode has only 99214 if:
    # - total_claims == 1
    # - n_99214 == 1
    # - n_unique_codes == 1
    episode_summary['only_99214'] = 0
    episode_summary.loc[
        (episode_summary['total_claims'] == 1) & 
        (episode_summary['n_99214'] == 1) & 
        (episode_summary['n_unique_codes'] == 1),
        'only_99214'
    ] = 1
    
    n_only_99214 = episode_summary['only_99214'].sum()
    print(f"  [{year}] Episodes with only 99214: {n_only_99214:,}")
    
    # Merge back to main df
    df = df.merge(
        episode_summary[['ENROLID', 'episode_id', 'only_99214']], 
        on=['ENROLID', 'episode_id'], 
        how='left'
    )
    
    # Calculate k from 99214-only episodes
    only_99214_claims = df[df['only_99214'] == 1].copy()
    
    if len(only_99214_claims) > 0:
        # Calculate Work + MP for these claims
        only_99214_claims['Work_MP_RVU'] = (
            pd.to_numeric(only_99214_claims['WORK_RVU'], errors='coerce').fillna(0) +
            pd.to_numeric(only_99214_claims['MP_RVU'], errors='coerce').fillna(0)
        )
        
        # Get modal value
        modal_k = only_99214_claims['Work_MP_RVU'].mode()
        mean_k = only_99214_claims['Work_MP_RVU'].mean()
        
        k = modal_k[0] if len(modal_k) > 0 else mean_k
        
        print(f"  [{year}] k from 99214-only episodes:")
        print(f"    Modal k (Work + MP): {modal_k[0] if len(modal_k) > 0 else 'N/A':.2f}")
        print(f"    Mean k (Work + MP): {mean_k:.2f}")
        print(f"    Using k = {k:.2f}")
    else:
        print(f"  [{year}] WARNING: No 99214-only episodes found, cannot calculate k")
        k = None
    
    return df, k

def analyze_year(year):
    """Analyze a single year with k-band classification (Level 1 CPT codes only)"""
    print(f"\n[{year}] Starting analysis...")
    
    # Load or create micro data
    df = load_or_create_micro_data(year)
    
    if df is None or len(df) == 0:
        print(f"[{year}] No data available")
        return None
    
    # Define episodes
    print(f"[{year}] Defining episodes (Level 1 codes only)...")
    df = define_episodes_optimized(df, DX_DIGITS, TIME_WINDOW_DAYS)
    
    # Identify 99214-only episodes and calculate k (BEFORE aggregation)
    print(f"[{year}] Identifying 99214-only episodes...")
    df, k = identify_99214_only_episodes(df, year)
    
    # Prepare aggregation dict
    agg_dict = {}
    for col in ['COB', 'COINS', 'COPAY', 'DEDUCT', 'NETPAY', 'PAY']:
        if col in df.columns:
            agg_dict[col] = 'sum'
    
    rvu_cols = ['PE_RVU_actualized', 'WORK_RVU', 'MP_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU']
    for col in rvu_cols:
        if col in df.columns:
            agg_dict[col] = 'sum'
    
    # Add only_99214 indicator to aggregation
    if 'only_99214' in df.columns:
        agg_dict['only_99214'] = 'max'  # All claims in episode have same value
    
    # Aggregate to episode level
    print(f"[{year}] Aggregating to episode level...")
    episodes = df.groupby(['ENROLID', 'episode_id']).agg(agg_dict).reset_index()
    
    # Calculate total Work + MP RVU per episode
    episodes['Total_Work_MP_RVU'] = episodes['WORK_RVU'] + episodes['MP_RVU']
    
    # Create in_k_band indicator
    episodes['in_k_band'] = 0
    if k is not None:
        episodes['k_threshold'] = k
        episodes.loc[
            (episodes['Total_Work_MP_RVU'] > 0) & 
            (episodes['Total_Work_MP_RVU'] <= k),
            'in_k_band'
        ] = 1
    else:
        episodes['k_threshold'] = np.nan
    
    # Calculate TOTPAY = PAY + COPAY + COINS + DEDUCT
    episodes['TOTPAY'] = (
        episodes['PAY'] + 
        episodes['COPAY'] + 
        episodes['COINS'] + 
        episodes['DEDUCT']
    )
    
    # Calculate TOTAL_RVU = Work + PE_actualized + MP
    episodes['TOTAL_RVU'] = (
        episodes['WORK_RVU'] + 
        episodes['PE_RVU_actualized'].fillna(0) + 
        episodes['MP_RVU']
    )
    
    episodes['year'] = year
    
    # Save episode dataset
    sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
    episode_file = os.path.join(output_dir, f'episodes_level1_k_band_{year}{sample_suffix}.csv')
    episodes.to_csv(episode_file, index=False)
    file_size = os.path.getsize(episode_file) / (1024**2)
    print(f"  [{year}] Episodes saved to: {episode_file} ({file_size:.1f} MB)")
    
    # Calculate statistics
    n_episodes = len(episodes)
    n_only_99214 = episodes['only_99214'].sum()
    n_in_k_band = episodes['in_k_band'].sum()
    
    # Stats for all episodes
    all_mean_totpay = episodes['TOTPAY'].mean()
    all_median_totpay = episodes['TOTPAY'].median()
    all_mean_total_rvu = episodes['TOTAL_RVU'].mean()
    all_median_total_rvu = episodes['TOTAL_RVU'].median()
    
    # Stats for k-band episodes
    k_band_episodes = episodes[episodes['in_k_band'] == 1]
    k_mean_totpay = k_band_episodes['TOTPAY'].mean() if len(k_band_episodes) > 0 else np.nan
    k_median_totpay = k_band_episodes['TOTPAY'].median() if len(k_band_episodes) > 0 else np.nan
    k_mean_work_mp = k_band_episodes['Total_Work_MP_RVU'].mean() if len(k_band_episodes) > 0 else np.nan
    k_mean_total_rvu = k_band_episodes['TOTAL_RVU'].mean() if len(k_band_episodes) > 0 else np.nan
    
    # Stats for above-k episodes
    above_k_episodes = episodes[episodes['in_k_band'] == 0]
    above_k_mean_totpay = above_k_episodes['TOTPAY'].mean() if len(above_k_episodes) > 0 else np.nan
    above_k_median_totpay = above_k_episodes['TOTPAY'].median() if len(above_k_episodes) > 0 else np.nan
    above_k_mean_total_rvu = above_k_episodes['TOTAL_RVU'].mean() if len(above_k_episodes) > 0 else np.nan
    
    # Print summary
    print(f"\n[{year}] SUMMARY:")
    print(f"  Total episodes (Level 1 only): {n_episodes:,}")
    print(f"  Episodes with only 99214: {n_only_99214:,} ({n_only_99214/n_episodes*100:.1f}%)")
    print(f"  k threshold (Work + MP): {k:.2f}" if k is not None else "  k threshold: N/A")
    print(f"  Episodes in k-band (Work+MP ≤ k): {n_in_k_band:,} ({n_in_k_band/n_episodes*100:.1f}%)")
    print(f"  Episodes above k-band: {len(above_k_episodes):,} ({len(above_k_episodes)/n_episodes*100:.1f}%)")
    print(f"\n  ALL EPISODES:")
    print(f"    Mean TOTPAY: ${all_mean_totpay:,.2f}, Median: ${all_median_totpay:,.2f}")
    print(f"    Mean TOTAL_RVU: {all_mean_total_rvu:.2f}, Median: {all_median_total_rvu:.2f}")
    print(f"\n  K-BAND EPISODES:")
    print(f"    Mean TOTPAY: ${k_mean_totpay:,.2f}, Median: ${k_median_totpay:,.2f}")
    print(f"    Mean Work+MP RVU: {k_mean_work_mp:.2f}")
    print(f"    Mean TOTAL_RVU: {k_mean_total_rvu:.2f}")
    print(f"\n  ABOVE-K EPISODES:")
    print(f"    Mean TOTPAY: ${above_k_mean_totpay:,.2f}, Median: ${above_k_median_totpay:,.2f}")
    print(f"    Mean TOTAL_RVU: {above_k_mean_total_rvu:.2f}")
    
    # Clean up
    del df, episodes, k_band_episodes, above_k_episodes
    gc.collect()
    
    # Results
    results = {
        'year': year,
        'k_threshold': k,
        'n_episodes': n_episodes,
        'n_only_99214': n_only_99214,
        'pct_only_99214': n_only_99214/n_episodes*100 if n_episodes > 0 else np.nan,
        'n_in_k_band': n_in_k_band,
        'pct_in_k_band': n_in_k_band/n_episodes*100 if n_episodes > 0 else np.nan,
        'all_mean_totpay': all_mean_totpay,
        'all_median_totpay': all_median_totpay,
        'all_mean_total_rvu': all_mean_total_rvu,
        'all_median_total_rvu': all_median_total_rvu,
        'k_band_mean_totpay': k_mean_totpay,
        'k_band_median_totpay': k_median_totpay,
        'k_band_mean_work_mp': k_mean_work_mp,
        'k_band_mean_total_rvu': k_mean_total_rvu,
        'above_k_mean_totpay': above_k_mean_totpay,
        'above_k_median_totpay': above_k_median_totpay,
        'above_k_mean_total_rvu': above_k_mean_total_rvu,
    }
    
    return results

# Run analysis
all_results = []

for year in YEARS:
    try:
        result = analyze_year(year)
        if result:
            all_results.append(result)
    except Exception as e:
        print(f"[{year}] ERROR: {e}")
        import traceback
        traceback.print_exc()

# Save summary results
if len(all_results) > 0:
    results_df = pd.DataFrame(all_results)
    sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
    output_file = os.path.join(output_dir, f'k_band_summary_level1{sample_suffix}.csv')
    results_df.to_csv(output_file, index=False)
    
    print(f"\n{'='*100}")
    print("FINAL SUMMARY ACROSS ALL YEARS")
    print("="*100)
    print(results_df.to_string(index=False))
    print(f"\n✓ Summary saved to: {output_file}")
    print(f"\nFiles generated:")
    print(f"  Micro-level data: ~/micro_data/micro_level1_YYYY{sample_suffix}.parquet (per year)")
    print(f"  Episode-level data: ~/episode_outputs/episodes_level1_k_band_YYYY{sample_suffix}.csv (per year)")
    print(f"  Summary: {output_file}")
    print(f"\nNote: Set USE_SAVED_MICRO_DATA = True to reload existing micro data and skip merge/sampling")
