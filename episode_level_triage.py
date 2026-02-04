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

os.makedirs(output_dir, exist_ok=True)
os.makedirs(temp_dir, exist_ok=True)

np.random.seed(42)

# Define facility vs non-facility place of service codes
FACILITY_POS = {2, 19, 21, 22, 23, 24, 26, 31, 34, 41, 42, 51, 52, 53, 56, 61}
NON_FACILITY_POS = {1, 3, 4, 11, 12, 13, 14, 15, 16, 17, 20, 25, 32, 33, 49, 50, 54, 55, 57, 60, 62, 65, 71, 72, 81, 99}
# NEITHER category (automatically averaged): 10, 18, 27, 35, 58, 95

print("="*100)
print("ENCOUNTER LEVEL COMPARISON: LEVEL 1 ONLY vs LEVEL 1+2+3")
print("="*100)

def classify_encounter_level(proc_code):
    """Level 1: 5 digits, Level 2: 1 letter + 4 digits, Level 3: 3-4 digits"""
    if pd.isna(proc_code) or proc_code == '':
        return None
    proc_str = str(proc_code).strip()
    if len(proc_str) == 5 and proc_str.isdigit():
        return 1
    if len(proc_str) == 5 and proc_str[0].isalpha() and proc_str[1:].isdigit():
        return 2
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

def analyze_year(year):
    """Analyze encounter levels for a single year"""
    print(f"\n[{year}] Starting analysis...")
    
    # Load RVU data
    rvu_df = load_rvu_data(year)
    
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
    
    print(f"[{year}] Loading data...")
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
    
    print(f"[{year}] Loaded {len(df):,} records")
    
    # Convert types
    df['SVCDATE'] = pd.to_datetime(df['SVCDATE'], errors='coerce')
    df = df.dropna(subset=['SVCDATE'])
    
    payment_cols = ['COB', 'COINS', 'COPAY', 'DEDUCT', 'NETPAY', 'PAY']
    for col in payment_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Merge with RVU data
    if rvu_df is not None and 'PROC1' in df.columns:
        df['PROC1'] = df['PROC1'].astype(str).str.strip()
        
        pre_merge_len = len(df)
        df = df.merge(rvu_df, on='PROC1', how='inner')
        post_merge_len = len(df)
        
        print(f"  [{year}] RVU merge kept {post_merge_len:,}/{pre_merge_len:,} records")
        
        # Create PE_RVU_actualized based on STDPLAC
        if 'STDPLAC' in df.columns:
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
                
                # Apply AVERAGE for NEITHER category (codes not in either list)
                neither_mask = ~facility_mask & ~nonfacility_mask & df['STDPLAC'].notna()
                if neither_mask.sum() > 0:
                    df.loc[neither_mask, 'PE_RVU_actualized'] = (
                        df.loc[neither_mask, pe_facility_col] + 
                        df.loc[neither_mask, pe_nonfacility_col]
                    ) / 2.0
                    print(f"  [{year}] {neither_mask.sum():,} records using averaged PE RVU (NEITHER category)")
    
    # Classify encounter levels
    df['encounter_level'] = df['PROC1'].apply(classify_encounter_level)
    
    # SCENARIO 1: Level 1 only
    print(f"[{year}] Scenario 1: Level 1 encounters only...")
    df_level1 = df[df['encounter_level'] == 1].copy()
    print(f"[{year}]   {len(df_level1):,} Level 1 records")
    
    if len(df_level1) > 0:
        df_level1 = define_episodes_optimized(df_level1, DX_DIGITS, TIME_WINDOW_DAYS)
        
        # Prepare aggregation dict
        agg_dict = {}
        for col in ['COB', 'COINS', 'COPAY', 'DEDUCT', 'NETPAY', 'PAY']:
            if col in df_level1.columns:
                agg_dict[col] = 'sum'
        
        rvu_cols = ['PE_RVU_actualized', 'WORK_RVU', 'MP_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU']
        for col in rvu_cols:
            if col in df_level1.columns:
                agg_dict[col] = 'sum'
        
        # Aggregate to episodes
        episodes_level1 = df_level1.groupby(['ENROLID', 'episode_id']).agg(agg_dict).reset_index()
        
        # Create TOTPAY = PAY + COPAY + COINS + DEDUCT
        episodes_level1['TOTPAY'] = (
            episodes_level1['PAY'] + 
            episodes_level1['COPAY'] + 
            episodes_level1['COINS'] + 
            episodes_level1['DEDUCT']
        )
        
        # Create TOTAL_RVU = sum of all RVU components
        episodes_level1['TOTAL_RVU'] = 0
        rvu_components = ['PE_RVU_actualized', 'WORK_RVU', 'MP_RVU']
        for col in rvu_components:
            if col in episodes_level1.columns:
                episodes_level1['TOTAL_RVU'] += episodes_level1[col].fillna(0)
        
        # Save Level 1 episode dataset
        sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
        level1_file = os.path.join(output_dir, f'episodes_level1_{year}{sample_suffix}.csv')
        episodes_level1.to_csv(level1_file, index=False)
        file_size = os.path.getsize(level1_file) / (1024**2)
        print(f"  [{year}] Level 1 episodes saved to: {level1_file} ({file_size:.1f} MB)")
        
        # Calculate statistics
        level1_mean = episodes_level1['TOTPAY'].mean()
        level1_median = episodes_level1['TOTPAY'].median()
        level1_min = episodes_level1['TOTPAY'].min()
        level1_max = episodes_level1['TOTPAY'].max()
        level1_sd = episodes_level1['TOTPAY'].std()
        level1_n = len(episodes_level1)
        
        level1_rvu_mean = episodes_level1['TOTAL_RVU'].mean()
        level1_rvu_median = episodes_level1['TOTAL_RVU'].median()
        level1_rvu_min = episodes_level1['TOTAL_RVU'].min()
        level1_rvu_max = episodes_level1['TOTAL_RVU'].max()
        level1_rvu_sd = episodes_level1['TOTAL_RVU'].std()
        level1_rvu_zero_count = (episodes_level1['TOTAL_RVU'] == 0).sum()
    else:
        level1_mean = level1_median = level1_min = level1_max = level1_sd = np.nan
        level1_rvu_mean = level1_rvu_median = level1_rvu_min = level1_rvu_max = level1_rvu_sd = np.nan
        level1_rvu_zero_count = 0
        level1_n = 0
    
    # SCENARIO 2: Level 1+2+3
    print(f"[{year}] Scenario 2: Level 1+2+3 encounters...")
    df_level123 = df[df['encounter_level'].isin([1, 2, 3])].copy()
    print(f"[{year}]   {len(df_level123):,} Level 1+2+3 records")
    
    if len(df_level123) > 0:
        df_level123 = define_episodes_optimized(df_level123, DX_DIGITS, TIME_WINDOW_DAYS)
        
        # Prepare aggregation dict
        agg_dict = {}
        for col in ['COB', 'COINS', 'COPAY', 'DEDUCT', 'NETPAY', 'PAY']:
            if col in df_level123.columns:
                agg_dict[col] = 'sum'
        
        rvu_cols = ['PE_RVU_actualized', 'WORK_RVU', 'MP_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU']
        for col in rvu_cols:
            if col in df_level123.columns:
                agg_dict[col] = 'sum'
        
        # Aggregate to episodes
        episodes_level123 = df_level123.groupby(['ENROLID', 'episode_id']).agg(agg_dict).reset_index()
        
        # Create TOTPAY = PAY + COPAY + COINS + DEDUCT
        episodes_level123['TOTPAY'] = (
            episodes_level123['PAY'] + 
            episodes_level123['COPAY'] + 
            episodes_level123['COINS'] + 
            episodes_level123['DEDUCT']
        )
        
        # Create TOTAL_RVU = sum of all RVU components
        episodes_level123['TOTAL_RVU'] = 0
        rvu_components = ['PE_RVU_actualized', 'WORK_RVU', 'MP_RVU']
        for col in rvu_components:
            if col in episodes_level123.columns:
                episodes_level123['TOTAL_RVU'] += episodes_level123[col].fillna(0)
        
        # Calculate statistics
        level123_mean = episodes_level123['TOTPAY'].mean()
        level123_median = episodes_level123['TOTPAY'].median()
        level123_min = episodes_level123['TOTPAY'].min()
        level123_max = episodes_level123['TOTPAY'].max()
        level123_sd = episodes_level123['TOTPAY'].std()
        level123_n = len(episodes_level123)
        
        level123_rvu_mean = episodes_level123['TOTAL_RVU'].mean()
        level123_rvu_median = episodes_level123['TOTAL_RVU'].median()
        level123_rvu_min = episodes_level123['TOTAL_RVU'].min()
        level123_rvu_max = episodes_level123['TOTAL_RVU'].max()
        level123_rvu_sd = episodes_level123['TOTAL_RVU'].std()
        level123_rvu_zero_count = (episodes_level123['TOTAL_RVU'] == 0).sum()
    else:
        level123_mean = level123_median = level123_min = level123_max = level123_sd = np.nan
        level123_rvu_mean = level123_rvu_median = level123_rvu_min = level123_rvu_max = level123_rvu_sd = np.nan
        level123_rvu_zero_count = 0
        level123_n = 0
    
    # Clean up
    del df
    if len(df_level1) > 0:
        del df_level1
    if len(df_level123) > 0:
        del df_level123
    gc.collect()
    
    # Results
    results = {
        'year': year,
        'level_1_n_episodes': level1_n,
        'level_1_mean_totpay': level1_mean,
        'level_1_median_totpay': level1_median,
        'level_1_min_totpay': level1_min,
        'level_1_max_totpay': level1_max,
        'level_1_sd_totpay': level1_sd,
        'level_1_mean_rvu': level1_rvu_mean,
        'level_1_median_rvu': level1_rvu_median,
        'level_1_min_rvu': level1_rvu_min,
        'level_1_max_rvu': level1_rvu_max,
        'level_1_sd_rvu': level1_rvu_sd,
        'level_1_rvu_zero_count': level1_rvu_zero_count,
        'level_123_n_episodes': level123_n,
        'level_123_mean_totpay': level123_mean,
        'level_123_median_totpay': level123_median,
        'level_123_min_totpay': level123_min,
        'level_123_max_totpay': level123_max,
        'level_123_sd_totpay': level123_sd,
        'level_123_mean_rvu': level123_rvu_mean,
        'level_123_median_rvu': level123_rvu_median,
        'level_123_min_rvu': level123_rvu_min,
        'level_123_max_rvu': level123_rvu_max,
        'level_123_sd_rvu': level123_rvu_sd,
        'level_123_rvu_zero_count': level123_rvu_zero_count,
    }
    
    print(f"\n[{year}] RESULTS:")
    print(f"  LEVEL 1 ONLY EPISODES:")
    print(f"    N episodes: {results['level_1_n_episodes']:,}")
    print(f"    TOTPAY - Mean: ${results['level_1_mean_totpay']:,.2f}, Median: ${results['level_1_median_totpay']:,.2f}")
    print(f"    TOTPAY - Min: ${results['level_1_min_totpay']:,.2f}, Max: ${results['level_1_max_totpay']:,.2f}, SD: ${results['level_1_sd_totpay']:,.2f}")
    print(f"    TOTAL_RVU - Mean: {results['level_1_mean_rvu']:,.2f}, Median: {results['level_1_median_rvu']:,.2f}")
    print(f"    TOTAL_RVU - Min: {results['level_1_min_rvu']:,.2f}, Max: {results['level_1_max_rvu']:,.2f}, SD: {results['level_1_sd_rvu']:,.2f}")
    print(f"    TOTAL_RVU = 0 count: {results['level_1_rvu_zero_count']:,}")
    
    print(f"\n  LEVEL 1+2+3 EPISODES:")
    print(f"    N episodes: {results['level_123_n_episodes']:,}")
    print(f"    TOTPAY - Mean: ${results['level_123_mean_totpay']:,.2f}, Median: ${results['level_123_median_totpay']:,.2f}")
    print(f"    TOTPAY - Min: ${results['level_123_min_totpay']:,.2f}, Max: ${results['level_123_max_totpay']:,.2f}, SD: ${results['level_123_sd_totpay']:,.2f}")
    print(f"    TOTAL_RVU - Mean: {results['level_123_mean_rvu']:,.2f}, Median: {results['level_123_median_rvu']:,.2f}")
    print(f"    TOTAL_RVU - Min: {results['level_123_min_rvu']:,.2f}, Max: {results['level_123_max_rvu']:,.2f}, SD: {results['level_123_sd_rvu']:,.2f}")
    print(f"    TOTAL_RVU = 0 count: {results['level_123_rvu_zero_count']:,}")
    
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

# Save results
if len(all_results) > 0:
    results_df = pd.DataFrame(all_results)
    sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
    output_file = os.path.join(output_dir, f'encounter_level_comparison{sample_suffix}.csv')
    results_df.to_csv(output_file, index=False)
    
    print(f"\n{'='*100}")
    print("FINAL RESULTS")
    print("="*100)
    print(results_df.to_string(index=False))
    print(f"\n✓ Saved to: {output_file}")
    print(f"\nAdditional files generated:")
    print(f"  ✓ episodes_level1_YYYY{sample_suffix}.csv (Level 1 episode datasets per year)")
