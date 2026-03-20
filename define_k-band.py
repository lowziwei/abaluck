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

# Medicare Conversion Factors (RVU to $)
CONVERSION_RATES = {
    2014: 35.8228,
    2015: 35.9335,
    2016: 35.8043,
    2017: 35.8887,
    2018: 35.9996,
    2019: 36.0391,
    2020: 36.0896,
    2021: 34.8931,
    2022: 34.6062,
    2023: 33.8872,
    2024: 33.2875
}

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

# Toggle to use saved claim data or regenerate
USE_SAVED_CLAIM_DATA = False

print("="*100)
print("EPISODE ANALYSIS WITH K-BAND CLASSIFICATION (LEVEL 1 CPT CODES ONLY)")
print("="*100)
print(f"Sample: {SAMPLE_SIZE:,} patients per year")
print(f"Episode definition: {DX_DIGITS}-digit DX codes, {TIME_WINDOW_DAYS}-day window")
print(f"CPT filter: Level 1 only (5-digit numeric codes)")
print(f"Use saved claim data: {USE_SAVED_CLAIM_DATA}")
print(f"Conversion factors: {CONVERSION_RATES}")
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

def load_or_create_claim_data(year):
    """
    Load existing claim-level data if available and USE_SAVED_CLAIM_DATA is True,
    otherwise create new data from raw files.
    """
    sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
    claim_file = os.path.join(output_dir, f'claims_level1_{year}{sample_suffix}.csv')

    # Try to load existing claim data
    if USE_SAVED_CLAIM_DATA and os.path.exists(claim_file):
        print(f"[{year}] Loading existing claim data from: {claim_file}")
        try:
            df = pd.read_csv(claim_file)
            df['SVCDATE'] = pd.to_datetime(df['SVCDATE'])
            print(f"[{year}] Loaded {len(df):,} records from saved claim data")
            return df
        except Exception as e:
            print(f"[{year}] Error loading saved claim data: {e}")
            print(f"[{year}] Will regenerate claim data...")

    # Create new claim data
    print(f"[{year}] Creating claim data from raw files...")

    # Load RVU data
 rvu_df = load_rvu_data(year)
    if rvu_df is None:
        print(f"[{year}] Cannot create claim data without RVU file")
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

    return df

def get_cpt_thresholds(rvu_df, year):
    """
    Get Work+MP RVU thresholds for 99213 and 99214 from RVU data
    """
    year_int = int(year)
    conversion_factor = CONVERSION_RATES.get(year_int, None)

    thresholds = {}

    for cpt_code in ['99213', '99214']:
        cpt_data = rvu_df[rvu_df['PROC1'] == cpt_code]

        if len(cpt_data) > 0:
            work_rvu = pd.to_numeric(cpt_data['WORK_RVU'].iloc[0], errors='coerce')
            mp_rvu = pd.to_numeric(cpt_data['MP_RVU'].iloc[0], errors='coerce')

            work_mp_rvu = work_rvu + mp_rvu
            work_mp_dollars = work_mp_rvu * conversion_factor if conversion_factor else None

            thresholds[cpt_code] = {
                'rvu': work_mp_rvu,
                'dollars': work_mp_dollars
            }
        else:
            thresholds[cpt_code] = {
                'rvu': None,
                'dollars': None
            }

    return thresholds, conversion_factor

def create_episode_indicators(df, year):
    """
    Create episode-level indicators for CPT code composition
    Called BEFORE aggregation to episode level.
    """
    # For each episode, identify CPT composition
    episode_summary = df.groupby(['ENROLID', 'episode_id']).agg({
        'PROC1': ['count', 'nunique', lambda x: list(x)]
    }).reset_index()

    episode_summary.columns = ['ENROLID', 'episode_id', 'n_claims', 'n_unique_codes', 'proc_list']

    # Contains 99213 (regardless if only)
    episode_summary['contains_99213'] = episode_summary['proc_list'].apply(
        lambda x: 1 if '99213' in x else 0
    )

    # Contains 99214 (regardless if only)
    episode_summary['contains_99214'] = episode_summary['proc_list'].apply(
        lambda x: 1 if '99214' in x else 0
    )

    # Only 99213 (single CPT code, all claims are 99213)
    episode_summary['only_99213'] = episode_summary.apply(
        lambda row: 1 if (row['n_unique_codes'] == 1 and row['proc_list'][0] == '99213') else 0,
        axis=1
    )

    # Only 99214 (single CPT code, all claims are 99214)
    episode_summary['only_99214'] = episode_summary.apply(
        lambda row: 1 if (row['n_unique_codes'] == 1 and row['proc_list'][0] == '99214') else 0,
        axis=1
    )

    # Merge back to main df
    df = df.merge(
        episode_summary[['ENROLID', 'episode_id', 'n_claims', 'contains_99213', 'contains_99214',
                        'only_99213', 'only_99214']],
        on=['ENROLID', 'episode_id'],
        how='left'
    )

    print(f"  [{year}] Episode indicators created:")
    print(f"    Episodes with only 99213: {episode_summary['only_99213'].sum():,}")
    print(f"    Episodes with only 99214: {episode_summary['only_99214'].sum():,}")
    print(f"    Episodes containing 99213: {episode_summary['contains_99213'].sum():,}")
    print(f"    Episodes containing 99214: {episode_summary['contains_99214'].sum():,}")

    return df

def analyze_year(year):
    """Analyze a single year with k-band classification (Level 1 CPT codes only)"""
    print(f"\n[{year}] Starting analysis...")

    # Load or create claim data
    df = load_or_create_claim_data(year)

    if df is None or len(df) == 0:
        print(f"[{year}] No data available")
        return None

    # Load RVU data to get thresholds
    rvu_df = load_rvu_data(year)
    if rvu_df is None:
        print(f"[{year}] Cannot get thresholds without RVU file")
        return None

    # Get 99213 and 99214 thresholds
    thresholds, conversion_factor = get_cpt_thresholds(rvu_df, year)

    k_99213_rvu = thresholds['99213']['rvu']
    k_99213_dollars = thresholds['99213']['dollars']
    k_99214_rvu = thresholds['99214']['rvu']
    k_99214_dollars = thresholds['99214']['dollars']

    print(f"  [{year}] Thresholds:")
    print(f"    99213: {k_99213_rvu:.2f} RVU = ${k_99213_dollars:.2f}" if k_99213_dollars else f"    99213: {k_99213_rvu:.2f} RVU")
    print(f"    99214: {k_99214_rvu:.2f} RVU = ${k_99214_dollars:.2f}" if k_99214_dollars else f"    99214: {k_99214_rvu:.2f} RVU")

    # Define episodes
    print(f"[{year}] Defining episodes (Level 1 codes only)...")
    df = define_episodes_optimized(df, DX_DIGITS, TIME_WINDOW_DAYS)

    # Create episode indicators (BEFORE aggregation)
    print(f"[{year}] Creating episode indicators...")
    df = create_episode_indicators(df, year)

    # SAVE CLAIM-LEVEL DATA
    sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
    claim_file = os.path.join(output_dir, f'claims_level1_{year}{sample_suffix}.csv')

    claim_cols = ['ENROLID', 'episode_id', 'SVCDATE', 'PROC1', 'STDPLAC',
                  'DX1', 'DX2', 'DX3', 'DX4',
                  'PAY', 'COPAY', 'COINS', 'DEDUCT', 'NETPAY', 'COB',
                  'WORK_RVU', 'MP_RVU', 'PE_RVU_actualized',
                  'FACILITY_PE_RVU', 'NON-FAC_PE_RVU',
                  'n_claims', 'contains_99213', 'contains_99214', 'only_99213', 'only_99214']

    claim_output = df[claim_cols].copy()
    claim_output.to_csv(claim_file, index=False)
    claim_file_size = os.path.getsize(claim_file) / (1024**2)
    print(f"  [{year}] CLAIM-LEVEL data saved to: {claim_file} ({claim_file_size:.1f} MB)")

    # Prepare aggregation dict for EPISODE-LEVEL data
    agg_dict = {}
    for col in ['COB', 'COINS', 'COPAY', 'DEDUCT', 'NETPAY', 'PAY']:
        if col in df.columns:
            agg_dict[col] = 'sum'

    rvu_cols = ['PE_RVU_actualized', 'WORK_RVU', 'MP_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU']
    for col in rvu_cols:
        if col in df.columns:
            agg_dict[col] = 'sum'

    # Add episode indicators (use max since all claims in episode have same value)
    for col in ['n_claims', 'contains_99213', 'contains_99214', 'only_99213', 'only_99214']:
        if col in df.columns:
            agg_dict[col] = 'max'

    # Aggregate to episode level
    print(f"[{year}] Aggregating to episode level...")
    episodes = df.groupby(['ENROLID', 'episode_id']).agg(agg_dict).reset_index()

    # Calculate total Work + MP RVU per episode
    episodes['Total_Work_MP_RVU'] = episodes['WORK_RVU'] + episodes['MP_RVU']

    # Convert RVUs to dollars
    if conversion_factor is not None:
        episodes['Total_Work_MP_Dollars'] = episodes['Total_Work_MP_RVU'] * conversion_factor
        episodes['TOTAL_RVU_Dollars'] = (
            episodes['WORK_RVU'] +
            episodes['PE_RVU_actualized'].fillna(0) +
            episodes['MP_RVU']
        ) * conversion_factor
        episodes['conversion_factor'] = conversion_factor
    else:
        episodes['Total_Work_MP_Dollars'] = np.nan
        episodes['TOTAL_RVU_Dollars'] = np.nan
        episodes['conversion_factor'] = np.nan

    # Add thresholds
    episodes['work_mp_99213_RVU'] = k_99213_rvu
    episodes['work_mp_99213_Dollars'] = k_99213_dollars
    episodes['work_mp_99214_RVU'] = k_99214_rvu
    episodes['work_mp_99214_Dollars'] = k_99214_dollars

    # Create in_k_band indicators
    episodes['in_k_99213'] = 0
    if k_99213_rvu is not None:
        episodes.loc[
            (episodes['Total_Work_MP_RVU'] > 0) &
            (episodes['Total_Work_MP_RVU'] <= k_99213_rvu),
            'in_k_99213'
        ] = 1

    episodes['in_k_99214'] = 0
    if k_99214_rvu is not None:
        episodes.loc[
            (episodes['Total_Work_MP_RVU'] > 0) &
            (episodes['Total_Work_MP_RVU'] <= k_99214_rvu),
            'in_k_99214'
        ] = 1

    # Calculate TOTPAY
    episodes['TOTPAY'] = (
        episodes['PAY'] 
    )
    
    # Calculate OOP
    episodes['OOP'] = (
        episodes['COINS'] +
        episodes['COPAY'] + 
        episodes['DEDUCT'] 
    )
    
    # Calculate TOTAL_RVU
    episodes['TOTAL_RVU'] = (
        episodes['WORK_RVU'] +
        episodes['PE_RVU_actualized'].fillna(0) +
        episodes['MP_RVU']
    )

    episodes['year'] = year

    # SAVE EPISODE-LEVEL DATA
    episode_file = os.path.join(output_dir, f'episodes_level1_k_band_{year}{sample_suffix}.csv')
    episodes.to_csv(episode_file, index=False)
    episode_file_size = os.path.getsize(episode_file) / (1024**2)
    print(f"  [{year}] EPISODE-LEVEL data saved to: {episode_file} ({episode_file_size:.1f} MB)")

    # Calculate statistics
    n_episodes = len(episodes)
    n_only_99213 = episodes['only_99213'].sum()
    n_only_99214 = episodes['only_99214'].sum()
    n_contains_99213 = episodes['contains_99213'].sum()
    n_contains_99214 = episodes['contains_99214'].sum()
    n_in_k_99213 = episodes['in_k_99213'].sum()
    n_in_k_99214 = episodes['in_k_99214'].sum()

    # Print summary
    print(f"\n[{year}] SUMMARY:")
    print(f"  Conversion factor: ${conversion_factor:.4f}" if conversion_factor else "  Conversion factor: N/A")
    print(f"  Total episodes: {n_episodes:,}")
    print(f"  Episodes with only 99213: {n_only_99213:,} ({n_only_99213/n_episodes*100:.1f}%)")
    print(f"  Episodes with only 99214: {n_only_99214:,} ({n_only_99214/n_episodes*100:.1f}%)")
    print(f"  Episodes containing 99213: {n_contains_99213:,} ({n_contains_99213/n_episodes*100:.1f}%)")
    print(f"  Episodes containing 99214: {n_contains_99214:,} ({n_contains_99214/n_episodes*100:.1f}%)")
    print(f"  Episodes in k-band (≤99213): {n_in_k_99213:,} ({n_in_k_99213/n_episodes*100:.1f}%)")
    print(f"  Episodes in k-band (≤99214): {n_in_k_99214:,} ({n_in_k_99214/n_episodes*100:.1f}%)")

    # Clean up
    del df, episodes
    gc.collect()

    # Results
    results = {
        'year': year,
        'conversion_factor': conversion_factor,
        'k_99213_rvu': k_99213_rvu,
        'k_99213_dollars': k_99213_dollars,
        'k_99214_rvu': k_99214_rvu,
        'k_99214_dollars': k_99214_dollars,
        'n_episodes': n_episodes,
        'n_only_99213': n_only_99213,
        'pct_only_99213': n_only_99213/n_episodes*100 if n_episodes > 0 else np.nan,
        'n_only_99214': n_only_99214,
        'pct_only_99214': n_only_99214/n_episodes*100 if n_episodes > 0 else np.nan,
        'n_contains_99213': n_contains_99213,
        'n_contains_99214': n_contains_99214,
        'n_in_k_99213': n_in_k_99213,
        'pct_in_k_99213': n_in_k_99213/n_episodes*100 if n_episodes > 0 else np.nan,
        'n_in_k_99214': n_in_k_99214,
        'pct_in_k_99214': n_in_k_99214/n_episodes*100 if n_episodes > 0 else np.nan,
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
    print(f"\nFiles generated per year:")
    print(f"  1. CLAIM-LEVEL: ~/episode_outputs/claims_level1_YYYY{sample_suffix}.csv")
    print(f"  2. EPISODE-LEVEL: ~/episode_outputs/episodes_level1_k_band_YYYY{sample_suffix}.csv")
    print(f"  3. SUMMARY: {output_file}")
