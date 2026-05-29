import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

# Settings
YEARS = [str(year) for year in range(2014, 2019)]  # 2014-2018
SAMPLE_SIZE = 15000
output_dir = '/home/zl749/episode_outputs'

# Load and combine all years
all_episodes = []

for year in YEARS:
    sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
    episode_file = os.path.join(output_dir, f'episodes_level1_k_band_{year}{sample_suffix}.csv')
    
    if not os.path.exists(episode_file):
        print(f"[{year}] File not found: {episode_file}")
        continue
    
    try:
        episodes = pd.read_csv(episode_file)
        print(f"[{year}] Loaded {len(episodes):,} episodes")
        all_episodes.append(episodes)
    except Exception as e:
        print(f"[{year}] Error loading: {e}")

if len(all_episodes) == 0:
    print("No data loaded!")
    exit()

# Combine all years
combined = pd.concat(all_episodes, ignore_index=True)
print(f"\nCombined: {len(combined):,} total episodes (2014-2018)")

# Calculate m_RVU and R_RVU
combined['m_RVU'] = combined['WORK_RVU'] + combined['MP_RVU']
combined['R_RVU'] = combined['WORK_RVU'] + combined['MP_RVU'] + combined['PE_RVU_actualized'].fillna(0)

# Calculate dollar amounts
combined['m_RVU_Dollars'] = combined['Total_Work_MP_Dollars']
combined['R_RVU_Dollars'] = combined['TOTAL_RVU_Dollars']
combined['R_Dollars_inc_COB'] = combined['PAY'] + combined['COPAY'] + combined['COINS'] + combined['DEDUCT'] + combined['COB']
combined['R_Dollars_omit_COB'] = combined['PAY'] + combined['COPAY'] + combined['COINS'] + combined['DEDUCT']

# Select columns for output CSV
output_columns = [
    'ENROLID', 
    'episode_id', 
    'year',
    'm_RVU',
    'R_RVU',
    'm_RVU_Dollars',
    'R_RVU_Dollars',
    'R_Dollars_inc_COB',
    'R_Dollars_omit_COB',
    'n_claims',
    'n_visit',
    'only_99213',
    'only_99214',
    'contains_99213',
    'contains_99214',
    'in_k_99213',
    'in_k_99214',
    'WORK_RVU',
    'MP_RVU',
    'PE_RVU_actualized',
    'PAY',
    'NETPAY',
    'COPAY',
    'COINS',
    'DEDUCT',
    'OOP',
    'COB',
    'conversion_factor'
]

# Create output dataframe
output_df = combined[output_columns].copy()

# Save full episode table
sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
full_table_file = os.path.join(output_dir, f'episodes_2014_2018_full_table{sample_suffix}.csv')
output_df.to_csv(full_table_file, index=False)
file_size = os.path.getsize(full_table_file) / (1024**2)
print(f"\n✓ Full episode table saved to: {full_table_file} ({file_size:.1f} MB)")
print(f"  Rows: {len(output_df):,}")
