import duckdb
import pandas as pd
from datetime import timedelta
import numpy as np

# Prelim settings
DATASET_TYPE = "COMMERCIAL_SET_A"  # {"COMMERCIAL_SET_A" "COMMERCIAL_SET_B", "MEDICARE_SET_A"}
DATABASE = "CCAE"                  # {"CCAE", "MDCR", "CCAEI"}  
TABLE_CODE = "O"                   # Using "O" for outpatient
YEARS = ["2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]

# Episode definition parameters
DX_DIGITS = 3  # Number of diagnosis code digits to use for matching (3 = ICD category level)
TIME_WINDOW_DAYS = 100  # Time window in days for episode grouping

# Connect to DuckDB
conn = duckdb.connect()

print("Step 1: Sampling 10 patients from 2014 data...")

# Get 10 random patients from just one year (2014) to keep memory low
sample_year_path = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_2014.parquet"

# Get unique patient IDs from 2014 and sample
patient_sample_query = f"""
SELECT DISTINCT ENROLID 
FROM '{sample_year_path}'
WHERE ENROLID IS NOT NULL
USING SAMPLE 0.1 PERCENT
"""

available_patients = conn.execute(patient_sample_query).df()
print(f"Found {len(available_patients):,} patients in sample")

# Randomly select 10 patients
np.random.seed(42)  # For reproducibility
if len(available_patients) >= 10:
    sampled_patients = np.random.choice(available_patients['ENROLID'].values, size=10, replace=False)
else:
    sampled_patients = available_patients['ENROLID'].values
    print(f"Warning: Only {len(sampled_patients)} patients available")

print(f"Selected patients: {sampled_patients}")

# Convert to list for SQL query
patient_list = ', '.join([str(int(p)) for p in sampled_patients])

print("\nStep 2: Loading data for selected patients across all years...")

# Now load only data for these specific patients across all years
df_list = []

for year in YEARS:
    file_path = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_{year}.parquet"
    
    try:
        query = f"""
        SELECT ENROLID, SVCDATE, DX1, DX2, DX3, DX4
        FROM '{file_path}'
        WHERE ENROLID IN ({patient_list})
        AND SVCDATE IS NOT NULL
        """
        
        df_year = conn.execute(query).df()
        
        if len(df_year) > 0:
            df_list.append(df_year)
            print(f"  {year}: {len(df_year):,} visits")
        else:
            print(f"  {year}: 0 visits")
            
    except Exception as e:
        print(f"  {year}: Error or file not found - {e}")

# Combine all years
if len(df_list) > 0:
    df = pd.concat(df_list, ignore_index=True)
    print(f"\nTotal visits loaded: {len(df):,}")
else:
    print("No data found for selected patients!")
    conn.close()
    exit()

# Convert SVCDATE to datetime
df['SVCDATE'] = pd.to_datetime(df['SVCDATE'])

# Sort by patient and date
df = df.sort_values(['ENROLID', 'SVCDATE']).reset_index(drop=True)

print(f"Date range: {df['SVCDATE'].min().date()} to {df['SVCDATE'].max().date()}")

print(f"\nStep 3: Defining episodes based on {TIME_WINDOW_DAYS}-day matching diagnostic codes (using first {DX_DIGITS} digits = ICD category level)...")

def define_episodes(patient_df, dx_digits=3, time_window_days=100):
    """
    Define episodes: if any diagnostic codes (DX1-DX4, first dx_digits) match within time_window_days of SVCDATE,
    they belong to the same episode.
    
    Special handling for visits with all None diagnoses:
    - If within time_window_days of an existing episode, assign to that episode
    - If more than time_window_days from any episode, create a new episode
    """
    if len(patient_df) == 0:
        return patient_df
    
    patient_df = patient_df.sort_values('SVCDATE').reset_index(drop=True)
    
    # Create a set of truncated diagnoses for each visit
    def get_dx_set(row, digits):
        dx_codes = []
        for dx_col in ['DX1', 'DX2', 'DX3', 'DX4']:
            val = row[dx_col]
            if pd.notna(val) and val != '' and val != ' ':
                # Truncate to first 'digits' characters
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
            # First visit starts episode 1
            episodes.append(episode_counter)
            continue
        
        # Look back at all previous visits within time_window_days
        lookback_date = current_date - timedelta(days=time_window_days)
        
        # Find matching episode
        matched_episode = None
        
        if has_dx:
            # Current visit has diagnoses - look for matching diagnosis codes
            for prev_idx in range(idx):
                prev_date = patient_df.iloc[prev_idx]['SVCDATE']
                prev_dx = patient_df.iloc[prev_idx]['all_dx']
                
                # Check if within time_window_days and has matching diagnosis
                if prev_date >= lookback_date:
                    # Check if any diagnosis code matches
                    if len(current_dx & prev_dx) > 0:
                        # Match found - use the same episode
                        matched_episode = episodes[prev_idx]
                        break
        else:
            # Current visit has NO diagnoses - assign to most recent episode within time_window_days
            for prev_idx in range(idx - 1, -1, -1):  # Start from most recent
                prev_date = patient_df.iloc[prev_idx]['SVCDATE']
                
                if prev_date >= lookback_date:
                    # Within time_window_days - use this episode
                    matched_episode = episodes[prev_idx]
                    break
        
        if matched_episode is not None:
            episodes.append(matched_episode)
        else:
            # No match found - start new episode
            episode_counter += 1
            episodes.append(episode_counter)
    
    patient_df['episode_id'] = episodes
    
    # Drop the temporary columns
    patient_df = patient_df.drop(['all_dx', 'has_dx'], axis=1)
    
    return patient_df

# Apply episode definition to each patient
df_with_episodes = pd.DataFrame()

for patient_id in sampled_patients:
    patient_data = df[df['ENROLID'] == patient_id].copy()
    if len(patient_data) > 0:
        patient_with_episodes = define_episodes(patient_data, dx_digits=DX_DIGITS, time_window_days=TIME_WINDOW_DAYS)
        df_with_episodes = pd.concat([df_with_episodes, patient_with_episodes], ignore_index=True)
        print(f"  Patient {patient_id}: {len(patient_data)} visits, {patient_with_episodes['episode_id'].nunique()} episodes")

# Create final long dataset with exact columns requested
final_dataset = df_with_episodes[['ENROLID', 'SVCDATE', 'DX1', 'DX2', 'DX3', 'DX4', 'episode_id']].copy()

# Extract year from SVCDATE for analysis
final_dataset['YEAR'] = final_dataset['SVCDATE'].dt.year

# Set display options to show ALL rows
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 30)

print("\n" + "="*100)
print(f"COMPLETE DATASET - ALL PATIENTS, ALL VISITS")
print(f"Episode Definition: First {DX_DIGITS} digits of diagnosis codes matching within {TIME_WINDOW_DAYS} days")
print("="*100)
print(f"Total rows: {len(final_dataset):,}")
print(f"Total patients: {final_dataset['ENROLID'].nunique()}")
print(f"Total episodes: {final_dataset['episode_id'].nunique()}")
print("\n")

# Print the entire dataset
print(final_dataset[['ENROLID', 'SVCDATE', 'DX1', 'DX2', 'DX3', 'DX4', 'episode_id']].to_string(index=False))

# COUNT UNIQUE EPISODES PER PATIENT PER YEAR
print("\n" + "="*100)
print("UNIQUE EPISODES PER PATIENT PER YEAR")
print("="*100)

episodes_per_patient_year = final_dataset.groupby(['ENROLID', 'YEAR'])['episode_id'].nunique().reset_index()
episodes_per_patient_year.columns = ['ENROLID', 'YEAR', 'Unique_Episodes']

# Pivot to show years as columns for easier reading
episodes_pivot = episodes_per_patient_year.pivot(index='ENROLID', columns='YEAR', values='Unique_Episodes')
episodes_pivot = episodes_pivot.fillna(0).astype(int)

print("\nEpisodes per patient per year (pivot table):")
print(episodes_pivot.to_string())

print("\n\nDetailed list:")
print(episodes_per_patient_year.to_string(index=False))

# Summary statistics
print("\n" + "="*100)
print("SUMMARY STATISTICS - EPISODES PER PATIENT PER YEAR")
print("="*100)
print(episodes_per_patient_year['Unique_Episodes'].describe())

print("\nTotal episodes by year:")
episodes_by_year = episodes_per_patient_year.groupby('YEAR')['Unique_Episodes'].sum().reset_index()
episodes_by_year.columns = ['YEAR', 'Total_Episodes']
print(episodes_by_year.to_string(index=False))

print("\n" + "="*100)
print("DETAILED BREAKDOWN BY PATIENT")
print("="*100)

for patient_id in sorted(sampled_patients):
    patient_visits = final_dataset[final_dataset['ENROLID'] == patient_id]
    
    if len(patient_visits) == 0:
        print(f"\n{'='*100}")
        print(f"Patient {patient_id}: No visits")
        print(f"{'='*100}")
        continue
    
    print(f"\n{'='*100}")
    print(f"Patient {patient_id}")
    print(f"{'='*100}")
    print(f"Total visits: {len(patient_visits)}")
    print(f"Total episodes: {patient_visits['episode_id'].nunique()}")
    print(f"Date range: {patient_visits['SVCDATE'].min().date()} to {patient_visits['SVCDATE'].max().date()}")
    
    # Episodes by year for this patient
    patient_episodes_by_year = patient_visits.groupby('YEAR')['episode_id'].nunique().reset_index()
    patient_episodes_by_year.columns = ['YEAR', 'Episodes']
    print("\nEpisodes by year:")
    print(patient_episodes_by_year.to_string(index=False))
    
    # Count visits with no diagnoses
    no_dx_count = patient_visits[(patient_visits['DX1'].isna() | (patient_visits['DX1'] == '')) & 
                                  (patient_visits['DX2'].isna() | (patient_visits['DX2'] == '')) &
                                  (patient_visits['DX3'].isna() | (patient_visits['DX3'] == '')) &
                                  (patient_visits['DX4'].isna() | (patient_visits['DX4'] == ''))].shape[0]
    if no_dx_count > 0:
        print(f"Visits with no diagnoses: {no_dx_count}")
    
    print("\nAll visits:")
    print(patient_visits[['SVCDATE', 'DX1', 'DX2', 'DX3', 'DX4', 'episode_id', 'YEAR']].to_string(index=False))

# Episode statistics
print("\n" + "="*100)
print("EPISODE STATISTICS")
print("="*100)

episode_summary = final_dataset.groupby(['ENROLID', 'episode_id']).agg({
    'SVCDATE': ['count', 'min', 'max']
})
episode_summary.columns = ['Visits', 'Start_Date', 'End_Date']
episode_summary['Duration_Days'] = (episode_summary['End_Date'] - episode_summary['Start_Date']).dt.days
episode_summary = episode_summary.reset_index()

print("\nAll episodes:")
print(episode_summary.to_string(index=False))

print("\n\nVisits per episode - Summary statistics:")
print(episode_summary['Visits'].describe())

print("\nEpisode duration (days) - Summary statistics:")
print(episode_summary['Duration_Days'].describe())

# Analyze episodes with many visits
print("\nEpisodes with 5+ visits:")
long_episodes = episode_summary[episode_summary['Visits'] >= 5]
if len(long_episodes) > 0:
    print(long_episodes.to_string(index=False))
else:
    print("No episodes with 5+ visits")

# Save final dataset
output_file = f'patient_episodes_long_dataset_{DX_DIGITS}digit_{TIME_WINDOW_DAYS}days.csv'
final_dataset[['ENROLID', 'SVCDATE', 'DX1', 'DX2', 'DX3', 'DX4', 'episode_id']].to_csv(output_file, index=False)

# Save episodes per patient per year summary
summary_file = f'episodes_per_patient_year_{DX_DIGITS}digit_{TIME_WINDOW_DAYS}days.csv'
episodes_per_patient_year.to_csv(summary_file, index=False)

print(f"\n{'='*100}")
print(f"Final long dataset saved to: {output_file}")
print(f"Episodes per patient per year saved to: {summary_file}")
print(f"Columns: {list(final_dataset[['ENROLID', 'SVCDATE', 'DX1', 'DX2', 'DX3', 'DX4', 'episode_id']].columns)}")
print(f"Total rows: {len(final_dataset):,}")
print(f"Episode definition: First {DX_DIGITS} digits of diagnosis codes matching within {TIME_WINDOW_DAYS} days")
print(f"{'='*100}")

conn.close()
print("\nAnalysis complete!")
