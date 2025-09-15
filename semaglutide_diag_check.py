import duckdb
import pandas as pd
import numpy as np
import csv
from datetime import datetime

# Memory-efficient semaglutide analysis
print("="*80)
print("MEMORY-EFFICIENT SEMAGLUTIDE ANALYSIS")
print("="*80)

# Configuration
CHUNK_SIZE = 500  # Process patients in smaller chunks
OUTPUT_FILE = '/data/MarketScan_data/semaglutide_analysis_results.csv'

# Semaglutide NDCs and diagnosis codes
semaglutide_ndcs = [
    '00169413001', '00169413013', '00169413211', '00169413212',
    '00169413290', '00169413297', '00169413602', '00169413611', 
    '00169418103', '00169418113', '00169418190', '00169418197', 
    '00169477211', '00169477212', '00169477290', '00169477297',
    '50090594900', '50090513800', '50090513900', '50090605100',
    '00169430301', '00169430313', '00169430330', '00169430390', 
    '00169430393', '00169430399', '00169430701', '00169430713', 
    '00169430730', '00169431401', '00169431413', '00169431430', 
    '00169480430', '00169480930', '00169481530', '00169481590',
    '00169450101', '00169450114', '00169450501', '00169450514',
    '00169451701', '00169451714', '00169452401', '00169452414', 
    '00169452501', '00169452514', '00169452590', '00169452594',
    '50090582400'
]

type2_diabetes_codes = ['25000', '25002', '25010', '25012', '25020', '25022', '25030', '25032',
                       '25040', '25042', '25050', '25052', '25060', '25062', '25070', '25072',
                       '25080', '25082', '25090', '25092', 'E11']

obesity_codes = ['27800', '27801', 'E6600', 'E6601', 'E6609', 'E663', 'E668', 'E669']

weight_comorbidity_codes = ['4019', 'I10', '2720', '2721', '2722', '2724', 'E780', 'E781', 'E782', 'E785']

ndc_list = "', '".join(semaglutide_ndcs)
years = range(2014, 2025)

conn = duckdb.connect()

def count_diagnosis_codes(patient_records, codes):
    """Count diagnosis codes in patient records"""
    dx_cols = ['PDX', 'DX1', 'DX2', 'DX3', 'DX4', 'DX5', 'DX6', 'DX7', 
               'DX8', 'DX9', 'DX10', 'DX11', 'DX12', 'DX13', 'DX14', 'DX15']
    
    count = 0
    for _, row in patient_records.iterrows():
        for col in dx_cols:
            if pd.notna(row[col]):
                code_str = str(row[col])
                for code in codes:
                    if code_str.startswith(code):
                        count += 1
                        break
    return count

def get_age_at_2018(patient_records):
    """Calculate age at 2018 from diagnosis records"""
    for _, row in patient_records.iterrows():
        if pd.notna(row['AGE']) and pd.notna(row['ADMDATE']):
            admission_year = pd.to_datetime(row['ADMDATE']).year
            age_at_admission = row['AGE']
            return age_at_admission + (2018 - admission_year)
    return np.nan

# Step 1: Process prescriptions year by year to save memory
print("Step 1: Processing prescriptions by year...")

# Initialize output file
with open(OUTPUT_FILE, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'ENROLID', 'SVCDATE', 'NDCNUM', 'YEAR',
        'type2_diabetes_frequency', 'obesity_frequency', 'weight_comorbidity_frequency',
        'age_at_2018', 'diabetes_indication', 'weight_indication', 'any_indication'
    ])

total_prescriptions = 0
processed_patients = set()

for year in years:
    print(f"\nProcessing {year}...")
    
    try:
        # Get prescriptions for this year only
        prescriptions = conn.execute(f"""
            SELECT ENROLID, SVCDATE, NDCNUM
            FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_{year}.parquet'
            WHERE NDCNUM IN ('{ndc_list}')
        """).df()
        
        if len(prescriptions) == 0:
            print(f"  No prescriptions in {year}")
            continue
            
        print(f"  Found {len(prescriptions):,} prescriptions")
        total_prescriptions += len(prescriptions)
        
        # Get unique patients for this year
        year_patients = prescriptions['ENROLID'].unique()
        new_patients = [p for p in year_patients if p not in processed_patients]
        
        print(f"  {len(new_patients):,} new patients to analyze")
        
        # Process patients in chunks
        patient_chunks = [new_patients[i:i+CHUNK_SIZE] for i in range(0, len(new_patients), CHUNK_SIZE)]
        
        patient_diagnosis_cache = {}  # Cache diagnosis data for this year's patients
        
        for chunk_idx, patient_chunk in enumerate(patient_chunks):
            print(f"    Processing chunk {chunk_idx+1}/{len(patient_chunks)} ({len(patient_chunk)} patients)")
            
            # Get diagnosis history for this chunk across all years
            chunk_diagnosis_data = []
            
            for diag_year in years:
                try:
                    patient_chunk_str = "', '".join(map(str, patient_chunk))
                    
                    diag_data = conn.execute(f"""
                        SELECT ENROLID, ADMDATE, AGE, 
                               PDX, DX1, DX2, DX3, DX4, DX5, DX6, DX7, DX8, DX9, 
                               DX10, DX11, DX12, DX13, DX14, DX15
                        FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_I_{diag_year}.parquet'
                        WHERE ENROLID IN ('{patient_chunk_str}')
                    """).df()
                    
                    if len(diag_data) > 0:
                        chunk_diagnosis_data.append(diag_data)
                        
                except Exception as e:
                    continue
            
            # Combine diagnosis data for this chunk
            if chunk_diagnosis_data:
                combined_diag = pd.concat(chunk_diagnosis_data, ignore_index=True)
                combined_diag['ADMDATE'] = pd.to_datetime(combined_diag['ADMDATE'])
                
                # Calculate frequencies for each patient in chunk
                for patient_id in patient_chunk:
                    patient_records = combined_diag[combined_diag['ENROLID'] == patient_id]
                    
                    if len(patient_records) > 0:
                        patient_diagnosis_cache[patient_id] = {
                            'diabetes_freq': count_diagnosis_codes(patient_records, type2_diabetes_codes),
                            'obesity_freq': count_diagnosis_codes(patient_records, obesity_codes),
                            'comorbidity_freq': count_diagnosis_codes(patient_records, weight_comorbidity_codes),
                            'age_2018': get_age_at_2018(patient_records)
                        }
                    else:
                        patient_diagnosis_cache[patient_id] = {
                            'diabetes_freq': 0, 'obesity_freq': 0, 'comorbidity_freq': 0, 'age_2018': np.nan
                        }
                
                # Clear memory
                del combined_diag, chunk_diagnosis_data
            
            # Add new patients to processed set
            processed_patients.update(patient_chunk)
        
        # Now process prescriptions for this year and write to file
        print(f"  Writing {len(prescriptions):,} prescription records...")
        
        with open(OUTPUT_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            
            for _, prescription in prescriptions.iterrows():
                patient_id = prescription['ENROLID']
                
                # Get cached diagnosis data
                if patient_id in patient_diagnosis_cache:
                    diag_data = patient_diagnosis_cache[patient_id]
                else:
                    diag_data = {'diabetes_freq': 0, 'obesity_freq': 0, 'comorbidity_freq': 0, 'age_2018': np.nan}
                
                # Calculate indications
                diabetes_indication = diag_data['diabetes_freq'] > 0
                weight_indication = (diag_data['obesity_freq'] > 0) or (diag_data['comorbidity_freq'] > 0)
                any_indication = diabetes_indication or weight_indication
                
                # Write row
                writer.writerow([
                    patient_id,
                    prescription['SVCDATE'],
                    prescription['NDCNUM'],
                    year,
                    diag_data['diabetes_freq'],
                    diag_data['obesity_freq'],
                    diag_data['comorbidity_freq'],
                    diag_data['age_2018'],
                    diabetes_indication,
                    weight_indication,
                    any_indication
                ])
        
        # Clear memory for next year
        del prescriptions, patient_diagnosis_cache
        print(f"  Year {year} complete. Memory cleared.")
        
    except Exception as e:
        print(f"  Error processing {year}: {e}")

conn.close()

# Step 2: Generate summary statistics
print(f"\n" + "="*80)
print("GENERATING SUMMARY STATISTICS")
print("="*80)

print(f"Reading results file for summary...")
results_df = pd.read_csv(OUTPUT_FILE)

print(f"\nFINAL RESULTS:")
print(f"Total prescriptions processed: {len(results_df):,}")
print(f"Unique patients: {results_df['ENROLID'].nunique():,}")
print(f"Prescriptions with diabetes indication: {results_df['diabetes_indication'].sum():,}")
print(f"Prescriptions with weight indication: {results_df['weight_indication'].sum():,}")
print(f"Prescriptions with any indication: {results_df['any_indication'].sum():,}")

print(f"\nAge statistics:")
print(results_df['age_at_2018'].describe())

print(f"\nMost common NDCs:")
print(results_df['NDCNUM'].value_counts().head(10))

print(f"\nSample records:")
print(results_df.head(10).to_string())

print(f"\nResults saved to: {OUTPUT_FILE}")
print("Analysis complete!")
