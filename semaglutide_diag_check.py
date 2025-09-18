import duckdb
import pandas as pd
import numpy as np
import gc
import os
from datetime import datetime

def print_memory_usage():
    """Print current memory usage using resource module (built-in)"""
    try:
        import resource
        memory_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Linux/Mac
        print(f"Current memory usage: {memory_mb:.0f} MB")
    except:
        print("Memory monitoring not available")

def process_single_year(year, semaglutide_ndcs, diabetes_codes, obesity_codes, output_dir):
    """Process a single year and return results"""
    
    print(f"\n" + "="*60)
    print(f"PROCESSING YEAR {year}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    print_memory_usage()
    
    conn = duckdb.connect()
    
    # Step 1: Get prescriptions for this year
    print(f"\nStep 1: Getting {year} prescriptions...")
    ndc_list = "', '".join(semaglutide_ndcs)
    
    try:
        prescriptions = conn.execute(f"""
            SELECT ENROLID, SVCDATE, NDCNUM, {year} as YEAR
            FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_{year}.parquet'
            WHERE NDCNUM IN ('{ndc_list}')
        """).df()
        
        print(f"  {year} prescriptions found: {len(prescriptions):,}")
        
        if len(prescriptions) == 0:
            print(f"  No prescriptions found for {year}, skipping...")
            conn.close()
            return None
            
        # Clean data
        prescriptions = prescriptions.dropna(subset=['ENROLID'])
        prescriptions['ENROLID'] = prescriptions['ENROLID'].astype(int)
        
        unique_patients = prescriptions['ENROLID'].unique()
        print(f"  Unique patients in {year}: {len(unique_patients):,}")
        print_memory_usage()
        
    except Exception as e:
        print(f"  Error getting {year} prescriptions: {e}")
        conn.close()
        return None
    
    # Step 2: Process diagnosis data for these patients
    print(f"\nStep 2: Processing diagnoses for {year} patients...")
    
    # Process in small chunks for memory efficiency
    chunk_size = 1000
    patient_chunks = [unique_patients[i:i + chunk_size] for i in range(0, len(unique_patients), chunk_size)]
    print(f"  Processing {len(patient_chunks)} chunks of {chunk_size} patients each")
    
    # Track diagnosis results
    cumulative_diabetes_counts = {}
    cumulative_obesity_counts = {}
    
    for chunk_idx, patient_chunk in enumerate(patient_chunks):
        if (chunk_idx + 1) % 10 == 0:
            print(f"    Processing chunk {chunk_idx + 1}/{len(patient_chunks)}...")
            print_memory_usage()
        
        patient_list = ', '.join(map(str, patient_chunk))
        
        # Process diabetes codes
        for code in diabetes_codes:
            try:
                result = conn.execute(f"""
                    SELECT ENROLID, COUNT(*) as freq
                    FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_I.parquet'
                    WHERE ENROLID IN ({patient_list})
                    AND (PDX = '{code}' OR DX1 = '{code}' OR DX2 = '{code}' OR DX3 = '{code}' OR 
                         DX4 = '{code}' OR DX5 = '{code}' OR DX6 = '{code}' OR DX7 = '{code}' OR
                         DX8 = '{code}' OR DX9 = '{code}' OR DX10 = '{code}' OR DX11 = '{code}' OR
                         DX12 = '{code}' OR DX13 = '{code}' OR DX14 = '{code}' OR DX15 = '{code}')
                    GROUP BY ENROLID
                """).df()
                
                if len(result) > 0:
                    for _, row in result.iterrows():
                        patient_id = row['ENROLID']
                        freq = row['freq']
                        if patient_id not in cumulative_diabetes_counts:
                            cumulative_diabetes_counts[patient_id] = 0
                        cumulative_diabetes_counts[patient_id] += freq
                
                del result
                
            except Exception as e:
                pass  # Skip errors for individual codes
        
        # Process obesity codes
        for code in obesity_codes:
            try:
                result = conn.execute(f"""
                    SELECT ENROLID, COUNT(*) as freq
                    FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_I.parquet'
                    WHERE ENROLID IN ({patient_list})
                    AND (PDX = '{code}' OR DX1 = '{code}' OR DX2 = '{code}' OR DX3 = '{code}' OR 
                         DX4 = '{code}' OR DX5 = '{code}' OR DX6 = '{code}' OR DX7 = '{code}' OR
                         DX8 = '{code}' OR DX9 = '{code}' OR DX10 = '{code}' OR DX11 = '{code}' OR
                         DX12 = '{code}' OR DX13 = '{code}' OR DX14 = '{code}' OR DX15 = '{code}')
                    GROUP BY ENROLID
                """).df()
                
                if len(result) > 0:
                    for _, row in result.iterrows():
                        patient_id = row['ENROLID']
                        freq = row['freq']
                        if patient_id not in cumulative_obesity_counts:
                            cumulative_obesity_counts[patient_id] = 0
                        cumulative_obesity_counts[patient_id] += freq
                
                del result
                
            except Exception as e:
                pass  # Skip errors for individual codes
        
        # Clean up
        gc.collect()
    
    print(f"  Diagnosis processing complete for {year}")
    print(f"    Patients with diabetes diagnoses: {len(cumulative_diabetes_counts):,}")
    print(f"    Patients with obesity diagnoses: {len(cumulative_obesity_counts):,}")
    
    # Step 3: Get age data for this year's patients
    print(f"\nStep 3: Getting age data for {year} patients...")
    
    age_data = pd.DataFrame()
    
    for patient_chunk in patient_chunks:
        patient_list = ', '.join(map(str, patient_chunk))
        
        try:
            chunk_age_result = conn.execute(f"""
                SELECT ENROLID, {year} - DOBYR as age_at_year
                FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_I.parquet'
                WHERE ENROLID IN ({patient_list})
                AND DOBYR IS NOT NULL
                GROUP BY ENROLID, DOBYR
            """).df()
            
            if len(chunk_age_result) > 0:
                age_data = pd.concat([age_data, chunk_age_result], ignore_index=True)
            
            del chunk_age_result
            gc.collect()
            
        except Exception as e:
            pass  # Skip age errors
    
    print(f"  Age data collected for {len(age_data):,} patients")
    
    # Step 4: Create final dataset for this year
    print(f"\nStep 4: Creating final dataset for {year}...")
    
    # Convert diagnosis counts to dataframes
    if cumulative_diabetes_counts:
        diabetes_summary = pd.DataFrame(list(cumulative_diabetes_counts.items()), 
                                      columns=['ENROLID', 'diabetes_frequency'])
    else:
        diabetes_summary = pd.DataFrame(columns=['ENROLID', 'diabetes_frequency'])
    
    if cumulative_obesity_counts:
        obesity_summary = pd.DataFrame(list(cumulative_obesity_counts.items()), 
                                     columns=['ENROLID', 'obesity_frequency'])
    else:
        obesity_summary = pd.DataFrame(columns=['ENROLID', 'obesity_frequency'])
    
    # Merge all data
    final_dataset = prescriptions.merge(diabetes_summary, on='ENROLID', how='left')
    del diabetes_summary
    gc.collect()
    
    final_dataset = final_dataset.merge(obesity_summary, on='ENROLID', how='left')
    del obesity_summary
    gc.collect()
    
    if len(age_data) > 0:
        final_dataset = final_dataset.merge(age_data, on='ENROLID', how='left')
    del age_data
    gc.collect()
    
    # Fill missing values
    final_dataset['diabetes_frequency'] = final_dataset['diabetes_frequency'].fillna(0).astype(int)
    final_dataset['obesity_frequency'] = final_dataset['obesity_frequency'].fillna(0).astype(int)
    
    # Summary for this year
    print(f"\n{year} RESULTS:")
    print(f"  Total prescriptions: {len(final_dataset):,}")
    print(f"  Unique patients: {final_dataset['ENROLID'].nunique():,}")
    print(f"  With diabetes diagnoses: {(final_dataset['diabetes_frequency'] > 0).sum():,}")
    print(f"  With obesity diagnoses: {(final_dataset['obesity_frequency'] > 0).sum():,}")
    print(f"  With both conditions: {((final_dataset['diabetes_frequency'] > 0) & (final_dataset['obesity_frequency'] > 0)).sum():,}")
    if 'age_at_year' in final_dataset.columns:
        print(f"  With age data: {final_dataset['age_at_year'].notna().sum():,}")
    
    # Save individual year file
    year_output_file = f'{output_dir}/semaglutide_{year}.csv'
    final_dataset.to_csv(year_output_file, index=False)
    print(f"  {year} data saved to: {year_output_file}")
    
    print(f"Year {year} completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print_memory_usage()
    
    conn.close()
    return final_dataset


# Main execution
def main():
    print("YEAR-BY-YEAR SEMAGLUTIDE ANALYSIS")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configuration
    semaglutide_ndcs = [
        '00169413001', '00169413013', '00169413211', '00169413212',
        '00169413290', '00169413297', '00169413602', '00169413611', 
        '00169418103', '00169418113', '00169418190', '00169418197', 
        '00169477211', '00169477212', '00169477290', '00169477297',
        '50090594900', '50090513800', '50090513900', '50090605100'
    ]
    
    diabetes_codes = [
        # ICD-9 codes
        '25000', '25002', '25010', '25012', '25020', '25022', '25030', '25032',
        '25040', '25042', '25050', '25052', '25060', '25062', '25070', '25072',
        '25080', '25082', '25090', '25092',
        
        # ICD-10 codes  
        'E1100', 'E1101', 'E1110', 'E1111', 'E1121', 'E1122', 'E1129',
        'E1131', 'E1132', 'E1133', 'E1134', 'E1135', 'E1136', 'E1137', 'E1138', 'E1139',
        'E1140', 'E1141', 'E1142', 'E1143', 'E1144', 'E1149',
        'E1151', 'E1152', 'E1159', 'E1161', 'E1162', 'E1163', 'E1164', 'E1165', 'E1169',
        'E118', 'E119'
    ]
    
    obesity_codes = [
        # ICD-9 codes
        '278', '27800', '27801', '27802', '27803',
        
        # ICD-10 codes
        'E660', 'E6601', 'E6609', 'E661', 'E662', 'E663', 'E668', 'E669',
        'Z6825', 'Z6826', 'Z6827', 'Z6828', 'Z6829',
        'Z6830', 'Z6831', 'Z6832', 'Z6833', 'Z6834',
        'Z6835', 'Z6836', 'Z6837', 'Z6838', 'Z6839',
        'Z684', 'Z6841', 'Z6842', 'Z6843', 'Z6844', 'Z6845'
    ]
    
    years = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
    
    # CUSTOMIZABLE YEAR SELECTION
    # years_to_process = years  # Process all years
    years_to_process = [2018]  # Only specific years

    output_dir = '/data/MarketScan_data'
    
    print(f"Total diabetes codes: {len(diabetes_codes)}")
    print(f"Total obesity codes: {len(obesity_codes)}")
    print(f"Years available: {years}")
    print(f"Years to process: {years_to_process}")
    
    # Process each year
    all_year_datasets = []
    successful_years = []
    
    for year in years_to_process:
        try:
            year_dataset = process_single_year(year, semaglutide_ndcs, diabetes_codes, obesity_codes, output_dir)
            
            if year_dataset is not None:
                all_year_datasets.append(year_dataset)
                successful_years.append(year)
            
            # Clean up after each year
            del year_dataset
            gc.collect()
            
        except Exception as e:
            print(f"Error processing {year}: {e}")
            continue
    
    # Combine all years
    if all_year_datasets:
        print(f"\n" + "="*60)
        print("COMBINING ALL YEARS")
        print("="*60)
        print(f"Successfully processed years: {successful_years}")
        print(f"Combining {len(all_year_datasets)} year datasets...")
        print_memory_usage()
        
        # Combine all datasets
        combined_dataset = pd.concat(all_year_datasets, ignore_index=True)
        
        print(f"\nCOMBINED RESULTS (2018-2024):")
        print(f"  Total prescriptions: {len(combined_dataset):,}")
        print(f"  Unique patients: {combined_dataset['ENROLID'].nunique():,}")
        print(f"  With diabetes diagnoses: {(combined_dataset['diabetes_frequency'] > 0).sum():,}")
        print(f"  With obesity diagnoses: {(combined_dataset['obesity_frequency'] > 0).sum():,}")
        print(f"  With both conditions: {((combined_dataset['diabetes_frequency'] > 0) & (combined_dataset['obesity_frequency'] > 0)).sum():,}")
        
        # Year breakdown
        print(f"\nBREAKDOWN BY YEAR:")
        year_summary = combined_dataset.groupby('YEAR').agg({
            'ENROLID': ['count', 'nunique'],
            'diabetes_frequency': lambda x: (x > 0).sum(),
            'obesity_frequency': lambda x: (x > 0).sum()
        })
        year_summary.columns = ['total_prescriptions', 'unique_patients', 'with_diabetes', 'with_obesity']
        print(year_summary)
        
        # Save combined dataset
        combined_output_file = f'{output_dir}/semaglutide_combined_2018_2024.csv'
        combined_dataset.to_csv(combined_output_file, index=False)
        print(f"\nCombined dataset saved to: {combined_output_file}")
        
        # Save summary
        summary_stats = {
            'total_prescriptions': len(combined_dataset),
            'unique_patients': combined_dataset['ENROLID'].nunique(),
            'with_diabetes': (combined_dataset['diabetes_frequency'] > 0).sum(),
            'with_obesity': (combined_dataset['obesity_frequency'] > 0).sum(),
            'with_both': ((combined_dataset['diabetes_frequency'] > 0) & (combined_dataset['obesity_frequency'] > 0)).sum(),
            'years_processed': successful_years,
            'processing_completed': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        summary_df = pd.DataFrame([summary_stats])
        summary_df.to_csv(f'{output_dir}/semaglutide_analysis_summary.csv', index=False)
        
        print_memory_usage()
        
    else:
        print("No data processed successfully!")
    
    print(f"\nAnalysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
