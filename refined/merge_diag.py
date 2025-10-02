import pandas as pd
import duckdb
import glob
import os
import time
from pathlib import Path

# Configuration
START_YEAR = 2018
END_YEAR = 2024
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"

def load_nonzero_diagnosis_codes():
    """
    Load diagnosis codes that actually appear in the data from the CSV file
    """
    csv_path = '/data/MarketScan_data/diagnosis_code_frequency_2021_sample_nonzero.csv'
    
    if not Path(csv_path).exists():
        print(f"ERROR: Could not find {csv_path}")
        print("Cannot proceed without diagnosis codes")
        return None, None
    
    # Read the CSV
    df = pd.read_csv(csv_path)
    
    # Filter for diabetes codes with non-zero occurrences
    diabetes_codes = df[df['code_type'] == 'diabetes']['diagnosis_code'].tolist()
    
    # Filter for obesity codes with non-zero occurrences
    obesity_codes = df[df['code_type'] == 'obesity']['diagnosis_code'].tolist()
    
    print(f"Loaded diagnosis codes from {csv_path}")
    print(f"  Diabetes codes (non-zero): {len(diabetes_codes)}")
    print(f"  Obesity codes (non-zero): {len(obesity_codes)}")
    
    return diabetes_codes, obesity_codes

def setup_duckdb_connection(memory_limit='8GB'):
    """
    Create optimized DuckDB connection with performance settings
    """
    conn = duckdb.connect(':memory:')
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.execute("SET max_temp_directory_size='50GB'")
    conn.execute("SET temp_directory='/tmp'")
    conn.execute("SET threads=8")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute("SET enable_progress_bar=false")
    return conn

def extract_diagnosis_flags_for_patients(patient_ids, year, diabetes_codes, obesity_codes):
    """
    Extract diagnosis eligibility flags for a set of patients
    Checks inpatient and outpatient files for diabetes and obesity codes
    Uses only codes that had non-zero occurrences in the data
    """
    print(f"     Checking diagnosis codes for {len(patient_ids):,} patients...")
    
    conn = setup_duckdb_connection()
    
    try:
        # Create patient lookup table
        patients_df = pd.DataFrame({'ENROLID': list(patient_ids)})
        conn.register('target_patients', patients_df)
        
        # Files to check - both inpatient and outpatient
        data_path = f"/data/MarketScan_data/{DATASET_TYPE}"
        files_to_check = []
        
        # Check all years up to and including the target year
        for y in range(START_YEAR, year + 1):
            inpatient_file = f"{data_path}/{DATABASE}_I_{y}.parquet"
            outpatient_file = f"{data_path}/{DATABASE}_O_{y}.parquet"
            
            if Path(inpatient_file).exists():
                files_to_check.append(inpatient_file)
            if Path(outpatient_file).exists():
                files_to_check.append(outpatient_file)
        
        if not files_to_check:
            print(f"     No diagnosis files found")
            result = patients_df.copy()
            result['d_diagnosis_eligible_diab'] = 0
            result['d_diagnosis_eligible_obes'] = 0
            return result
        
        print(f"     Checking {len(files_to_check)} diagnosis files...")
        
        # Build diagnosis checking query
        diabetes_codes_str = "', '".join(diabetes_codes)
        obesity_codes_str = "', '".join(obesity_codes)
        file_list_str = "', '".join(files_to_check)
        
        # Query to find patients with diabetes or obesity codes
        diagnosis_query = f"""
        WITH diagnosis_data AS (
            SELECT DISTINCT
                ENROLID,
                CASE WHEN (
                    PDX IN ('{diabetes_codes_str}') OR
                    DX1 IN ('{diabetes_codes_str}') OR
                    DX2 IN ('{diabetes_codes_str}') OR
                    DX3 IN ('{diabetes_codes_str}') OR
                    DX4 IN ('{diabetes_codes_str}') OR
                    DX5 IN ('{diabetes_codes_str}') OR
                    DX6 IN ('{diabetes_codes_str}') OR
                    DX7 IN ('{diabetes_codes_str}') OR
                    DX8 IN ('{diabetes_codes_str}') OR
                    DX9 IN ('{diabetes_codes_str}') OR
                    DX10 IN ('{diabetes_codes_str}') OR
                    DX11 IN ('{diabetes_codes_str}') OR
                    DX12 IN ('{diabetes_codes_str}') OR
                    DX13 IN ('{diabetes_codes_str}') OR
                    DX14 IN ('{diabetes_codes_str}') OR
                    DX15 IN ('{diabetes_codes_str}')
                ) THEN 1 ELSE 0 END as has_diabetes,
                CASE WHEN (
                    PDX IN ('{obesity_codes_str}') OR
                    DX1 IN ('{obesity_codes_str}') OR
                    DX2 IN ('{obesity_codes_str}') OR
                    DX3 IN ('{obesity_codes_str}') OR
                    DX4 IN ('{obesity_codes_str}') OR
                    DX5 IN ('{obesity_codes_str}') OR
                    DX6 IN ('{obesity_codes_str}') OR
                    DX7 IN ('{obesity_codes_str}') OR
                    DX8 IN ('{obesity_codes_str}') OR
                    DX9 IN ('{obesity_codes_str}') OR
                    DX10 IN ('{obesity_codes_str}') OR
                    DX11 IN ('{obesity_codes_str}') OR
                    DX12 IN ('{obesity_codes_str}') OR
                    DX13 IN ('{obesity_codes_str}') OR
                    DX14 IN ('{obesity_codes_str}') OR
                    DX15 IN ('{obesity_codes_str}')
                ) THEN 1 ELSE 0 END as has_obesity
            FROM read_parquet(['{file_list_str}'])
            WHERE ENROLID IN (SELECT ENROLID FROM target_patients)
        ),
        patient_flags AS (
            SELECT 
                ENROLID,
                MAX(has_diabetes) as d_diagnosis_eligible_diab,
                MAX(has_obesity) as d_diagnosis_eligible_obes
            FROM diagnosis_data
            GROUP BY ENROLID
        )
        SELECT 
            tp.ENROLID,
            COALESCE(pf.d_diagnosis_eligible_diab, 0) as d_diagnosis_eligible_diab,
            COALESCE(pf.d_diagnosis_eligible_obes, 0) as d_diagnosis_eligible_obes
        FROM target_patients tp
        LEFT JOIN patient_flags pf ON tp.ENROLID = pf.ENROLID
        """
        
        result_df = conn.execute(diagnosis_query).fetchdf()
        conn.close()
        
        diab_count = result_df['d_diagnosis_eligible_diab'].sum()
        obes_count = result_df['d_diagnosis_eligible_obes'].sum()
        print(f"     Diabetes eligible: {diab_count:,} ({diab_count/len(result_df)*100:.1f}%)")
        print(f"     Obesity eligible: {obes_count:,} ({obes_count/len(result_df)*100:.1f}%)")
        
        return result_df
        
    except Exception as e:
        print(f"     Error extracting diagnosis flags: {e}")
        import traceback
        traceback.print_exc()
        conn.close()
        # Return dataframe with all zeros on error
        result = patients_df.copy()
        result['d_diagnosis_eligible_diab'] = 0
        result['d_diagnosis_eligible_obes'] = 0
        return result

def add_diagnosis_flags_to_file(events_file, year, diabetes_codes, obesity_codes):
    """
    Add diagnosis eligibility flags to a single prescription events file
    """
    filename = os.path.basename(events_file)
    print(f"  Processing: {filename}")
    
    try:
        # Load events file
        events_df = pd.read_parquet(events_file)
        
        # Get unique patients
        unique_patients = set(events_df['ENROLID'].unique())
        print(f"     Total events: {len(events_df):,}")
        print(f"     Unique patients: {len(unique_patients):,}")
        
        # Extract diagnosis flags for these patients
        diagnosis_flags_df = extract_diagnosis_flags_for_patients(unique_patients, year, 
                                                                  diabetes_codes, obesity_codes)
        
        # Merge diagnosis flags with events
        conn = setup_duckdb_connection()
        conn.register('events_df', events_df)
        conn.register('diagnosis_flags', diagnosis_flags_df)
        
        merge_query = """
        SELECT 
            e.*,
            COALESCE(d.d_diagnosis_eligible_diab, 0) as d_diagnosis_eligible_diab,
            COALESCE(d.d_diagnosis_eligible_obes, 0) as d_diagnosis_eligible_obes
        FROM events_df e
        LEFT JOIN diagnosis_flags d ON e.ENROLID = d.ENROLID
        """
        
        merged_df = conn.execute(merge_query).fetchdf()
        conn.close()
        
        # Statistics
        total_events = len(merged_df)
        diab_eligible = merged_df['d_diagnosis_eligible_diab'].sum()
        obes_eligible = merged_df['d_diagnosis_eligible_obes'].sum()
        either_eligible = ((merged_df['d_diagnosis_eligible_diab'] == 1) | 
                          (merged_df['d_diagnosis_eligible_obes'] == 1)).sum()
        
        print(f"     Total events: {total_events:,}")
        print(f"     Diabetes eligible: {diab_eligible:,} ({diab_eligible/total_events*100:.1f}%)")
        print(f"     Obesity eligible: {obes_eligible:,} ({obes_eligible/total_events*100:.1f}%)")
        print(f"     Either diagnosis: {either_eligible:,} ({either_eligible/total_events*100:.1f}%)")
        
        # Create output filename
        if '_with_diagnosis' in filename:
            output_file = filename  # Already has diagnosis, overwrite
        else:
            output_file = filename.replace('.parquet', '_with_diagnosis.parquet')
        
        # Save file
        merged_df.to_parquet(output_file, compression='snappy')
        
        output_size_mb = Path(output_file).stat().st_size / (1024*1024)
        print(f"     Saved: {os.path.basename(output_file)} ({output_size_mb:.1f} MB)")
        
        # Clean up
        del events_df, diagnosis_flags_df, merged_df
        import gc
        gc.collect()
        
        return output_file
        
    except Exception as e:
        print(f"     Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def process_year(year, diabetes_codes, obesity_codes):
    """
    Process all prescription events files for a given year
    """
    print(f"\nPROCESSING YEAR {year}")
    print("=" * 50)
    
    # Find prescription events files for this year
    # Look for files with or without NDCNUM
    patterns = [
        f"prescription_events_{year}_*_with_ndcnum.parquet",
        f"prescription_events_{year}_*.parquet"
    ]
    
    events_files = []
    for pattern in patterns:
        files = glob.glob(pattern)
        events_files.extend(files)
    
    # Remove duplicates and files already processed
    events_files = list(set(events_files))
    events_files = [f for f in events_files if '_with_diagnosis' not in f]
    
    if not events_files:
        print(f"  No prescription events files found for {year}")
        return 0
    
    print(f"  Found {len(events_files)} files to process")
    
    processed_files = []
    for events_file in sorted(events_files):
        result = add_diagnosis_flags_to_file(events_file, year, diabetes_codes, obesity_codes)
        if result:
            processed_files.append(result)
    
    print(f"  Year {year} complete: {len(processed_files)} files processed")
    return len(processed_files)

def main():
    """
    Main function
    """
    print("=" * 60)
    print("ADD DIAGNOSIS ELIGIBILITY FLAGS TO PRESCRIPTION EVENTS")
    print("=" * 60)
    
    # Load diagnosis codes
    diabetes_codes, obesity_codes = load_nonzero_diagnosis_codes()
    
    if diabetes_codes is None or obesity_codes is None:
        print("\nERROR: Cannot proceed without diagnosis codes")
        return
    
    print(f"\nProcessing years {START_YEAR} to {END_YEAR}")
    print("=" * 60)
    
    total_start_time = time.time()
    total_files_processed = 0
    
    for year in range(START_YEAR, END_YEAR + 1):
        files_processed = process_year(year, diabetes_codes, obesity_codes)
        total_files_processed += files_processed
    
    total_time = time.time() - total_start_time
    
    print(f"\n" + "=" * 60)
    print("ALL YEARS COMPLETE")
    print("=" * 60)
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Total files processed: {total_files_processed}")
    
    # Show output files
    output_files = glob.glob("prescription_events_*_with_diagnosis.parquet")
    print(f"\nOutput files: {len(output_files)}")
    for file in sorted(output_files)[:10]:
        file_size = Path(file).stat().st_size / (1024*1024)
        print(f"   {os.path.basename(file)} ({file_size:.1f} MB)")
    
    if len(output_files) > 10:
        print(f"   ... and {len(output_files) - 10} more files")

if __name__ == "__main__":
    main()
