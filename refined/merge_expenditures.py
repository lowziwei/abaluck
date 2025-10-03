import pandas as pd
import duckdb
import os
import time
from pathlib import Path

# Configuration
STUDY_START_DATE = '2021-06-01'
STUDY_END_DATE = '2023-06-30'
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"

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

def extract_expenditures_for_patient_dates(patient_dates, chunk_size=200000):
    """
    MEMORY-OPTIMIZED: Extract expenditures in chunks to handle large patient-date lists
    
    Instead of querying with all patient-dates at once (memory-intensive),
    processes in smaller chunks for better memory efficiency.
    
    Parameters:
    -----------
    patient_dates : DataFrame with columns ['ENROLID', 'SVCDATE']
    chunk_size : int - Number of patient-dates to process at a time (default 200k)
    
    Returns:
    --------
    DataFrame with columns: ENROLID, SVCDATE, inpatient_pay, outpatient_pay, drug_pay, total_pay
    """
    print(f"    Extracting expenditures for {len(patient_dates):,} patient-date pairs...")
    
    # If small enough, process in one go
    if len(patient_dates) <= chunk_size:
        print(f"      Processing in single batch (small enough)")
        return _extract_expenditures_chunk(patient_dates)
    
    # Otherwise, process in chunks
    print(f"      Processing in chunks of {chunk_size:,} (memory-optimized)")
    num_chunks = (len(patient_dates) + chunk_size - 1) // chunk_size
    
    all_results = []
    for i in range(0, len(patient_dates), chunk_size):
        chunk_num = i // chunk_size + 1
        chunk = patient_dates.iloc[i:i+chunk_size]
        print(f"      Chunk {chunk_num}/{num_chunks}: {len(chunk):,} patient-dates")
        
        result_chunk = _extract_expenditures_chunk(chunk)
        all_results.append(result_chunk)
        
        # Clean up between chunks
        import gc
        gc.collect()
    
    # Combine all chunks
    print(f"      Combining {len(all_results)} chunks...")
    final_result = pd.concat(all_results, ignore_index=True)
    
    return final_result

def _extract_expenditures_chunk(patient_dates):
    """
    Extract expenditures for a single chunk of patient-dates
    """
    conn = setup_duckdb_connection()
    
    try:
        conn.register('target_patient_dates', patient_dates)
        data_path = f"/data/MarketScan_data/{DATASET_TYPE}"
        
        # Get unique patients to make queries more efficient
        unique_patients = patient_dates['ENROLID'].unique().tolist()
        patients_str = "', '".join(map(str, unique_patients))
        
        # Extract inpatient expenditures (TOTNET)
        inpatient_file = f"{data_path}/{DATABASE}_I.parquet"
        if Path(inpatient_file).exists():
            # Two-stage filter: first by ENROLID (fast), then by exact date pairs
            inpatient_query = f"""
            WITH filtered_data AS (
                SELECT 
                    ENROLID,
                    ADMDATE as SVCDATE,
                    TOTNET
                FROM '{inpatient_file}'
                WHERE ENROLID IN ('{patients_str}')
                AND ADMDATE >= DATE '{STUDY_START_DATE}'
                AND ADMDATE <= DATE '{STUDY_END_DATE}'
            )
            SELECT 
                f.ENROLID,
                f.SVCDATE,
                SUM(COALESCE(f.TOTNET, 0)) as inpatient_pay
            FROM filtered_data f
            INNER JOIN target_patient_dates t 
                ON f.ENROLID = t.ENROLID AND f.SVCDATE = t.SVCDATE
            GROUP BY f.ENROLID, f.SVCDATE
            """
            inpatient_df = conn.execute(inpatient_query).fetchdf()
        else:
            inpatient_df = pd.DataFrame(columns=['ENROLID', 'SVCDATE', 'inpatient_pay'])
        
        # Extract outpatient expenditures (NETPAY)
        outpatient_file = f"{data_path}/{DATABASE}_O.parquet"
        if Path(outpatient_file).exists():
            outpatient_query = f"""
            WITH filtered_data AS (
                SELECT 
                    ENROLID,
                    SVCDATE,
                    NETPAY
                FROM '{outpatient_file}'
                WHERE ENROLID IN ('{patients_str}')
                AND SVCDATE >= DATE '{STUDY_START_DATE}'
                AND SVCDATE <= DATE '{STUDY_END_DATE}'
            )
            SELECT 
                f.ENROLID,
                f.SVCDATE,
                SUM(COALESCE(f.NETPAY, 0)) as outpatient_pay
            FROM filtered_data f
            INNER JOIN target_patient_dates t 
                ON f.ENROLID = t.ENROLID AND f.SVCDATE = t.SVCDATE
            GROUP BY f.ENROLID, f.SVCDATE
            """
            outpatient_df = conn.execute(outpatient_query).fetchdf()
        else:
            outpatient_df = pd.DataFrame(columns=['ENROLID', 'SVCDATE', 'outpatient_pay'])
        
        # Extract drug expenditures (NETPAY)
        drug_file = f"{data_path}/{DATABASE}_D.parquet"
        if Path(drug_file).exists():
            drug_query = f"""
            WITH filtered_data AS (
                SELECT 
                    ENROLID,
                    SVCDATE,
                    NETPAY
                FROM '{drug_file}'
                WHERE ENROLID IN ('{patients_str}')
                AND SVCDATE >= DATE '{STUDY_START_DATE}'
                AND SVCDATE <= DATE '{STUDY_END_DATE}'
            )
            SELECT 
                f.ENROLID,
                f.SVCDATE,
                SUM(COALESCE(f.NETPAY, 0)) as drug_pay
            FROM filtered_data f
            INNER JOIN target_patient_dates t 
                ON f.ENROLID = t.ENROLID AND f.SVCDATE = t.SVCDATE
            GROUP BY f.ENROLID, f.SVCDATE
            """
            drug_df = conn.execute(drug_query).fetchdf()
        else:
            drug_df = pd.DataFrame(columns=['ENROLID', 'SVCDATE', 'drug_pay'])
        
        # Merge expenditures
        conn.register('inpatient_exp', inpatient_df)
        conn.register('outpatient_exp', outpatient_df)
        conn.register('drug_exp', drug_df)
        
        merge_query = """
        SELECT 
            pd.ENROLID,
            pd.SVCDATE,
            COALESCE(i.inpatient_pay, 0) as inpatient_pay,
            COALESCE(o.outpatient_pay, 0) as outpatient_pay,
            COALESCE(d.drug_pay, 0) as drug_pay,
            COALESCE(i.inpatient_pay, 0) + COALESCE(o.outpatient_pay, 0) + COALESCE(d.drug_pay, 0) as total_pay
        FROM target_patient_dates pd
        LEFT JOIN inpatient_exp i ON pd.ENROLID = i.ENROLID AND pd.SVCDATE = i.SVCDATE
        LEFT JOIN outpatient_exp o ON pd.ENROLID = o.ENROLID AND pd.SVCDATE = o.SVCDATE
        LEFT JOIN drug_exp d ON pd.ENROLID = d.ENROLID AND pd.SVCDATE = d.SVCDATE
        """
        
        result_df = conn.execute(merge_query).fetchdf()
        conn.close()
        
        return result_df
        
    except Exception as e:
        print(f"        Error extracting expenditures: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.close()
        result_df = patient_dates.copy()
        result_df['inpatient_pay'] = 0.0
        result_df['outpatient_pay'] = 0.0
        result_df['drug_pay'] = 0.0
        result_df['total_pay'] = 0.0
        return result_df

def process_file(input_file, year):
    """
    Process a single aggregated diagnosis file:
    1. Filter to study period (June 2021 - June 2023)
    2. Add expenditure information
    3. Save with new filename
    """
    print(f"\n{'='*70}")
    print(f"PROCESSING: {input_file}")
    print(f"{'='*70}")
    
    # Load file
    print(f"  Step 1: Loading file...")
    df = pd.read_parquet(input_file)
    
    # Detect date column
    date_col = None
    for col in ['SVCDATE', 'svcdate', 'date', 'DATE', 'service_date', 'SVCDT']:
        if col in df.columns:
            date_col = col
            break
    
    if not date_col:
        print(f"  ERROR: No date column found!")
        print(f"  Available columns: {list(df.columns)}")
        return None
    
    print(f"  Date column: {date_col}")
    print(f"  Original events: {len(df):,}")
    
    # Filter to study period
    print(f"  Step 2: Filtering to study period ({STUDY_START_DATE} to {STUDY_END_DATE})...")
    original_count = len(df)
    df_filtered = df[
        (df[date_col] >= pd.to_datetime(STUDY_START_DATE)) &
        (df[date_col] <= pd.to_datetime(STUDY_END_DATE))
    ].copy()
    filtered_count = len(df_filtered)
    
    print(f"  Events after filter: {filtered_count:,} ({filtered_count/original_count*100:.1f}% of original)")
    
    if filtered_count == 0:
        print(f"  No events in study period!")
        return None
    
    # Get unique patient-dates
    print(f"  Step 3: Extracting expenditures...")
    patient_dates = df_filtered[['ENROLID', date_col]].drop_duplicates()
    patient_dates = patient_dates.rename(columns={date_col: 'SVCDATE'})
    print(f"    Unique patient-dates: {len(patient_dates):,}")
    
    expenditures_df = extract_expenditures_for_patient_dates(patient_dates)
    
    # Merge
    print(f"  Step 4: Merging expenditures with prescription data...")
    conn = setup_duckdb_connection()
    conn.register('events_df', df_filtered)
    conn.register('expenditures', expenditures_df)
    
    merge_query = f"""
    SELECT 
        e.*,
        COALESCE(x.inpatient_pay, 0) as inpatient_pay,
        COALESCE(x.outpatient_pay, 0) as outpatient_pay,
        COALESCE(x.drug_pay, 0) as drug_pay,
        COALESCE(x.total_pay, 0) as total_pay
    FROM events_df e
    LEFT JOIN expenditures x ON e.ENROLID = x.ENROLID AND e.{date_col} = x.SVCDATE
    """
    
    merged_df = conn.execute(merge_query).fetchdf()
    conn.close()
    
    # Statistics
    print(f"\n  Results:")
    print(f"    Total events: {len(merged_df):,}")
    print(f"    Total expenditure: ${merged_df['total_pay'].sum():,.2f}")
    print(f"    Average expenditure per event: ${merged_df['total_pay'].mean():,.2f}")
    print(f"    Events with expenditure > $0: {(merged_df['total_pay'] > 0).sum():,} ({(merged_df['total_pay'] > 0).sum()/len(merged_df)*100:.1f}%)")
    
    exp_breakdown = {
        'Inpatient': merged_df['inpatient_pay'].sum(),
        'Outpatient': merged_df['outpatient_pay'].sum(),
        'Drug': merged_df['drug_pay'].sum()
    }
    print(f"\n  Expenditure breakdown:")
    for source, amount in exp_breakdown.items():
        pct = (amount / merged_df['total_pay'].sum() * 100) if merged_df['total_pay'].sum() > 0 else 0
        print(f"    {source}: ${amount:,.2f} ({pct:.1f}%)")
    
    # Save
    output_file = f"prescription_events_{year}_study_period_with_expenditure.parquet"
    print(f"\n  Step 5: Saving to {output_file}...")
    merged_df.to_parquet(output_file, compression='snappy')
    
    output_size_mb = Path(output_file).stat().st_size / (1024*1024)
    print(f"  Saved: {output_size_mb:.1f} MB")
    
    # Cleanup
    del df, df_filtered, patient_dates, expenditures_df, merged_df
    import gc
    gc.collect()
    
    return output_file

def main():
    """
    Main function: Add expenditures to aggregated diagnosis files for 2021-2023
    """
    print("=" * 70)
    print("ADD EXPENDITURES TO AGGREGATED DIAGNOSIS FILES")
    print("STUDY PERIOD: JUNE 2021 - JUNE 2023")
    print("=" * 70)
    
    # Find the 3 aggregated files
    years = [2021, 2022, 2023]
    input_files = {}
    
    for year in years:
        # Look for the actual file pattern
        pattern = f"prescription_events_{year}_with_ndcnum_with_diagnosis.parquet"
        
        if Path(pattern).exists():
            input_files[year] = pattern
        else:
            print(f"\nWARNING: Could not find aggregated file for {year}")
            print(f"  Looking for: {pattern}")
    
    if not input_files:
        print(f"\nERROR: No aggregated diagnosis files found!")
        print(f"Looking for files like: prescription_events_YYYY_with_diagnosis.parquet")
        return
    
    print(f"\nFound {len(input_files)} aggregated files:")
    for year, filepath in sorted(input_files.items()):
        file_size = Path(filepath).stat().st_size / (1024*1024)
        print(f"  {year}: {filepath} ({file_size:.1f} MB)")
    
    print(f"\nStudy period: {STUDY_START_DATE} to {STUDY_END_DATE}")
    print("\nExpenditures will be added from:")
    print(f"  - Inpatient: TOTNET from CCAE_I.parquet")
    print(f"  - Outpatient: NETPAY from CCAE_O.parquet")
    print(f"  - Drug: NETPAY from CCAE_D.parquet")
    print("=" * 70)
    
    # Process each file
    total_start_time = time.time()
    processed_files = []
    
    for year in sorted(input_files.keys()):
        file_start = time.time()
        result = process_file(input_files[year], year)
        if result:
            processed_files.append(result)
        file_time = time.time() - file_start
        print(f"  Processing time: {file_time/60:.1f} minutes")
    
    total_time = time.time() - total_start_time
    
    # Final summary
    print(f"\n{'='*70}")
    print("PROCESSING COMPLETE")
    print(f"{'='*70}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Files processed: {len(processed_files)}/{len(input_files)}")
    
    if processed_files:
        print(f"\nOutput files created:")
        total_events = 0
        total_expenditure = 0.0
        
        for file in processed_files:
            file_size = Path(file).stat().st_size / (1024*1024)
            df = pd.read_parquet(file, columns=['total_pay'])
            total_events += len(df)
            total_expenditure += df['total_pay'].sum()
            print(f"  {file}")
            print(f"    Size: {file_size:.1f} MB")
            print(f"    Events: {len(df):,}")
            print(f"    Total expenditure: ${df['total_pay'].sum():,.2f}")
        
        print(f"\nOverall totals across all files:")
        print(f"  Total events: {total_events:,}")
        print(f"  Total expenditure: ${total_expenditure:,.2f}")
        print(f"  Average per event: ${total_expenditure/total_events:,.2f}")

if __name__ == "__main__":
    main()
