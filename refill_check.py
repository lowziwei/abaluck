import duckdb
import pandas as pd
from pathlib import Path

def create_2018_prescription_flags_simple_chunks(dataset_type="COMMERCIAL_SET_A", database="CCAE", table_code="D"):
    """
    Simplified chunking approach:
    1. Chunk all 2018 data by ENROLID (no pre-filtering)
    2. Within each chunk, filter for REFILL=0 and apply flagging
    3. Save results every 15 chunks to parquet files
    """
    
    conn = duckdb.connect()
    
    # File paths
    data_path = f"/data/MarketScan_data/{dataset_type}"
    file_2018 = f"{data_path}/{database}_{table_code}_2018.parquet"
    
    # Check which previous year files exist
    previous_years = []
    for year in [2017, 2016, 2015, 2014]:
        file_path = f"{data_path}/{database}_{table_code}_{year}.parquet"
        try:
            conn.execute(f"SELECT COUNT(*) FROM '{file_path}' LIMIT 1")
            previous_years.append(year)
            print(f"Found data for year {year}")
        except:
            print(f"No data found for year {year}")
    
    if not previous_years:
        print("No previous year data found!")
        return pd.DataFrame()
    
    print(f"Will check against years: {previous_years}")
    
    # Get ENROLID range and total counts
    print("Getting data overview...")
    overview = conn.execute(f"""
        SELECT 
            MIN(ENROLID) as min_enrolid, 
            MAX(ENROLID) as max_enrolid, 
            COUNT(DISTINCT ENROLID) as unique_patients,
            COUNT(*) as total_records,
            SUM(CASE WHEN REFILL = 0 THEN 1 ELSE 0 END) as refill_zero_count
        FROM '{file_2018}'
    """).fetchone()
    
    min_enrolid, max_enrolid, unique_patients, total_records, refill_zero_count = overview
    print(f"2018 Data Overview:")
    print(f"  Total records: {total_records:,}")
    print(f"  REFILL=0 records: {refill_zero_count:,}")
    print(f"  Unique patients: {unique_patients:,}")
    print(f"  ENROLID range: {min_enrolid} to {max_enrolid}")
    
    # Calculate standard chunk size 
    CHUNK_SIZE = 100000  # Standard 100k patients per chunk
    print(f"Using standard chunk size: {CHUNK_SIZE:,} patients per chunk")
    
    # Get ALL unique ENROLIDs for chunking (much simpler approach)
    print("Getting ALL unique ENROLIDs for chunking...")
    import time
    import gc
    
    step1_start = time.time()
    
    enrolid_query = f"""
        SELECT DISTINCT ENROLID
        FROM '{file_2018}'
        WHERE ENROLID IS NOT NULL
        ORDER BY ENROLID
    """
    
    enrolid_df = conn.execute(enrolid_query).df()
    total_patients = len(enrolid_df)
    
    # Create chunks of exact size
    chunks = [enrolid_df.iloc[i:i + CHUNK_SIZE]['ENROLID'].tolist() 
             for i in range(0, total_patients, CHUNK_SIZE)]
    
    step1_time = time.time() - step1_start
    print(f"  Total patients: {total_patients:,}")
    print(f"  Total chunks: {len(chunks)} (size: {CHUNK_SIZE:,} each)")
    print(f"  Step 1 completed in {step1_time:.1f} seconds")
    
    # Clean up memory
    del enrolid_df
    gc.collect()
    
    total_chunks = len(chunks)
    
    # Initialize tracking variables
    all_chunk_results = []
    file_counter = 1
    chunks_processed = 0
    total_flagged_records = 0
    
    # Process each chunk
    for i, enrolid_chunk in enumerate(chunks):
        print(f"Processing chunk {i+1}/{total_chunks}: {len(enrolid_chunk)} patients...")
        
        # Convert ENROLID list to SQL IN clause format
        enrolid_list_str = ','.join(map(str, enrolid_chunk))
        
        # Build case conditions for flagging
        case_conditions = []
        
        # Check within same year (2018) - flag if REFILL=0 is NOT the first chronological instance
        case_conditions.append(f"""
            WHEN EXISTS (
                SELECT 1 FROM '{file_2018}' same_year
                WHERE same_year.ENROLID = r.ENROLID 
                AND same_year.NDCNUM = r.NDCNUM
                AND same_year.SVCDATE < r.SVCDATE
            ) THEN 0
        """)
        
        # Check previous years
        for year in sorted(previous_years, reverse=True):
            years_back = 2018 - year
            file_path = f"{data_path}/{database}_{table_code}_{year}.parquet"
            
            case_conditions.append(f"""
                WHEN EXISTS (
                    SELECT 1 FROM '{file_path}' p{year}
                    WHERE p{year}.ENROLID = r.ENROLID 
                    AND p{year}.NDCNUM = r.NDCNUM
                ) THEN -{years_back}
            """)
        
        # Query for this chunk: get all data, filter for REFILL=0, apply flagging
        chunk_query = f"""
        WITH chunk_data AS (
            SELECT ENROLID, NDCNUM, REFILL, YEAR, SVCDATE
            FROM '{file_2018}'
            WHERE ENROLID IN ({enrolid_list_str})
        ),
        refill_zero_chunk AS (
            SELECT * FROM chunk_data WHERE REFILL = 0
        )
        SELECT r.*,
               CASE 
                   {''.join(case_conditions)}
                   ELSE NULL
               END as FLAG
        FROM refill_zero_chunk r
        ORDER BY ENROLID, NDCNUM, SVCDATE
        """
        
        # Execute chunk query
        chunk_result = conn.execute(chunk_query).df()
        
        if len(chunk_result) > 0:
            all_chunk_results.append(chunk_result)
            
            # Track progress
            chunk_flagged = chunk_result['FLAG'].notna().sum()
            total_flagged_records += chunk_flagged
            unique_patients_chunk = chunk_result['ENROLID'].nunique()
            
            print(f"  Processed: {len(chunk_result):,} REFILL=0 records, {unique_patients_chunk:,} patients, {chunk_flagged:,} flagged ({chunk_flagged/len(chunk_result)*100:.1f}%)")
        
        chunks_processed += 1
        
        # Save every 15 chunks or at the end
        if chunks_processed % 15 == 0 or i == total_chunks - 1:
            if all_chunk_results:
                print(f"  Saving results from {len(all_chunk_results)} chunks to file...")
                
                # Combine chunks and save
                combined_df = pd.concat(all_chunk_results, ignore_index=True)
                output_file = f"prescription_flags_2018_{DATABASE}_{TABLE_CODE}_part{file_counter:02d}.parquet"
                combined_df.to_parquet(output_file)
                
                print(f"  Saved {len(combined_df):,} records to {output_file}")
                
                # Reset for next batch
                all_chunk_results = []
                file_counter += 1
    
    print(f"\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    print(f"Total chunks processed: {chunks_processed}")
    print(f"Total flagged records: {total_flagged_records:,}")
    print(f"Files created: {file_counter - 1}")
    print(f"File pattern: prescription_flags_2018_{DATABASE}_{TABLE_CODE}_part##.parquet")
    
    # Optionally combine all files into one final file
    combine_files = input("\nCombine all part files into one final file? (y/n): ").lower().strip()
    
    if combine_files == 'y':
        print("Combining all part files...")
        all_parts = []
        
        for part_num in range(1, file_counter):
            part_file = f"prescription_flags_2018_{DATABASE}_{TABLE_CODE}_part{part_num:02d}.parquet"
            if Path(part_file).exists():
                part_df = pd.read_parquet(part_file)
                all_parts.append(part_df)
                print(f"  Loaded {part_file}: {len(part_df):,} records")
        
        if all_parts:
            final_df = pd.concat(all_parts, ignore_index=True)
            final_file = f"prescription_flags_2018_{DATABASE}_{TABLE_CODE}_FINAL.parquet"
            final_df.to_parquet(final_file)
            print(f"Final combined file: {final_file} ({len(final_df):,} records)")
            
            # Show final summary
            print(f"\nFinal Summary:")
            flagged_count = final_df['FLAG'].notna().sum()
            print(f"Total REFILL=0 prescriptions: {len(final_df):,}")
            print(f"Total flagged: {flagged_count:,} ({flagged_count/len(final_df)*100:.1f}%)")
            
            if flagged_count > 0:
                flag_dist = final_df['FLAG'].value_counts().sort_index()
                print(f"Flag distribution:")
                for flag, count in flag_dist.items():
                    if flag == 0:
                        print(f"  FLAG  0 (not first chronological): {count:6,} ({count/flagged_count*100:.1f}%)")
                    else:
                        years_back = abs(int(flag))
                        check_year = 2018 - years_back
                        print(f"  FLAG {int(flag):2d} (found in {check_year}): {count:6,} ({count/flagged_count*100:.1f}%)")
            
            return final_df
    
    return None

def run_simple_chunked_analysis():
    """
    Run the simplified chunked analysis
    """
    
    DATASET_TYPE = "COMMERCIAL_SET_A"
    DATABASE = "CCAE" 
    TABLE_CODE = "D"
    
    print("="*60)
    print("2018 PRESCRIPTION FLAG ANALYSIS - SIMPLE CHUNKED")
    print("="*60)
    print(f"Dataset: {DATASET_TYPE}")
    print(f"Database: {DATABASE}")
    print(f"Table: {TABLE_CODE}")
    print()
    
    try:
        # Make DATABASE and TABLE_CODE available globally for the output file names
        globals()['DATABASE'] = DATABASE
        globals()['TABLE_CODE'] = TABLE_CODE
        
        results = create_2018_prescription_flags_simple_chunks(DATASET_TYPE, DATABASE, TABLE_CODE)
        
        if results is not None:
            print(f"\nAnalysis completed successfully!")
            print(f"Sample of final results:")
            print(results[['ENROLID', 'NDCNUM', 'YEAR', 'SVCDATE', 'REFILL', 'FLAG']].head(10))
        
        return results
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    results = run_simple_chunked_analysis()
