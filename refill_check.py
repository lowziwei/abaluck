import duckdb
import pandas as pd
import time
import gc
import glob
import os
from pathlib import Path

# Configuration
START_YEAR = 2014  # Start from 2014 for historical analysis
PROCESS_START_YEAR = 2018  
END_YEAR = 2024
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
TABLE_CODE = "D"
PATIENT_CHUNK_SIZE = 100000  # Match your 100k patients per chunk
SAVE_EVERY = 15  # Save intermediate results every 15 chunks (match your approach)

def create_prescription_flags_simple_chunks(target_year):
    """
    1. Chunk all target_year data by ENROLID (no pre-filtering)
    2. Within each chunk, filter for REFILL=0 and apply flagging using CASE statements
    3. Save results every 15 chunks to parquet files
    """

    conn = duckdb.connect()
    conn.execute("SET memory_limit='28GB'")
    conn.execute("SET threads=4")

    # File paths
    data_path = f"/data/MarketScan_data/{DATASET_TYPE}"
    target_file = f"{data_path}/{DATABASE}_{TABLE_CODE}_{target_year}.parquet"

    if not Path(target_file).exists():
        print(f"ERROR: Target file does not exist: {target_file}")
        return None

    # Check which previous year files exist
    previous_years = []
    for year in range(target_year - 1, START_YEAR - 1, -1):  # Go backwards from target_year-1 to 2014
        file_path = f"{data_path}/{DATABASE}_{TABLE_CODE}_{year}.parquet"
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

    # Get data overview
    print("Getting data overview...")
    overview = conn.execute(f"""
        SELECT 
            MIN(ENROLID) as min_enrolid, 
            MAX(ENROLID) as max_enrolid, 
            COUNT(DISTINCT ENROLID) as unique_patients,
            COUNT(*) as total_records,
            SUM(CASE WHEN REFILL = 0 THEN 1 ELSE 0 END) as refill_zero_count
        FROM '{target_file}'
    """).fetchone()

    min_enrolid, max_enrolid, unique_patients, total_records, refill_zero_count = overview
    print(f"{target_year} Data Overview:")
print(f"  Total records: {total_records:,}")
    print(f"  REFILL=0 records: {refill_zero_count:,}")
    print(f"  Unique patients: {unique_patients:,}")
    print(f"  ENROLID range: {min_enrolid} to {max_enrolid}")

    print(f"Using chunk size: {PATIENT_CHUNK_SIZE:,} patients per chunk")

    # Get ALL unique ENROLIDs for chunking
    print("Getting ALL unique ENROLIDs for chunking...")
    step1_start = time.time()

    enrolid_query = f"""
        SELECT DISTINCT ENROLID
        FROM '{target_file}'
        WHERE ENROLID IS NOT NULL
        ORDER BY ENROLID
    """

    enrolid_df = conn.execute(enrolid_query).df()
    total_patients = len(enrolid_df)

    # Create chunks of exact size
    chunks = [enrolid_df.iloc[i:i + PATIENT_CHUNK_SIZE]['ENROLID'].tolist()
             for i in range(0, total_patients, PATIENT_CHUNK_SIZE)]

    step1_time = time.time() - step1_start
    print(f"  Total patients: {total_patients:,}")
    print(f"  Total chunks: {len(chunks)} (size: {PATIENT_CHUNK_SIZE:,} each)")
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

        # Check within same year - flag if REFILL=0 is NOT the first chronological instance
        case_conditions.append(f"""
            WHEN EXISTS (
                SELECT 1 FROM '{target_file}' same_year
                WHERE same_year.ENROLID = r.ENROLID 
                AND same_year.NDCNUM = r.NDCNUM
                AND same_year.SVCDATE < r.SVCDATE
            ) THEN 0
        """)

        # Check previous years
        for year in sorted(previous_years, reverse=True):
            years_back = target_year - year
            file_path = f"{data_path}/{DATABASE}_{TABLE_CODE}_{year}.parquet"

            case_conditions.append(f"""
WHEN EXISTS (
                SELECT 1 FROM '{target_file}' same_year
                WHERE same_year.ENROLID = r.ENROLID 
                AND same_year.NDCNUM = r.NDCNUM
                AND same_year.SVCDATE < r.SVCDATE
            ) THEN 0
        """)

        # Check previous years
        for year in sorted(previous_years, reverse=True):
            years_back = target_year - year
            file_path = f"{data_path}/{DATABASE}_{TABLE_CODE}_{year}.parquet"

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
            FROM '{target_file}'
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
        if chunks_processed % SAVE_EVERY == 0 or i == total_chunks - 1:
            if all_chunk_results:
                print(f"  Saving results from {len(all_chunk_results)} chunks to file...")

                # Combine chunks and save
                combined_df = pd.concat(all_chunk_results, ignore_index=True)
                output_file = f"prescription_flags_{target_year}_{DATABASE}_{TABLE_CODE}_part{file_counter:02d}.parquet"
                combined_df.to_parquet(output_file)

                print(f"  Saved {len(combined_df):,} records to {output_file}")
# Reset for next batch
                all_chunk_results = []
                file_counter += 1

        # Clean up
        del enrolid_chunk
        gc.collect()

    print(f"\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    print(f"Total chunks processed: {chunks_processed}")
    print(f"Total flagged records: {total_flagged_records:,}")
    print(f"Files created: {file_counter - 1}")
    print(f"File pattern: prescription_flags_{target_year}_{DATABASE}_{TABLE_CODE}_part##.parquet")

    # Ask about combining files
    combine_files = input(f"\nCombine all part files into one final file for {target_year}? (y/n): ").lower().strip()

    if combine_files == 'y':
        print("Combining all part files...")
        all_parts = []

        for part_num in range(1, file_counter):
            part_file = f"prescription_flags_{target_year}_{DATABASE}_{TABLE_CODE}_part{part_num:02d}.parquet"
            if Path(part_file).exists():
                part_df = pd.read_parquet(part_file)
                all_parts.append(part_df)
                print(f"  Loaded {part_file}: {len(part_df):,} records")

        if all_parts:
            final_df = pd.concat(all_parts, ignore_index=True)
            final_file = f"prescription_flags_{target_year}_{DATABASE}_{TABLE_CODE}_FINAL.parquet"
            final_df.to_parquet(final_file)
            print(f"Final combined file: {final_file} ({len(final_df):,} records)")

            # Show final summary
            print(f"\nFinal Summary for {target_year}:")
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
                        check_year = target_year - years_back
                        print(f"  FLAG {int(flag):2d} (found in {check_year}): {count:6,} ({count/flagged_count*100:.1f}%)")

            return final_df

    # Close connection
    conn.close()
    return None

def process_all_pharmacy_years():
"""
    Process pharmacy files for years 2018-2024 
    """
    print("PHARMACY PRESCRIPTION FLAG ANALYSIS - YEARS 2018-2024")
    print("=" * 80)
    print(f"Processing years: {PROCESS_START_YEAR} to {END_YEAR}")
    print(f"Dataset: {DATASET_TYPE}")
    print(f"Database: {DATABASE}")
    print(f"Table: {TABLE_CODE}")
    print(f"Patient chunk size: {PATIENT_CHUNK_SIZE:,}")
    print("=" * 80)

    # Process each year from 2018 to 2024
    for target_year in range(PROCESS_START_YEAR, END_YEAR + 1):
        print(f"\n🎯 PROCESSING PHARMACY YEAR: {target_year}")
        print("=" * 60)

        # Process D file (pharmacy) for this target year
        d_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_{target_year}.parquet"
        if Path(d_file).exists():
            print(f"\n💊 Processing PHARMACY data for {target_year}")
            results = create_prescription_flags_simple_chunks(target_year)
            if results is not None:
                print(f"✅ Completed {target_year}")
            else:
                print(f"⚠️ {target_year} processed but no final combined file created")
        else:
            print(f"⚠️  PHARMACY FILE NOT FOUND: {d_file}")

    print(f"\n{'='*80}")
    print("🎉 PHARMACY PRESCRIPTION FLAG ANALYSIS COMPLETE!")
    print(f"{'='*80}")
    print("Output files created for each year:")
    print("  - prescription_flags_YYYY_*_part##.parquet: Intermediate files")
    print("  - prescription_flags_YYYY_*_FINAL.parquet: Combined files (if you chose to combine)")
    print("\nFlag meanings:")
    print("  - FLAG = NULL: Truly new prescriptions (no prior history)")
    print("  - FLAG = 0: Not first chronological instance in same year")
    print("  - FLAG = -1: Found in previous year (year-1)")
    print("  - FLAG = -2: Found 2 years back (year-2)")
    print("  - etc.")

def main():
    """
    Main function matching your 2018 approach exactly
    """
    print("MarketScan Pharmacy Data - PRESCRIPTION FLAG ANALYSIS")
    print("=" * 80)
    print(f"Processing years: {PROCESS_START_YEAR} to {END_YEAR}")
    print("Method: Exactly matching your 2018 chunked approach")
    print("")
    print("Approach:")
    print("  1. Chunk all patients from target year (100k per chunk)")
    print("  2. For each chunk, get REFILL=0 records")
    print("  3. Use CASE statements to flag based on prior history")
    print("  4. Save every 15 chunks to parquet files")
    print("  5. Optionally combine into final file")
    print("")
    print("Flag Logic:")
    print("  - Check same year first (chronological order)")
    print("  - Then check previous years back to 2014")
    print("  - NULL flags = truly new prescriptions")
    print("")
    print(f"Memory constraint: 30GB (using 28GB with buffer)")
    print(f"Patient chunk size: {PATIENT_CHUNK_SIZE:,}")
    print("=" * 80)

    # Run pharmacy analysis
    process_all_pharmacy_years()
print("\n🎉 Analysis complete!")
    print("Results are in prescription_flags_YYYY_*_part##.parquet files")
    print("Use the FINAL files for your analysis if you combined them")

if __name__ == "__main__":
    main():q
