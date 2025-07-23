import duckdb
import pandas as pd
import time

# =============================================================================
# CONFIGURATION
# =============================================================================
START_YEAR = 2014
END_YEAR = 2024
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
CHUNK_SIZE = 100000  # 100k patients per chunk

def process_year_steps_1_and_2(year):
    """Execute Steps 1 and 2 only for one year with chunking"""
    print(f"\n{'='*60}")
    print(f"PROCESSING YEAR {year} - STEPS 1 AND 2 ONLY")
    print(f"{'='*60}")
    
    # File paths
    d_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_D_{year}.parquet"
    o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"
    
    total_start_time = time.time()
    
    # =============================================================================
    # SETUP: Get chunking parameters
    # =============================================================================
    print("Setup: Getting chunking parameters...")
    setup_start = time.time()
    
    # Get ENROLID ranges for chunking
    enrolid_info = conn.execute(f"""
        SELECT 
            MIN(ENROLID) as min_enrolid,
            MAX(ENROLID) as max_enrolid,
            COUNT(DISTINCT ENROLID) as unique_patients
        FROM '{d_file}'
    """).fetchone()
    
    min_enrolid, max_enrolid, total_patients = enrolid_info
    enrolid_range = max_enrolid - min_enrolid
    chunk_step = enrolid_range // (total_patients // CHUNK_SIZE + 1)
    num_chunks = (enrolid_range // chunk_step) + 1
    
    print(f"  Patients: {total_patients:,}, Chunks: {num_chunks}")
    print(f"  Setup completed in {time.time() - setup_start:.2f} seconds")
    
    # =============================================================================
    # STEP 1: Process Prescription File (CHUNKED)
    # =============================================================================
    print("\nSTEP 1: Processing prescription file...")
    step1_start = time.time()
    
    # Initialize expanded prescriptions table
    conn.execute("""
        CREATE OR REPLACE TABLE expanded_prescriptions_all AS
        SELECT 
            CAST(NULL AS BIGINT) as ENROLID,
            CAST(NULL AS DATE) as original_svcdate,
            CAST(NULL AS DATE) as svcdate_band
        WHERE FALSE
    """)
    
    chunk_start_enrolid = min_enrolid
    chunk_num = 0
    
    print("  Step 1.1: Keep only ENROLID, SVCDATE columns and first row per combination")
    print("  Step 1.2: Create SVCDATE_band variable with +/- 3 days (7 rows per original)")
    
    while chunk_start_enrolid <= max_enrolid:
        chunk_end_enrolid = min(chunk_start_enrolid + chunk_step, max_enrolid)
        chunk_num += 1
        
        print(f"    Chunk {chunk_num}/{num_chunks}: ENROLIDs {chunk_start_enrolid:,} to {chunk_end_enrolid:,}")
        
        try:
            # Step 1.1: Keep only ENROLID, SVCDATE columns and get first row per combination
            conn.execute(f"""
                CREATE OR REPLACE TABLE chunk_unique_dates AS
                SELECT DISTINCT ENROLID, SVCDATE
                FROM (
                    SELECT ENROLID, SVCDATE 
                    FROM '{d_file}'
                    WHERE ENROLID BETWEEN {chunk_start_enrolid} AND {chunk_end_enrolid}
                )
            """)
            
            chunk_dates = conn.execute("SELECT COUNT(*) FROM chunk_unique_dates").fetchone()[0]
            
            # Step 1.2: Create SVCDATE_band variable (+/- 3 days)
            conn.execute(f"""
                CREATE OR REPLACE TABLE chunk_expanded AS
                SELECT 
                    ENROLID,
                    SVCDATE as original_svcdate,
                    (SVCDATE + INTERVAL (day_offset) DAY)::DATE as svcdate_band
                FROM chunk_unique_dates
                CROSS JOIN (VALUES (-3), (-2), (-1), (0), (1), (2), (3)) t(day_offset)
            """)
            
            expanded_count = conn.execute("SELECT COUNT(*) FROM chunk_expanded").fetchone()[0]
            print(f"      {chunk_dates:,} unique dates → {expanded_count:,} expanded rows (7x)")
            
            # Add to combined results
            conn.execute("""
                INSERT INTO expanded_prescriptions_all 
                SELECT * FROM chunk_expanded
            """)
            
            # Clean up chunk tables
            conn.execute("DROP TABLE chunk_unique_dates")
            conn.execute("DROP TABLE chunk_expanded")
            
        except Exception as e:
            print(f"      ERROR in chunk {chunk_num}: {e}")
            break
            
        chunk_start_enrolid = chunk_end_enrolid + 1
        
        if chunk_num % 20 == 0:  # Progress update every 20 chunks
            elapsed = time.time() - step1_start
            print(f"      Progress: {chunk_num}/{num_chunks} chunks ({elapsed:.1f}s elapsed)")
            time.sleep(1)
    
    total_expanded = conn.execute("SELECT COUNT(*) FROM expanded_prescriptions_all").fetchone()[0]
    print(f"  Step 1 completed: {total_expanded:,} expanded rows in {time.time() - step1_start:.2f} seconds")
    
    # =============================================================================
    # STEP 2: Process Physician File and Merge (CHUNKED)
    # =============================================================================
    print("\nSTEP 2: Processing physician file and merging...")
    step2_start = time.time()
    
    # Initialize NPI counts table
    conn.execute("""
        CREATE OR REPLACE TABLE npi_counts_all AS
        SELECT 
            CAST(NULL AS BIGINT) as ENROLID,
            CAST(NULL AS DATE) as original_svcdate,
            CAST(NULL AS INTEGER) as unique_npis
        WHERE FALSE
    """)
    
    chunk_start_enrolid = min_enrolid
    chunk_num = 0
    
    print("  Step 2.1: Keep only ENROLID, SVCDATE, NPI columns and unique combinations")
    print("  Step 2.2: Merge physician file into prescription file")
    print("  Step 2.3: Collapse and count unique NPIs per ENROLID, SVCDATE")
    
    while chunk_start_enrolid <= max_enrolid:
        chunk_end_enrolid = min(chunk_start_enrolid + chunk_step, max_enrolid)
        chunk_num += 1
        
        print(f"    Chunk {chunk_num}/{num_chunks}: Processing physician data...")
        
        try:
            # Step 2.1: Keep only ENROLID, SVCDATE, NPI columns and get unique combinations
            conn.execute(f"""
                CREATE OR REPLACE TABLE chunk_unique_visits AS
                SELECT DISTINCT ENROLID, SVCDATE, NPI
                FROM (
                    SELECT ENROLID, SVCDATE, NPI
                    FROM '{o_file}'
                    WHERE ENROLID BETWEEN {chunk_start_enrolid} AND {chunk_end_enrolid}
                )
            """)
            
            chunk_visits = conn.execute("SELECT COUNT(*) FROM chunk_unique_visits").fetchone()[0]
            
            # Get expanded prescriptions for this chunk
            conn.execute(f"""
                CREATE OR REPLACE TABLE chunk_expanded_prescriptions AS
                SELECT * FROM expanded_prescriptions_all
                WHERE ENROLID BETWEEN {chunk_start_enrolid} AND {chunk_end_enrolid}
            """)
            
            chunk_prescriptions = conn.execute("SELECT COUNT(*) FROM chunk_expanded_prescriptions").fetchone()[0]
            
            # Step 2.2: Merge physician file into prescription file
            conn.execute(f"""
                CREATE OR REPLACE TABLE chunk_merged AS
                SELECT 
                    p.ENROLID,
                    p.original_svcdate,
                    p.svcdate_band,
                    o.NPI
                FROM chunk_expanded_prescriptions p
                LEFT JOIN chunk_unique_visits o
                ON p.ENROLID = o.ENROLID AND p.svcdate_band = o.SVCDATE
            """)
            
            # Step 2.3: Collapse and count unique NPIs per ENROLID, original_svcdate
            conn.execute(f"""
                CREATE OR REPLACE TABLE chunk_npi_counts AS
                SELECT 
                    ENROLID,
                    original_svcdate,
                    COUNT(DISTINCT NPI) as unique_npis
                FROM chunk_merged
                WHERE NPI IS NOT NULL
                GROUP BY ENROLID, original_svcdate
            """)
            
            chunk_count = conn.execute("SELECT COUNT(*) FROM chunk_npi_counts").fetchone()[0]
            print(f"      {chunk_visits:,} visits + {chunk_prescriptions:,} prescriptions → {chunk_count:,} NPI counts")
            
            # Add to combined results
            conn.execute("""
                INSERT INTO npi_counts_all 
                SELECT * FROM chunk_npi_counts
            """)
            
            # Clean up chunk tables
            conn.execute("DROP TABLE chunk_unique_visits")
            conn.execute("DROP TABLE chunk_expanded_prescriptions")
            conn.execute("DROP TABLE chunk_merged")
            conn.execute("DROP TABLE chunk_npi_counts")
            
        except Exception as e:
            print(f"      ERROR in chunk {chunk_num}: {e}")
            break
            
        chunk_start_enrolid = chunk_end_enrolid + 1
        
        if chunk_num % 20 == 0:
            elapsed = time.time() - step2_start
            print(f"      Progress: {chunk_num}/{num_chunks} chunks ({elapsed:.1f}s elapsed)")
            time.sleep(1)
    
    total_npi_counts = conn.execute("SELECT COUNT(*) FROM npi_counts_all").fetchone()[0]
    print(f"  Step 2 completed: {total_npi_counts:,} patient-dates with NPI counts in {time.time() - step2_start:.2f} seconds")
    
    # =============================================================================
    # STEP 2.4: Make Histogram
    # =============================================================================
    print("\nSTEP 2.4: Creating NPI count histogram...")
    histogram_data = conn.execute("""
        SELECT 
            unique_npis,
            COUNT(*) as patient_date_count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
        FROM npi_counts_all
        GROUP BY unique_npis
        ORDER BY unique_npis
    """).df()
    
    print("NPI Count Histogram:")
    print(histogram_data.to_string(index=False))
    
    # Save histogram
    histogram_data.to_csv(f"/home/zl749/npi_histogram_{year}.csv", index=False)
    print(f"  Histogram saved to: /home/zl749/npi_histogram_{year}.csv")
    
    # Step 2.5: Subset to keep cases with 1 unique NPI
    conn.execute("""
        CREATE OR REPLACE TABLE unique_npi_cases AS
        SELECT ENROLID, original_svcdate
        FROM npi_counts_all
        WHERE unique_npis = 1
    """)
    
    unique_npi_count = conn.execute("SELECT COUNT(*) FROM unique_npi_cases").fetchone()[0]
    print(f"  Cases with exactly 1 NPI: {unique_npi_count:,} ({unique_npi_count/total_npi_counts*100:.1f}%)")
    
    # Save the unique NPI cases for later use
    unique_cases = conn.execute("SELECT * FROM unique_npi_cases").df()
    unique_cases.to_csv(f"/home/zl749/unique_npi_cases_{year}.csv", index=False)
    print(f"  Unique NPI cases saved to: /home/zl749/unique_npi_cases_{year}.csv")
    
    # Clean up large intermediate tables
    conn.execute("DROP TABLE expanded_prescriptions_all")
    conn.execute("DROP TABLE npi_counts_all")
    conn.execute("DROP TABLE unique_npi_cases")
    
    total_time = time.time() - total_start_time
    print(f"\nYear {year} Steps 1-2 completed in {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
    print(f"Files created:")
    print(f"  - /home/zl749/npi_histogram_{year}.csv")
    print(f"  - /home/zl749/unique_npi_cases_{year}.csv")
    
    return True, total_npi_counts, unique_npi_count

# =============================================================================
# MAIN EXECUTION
# =============================================================================
print("SEMAGLUTIDE ANALYSIS - STEPS 1 AND 2 ONLY")
print("="*60)
print(f"Processing years {START_YEAR}-{END_YEAR} for {DATABASE}")
print(f"Chunk size: {CHUNK_SIZE:,} patients")
print("="*60)

# Connect with optimized settings
conn = duckdb.connect()
conn.execute("SET memory_limit='8GB'")
conn.execute("SET threads=4")

successful_years = []
failed_years = []
overall_start_time = time.time()

try:
    # Process each year
    for year in range(START_YEAR, END_YEAR + 1):
        try:
            success, total_npi_counts, unique_npi_count = process_year_steps_1_and_2(year)
            if success:
                successful_years.append((year, total_npi_counts, unique_npi_count))
            else:
                failed_years.append(year)
        except Exception as e:
            print(f"ERROR processing {year}: {e}")
            failed_years.append(year)
        
        # Brief pause between years
        time.sleep(3)
    
    # Final summary
    total_time = time.time() - overall_start_time
    print(f"\n{'='*60}")
    print("FINAL SUMMARY - STEPS 1 AND 2")
    print(f"{'='*60}")
    print(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
    print(f"Successful years: {len(successful_years)}")
    print(f"Failed years: {len(failed_years)}")
    
    if successful_years:
        print("\nResults by year:")
        print("Year | Total NPI Counts | Unique NPI Cases | % Unique")
        print("-" * 55)
        for year, total_counts, unique_counts in successful_years:
            pct = unique_counts/total_counts*100 if total_counts > 0 else 0
            print(f"{year} | {total_counts:14,} | {unique_counts:14,} | {pct:6.1f}%")
        
        print(f"\nOutput files created:")
        for year, _, _ in successful_years:
            print(f"  /home/zl749/npi_histogram_{year}.csv")
            print(f"  /home/zl749/unique_npi_cases_{year}.csv")

except Exception as e:
    print(f"CRITICAL ERROR: {e}")

finally:
    conn.close()

print("\nSteps 1 and 2 completed! Ready for Steps 3 and 4.")
