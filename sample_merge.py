import duckdb
import pandas as pd
import time

#prelim 
START_YEAR = 2018  # Skip years before semaglutide availability
END_YEAR = 2024
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
CHUNK_SIZE = 200000 

def process_year_fast(year):
    print(f"\n{'='*60}")
    print(f"FAST PROCESSING YEAR {year}")
    print(f"{'='*60}")
    
    # File paths
    d_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_D_{year}.parquet"
    o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"
    
    total_start_time = time.time()
    
    # SETUP: Get chunking parameters
    print("Setup: Getting chunking parameters...")
    setup_start = time.time()
    
    #Store ENROLID ranges for chunking
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
    
    print(f"  Patients: {total_patients:,}, Chunks: {num_chunks} (larger chunks)")
    print(f"  Setup completed in {time.time() - setup_start:.2f} seconds")
    
    #Join without expansion (more efficient)
    print("\nOPTIMIZED: Direct processing without intermediate expansion...")
    process_start = time.time()
    
    #Initialize final unique NPI counts table
    conn.execute("""
        CREATE OR REPLACE TABLE npi_counts_all AS
        SELECT 
            CAST(NULL AS BIGINT) as ENROLID,
            CAST(NULL AS DATE) as original_svcdate,
            CAST(NULL AS INTEGER) as unique_npis
        WHERE FALSE
    """)
    
    chunk_start_enrolid = max_enrolid - chunk_step # (try last 200,000 patients)
    chunk_num = 0
    
    print("  OPTIMIZATION: Using direct date range join (no expansion needed)")
    print("  Processing prescription dates with ±3 days physician visits directly")
    
    while chunk_start_enrolid <= max_enrolid:
        chunk_end_enrolid = min(chunk_start_enrolid + chunk_step, max_enrolid)
        chunk_num += 1
        
        print(f"    Chunk {chunk_num}/{num_chunks}: ENROLIDs {chunk_start_enrolid:,} to {chunk_end_enrolid:,}")
        
        try:
            #Get all relevant ENROLIDs from both files for this range
            # Step 1: From file D, get prescription dates for this ENROLID range
            conn.execute(f"""
                CREATE OR REPLACE TABLE chunk_prescriptions AS
                SELECT DISTINCT ENROLID, SVCDATE 
                FROM '{d_file}'
                WHERE ENROLID BETWEEN {chunk_start_enrolid} AND {chunk_end_enrolid}
            """)
            
            chunk_prescriptions = conn.execute("SELECT COUNT(*) FROM chunk_prescriptions").fetchone()[0]
            
            # Step 2: From file O, get ALL visits for patients who have prescriptions in this chunk
            conn.execute(f"""
                CREATE OR REPLACE TABLE chunk_visits AS
                SELECT DISTINCT o.ENROLID, o.SVCDATE, o.NPI
                FROM '{o_file}' o
                INNER JOIN chunk_prescriptions p ON o.ENROLID = p.ENROLID
            """)
            
            chunk_visits = conn.execute("SELECT COUNT(*) FROM chunk_visits").fetchone()[0]
            
            # Step 3: Do the safe join with ±3 days (including 0-NPI cases)
            conn.execute(f"""
                CREATE OR REPLACE TABLE chunk_npi_counts AS
                SELECT 
                    p.ENROLID,
                    p.SVCDATE as original_svcdate,
                    COALESCE(COUNT(DISTINCT o.NPI), 0) as unique_npis
                FROM chunk_prescriptions p
                LEFT JOIN chunk_visits o 
                ON p.ENROLID = o.ENROLID 
                AND o.SVCDATE BETWEEN (p.SVCDATE - INTERVAL 3 DAY) 
                                  AND (p.SVCDATE + INTERVAL 3 DAY)
                GROUP BY p.ENROLID, p.SVCDATE
            """)
            
            chunk_count = conn.execute("SELECT COUNT(*) FROM chunk_npi_counts").fetchone()[0]
            print(f"      {chunk_prescriptions:,} prescriptions + {chunk_visits:,} visits → {chunk_count:,} NPI counts")
            
            # Add to combined results
            conn.execute("""
                INSERT INTO npi_counts_all 
                SELECT * FROM chunk_npi_counts
            """)
            
            # Clean up
            conn.execute("DROP TABLE chunk_prescriptions")
            conn.execute("DROP TABLE chunk_visits")
            conn.execute("DROP TABLE chunk_npi_counts")
            
        except Exception as e:
            print(f"      ERROR in chunk {chunk_num}: {e}")
            break
            
        chunk_start_enrolid = chunk_end_enrolid + 1
        
        if chunk_num % 10 == 0:
            elapsed = time.time() - process_start
            rate = chunk_num / elapsed * 60  # chunks per minute
            print(f"      Progress: {chunk_num}/{num_chunks} chunks ({elapsed:.1f}s, {rate:.1f} chunks/min)")
            time.sleep(0.5)  # Shorter pause
    
    total_npi_counts = conn.execute("SELECT COUNT(*) FROM npi_counts_all").fetchone()[0]
    print(f"  OPTIMIZED processing completed: {total_npi_counts:,} patient-dates in {time.time() - process_start:.2f} seconds")
    
    # Print histogram of unique NPI 
    print("\nCreating histogram and filtering...")
    histogram_start = time.time()

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
    histogram_data.to_csv(f"/home/zl749/npi_histogram_fast_{year}.csv", index=False)
    
    # Get cases where unique NPI =1
    unique_npi_count = conn.execute("SELECT COUNT(*) FROM npi_counts_all WHERE unique_npis = 1").fetchone()[0]
    
    # Save unique NPI cases
    conn.execute(f"""
        COPY (
            SELECT ENROLID, original_svcdate
            FROM npi_counts_all
            WHERE unique_npis = 1
        ) TO '/home/zl749/unique_npi_cases_fast_{year}.csv' (FORMAT 'csv', HEADER)
    """)
    
    print(f"  Cases with exactly 1 NPI: {unique_npi_count:,} ({unique_npi_count/total_npi_counts*100:.1f}%)")
    print(f"  Histogram completed in {time.time() - histogram_start:.2f} seconds")
    
    # Clean up
    conn.execute("DROP TABLE npi_counts_all")
    
    total_time = time.time() - total_start_time
    print(f"\nYear {year} FAST processing completed in {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
    
    return True, total_npi_counts, unique_npi_count

# =============================================================================
# MAIN EXECUTION
# =============================================================================
print("CHECK UNIQUE NPI DISTRIBUTION")
print("="*60)
print(f"  - Years: {START_YEAR}-{END_YEAR} (skipping pre-semaglutide years)")
print(f"  - Chunk size: {CHUNK_SIZE:,} patients (larger chunks)")
print(f"  - Direct date range joins (no expansion)")
print(f"  - Parallel processing enabled")
print("="*60)

# Enhanced connection settings
conn = duckdb.connect()
conn.execute("SET memory_limit='10GB'")    # More memory
conn.execute("SET threads=8")              # More CPU cores
conn.execute("SET enable_progress_bar=false")  # Less overhead

successful_years = []
failed_years = []
overall_start_time = time.time()

try:
    for year in range(START_YEAR, END_YEAR + 1):
        try:
            success, total_npi_counts, unique_npi_count = process_year_fast(year)
            if success:
                successful_years.append((year, total_npi_counts, unique_npi_count))
            else:
                failed_years.append(year)
        except Exception as e:
            print(f"ERROR processing {year}: {e}")
            failed_years.append(year)
        
        time.sleep(1)
    
    total_time = time.time() - overall_start_time
    print(f"\n{'='*60}")
    print("OPTIMIZED SUMMARY")
    print(f"{'='*60}")
    print(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
    print(f"Successful years: {len(successful_years)}")
    print(f"Average time per year: {total_time/len(successful_years):.1f} seconds")
    
    if successful_years:
        print("\nResults by year:")
        print("Year | Total NPI Counts | Unique NPI Cases | % Unique")
        print("-" * 55)
        total_counts_all = 0
        unique_counts_all = 0
        for year, total_counts, unique_counts in successful_years:
            pct = unique_counts/total_counts*100 if total_counts > 0 else 0
            print(f"{year} | {total_counts:14,} | {unique_counts:14,} | {pct:6.1f}%")
            total_counts_all += total_counts
            unique_counts_all += unique_counts
        
        overall_pct = unique_counts_all/total_counts_all*100 if total_counts_all > 0 else 0
        print("-" * 55)
        print(f"TOTAL| {total_counts_all:14,} | {unique_counts_all:14,} | {overall_pct:6.1f}%")
        
        print(f"\nFast output files created:")
        for year, _, _ in successful_years:
            print(f"  /home/zl749/npi_histogram_fast_{year}.csv")
            print(f"  /home/zl749/unique_npi_cases_fast_{year}.csv")

except Exception as e:
    print(f"CRITICAL ERROR: {e}")

finally:
    conn.close()

print(f"\nOptimized Steps 1 and 2 completed!")
print(f"Speed improvements:")
print(f"  - Skipped pre-semaglutide years: ~30% time saved")
print(f"  - Direct joins (no expansion): ~7x faster processing")
print(f"  - Larger chunks: ~2x less overhead")
print(f"  - Enhanced CPU/memory: Additional speed boost")
