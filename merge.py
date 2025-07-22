import duckdb
import pandas as pd
import time

# =============================================================================
# CONFIGURATION
# =============================================================================
START_YEAR = 2014
END_YEAR = 2024  # Adjust as needed
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
SAMPLE_PERCENT = 100  

# Semaglutide NDC codes
semaglutide_ndcs = [
    # Ozempic
    '00169413001', '00169413013', '00169413211', '00169413212',
    '00169413290', '00169413297', '00169413602', '00169413611', 
    '00169418103', '00169418113', '00169418190', '00169418197', 
    '00169477211', '00169477212', '00169477290', '00169477297',
    '50090594900', '50090513800', '50090513900', '50090605100',
    
    # Rybelsus
    '00169430301', '00169430313', '00169430330', '00169430390', 
    '00169430393', '00169430399', '00169430701', '00169430713', 
    '00169430730', '00169431401', '00169431413', '00169431430', 
    '00169480430', '00169480930', '00169481530', '00169481590',
    
    # Wegovy  
    '00169450101', '00169450114', '00169450501', '00169450514',
    '00169451701', '00169451714', '00169452401', '00169452414', 
    '00169452501', '00169452514', '00169452590', '00169452594',
    '50090582400'
]

# Convert list to SQL IN clause format
ndcs_sql = "'" + "','".join(semaglutide_ndcs) + "'"

def process_year(year):
    """Process semaglutide analysis for a single year"""
    print(f"\n{'='*20} YEAR {year} {'='*20}")
    
    # File paths
    d_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_D_{year}.parquet"
    o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"
    
    try:
        # Check if files exist
        d_count = conn.execute(f"SELECT COUNT(*) FROM '{d_file}'").fetchone()[0]
        o_count = conn.execute(f"SELECT COUNT(*) FROM '{o_file}'").fetchone()[0]
        print(f"  D file: {d_count:,} rows")
        print(f"  O file: {o_count:,} rows")
        
    except Exception as e:
        print(f"  ERROR: Files not found - {e}")
        return False, 0, 0

    # =============================================================================
    # STEP 1: Process D file - Create semaglutide indicator
    # =============================================================================
    print("  Step 1: Processing D file for semaglutide...")
    start_time = time.time()

    # First, check sample size
    d_sample_count = conn.execute(f"""
        SELECT COUNT(*) FROM '{d_file}' 
        WHERE NDCNUM IN ({ndcs_sql})
    """).fetchone()[0]

    print(f"    Semaglutide prescriptions in full dataset: {d_sample_count:,}")

    if d_sample_count == 0:
        print("    No semaglutide prescriptions found - will create all 0 indicators")
        # Create empty D table for consistency
        conn.execute(f"""
            CREATE OR REPLACE TABLE d_final_{year} AS
            SELECT 
                CAST(NULL AS BIGINT) as ENROLID,
                CAST(NULL AS DATE) as SVCDATE,
                0 as semaglutide_indicator
            WHERE FALSE  -- Empty table with correct schema
        """)
    else:
        # Create D processed table with semaglutide indicator and collapse
        conn.execute(f"""
            CREATE OR REPLACE TABLE d_processed_{year} AS
            SELECT 
                ENROLID,
                SVCDATE,
                1 as semaglutide_indicator
            FROM '{d_file}'
            WHERE NDCNUM IN ({ndcs_sql})
            GROUP BY ENROLID, SVCDATE  -- Remove duplicates at this stage
        """)

        d_processed_count = conn.execute(f"SELECT COUNT(*) FROM d_processed_{year}").fetchone()[0]
        print(f"    D processed (unique ENROLID, SVCDATE): {d_processed_count:,} rows")

    print(f"    Step 1 completed in {time.time() - start_time:.2f} seconds")

    # =============================================================================
    # STEP 2: Add +/- 3 days window (ALWAYS, even if no semaglutide found)
    # =============================================================================
    print("  Step 2: Adding +/- 3 days window...")
    start_time = time.time()

    if d_sample_count > 0:
        # Process the semaglutide data with window
        conn.execute(f"""
            CREATE OR REPLACE TABLE d_with_window_{year} AS
            SELECT 
                ENROLID,
                (SVCDATE + INTERVAL (day_offset) DAY)::DATE as SVCDATE,
                semaglutide_indicator
            FROM d_processed_{year}
            CROSS JOIN (
                SELECT day_offset FROM (VALUES (-3), (-2), (-1), (0), (1), (2), (3)) t(day_offset)
            ) days
        """)

        # Collapse again to handle duplicates after date expansion
        conn.execute(f"""
            CREATE OR REPLACE TABLE d_final_{year} AS
            SELECT 
                ENROLID,
                SVCDATE,
                MAX(semaglutide_indicator) as semaglutide_indicator
            FROM d_with_window_{year}
            GROUP BY ENROLID, SVCDATE
        """)

        d_final_count = conn.execute(f"SELECT COUNT(*) FROM d_final_{year}").fetchone()[0]
        print(f"    D final (with ±3 days window): {d_final_count:,} rows")
    else:
        # Even with no semaglutide, create the empty table structure for consistent merging
        print("    No semaglutide found - creating empty D table for consistent merge structure")
        d_final_count = 0

    print(f"    Step 2 completed in {time.time() - start_time:.2f} seconds")

    # =============================================================================
    # STEP 3: Process O file - Check for multiple NPIs
    # =============================================================================
    print("  Step 3: Processing O file...")
    start_time = time.time()

    # First check for multiple NPIs per ENROLID, SVCDATE
    npi_check = conn.execute(f"""
        SELECT COUNT(*) as multi_npi_cases
        FROM (
            SELECT ENROLID, SVCDATE, COUNT(DISTINCT NPI) as npi_count
            FROM '{o_file}'
            GROUP BY ENROLID, SVCDATE
            HAVING COUNT(DISTINCT NPI) > 1
        )
    """).fetchone()[0]

    print(f"    Cases with multiple NPIs per ENROLID, SVCDATE: {npi_check:,}")

    if npi_check > 0:
        print("    Multiple NPIs found - keeping separate rows for each NPI")
        # Keep all NPI combinations
        conn.execute(f"""
            CREATE OR REPLACE TABLE o_processed_{year} AS
            SELECT DISTINCT
                ENROLID,
                SVCDATE,
                NPI
            FROM '{o_file}'
        """)
    else:
        print("    No multiple NPIs - collapsing to unique ENROLID, SVCDATE")
        # Can safely collapse since no multiple NPIs
        conn.execute(f"""
            CREATE OR REPLACE TABLE o_processed_{year} AS
            SELECT 
                ENROLID,
                SVCDATE,
                MIN(NPI) as NPI  -- Take first NPI since they're all the same
            FROM '{o_file}'
            GROUP BY ENROLID, SVCDATE
        """)

    o_processed_count = conn.execute(f"SELECT COUNT(*) FROM o_processed_{year}").fetchone()[0]
    print(f"    O processed: {o_processed_count:,} rows")
    print(f"    Step 3 completed in {time.time() - start_time:.2f} seconds")

    # =============================================================================
    # STEP 4: Final merge
    # =============================================================================
    print("  Step 4: Final merge...")
    start_time = time.time()

    conn.execute(f"""
        CREATE OR REPLACE TABLE final_merged_{year} AS
        SELECT 
            o.ENROLID,
            o.SVCDATE,
            o.NPI,
            COALESCE(d.semaglutide_indicator, 0) as semaglutide_indicator
        FROM o_processed_{year} o
        LEFT JOIN d_final_{year} d
        ON o.ENROLID = d.ENROLID 
        AND o.SVCDATE = d.SVCDATE
    """)

    # Get merge statistics
    final_count = conn.execute(f"SELECT COUNT(*) FROM final_merged_{year}").fetchone()[0]
    sema_matches = conn.execute(f"SELECT COUNT(*) FROM final_merged_{year} WHERE semaglutide_indicator = 1").fetchone()[0]

    print(f"    Final merged dataset: {final_count:,} rows")
    print(f"    Rows with semaglutide indicator: {sema_matches:,} ({sema_matches/final_count*100:.2f}%)")
    print(f"    Step 4 completed in {time.time() - start_time:.2f} seconds")

    # =============================================================================
    # STEP 5: Export results
    # =============================================================================
    print("  Step 5: Exporting results...")
    output_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_SEMAGLUTIDE_ANALYSIS_{year}_FULL.parquet"

    conn.execute(f"""
        COPY final_merged_{year} TO '{output_file}' (FORMAT 'parquet')
    """)

    # Clean up year-specific tables
    if d_sample_count > 0:
        conn.execute(f"DROP TABLE d_processed_{year}")
        conn.execute(f"DROP TABLE d_with_window_{year}") 
    conn.execute(f"DROP TABLE d_final_{year}")
    conn.execute(f"DROP TABLE o_processed_{year}")
    conn.execute(f"DROP TABLE final_merged_{year}")
    
    print(f"  ✓ {year} completed successfully: {output_file}")
    return True, final_count, sema_matches

# =============================================================================
# MAIN PROCESSING LOOP
# =============================================================================
print("="*60)
print(f"PROCESSING SEMAGLUTIDE ANALYSIS FOR YEARS {START_YEAR}-{END_YEAR}")
print("="*60)
print(f"Sample size: FULL DATASET (100%)")
print("="*60)

# Connect to DuckDB and set temp directory
conn = duckdb.connect()
conn.execute("SET temp_directory='/data/temp_duckdb'")
conn.execute("SET memory_limit='8GB'")  # Increase memory for full sample
conn.execute("SET threads=4")  # Use multiple threads for better performance

successful_years = []
failed_years = []
total_start_time = time.time()

for year in range(START_YEAR, END_YEAR + 1):
    try:
        success, final_count, sema_matches = process_year(year)
        if success:
            successful_years.append((year, final_count, sema_matches))
        else:
            failed_years.append(year)
    except Exception as e:
        print(f"  ERROR processing {year}: {e}")
        failed_years.append(year)
    
    # Small pause to let system breathe (longer pause for full dataset processing)
    time.sleep(5)

# =============================================================================
# FINAL SUMMARY
# =============================================================================
total_time = time.time() - total_start_time

print("\n" + "="*60)
print("FINAL SUMMARY")
print("="*60)
print(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
print(f"Successful years: {len(successful_years)}")
print(f"Failed years: {len(failed_years)}")

if successful_years:
    print("\nSuccessful years:")
    total_rows = 0
    total_sema = 0
    for year, final_count, sema_matches in successful_years:
        match_rate = sema_matches/final_count*100 if final_count > 0 else 0
        print(f"  {year}: {final_count:,} rows, {sema_matches:,} with semaglutide ({match_rate:.2f}%)")
        total_rows += final_count
        total_sema += sema_matches
    
    print(f"\nOverall totals:")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Total semaglutide cases: {total_sema:,}")
    print(f"  Overall match rate: {total_sema/total_rows*100:.2f}%")

if failed_years:
    print(f"\nFailed years: {failed_years}")

print("\nOutput files created:")
for year, _, _ in successful_years:
    output_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_SEMAGLUTIDE_ANALYSIS_{year}_SAMPLE{SAMPLE_PERCENT}PCT.parquet"
    print(f"  {output_file}")

conn.close()
print("Script completed!")
