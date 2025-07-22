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

def process_year_efficient(year):
    """Process semaglutide analysis for a single year using efficient single-pass approach"""
    print(f"\n{'='*20} YEAR {year} {'='*20}")
    
    # File paths
    d_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_D_{year}.parquet"
    o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"
    output_file = f"/home/zl749/{DATABASE}_SEMAGLUTIDE_ANALYSIS_{year}_FULL.parquet"
    
    try:
        # Quick file validation
        d_count = conn.execute(f"SELECT COUNT(*) FROM '{d_file}' LIMIT 1").fetchone()[0] > 0
        o_count = conn.execute(f"SELECT COUNT(*) FROM '{o_file}' LIMIT 1").fetchone()[0] > 0
        if not (d_count and o_count):
            raise Exception("Files not accessible")
        print(f"  Files validated successfully")
        
    except Exception as e:
        print(f"  ERROR: Files not found - {e}")
        return False, 0, 0

    total_start_time = time.time()

    # =============================================================================
    # STEP 1: Process D file - Create semaglutide indicators
    # =============================================================================
    print("  Step 1: Processing D file for semaglutide...")
    start_time = time.time()

    # Get basic semaglutide combinations first
    conn.execute(f"""
        CREATE OR REPLACE TABLE d_base_{year} AS
        SELECT DISTINCT ENROLID, SVCDATE, 1 as semaglutide_indicator
        FROM '{d_file}'
        WHERE NDCNUM IN ({ndcs_sql})
    """)
    
    d_base_count = conn.execute(f"SELECT COUNT(*) FROM d_base_{year}").fetchone()[0]
    print(f"    Base semaglutide combinations: {d_base_count:,}")
    print(f"    Step 1 completed in {time.time() - start_time:.2f} seconds")

    # =============================================================================
    # STEP 2: Add ±3 days window to D file (ALWAYS, regardless of semaglutide count)
    # =============================================================================
    print("  Step 2: Adding ±3 days window to D file...")
    start_time = time.time()
    
    if d_base_count > 0:
        # Expand semaglutide dates with ±3 days window
        conn.execute(f"""
            CREATE OR REPLACE TABLE d_expanded_{year} AS
            SELECT DISTINCT
                ENROLID,
                (SVCDATE + INTERVAL (day_offset) DAY)::DATE as SVCDATE,
                semaglutide_indicator
            FROM d_base_{year}
            CROSS JOIN (VALUES (-3), (-2), (-1), (0), (1), (2), (3)) t(day_offset)
        """)
        
        d_expanded_count = conn.execute(f"SELECT COUNT(*) FROM d_expanded_{year}").fetchone()[0]
        print(f"    D file expanded with ±3 days: {d_expanded_count:,} combinations")
    else:
        # Create empty expanded table for consistency
        conn.execute(f"""
            CREATE OR REPLACE TABLE d_expanded_{year} AS
            SELECT 
                CAST(NULL AS BIGINT) as ENROLID,
                CAST(NULL AS DATE) as SVCDATE,
                0 as semaglutide_indicator
            WHERE FALSE
        """)
        print(f"    No semaglutide found - empty expanded table created")

    print(f"    Step 2 completed in {time.time() - start_time:.2f} seconds")

    # =============================================================================  
    # STEP 3: Process O file and merge with expanded D file
    # =============================================================================
    print("  Step 3: Processing O file and merging...")
    start_time = time.time()
    
    # Single query: get unique O combinations and join with expanded D
    conn.execute(f"""
        COPY (
            SELECT 
                o.ENROLID,
                o.SVCDATE,
                o.NPI,
                COALESCE(d.semaglutide_indicator, 0) as semaglutide_indicator
            FROM (
                SELECT DISTINCT ENROLID, SVCDATE, NPI
                FROM '{o_file}'
            ) o
            LEFT JOIN d_expanded_{year} d
            ON o.ENROLID = d.ENROLID AND o.SVCDATE = d.SVCDATE
        ) TO '{output_file}' (FORMAT 'parquet')
    """)
    
    print(f"    Single-pass processing and export completed in {time.time() - start_time:.2f} seconds")

    # =============================================================================
    # STEP 4: Get final statistics
    # =============================================================================
    print("  Step 4: Getting final statistics...")
    start_time = time.time()
    
    # Read back just the stats we need
    final_count = conn.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
    sema_matches = conn.execute(f"SELECT COUNT(*) FROM '{output_file}' WHERE semaglutide_indicator = 1").fetchone()[0]
    
    print(f"    Final dataset: {final_count:,} rows")
    print(f"    Semaglutide matches: {sema_matches:,} ({sema_matches/final_count*100:.2f}%)")
    print(f"    Step 4 completed in {time.time() - start_time:.2f} seconds")

    # Clean up temporary tables
    conn.execute(f"DROP TABLE d_base_{year}")
    if d_base_count > 0:
        conn.execute(f"DROP TABLE d_expanded_{year}")
    
    total_time = time.time() - total_start_time
    print(f"  ✓ {year} completed successfully in {total_time:.2f} seconds: {output_file}")
    
    return True, final_count, sema_matches

# =============================================================================
# MAIN PROCESSING LOOP
# =============================================================================
print("="*60)
print(f"PROCESSING SEMAGLUTIDE ANALYSIS FOR YEARS {START_YEAR}-{END_YEAR}")
print("="*60)
print("Using EFFICIENT SINGLE-PASS approach for full datasets")
print("="*60)

# Connect to DuckDB with optimized settings
conn = duckdb.connect()
# Let DuckDB manage its own temp directory
conn.execute("SET memory_limit='12GB'")  # Increase memory for large joins
conn.execute("SET threads=6")  # Use more threads for parallel processing
conn.execute("SET enable_progress_bar=true")  # Show progress for long operations
conn.execute("SET preserve_insertion_order=false")  # Allow reordering for efficiency

successful_years = []
failed_years = []
total_start_time = time.time()

for year in range(START_YEAR, END_YEAR + 1):
    try:
        success, final_count, sema_matches = process_year_efficient(year)
        if success:
            successful_years.append((year, final_count, sema_matches))
        else:
            failed_years.append(year)
    except Exception as e:
        print(f"  ERROR processing {year}: {e}")
        failed_years.append(year)
    
    # Brief pause between years
    time.sleep(3)

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
        print(f"  {year}: {final_count:,} rows, {sema_matches:,} semaglutide ({match_rate:.2f}%)")
        total_rows += final_count
        total_sema += sema_matches
    
    print(f"\nOverall totals:")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Total semaglutide cases: {total_sema:,}")
    if total_rows > 0:
        print(f"  Overall match rate: {total_sema/total_rows*100:.2f}%")

if failed_years:
    print(f"\nFailed years: {failed_years}")

print("\nOutput files created:")
for year, _, _ in successful_years:
    output_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_SEMAGLUTIDE_ANALYSIS_{year}_FULL.parquet"
    print(f"  {output_file}")

conn.close()
print("Script completed!")
