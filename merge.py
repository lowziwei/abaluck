import duckdb
import pandas as pd
import time

# prelim
SAMPLE_PERCENT = 10  # Start with 10%
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
START_YEAR = 2014
END_YEAR = 2024 

conn = duckdb.connect()

def merge_ndcnum_for_year(year, sample_percent=10):
    """
    Merge NDCNUM from D dataset to O dataset for a specific year
    """
    d_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_D_{year}.parquet"
    o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"
    output_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_WITH_NDCNUM_{year}_SAMPLE{sample_percent}PCT.parquet"
    
    print(f"Processing {year} with {sample_percent}% sample...")
    
    try:
        # First, check if files exist and get basic info
        print(f"  Checking D file: {d_file}")
        d_count = conn.execute(f"SELECT COUNT(*) FROM '{d_file}'").fetchone()[0]
        print(f"  D dataset rows: {d_count:,}")
        
        print(f"  Checking O file: {o_file}")
        o_count = conn.execute(f"SELECT COUNT(*) FROM '{o_file}'").fetchone()[0]
        print(f"  O dataset rows: {o_count:,}")
        
        # Create the merged table with samples
        print(f"  Creating merge with {sample_percent}% samples...")
        start_time = time.time()
        
        conn.execute(f"""
            CREATE OR REPLACE TABLE merged_sample_{year} AS
            SELECT 
                o.*,
                d.NDCNUM
            FROM (
                SELECT * FROM '{o_file}' 
                USING SAMPLE {sample_percent} PERCENT
            ) o
            LEFT JOIN (
                SELECT SEQNUM, ENROLID, SVCDATE, NDCNUM
                FROM '{d_file}'
                USING SAMPLE {sample_percent} PERCENT
            ) d 
            ON o.SEQNUM = d.SEQNUM 
            AND o.ENROLID = d.ENROLID 
            AND o.SVCDATE = d.SVCDATE
        """)
        
        # Get merge statistics
        result_count = conn.execute(f"SELECT COUNT(*) FROM merged_sample_{year}").fetchone()[0]
        matched_count = conn.execute(f"SELECT COUNT(*) FROM merged_sample_{year} WHERE NDCNUM IS NOT NULL").fetchone()[0]
        
        merge_time = time.time() - start_time
        print(f"  Merge completed in {merge_time:.2f} seconds")
        print(f"  Result rows: {result_count:,}")
        print(f"  Rows with NDCNUM: {matched_count:,} ({matched_count/result_count*100:.1f}%)")
        
        # Export to parquet
        print(f"  Exporting to: {output_file}")
        export_start = time.time()
        
        conn.execute(f"""
            COPY merged_sample_{year} TO '{output_file}' (FORMAT 'parquet')
        """)
        
        export_time = time.time() - export_start
        print(f"  Export completed in {export_time:.2f} seconds")
        
        # Clean up temp table to save memory
        conn.execute(f"DROP TABLE merged_sample_{year}")
        
        return True, result_count, matched_count
        
    except Exception as e:
        print(f"  ERROR: {e}")
        return False, 0, 0

# =============================================================================
# MAIN PROCESSING LOOP
# =============================================================================
print("="*60)
print("MERGING NDCNUM FROM D DATASETS TO O DATASETS")
print("="*60)
print(f"Sample size: {SAMPLE_PERCENT}%")
print(f"Years: {START_YEAR} to {END_YEAR}")
print("="*60)

successful_merges = []
failed_merges = []
total_start_time = time.time()

for year in range(START_YEAR, END_YEAR + 1):
    print(f"\n{'='*20} YEAR {year} {'='*20}")
    
    success, result_rows, matched_rows = merge_ndcnum_for_year(year, SAMPLE_PERCENT)
    
    if success:
        successful_merges.append((year, result_rows, matched_rows))
        print(f"✓ {year} completed successfully")
    else:
        failed_merges.append(year)
        print(f"✗ {year} failed")
    
    # Small pause to let system breathe
    time.sleep(1)

# =============================================================================
# SUMMARY REPORT
# =============================================================================
total_time = time.time() - total_start_time

print("\n" + "="*60)
print("FINAL SUMMARY")
print("="*60)
print(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
print(f"Successful merges: {len(successful_merges)}")
print(f"Failed merges: {len(failed_merges)}")

if successful_merges:
    print("\nSuccessful merges:")
    for year, result_rows, matched_rows in successful_merges:
        match_rate = matched_rows/result_rows*100 if result_rows > 0 else 0
        print(f"  {year}: {result_rows:,} rows, {matched_rows:,} with NDCNUM ({match_rate:.1f}%)")

if failed_merges:
    print(f"\nFailed years: {failed_merges}")

print("\nOutput files created:")
for year, _, _ in successful_merges:
    output_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_WITH_NDCNUM_{year}_SAMPLE{SAMPLE_PERCENT}PCT.parquet"
    print(f"  {output_file}")

conn.close()
print("\nScript completed!")
