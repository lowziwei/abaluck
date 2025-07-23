import duckdb
import pandas as pd
import time

# Configuration
START_YEAR = 2018
END_YEAR = 2024
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
CHUNK_SIZE = 200000

def process_year_fast_fixed(year):
    print(f"\n{'='*60}")
    print(f"FAST PROCESSING YEAR {year} - WITH OUTPATIENT + INPATIENT")
    print(f"{'='*60}")

    d_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_D_{year}.parquet"
    o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"
    i_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_I_{year}.parquet"  # Added inpatient file

    total_start_time = time.time()

    # Step 1: Load unique ENROLIDs from prescriptions to chunk better
    print("Step 1: Getting unique ENROLIDs for chunking...")
    enrolid_df = conn.execute(f"""
        SELECT DISTINCT ENROLID
        FROM '{d_file}'
        ORDER BY ENROLID
    """).fetchdf()

    total_patients = len(enrolid_df)
    chunks = [enrolid_df.iloc[i:i + CHUNK_SIZE]['ENROLID'].tolist() for i in range(0, total_patients, CHUNK_SIZE)]

    print(f"  Total patients: {total_patients:,}, Chunks: {len(chunks)}")

    # Step 2: Cache BOTH outpatient AND inpatient visits for year
    print("\nStep 2: Caching outpatient AND inpatient visits...")
    step2_start = time.time()
    
    # First check file sizes
    try:
        o_count = conn.execute(f"SELECT COUNT(*) FROM '{o_file}'").fetchone()[0]
        print(f"  Outpatient file: {o_count:,} rows")
    except Exception as e:
        print(f"  WARNING: Could not access outpatient file: {e}")
        o_count = 0
    
    try:
        i_count = conn.execute(f"SELECT COUNT(*) FROM '{i_file}'").fetchone()[0]
        print(f"  Inpatient file: {i_count:,} rows")
    except Exception as e:
        print(f"  WARNING: Could not access inpatient file: {e}")
        i_count = 0
    
    total_visits = o_count + i_count
    print(f"  Total visits to cache: {total_visits:,} rows")
    
    # Cache both files together
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE cached_visits AS
        SELECT ENROLID, SVCDATE, NPI, 'Outpatient' as source_type
        FROM '{o_file}'
        UNION ALL
        SELECT ENROLID, SVCDATE, NPI, 'Inpatient' as source_type  
        FROM '{i_file}'
    """)
    
    cached_count = conn.execute("SELECT COUNT(*) FROM cached_visits").fetchone()[0]
    step2_time = time.time() - step2_start
    print(f"  Cached visits: {cached_count:,} rows in {step2_time:.1f} seconds")
    
    # Show breakdown by source
    source_breakdown = conn.execute("""
        SELECT source_type, COUNT(*) as visit_count
        FROM cached_visits
        GROUP BY source_type
        ORDER BY source_type
    """).df()
    print("  Source breakdown:")
    print(source_breakdown.to_string(index=False, justify='left'))

    # Step 3: Prepare result storage
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE npi_counts_all AS
        SELECT 
            CAST(NULL AS BIGINT) as ENROLID,
            CAST(NULL AS DATE) as original_svcdate,
            CAST(NULL AS INTEGER) as unique_npis,
            CAST(NULL AS VARCHAR) as npi_sources
        WHERE FALSE
    """)

    # Step 4: Process chunks
    print("\nStep 3: Processing chunks with temporal join ±3 days")
    successful_chunks = 0
    
    for chunk_idx, chunk_ids in enumerate(chunks, start=1):
        print(f"  Chunk {chunk_idx}/{len(chunks)}: {len(chunk_ids):,} ENROLIDs")
        
        try:
            # Create temporary table for chunk ENROLIDs
            conn.execute("CREATE OR REPLACE TEMP TABLE chunk_enrolids AS SELECT NULL::BIGINT AS ENROLID WHERE FALSE")
            
            # Insert ENROLIDs in batches to avoid memory issues
            batch_size = 1000
            for i in range(0, len(chunk_ids), batch_size):
                batch = chunk_ids[i:i + batch_size]
                values_str = ",".join(f"({eid})" for eid in batch)
                conn.execute(f"INSERT INTO chunk_enrolids VALUES {values_str}")
            
            # Process chunk with JOIN instead of IN clause
            conn.execute(f"""
                WITH chunk_prescriptions AS (
                    SELECT DISTINCT p.ENROLID, p.SVCDATE
                    FROM '{d_file}' p
                    INNER JOIN chunk_enrolids c ON p.ENROLID = c.ENROLID
                ),
                chunk_visits AS (
                    SELECT v.ENROLID, v.SVCDATE, v.NPI, v.source_type
                    FROM cached_visits v
                    INNER JOIN chunk_enrolids c ON v.ENROLID = c.ENROLID
                ),
                chunk_npi_counts AS (
                    SELECT 
                        p.ENROLID,
                        p.SVCDATE AS original_svcdate,
                        COALESCE(COUNT(DISTINCT o.NPI), 0) AS unique_npis,
                        STRING_AGG(DISTINCT o.source_type, '|') as npi_sources
                    FROM chunk_prescriptions p
                    LEFT JOIN chunk_visits o 
                        ON p.ENROLID = o.ENROLID
                        AND o.SVCDATE BETWEEN (p.SVCDATE - INTERVAL 3 DAY) 
                                          AND (p.SVCDATE + INTERVAL 3 DAY)
                    GROUP BY p.ENROLID, p.SVCDATE
                )
                INSERT INTO npi_counts_all
                SELECT * FROM chunk_npi_counts
            """)
            
            # Clean up chunk table
            conn.execute("DROP TABLE chunk_enrolids")
            successful_chunks += 1
            
        except Exception as e:
            print(f"    ERROR in chunk {chunk_idx}: {e}")
            break
        
        # Progress update every 10 chunks
        if chunk_idx % 10 == 0:
            elapsed = time.time() - total_start_time
            rate = chunk_idx / elapsed * 60
            print(f"    Progress: {chunk_idx}/{len(chunks)} chunks ({elapsed:.1f}s, {rate:.1f} chunks/min)")

    print(f"  Successfully processed {successful_chunks}/{len(chunks)} chunks")

    # Step 5: Create histogram and save results
    print("\nStep 4: Computing histogram and source analysis...")
    
    # Basic histogram
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
    histogram_data.to_csv(f"/home/zl749/npi_histogram_combined_{year}.csv", index=False)

    # Source breakdown for cases with NPIs
    print("\nSource breakdown for cases with NPIs:")
    source_analysis = conn.execute("""
        SELECT 
            npi_sources,
            COUNT(*) as count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
        FROM npi_counts_all
        WHERE unique_npis > 0
        GROUP BY npi_sources
        ORDER BY count DESC
    """).df()
    print(source_analysis.to_string(index=False))
    source_analysis.to_csv(f"/home/zl749/source_breakdown_{year}.csv", index=False)

    total_npi_counts = conn.execute("SELECT COUNT(*) FROM npi_counts_all").fetchone()[0]
    unique_npi_count = conn.execute("SELECT COUNT(*) FROM npi_counts_all WHERE unique_npis = 1").fetchone()[0]
    
    # Additional statistics
    outpatient_only = conn.execute("SELECT COUNT(*) FROM npi_counts_all WHERE npi_sources = 'Outpatient' AND unique_npis = 1").fetchone()[0]
    inpatient_only = conn.execute("SELECT COUNT(*) FROM npi_counts_all WHERE npi_sources = 'Inpatient' AND unique_npis = 1").fetchone()[0]
    both_sources = conn.execute("SELECT COUNT(*) FROM npi_counts_all WHERE npi_sources = 'Inpatient|Outpatient' AND unique_npis = 1").fetchone()[0]

    print(f"\nAssignment Statistics:")
    print(f"  Total prescriptions: {total_npi_counts:,}")
    print(f"  Cases with exactly 1 NPI: {unique_npi_count:,} ({unique_npi_count / total_npi_counts * 100:.1f}%)")
    print(f"    - From Outpatient only: {outpatient_only:,}")
    print(f"    - From Inpatient only: {inpatient_only:,}")
    print(f"    - From Both sources: {both_sources:,}")
    
    improvement = (unique_npi_count - outpatient_only) / outpatient_only * 100 if outpatient_only > 0 else 0
    print(f"  Improvement from adding Inpatient: +{improvement:.1f}%")

    # Save unique NPI cases
    conn.execute(f"""
        COPY (
            SELECT ENROLID, original_svcdate, npi_sources
            FROM npi_counts_all
            WHERE unique_npis = 1
        ) TO '/home/zl749/unique_npi_cases_combined_{year}.csv' (FORMAT 'csv', HEADER)
    """)

    # Clean up
    conn.execute("DROP TABLE npi_counts_all")
    conn.execute("DROP TABLE cached_visits")

    elapsed = time.time() - total_start_time
    print(f"\nYear {year} completed in {elapsed:.2f} seconds ({elapsed / 60:.1f} minutes)")
    print(f"Files created:")
    print(f"  - /home/zl749/npi_histogram_combined_{year}.csv")
    print(f"  - /home/zl749/source_breakdown_{year}.csv")
    print(f"  - /home/zl749/unique_npi_cases_combined_{year}.csv")
    
    return True, total_npi_counts, unique_npi_count, improvement

# Main execution
conn = duckdb.connect()
conn.execute("SET memory_limit='12GB'")  # Increased for both files
conn.execute("SET threads=6")

successful_years = []
failed_years = []

for year in range(START_YEAR, END_YEAR + 1):
    try:
        success, total_counts, unique_counts, improvement = process_year_fast_fixed(year)
        if success:
            print(f"✓ {year}: {total_counts:,} total, {unique_counts:,} unique (+{improvement:.1f}% improvement)")
            successful_years.append((year, total_counts, unique_counts, improvement))
        time.sleep(2)  # Brief pause between years
    except Exception as e:
        print(f"✗ {year}: ERROR - {e}")
        failed_years.append(year)

# Summary
if successful_years:
    print(f"\n{'='*60}")
    print("FINAL SUMMARY - OUTPATIENT + INPATIENT ANALYSIS")
    print(f"{'='*60}")
    total_improvement = sum(imp for _, _, _, imp in successful_years) / len(successful_years)
    print(f"Average improvement from adding Inpatient data: +{total_improvement:.1f}%")
    
    print("\nYear-by-year results:")
    print("Year | Total      | Assignable | Assignment% | Improvement")
    print("-" * 60)
    for year, total, unique, imp in successful_years:
        pct = unique/total*100 if total > 0 else 0
        print(f"{year} | {total:9,} | {unique:9,} | {pct:8.1f}% | +{imp:6.1f}%")

conn.close()
print("All years completed!")
