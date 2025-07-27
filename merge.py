import duckdb
import pandas as pd
import time
import gc
import matplotlib.pyplot as plt
import glob
import os
from pathlib import Path

# Configuration - BATCH PROCESSING WITH INTERMEDIATE SAVES
START_YEAR = 2018
END_YEAR = 2018
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
CHUNK_SIZE = 50000
SAVE_EVERY = 10  # Save intermediate results every 10 chunks

def process_full_year_with_saves(year):
    print(f"\n{'='*60}")
    print(f"BATCH PROCESSING WITH SAVES - YEAR {year}")
    print(f"{'='*60}")

    # File paths
    d_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_D_{year}.parquet"
    o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"

    # Verify files exist
    for file_type, file_path in [('Prescription', d_file), ('Outpatient', o_file)]:
        if not Path(file_path).exists():
            print(f"ERROR: Missing {file_type} file: {file_path}")
            return None

    # Initialize DuckDB
    conn = duckdb.connect()
    conn.execute("SET memory_limit='6GB'")
    conn.execute("SET threads=3")
    
    total_start_time = time.time()

    try:
        # Step 1: Get ALL unique ENROLIDs for chunking
        print("\nStep 1: Getting ALL unique ENROLIDs for chunking...")
        step1_start = time.time()
        
        enrolid_query = f"""
            SELECT DISTINCT ENROLID
            FROM '{d_file}'
            WHERE ENROLID IS NOT NULL
            ORDER BY ENROLID
        """
        
        enrolid_df = conn.execute(enrolid_query).fetchdf()
        total_patients = len(enrolid_df)
        chunks = [enrolid_df.iloc[i:i + CHUNK_SIZE]['ENROLID'].tolist() 
                 for i in range(0, total_patients, CHUNK_SIZE)]
        
        step1_time = time.time() - step1_start
        print(f"  Total patients: {total_patients:,}")
        print(f"  Total chunks: {len(chunks)} (size: {CHUNK_SIZE:,} each)")
        print(f"  Step 1 completed in {step1_time:.1f} seconds")
        
        del enrolid_df
        gc.collect()

         # Step 2: Cache data once
        print("\nStep 2: Caching data for progressive deletion...")
        step2_start = time.time()
        
        # Cache prescription data (only needed columns)
        print("  Caching ALL prescription data...")
        conn.execute(f"""
            CREATE TEMP TABLE remaining_prescriptions AS
            SELECT ENROLID, SVCDATE
            FROM '{d_file}'
            WHERE ENROLID IS NOT NULL AND SVCDATE IS NOT NULL
        """)
        conn.execute("CREATE INDEX idx_remaining_presc ON remaining_prescriptions(ENROLID)")
        
        # Cache ALL outpatient data (only needed columns)
        print("  Caching ALL outpatient data...")
        conn.execute(f"""
            CREATE TEMP TABLE remaining_outpatient AS
            SELECT ENROLID, SVCDATE, NPI
            FROM '{o_file}'
            WHERE ENROLID IS NOT NULL AND SVCDATE IS NOT NULL
        """)
        conn.execute("CREATE INDEX idx_remaining_out ON remaining_outpatient(ENROLID)")
        
        presc_count = conn.execute("SELECT COUNT(*) FROM remaining_prescriptions").fetchone()[0]
        out_count = conn.execute("SELECT COUNT(*) FROM remaining_outpatient").fetchone()[0]
        
        step2_time = time.time() - step2_start
        print(f"  Cached {presc_count:,} prescriptions and {out_count:,} outpatient visits")
        print(f"  Step 2 completed in {step2_time:.1f} seconds")

        # Step 3: Process chunks with progressive deletion
        print(f"\nStep 3: Processing {len(chunks)} chunks with progressive deletion...")
        
        # Clean up any existing intermediate files
        intermediate_files = glob.glob(f'chunks_{year}_batch_*.parquet')
        for f in intermediate_files:
            os.remove(f)
            
        current_batch_results = []
        batch_number = 0

        for chunk_idx, chunk_enrolids in enumerate(chunks):
            chunk_start = time.time()
            
            # Check remaining data
            remaining_presc = conn.execute("SELECT COUNT(*) FROM remaining_prescriptions").fetchone()[0]
            remaining_out = conn.execute("SELECT COUNT(*) FROM remaining_outpatient").fetchone()[0]
            
            print(f"  Chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk_enrolids):,} patients)")
            print(f"    Remaining: {remaining_presc:,} prescriptions, {remaining_out:,} outpatient")

            try:
                enrolid_list = "', '".join(map(str, chunk_enrolids))

                # Process ONLY current chunk's ENROLIDs
                chunk_query = f"""
                WITH 
                -- Get prescription data ONLY for current chunk ENROLIDs
                chunk_prescriptions_raw AS (
                    SELECT ENROLID, SVCDATE
                    FROM remaining_prescriptions
                    WHERE ENROLID IN ('{enrolid_list}')
                ),
                -- Keep first row per ENROLID, SVCDATE
                chunk_prescriptions AS (
                    SELECT ENROLID, SVCDATE,
                           ROW_NUMBER() OVER (PARTITION BY ENROLID, SVCDATE ORDER BY ENROLID) as rn
                    FROM chunk_prescriptions_raw
                ),
                unique_chunk_prescriptions AS (
                    SELECT ENROLID, SVCDATE
                    FROM chunk_prescriptions
                    WHERE rn = 1
                ),
                -- Get outpatient data ONLY for current chunk ENROLIDs
                chunk_outpatient AS (
                    SELECT ENROLID, SVCDATE, NPI
                    FROM remaining_outpatient
                    WHERE ENROLID IN ('{enrolid_list}')
                ),
                -- Merge chunk prescription with chunk outpatient (±3 days)
                matched_visits AS (
                    SELECT 
                        p.ENROLID,
                        p.SVCDATE,
                        o.NPI
                    FROM unique_chunk_prescriptions p
                    LEFT JOIN chunk_outpatient o 
                        ON p.ENROLID = o.ENROLID 
                        AND o.SVCDATE BETWEEN (p.SVCDATE - INTERVAL 3 DAY) 
                                          AND (p.SVCDATE + INTERVAL 3 DAY)
                )
                -- Count unique NPIs per prescription
                SELECT 
                    ENROLID,
                    SVCDATE,
                    COUNT(DISTINCT CASE WHEN NPI IS NOT NULL THEN NPI END) as unique_npi_count
                FROM matched_visits
                GROUP BY ENROLID, SVCDATE
                ORDER BY ENROLID, SVCDATE
                """

                chunk_result = conn.execute(chunk_query).fetchdf()
                
                if not chunk_result.empty:
                    current_batch_results.append(chunk_result)

                # DELETE current chunk's ENROLIDs from BOTH temp tables
                print(f"    Deleting chunk ENROLIDs from cached data...")
                delete_start = time.time()
                
                conn.execute(f"""
                    DELETE FROM remaining_prescriptions 
                    WHERE ENROLID IN ('{enrolid_list}')
                """)
                
                conn.execute(f"""
                    DELETE FROM remaining_outpatient 
                    WHERE ENROLID IN ('{enrolid_list}')
                """)
                
                delete_time = time.time() - delete_start
                chunk_time = time.time() - chunk_start
                
                print(f"    Results: {len(chunk_result):,} prescription events")
                print(f"    Deletion: {delete_time:.1f}s, Total chunk: {chunk_time:.1f}s")

                # Clean up chunk data
                del chunk_result, chunk_enrolids

                # Save intermediate results every SAVE_EVERY chunks
                if (chunk_idx + 1) % SAVE_EVERY == 0 or (chunk_idx + 1) == len(chunks):
                    batch_number += 1
                    batch_start_chunk = max(0, chunk_idx + 1 - SAVE_EVERY)
                    batch_end_chunk = chunk_idx + 1
                    
                    print(f"    💾 Saving batch {batch_number} (chunks {batch_start_chunk + 1}-{batch_end_chunk})...")
                    
                    if current_batch_results:
                        batch_df = pd.concat(current_batch_results, ignore_index=True)
                        batch_file = f'chunks_batch_{batch_number:03d}.parquet'
                        batch_df.to_parquet(batch_file, compression='snappy')
                        
                        print(f"       Saved {len(batch_df):,} prescription events to {batch_file}")
                        
                        # Clear batch data and force cleanup
                        del batch_df, current_batch_results
                        current_batch_results = []
                        gc.collect()

            except Exception as e:
                print(f"    ERROR in chunk {chunk_idx + 1}: {e}")
                continue

        # Step 4: Load and combine all intermediate files
        print(f"\nStep 3: Loading and combining intermediate batch files...")
        
        intermediate_files = sorted(glob.glob(f'chunks_{year}_batch_*.parquet'))
        print(f"  Found {len(intermediate_files)} batch files to combine")
        
        if not intermediate_files:
            print("ERROR: No intermediate files found!")
            return None

        # Load and combine all batch files
        all_batch_dfs = []
        total_events = 0
        
        for i, batch_file in enumerate(intermediate_files):
            print(f"  Loading {batch_file}...")
            batch_df = pd.read_parquet(batch_file)
            all_batch_dfs.append(batch_df)
            total_events += len(batch_df)
            print(f"    Loaded {len(batch_df):,} prescription events")

        final_df = pd.concat(all_batch_dfs, ignore_index=True)
        print(f"  Combined total: {len(final_df):,} prescription events")
        print(f"  Columns: {list(final_df.columns)}")

        # Clean up intermediate files and data
        del all_batch_dfs
        gc.collect()
        
        # Clean up intermediate files (they're no longer needed)
        #print(f"  Cleaning up {len(intermediate_files)} intermediate files...")
        #for batch_file in intermediate_files:
            #os.remove(batch_file)
        #print(f"  All intermediate files deleted")

        # Step 4: Create histogram
        print("\nStep 4: Creating histogram...")
        histogram_data = final_df['unique_npi_count'].value_counts().sort_index()
        total_events = len(final_df)
        
        print(f"\nHistogram of Unique NPI Counts:")
        print("=" * 50)
        for npi_count, frequency in histogram_data.items():
            percentage = (frequency / total_events) * 100
            print(f"  {npi_count:2d} NPI(s): {frequency:6,} events ({percentage:5.1f}%)")

        # Create visual histogram
        plt.figure(figsize=(10, 6))
        plt.bar(histogram_data.index, histogram_data.values)
        plt.xlabel('Number of Unique NPIs per Prescription Event')
        plt.ylabel('Frequency')
        plt.title(f'Distribution of Provider Counts per Prescription Event - {year}')
        plt.grid(True, alpha=0.3)
        
        # Add percentage labels
        for npi_count, freq in histogram_data.items():
            pct = (freq / total_events) * 100
            plt.text(npi_count, freq + max(histogram_data.values) * 0.01, 
                    f'{pct:.1f}%', ha='center', va='bottom')
        
        plt.tight_layout()
        histogram_file = f'npi_histogram_{year}.png'
        plt.savefig(histogram_file, dpi=300, bbox_inches='tight')
        print(f"\nHistogram saved as: {histogram_file}")

        # Step 5: Single NPI subset
        print("\nStep 5: Single NPI subset...")
        single_npi_df = final_df[final_df['unique_npi_count'] == 1].copy()
        
        print(f"  Single NPI cases: {len(single_npi_df):,} ({len(single_npi_df)/len(final_df)*100:.1f}%)")

        # Step 6: Save results
        print("\nStep 6: Saving results...")
        
        # Save full dataset (only 3 columns)
        merged_file = f'prescription_npi_counts_{year}.parquet'
        final_df.to_parquet(merged_file, compression='snappy')
        
        # Save single NPI subset (only 3 columns)
        single_npi_file = f'single_npi_prescriptions_{year}.parquet'
        single_npi_df.to_parquet(single_npi_file, compression='snappy')
        
        # Save histogram data
        histogram_df = pd.DataFrame({
            'unique_npi_count': histogram_data.index,
            'frequency': histogram_data.values,
            'percentage': (histogram_data.values / total_events) * 100
        })
        histogram_data_file = f'npi_histogram_data_{year}.csv'
        histogram_df.to_csv(histogram_data_file, index=False)

        total_time = time.time() - total_start_time
        
        print(f"\n{'='*60}")
        print(f"BATCH PROCESSING COMPLETE - {total_time/60:.1f} minutes")
        print(f"{'='*60}")
        print(f"Processed: {total_patients:,} patients in {len(chunks)} chunks")
        print(f"Results: {len(final_df):,} prescription events")
        print(f"Files: {merged_file}, {single_npi_file}, {histogram_data_file}")
        print(f"{'='*60}")

        return final_df

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        try:
            conn.close()
        except:
            pass
        gc.collect()

def main():
    print("MarketScan Analysis - BATCH PROCESSING WITH SAVES")
    print("=" * 60)
    
    result = process_full_year_with_saves(2018)
    
    if result is not None:
        print("\n🎉 Batch processing completed!")
    else:
        print("\n❌ Processing failed")

if __name__ == "__main__":
    main()
