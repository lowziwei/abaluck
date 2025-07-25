import duckdb
import pandas as pd
import time
import gc
import matplotlib.pyplot as plt
from pathlib import Path

# Configuration - FULL DATASET
START_YEAR = 2018
END_YEAR = 2018
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
CHUNK_SIZE = 50000

def process_full_year(year):
    print(f"\n{'='*60}")
    print(f"FULL DATASET PROCESSING - YEAR {year}")
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

        # Step 2: Process ALL chunks with AGGRESSIVE memory management
        print(f"\nStep 2: Processing ALL {len(chunks)} chunks with aggressive memory optimization...")
        all_results = []

        for chunk_idx, chunk_enrolids in enumerate(chunks):
            chunk_start = time.time()
            print(f"  Chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk_enrolids):,} patients)")

            try:
                enrolid_list = "', '".join(map(str, chunk_enrolids))

                # AGGRESSIVE APPROACH: Everything in one optimized query
                chunk_query = f"""
                WITH 
                -- Step 1: From file D, get ONLY ENROLID, SVCDATE for chunk X patients
                chunk_prescriptions_raw AS (
                    SELECT ENROLID, SVCDATE
                    FROM '{d_file}'
                    WHERE ENROLID IS NOT NULL 
                      AND SVCDATE IS NOT NULL
                      AND ENROLID IN ('{enrolid_list}')
                ),
                -- Keep first row per ENROLID, SVCDATE (as specified)
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
                -- Step 2: From file O, get ONLY ENROLID, SVCDATE, NPI for chunk X patients  
                chunk_outpatient AS (
                    SELECT ENROLID, SVCDATE, NPI
                    FROM '{o_file}'
                    WHERE ENROLID IS NOT NULL 
                      AND SVCDATE IS NOT NULL 
                      AND ENROLID IN ('{enrolid_list}')
                ),
                -- Step 3: Merge with +/- 3 day SVCDATE band around prescriptions
                matched_visits AS (
                    SELECT 
                        p.ENROLID,
                        p.SVCDATE,
                        o.SVCDATE as physician_date,
                        o.NPI
                    FROM unique_chunk_prescriptions p
                    INNER JOIN chunk_outpatient o 
                        ON p.ENROLID = o.ENROLID 
                        AND o.SVCDATE BETWEEN (p.SVCDATE - INTERVAL 3 DAY) 
                                          AND (p.SVCDATE + INTERVAL 3 DAY)
                )
                -- Step 4: Collapse to count UNIQUE NPIs per ENROLID, SVCDATE (excluding NULLs from count)
                SELECT 
                    ENROLID,
                    SVCDATE as prescription_date,
                    COUNT(DISTINCT CASE WHEN NPI IS NOT NULL THEN NPI END) as unique_npi_count,
                    COUNT(*) as total_visits,
                    ARRAY_AGG(DISTINCT CASE WHEN NPI IS NOT NULL THEN NPI END) as npi_list
                FROM matched_visits
                GROUP BY ENROLID, SVCDATE
                ORDER BY ENROLID, SVCDATE
                """

                chunk_result = conn.execute(chunk_query).fetchdf()
                
                if not chunk_result.empty:
                    all_results.append(chunk_result)

                chunk_time = time.time() - chunk_start
                print(f"    Results: {len(chunk_result):,} prescription events in {chunk_time:.1f}s")
                print(f"    Memory: Data for chunk {chunk_idx + 1} automatically discarded")

                # Clean up chunk data (aggressive cleanup)
                del chunk_result, chunk_enrolids
                
                # More frequent garbage collection for aggressive memory management
                if (chunk_idx + 1) % 5 == 0:
                    gc.collect()
                    print(f"    Aggressive memory cleanup performed")

            except Exception as e:
                print(f"    ERROR in chunk {chunk_idx + 1}: {e}")
                continue

        # Step 3: Combine all results
        print(f"\nStep 3: Combining {len(all_results)} chunks...")
        
        if not all_results:
            print("ERROR: No successful chunks!")
            return None

        final_df = pd.concat(all_results, ignore_index=True)
        print(f"  Final dataset: {len(final_df):,} prescription events with provider matches")

        del all_results
        gc.collect()

        # Step 4: Create histogram
        print("\nStep 4: Creating histogram...")
        histogram_data = final_df['unique_npi_count'].value_counts().sort_index()
        total_events = len(final_df)
        
        print(f"\nHistogram of Unique NPI Counts per Prescription Event:")
        print("=" * 60)
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

        # Clean up single NPI data
        single_npi_clean = single_npi_df[['ENROLID', 'prescription_date', 'total_visits']].copy()
        single_npi_clean['NPI'] = single_npi_df['npi_list'].apply(lambda x: x[0] if x else None)

        # Step 6: Save files
        print("\nStep 6: Saving results...")
        
        merged_file = f'merged_prescriptions_providers_{year}.parquet'
        final_df.to_parquet(merged_file, compression='snappy')
        
        single_npi_file = f'single_npi_prescriptions_{year}.parquet'
        single_npi_clean.to_parquet(single_npi_file, compression='snappy')
        
        histogram_df = pd.DataFrame({
            'unique_npi_count': histogram_data.index,
            'frequency': histogram_data.values,
            'percentage': (histogram_data.values / total_events) * 100
        })
        histogram_data_file = f'npi_histogram_data_{year}.csv'
        histogram_df.to_csv(histogram_data_file, index=False)

        total_time = time.time() - total_start_time
        
        print(f"\n{'='*60}")
        print(f"FULL DATASET COMPLETE - {total_time/60:.1f} minutes")
        print(f"{'='*60}")
        print(f"Processed: {total_patients:,} patients")
        print(f"Results: {len(final_df):,} prescription events")
        print(f"Files created:")
        print(f"  - {merged_file}")
        print(f"  - {single_npi_file}")
        print(f"  - {histogram_data_file}")
        print(f"  - {histogram_file}")
        print(f"{'='*60}")

        return {
            'merged_data': final_df,
            'single_npi_data': single_npi_clean,
            'histogram_data': histogram_df
        }

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
    print("MarketScan Analysis - FULL DATASET")
    print("=" * 40)
    
    result = process_full_year(2018)
    
    if result:
        print("\n🎉 Full dataset processing completed successfully!")
    else:
        print("\n❌ Processing failed")

if __name__ == "__main__":
    main()
