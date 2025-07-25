import duckdb
import pandas as pd
import time
import gc
import matplotlib.pyplot as plt
from pathlib import Path

# Configuration
START_YEAR = 2018
END_YEAR = 2018
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
CHUNK_SIZE = 50000

def process_year_sql_approach(year):
    print(f"\n{'='*60}")
    print(f"SQL APPROACH PROCESSING YEAR {year}")
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
        # Step 1: Get unique ENROLIDs for chunking (only ENROLID column)
        print("\nStep 1: Getting unique ENROLIDs for chunking...")
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
        
        print(f"  Total patients: {total_patients:,}, Chunks: {len(chunks)}")
        
        del enrolid_df
        gc.collect()

        # Step 2: Cache ONLY relevant columns from outpatient file
        print("\nStep 2: Caching outpatient data (ENROLID, SVCDATE, NPI only)...")
        conn.execute(f"""
            CREATE TEMP TABLE remaining_outpatient AS
            SELECT ENROLID, SVCDATE, NPI
            FROM '{o_file}'
            WHERE ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL 
              AND NPI IS NOT NULL
        """)
        
        conn.execute("CREATE INDEX idx_remaining_enrolid ON remaining_outpatient(ENROLID)")
        initial_count = conn.execute("SELECT COUNT(*) FROM remaining_outpatient").fetchone()[0]
        print(f"  Cached outpatient visits: {initial_count:,} (3 columns only)")

        # Step 3: Process chunks and progressively remove processed ENROLIDs
        print(f"\nStep 3: Processing {len(chunks)} chunks with progressive filtering...")
        all_results = []

        for chunk_idx, chunk_enrolids in enumerate(chunks):
            chunk_start = time.time()
            
            # Check how many outpatient records remain
            remaining_count = conn.execute("SELECT COUNT(*) FROM remaining_outpatient").fetchone()[0]
            print(f"  Chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk_enrolids):,} patients, {remaining_count:,} outpatient records left)")

            try:
                enrolid_list = "', '".join(map(str, chunk_enrolids))

                # Process this chunk using remaining outpatient data (only relevant columns)
                chunk_query = f"""
                WITH 
                -- Get ONLY ENROLID, SVCDATE from prescription file for this chunk
                first_prescriptions AS (
                    SELECT ENROLID, SVCDATE,
                           ROW_NUMBER() OVER (PARTITION BY ENROLID, SVCDATE ORDER BY ENROLID) as rn
                    FROM (
                        SELECT ENROLID, SVCDATE
                        FROM '{d_file}'
                        WHERE ENROLID IS NOT NULL 
                          AND SVCDATE IS NOT NULL
                          AND ENROLID IN ('{enrolid_list}')
                    )
                ),
                chunk_prescriptions AS (
                    SELECT ENROLID, SVCDATE as prescription_date
                    FROM first_prescriptions 
                    WHERE rn = 1
                ),
                -- Join prescriptions to remaining physicians within ±3 days
                matched_visits AS (
                    SELECT 
                        p.ENROLID,
                        p.prescription_date,
                        o.SVCDATE as physician_date,
                        o.NPI
                    FROM chunk_prescriptions p
                    INNER JOIN remaining_outpatient o 
                        ON p.ENROLID = o.ENROLID 
                        AND o.SVCDATE BETWEEN (p.prescription_date - INTERVAL 3 DAY) 
                                          AND (p.prescription_date + INTERVAL 3 DAY)
                )
                -- Count unique NPIs per prescription
                SELECT 
                    ENROLID,
                    prescription_date,
                    COUNT(DISTINCT NPI) as unique_npi_count,
                    COUNT(*) as total_visits,
                    ARRAY_AGG(DISTINCT NPI ORDER BY NPI) as npi_list
                FROM matched_visits
                GROUP BY ENROLID, prescription_date
                ORDER BY ENROLID, prescription_date
                """

                chunk_result = conn.execute(chunk_query).fetchdf()
                
                if not chunk_result.empty:
                    all_results.append(chunk_result)

                # REMOVE processed ENROLIDs from remaining outpatient data
                conn.execute(f"""
                    DELETE FROM remaining_outpatient 
                    WHERE ENROLID IN ('{enrolid_list}')
                """)

                chunk_time = time.time() - chunk_start
                after_delete_count = conn.execute("SELECT COUNT(*) FROM remaining_outpatient").fetchone()[0]
                print(f"    Results: {len(chunk_result):,} prescription events in {chunk_time:.1f}s")
                print(f"    Outpatient records after deletion: {after_delete_count:,}")

                # Clean up
                del chunk_result, chunk_enrolids
                
                # Garbage collection every 10 chunks
                if (chunk_idx + 1) % 10 == 0:
                    gc.collect()
                    print(f"    Memory cleanup")

            except Exception as e:
                print(f"    ERROR in chunk {chunk_idx + 1}: {e}")
                continue

        # Step 4: Combine results
        print(f"\nStep 3: Combining {len(all_results)} chunks...")
        
        if not all_results:
            print("ERROR: No successful chunks!")
            return None

        final_df = pd.concat(all_results, ignore_index=True)
        print(f"  Final dataset: {len(final_df):,} prescription events with provider matches")

        del all_results
        gc.collect()

        # Step 5: Create histogram
        print("\nStep 4: Creating histogram...")
        histogram_data = final_df['unique_npi_count'].value_counts().sort_index()
        total_events = len(final_df)
        
        print("\nHistogram of Unique NPI Counts per Prescription Event:")
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

        # Step 6: Single NPI subset
        print("\nStep 5: Single NPI subset...")
        single_npi_df = final_df[final_df['unique_npi_count'] == 1].copy()
        
        print(f"  Single NPI cases: {len(single_npi_df):,} ({len(single_npi_df)/len(final_df)*100:.1f}%)")

        # Clean up single NPI data
        single_npi_clean = single_npi_df[['ENROLID', 'prescription_date', 'total_visits']].copy()
        single_npi_clean['NPI'] = single_npi_df['npi_list'].apply(lambda x: x[0] if x else None)

        # Step 7: Save files
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
        print(f"SQL APPROACH COMPLETE - {total_time:.1f} seconds")
        print(f"Files: {merged_file}, {single_npi_file}, {histogram_data_file}")
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
    print("MarketScan Analysis - SQL APPROACH")
    print("=" * 40)
    
    for year in range(START_YEAR, END_YEAR + 1):
        try:
            result = process_year_sql_approach(year)
            if result is not None:
                print(f"Success for year {year}!")
        except Exception as e:
            print(f"Failed year {year}: {e}")

if __name__ == "__main__":
    main()
