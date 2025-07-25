import duckdb
import pandas as pd
import time
import gc
import matplotlib.pyplot as plt
from pathlib import Path

# Configuration - FIRST CHUNK TEST
START_YEAR = 2018
END_YEAR = 2018
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
TEST_CHUNK_SIZE = 200000  # First 200k patients

def test_first_chunk_null_fix(year):
    print(f"\n{'='*60}")
    print(f"FIRST CHUNK TEST - 200k PATIENTS (NULL NPI FIX)")
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
        # Step 1: Get first 200k patients
        print(f"\nStep 1: Getting first {TEST_CHUNK_SIZE:,} patients...")
        step1_start = time.time()
        
        enrolid_query = f"""
            SELECT DISTINCT ENROLID
            FROM '{d_file}'
            WHERE ENROLID IS NOT NULL
            ORDER BY ENROLID
            LIMIT {TEST_CHUNK_SIZE}
        """
        
        test_enrolids = conn.execute(enrolid_query).fetchdf()['ENROLID'].tolist()
        step1_time = time.time() - step1_start
        
        print(f"  Selected {len(test_enrolids):,} patients in {step1_time:.2f} seconds")

        # Step 2: Process this chunk with NULL NPI fix
        print(f"\nStep 2: Processing chunk with NULL NPI handling...")
        step2_start = time.time()
        
        enrolid_list = "', '".join(map(str, test_enrolids))

        chunk_query = f"""
        WITH 
        -- Step 1: From file D, get ONLY ENROLID, SVCDATE for test patients
        chunk_prescriptions_raw AS (
            SELECT ENROLID, SVCDATE
            FROM '{d_file}'
            WHERE ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL
              AND ENROLID IN ('{enrolid_list}')
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
        -- Step 2: From file O, get ENROLID, SVCDATE, NPI (INCLUDING NULL NPIs!)
        chunk_outpatient AS (
            SELECT ENROLID, SVCDATE, NPI
            FROM '{o_file}'
            WHERE ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL 
              AND ENROLID IN ('{enrolid_list}')
        ),
        -- Step 3: Merge with +/- 3 day SVCDATE band
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
        -- Step 4: Collapse counting only non-NULL NPIs
        SELECT 
            ENROLID,
            SVCDATE as prescription_date,
            COUNT(DISTINCT CASE WHEN NPI IS NOT NULL THEN NPI END) as unique_npi_count,
            COUNT(*) as total_visits,
            COUNT(CASE WHEN NPI IS NULL THEN 1 END) as null_npi_visits,
            ARRAY_AGG(DISTINCT CASE WHEN NPI IS NOT NULL THEN NPI END) as npi_list
        FROM matched_visits
        GROUP BY ENROLID, SVCDATE
        ORDER BY ENROLID, SVCDATE
        """

        final_df = conn.execute(chunk_query).fetchdf()
        step2_time = time.time() - step2_start
        
        print(f"  Processed {len(final_df):,} prescription events in {step2_time:.1f} seconds")

        # Step 3: Analyze results
        print(f"\nStep 3: Analysis of results...")
        
        if len(final_df) > 0:
            # NULL NPI analysis
            events_with_nulls = (final_df['null_npi_visits'] > 0).sum()
            print(f"  Prescription events with NULL NPI visits: {events_with_nulls:,} ({events_with_nulls/len(final_df)*100:.1f}%)")
            
            # Events per patient
            events_per_patient = len(final_df) / len(test_enrolids)
            print(f"  Events per patient: {events_per_patient:.2f}")
            
            # Histogram
            histogram_data = final_df['unique_npi_count'].value_counts().sort_index()
            total_events = len(final_df)
            
            print(f"\nHistogram (first {TEST_CHUNK_SIZE:,} patients):")
            print("=" * 50)
            for npi_count, frequency in histogram_data.items():
                percentage = (frequency / total_events) * 100
                print(f"  {npi_count:2d} NPI(s): {frequency:6,} events ({percentage:5.1f}%)")
                
            # Single NPI summary
            single_npi_count = histogram_data.get(1, 0)
            single_npi_pct = (single_npi_count / total_events) * 100 if total_events > 0 else 0
            
            print(f"\nSingle NPI Summary:")
            print(f"  Cases with exactly 1 NPI: {single_npi_count:,} ({single_npi_pct:.1f}%)")
            
        else:
            print("  No prescription events found!")

        total_time = time.time() - total_start_time
        
        print(f"\n{'='*60}")
        print(f"FIRST CHUNK TEST COMPLETE - {total_time:.1f} seconds")
        print(f"{'='*60}")
        print(f"Patients processed: {len(test_enrolids):,}")
        print(f"Prescription events: {len(final_df):,}")
        print(f"Processing rate: {len(test_enrolids)/total_time:,.0f} patients/second")
        
        if len(final_df) > 0:
            print(f"Events per patient: {len(final_df)/len(test_enrolids):.2f}")
            print(f"NULL NPI fix working: {events_with_nulls:,} events kept that would have been lost")
        
        print(f"{'='*60}")

        return {
            'patients_processed': len(test_enrolids),
            'prescription_events': len(final_df),
            'total_time': total_time,
            'events_per_patient': len(final_df)/len(test_enrolids),
            'null_npi_events': events_with_nulls if len(final_df) > 0 else 0
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
    print("MarketScan Analysis - FIRST CHUNK TEST")
    print("Testing NULL NPI fix on first 200k patients")
    print("=" * 50)
    
    result = test_first_chunk_null_fix(2018)
    
    if result:
        print(f"\n🎉 First chunk test completed!")
        print(f"Events per patient: {result['events_per_patient']:.2f}")
        print(f"NULL NPI events preserved: {result['null_npi_events']:,}")
        print("Ready to run full dataset if numbers look good!")
    else:
        print("\n❌ Test failed")

if __name__ == "__main__":
    main()
