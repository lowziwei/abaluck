import duckdb
import pandas as pd
import time
import gc
import matplotlib.pyplot as plt
from pathlib import Path

# Configuration - TEST VERSION
START_YEAR = 2018
END_YEAR = 2018
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
TEST_PATIENTS = 200000  # Test with first 200k patients

def test_first_200k_patients(year):
    print(f"\n{'='*60}")
    print(f"TEST: FIRST 200,000 PATIENTS - YEAR {year}")
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
    
    overall_start_time = time.time()

    try:
        # Step 1: Get first 200k patients
        print("\nStep 1: Getting first 200,000 patients...")
        step1_start = time.time()
        
        enrolid_query = f"""
            SELECT DISTINCT ENROLID
            FROM '{d_file}'
            WHERE ENROLID IS NOT NULL
            ORDER BY ENROLID
            LIMIT {TEST_PATIENTS}
        """
        
        test_enrolids = conn.execute(enrolid_query).fetchdf()['ENROLID'].tolist()
        step1_time = time.time() - step1_start
        
        print(f"  Selected {len(test_enrolids):,} patients in {step1_time:.2f} seconds")

        # Step 2: Cache outpatient data for these patients only
        print("\nStep 2: Caching outpatient data for test patients...")
        step2_start = time.time()
        
        enrolid_list = "', '".join(map(str, test_enrolids))
        
        conn.execute(f"""
            CREATE TEMP TABLE test_outpatient AS
            SELECT ENROLID, SVCDATE, NPI
            FROM '{o_file}'
            WHERE ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL 
              AND NPI IS NOT NULL
              AND ENROLID IN ('{enrolid_list}')
        """)
        
        conn.execute("CREATE INDEX idx_test_out ON test_outpatient(ENROLID)")
        outpatient_count = conn.execute("SELECT COUNT(*) FROM test_outpatient").fetchone()[0]
        step2_time = time.time() - step2_start
        
        print(f"  Cached {outpatient_count:,} outpatient visits in {step2_time:.2f} seconds")

        # Step 3: Process prescriptions for test patients
        print("\nStep 3: Processing prescriptions for test patients...")
        step3_start = time.time()
        
        main_query = f"""
        WITH 
        -- Get ONLY ENROLID, SVCDATE from prescription file for test patients
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
        test_prescriptions AS (
            SELECT ENROLID, SVCDATE as prescription_date
            FROM first_prescriptions 
            WHERE rn = 1
        ),
        -- Join prescriptions to outpatient visits within ±3 days
        matched_visits AS (
            SELECT 
                p.ENROLID,
                p.prescription_date,
                o.SVCDATE as physician_date,
                o.NPI
            FROM test_prescriptions p
            INNER JOIN test_outpatient o 
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

        final_df = conn.execute(main_query).fetchdf()
        step3_time = time.time() - step3_start
        
        print(f"  Processed {len(final_df):,} prescription events in {step3_time:.2f} seconds")

        # Step 4: Create histogram
        print("\nStep 4: Creating histogram...")
        step4_start = time.time()
        
        if len(final_df) > 0:
            histogram_data = final_df['unique_npi_count'].value_counts().sort_index()
            total_events = len(final_df)
            
            print(f"\nHistogram Results (200k patients sample):")
            print("=" * 50)
            for npi_count, frequency in histogram_data.items():
                percentage = (frequency / total_events) * 100
                print(f"  {npi_count:2d} NPI(s): {frequency:6,} events ({percentage:5.1f}%)")
            
            # Single NPI cases
            single_npi_count = histogram_data.get(1, 0)
            single_npi_pct = (single_npi_count / total_events) * 100 if total_events > 0 else 0
            
            print(f"\nSingle NPI Summary:")
            print(f"  Cases with exactly 1 NPI: {single_npi_count:,} ({single_npi_pct:.1f}%)")
            
        else:
            print("  No prescription events found!")
            
        step4_time = time.time() - step4_start
        print(f"  Histogram created in {step4_time:.2f} seconds")

        # Overall timing
        total_time = time.time() - overall_start_time
        
        print(f"\n{'='*60}")
        print(f"TEST COMPLETE - TIMING BREAKDOWN")
        print(f"{'='*60}")
        print(f"Step 1 (Get 200k patients):     {step1_time:6.2f} seconds")
        print(f"Step 2 (Cache outpatient):      {step2_time:6.2f} seconds") 
        print(f"Step 3 (Process & join):        {step3_time:6.2f} seconds")
        print(f"Step 4 (Create histogram):      {step4_time:6.2f} seconds")
        print(f"{'='*60}")
        print(f"TOTAL TIME:                     {total_time:6.2f} seconds")
        print(f"{'='*60}")
        
        if len(final_df) > 0:
            print(f"Processing rate: {len(test_enrolids)/total_time:,.0f} patients/second")
            print(f"Estimated time for full dataset: {(total_time * len(test_enrolids) / TEST_PATIENTS) / 60:.1f} minutes")
        
        return {
            'patients_processed': len(test_enrolids),
            'prescription_events': len(final_df) if len(final_df) > 0 else 0,
            'total_time': total_time,
            'step_times': {
                'get_patients': step1_time,
                'cache_outpatient': step2_time, 
                'process_join': step3_time,
                'histogram': step4_time
            }
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
    print("MarketScan Analysis - 200K PATIENT TEST")
    print("=" * 50)
    
    result = test_first_200k_patients(2018)
    
    if result:
        print("\n🎉 Test completed successfully!")
        print("Ready to run full dataset? (Results will help estimate total time)")
    else:
        print("\n❌ Test failed - need to debug before running full dataset")

if __name__ == "__main__":
    main()
