#Keep only REFILL = 0. To address high instance of NPI = 0. 

import duckdb
import pandas as pd
import time
import gc
import matplotlib.pyplot as plt
from pathlib import Path

# Configuration - FIRST 200K PATIENTS TEST
START_YEAR = 2018
END_YEAR = 2018
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
TEST_PATIENT_LIMIT = 200000  # First 200k patients

def test_first_200k_patients(year):
    print(f"\n{'='*60}")
    print(f"FIRST 200K PATIENTS TEST - YEAR {year}")
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
        print(f"\nStep 1: Getting first {TEST_PATIENT_LIMIT:,} patients...")
        step1_start = time.time()
        
        enrolid_query = f"""
            SELECT DISTINCT ENROLID
            FROM '{d_file}'
            WHERE ENROLID IS NOT NULL
            ORDER BY ENROLID
            LIMIT {TEST_PATIENT_LIMIT}
        """
        
        test_enrolids = conn.execute(enrolid_query).fetchdf()['ENROLID'].tolist()
        total_patients = len(test_enrolids)
        
        step1_time = time.time() - step1_start
        print(f"  Selected {total_patients:,} patients in {step1_time:.2f} seconds")
        
        gc.collect()

        # Step 2: Process the 200k patients
        print(f"\nStep 2: Processing {total_patients:,} patients...")
        step2_start = time.time()
        
        enrolid_list = "', '".join(map(str, test_enrolids))

        # Single query to process all 200k patients
        chunk_query = f"""
        WITH 
        -- Get prescription data for test patients
        chunk_prescriptions_raw AS (
            SELECT ENROLID, SVCDATE
            FROM '{d_file}'
            WHERE ENROLID IN ('{enrolid_list}')
              AND ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL
              AND REFILL = 0
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
        -- Get outpatient data for test patients
        chunk_outpatient AS (
            SELECT ENROLID, SVCDATE, NPI
            FROM '{o_file}'
            WHERE ENROLID IN ('{enrolid_list}')
              AND ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL
        ),
        -- Merge prescription with outpatient (±3 days)
        matched_visits AS (
            SELECT 
                p.ENROLID,
                p.SVCDATE as prescription_date,
                o.SVCDATE as physician_date,
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
            prescription_date as SVCDATE,
            COUNT(DISTINCT CASE WHEN NPI IS NOT NULL THEN NPI END) as unique_npi_count,
            COUNT(*) as total_visits,
            COUNT(CASE WHEN NPI IS NULL THEN 1 END) as null_npi_visits
        FROM matched_visits
        GROUP BY ENROLID, prescription_date
        ORDER BY ENROLID, prescription_date
        """

        final_df = conn.execute(chunk_query).fetchdf()
        step2_time = time.time() - step2_start
        
        print(f"  Processed {len(final_df):,} prescription events in {step2_time:.1f} seconds")
        
        # Clean up
        del test_enrolids
        gc.collect()

        # Step 3: Analyze results
        print(f"\nStep 3: Analysis of results...")
        
        if len(final_df) > 0:
            # NULL NPI analysis
            events_with_nulls = (final_df['null_npi_visits'] > 0).sum()
            print(f"  Prescription events with NULL NPI visits: {events_with_nulls:,} ({events_with_nulls/len(final_df)*100:.1f}%)")
            
            # Events per patient
            events_per_patient = len(final_df) / total_patients
            print(f"  Events per patient: {events_per_patient:.2f}")
            
            # Histogram
            histogram_data = final_df['unique_npi_count'].value_counts().sort_index()
            total_events = len(final_df)
            
            print(f"\nHistogram (first {TEST_PATIENT_LIMIT:,} patients):")
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
            events_with_nulls = 0

        # Step 4: Create histogram (optional)
        print("\nStep 4: Creating histogram...")
        if len(final_df) > 0:
            histogram_data = final_df['unique_npi_count'].value_counts().sort_index()
            total_events = len(final_df)

            # Create visual histogram
            plt.figure(figsize=(10, 6))
            plt.bar(histogram_data.index, histogram_data.values)
            plt.xlabel('Number of Unique NPIs per Prescription Event')
            plt.ylabel('Frequency')
            plt.title(f'Distribution of Provider Counts per Prescription Event - First {TEST_PATIENT_LIMIT:,} Patients')
            plt.grid(True, alpha=0.3)
            
            # Add percentage labels
            for npi_count, freq in histogram_data.items():
                pct = (freq / total_events) * 100
                plt.text(npi_count, freq + max(histogram_data.values) * 0.01, 
                        f'{pct:.1f}%', ha='center', va='bottom')
            
            plt.tight_layout()
            histogram_file = f'npi_histogram_first_{TEST_PATIENT_LIMIT//1000}k_{year}.png'
            plt.savefig(histogram_file, dpi=300, bbox_inches='tight')
            print(f"  Histogram saved as: {histogram_file}")
        else:
            print("  No data to plot histogram")

        total_time = time.time() - total_start_time
        
        print(f"\n{'='*60}")
        print(f"FIRST 200K PATIENTS TEST COMPLETE - {total_time:.1f} seconds")
        print(f"{'='*60}")
        print(f"Patients processed: {total_patients:,}")
        print(f"Prescription events: {len(final_df):,}")
        print(f"Processing rate: {total_patients/total_time:,.0f} patients/second")
        
        if len(final_df) > 0:
            print(f"Events per patient: {len(final_df)/total_patients:.2f}")
            print(f"NULL NPI events preserved: {events_with_nulls:,}")
        
        print(f"{'='*60}")

        return {
            'patients_processed': total_patients,
            'prescription_events': len(final_df),
            'total_time': total_time,
            'events_per_patient': len(final_df)/total_patients if total_patients > 0 else 0,
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
    print("MarketScan Analysis - FIRST 200K PATIENTS TEST")
    print("Testing direct read processing on first 200k patients")
    print("=" * 60)
    
    result = test_first_200k_patients(2018)
    
    if result:
        print(f"\n🎉 First 200k patients test completed!")
        print(f"Events per patient: {result['events_per_patient']:.2f}")
        print(f"NULL NPI events preserved: {result['null_npi_events']:,}")
        print("Ready to scale up to full dataset if numbers look good!")
    else:
        print("\n❌ Test failed")

if __name__ == "__main__":
    main()
