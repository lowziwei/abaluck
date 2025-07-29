##From truly new prescriptions, check unique NPI counts. 

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
TEST_PATIENT_LIMIT = 200000  # First 200k patients

def load_truly_new_prescriptions():
    """
    Load prescriptions with FLAG = NULL (truly new) from the 12 flag files
    All records are already REFILL = 0, so just filter for FLAG = NULL
    """
    print("Loading truly new prescriptions (FLAG = NULL) from flag files...")
    
    all_truly_new = []
    
    for i in range(1, 13):  # 12 files
        file_path = f"prescription_flags_2018_{DATABASE}_D_part{i:02d}.parquet"
        
        if Path(file_path).exists():
            print(f"  Loading {file_path}...")
            df = pd.read_parquet(file_path)
            
            # Filter for truly new prescriptions (FLAG is NULL)
            # Note: All records are already REFILL = 0 from the flagging process
            truly_new = df[df['FLAG'].isna()].copy()
            
            if len(truly_new) > 0:
                # Keep only needed columns for NPI matching
                truly_new_subset = truly_new[['ENROLID', 'SVCDATE']].copy()
                all_truly_new.append(truly_new_subset)
                
            print(f"    Total records: {len(df):,}")
            print(f"    Truly new (FLAG=NULL): {len(truly_new):,}")
        else:
            print(f"  File {file_path} not found!")
    
    if not all_truly_new:
        print("No truly new prescriptions found!")
        return pd.DataFrame()
    
    # Combine all truly new prescriptions
    combined_df = pd.concat(all_truly_new, ignore_index=True)
    print(f"\nTotal truly new prescriptions loaded: {len(combined_df):,}")
    
    # Clean up
    del all_truly_new
    gc.collect()
    
    return combined_df

def test_truly_new_prescriptions(year):
    print(f"\n{'='*60}")
    print(f"TRULY NEW PRESCRIPTIONS NPI ANALYSIS - YEAR {year}")
    print(f"{'='*60}")

    # File paths
    o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"

    # Verify outpatient file exists
    if not Path(o_file).exists():
        print(f"ERROR: Missing Outpatient file: {o_file}")
        return None

    # Initialize DuckDB
    conn = duckdb.connect()
    conn.execute("SET memory_limit='6GB'")
    conn.execute("SET threads=3")
    
    total_start_time = time.time()

    try:
        # Step 1: Load truly new prescriptions
        print(f"\nStep 1: Loading truly new prescriptions from flag files...")
        step1_start = time.time()
        
        truly_new_df = load_truly_new_prescriptions()
        
        if len(truly_new_df) == 0:
            print("No truly new prescriptions found!")
            return None
        
        step1_time = time.time() - step1_start
        print(f"  Loaded {len(truly_new_df):,} truly new prescriptions in {step1_time:.2f} seconds")

        # Step 2: Take first 200k patients for testing
        print(f"\nStep 2: Taking first {TEST_PATIENT_LIMIT:,} patients...")
        step2_start = time.time()
        
        # Get unique patients and take first 200k
        unique_patients = truly_new_df['ENROLID'].drop_duplicates().sort_values()
        test_patients = unique_patients.head(TEST_PATIENT_LIMIT).tolist()
        
        # Filter prescriptions to only include test patients
        test_prescriptions = truly_new_df[truly_new_df['ENROLID'].isin(test_patients)].copy()
        
        step2_time = time.time() - step2_start
        print(f"  Selected {len(test_patients):,} patients with {len(test_prescriptions):,} prescriptions")
        print(f"  Step 2 completed in {step2_time:.2f} seconds")
        
        # Clean up
        del truly_new_df, unique_patients
        gc.collect()

        # Step 3: Process NPI matching
        print(f"\nStep 3: Processing NPI matching for test patients...")
        step3_start = time.time()
        
        # Convert to format for SQL IN clause
        enrolid_list = "', '".join(map(str, test_patients))

        # Create temporary table with prescription data
        print(f"  Creating temporary table with {len(test_prescriptions):,} prescriptions...")
        
        # Convert DataFrame to list of tuples for SQL
        prescription_values = []
        for _, row in test_prescriptions.iterrows():
            enrolid = row['ENROLID']
            svcdate = row['SVCDATE']
            prescription_values.append(f"({enrolid}, '{svcdate}')")
        
        values_str = ', '.join(prescription_values)
        
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE test_prescriptions AS
            SELECT ENROLID, prescription_date
            FROM (VALUES {values_str}) AS t(ENROLID, prescription_date)
        """)

        # Process NPI matching
        matching_query = f"""
        WITH 
        -- Get outpatient data for test patients
        chunk_outpatient AS (
            SELECT ENROLID, SVCDATE, NPI
            FROM '{o_file}'
            WHERE ENROLID IN ('{enrolid_list}')
              AND ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL
        ),
        -- Merge prescription with outpatient (±5 days)
        matched_visits AS (
            SELECT 
                p.ENROLID,
                p.prescription_date,
                o.SVCDATE as physician_date,
                o.NPI
            FROM test_prescriptions p
            LEFT JOIN chunk_outpatient o 
                ON p.ENROLID = o.ENROLID 
                AND o.SVCDATE BETWEEN (p.prescription_date::DATE - INTERVAL 5 DAY) 
                                  AND (p.prescription_date::DATE + INTERVAL 5 DAY)
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

        final_df = conn.execute(matching_query).fetchdf()
        step3_time = time.time() - step3_start
        
        print(f"  Processed {len(final_df):,} prescription events in {step3_time:.1f} seconds")
        
        # Clean up
        del test_patients, test_prescriptions
        gc.collect()

        # Step 4: Analyze results
        print(f"\nStep 4: Analysis of results...")
        
        if len(final_df) > 0:
            # NULL NPI analysis
            events_with_nulls = (final_df['null_npi_visits'] > 0).sum()
            print(f"  Prescription events with NULL NPI visits: {events_with_nulls:,} ({events_with_nulls/len(final_df)*100:.1f}%)")
            
            # Events per patient
            total_patients = len(test_patients) if 'test_patients' in locals() else TEST_PATIENT_LIMIT
            events_per_patient = len(final_df) / total_patients
            print(f"  Events per patient: {events_per_patient:.2f}")
            
            # Histogram
            histogram_data = final_df['unique_npi_count'].value_counts().sort_index()
            total_events = len(final_df)
            
            print(f"\nHistogram - Truly New Prescriptions (first {TEST_PATIENT_LIMIT:,} patients):")
            print("=" * 65)
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

        # Step 5: Create histogram
        print("\nStep 5: Creating histogram...")
        if len(final_df) > 0:
            histogram_data = final_df['unique_npi_count'].value_counts().sort_index()
            total_events = len(final_df)

            # Create visual histogram
            plt.figure(figsize=(12, 7))
            bars = plt.bar(histogram_data.index, histogram_data.values, 
                          color='lightblue', edgecolor='black', alpha=0.7)
            plt.xlabel('Number of Unique NPIs per Prescription Event')
            plt.ylabel('Frequency')
            plt.title(f'Distribution of Provider Counts per Truly New Prescription Event\n'
                     f'(FLAG=NULL, REFILL=0) - First {TEST_PATIENT_LIMIT:,} Patients')
            plt.grid(True, alpha=0.3)
            
            # Add percentage labels
            for bar, (npi_count, freq) in zip(bars, histogram_data.items()):
                pct = (freq / total_events) * 100
                plt.text(bar.get_x() + bar.get_width()/2., bar.get_height() + max(histogram_data.values) * 0.01, 
                        f'{pct:.1f}%', ha='center', va='bottom', fontsize=9)
            
            plt.tight_layout()
            histogram_file = f'npi_histogram_truly_new_{TEST_PATIENT_LIMIT//1000}k_{year}.png'
            plt.savefig(histogram_file, dpi=300, bbox_inches='tight')
            print(f"  Histogram saved as: {histogram_file}")
            plt.show()
        else:
            print("  No data to plot histogram")

        total_time = time.time() - total_start_time
        
        print(f"\n{'='*60}")
        print(f"TRULY NEW PRESCRIPTIONS ANALYSIS COMPLETE - {total_time:.1f} seconds")
        print(f"{'='*60}")
        print(f"Patients processed: {len(test_patients) if 'test_patients' in locals() else TEST_PATIENT_LIMIT:,}")
        print(f"Prescription events: {len(final_df):,}")
        print(f"Processing rate: {(len(test_patients) if 'test_patients' in locals() else TEST_PATIENT_LIMIT)/total_time:,.0f} patients/second")
        
        if len(final_df) > 0:
            print(f"Events per patient: {len(final_df)/(len(test_patients) if 'test_patients' in locals() else TEST_PATIENT_LIMIT):.2f}")
            print(f"NULL NPI events preserved: {events_with_nulls:,}")
        
        print(f"{'='*60}")

        return {
            'patients_processed': len(test_patients) if 'test_patients' in locals() else TEST_PATIENT_LIMIT,
            'prescription_events': len(final_df),
            'total_time': total_time,
            'events_per_patient': len(final_df)/(len(test_patients) if 'test_patients' in locals() else TEST_PATIENT_LIMIT) if 'test_patients' in locals() else 0,
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
    print("MarketScan Analysis - TRULY NEW PRESCRIPTIONS NPI ANALYSIS")
    print("Using prescriptions with FLAG = NULL (truly new) and REFILL = 0")
    print("=" * 60)
    
    result = test_truly_new_prescriptions(2018)
    
    if result:
        print(f"\n🎉 Truly new prescriptions analysis completed!")
        print(f"Events per patient: {result['events_per_patient']:.2f}")
        print(f"NULL NPI events preserved: {result['null_npi_events']:,}")
        print(f"This analysis used only genuinely NEW prescription starts!")
    else:
        print("\n❌ Analysis failed")

if __name__ == "__main__":
    main()
