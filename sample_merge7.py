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
    s_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_S.parquet"  # Not partitioned by year

    # Verify files exist
    for file_type, file_path in [('Outpatient', o_file), ('Inpatient', s_file)]:
        if not Path(file_path).exists():
            print(f"ERROR: Missing {file_type} file: {file_path}")
            return None

    # Initialize DuckDB
    conn = duckdb.connect()
    conn.execute("SET memory_limit='6GB'")
    conn.execute("SET threads=3")
    
    total_start_time = time.time()

    try:
        # Step 1: Get the same first 200k patients as the original analysis
        print(f"\nStep 1: Getting the same first {TEST_PATIENT_LIMIT:,} patients from original file...")
        step1_start = time.time()
        
        # Use the same logic as the original file to get first 200k patients
        d_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_D_{year}.parquet"
        
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

        # Step 2: Load truly new prescriptions for these specific patients
        print(f"\nStep 2: Loading truly new prescriptions for these {total_patients:,} patients...")
        step2_start = time.time()
        
        truly_new_df = load_truly_new_prescriptions()
        
        if len(truly_new_df) == 0:
            print("No truly new prescriptions found!")
            return None
        
        # Filter to only include the same 200k patients from Step 1
        test_prescriptions = truly_new_df[truly_new_df['ENROLID'].isin(test_enrolids)].copy()
        
        step2_time = time.time() - step2_start
        print(f"  Loaded {len(test_prescriptions):,} truly new prescriptions for test patients")
        print(f"  Step 2 completed in {step2_time:.2f} seconds")
        
        # Clean up
        del truly_new_df
        gc.collect()

        # Step 3: Process NPI matching
        print(f"\nStep 3: Processing NPI matching for test patients...")
        step3_start = time.time()
        
        # Convert to format for SQL IN clause  
        enrolid_list = "', '".join(map(str, test_enrolids))

        # Debug: Check what NPI values actually exist
        debug_query = f"""
        SELECT 
            CASE 
                WHEN NPI IS NULL THEN 'SQL_NULL'
                WHEN UPPER(TRIM(CAST(NPI as VARCHAR))) = 'NULL' THEN 'STRING_NULL'
                WHEN UPPER(TRIM(CAST(NPI as VARCHAR))) = '' THEN 'EMPTY_STRING'
                WHEN LENGTH(TRIM(CAST(NPI as VARCHAR))) = 0 THEN 'ZERO_LENGTH'
                ELSE 'REAL_NPI'
            END as npi_type,
            COUNT(*) as count
        FROM '{o_file}'
        WHERE ENROLID IN ('{enrolid_list}')
        GROUP BY 1
        UNION ALL
        SELECT 
            CASE 
                WHEN NPI IS NULL THEN 'SQL_NULL'
                WHEN UPPER(TRIM(CAST(NPI as VARCHAR))) = 'NULL' THEN 'STRING_NULL'
                WHEN UPPER(TRIM(CAST(NPI as VARCHAR))) = '' THEN 'EMPTY_STRING'
                WHEN LENGTH(TRIM(CAST(NPI as VARCHAR))) = 0 THEN 'ZERO_LENGTH'
                ELSE 'REAL_NPI'
            END as npi_type,
            COUNT(*) as count
        FROM '{s_file}'
        WHERE ENROLID IN ('{enrolid_list}') AND YEAR = {year}
        GROUP BY 1
        """
        
        debug_result = conn.execute(debug_query).fetchdf()
        print(f"  Debug - NPI value types in your data:")
        for _, row in debug_result.iterrows():
            print(f"    {row['npi_type']}: {row['count']:,} records")
        
        print(f"  ** If you see 0 STRING_NULL records, that explains why the distributions are identical **")

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

        # Process NPI matching using both Outpatient (O) and Inpatient (S) files
        matching_query = f"""
        WITH 
        -- Get outpatient data for test patients and year
        chunk_outpatient AS (
            SELECT ENROLID, SVCDATE, NPI, 'Outpatient' as source_file
            FROM '{o_file}'
            WHERE ENROLID IN ('{enrolid_list}')
              AND ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL
        ),
        -- Get inpatient data for test patients and year (S file not partitioned by year)
        chunk_inpatient AS (
            SELECT ENROLID, SVCDATE, NPI, 'Inpatient' as source_file
            FROM '{s_file}'
            WHERE ENROLID IN ('{enrolid_list}')
              AND ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL
              AND YEAR = {year}
        ),
        -- Combine outpatient and inpatient data
        combined_visits AS (
            SELECT ENROLID, SVCDATE, NPI, source_file FROM chunk_outpatient
            UNION ALL
            SELECT ENROLID, SVCDATE, NPI, source_file FROM chunk_inpatient
        ),
        -- Merge prescription with combined visits (±30 days)
        matched_visits AS (
            SELECT 
                p.ENROLID,
                p.prescription_date,
                c.SVCDATE as visit_date,
                c.NPI,
                c.source_file
            FROM test_prescriptions p
            LEFT JOIN combined_visits c 
                ON p.ENROLID = c.ENROLID 
                AND c.SVCDATE BETWEEN (p.prescription_date::DATE - INTERVAL 30 DAY) 
                                  AND (p.prescription_date::DATE + INTERVAL 30 DAY)
        )
        -- Count visits and NPIs per prescription - only count string "NULL" as distinct
        SELECT 
            ENROLID,
            prescription_date as SVCDATE,
            -- CORRECTED LOGIC: Count ANY visit as at least 1 distinct provider (even with missing NPI)
            CASE 
                WHEN COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) = 0 THEN 0  -- No visits = 0
                ELSE COUNT(DISTINCT COALESCE(CAST(NPI as VARCHAR), 'MISSING_PROVIDER'))  -- SQL NULL becomes 'MISSING_PROVIDER'
            END as unique_npi_count,
            -- ALTERNATIVE: Count only real NPIs, treat SQL NULL as missing (not distinct)
            COUNT(DISTINCT CASE 
                WHEN visit_date IS NOT NULL AND NPI IS NOT NULL
                THEN CAST(NPI as VARCHAR)
            END) as unique_real_npi_count,
            -- Additional debug
            COUNT(CASE WHEN visit_date IS NOT NULL AND NPI IS NOT NULL THEN 1 END) as visits_with_real_npi,
            COUNT(CASE WHEN visit_date IS NOT NULL AND NPI IS NULL THEN 1 END) as visits_with_sql_null,
            -- Debug info
            COUNT(*) as total_visits,
            COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) as actual_visits,
            COUNT(CASE WHEN visit_date IS NOT NULL AND NPI IS NULL THEN 1 END) as sql_null_visits,
            COUNT(CASE WHEN visit_date IS NOT NULL AND UPPER(TRIM(CAST(NPI as VARCHAR))) = 'NULL' THEN 1 END) as string_null_visits,
            COUNT(CASE WHEN visit_date IS NOT NULL AND NPI IS NOT NULL AND UPPER(TRIM(CAST(NPI as VARCHAR))) != 'NULL' THEN 1 END) as real_npi_visits,
            -- Key indicator: "out of thin air" = no visits at all  
            CASE WHEN COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) = 0 THEN 1 ELSE 0 END as out_of_thin_air
        FROM matched_visits
        GROUP BY ENROLID, prescription_date
        ORDER BY ENROLID, prescription_date
        """

        final_df = conn.execute(matching_query).fetchdf()
        step3_time = time.time() - step3_start
        
        print(f"  Processed {len(final_df):,} prescription events in {step3_time:.1f} seconds")
        
        # Clean up
        del test_enrolids, test_prescriptions
        gc.collect()

        # Step 4: Analyze results
        print(f"\nStep 4: Analysis of results...")
        
        if len(final_df) > 0:
            # Show NPI counting results
            print(f"  NPI counting (string 'NULL' counts as distinct):")
            
            # Show both histograms
            histogram_data_with_missing = final_df['unique_npi_count'].value_counts().sort_index()
            histogram_data_real_only = final_df['unique_real_npi_count'].value_counts().sort_index()
            total_events = len(final_df)
            
            print(f"\nHistogram 1 - Count Missing Provider as Distinct:")
            print("=" * 55)
            for npi_count, frequency in histogram_data_with_missing.items():
                percentage = (frequency / total_events) * 100
                print(f"  {npi_count:2d} NPI(s): {frequency:6,} events ({percentage:5.1f}%)")
            
            print(f"\nHistogram 2 - Only Count Real NPIs (Missing = Not Distinct):")
            print("=" * 65)
            for npi_count, frequency in histogram_data_real_only.items():
                percentage = (frequency / total_events) * 100
                print(f"  {npi_count:2d} NPI(s): {frequency:6,} events ({percentage:5.1f}%)")
            
            # Compare key stats
            zero_npis_method1 = histogram_data_with_missing.get(0, 0)
            zero_npis_method2 = histogram_data_real_only.get(0, 0)
            
            print(f"\nComparison:")
            print(f"  Method 1 (Missing=Distinct) - 0 NPIs: {zero_npis_method1:,}")
            print(f"  Method 2 (Missing=Not Distinct) - 0 NPIs: {zero_npis_method2:,}")
            print(f"  'Out of thin air' (no visits): {out_of_thin_air_count:,}")
            print(f"  Difference due to missing provider info: {zero_npis_method2 - zero_npis_method1:,}")
            
            # Show debug info about NPI types
            total_sql_nulls = final_df['sql_null_visits'].sum()
            total_string_nulls = final_df['string_null_visits'].sum()
            total_real_npis = final_df['real_npi_visits'].sum()
            
            print(f"\n  Debug - Visit types:")
            print(f"    SQL NULL visits: {total_sql_nulls:,}")
            print(f"    String 'NULL' visits: {total_string_nulls:,}")
            print(f"    Real NPI visits: {total_real_npis:,}")
            
            # "Out of thin air" analysis
            out_of_thin_air_count = final_df['out_of_thin_air'].sum()
            has_visits_count = (final_df['actual_visits'] > 0).sum()
            
            print(f"\n  DEBUG BREAKDOWN:")
            print(f"    0 NPI prescriptions: {(final_df['unique_npi_count'] == 0).sum():,}")
            print(f"    'Out of thin air' prescriptions: {out_of_thin_air_count:,}")
            print(f"    Prescriptions with visits but 0 NPIs: {(final_df['unique_npi_count'] == 0).sum() - out_of_thin_air_count:,}")
            
            # Sample prescriptions with visits but 0 NPIs
            weird_cases = final_df[(final_df['unique_npi_count'] == 0) & (final_df['actual_visits'] > 0)]
            if len(weird_cases) > 0:
                print(f"\n    Sample prescriptions with visits but 0 NPIs:")
                print(weird_cases[['ENROLID', 'unique_npi_count', 'actual_visits', 'visits_with_real_npi', 'visits_with_sql_null']].head())
            
            print(f"\n  'Out of thin air' prescriptions (no visits at all): {out_of_thin_air_count:,} ({out_of_thin_air_count/len(final_df)*100:.1f}%)")
            print(f"  Prescriptions with visits found: {has_visits_count:,} ({has_visits_count/len(final_df)*100:.1f}%)")
            
            # Show examples with string NULL
            prescriptions_with_string_null = (final_df['string_null_visits'] > 0).sum()
            if prescriptions_with_string_null > 0:
                print(f"  Prescriptions with string 'NULL' NPIs: {prescriptions_with_string_null:,} ({prescriptions_with_string_null/len(final_df)*100:.1f}%)")
            
        else:
            print("  No prescription events found!")
            out_of_thin_air_count = 0

        # Step 5: Create both histograms
        print("\nStep 5: Creating histograms...")
        if len(final_df) > 0:
            histogram_data_with_missing = final_df['unique_npi_count'].value_counts().sort_index()
            histogram_data_real_only = final_df['unique_real_npi_count'].value_counts().sort_index()
            total_events = len(final_df)

            # Create dual histogram plot
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
            
            # Histogram 1: Missing counts as distinct
            bars1 = ax1.bar(histogram_data_with_missing.index, histogram_data_with_missing.values, 
                           color='lightblue', edgecolor='black', alpha=0.7)
            ax1.set_xlabel('Number of Unique NPIs per Prescription Event')
            ax1.set_ylabel('Frequency')
            ax1.set_title('Method 1: Missing Provider = 1 Distinct NPI\n(Any Healthcare Contact)')
            ax1.grid(True, alpha=0.3)
            
            # Add percentage labels for first histogram
            for bar, (npi_count, freq) in zip(bars1, histogram_data_with_missing.items()):
                if npi_count <= 10:  # Only label first 10 bars to avoid clutter
                    pct = (freq / total_events) * 100
                    ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height() + max(histogram_data_with_missing.values) * 0.01, 
                            f'{pct:.1f}%', ha='center', va='bottom', fontsize=8)
            
            # Histogram 2: Only real NPIs
            bars2 = ax2.bar(histogram_data_real_only.index, histogram_data_real_only.values, 
                           color='lightcoral', edgecolor='black', alpha=0.7)
            ax2.set_xlabel('Number of Unique NPIs per Prescription Event')
            ax2.set_ylabel('Frequency')
            ax2.set_title('Method 2: Only Real NPIs Count\n(Identifiable Providers Only)')
            ax2.grid(True, alpha=0.3)
            
            # Add percentage labels for second histogram
            for bar, (npi_count, freq) in zip(bars2, histogram_data_real_only.items()):
                if npi_count <= 10:  # Only label first 10 bars to avoid clutter
                    pct = (freq / total_events) * 100
                    ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height() + max(histogram_data_real_only.values) * 0.01, 
                            f'{pct:.1f}%', ha='center', va='bottom', fontsize=8)
            
            plt.tight_layout()
            histogram_file = f'npi_histogram_comparison_{TEST_PATIENT_LIMIT//1000}k_{year}.png'
            plt.savefig(histogram_file, dpi=300, bbox_inches='tight')
            print(f"  Dual histogram saved as: {histogram_file}")
            plt.show()
        else:
            print("  No data to plot histogram")

        total_time = time.time() - total_start_time
        
        print(f"\n{'='*60}")
        print(f"TRULY NEW PRESCRIPTIONS ANALYSIS COMPLETE - {total_time:.1f} seconds")
        print(f"{'='*60}")
        print(f"Patients processed: {total_patients:,}")
        print(f"Prescription events: {len(final_df):,}")
        print(f"Processing rate: {total_patients/total_time:,.0f} patients/second")
        
        if len(final_df) > 0:
            print(f"Events per patient: {len(final_df)/total_patients:.2f}")
            print(f"'Out of thin air' prescriptions: {out_of_thin_air_count:,}")
            
        print(f"{'='*60}")

        return {
            'patients_processed': total_patients,
            'prescription_events': len(final_df),
            'total_time': total_time,
            'events_per_patient': len(final_df)/total_patients if total_patients > 0 else 0,
            'null_npi_events': out_of_thin_air_count if len(final_df) > 0 else 0
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
    print("Counting string 'NULL' as a distinct NPI value")
    print("=" * 60)
    
    result = test_truly_new_prescriptions(2018)
    
    if result:
        print(f"\n🎉 Truly new prescriptions analysis completed!")
        print(f"Events per patient: {result['events_per_patient']:.2f}")
        print(f"'Out of thin air' prescriptions: {result['null_npi_events']:,}")
        print(f"This analysis counts string 'NULL' as a distinct NPI!")
    else:
        print("\n❌ Analysis failed")

if __name__ == "__main__":
    main()
