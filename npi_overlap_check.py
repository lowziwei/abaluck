import duckdb
import pandas as pd
import time
import gc
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np

# Configuration
START_YEAR = 2018
END_YEAR = 2018
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
TEST_PATIENT_LIMIT = 50000  # REDUCED: First 50k patients (was 200k) to save memory

# Memory management settings
CHUNK_SIZE = 10000  # Process prescriptions in chunks to manage memory

def load_truly_new_prescriptions():
    """
    Load prescriptions with FLAG = NULL (truly new) from the 12 flag files
    All records are already REFILL = 0, so just filter for FLAG = NULL
    MEMORY OPTIMIZED: Process files one at a time and only keep essential columns
    """
    print("Loading truly new prescriptions (FLAG = NULL) from flag files...")
    
    all_truly_new = []
    
    for i in range(1, 13):  # 12 files
        file_path = f"prescription_flags_2018_{DATABASE}_D_part{i:02d}.parquet"
        
        if Path(file_path).exists():
            print(f"  Loading {file_path}...")
            
            # MEMORY OPTIMIZATION: Read only needed columns
            try:
                df = pd.read_parquet(file_path, columns=['ENROLID', 'SVCDATE', 'FLAG'])
            except:
                # Fallback if columns parameter doesn't work
                df = pd.read_parquet(file_path)
                df = df[['ENROLID', 'SVCDATE', 'FLAG']].copy()
            
            # Filter for truly new prescriptions (FLAG is NULL)
            truly_new = df[df['FLAG'].isna()].copy()
            
            if len(truly_new) > 0:
                # Keep only needed columns for NPI matching
                truly_new_subset = truly_new[['ENROLID', 'SVCDATE']].copy()
                all_truly_new.append(truly_new_subset)
                
            print(f"    Total records: {len(df):,}")
            print(f"    Truly new (FLAG=NULL): {len(truly_new):,}")
            
            # MEMORY CLEANUP: Delete DataFrames immediately after use
            del df, truly_new
            gc.collect()
            
        else:
            print(f"  File {file_path} not found!")
    
    if not all_truly_new:
        print("No truly new prescriptions found!")
        return pd.DataFrame()
    
    # Combine all truly new prescriptions
    combined_df = pd.concat(all_truly_new, ignore_index=True)
    print(f"\nTotal truly new prescriptions loaded: {len(combined_df):,}")
    
    # MEMORY CLEANUP
    del all_truly_new
    gc.collect()
    
    return combined_df

def analyze_npi_provid_availability(year):
    """
    Analyze NPI and PROVID availability in inpatient and outpatient data separately
    """
    print(f"\n{'='*80}")
    print(f"NPI AND PROVID AVAILABILITY ANALYSIS - YEAR {year}")
    print(f"{'='*80}")

    # File paths
    o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"
    s_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_S.parquet"  # Not partitioned by year

    # Verify files exist
    for file_type, file_path in [('Outpatient', o_file), ('Inpatient', s_file)]:
        if not Path(file_path).exists():
            print(f"ERROR: Missing {file_type} file: {file_path}")
            return None

    # Initialize DuckDB with memory constraints
    conn = duckdb.connect()
    conn.execute("SET memory_limit='25GB'")  # Leave 5GB for Python/OS
    conn.execute("SET threads=2")  # Reduce threads to save memory
    conn.execute("SET temp_directory='/tmp'")  # Use disk for temporary storage
    
    total_start_time = time.time()

    try:
        # Step 1: Get the same first 200k patients as the original analysis
        print(f"\nStep 1: Getting the same first {TEST_PATIENT_LIMIT:,} patients...")
        step1_start = time.time()
        
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
        
        # Convert to format for SQL IN clause  
        enrolid_list = "', '".join(map(str, test_enrolids))

        # Step 2: Analyze OUTPATIENT data NPI and PROVID availability
        print(f"\nStep 2: Analyzing OUTPATIENT data NPI and PROVID availability...")
        step2_start = time.time()
        
        outpatient_analysis_query = f"""
        SELECT 
            -- NPI Analysis
            CASE 
                WHEN NPI IS NULL THEN 'NPI_SQL_NULL'
                WHEN UPPER(TRIM(CAST(NPI as VARCHAR))) = 'NULL' THEN 'NPI_STRING_NULL'
                WHEN UPPER(TRIM(CAST(NPI as VARCHAR))) = '' THEN 'NPI_EMPTY_STRING'
                WHEN LENGTH(TRIM(CAST(NPI as VARCHAR))) = 0 THEN 'NPI_ZERO_LENGTH'
                ELSE 'NPI_REAL_VALUE'
            END as npi_status,
            
            -- PROVID Analysis  
            CASE 
                WHEN PROVID IS NULL THEN 'PROVID_SQL_NULL'
                WHEN UPPER(TRIM(CAST(PROVID as VARCHAR))) = 'NULL' THEN 'PROVID_STRING_NULL'
                WHEN UPPER(TRIM(CAST(PROVID as VARCHAR))) = '' THEN 'PROVID_EMPTY_STRING'
                WHEN LENGTH(TRIM(CAST(PROVID as VARCHAR))) = 0 THEN 'PROVID_ZERO_LENGTH'
                ELSE 'PROVID_REAL_VALUE'
            END as provid_status,
            
            -- Combined Analysis
            CASE 
                WHEN (NPI IS NOT NULL AND UPPER(TRIM(CAST(NPI as VARCHAR))) != 'NULL' AND TRIM(CAST(NPI as VARCHAR)) != '')
                     AND (PROVID IS NOT NULL AND UPPER(TRIM(CAST(PROVID as VARCHAR))) != 'NULL' AND TRIM(CAST(PROVID as VARCHAR)) != '')
                THEN 'BOTH_AVAILABLE'
                WHEN (NPI IS NOT NULL AND UPPER(TRIM(CAST(NPI as VARCHAR))) != 'NULL' AND TRIM(CAST(NPI as VARCHAR)) != '')
                     AND (PROVID IS NULL OR UPPER(TRIM(CAST(PROVID as VARCHAR))) = 'NULL' OR TRIM(CAST(PROVID as VARCHAR)) = '')
                THEN 'ONLY_NPI'
                WHEN (NPI IS NULL OR UPPER(TRIM(CAST(NPI as VARCHAR))) = 'NULL' OR TRIM(CAST(NPI as VARCHAR)) = '')
                     AND (PROVID IS NOT NULL AND UPPER(TRIM(CAST(PROVID as VARCHAR))) != 'NULL' AND TRIM(CAST(PROVID as VARCHAR)) != '')
                THEN 'ONLY_PROVID'
                ELSE 'NEITHER_AVAILABLE'
            END as availability_status,
            
            COUNT(*) as record_count
        FROM '{o_file}'
        WHERE ENROLID IN ('{enrolid_list}')
          AND ENROLID IS NOT NULL 
          AND SVCDATE IS NOT NULL
        GROUP BY npi_status, provid_status, availability_status
        ORDER BY record_count DESC
        """
        
        outpatient_results = conn.execute(outpatient_analysis_query).fetchdf()
        step2_time = time.time() - step2_start
        
        print(f"  Outpatient analysis completed in {step2_time:.2f} seconds")
        print(f"  Found {outpatient_results['record_count'].sum():,} outpatient records")

        # Step 3: Analyze INPATIENT data NPI and PROVID availability
        print(f"\nStep 3: Analyzing INPATIENT data NPI and PROVID availability...")
        step3_start = time.time()
        
        inpatient_analysis_query = f"""
        SELECT 
            -- NPI Analysis
            CASE 
                WHEN NPI IS NULL THEN 'NPI_SQL_NULL'
                WHEN UPPER(TRIM(CAST(NPI as VARCHAR))) = 'NULL' THEN 'NPI_STRING_NULL'
                WHEN UPPER(TRIM(CAST(NPI as VARCHAR))) = '' THEN 'NPI_EMPTY_STRING'
                WHEN LENGTH(TRIM(CAST(NPI as VARCHAR))) = 0 THEN 'NPI_ZERO_LENGTH'
                ELSE 'NPI_REAL_VALUE'
            END as npi_status,
            
            -- PROVID Analysis  
            CASE 
                WHEN PROVID IS NULL THEN 'PROVID_SQL_NULL'
                WHEN UPPER(TRIM(CAST(PROVID as VARCHAR))) = 'NULL' THEN 'PROVID_STRING_NULL'
                WHEN UPPER(TRIM(CAST(PROVID as VARCHAR))) = '' THEN 'PROVID_EMPTY_STRING'
                WHEN LENGTH(TRIM(CAST(PROVID as VARCHAR))) = 0 THEN 'PROVID_ZERO_LENGTH'
                ELSE 'PROVID_REAL_VALUE'
            END as provid_status,
            
            -- Combined Analysis
            CASE 
                WHEN (NPI IS NOT NULL AND UPPER(TRIM(CAST(NPI as VARCHAR))) != 'NULL' AND TRIM(CAST(NPI as VARCHAR)) != '')
                     AND (PROVID IS NOT NULL AND UPPER(TRIM(CAST(PROVID as VARCHAR))) != 'NULL' AND TRIM(CAST(PROVID as VARCHAR)) != '')
                THEN 'BOTH_AVAILABLE'
                WHEN (NPI IS NOT NULL AND UPPER(TRIM(CAST(NPI as VARCHAR))) != 'NULL' AND TRIM(CAST(NPI as VARCHAR)) != '')
                     AND (PROVID IS NULL OR UPPER(TRIM(CAST(PROVID as VARCHAR))) = 'NULL' OR TRIM(CAST(PROVID as VARCHAR)) = '')
                THEN 'ONLY_NPI'
                WHEN (NPI IS NULL OR UPPER(TRIM(CAST(NPI as VARCHAR))) = 'NULL' OR TRIM(CAST(NPI as VARCHAR)) = '')
                     AND (PROVID IS NOT NULL AND UPPER(TRIM(CAST(PROVID as VARCHAR))) != 'NULL' AND TRIM(CAST(PROVID as VARCHAR)) != '')
                THEN 'ONLY_PROVID'
                ELSE 'NEITHER_AVAILABLE'
            END as availability_status,
            
            COUNT(*) as record_count
        FROM '{s_file}'
        WHERE ENROLID IN ('{enrolid_list}')
          AND ENROLID IS NOT NULL 
          AND SVCDATE IS NOT NULL
          AND YEAR = {year}
        GROUP BY npi_status, provid_status, availability_status
        ORDER BY record_count DESC
        """
        
        inpatient_results = conn.execute(inpatient_analysis_query).fetchdf()
        step3_time = time.time() - step3_start
        
        print(f"  Inpatient analysis completed in {step3_time:.2f} seconds")
        print(f"  Found {inpatient_results['record_count'].sum():,} inpatient records")

        # Step 4: Load truly new prescriptions
        print(f"\nStep 4: Loading truly new prescriptions...")
        step4_start = time.time()
        
        truly_new_df = load_truly_new_prescriptions()
        
        if len(truly_new_df) == 0:
            print("No truly new prescriptions found!")
            return None
        
        # Filter to only include the same patients from Step 1
        test_prescriptions = truly_new_df[truly_new_df['ENROLID'].isin(test_enrolids)].copy()
        
        step4_time = time.time() - step4_start
        print(f"  Loaded {len(test_prescriptions):,} truly new prescriptions for test patients")
        print(f"  Step 4 completed in {step4_time:.2f} seconds")

        # Step 5: Prescription matching analysis with NPI vs PROVID
        print(f"\nStep 5: Prescription matching analysis (NPI vs PROVID)...")
        step5_start = time.time()
        
        # Convert prescriptions to temporary table
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

        # Prescription matching query - comparing NPI vs PROVID effectiveness
        prescription_matching_query = f"""
        WITH 
        -- Get outpatient data for test patients and year
        chunk_outpatient AS (
            SELECT ENROLID, SVCDATE, NPI, PROVID, 'Outpatient' as source_file
            FROM '{o_file}'
            WHERE ENROLID IN ('{enrolid_list}')
              AND ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL
        ),
        -- Get inpatient data for test patients and year
        chunk_inpatient AS (
            SELECT ENROLID, SVCDATE, NPI, PROVID, 'Inpatient' as source_file
            FROM '{s_file}'
            WHERE ENROLID IN ('{enrolid_list}')
              AND ENROLID IS NOT NULL 
              AND SVCDATE IS NOT NULL
              AND YEAR = {year}
        ),
        -- Combine outpatient and inpatient data
        combined_visits AS (
            SELECT ENROLID, SVCDATE, NPI, PROVID, source_file FROM chunk_outpatient
            UNION ALL
            SELECT ENROLID, SVCDATE, NPI, PROVID, source_file FROM chunk_inpatient
        ),
        -- Merge prescription with combined visits (±30 days)
        matched_visits AS (
            SELECT 
                p.ENROLID,
                p.prescription_date,
                c.SVCDATE as visit_date,
                c.NPI,
                c.PROVID,
                c.source_file
            FROM test_prescriptions p
            LEFT JOIN combined_visits c 
                ON p.ENROLID = c.ENROLID 
                AND c.SVCDATE BETWEEN (p.prescription_date::DATE - INTERVAL 30 DAY) 
                                  AND (p.prescription_date::DATE + INTERVAL 30 DAY)
        )
        -- Count providers using different methods
        SELECT 
            ENROLID,
            prescription_date as SVCDATE,
            
            -- Method 1: Count unique NPIs (treating missing as distinct)
            CASE 
                WHEN COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) = 0 THEN 0
                ELSE COUNT(DISTINCT COALESCE(CAST(NPI as VARCHAR), 'MISSING_NPI'))
            END as unique_npi_count_with_missing,
            
            -- Method 2: Count only real NPIs
            COUNT(DISTINCT CASE 
                WHEN visit_date IS NOT NULL AND NPI IS NOT NULL 
                     AND UPPER(TRIM(CAST(NPI as VARCHAR))) != 'NULL'
                     AND TRIM(CAST(NPI as VARCHAR)) != ''
                THEN CAST(NPI as VARCHAR)
            END) as unique_real_npi_count,
            
            -- Method 3: Count unique PROVIDs (treating missing as distinct)
            CASE 
                WHEN COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) = 0 THEN 0
                ELSE COUNT(DISTINCT COALESCE(CAST(PROVID as VARCHAR), 'MISSING_PROVID'))
            END as unique_provid_count_with_missing,
            
            -- Method 4: Count only real PROVIDs
            COUNT(DISTINCT CASE 
                WHEN visit_date IS NOT NULL AND PROVID IS NOT NULL 
                     AND UPPER(TRIM(CAST(PROVID as VARCHAR))) != 'NULL'
                     AND TRIM(CAST(PROVID as VARCHAR)) != ''
                THEN CAST(PROVID as VARCHAR)
            END) as unique_real_provid_count,
            
            -- Method 5: Combined approach - use NPI when available, fall back to PROVID
            COUNT(DISTINCT CASE 
                WHEN visit_date IS NOT NULL THEN
                    CASE 
                        WHEN NPI IS NOT NULL AND UPPER(TRIM(CAST(NPI as VARCHAR))) != 'NULL' AND TRIM(CAST(NPI as VARCHAR)) != ''
                        THEN CONCAT('NPI_', CAST(NPI as VARCHAR))
                        WHEN PROVID IS NOT NULL AND UPPER(TRIM(CAST(PROVID as VARCHAR))) != 'NULL' AND TRIM(CAST(PROVID as VARCHAR)) != ''
                        THEN CONCAT('PROVID_', CAST(PROVID as VARCHAR))
                        ELSE 'NO_PROVIDER_ID'
                    END
            END) as unique_combined_provider_count,
            
            -- Diagnostic information
            COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) as total_visits,
            COUNT(CASE WHEN visit_date IS NOT NULL AND NPI IS NOT NULL AND UPPER(TRIM(CAST(NPI as VARCHAR))) != 'NULL' AND TRIM(CAST(NPI as VARCHAR)) != '' THEN 1 END) as visits_with_real_npi,
            COUNT(CASE WHEN visit_date IS NOT NULL AND PROVID IS NOT NULL AND UPPER(TRIM(CAST(PROVID as VARCHAR))) != 'NULL' AND TRIM(CAST(PROVID as VARCHAR)) != '' THEN 1 END) as visits_with_real_provid,
            COUNT(CASE WHEN visit_date IS NOT NULL AND 
                       (NPI IS NULL OR UPPER(TRIM(CAST(NPI as VARCHAR))) = 'NULL' OR TRIM(CAST(NPI as VARCHAR)) = '') AND
                       (PROVID IS NULL OR UPPER(TRIM(CAST(PROVID as VARCHAR))) = 'NULL' OR TRIM(CAST(PROVID as VARCHAR)) = '')
                  THEN 1 END) as visits_with_no_provider_id,
            
            -- Key indicator: "out of thin air" = no visits at all  
            CASE WHEN COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) = 0 THEN 1 ELSE 0 END as out_of_thin_air
        FROM matched_visits
        GROUP BY ENROLID, prescription_date
        ORDER BY ENROLID, prescription_date
        """

        prescription_results = conn.execute(prescription_matching_query).fetchdf()
        step5_time = time.time() - step5_start
        
        print(f"  Processed {len(prescription_results):,} prescription events in {step5_time:.1f} seconds")

        # Step 6: Generate comprehensive analysis and visualizations
        print(f"\nStep 6: Generating analysis and visualizations...")
        
        # Print detailed results
        print(f"\n{'='*80}")
        print(f"DETAILED ANALYSIS RESULTS")
        print(f"{'='*80}")
        
        # Outpatient results
        print(f"\nOUTPATIENT DATA ANALYSIS:")
        print(f"Total outpatient records: {outpatient_results['record_count'].sum():,}")
        
        # Aggregate outpatient by availability status
        outpatient_summary = outpatient_results.groupby('availability_status')['record_count'].sum().sort_values(ascending=False)
        for status, count in outpatient_summary.items():
            pct = (count / outpatient_results['record_count'].sum()) * 100
            print(f"  {status}: {count:,} ({pct:.1f}%)")
        
        # Inpatient results
        print(f"\nINPATIENT DATA ANALYSIS:")
        print(f"Total inpatient records: {inpatient_results['record_count'].sum():,}")
        
        # Aggregate inpatient by availability status
        inpatient_summary = inpatient_results.groupby('availability_status')['record_count'].sum().sort_values(ascending=False)
        for status, count in inpatient_summary.items():
            pct = (count / inpatient_results['record_count'].sum()) * 100
            print(f"  {status}: {count:,} ({pct:.1f}%)")
        
        # Prescription matching results
        if len(prescription_results) > 0:
            print(f"\nPRESCRIPTION MATCHING ANALYSIS:")
            print(f"Total prescription events analyzed: {len(prescription_results):,}")
            
            # Create histograms for different counting methods
            methods = {
                'NPI Only (Real Values)': 'unique_real_npi_count',
                'PROVID Only (Real Values)': 'unique_real_provid_count',
                'Combined NPI+PROVID': 'unique_combined_provider_count'
            }
            
            print(f"\nProvider Count Distributions:")
            for method_name, column in methods.items():
                dist = prescription_results[column].value_counts().sort_index()
                zero_count = dist.get(0, 0)
                zero_pct = (zero_count / len(prescription_results)) * 100
                print(f"\n{method_name}:")
                print(f"  0 Providers: {zero_count:,} ({zero_pct:.1f}%)")
                for provider_count in [1, 2, 3, 4, 5]:
                    count = dist.get(provider_count, 0)
                    pct = (count / len(prescription_results)) * 100
                    if count > 0:
                        print(f"  {provider_count} Provider(s): {count:,} ({pct:.1f}%)")
            
            # Out of thin air analysis
            out_of_thin_air_count = prescription_results['out_of_thin_air'].sum()
            print(f"\n'Out of thin air' prescriptions (no visits found): {out_of_thin_air_count:,} ({out_of_thin_air_count/len(prescription_results)*100:.1f}%)")
            
            # Provider ID coverage analysis
            total_visits = prescription_results['total_visits'].sum()
            visits_with_npi = prescription_results['visits_with_real_npi'].sum()
            visits_with_provid = prescription_results['visits_with_real_provid'].sum()
            visits_with_no_id = prescription_results['visits_with_no_provider_id'].sum()
            
            print(f"\nProvider ID Coverage in Visits:")
            print(f"  Total visits found: {total_visits:,}")
            print(f"  Visits with real NPI: {visits_with_npi:,} ({visits_with_npi/total_visits*100:.1f}%)")
            print(f"  Visits with real PROVID: {visits_with_provid:,} ({visits_with_provid/total_visits*100:.1f}%)")
            print(f"  Visits with no provider ID: {visits_with_no_id:,} ({visits_with_no_id/total_visits*100:.1f}%)")
            
            # PROVID as complement analysis
            visits_npi_only = 0
            visits_provid_only = 0
            visits_both = 0
            visits_neither = 0
            
            for _, row in prescription_results.iterrows():
                if row['visits_with_real_npi'] > 0 and row['visits_with_real_provid'] > 0:
                    visits_both += 1
                elif row['visits_with_real_npi'] > 0:
                    visits_npi_only += 1
                elif row['visits_with_real_provid'] > 0:
                    visits_provid_only += 1
                else:
                    visits_neither += 1
            
            total_prescriptions = len(prescription_results)
            print(f"\nPROVID as Complement to NPI (Prescription Level):")
            print(f"  Prescriptions with both NPI and PROVID visits: {visits_both:,} ({visits_both/total_prescriptions*100:.1f}%)")
            print(f"  Prescriptions with only NPI visits: {visits_npi_only:,} ({visits_npi_only/total_prescriptions*100:.1f}%)")
            print(f"  Prescriptions with only PROVID visits: {visits_provid_only:,} ({visits_provid_only/total_prescriptions*100:.1f}%)")
            print(f"  Prescriptions with neither: {visits_neither:,} ({visits_neither/total_prescriptions*100:.1f}%)")
            
            improvement = visits_provid_only
            print(f"\nPROVID adds {improvement:,} prescriptions ({improvement/total_prescriptions*100:.1f}%) that would have 0 providers with NPI alone")

        # Create comprehensive visualization
        create_comprehensive_visualizations(outpatient_results, inpatient_results, prescription_results, year)
        
        total_time = time.time() - total_start_time
        
        print(f"\n{'='*80}")
        print(f"ANALYSIS COMPLETE - {total_time:.1f} seconds")
        print(f"{'='*80}")
        
        return {
            'outpatient_results': outpatient_results,
            'inpatient_results': inpatient_results,
            'prescription_results': prescription_results,
            'total_time': total_time
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

def create_comprehensive_visualizations(outpatient_results, inpatient_results, prescription_results, year):
    """
    Create comprehensive visualizations for NPI and PROVID analysis
    """
    print("Creating comprehensive visualizations...")
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create a large figure with multiple subplots
    fig = plt.figure(figsize=(24, 16))
    
    # 1. Provider ID Availability Comparison (Outpatient vs Inpatient)
    ax1 = plt.subplot(2, 3, 1)
    
    # Prepare data for comparison
    outpatient_summary = outpatient_results.groupby('availability_status')['record_count'].sum()
    inpatient_summary = inpatient_results.groupby('availability_status')['record_count'].sum()
    
    # Normalize to percentages
    outpatient_pct = (outpatient_summary / outpatient_summary.sum() * 100)
    inpatient_pct = (inpatient_summary / inpatient_summary.sum() * 100)
    
    # Create comparison bar plot
    categories = ['BOTH_AVAILABLE', 'ONLY_NPI', 'ONLY_PROVID', 'NEITHER_AVAILABLE']
    x = np.arange(len(categories))
    width = 0.35
    
    outpatient_values = [outpatient_pct.get(cat, 0) for cat in categories]
    inpatient_values = [inpatient_pct.get(cat, 0) for cat in categories]
    
    bars1 = ax1.bar(x - width/2, outpatient_values, width, label='Outpatient', alpha=0.8)
    bars2 = ax1.bar(x + width/2, inpatient_values, width, label='Inpatient', alpha=0.8)
    
    ax1.set_xlabel('Provider ID Availability')
    ax1.set_ylabel('Percentage of Records')
    ax1.set_title('Provider ID Availability: Outpatient vs Inpatient')
    ax1.set_xticks(x)
    ax1.set_xticklabels([cat.replace('_', '\n') for cat in categories], rotation=0, ha='center')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Add percentage labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5, 
                        f'{height:.1f}%', ha='center', va='bottom', fontsize=8)
    
    # 2. NPI Missing Frequency
    ax2 = plt.subplot(2, 3, 2)
    
    # Combine outpatient and inpatient NPI status
    all_npi_status = pd.concat([
        outpatient_results.groupby('npi_status')['record_count'].sum().reset_index().assign(source='Outpatient'),
        inpatient_results.groupby('npi_status')['record_count'].sum().reset_index().assign(source='Inpatient')
    ])
    
    # Create grouped bar chart for NPI status
    npi_pivot = all_npi_status.pivot(index='npi_status', columns='source', values='record_count').fillna(0)
    npi_pivot_pct = npi_pivot.div(npi_pivot.sum()) * 100
    
    npi_pivot_pct.plot(kind='bar', ax=ax2, alpha=0.8)
    ax2.set_title('NPI Missing Frequency')
    ax2.set_xlabel('NPI Status')
    ax2.set_ylabel('Percentage of Records')
    ax2.legend(title='Data Source')
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(True, alpha=0.3)
    
    # 3. PROVID Missing Frequency
    ax3 = plt.subplot(2, 3, 3)
    
    # Combine outpatient and inpatient PROVID status
    all_provid_status = pd.concat([
        outpatient_results.groupby('provid_status')['record_count'].sum().reset_index().assign(source='Outpatient'),
        inpatient_results.groupby('provid_status')['record_count'].sum().reset_index().assign(source='Inpatient')
    ])
    
    # Create grouped bar chart for PROVID status
    provid_pivot = all_provid_status.pivot(index='provid_status', columns='source', values='record_count').fillna(0)
    provid_pivot_pct = provid_pivot.div(provid_pivot.sum()) * 100
    
    provid_pivot_pct.plot(kind='bar', ax=ax3, alpha=0.8)
    ax3.set_title('PROVID Missing Frequency')
    ax3.set_xlabel('PROVID Status')
    ax3.set_ylabel('Percentage of Records')
    ax3.legend(title='Data Source')
    ax3.tick_params(axis='x', rotation=45)
    ax3.grid(True, alpha=0.3)
    
    # 4. Prescription Matching: Provider Count Distributions
    if len(prescription_results) > 0:
        ax4 = plt.subplot(2, 3, 4)
        
        # Compare different provider counting methods
        methods = {
            'NPI Only': 'unique_real_npi_count',
            'PROVID Only': 'unique_real_provid_count',
            'Combined': 'unique_combined_provider_count'
        }
        
        # Create histogram data for different methods
        max_providers = 5  # Show up to 5 providers
        provider_counts = range(0, max_providers + 1)
        
        method_data = []
        for method_name, column in methods.items():
            dist = prescription_results[column].value_counts().sort_index()
            percentages = []
            for count in provider_counts:
                pct = (dist.get(count, 0) / len(prescription_results)) * 100
                percentages.append(pct)
            method_data.append(percentages)
        
        # Create grouped bar chart
        x = np.arange(len(provider_counts))
        width = 0.25
        
        for i, (method_name, percentages) in enumerate(zip(methods.keys(), method_data)):
            ax4.bar(x + i*width, percentages, width, label=method_name, alpha=0.8)
        
        ax4.set_xlabel('Number of Unique Providers per Prescription')
        ax4.set_ylabel('Percentage of Prescriptions')
        ax4.set_title('Provider Count Distribution by Method')
        ax4.set_xticks(x + width)
        ax4.set_xticklabels([f'{count}' if count < max_providers else f'{count}+' for count in provider_counts])
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        # 5. PROVID Complement Analysis
        ax5 = plt.subplot(2, 3, 5)
        
        # Calculate prescription-level provider availability
        visits_both = ((prescription_results['visits_with_real_npi'] > 0) & 
                      (prescription_results['visits_with_real_provid'] > 0)).sum()
        visits_npi_only = ((prescription_results['visits_with_real_npi'] > 0) & 
                          (prescription_results['visits_with_real_provid'] == 0)).sum()
        visits_provid_only = ((prescription_results['visits_with_real_npi'] == 0) & 
                             (prescription_results['visits_with_real_provid'] > 0)).sum()
        visits_neither = ((prescription_results['visits_with_real_npi'] == 0) & 
                         (prescription_results['visits_with_real_provid'] == 0)).sum()
        
        # Create pie chart
        sizes = [visits_both, visits_npi_only, visits_provid_only, visits_neither]
        labels = ['Both NPI & PROVID', 'Only NPI', 'Only PROVID', 'Neither']
        colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99']
        
        wedges, texts, autotexts = ax5.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', 
                                          startangle=90, textprops={'fontsize': 10})
        ax5.set_title('Prescription-Level Provider ID Availability')
        
        # 6. Provider Coverage Improvement
        ax6 = plt.subplot(2, 3, 6)
        
        # Calculate improvement metrics
        total_prescriptions = len(prescription_results)
        npi_only_zero = (prescription_results['unique_real_npi_count'] == 0).sum()
        combined_zero = (prescription_results['unique_combined_provider_count'] == 0).sum()
        improvement = npi_only_zero - combined_zero
        
        categories = ['NPI Only', 'NPI + PROVID\n(Combined)']
        zero_provider_counts = [npi_only_zero, combined_zero]
        zero_provider_pcts = [(count / total_prescriptions) * 100 for count in zero_provider_counts]
        
        bars = ax6.bar(categories, zero_provider_pcts, 
                      color=['lightcoral', 'lightgreen'], alpha=0.8)
        ax6.set_ylabel('Percentage of Prescriptions with 0 Providers')
        ax6.set_title('Provider Matching Improvement\nwith PROVID Complement')
        ax6.grid(True, alpha=0.3)
        
        # Add value labels and improvement annotation
        for bar, count, pct in zip(bars, zero_provider_counts, zero_provider_pcts):
            ax6.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.5, 
                    f'{count:,}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=10)
        
        # Add improvement annotation
        ax6.annotate(f'Improvement:\n{improvement:,} prescriptions\n({improvement/total_prescriptions*100:.1f}%)', 
                    xy=(0.5, max(zero_provider_pcts) * 0.7), xycoords='data',
                    ha='center', va='center', fontsize=11, 
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7))
    
    plt.tight_layout()
    
    # Save the comprehensive plot
    filename = f'npi_provid_comprehensive_analysis_{TEST_PATIENT_LIMIT//1000}k_{year}.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Comprehensive analysis plot saved as: {filename}")
    plt.show()
    
    # Create additional detailed plots if prescription data exists
    if len(prescription_results) > 0:
        create_detailed_prescription_plots(prescription_results, year)

def create_detailed_prescription_plots(prescription_results, year):
    """
    Create detailed plots focusing on prescription matching patterns
    """
    print("Creating detailed prescription matching plots...")
    
    # Create figure for detailed prescription analysis
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Distribution of total visits per prescription
    visit_dist = prescription_results['total_visits'].value_counts().sort_index()
    max_visits = min(10, visit_dist.index.max())  # Show up to 10 visits
    
    visit_counts = []
    visit_labels = []
    for i in range(max_visits + 1):
        if i < max_visits:
            count = visit_dist.get(i, 0)
            visit_counts.append(count)
            visit_labels.append(str(i))
        else:
            # Aggregate all visits >= max_visits
            count = visit_dist[visit_dist.index >= max_visits].sum()
            visit_counts.append(count)
            visit_labels.append(f'{max_visits}+')
    
    bars1 = ax1.bar(visit_labels, visit_counts, color='skyblue', alpha=0.8)
    ax1.set_xlabel('Number of Visits per Prescription (±30 days)')
    ax1.set_ylabel('Number of Prescriptions')
    ax1.set_title('Distribution of Healthcare Visits per Prescription')
    ax1.grid(True, alpha=0.3)
    
    # Add percentage labels
    total_prescriptions = len(prescription_results)
    for bar, count in zip(bars1, visit_counts):
        if count > 0:
            pct = (count / total_prescriptions) * 100
            ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height() + max(visit_counts) * 0.01, 
                    f'{pct:.1f}%', ha='center', va='bottom', fontsize=9)
    
    # 2. Provider ID coverage in visits
    total_visits = prescription_results['total_visits'].sum()
    visits_with_npi = prescription_results['visits_with_real_npi'].sum()
    visits_with_provid = prescription_results['visits_with_real_provid'].sum()
    visits_with_no_id = prescription_results['visits_with_no_provider_id'].sum()
    
    coverage_data = {
        'Real NPI': visits_with_npi,
        'Real PROVID': visits_with_provid,
        'No Provider ID': visits_with_no_id
    }
    
    bars2 = ax2.bar(coverage_data.keys(), coverage_data.values(), 
                   color=['lightcoral', 'lightgreen', 'lightgray'], alpha=0.8)
    ax2.set_ylabel('Number of Visits')
    ax2.set_title('Provider ID Coverage in Healthcare Visits')
    ax2.grid(True, alpha=0.3)
    
    # Add percentage labels
    for bar, (label, count) in zip(bars2, coverage_data.items()):
        if count > 0:
            pct = (count / total_visits) * 100
            ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height() + max(coverage_data.values()) * 0.01, 
                    f'{count:,}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=9)
    
    # 3. Scatter plot: NPI count vs PROVID count per prescription
    ax3.scatter(prescription_results['unique_real_npi_count'], 
               prescription_results['unique_real_provid_count'], 
               alpha=0.6, s=30)
    ax3.set_xlabel('Unique Real NPIs per Prescription')
    ax3.set_ylabel('Unique Real PROVIDs per Prescription')
    ax3.set_title('NPI vs PROVID Count Correlation')
    ax3.grid(True, alpha=0.3)
    
    # Add diagonal line for reference
    max_val = max(prescription_results['unique_real_npi_count'].max(), 
                  prescription_results['unique_real_provid_count'].max())
    ax3.plot([0, max_val], [0, max_val], 'r--', alpha=0.5, label='Equal counts')
    ax3.legend()
    
    # 4. Improvement analysis: Before and after adding PROVID
    improvement_data = []
    
    for npi_count in range(6):  # 0 to 5+ providers
        npi_only = (prescription_results['unique_real_npi_count'] == npi_count).sum()
        combined = (prescription_results['unique_combined_provider_count'] == npi_count).sum()
        
        improvement_data.append({
            'Provider Count': f'{npi_count}' if npi_count < 5 else '5+',
            'NPI Only': npi_only,
            'NPI + PROVID': combined
        })
    
    improvement_df = pd.DataFrame(improvement_data)
    
    x = np.arange(len(improvement_df))
    width = 0.35
    
    bars3 = ax4.bar(x - width/2, improvement_df['NPI Only'], width, 
                   label='NPI Only', alpha=0.8, color='lightcoral')
    bars4 = ax4.bar(x + width/2, improvement_df['NPI + PROVID'], width, 
                   label='NPI + PROVID Combined', alpha=0.8, color='lightgreen')
    
    ax4.set_xlabel('Number of Providers per Prescription')
    ax4.set_ylabel('Number of Prescriptions')
    ax4.set_title('Provider Count Distribution: Before vs After PROVID')
    ax4.set_xticks(x)
    ax4.set_xticklabels(improvement_df['Provider Count'])
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save detailed prescription plots
    filename = f'prescription_matching_detailed_{TEST_PATIENT_LIMIT//1000}k_{year}.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Detailed prescription matching plots saved as: {filename}")
    plt.show()

def main():
    print("MarketScan Analysis - NPI AND PROVID COMPREHENSIVE ANALYSIS")
    print("Analyzing truly new prescriptions (FLAG = NULL) for provider ID availability")
    print("Comparing NPI vs PROVID effectiveness for prescription-to-provider matching")
    print("=" * 80)
    
    result = analyze_npi_provid_availability(2018)
    
    if result:
        print(f"\n🎉 Comprehensive NPI and PROVID analysis completed!")
        print(f"Analysis time: {result['total_time']:.1f} seconds")
        
        # Generate summary recommendations
        print(f"\n{'='*80}")
        print(f"SUMMARY RECOMMENDATIONS")
        print(f"{'='*80}")
        
        if len(result['prescription_results']) > 0:
            prescription_df = result['prescription_results']
            
            # Calculate key metrics
            total_prescriptions = len(prescription_df)
            npi_only_zero = (prescription_df['unique_real_npi_count'] == 0).sum()
            combined_zero = (prescription_df['unique_combined_provider_count'] == 0).sum()
            improvement = npi_only_zero - combined_zero
            
            print(f"1. PROVID Complement Value:")
            print(f"   - Using NPI alone: {npi_only_zero:,} prescriptions ({npi_only_zero/total_prescriptions*100:.1f}%) have 0 providers")
            print(f"   - Using NPI + PROVID: {combined_zero:,} prescriptions ({combined_zero/total_prescriptions*100:.1f}%) have 0 providers")
            print(f"   - Improvement: {improvement:,} prescriptions ({improvement/total_prescriptions*100:.1f}%) gained provider matches")
            
            if improvement > 0:
                print(f"\n2. RECOMMENDATION: Use PROVID as complement to NPI")
                print(f"   - PROVID significantly improves provider matching capability")
                print(f"   - Reduces 'orphaned' prescriptions by {improvement/total_prescriptions*100:.1f} percentage points")
            else:
                print(f"\n2. RECOMMENDATION: NPI alone may be sufficient")
                print(f"   - PROVID provides minimal additional matching capability")
            
            # Provider coverage in visits
            total_visits = prescription_df['total_visits'].sum()
            visits_with_npi = prescription_df['visits_with_real_npi'].sum()
            visits_with_provid = prescription_df['visits_with_real_provid'].sum()
            
            print(f"\n3. Provider ID Coverage in Healthcare Visits:")
            print(f"   - NPI coverage: {visits_with_npi/total_visits*100:.1f}% of visits have real NPI")
            print(f"   - PROVID coverage: {visits_with_provid/total_visits*100:.1f}% of visits have real PROVID")
            
        # Data quality insights
        outpatient_df = result['outpatient_results']
        inpatient_df = result['inpatient_results']
        
        outpatient_summary = outpatient_df.groupby('availability_status')['record_count'].sum()
        inpatient_summary = inpatient_df.groupby('availability_status')['record_count'].sum()
        
        print(f"\n4. Data Quality Insights:")
        print(f"   Outpatient - Both IDs available: {outpatient_summary.get('BOTH_AVAILABLE', 0)/outpatient_summary.sum()*100:.1f}%")
        print(f"   Inpatient - Both IDs available: {inpatient_summary.get('BOTH_AVAILABLE', 0)/inpatient_summary.sum()*100:.1f}%")
        
        print(f"\n{'='*80}")
        
    else:
        print("\n❌ Analysis failed")

if __name__ == "__main__":
    main()
