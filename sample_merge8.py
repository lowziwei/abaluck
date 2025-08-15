import duckdb
import pandas as pd
import time
import gc
import matplotlib.pyplot as plt
from pathlib import Path
import glob
import os

# Configuration
START_YEAR = 2018
END_YEAR = 2024
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
PATIENT_CHUNK_SIZE = 200000  # Process 200k patients per chunk
OUTPUT_DIR = "histogram_results"  # Directory for saving histogram data

def get_all_patients_for_year(year):
    """
    Get all unique patients for a given year, ordered by ENROLID
    """
    print(f"  Getting all patients for year {year}...")
    
    d_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_D_{year}.parquet"
    
    if not Path(d_file).exists():
        print(f"    ERROR: Missing prescription file: {d_file}")
        return []
    
    conn = duckdb.connect()
    try:
        enrolid_query = f"""
            SELECT DISTINCT ENROLID
            FROM '{d_file}'
            WHERE ENROLID IS NOT NULL
            ORDER BY ENROLID
        """
        
        all_patients = conn.execute(enrolid_query).fetchdf()['ENROLID'].tolist()
        print(f"    Found {len(all_patients):,} total patients for {year}")
        return all_patients
        
    finally:
        conn.close()

def create_patient_chunks(all_patients, chunk_size):
    """
    Split patients into chunks of specified size
    """
    chunks = []
    for i in range(0, len(all_patients), chunk_size):
        chunk = all_patients[i:i + chunk_size]
        chunks.append(chunk)
    
    print(f"  Created {len(chunks)} chunks of {chunk_size:,} patients each")
    if len(chunks) > 0:
        print(f"  Last chunk size: {len(chunks[-1]):,} patients")
    
    return chunks

def save_histogram_data(year, chunk_num, final_df, output_dir):
    """
    Save histogram data as parquet file with unique_phys naming
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Create histogram data
    histogram_data = final_df['unique_phys_id_count'].value_counts().sort_index()
    total_events = len(final_df)
    
    # Convert to DataFrame for saving
    histogram_df = pd.DataFrame({
        'unique_phys': histogram_data.index,
        'prescription_events': histogram_data.values,
        'percentage': (histogram_data.values / total_events * 100).round(2)
    })
    
    # Add metadata
    histogram_df['year'] = year
    histogram_df['file_number'] = chunk_num
    histogram_df['total_events'] = total_events
    histogram_df['chunk_start_patient'] = (chunk_num - 1) * PATIENT_CHUNK_SIZE + 1
    histogram_df['chunk_end_patient'] = chunk_num * PATIENT_CHUNK_SIZE
    
    # Calculate summary stats
    out_of_thin_air = final_df['out_of_thin_air'].sum()
    zero_physicians = histogram_data.get(0, 0)
    
    histogram_df['out_of_thin_air_events'] = out_of_thin_air
    histogram_df['zero_physician_events'] = zero_physicians
    
    # Save as parquet with unique_phys naming
    filename = f"unique_phys_{year}_{chunk_num:02d}.parquet"
    filepath = os.path.join(output_dir, filename)
    histogram_df.to_parquet(filepath, compression='snappy')
    
    print(f"    💾 Saved histogram data: {filename}")
    return filepath, histogram_df
    """
    OPTIMIZED: Load prescriptions using simple math - no peeking needed!
    Each prescription chunk = 100k patients in ENROLID order
    For 200k test patients, we need chunks 01-02 only
    """
    print("Loading truly new prescriptions using simple chunk math...")
    
    if test_patient_set is not None:
        test_patient_set = set(test_patient_set)
        max_test_enrolid = max(test_patient_set)
        
        # Simple math: each chunk = 100k patients
        chunks_needed = (max_test_enrolid // 100000) + 1
        print(f"  🎯 Test patients go up to ENROLID {max_test_enrolid:,}")
        print(f"  📊 Need first {chunks_needed} prescription chunks (100k patients each)")
    else:
        chunks_needed = 12  # Process all chunks
        print("  📊 Processing all 12 prescription chunks")
    
    all_truly_new = []
    total_records_checked = 0
    total_truly_new_found = 0
    
    for i in range(1, min(chunks_needed + 1, 13)):  # chunks 01 to chunks_needed
        file_path = f"prescription_flags_2018_{DATABASE}_D_part{i:02d}.parquet"
        
        if not Path(file_path).exists():
            print(f"  ⚠️  File not found: {file_path}")
            continue
        
        print(f"  📥 Loading prescription chunk {i:02d} (patients {(i-1)*100000+1:,}-{i*100000:,})...")
        chunk_start = time.time()
        
        try:
            # Load chunk
            df = pd.read_parquet(file_path)
            total_records_checked += len(df)
            
            # Filter for truly new prescriptions (FLAG is NULL)
            truly_new = df[df['FLAG'].isna()].copy()
            
            # Filter to test patients if specified
            if test_patient_set:
                original_truly_new = len(truly_new)
                truly_new = truly_new[truly_new['ENROLID'].isin(test_patient_set)]
                print(f"      🔍 Filtered: {original_truly_new:,} → {len(truly_new):,} truly new prescriptions")
            
            if len(truly_new) > 0:
                # Keep only needed columns
                truly_new_subset = truly_new[['ENROLID', 'SVCDATE']].copy()
                all_truly_new.append(truly_new_subset)
                total_truly_new_found += len(truly_new_subset)
            
            chunk_time = time.time() - chunk_start
            print(f"      ✅ Found {len(truly_new):,} truly new prescriptions ({chunk_time:.2f}s)")
            
        except Exception as e:
            print(f"      💥 ERROR: {e}")
        
        # Clean up
        if 'df' in locals():
            del df
        if 'truly_new' in locals():
            del truly_new
        gc.collect()
    
    # Skip remaining chunks
    if chunks_needed < 12:
        print(f"  ⏩ SKIPPED chunks {chunks_needed+1:02d}-12 (beyond test patient range)")
    
    if not all_truly_new:
        print("❌ No truly new prescriptions found!")
        return pd.DataFrame()
    
    # Combine results
    print(f"\n🔄 Combining truly new prescriptions from {len(all_truly_new)} chunks...")
    combined_df = pd.concat(all_truly_new, ignore_index=True)
    
    print(f"✅ PRESCRIPTION RESULTS:")
    print(f"   Chunks processed: {min(chunks_needed, 12)}/12")
    print(f"   Total records checked: {total_records_checked:,}")
    print(f"   Truly new prescriptions found: {len(combined_df):,}")
    
    del all_truly_new
    gc.collect()
    return combined_df

def load_physician_data_chunked(year, test_patient_set=None):
    """
    CHECK EACH PHYSICIAN FILE: Find which files actually contain our test patients
    Don't assume anything about patient distribution - just check each file
    """
    print(f"Checking physician ID files for year {year} to find test patients...")
    
    # Find ALL physician ID batch files for the specific year
    pattern = f"clean_phys_id_batch_outpatient_{year}_*.parquet"
    phys_files = glob.glob(pattern)
    
    if not phys_files:
        print(f"❌ No physician ID files found matching pattern: {pattern}")
        return pd.DataFrame()
    
    # Sort files by chunk number
    phys_files = sorted(phys_files, key=lambda x: int(x.split('_')[-1].replace('.parquet', '')))
    
    print(f"Found {len(phys_files)} physician ID files for year {year}")
    
    if test_patient_set is not None:
        test_patient_set = set(test_patient_set)
        print(f"  🎯 Looking for {len(test_patient_set):,} test patients in these files...")
    else:
        print(f"  📊 Processing all physician files (no patient filtering)")
    
    relevant_phys_data = []
    total_records_checked = 0
    total_records_kept = 0
    files_with_patients = []
    files_without_patients = []
    
    # Check each file individually to see if it contains our test patients
    for i, file_path in enumerate(phys_files):
        chunk_num = file_path.split('_')[-1].replace('.parquet', '')
        file_size = Path(file_path).stat().st_size / (1024 * 1024)  # MB
        
        print(f"  📋 Checking file {i+1}/{len(phys_files)}: {os.path.basename(file_path)} ({file_size:.1f} MB)")
        chunk_start = time.time()
        
        try:
            # Load the file
            chunk_df = pd.read_parquet(file_path)
            total_records_checked += len(chunk_df)
            
            # Check columns
            if 'PHYS_ID' not in chunk_df.columns:
                print(f"      ❌ WARNING: 'PHYS_ID' column not found")
                print(f"      Available columns: {chunk_df.columns.tolist()}")
                continue
            
            # Check if this file contains any of our test patients
            if test_patient_set:
                patients_in_file = set(chunk_df['ENROLID'].unique())
                overlap = test_patient_set.intersection(patients_in_file)
                
                if len(overlap) > 0:
                    # This file contains some of our test patients
                    print(f"      ✅ CONTAINS {len(overlap):,} of our test patients")
                    files_with_patients.append(os.path.basename(file_path))
                    
                    # Filter to only our test patients
                    chunk_df = chunk_df[chunk_df['ENROLID'].isin(test_patient_set)]
                    
                    # Keep only needed columns
                    phys_subset = chunk_df[['ENROLID', 'SVCDATE', 'PHYS_ID']].copy()
                    relevant_phys_data.append(phys_subset)
                    total_records_kept += len(phys_subset)
                    
                    chunk_time = time.time() - chunk_start
                    print(f"      📥 Kept: {len(phys_subset):,} records ({chunk_time:.2f}s)")
                else:
                    # This file doesn't contain any of our test patients
                    chunk_time = time.time() - chunk_start
                    print(f"      ⏩ SKIP: No test patients in this file ({chunk_time:.2f}s)")
                    files_without_patients.append(os.path.basename(file_path))
            else:
                # No filtering - keep all data
                phys_subset = chunk_df[['ENROLID', 'SVCDATE', 'PHYS_ID']].copy()
                relevant_phys_data.append(phys_subset)
                total_records_kept += len(phys_subset)
                files_with_patients.append(os.path.basename(file_path))
                
                chunk_time = time.time() - chunk_start
                print(f"      ✅ Kept: {len(phys_subset):,} records ({chunk_time:.2f}s)")
            
        except Exception as e:
            print(f"      💥 ERROR: {e}")
        
        # Clean up
        if 'chunk_df' in locals():
            del chunk_df
        gc.collect()
    
    # Summary of which files contained our patients
    print(f"\n📋 FILE SUMMARY:")
    print(f"   Files containing test patients: {len(files_with_patients)}")
    for filename in files_with_patients:
        print(f"      ✅ {filename}")
    
    if files_without_patients:
        print(f"   Files without test patients: {len(files_without_patients)}")
        for filename in files_without_patients:
            print(f"      ❌ {filename}")
    
    if not relevant_phys_data:
        print("❌ No relevant physician ID data found!")
        return pd.DataFrame()
    
    # Combine results
    print(f"\n🔄 Combining physician data from {len(relevant_phys_data)} relevant files...")
    combined_phys_df = pd.concat(relevant_phys_data, ignore_index=True)
    
    print(f"✅ PHYSICIAN RESULTS:")
    print(f"   Files processed: {len(files_with_patients)}/{len(phys_files)}")
    print(f"   Total records checked: {total_records_checked:,}")
    print(f"   Final physician records: {len(combined_phys_df):,}")
    
    del relevant_phys_data
    gc.collect()
    return combined_phys_df

def analyze_phys_id_data_quality(phys_df):
    """
    Quick analysis of physician ID data quality
    """
    print(f"\nPhysician ID Data Quality Analysis:")
    print("=" * 50)
    
    total_records = len(phys_df)
    
    # Analyze PHYS_ID values
    phys_id_null = phys_df['PHYS_ID'].isna().sum()
    phys_id_not_null = total_records - phys_id_null
    
    print(f"  Total physician records: {total_records:,}")
    print(f"  Records with PHYS_ID: {phys_id_not_null:,} ({phys_id_not_null/total_records*100:.1f}%)")
    print(f"  Records without PHYS_ID: {phys_id_null:,} ({phys_id_null/total_records*100:.1f}%)")
    
    if phys_id_not_null > 0:
        # Check for different types of PHYS_ID values
        phys_id_values = phys_df['PHYS_ID'].dropna().astype(str)
        
        string_null_count = (phys_id_values.str.upper() == 'NULL').sum()
        empty_string_count = (phys_id_values.str.strip() == '').sum()
        real_phys_id_count = len(phys_id_values) - string_null_count - empty_string_count
        unique_phys_ids = phys_id_values[~phys_id_values.str.upper().isin(['NULL', ''])].nunique()
        
        print(f"  String 'NULL' values: {string_null_count:,}")
        print(f"  Empty string values: {empty_string_count:,}")
        print(f"  Real PHYS_ID values: {real_phys_id_count:,}")
        print(f"  Unique real PHYS_IDs: {unique_phys_ids:,}")
    
    print("=" * 50)

def process_patient_chunk(year, chunk_patients, chunk_num, total_chunks):
    """
    Process a single chunk of patients for a given year
    """
    print(f"\n  📦 Processing chunk {chunk_num}/{total_chunks} - Year {year} ({len(chunk_patients):,} patients)")
    chunk_start_time = time.time()

    # Initialize DuckDB
    conn = duckdb.connect()
    conn.execute("SET memory_limit='6GB'")
    conn.execute("SET threads=3")
    
    try:
        # Step 1: Load truly new prescriptions for this chunk
        truly_new_df = load_truly_new_prescriptions_optimized(year, chunk_patients)
        
        if len(truly_new_df) == 0:
            print("    ❌ No truly new prescriptions found for this chunk!")
            return None
        
        # Step 2: Load physician data for this chunk
        print(f"    Loading physician ID data for chunk...")
        physician_df = load_physician_data_chunked(year, chunk_patients)
        
        if len(physician_df) == 0:
            print("    ❌ No physician ID data found for this chunk!")
            return None
        
        # Step 3: Process matching using DuckDB
        print(f"    Processing physician ID matching...")
        
        # Register DataFrames with DuckDB
        conn.register('test_prescriptions_df', truly_new_df)
        conn.register('physician_data_df', physician_df)

        # Process physician ID matching (±30 days window)
        matching_query = """
        WITH 
        -- Merge prescription with physician data (±30 days)
        matched_visits AS (
            SELECT 
                p.ENROLID,
                p.SVCDATE as prescription_date,
                ph.SVCDATE as visit_date,
                ph.PHYS_ID
            FROM test_prescriptions_df p
            LEFT JOIN physician_data_df ph
                ON p.ENROLID = ph.ENROLID 
                AND ph.SVCDATE BETWEEN (p.SVCDATE::DATE - INTERVAL 30 DAY) 
                                   AND (p.SVCDATE::DATE + INTERVAL 30 DAY)
        )
        -- Count visits and physician IDs per prescription
        SELECT 
            ENROLID,
            prescription_date as SVCDATE,
            -- Count only real physician IDs (missing = 0, not distinct)
            COUNT(DISTINCT CASE 
                WHEN visit_date IS NOT NULL AND PHYS_ID IS NOT NULL
                THEN CAST(PHYS_ID as VARCHAR)
            END) as unique_phys_id_count,
            -- Debug info
            COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) as actual_visits,
            COUNT(CASE WHEN visit_date IS NOT NULL AND PHYS_ID IS NOT NULL THEN 1 END) as visits_with_phys_id,
            COUNT(CASE WHEN visit_date IS NOT NULL AND PHYS_ID IS NULL THEN 1 END) as visits_without_phys_id,
            -- Key indicator: "out of thin air" = no visits at all  
            CASE WHEN COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) = 0 THEN 1 ELSE 0 END as out_of_thin_air
        FROM matched_visits
        GROUP BY ENROLID, prescription_date
        ORDER BY ENROLID, prescription_date
        """

        final_df = conn.execute(matching_query).fetchdf()
        
        if len(final_df) == 0:
            print("    ❌ No prescription events found after matching!")
            return None
        
        # Step 4: Save histogram data
        print(f"    Saving histogram data...")
        histogram_file, histogram_df = save_histogram_data(year, chunk_num, final_df, OUTPUT_DIR)
        
        # Step 5: Quick summary
        chunk_time = time.time() - chunk_start_time
        out_of_thin_air = final_df['out_of_thin_air'].sum()
        zero_physicians = (final_df['unique_phys_id_count'] == 0).sum()
        
        print(f"    ✅ Chunk {chunk_num} completed in {chunk_time:.1f}s")
        print(f"       Prescription events: {len(final_df):,}")
        print(f"       'Out of thin air': {out_of_thin_air:,} ({out_of_thin_air/len(final_df)*100:.1f}%)")
        print(f"       Zero physicians: {zero_physicians:,} ({zero_physicians/len(final_df)*100:.1f}%)")
        
        # Clean up
        del truly_new_df, physician_df, final_df
        gc.collect()
        
        return {
            'year': year,
            'chunk_num': chunk_num,
            'patients_processed': len(chunk_patients),
            'prescription_events': len(final_df) if 'final_df' in locals() else 0,
            'processing_time': chunk_time,
            'histogram_file': histogram_file,
            'out_of_thin_air': out_of_thin_air if 'out_of_thin_air' in locals() else 0
        }

    except Exception as e:
        print(f"    💥 ERROR in chunk {chunk_num}: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        try:
            conn.close()
        except:
            pass
        gc.collect()

def process_year(year):
    """
    Process all patient chunks for a given year
    """
    print(f"\n{'='*60}")
    print(f"PROCESSING YEAR {year}")
    print(f"{'='*60}")
    
    year_start_time = time.time()
    
    # Get all patients for this year
    all_patients = get_all_patients_for_year(year)
    if not all_patients:
        print(f"❌ No patients found for year {year}")
        return []
    
    # Create patient chunks
    patient_chunks = create_patient_chunks(all_patients, PATIENT_CHUNK_SIZE)
    if not patient_chunks:
        print(f"❌ No patient chunks created for year {year}")
        return []
    
    # Process each chunk
    chunk_results = []
    for chunk_num, chunk_patients in enumerate(patient_chunks, 1):
        result = process_patient_chunk(year, chunk_patients, chunk_num, len(patient_chunks))
        if result:
            chunk_results.append(result)
    
    # Year summary
    year_time = time.time() - year_start_time
    total_patients = sum(r['patients_processed'] for r in chunk_results)
    total_events = sum(r['prescription_events'] for r in chunk_results)
    
    print(f"\n📊 YEAR {year} SUMMARY:")
    print(f"   Chunks processed: {len(chunk_results)}/{len(patient_chunks)}")
    print(f"   Total patients: {total_patients:,}")
    print(f"   Total prescription events: {total_events:,}")
    print(f"   Processing time: {year_time/60:.1f} minutes")
    print(f"   Histogram files saved: {len(chunk_results)}")
    
    # Clean up
    del all_patients, patient_chunks
    gc.collect()
    
    return chunk_results
    print(f"\n{'='*60}")
    print(f"TRULY NEW PRESCRIPTIONS PHYSICIAN ID ANALYSIS - YEAR {year}")
    print(f"Using pre-generated physician ID batch files")
    print(f"{'='*60}")

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
        
        if not Path(d_file).exists():
            print(f"ERROR: Missing prescription file: {d_file}")
            return None
        
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

        # Step 2: Load truly new prescriptions efficiently using ENROLID optimization
        print(f"\nStep 2: Loading truly new prescriptions for these {total_patients:,} patients...")
        step2_start = time.time()
        
        # Pass test patient list to optimize prescription loading too!
        truly_new_df = load_truly_new_prescriptions_optimized(test_enrolids)
        
        if len(truly_new_df) == 0:
            print("No truly new prescriptions found!")
            return None
        
        step2_time = time.time() - step2_start
        print(f"  Loaded {len(truly_new_df):,} truly new prescriptions for test patients")
        print(f"  Step 2 completed in {step2_time:.2f} seconds")
        
        # Note: No additional filtering needed since we already filtered in the optimized function
        test_prescriptions = truly_new_df.copy()
        
        # Clean up
        del truly_new_df
        gc.collect()

        # Step 3: Load physician ID data efficiently (only for test patients)
        print(f"\nStep 3: Loading physician ID data for test patients...")
        step3_start = time.time()
        
        # Pass test patient list to optimize loading
        physician_df = load_physician_data_chunked(year, test_enrolids)
        
        if len(physician_df) == 0:
            print("No physician ID data found!")
            return None
        
        step3_time = time.time() - step3_start
        print(f"  Step 3 completed in {step3_time:.2f} seconds")
        
        # Analyze data quality
        analyze_phys_id_data_quality(physician_df)
        
        gc.collect()

        # Step 4: Process physician ID matching using DuckDB
        print(f"\nStep 4: Processing physician ID matching for test patients...")
        step4_start = time.time()
        
        # Register DataFrames with DuckDB
        conn.register('test_prescriptions_df', test_prescriptions)
        conn.register('physician_data_df', physician_df)

        # Debug: Check what phys_id values actually exist
        debug_query = """
        SELECT 
            CASE 
                WHEN PHYS_ID IS NULL THEN 'SQL_NULL'
                WHEN UPPER(TRIM(CAST(PHYS_ID as VARCHAR))) = 'NULL' THEN 'STRING_NULL'
                WHEN UPPER(TRIM(CAST(PHYS_ID as VARCHAR))) = '' THEN 'EMPTY_STRING'
                WHEN LENGTH(TRIM(CAST(PHYS_ID as VARCHAR))) = 0 THEN 'ZERO_LENGTH'
                ELSE 'REAL_PHYS_ID'
            END as phys_id_type,
            COUNT(*) as count
        FROM physician_data_df
        GROUP BY 1
        ORDER BY count DESC
        """
        
        debug_result = conn.execute(debug_query).fetchdf()
        print(f"  Debug - Physician ID value types in your data:")
        for _, row in debug_result.iterrows():
            print(f"    {row['phys_id_type']}: {row['count']:,} records")

        matching_query = """
        WITH 
        -- Merge prescription with physician data (±30 days)
        matched_visits AS (
            SELECT 
                p.ENROLID,
                p.SVCDATE as prescription_date,
                ph.SVCDATE as visit_date,
                ph.PHYS_ID
            FROM test_prescriptions_df p
            LEFT JOIN physician_data_df ph
                ON p.ENROLID = ph.ENROLID 
                AND ph.SVCDATE BETWEEN (p.SVCDATE::DATE - INTERVAL 30 DAY) 
                                   AND (p.SVCDATE::DATE + INTERVAL 30 DAY)
        )
        -- Count visits and physician IDs per prescription
        SELECT 
            ENROLID,
            prescription_date as SVCDATE,
            -- Count only real physician IDs (missing = 0, not distinct)
            COUNT(DISTINCT CASE 
                WHEN visit_date IS NOT NULL AND PHYS_ID IS NOT NULL
                THEN CAST(PHYS_ID as VARCHAR)
            END) as unique_phys_id_count,
            -- Debug info
            COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) as actual_visits,
            COUNT(CASE WHEN visit_date IS NOT NULL AND PHYS_ID IS NOT NULL THEN 1 END) as visits_with_phys_id,
            COUNT(CASE WHEN visit_date IS NOT NULL AND PHYS_ID IS NULL THEN 1 END) as visits_without_phys_id,
            -- Key indicator: "out of thin air" = no visits at all  
            CASE WHEN COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) = 0 THEN 1 ELSE 0 END as out_of_thin_air
        FROM matched_visits
        GROUP BY ENROLID, prescription_date
        ORDER BY ENROLID, prescription_date
        """

        final_df = conn.execute(matching_query).fetchdf()
        step4_time = time.time() - step4_start
        
        print(f"  Processed {len(final_df):,} prescription events in {step4_time:.1f} seconds")
        
        # Clean up
        del test_enrolids, test_prescriptions, physician_df
        gc.collect()

        # Step 5: Analyze results
        print(f"\nStep 5: Analysis of results...")
        
        if len(final_df) > 0:
            # Show physician ID counting results
            print(f"  Physician ID counting analysis:")
            
            # Show histogram
            histogram_data = final_df['unique_phys_id_count'].value_counts().sort_index()
            total_events = len(final_df)
            
            print(f"\nHistogram - Unique Physician IDs per Prescription:")
            print("=" * 55)
            for phys_id_count, frequency in histogram_data.items():
                percentage = (frequency / total_events) * 100
                print(f"  {phys_id_count:2d} Phys_ID(s): {frequency:6,} events ({percentage:5.1f}%)")
            
            # Key stats
            zero_phys_ids = histogram_data.get(0, 0)
            out_of_thin_air_count = final_df['out_of_thin_air'].sum()
            
            print(f"\nSUMMARY:")
            print(f"  Total prescription events: {len(final_df):,}")
            print(f"  Events with 0 physicians: {zero_phys_ids:,} ({zero_phys_ids/len(final_df)*100:.1f}%)")
            print(f"  'Out of thin air' (no visits): {out_of_thin_air_count:,} ({out_of_thin_air_count/len(final_df)*100:.1f}%)")
            print(f"  Events with visits but no phys_id: {zero_phys_ids - out_of_thin_air_count:,}")
            
            # Show debug info
            total_visits = final_df['actual_visits'].sum()
            visits_with_phys_id = final_df['visits_with_phys_id'].sum()
            visits_without_phys_id = final_df['visits_without_phys_id'].sum()
            
            print(f"\n  DEBUG - Visit breakdown:")
            print(f"    Total visits found: {total_visits:,}")
            print(f"    Visits with phys_id: {visits_with_phys_id:,} ({visits_with_phys_id/total_visits*100:.1f}%)")
            print(f"    Visits without phys_id: {visits_without_phys_id:,} ({visits_without_phys_id/total_visits*100:.1f}%)")
            
        else:
            print("  No prescription events found!")
            out_of_thin_air_count = 0

        # Step 6: Create histogram
        print("\nStep 6: Creating histogram...")
        if len(final_df) > 0:
            histogram_data = final_df['unique_phys_id_count'].value_counts().sort_index()
            total_events = len(final_df)

            # Create histogram plot
            fig, ax = plt.subplots(1, 1, figsize=(12, 8))
            
            bars = ax.bar(histogram_data.index, histogram_data.values, 
                         color='lightblue', edgecolor='black', alpha=0.7)
            ax.set_xlabel('Number of Unique Physician IDs per Prescription Event')
            ax.set_ylabel('Frequency')
            ax.set_title('Physician ID Count per Truly New Prescription\n(Missing phys_id = 0, not distinct)')
            ax.grid(True, alpha=0.3)
            
            # Add percentage labels
            for bar, (phys_id_count, freq) in zip(bars, histogram_data.items()):
                if phys_id_count <= 10:  # Only label first 10 bars
                    pct = (freq / total_events) * 100
                    ax.text(bar.get_x() + bar.get_width()/2., bar.get_height() + max(histogram_data.values) * 0.01, 
                            f'{pct:.1f}%', ha='center', va='bottom', fontsize=8)
            
            plt.tight_layout()
            histogram_file = f'phys_id_histogram_{TEST_PATIENT_LIMIT//1000}k_{year}.png'
            plt.savefig(histogram_file, dpi=300, bbox_inches='tight')
            print(f"  Histogram saved as: {histogram_file}")
            plt.show()
        else:
            print("  No data to plot histogram")

        total_time = time.time() - total_start_time
        
        print(f"\n{'='*60}")
        print(f"TRULY NEW PRESCRIPTIONS PHYSICIAN ID ANALYSIS COMPLETE - {total_time:.1f} seconds")
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
            'null_phys_id_events': out_of_thin_air_count if len(final_df) > 0 else 0
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
    print("MarketScan Analysis - MULTI-YEAR PHYSICIAN ID ANALYSIS")
    print("Processing all years with 200k patient chunks")
    print("Saving histogram data as parquet files")
    print("=" * 60)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"📁 Output directory: {OUTPUT_DIR}")
    
    # Process all years
    all_results = []
    total_start_time = time.time()
    
    for year in range(START_YEAR, END_YEAR + 1):
        year_results = process_year(year)
        all_results.extend(year_results)
    
    # Overall summary
    total_time = time.time() - total_start_time
    
    print(f"\n{'='*60}")
    print(f"OVERALL ANALYSIS COMPLETE")
    print(f"{'='*60}")
    
    if all_results:
        # Create summary DataFrame
        summary_df = pd.DataFrame(all_results)
        
        # Overall stats
        total_patients = summary_df['patients_processed'].sum()
        total_events = summary_df['prescription_events'].sum()
        years_processed = summary_df['year'].nunique()
        chunks_processed = len(summary_df)
        
        print(f"Years processed: {years_processed} ({START_YEAR}-{END_YEAR})")
        print(f"Patient chunks processed: {chunks_processed}")
        print(f"Total patients analyzed: {total_patients:,}")
        print(f"Total prescription events: {total_events:,}")
        print(f"Average events per patient: {total_events/total_patients:.2f}")
        print(f"Total processing time: {total_time/60:.1f} minutes")
        
        # Save overall summary
        summary_file = os.path.join(OUTPUT_DIR, f"processing_summary_{START_YEAR}_{END_YEAR}.csv")
        summary_df.to_csv(summary_file, index=False)
        print(f"\n💾 Processing summary saved: {summary_file}")
        
        # Show histogram files created
        histogram_files = glob.glob(os.path.join(OUTPUT_DIR, "unique_phys_*.parquet"))
        print(f"\n📊 Histogram files created: {len(histogram_files)}")
        
        # Group by year
        for year in sorted(summary_df['year'].unique()):
            year_chunks = summary_df[summary_df['year'] == year]
            print(f"   Year {year}: {len(year_chunks)} files (unique_phys_{year}_01.parquet to unique_phys_{year}_{len(year_chunks):02d}.parquet)")
        
        print(f"\n🎉 Multi-year analysis completed successfully!")
        print(f"📁 All histogram data saved in: {OUTPUT_DIR}")
        
    else:
        print("❌ No results generated - check for errors above")

if __name__ == "__main__":
    main()
