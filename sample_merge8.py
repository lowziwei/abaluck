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
OUTPUT_DIR = "histogram_results"  # Directory for saving histogram data

def check_parquet_file_health(file_path):
    """
    Diagnose parquet file issues
    """
    try:
        # Check basic file properties
        if not Path(file_path).exists():
            return False, "File does not exist"
        
        file_size = Path(file_path).stat().st_size
        if file_size == 0:
            return False, "File is empty"
        
        # Check file header/footer
        with open(file_path, 'rb') as f:
            # Check first few bytes
            first_bytes = f.read(10)
            
            # Check last few bytes (parquet magic bytes should be at end)
            f.seek(-8, 2)  # Go to last 8 bytes
            last_bytes = f.read(8)
            
            # Parquet files should end with b'PAR1'
            if not last_bytes.endswith(b'PAR1'):
                return False, f"Missing PAR1 magic bytes. Last 8 bytes: {last_bytes}"
        
        # Try to load with pandas
        df = pd.read_parquet(file_path)
        return True, f"OK - {len(df):,} records, {file_size/(1024*1024):.1f} MB"
        
    except Exception as e:
        return False, f"Error: {e}"

def find_prescription_files(year):
    """
    Find all prescription flag files for a given year
    """
    print(f"  🔍 Looking for prescription flag files for year {year}...")
    
    # Use the exact naming pattern you specified
    pattern = f"prescription_flags_{year}_*_part*.parquet"
    files = glob.glob(pattern)
    
    if files:
        files = sorted(files)
        print(f"  ✅ Found {len(files)} prescription files: {pattern}")
        return files
    else:
        print(f"  ❌ No prescription flag files found for {year}")
        print(f"     Pattern tried: {pattern}")
        # Show what files actually exist
        all_files = glob.glob("prescription_flags_*.parquet")
        if all_files:
            print(f"     Available prescription files: {[os.path.basename(f) for f in all_files[:5]]}...")
        return []

def load_prescription_chunk(file_path):
    """
    Load a single prescription chunk and filter for truly new prescriptions
    """
    print(f"    📥 Loading prescription file: {os.path.basename(file_path)}")
    
    # Check file health
    is_healthy, health_msg = check_parquet_file_health(file_path)
    if not is_healthy:
        print(f"      ❌ CORRUPTED: {health_msg}")
        return pd.DataFrame()
    
    try:
        # Load file
        df = pd.read_parquet(file_path)
        print(f"      Loaded {len(df):,} total records")
        
        # Check for FLAG column
        if 'FLAG' not in df.columns:
            print(f"      ❌ No 'FLAG' column found")
            print(f"      Available columns: {df.columns.tolist()}")
            return pd.DataFrame()
        
        # Filter for truly new prescriptions (FLAG = NULL)
        truly_new = df[df['FLAG'].isna()].copy()
        print(f"      Found {len(truly_new):,} truly new prescriptions (FLAG=NULL)")
        
        if len(truly_new) == 0:
            return pd.DataFrame()
        
        # Get unique patients in this chunk - BACK TO FULL SIZE
        patients_in_chunk = set(truly_new['ENROLID'].unique())
        print(f"      Unique patients with truly new prescriptions: {len(patients_in_chunk):,}")
        
        # Keep only needed columns
        result = truly_new[['ENROLID', 'SVCDATE']].copy()
        
        # Clean up
        del df, truly_new
        gc.collect()
        
        return result, patients_in_chunk
        
    except Exception as e:
        print(f"      💥 ERROR: {e}")
        return pd.DataFrame()

def find_physician_files_for_patients(year, patients_set):
    """
    Find which physician files contain our patients
    """
    print(f"    🔍 Finding physician files containing {len(patients_set):,} patients...")
    
    # Search in the processed_marketscan directory
    phys_dir = "/home/zl749/processed_marketscan"
    
    # Find all physician files for the year - check both inpatient and outpatient
    outpatient_pattern = f"{phys_dir}/clean_phys_id_batch_outpatient_{year}_*.parquet"
    inpatient_pattern = f"{phys_dir}/clean_phys_id_batch_inpatient_{year}_*.parquet"
    
    all_phys_files = []
    
    # Check outpatient files
    outpatient_files = glob.glob(outpatient_pattern)
    if outpatient_files:
        all_phys_files.extend(outpatient_files)
        print(f"      Found {len(outpatient_files)} outpatient files")
    
    # Check inpatient files  
    inpatient_files = glob.glob(inpatient_pattern)
    if inpatient_files:
        all_phys_files.extend(inpatient_files)
        print(f"      Found {len(inpatient_files)} inpatient files")
    
    if not all_phys_files:
        print(f"      ❌ No physician files found for {year}")
        print(f"         Tried patterns: {outpatient_pattern}, {inpatient_pattern}")
        return []
    
    all_phys_files = sorted(all_phys_files, key=lambda x: int(x.split('_')[-1].replace('.parquet', '')))
    print(f"      Total physician files to check: {len(all_phys_files)}")
    
    relevant_files = []
    
    for file_path in all_phys_files:
        filename = os.path.basename(file_path)
        
        # Check file health first
        is_healthy, health_msg = check_parquet_file_health(file_path)
        if not is_healthy:
            print(f"      ❌ CORRUPTED {filename}: {health_msg}")
            continue
        
        try:
            # Quick check - load just ENROLID column to see if our patients are here
            df = pd.read_parquet(file_path, columns=['ENROLID'])
            patients_in_file = set(df['ENROLID'].unique())
            
            # Check overlap
            overlap = patients_set.intersection(patients_in_file)
            
            if len(overlap) > 0:
                print(f"      ✅ {filename}: contains {len(overlap):,} of our patients")
                relevant_files.append(file_path)
            else:
                print(f"      ⏩ {filename}: no overlap")
            
            del df
            gc.collect()
            
        except Exception as e:
            print(f"      💥 ERROR checking {filename}: {e}")
    
    print(f"      Result: {len(relevant_files)} physician files contain our patients")
    return relevant_files

def load_physician_data_for_patients(physician_files, patients_set):
    """
    Load physician data from relevant files for our patients
    """
    print(f"    📥 Loading physician data from {len(physician_files)} files...")
    
    all_phys_data = []
    total_records = 0
    
    for file_path in physician_files:
        filename = os.path.basename(file_path)
        
        try:
            # Load full file
            df = pd.read_parquet(file_path)
            
            # Check for PHYS_ID column
            if 'PHYS_ID' not in df.columns:
                print(f"      ❌ {filename}: No PHYS_ID column")
                continue
            
            # Filter to our patients
            df_filtered = df[df['ENROLID'].isin(patients_set)].copy()
            
            if len(df_filtered) > 0:
                # Keep only needed columns
                phys_subset = df_filtered[['ENROLID', 'SVCDATE', 'PHYS_ID']].copy()
                all_phys_data.append(phys_subset)
                total_records += len(phys_subset)
                print(f"      ✅ {filename}: kept {len(phys_subset):,} records")
            
            del df, df_filtered
            gc.collect()
            
        except Exception as e:
            print(f"      💥 ERROR loading {filename}: {e}")
    
    if not all_phys_data:
        print(f"      ❌ No physician data found")
        return pd.DataFrame()
    
    # Combine all physician data
    combined_df = pd.concat(all_phys_data, ignore_index=True)
    print(f"      ✅ Total physician records: {len(combined_df):,}")
    
    del all_phys_data
    gc.collect()
    
    return combined_df

def process_prescription_chunk(year, prescription_file):
    """
    Process a single prescription chunk file
    """
    part_name = os.path.basename(prescription_file).replace('.parquet', '').split('_')[-1]
    print(f"\n  📦 Processing {year} {part_name}")
    chunk_start_time = time.time()
    
    # Step 1: Load prescription chunk
    prescription_result = load_prescription_chunk(prescription_file)
    
    if isinstance(prescription_result, tuple):
        prescriptions_df, patients_in_chunk = prescription_result
    else:
        print(f"    ❌ No prescription data loaded")
        return None
    
    if len(prescriptions_df) == 0:
        print(f"    ❌ No truly new prescriptions found")
        return None
    
    # Step 2: Find relevant physician files
    physician_files = find_physician_files_for_patients(year, patients_in_chunk)
    
    if not physician_files:
        print(f"    ❌ No physician files found for these patients")
        return None
    
    # Step 3: Load physician data
    physician_df = load_physician_data_for_patients(physician_files, patients_in_chunk)
    
    if len(physician_df) == 0:
        print(f"    ❌ No physician data found")
        return None
    
    # Step 4: Match prescriptions to physicians using DuckDB
    print(f"    🔗 Matching prescriptions to physicians...")
    
    conn = duckdb.connect()
    conn.execute("SET memory_limit='6GB'")  # Back to higher memory since we have space
    conn.execute("SET threads=3")           # Back to 3 threads
    # Use root filesystem which has 355GB free instead of /tmp (only 1.9GB)
    conn.execute("SET temp_directory='/home/zl749/duckdb_temp'")
    # No need for temp size limit with 355GB available
    
    # Create temp directory if it doesn't exist
    temp_dir = "/home/zl749/duckdb_temp"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # Register DataFrames
        conn.register('prescriptions_df', prescriptions_df)
        conn.register('physician_df', physician_df)
        
        # Match and count physicians (±30 days)
        matching_query = """
        WITH matched_visits AS (
            SELECT 
                p.ENROLID,
                p.SVCDATE as prescription_date,
                ph.SVCDATE as visit_date,
                ph.PHYS_ID
            FROM prescriptions_df p
            LEFT JOIN physician_df ph
                ON p.ENROLID = ph.ENROLID 
                AND ph.SVCDATE BETWEEN (p.SVCDATE::DATE - INTERVAL 30 DAY) 
                                   AND (p.SVCDATE::DATE + INTERVAL 30 DAY)
        ),
        unique_phys_per_prescription AS (
            SELECT DISTINCT
                ENROLID,
                prescription_date,
                CASE WHEN visit_date IS NOT NULL AND PHYS_ID IS NOT NULL
                     THEN CAST(PHYS_ID as VARCHAR)
                     ELSE NULL
                END as phys_id
            FROM matched_visits
        ),
        aggregated_data AS (
            SELECT 
                ENROLID,
                prescription_date,
                -- Count unique physician IDs
                COUNT(CASE WHEN phys_id IS NOT NULL THEN 1 END) as unique_phys_id_count,
                -- Collect all physician IDs (will sort later)
                STRING_AGG(phys_id, ',' ORDER BY phys_id) as all_phys_ids_str,
                -- Get visit stats from original data
                (SELECT COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) 
                 FROM matched_visits mv 
                 WHERE mv.ENROLID = unique_phys_per_prescription.ENROLID 
                 AND mv.prescription_date = unique_phys_per_prescription.prescription_date) as actual_visits,
                (SELECT COUNT(CASE WHEN visit_date IS NOT NULL AND PHYS_ID IS NOT NULL THEN 1 END)
                 FROM matched_visits mv 
                 WHERE mv.ENROLID = unique_phys_per_prescription.ENROLID 
                 AND mv.prescription_date = unique_phys_per_prescription.prescription_date) as visits_with_phys_id,
                (SELECT COUNT(CASE WHEN visit_date IS NOT NULL AND PHYS_ID IS NULL THEN 1 END)
                 FROM matched_visits mv 
                 WHERE mv.ENROLID = unique_phys_per_prescription.ENROLID 
                 AND mv.prescription_date = unique_phys_per_prescription.prescription_date) as visits_without_phys_id,
                -- "Out of thin air" indicator
                CASE WHEN (SELECT COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) 
                          FROM matched_visits mv 
                          WHERE mv.ENROLID = unique_phys_per_prescription.ENROLID 
                          AND mv.prescription_date = unique_phys_per_prescription.prescription_date) = 0 
                     THEN 1 ELSE 0 END as out_of_thin_air
            FROM unique_phys_per_prescription
            GROUP BY ENROLID, prescription_date
        )
        SELECT 
            ENROLID,
            prescription_date as SVCDATE,
            unique_phys_id_count,
            -- Create phys_ids string with max 3 IDs + ",etc." if more
            CASE 
                WHEN unique_phys_id_count = 0 THEN ''
                WHEN unique_phys_id_count <= 3 THEN all_phys_ids_str
                ELSE STRING_SPLIT(all_phys_ids_str, ',')[1] || ',' || 
                     STRING_SPLIT(all_phys_ids_str, ',')[2] || ',' || 
                     STRING_SPLIT(all_phys_ids_str, ',')[3] || ',etc.'
            END as phys_ids,
            actual_visits,
            visits_with_phys_id,
            visits_without_phys_id,
            out_of_thin_air
        FROM aggregated_data
        ORDER BY ENROLID, prescription_date
        """
        
        final_df = conn.execute(matching_query).fetchdf()
        
        if len(final_df) == 0:
            print(f"    ❌ No prescription events after matching")
            return None
        
        # Step 5: Create and save histogram + individual events
        print(f"    📊 Creating histogram and saving individual events...")
        histogram_data = final_df['unique_phys_id_count'].value_counts().sort_index()
        total_events = len(final_df)
        
        # Create histogram DataFrame
        histogram_df = pd.DataFrame({
            'unique_phys': histogram_data.index,
            'prescription_events': histogram_data.values,
            'percentage': (histogram_data.values / total_events * 100).round(2)
        })
        
        # Add metadata to histogram
        histogram_df['year'] = year
        histogram_df['file_number'] = part_name
        histogram_df['total_events'] = total_events
        
        # Summary stats
        out_of_thin_air = final_df['out_of_thin_air'].sum()
        zero_physicians = histogram_data.get(0, 0)
        
        histogram_df['out_of_thin_air_events'] = out_of_thin_air
        histogram_df['zero_physician_events'] = zero_physicians
        
        # Prepare individual events data
        events_df = final_df[['ENROLID', 'SVCDATE', 'unique_phys_id_count', 'phys_ids', 'actual_visits', 'visits_with_phys_id', 'visits_without_phys_id', 'out_of_thin_air']].copy()
        events_df['year'] = year
        events_df['file_number'] = part_name
        
        # Save both files
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Save histogram
        histogram_file = f"unique_phys_{year}_{part_name}.parquet"
        histogram_path = os.path.join(OUTPUT_DIR, histogram_file)
        histogram_df.to_parquet(histogram_path, compression='snappy')
        
        # Save individual events
        events_file = f"prescription_events_{year}_{part_name}.parquet"
        events_path = os.path.join(OUTPUT_DIR, events_file)
        events_df.to_parquet(events_path, compression='snappy')
        
        chunk_time = time.time() - chunk_start_time
        
        print(f"    ✅ Completed in {chunk_time:.1f}s")
        print(f"       Prescription events: {total_events:,}")
        print(f"       Out of thin air: {out_of_thin_air:,} ({out_of_thin_air/total_events*100:.1f}%)")
        print(f"       Zero physicians: {zero_physicians:,} ({zero_physicians/total_events*100:.1f}%)")
        print(f"       💾 Saved histogram: {histogram_file}")
        print(f"       💾 Saved events: {events_file}")
        
        # Clean up
        del prescriptions_df, physician_df, final_df, events_df
        gc.collect()
        
        return {
            'year': year,
            'part': part_name,
            'prescription_events': total_events,
            'processing_time': chunk_time,
            'histogram_file': histogram_file,
            'events_file': events_file,
            'out_of_thin_air': out_of_thin_air,
            'zero_physicians': zero_physicians
        }
        
    except Exception as e:
        print(f"    💥 ERROR in matching: {e}")
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
    Process all prescription chunks for a given year
    """
    print(f"\n{'='*60}")
    print(f"PROCESSING YEAR {year}")
    print(f"{'='*60}")
    
    year_start_time = time.time()
    
    # Find all prescription files for this year
    prescription_files = find_prescription_files(year)
    
    if not prescription_files:
        print(f"❌ No prescription files found for {year}")
        return []
    
    print(f"Found {len(prescription_files)} prescription files to process")
    
    # Process each prescription file
    results = []
    for i, prescription_file in enumerate(prescription_files, 1):
        print(f"\nProcessing file {i}/{len(prescription_files)}")
        result = process_prescription_chunk(year, prescription_file)
        if result:
            results.append(result)
    
    # Year summary
    year_time = time.time() - year_start_time
    if results:
        total_events = sum(r['prescription_events'] for r in results)
        total_out_of_thin_air = sum(r['out_of_thin_air'] for r in results)
        
        print(f"\n📊 YEAR {year} SUMMARY:")
        print(f"   Files processed: {len(results)}/{len(prescription_files)}")
        print(f"   Total prescription events: {total_events:,}")
        print(f"   Total 'out of thin air': {total_out_of_thin_air:,}")
        print(f"   Processing time: {year_time/60:.1f} minutes")
        print(f"   Output files: {len(results)}")
    
    return results

def test_single_file():
    """
    Test with just one prescription file to make sure everything works
    """
    print("🧪 TESTING WITH SINGLE PRESCRIPTION FILE")
    print("=" * 50)
    
    # Test parameters
    test_year = 2018
    test_part = "part01"  # You can change this to test different files
    
    # Use the exact naming pattern: prescription_flags_YYYY_*_part##.parquet
    test_pattern = f"prescription_flags_{test_year}_*_{test_part}.parquet"
    files = glob.glob(test_pattern)
    
    if files:
        test_file = files[0]  # Take the first match
        print(f"✅ Found test file: {test_file}")
    else:
        print(f"❌ Could not find test file for {test_year} {test_part}")
        print(f"Pattern tried: {test_pattern}")
        # List what prescription files actually exist
        all_prescription_files = glob.glob("prescription_flags_*.parquet")
        print(f"Available prescription files:")
        for f in all_prescription_files[:10]:  # Show first 10
            print(f"   {os.path.basename(f)}")
        return
    
    # Process just this one file
    print(f"\n🔬 Testing with: {os.path.basename(test_file)}")
    
    result = process_prescription_chunk(test_year, test_file)
    
    if result:
        print(f"\n🎉 TEST SUCCESSFUL!")
        print(f"   Histogram file: {result['histogram_file']}")
        print(f"   Events file: {result['events_file']}")
        print(f"   Prescription events: {result['prescription_events']:,}")
        print(f"   Processing time: {result['processing_time']:.1f}s")
        print(f"   'Out of thin air': {result['out_of_thin_air']:,}")
        
        # Show the histogram file was created
        histogram_path = os.path.join(OUTPUT_DIR, result['histogram_file'])
        events_path = os.path.join(OUTPUT_DIR, result['events_file'])
        
        if Path(histogram_path).exists():
            print(f"   ✅ Histogram file created: {Path(histogram_path).stat().st_size / 1024:.1f} KB")
            
            # Quick peek at the histogram data
            histogram_df = pd.read_parquet(histogram_path)
            print(f"   📊 Histogram preview:")
            print(histogram_df.head())
        
        if Path(events_path).exists():
            print(f"   ✅ Events file created: {Path(events_path).stat().st_size / (1024*1024):.1f} MB")
            
            # Quick peek at the events data
            events_df = pd.read_parquet(events_path)
            print(f"   📋 Events preview:")
            print(events_df[['ENROLID', 'SVCDATE', 'unique_phys_id_count', 'phys_ids']].head())
        
    else:
        print(f"\n❌ TEST FAILED - check errors above")

def main():
    print("MarketScan Analysis - SINGLE FILE TEST MODE")
    print("Testing with one prescription file to check the new phys_ids logic")
    print("=" * 60)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"📁 Output directory: {OUTPUT_DIR}")
    
    # Run single file test
    test_single_file()

if __name__ == "__main__":
    main()
