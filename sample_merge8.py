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
    
    # Try different naming patterns
    patterns = [
        f"prescription_flags_{year}_{DATABASE}_D_part*.parquet",
        f"prescription_flags_{DATABASE}_D_{year}_part*.parquet",
        f"prescription_flags_{year}_part*.parquet"
    ]
    
    for pattern in patterns:
        files = glob.glob(pattern)
        if files:
            files = sorted(files)
            print(f"  ✅ Found {len(files)} prescription files: {pattern}")
            return files
    
    print(f"  ❌ No prescription flag files found for {year}")
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
        
        # Get unique patients in this chunk
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
    
    # Find all physician files for the year
    pattern = f"clean_phys_id_batch_outpatient_{year}_*.parquet"
    all_phys_files = glob.glob(pattern)
    
    if not all_phys_files:
        print(f"      ❌ No physician files found for {year}")
        return []
    
    all_phys_files = sorted(all_phys_files, key=lambda x: int(x.split('_')[-1].replace('.parquet', '')))
    print(f"      Found {len(all_phys_files)} physician files to check")
    
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
    conn.execute("SET memory_limit='6GB'")
    conn.execute("SET threads=3")
    
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
        )
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
            -- "Out of thin air" indicator
            CASE WHEN COUNT(CASE WHEN visit_date IS NOT NULL THEN 1 END) = 0 THEN 1 ELSE 0 END as out_of_thin_air
        FROM matched_visits
        GROUP BY ENROLID, prescription_date
        ORDER BY ENROLID, prescription_date
        """
        
        final_df = conn.execute(matching_query).fetchdf()
        
        if len(final_df) == 0:
            print(f"    ❌ No prescription events after matching")
            return None
        
        # Step 5: Create and save histogram
        print(f"    📊 Creating histogram...")
        histogram_data = final_df['unique_phys_id_count'].value_counts().sort_index()
        total_events = len(final_df)
        
        # Create histogram DataFrame
        histogram_df = pd.DataFrame({
            'unique_phys': histogram_data.index,
            'prescription_events': histogram_data.values,
            'percentage': (histogram_data.values / total_events * 100).round(2)
        })
        
        # Add metadata
        histogram_df['year'] = year
        histogram_df['file_number'] = part_name
        histogram_df['total_events'] = total_events
        
        # Summary stats
        out_of_thin_air = final_df['out_of_thin_air'].sum()
        zero_physicians = histogram_data.get(0, 0)
        
        histogram_df['out_of_thin_air_events'] = out_of_thin_air
        histogram_df['zero_physician_events'] = zero_physicians
        
        # Save histogram
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_file = f"unique_phys_{year}_{part_name}.parquet"
        output_path = os.path.join(OUTPUT_DIR, output_file)
        histogram_df.to_parquet(output_path, compression='snappy')
        
        chunk_time = time.time() - chunk_start_time
        
        print(f"    ✅ Completed in {chunk_time:.1f}s")
        print(f"       Prescription events: {total_events:,}")
        print(f"       Out of thin air: {out_of_thin_air:,} ({out_of_thin_air/total_events*100:.1f}%)")
        print(f"       Zero physicians: {zero_physicians:,} ({zero_physicians/total_events*100:.1f}%)")
        print(f"       💾 Saved: {output_file}")
        
        # Clean up
        del prescriptions_df, physician_df, final_df
        gc.collect()
        
        return {
            'year': year,
            'part': part_name,
            'prescription_events': total_events,
            'processing_time': chunk_time,
            'output_file': output_file,
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

def main():
    print("MarketScan Analysis - MULTI-YEAR PHYSICIAN ID ANALYSIS")
    print("Processing in 100k chunks using existing prescription file structure")
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
        total_events = summary_df['prescription_events'].sum()
        years_processed = summary_df['year'].nunique()
        files_processed = len(summary_df)
        
        print(f"Years processed: {years_processed} ({START_YEAR}-{END_YEAR})")
        print(f"Prescription files processed: {files_processed}")
        print(f"Total prescription events: {total_events:,}")
        print(f"Total processing time: {total_time/60:.1f} minutes")
        
        # Save overall summary
        summary_file = os.path.join(OUTPUT_DIR, f"processing_summary_{START_YEAR}_{END_YEAR}.csv")
        summary_df.to_csv(summary_file, index=False)
        print(f"\n💾 Processing summary saved: {summary_file}")
        
        # Show output files created
        output_files = glob.glob(os.path.join(OUTPUT_DIR, "unique_phys_*.parquet"))
        print(f"\n📊 Histogram files created: {len(output_files)}")
        
        # Group by year
        for year in sorted(summary_df['year'].unique()):
            year_files = summary_df[summary_df['year'] == year]
            print(f"   Year {year}: {len(year_files)} files")
        
        print(f"\n🎉 Multi-year analysis completed successfully!")
        print(f"📁 All histogram data saved in: {OUTPUT_DIR}")
        
    else:
        print("❌ No results generated - check for errors above")

if __name__ == "__main__":
    main()
