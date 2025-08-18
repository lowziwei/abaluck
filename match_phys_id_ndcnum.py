import pandas as pd
import duckdb
import glob
import os
import time
from pathlib import Path

# Configuration
START_YEAR = 2018
END_YEAR = 2024
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
OUTPUT_DIR = "histogram_results"

def setup_duckdb_connection(memory_limit='8GB'):
    """
    Create optimized DuckDB connection with performance settings
    """
    conn = duckdb.connect(':memory:')
    
    # Optimized settings for large data processing
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.execute("SET max_temp_directory_size='50GB'")
    conn.execute("SET temp_directory='/tmp'")
    conn.execute("SET threads=8")  # Adjust based on your CPU
    conn.execute("SET preserve_insertion_order=false")
    conn.execute("SET enable_progress_bar=true")
    conn.execute("SET enable_profiling=true")
    
    # Additional optimizations
    conn.execute("SET default_order='ASC'")
    conn.execute("SET default_null_order='NULLS_LAST'")
    conn.execute("SET force_parallelism=true")
    
    return conn

def extract_ndcnum_for_year_duckdb(year):
    """
    Extract ENROLID, SVCDATE, NDCNUM using DuckDB for a given year
    Only for records with FLAG = NULL (truly new prescriptions)
    """
    print(f"  📥 Extracting NDCNUM data for year {year} using DuckDB...")
    
    # Find corresponding prescription flags files for this year
    flag_pattern = f"prescription_flags_{year}_*_part*.parquet"
    flag_files = glob.glob(flag_pattern)
    
    if not flag_files:
        print(f"    ❌ No prescription flag files found for {year}")
        return None
    
    print(f"    Found {len(flag_files)} prescription flag files for {year}")
    
    # Use DuckDB to process all flag files efficiently
    conn = setup_duckdb_connection()
    
    try:
        print(f"    📊 Processing prescription flag files with DuckDB...")
        
        # Create a UNION query to read all flag files at once
        flag_file_list = "', '".join(flag_files)
        
        # Query to get all truly new prescriptions from all flag files
        query = f"""
        SELECT ENROLID, SVCDATE, NDCNUM
        FROM read_parquet(['{flag_file_list}'])
        WHERE FLAG IS NULL
            AND NDCNUM IS NOT NULL
        """
        
        truly_new_df = conn.execute(query).fetchdf()
        
        if len(truly_new_df) == 0:
            print(f"    ❌ No truly new prescriptions found for {year}")
            conn.close()
            return None
        
        print(f"    ✅ Total truly new prescriptions: {len(truly_new_df):,}")
        
        conn.close()
        return truly_new_df
        
    except Exception as e:
        print(f"    ❌ Error processing flag files with DuckDB: {e}")
        conn.close()
        return None

def merge_ndcnum_with_events_duckdb(year):
    """
    Merge NDCNUM data with prescription events files using DuckDB
    Filters for unique_phys_id = 1
    """
    print(f"\n{'='*60}")
    print(f"ADDING NDCNUM TO PRESCRIPTION EVENTS - YEAR {year} (DuckDB)")
    print(f"{'='*60}")
    
    # Step 1: Extract NDCNUM data
    ndcnum_data = extract_ndcnum_for_year_duckdb(year)
    
    if ndcnum_data is None:
        print(f"❌ Could not extract NDCNUM data for {year}")
        return 0  # Return 0 files processed
    
    # Step 2: Find all prescription events files for this year
    events_pattern = f"histogram_results/prescription_events_{year}_*.parquet"
    events_files = glob.glob(events_pattern)
    
    if not events_files:
        print(f"❌ No prescription events files found for {year} in histogram_results/")
        print(f"   Looking for pattern: {events_pattern}")
        # Also check current directory
        alt_pattern = f"prescription_events_{year}_*.parquet"
        alt_files = glob.glob(alt_pattern)
        if alt_files:
            print(f"   Found {len(alt_files)} files in current directory instead")
            events_files = alt_files
        else:
            print(f"   Also tried pattern: {alt_pattern}")
            return 0
    
    # Filter out files already processed
    original_count = len(events_files)
    events_files = [f for f in events_files if '_with_ndcnum' not in f]
    
    if len(events_files) != original_count:
        skipped = original_count - len(events_files)
        print(f"   ⚠️  Skipping {skipped} already processed files")
    
    if not events_files:
        print(f"   ✅ All files already processed for {year}")
        return 0
    
    print(f"📊 Found {len(events_files)} prescription events files to process for {year}")
    
    # Step 3: Process each events file efficiently with DuckDB
    merged_files = []
    conn = setup_duckdb_connection()
    
    try:
        # Register the NDCNUM data once
        conn.register('ndcnum_lookup', ndcnum_data)
        
        # Create index on the lookup table for faster joins
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ndcnum_lookup 
        ON ndcnum_lookup (ENROLID, SVCDATE)
        """)
        
        for events_file in sorted(events_files):
            filename = os.path.basename(events_file)
            print(f"\n  📝 Processing: {filename}")
            
            try:
                # Get file info
                file_size_mb = Path(events_file).stat().st_size / (1024*1024)
                print(f"    File size: {file_size_mb:.1f} MB")
                
                # Use DuckDB to read, filter, and merge in one operation
                merge_query = f"""
                SELECT 
                    e.*,
                    n.NDCNUM
                FROM read_parquet('{events_file}') e
                LEFT JOIN ndcnum_lookup n
                    ON e.ENROLID = n.ENROLID 
                    AND e.SVCDATE = n.SVCDATE
                WHERE e.unique_phys_id = 1
                """
                
                print(f"    🔄 Executing merge query with unique_phys_id = 1 filter...")
                merged_df = conn.execute(merge_query).fetchdf()
                
                # Check results
                original_count = len(merged_df)
                matched_count = merged_df['NDCNUM'].notna().sum()
                print(f"    After filtering & merge: {original_count:,} events (unique_phys_id = 1)")
                print(f"    Matched with NDCNUM: {matched_count:,} ({matched_count/original_count*100:.1f}%)")
                print(f"    Missing NDCNUM: {original_count - matched_count:,}")
                
                # Create new output filename - simplified naming
                # Extract year from filename (works for both prescription_events_2024_part01.parquet and similar patterns)
                year_match = filename.split('_')[2]  # Gets "2024" from "prescription_events_2024_part01.parquet"
                output_file = f"prescription_events_{year_match}_with_ndcnum.parquet"
                
                # Use DuckDB to write parquet directly for better compression
                write_query = f"""
                COPY (
                    SELECT 
                        e.*,
                        n.NDCNUM
                    FROM read_parquet('{events_file}') e
                    LEFT JOIN ndcnum_lookup n
                        ON e.ENROLID = n.ENROLID 
                        AND e.SVCDATE = n.SVCDATE
                    WHERE e.unique_phys_id = 1
                ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
                """
                
                conn.execute(write_query)
                merged_files.append(output_file)
                
                output_size_mb = Path(output_file).stat().st_size / (1024*1024)
                print(f"    💾 Saved: {os.path.basename(output_file)} ({output_size_mb:.1f} MB)")
                
                # Clean up
                del merged_df
                import gc
                gc.collect()
                
            except Exception as e:
                print(f"    💥 Error processing {filename}: {e}")
                import gc
                gc.collect()
        
        conn.close()
        
    except Exception as e:
        print(f"💥 Error in DuckDB processing for year {year}: {e}")
        conn.close()
    
    print(f"\n✅ Year {year} complete:")
    print(f"   Files processed: {len(merged_files)}")
    print(f"   Output files: {[os.path.basename(f) for f in merged_files]}")
    
    return len(merged_files)

def process_all_years_automatically():
    """
    Automatically process all years from 2018-2024 using optimized DuckDB
    """
    print("🚀 AUTOMATICALLY PROCESSING ALL YEARS WITH DUCKDB")
    print("=" * 80)
    print(f"Years: {START_YEAR} to {END_YEAR}")
    print(f"Filter: unique_phys_id = 1")
    print(f"Processing: File by file with DuckDB optimization")
    print("=" * 80)
    
    total_start_time = time.time()
    year_results = {}
    total_files_processed = 0
    
    for year in range(START_YEAR, END_YEAR + 1):
        year_start_time = time.time()
        print(f"\n🗓️  PROCESSING YEAR {year} ({year - START_YEAR + 1}/{END_YEAR - START_YEAR + 1})")
        
        files_processed = merge_ndcnum_with_events_duckdb(year)
        
        year_time = time.time() - year_start_time
        year_results[year] = {
            'files_processed': files_processed,
            'processing_time_minutes': year_time / 60
        }
        total_files_processed += files_processed
        
        print(f"    ⏱️  Year {year} completed in {year_time/60:.1f} minutes")
        print(f"    📊 Files processed: {files_processed}")
    
    total_time = time.time() - total_start_time
    
    # Final summary
    print(f"\n{'='*80}")
    print(f"🎉 ALL YEARS COMPLETE!")
    print(f"{'='*80}")
    print(f"⏱️  Total processing time: {total_time/60:.1f} minutes")
    print(f"📊 Total files processed: {total_files_processed}")
    print(f"🎯 Filter applied: unique_phys_id = 1")
    
    print(f"\n📋 Summary by year:")
    for year in range(START_YEAR, END_YEAR + 1):
        result = year_results[year]
        print(f"   {year}: {result['files_processed']} files, {result['processing_time_minutes']:.1f} min")
    
    # Show all output files created (updated pattern)
    print(f"\n📊 Checking output files...")
    output_files = glob.glob("prescription_events_*_with_ndcnum.parquet")
    if OUTPUT_DIR and Path(OUTPUT_DIR).exists():
        output_files.extend(glob.glob(f"{OUTPUT_DIR}/prescription_events_*_with_ndcnum.parquet"))
    
    # Remove duplicates and sort
    output_files = sorted(list(set(output_files)))
    
    print(f"📄 Output files created: {len(output_files)}")
    total_output_size_mb = 0
    
    for file in output_files:
        if Path(file).exists():
            file_size = Path(file).stat().st_size / (1024*1024)  # MB
            total_output_size_mb += file_size
            print(f"   📄 {os.path.basename(file)} ({file_size:.1f} MB)")
    
    print(f"\n📊 Total output size: {total_output_size_mb:.1f} MB")
    
    # Create processing summary
    summary_data = {
        'total_processing_time_minutes': total_time/60,
        'total_files_processed': total_files_processed,
        'total_output_files': len(output_files),
        'total_output_size_mb': total_output_size_mb,
        'years_processed': f"{START_YEAR}-{END_YEAR}",
        'filter_applied': 'unique_phys_id = 1',
        'processing_method': 'file-by-file with DuckDB'
    }
    
    # Add per-year details
    for year, result in year_results.items():
        summary_data[f'year_{year}_files'] = result['files_processed']
        summary_data[f'year_{year}_time_min'] = result['processing_time_minutes']
    
    summary_df = pd.DataFrame([summary_data])
    summary_file = 'ndcnum_merge_summary.csv'
    summary_df.to_csv(summary_file, index=False)
    print(f"\n📋 Processing summary saved: {summary_file}")
    
    print(f"\n🎉 NDCNUM merge complete for all years!")

def debug_file_locations():
    """
    Show what files exist and where before processing
    """
    print("🔍 DEBUGGING FILE LOCATIONS:")
    print("=" * 50)
    
    for year in range(START_YEAR, END_YEAR + 1):
        print(f"\nYear {year}:")
        
        # Check events files
        events_pattern1 = f"histogram_results/prescription_events_{year}_*.parquet"
        events_pattern2 = f"prescription_events_{year}_*.parquet"
        
        events1 = glob.glob(events_pattern1)
        events2 = glob.glob(events_pattern2)
        
        # Filter out already processed
        events1_new = [f for f in events1 if '_with_ndcnum' not in f]
        events2_new = [f for f in events2 if '_with_ndcnum' not in f]
        
        print(f"  📊 Events files in histogram_results/: {len(events1_new)} new, {len(events1) - len(events1_new)} already processed")
        print(f"  📊 Events files in current dir: {len(events2_new)} new, {len(events2) - len(events2_new)} already processed")
        
        # Check flag files
        flag_pattern = f"prescription_flags_{year}_*_part*.parquet"
        flag_files = glob.glob(flag_pattern)
        print(f"  📊 Flag files: {len(flag_files)}")
    
    # Check existing output files (updated pattern)
    existing_output = glob.glob("prescription_events_*_with_ndcnum.parquet")
    print(f"\n📄 Existing output files: {len(existing_output)}")
    for f in sorted(existing_output)[:10]:  # Show first 10
        print(f"   📄 {f}")
    if len(existing_output) > 10:
        print(f"   ... and {len(existing_output)-10} more")

def main():
    """
    Main function - automatically process all years
    """
    print("MarketScan Analysis - AUTO NDCNUM MERGE (ALL YEARS)")
    print("=" * 80)
    
    # First debug to see what files we have
    debug_file_locations()
    
    print(f"\n" + "="*80)
    
    # Automatically process all years
    process_all_years_automatically()

if __name__ == "__main__":
    main()
