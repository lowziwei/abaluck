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

    # Core performance settings
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.execute("SET max_temp_directory_size='50GB'")
    conn.execute("SET temp_directory='/tmp'")
    conn.execute("SET threads=8")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute("SET enable_progress_bar=false")  # Disable progress bar

    return conn

def extract_ndcnum_for_year_duckdb(year):
    """
    Extract ENROLID, SVCDATE, NDCNUM using DuckDB for a given year
    Only for records with FLAG = NULL (truly new prescriptions)
    """
    # Find corresponding prescription flags files for this year
    flag_pattern = f"prescription_flags_{year}_*_part*.parquet"
    flag_files = glob.glob(flag_pattern)

    if not flag_files:
        return None

    # Use DuckDB to process all flag files efficiently
    conn = setup_duckdb_connection()

    try:
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
        conn.close()

        if len(truly_new_df) == 0:
            return None

        return truly_new_df

    except Exception as e:
        conn.close()
        return None

def merge_ndcnum_with_events_duckdb(year):
    """
    Merge NDCNUM data with prescription events files using DuckDB
    Filters for unique_phys_id_count = 1
    """
    print(f"🗓️   PROCESSING YEAR {year}")

    # Step 1: Extract NDCNUM data (silently)
    ndcnum_data = extract_ndcnum_for_year_duckdb(year)

 # Step 2: Find all prescription events files for this year (silently)
    events_pattern = f"histogram_results/prescription_events_{year}_*.parquet"
    events_files = glob.glob(events_pattern)

    if not events_files:
        alt_pattern = f"prescription_events_{year}_*.parquet"
        events_files = glob.glob(alt_pattern)

    if not events_files:
        print(f"❌ No prescription events files found for {year}")
        return 0

    # Filter out files already processed (silently)
    events_files = [f for f in events_files if '_with_ndcnum' not in f]

    if not events_files:
        print(f"✅ All files already processed for {year}")
        return 0

    # Step 3: Process each events file
    merged_files = []
    conn = setup_duckdb_connection()

    try:
        # Register the NDCNUM data as a temporary table
        conn.register('ndcnum_lookup', ndcnum_data)

        for events_file in sorted(events_files):
            filename = os.path.basename(events_file)
            print(f"  📝 Processing: {filename}")

            try:
                # Create output filename with part number preserved
                # Extract year and part from filename like "prescription_events_2024_part01.parquet"
                print(f"     🔧 Debug: Processing filename: {filename}")
                filename_parts = filename.replace('.parquet', '').split('_')
                print(f"     🔧 Debug: Split parts: {filename_parts}")

                if len(filename_parts) >= 4:
                    year_part = filename_parts[2]  # "2024"
                    part_section = filename_parts[3]  # "part01" or "single"
                    print(f"     🔧 Debug: Year={year_part}, Part section={part_section}")

                    if part_section.startswith('part'):
                        # Extract just the number: "part01" -> "01"
                        part_number = part_section[4:]  # Remove "part" prefix
                        output_file = f"prescription_events_{year_part}_{part_number}_with_ndcnum.parquet"
                        print(f"     🔧 Debug: Created output file: {output_file}")
                    else:
                        # Handle cases like "single_physician"
                        output_file = f"prescription_events_{year_part}_{part_section}_with_ndcnum.parquet"
                        print(f"     🔧 Debug: Created output file (non-part): {output_file}")
                else:
                    # Fallback if filename format is unexpected
                    output_file = filename.replace('.parquet', '_with_ndcnum.parquet')
                    print(f"     🔧 Debug: Used fallback output: {output_file}")

                # Execute merge query
                merge_query = f"""
                SELECT 
                    e.*,
                    n.NDCNUM
                FROM read_parquet('{events_file}') e
                LEFT JOIN ndcnum_lookup n
                    ON e.ENROLID = n.ENROLID 
                    AND e.SVCDATE = n.SVCDATE
                WHERE e.unique_phys_id_count = 1
                """

                merged_df = conn.execute(merge_query).fetchdf()

                # Results
                total_events = len(merged_df)
                matched_count = merged_df['NDCNUM'].notna().sum()
                missing_count = total_events - matched_count
                match_pct = (matched_count/total_events*100) if total_events > 0 else 0

                print(f"     Events (unique_phys_id_count = 1): {total_events:,}")
                print(f"     Matched with NDCNUM: {matched_count:,} ({match_pct:.1f}%)")
                print(f"     Missing NDCNUM: {missing_count:,}")

                # Save file
                merged_df.to_parquet(output_file, compression='snappy')
                merged_files.append(output_file)

                output_size_mb = Path(output_file).stat().st_size / (1024*1024)
                print(f"     💾 Saved: {os.path.basename(output_file)} ({output_size_mb:.1f} MB)")

                # Clean up
                del merged_df
                import gc
                gc.collect()

            except Exception as e:
                print(f"     💥 Error: {e}")
                import gc
                gc.collect()

        conn.close()

    except Exception as e:
        print(f"💥 Error processing year {year}: {e}")
        conn.close()

    print(f"✅ Year {year} complete: {len(merged_files)} files processed\n")
    return len(merged_files)

def process_all_years_automatically():
    """
    Automatically process all years from 2018-2024 using optimized DuckDB
    """
    print("🚀 PROCESSING ALL YEARS (2018-2024)")
    print("=" * 50)

    total_start_time = time.time()
    total_files_processed = 0

    for year in range(START_YEAR, END_YEAR + 1):
        files_processed = merge_ndcnum_with_events_duckdb(year)
        total_files_processed += files_processed

    total_time = time.time() - total_start_time

    print(f"🎉 ALL YEARS COMPLETE!")
    print(f"⏱️   Total time: {total_time/60:.1f} minutes")
    print(f"📊 Total files processed: {total_files_processed}")

    # Show final output files
    output_files = glob.glob("prescription_events_*_with_ndcnum.parquet")
    print(f"📄 Output files: {len(output_files)}")
    for file in sorted(output_files):
        file_size = Path(file).stat().st_size / (1024*1024)
        print(f"   {os.path.basename(file)} ({file_size:.1f} MB)")

    print(f"\n✅ Processing complete!")

def main():
    """
    Main function - automatically process all years
    """
    print("MarketScan NDCNUM Merge")
    print("=" * 30)

    # Process all years
    process_all_years_automatically()

if __name__ == "__main__":
    main()
                                                                                                                                                             231,10        Bot
