import pandas as pd
import glob
import os
from pathlib import Path
import time

def combine_events_by_year():
    """
    Combine prescription event files by year, BUT ONLY for events with exactly 1 physician
    """
    
    print("MarketScan Analysis - COMBINE EVENTS BY YEAR (SINGLE PHYSICIAN ONLY)")
    print("=" * 70)
    print("🔍 Filtering for unique_phys_id_count == 1 only")
    
    # Find all event files
    events_pattern = "histogram_results/prescription_events_*.parquet"
    event_files = glob.glob(events_pattern)
    
    if not event_files:
        print("❌ No event files found!")
        print(f"   Looking for pattern: {events_pattern}")
        return
    
    print(f"📊 Found {len(event_files)} event files")
    
    # Group files by year
    files_by_year = {}
    
    for file_path in event_files:
        filename = os.path.basename(file_path)
        
        # Extract year from filename: prescription_events_2018_part01.parquet
        try:
            parts = filename.split('_')
            year = int(parts[2])  # Should be the year
            
            if year not in files_by_year:
                files_by_year[year] = []
            files_by_year[year].append(file_path)
            
        except (IndexError, ValueError) as e:
            print(f"   ⚠️  Could not parse year from filename: {filename}")
    
    if not files_by_year:
        print("❌ No files could be grouped by year!")
        return
    
    print(f"📅 Found data for years: {sorted(files_by_year.keys())}")
    
    # Process each year
    for year in sorted(files_by_year.keys()):
        year_files = sorted(files_by_year[year])
        print(f"\n🔄 Processing year {year} ({len(year_files)} files)...")
        
        year_start_time = time.time()
        all_year_data = []
        records_processed = 0
        single_phys_records = 0
        
        # Load all files for this year
        for i, file_path in enumerate(year_files, 1):
            filename = os.path.basename(file_path)
            
            try:
                print(f"   📥 Loading file {i}/{len(year_files)}: {filename}")
                df = pd.read_parquet(file_path)
                
                # FILTER: Only keep records with exactly 1 physician
                original_count = len(df)
                df_filtered = df[df['unique_phys_id_count'] == 1].copy()
                filtered_count = len(df_filtered)
                
                if filtered_count > 0:
                    all_year_data.append(df_filtered)
                    single_phys_records += filtered_count
                
                records_processed += original_count
                
                print(f"      Original records: {original_count:,}")
                print(f"      Single physician: {filtered_count:,} ({filtered_count/original_count*100:.1f}%)")
                
                # Show progress every 5 files
                if i % 5 == 0 or i == len(year_files):
                    print(f"      Total single-phys records so far: {single_phys_records:,}")
                
            except Exception as e:
                print(f"      💥 Error loading {filename}: {e}")
        
        if not all_year_data:
            print(f"   ❌ No single-physician data found for year {year}")
            continue
        
        # Combine all single-physician data for this year
        print(f"   🔗 Combining {len(all_year_data)} files (single-physician only)...")
        combined_year_df = pd.concat(all_year_data, ignore_index=True)
        
        # Sort by ENROLID and SVCDATE for better organization
        print(f"   📊 Sorting {len(combined_year_df):,} single-physician records...")
        combined_year_df = combined_year_df.sort_values(['ENROLID', 'SVCDATE']).reset_index(drop=True)
        
        # Save combined file
        output_file = f"histogram_results/prescription_events_{year}_single_physician.parquet"
        print(f"   💾 Saving to: {output_file}")
        
        combined_year_df.to_parquet(output_file, compression='snappy')
        
        # Get file size
        file_size_mb = Path(output_file).stat().st_size / (1024 * 1024)
        year_time = time.time() - year_start_time
        
        print(f"   ✅ Year {year} complete:")
        print(f"      Files processed: {len(year_files)}")
        print(f"      Total original records: {records_processed:,}")
        print(f"      Single-physician records: {len(combined_year_df):,} ({len(combined_year_df)/records_processed*100:.1f}%)")
        print(f"      File size: {file_size_mb:.1f} MB")
        print(f"      Processing time: {year_time:.1f} seconds")
        
        # Show sample of data structure
        print(f"   📋 Data structure preview:")
        print(f"      Columns: {list(combined_year_df.columns)}")
        print(f"      Date range: {combined_year_df['SVCDATE'].min()} to {combined_year_df['SVCDATE'].max()}")
        print(f"      Unique patients: {combined_year_df['ENROLID'].nunique():,}")
        
        # Show physician ID info
        if 'phys_ids' in combined_year_df.columns:
            unique_physicians = combined_year_df['phys_ids'].nunique()
            print(f"      Unique physicians involved: {unique_physicians:,}")
        
        # Clean up memory
        del all_year_data, combined_year_df
        import gc
        gc.collect()
    
    print(f"\n✅ ALL YEARS PROCESSED (SINGLE PHYSICIAN ONLY)!")
    print("=" * 50)
    
    # Show final summary
    combined_files = glob.glob("histogram_results/prescription_events_*_single_physician.parquet")
    print(f"📁 Single-physician files created: {len(combined_files)}")
    
    total_size = 0
    total_records = 0
    
    for file_path in sorted(combined_files):
        filename = os.path.basename(file_path)
        file_size = Path(file_path).stat().st_size / (1024 * 1024)  # MB
        total_size += file_size
        
        # Quick count of records
        try:
            df_info = pd.read_parquet(file_path, columns=['ENROLID'])
            record_count = len(df_info)
            total_records += record_count
            print(f"   📄 {filename}: {record_count:,} records, {file_size:.1f} MB")
        except:
            print(f"   📄 {filename}: {file_size:.1f} MB")
    
    print(f"\n📊 OVERALL SUMMARY (SINGLE PHYSICIAN ONLY):")
    print(f"   Total combined files: {len(combined_files)}")
    print(f"   Total single-physician records: {total_records:,}")
    print(f"   Total size: {total_size:.1f} MB")
    print(f"   Average records per year: {total_records/len(combined_files):,.0f}")
    
    print(f"\n🗂️  FILE NAMING CONVENTION:")
    print(f"   prescription_events_YYYY_single_physician.parquet")
    print(f"   Example: prescription_events_2018_single_physician.parquet")
    
    print(f"\n🔒 All data remains secure on server")

def show_year_file_info():
    """
    Display info about the single-physician combined files
    """
    print(f"\n📋 SINGLE-PHYSICIAN FILES INFORMATION:")
    print("=" * 55)
    
    combined_files = glob.glob("histogram_results/prescription_events_*_single_physician.parquet")
    
    if not combined_files:
        print("❌ No single-physician combined files found")
        return
    
    for file_path in sorted(combined_files):
        filename = os.path.basename(file_path)
        
        try:
            # Get basic info without loading full file
            df_sample = pd.read_parquet(file_path, columns=['ENROLID', 'SVCDATE', 'unique_phys_id_count', 'phys_ids'])
            
            year = filename.split('_')[2]
            record_count = len(df_sample)
            unique_patients = df_sample['ENROLID'].nunique()
            date_range = f"{df_sample['SVCDATE'].min()} to {df_sample['SVCDATE'].max()}"
            
            # Verify all are single physician
            single_phys_check = (df_sample['unique_phys_id_count'] == 1).all()
            unique_physicians = df_sample['phys_ids'].nunique() if 'phys_ids' in df_sample.columns else 0
            
            print(f"📅 Year {year}:")
            print(f"   Records: {record_count:,}")
            print(f"   Patients: {unique_patients:,}")
            print(f"   Date range: {date_range}")
            print(f"   All single physician: {'✅' if single_phys_check else '❌'}")
            print(f"   Unique physicians: {unique_physicians:,}")
            print()
            
        except Exception as e:
            print(f"   ⚠️  Error reading {filename}: {e}")

def main():
    """
    Main function to combine events by year
    """
    combine_events_by_year()
    show_year_file_info()

if __name__ == "__main__":
    main()
