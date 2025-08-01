import duckdb
import pandas as pd
import time
import gc
import glob
import os
from pathlib import Path

# Configuration
START_YEAR = 2018
END_YEAR = 2023  # Adjust as needed
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
PATIENT_CHUNK_SIZE = 200000  # Process 200k patients at a time
SAVE_EVERY = 5  # Save intermediate results every 5 chunks

def get_all_patients_for_year(conn, file_path, file_type, year=None):
    """
    Get all unique ENROLIDs for a given file/year
    """
    print(f"  Getting all unique patients from {file_type} {year if year else 'ALL'}...")
    
    # Add year filter for S files (inpatient) since they're not partitioned by year
    year_filter = f"AND YEAR = {year}" if file_type == "INPATIENT" and year else ""
    
    enrolid_query = f"""
        SELECT DISTINCT ENROLID
        FROM '{file_path}'
        WHERE ENROLID IS NOT NULL {year_filter}
        ORDER BY ENROLID
    """
    
    enrolid_df = conn.execute(enrolid_query).fetchdf()
    patient_list = enrolid_df['ENROLID'].tolist()
    
    print(f"    Found {len(patient_list):,} unique patients")
    
    del enrolid_df
    gc.collect()
    
    return patient_list

def process_patient_chunk(conn, file_path, file_type, year, chunk_patients, chunk_idx, total_chunks):
    """
    Process a single chunk of patients and return cleaned data
    """
    print(f"\n    📦 Processing chunk {chunk_idx + 1}/{total_chunks} ({len(chunk_patients):,} patients)")
    chunk_start = time.time()
    
    try:
        # Convert patient list to SQL format
        enrolid_list = "', '".join(map(str, chunk_patients))
        
        # Add year filter for S files
        year_filter = f"AND YEAR = {year}" if file_type == "INPATIENT" and year else ""
        
        # Step 1: Get PROVID-NPI mappings for this chunk
        mapping_query = f"""
        WITH chunk_data AS (
            SELECT DISTINCT PROVID, NPI
            FROM '{file_path}'
            WHERE ENROLID IN ('{enrolid_list}')
              AND PROVID IS NOT NULL 
              AND NPI IS NOT NULL
              {year_filter}
        )
        SELECT PROVID, NPI FROM chunk_data
        """
        
        chunk_mappings = conn.execute(mapping_query).fetchdf()
        
        # Step 2: Identify bad PROVIDs and NPIs within this chunk
        bad_provids = []
        bad_npis = []
        
        if len(chunk_mappings) > 0:
            # Find PROVIDs that map to multiple NPIs
            provid_counts = chunk_mappings.groupby('PROVID')['NPI'].nunique()
            bad_provids = provid_counts[provid_counts > 1].index.tolist()
            
            # Find NPIs that map to multiple PROVIDs
            npi_counts = chunk_mappings.groupby('NPI')['PROVID'].nunique()
            bad_npis = npi_counts[npi_counts > 1].index.tolist()
        
        # Step 3: Get cleaned data for this chunk
        exclusion_conditions = [f"ENROLID IN ('{enrolid_list}')"]
        
        if bad_provids:
            bad_provids_sql = "', '".join(map(str, bad_provids))
            exclusion_conditions.append(f"PROVID NOT IN ('{bad_provids_sql}')")
        
        if bad_npis:
            bad_npis_sql = "', '".join(map(str, bad_npis))
            exclusion_conditions.append(f"NPI NOT IN ('{bad_npis_sql}')")
        
        if file_type == "INPATIENT" and year:
            exclusion_conditions.append(f"YEAR = {year}")
        
        where_clause = "WHERE " + " AND ".join(exclusion_conditions)
        
        # Get cleaned chunk data with PHYS_ID
        cleaned_query = f"""
        SELECT *,
               COALESCE(NPI, PROVID) as PHYS_ID
        FROM '{file_path}'
        {where_clause}
        """
        
        chunk_df = conn.execute(cleaned_query).fetchdf()
        
        chunk_time = time.time() - chunk_start
        
        # Calculate stats for this chunk
        original_count_query = f"""
        SELECT COUNT(*) as count 
        FROM '{file_path}' 
        WHERE ENROLID IN ('{enrolid_list}') {year_filter}
        """
        original_count = conn.execute(original_count_query).fetchone()[0]
        
        records_removed = original_count - len(chunk_df)
        
        print(f"      Original records: {original_count:,}")
        print(f"      Cleaned records: {len(chunk_df):,}")
        print(f"      Records removed: {records_removed:,}")
        print(f"      Bad PROVIDs: {len(bad_provids)}")
        print(f"      Bad NPIs: {len(bad_npis)}")
        print(f"      Chunk time: {chunk_time:.1f}s")
        
        # Clean up
        del chunk_mappings, chunk_patients
        gc.collect()
        
        return {
            'data': chunk_df,
            'stats': {
                'original_records': original_count,
                'cleaned_records': len(chunk_df),
                'records_removed': records_removed,
                'bad_provids': len(bad_provids),
                'bad_npis': len(bad_npis),
                'processing_time': chunk_time
            }
        }
        
    except Exception as e:
        print(f"      ERROR in chunk {chunk_idx + 1}: {e}")
        return None

def clean_provider_mappings_chunked(file_path, file_type, year=None):
    """
    Clean provider mappings for a single file using patient chunks
    """
    print(f"\n{'='*80}")
    print(f"CHUNKED CLEANING - {file_type} {year if year else 'ALL_YEARS'}")
    print(f"File: {file_path}")
    print(f"Chunk size: {PATIENT_CHUNK_SIZE:,} patients")
    print(f"{'='*80}")
    
    if not Path(file_path).exists():
        print(f"ERROR: File does not exist: {file_path}")
        return False
    
    # Initialize DuckDB
    conn = duckdb.connect()
    conn.execute("SET memory_limit='28GB'")  # Leave some buffer
    conn.execute("SET threads=4")
    
    start_time = time.time()
    
    try:
        # Step 1: Get all patients for this file/year
        print(f"\nStep 1: Getting all patients...")
        all_patients = get_all_patients_for_year(conn, file_path, file_type, year)
        
        if not all_patients:
            print("No patients found!")
            return False
        
        # Step 2: Create patient chunks
        chunks = [all_patients[i:i + PATIENT_CHUNK_SIZE] 
                 for i in range(0, len(all_patients), PATIENT_CHUNK_SIZE)]
        
        print(f"\nStep 2: Created {len(chunks)} chunks of {PATIENT_CHUNK_SIZE:,} patients each")
        print(f"  Total patients: {len(all_patients):,}")
        print(f"  Last chunk size: {len(chunks[-1]):,}")
        
        # Clean up patient list
        del all_patients
        gc.collect()
        
        # Step 3: Process chunks and save intermediate results
        print(f"\nStep 3: Processing {len(chunks)} chunks...")
        
        all_chunk_results = []
        batch_results = []
        batch_number = 0
        total_stats = {
            'original_records': 0,
            'cleaned_records': 0,
            'records_removed': 0,
            'bad_provids': 0,
            'bad_npis': 0
        }
        
        # Clean up any existing intermediate files
        base_name = f"{file_type.lower()}_{year if year else 'all'}"
        intermediate_pattern = f'provider_cleaned_batch_{base_name}_*.parquet'
        existing_files = glob.glob(intermediate_pattern)
        for f in existing_files:
            os.remove(f)
            
        for chunk_idx, chunk_patients in enumerate(chunks):
            chunk_result = process_patient_chunk(
                conn, file_path, file_type, year, chunk_patients, chunk_idx, len(chunks)
            )
            
            if chunk_result is not None:
                batch_results.append(chunk_result['data'])
                
                # Update total stats
                for key in total_stats:
                    total_stats[key] += chunk_result['stats'][key]
                
                # Save intermediate results every SAVE_EVERY chunks
                if (chunk_idx + 1) % SAVE_EVERY == 0 or (chunk_idx + 1) == len(chunks):
                    batch_number += 1
                    batch_start_chunk = max(0, chunk_idx + 1 - SAVE_EVERY)
                    batch_end_chunk = chunk_idx + 1
                    
                    print(f"    💾 Saving batch {batch_number} (chunks {batch_start_chunk + 1}-{batch_end_chunk})...")
                    
                    if batch_results:
                        # Combine batch data
                        batch_df = pd.concat(batch_results, ignore_index=True)
                        batch_file = f'provider_cleaned_batch_{base_name}_{batch_number:03d}.parquet'
                        batch_df.to_parquet(batch_file, compression='snappy')
                        
                        print(f"       Saved {len(batch_df):,} records to {batch_file}")
                        
                        # Clear batch data and force cleanup
                        del batch_df, batch_results
                        batch_results = []
                        gc.collect()
            
            # Clean up chunk data
            if chunk_result:
                del chunk_result
            del chunk_patients
            gc.collect()
        
        # Step 4: Combine all intermediate files
        print(f"\nStep 4: Combining intermediate batch files...")
        
        intermediate_files = sorted(glob.glob(f'provider_cleaned_batch_{base_name}_*.parquet'))
        print(f"  Found {len(intermediate_files)} batch files to combine")
        
        if not intermediate_files:
            print("ERROR: No intermediate files found!")
            return False
        
        # Combine all batch files
        all_dfs = []
        for batch_file in intermediate_files:
            print(f"  Loading {batch_file}...")
            batch_df = pd.read_parquet(batch_file)
            all_dfs.append(batch_df)
            print(f"    Loaded {len(batch_df):,} records")
        
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"  Combined total: {len(final_df):,} records")
        
        # Clean up intermediate data
        del all_dfs
        gc.collect()
        
        # Step 5: Generate final statistics
        print(f"\nStep 5: Final statistics...")
        
        final_stats = {
            'total_records': len(final_df),
            'unique_provids': final_df['PROVID'].nunique(),
            'unique_npis': final_df['NPI'].nunique(),
            'unique_phys_ids': final_df['PHYS_ID'].nunique(),
            'records_with_provid': final_df['PROVID'].notna().sum(),
            'records_with_npi': final_df['NPI'].notna().sum(),
            'records_with_both': ((final_df['PROVID'].notna()) & (final_df['NPI'].notna())).sum(),
            'phys_id_from_npi': (final_df['PHYS_ID'] == final_df['NPI']).sum(),
            'phys_id_from_provid': ((final_df['PHYS_ID'] == final_df['PROVID']) & (final_df['NPI'].isna())).sum()
        }
        
        removal_rate = (total_stats['records_removed'] / total_stats['original_records']) * 100
        
        print(f"  Final statistics:")
        print(f"    Total records: {final_stats['total_records']:,}")
        print(f"    Records removed: {total_stats['records_removed']:,} ({removal_rate:.2f}%)")
        print(f"    Unique PROVIDs: {final_stats['unique_provids']:,}")
        print(f"    Unique NPIs: {final_stats['unique_npis']:,}")
        print(f"    Unique PHYS_IDs: {final_stats['unique_phys_ids']:,}")
        print(f"    PHYS_ID from NPI: {final_stats['phys_id_from_npi']:,}")
        print(f"    PHYS_ID from PROVID: {final_stats['phys_id_from_provid']:,}")
        
        # Step 6: Save final cleaned file
        print(f"\nStep 6: Saving final cleaned file...")
        
        if file_type == "INPATIENT":
            output_path = file_path.replace('.parquet', f'_cleaned_{year}.parquet')
        else:
            output_path = file_path.replace('.parquet', '_cleaned.parquet')
        
        final_df.to_parquet(output_path, compression='snappy')
        
        # Clean up intermediate files
        print(f"  Cleaning up {len(intermediate_files)} intermediate files...")
        for batch_file in intermediate_files:
            os.remove(batch_file)
        
        processing_time = time.time() - start_time
        
        print(f"  Final file saved: {output_path}")
        print(f"  Total processing time: {processing_time/60:.1f} minutes")
        
        # Create summary data
        summary_data = {
            'file_type': file_type,
            'year': year if year else 'ALL',
            'total_patients': len(all_patients) if 'all_patients' in locals() else 0,
            'chunks_processed': len(chunks),
            'original_records': total_stats['original_records'],
            'cleaned_records': final_stats['total_records'],
            'records_removed': total_stats['records_removed'],
            'removal_rate_pct': removal_rate,
            'bad_provids_total': total_stats['bad_provids'],
            'bad_npis_total': total_stats['bad_npis'],
            'unique_phys_ids': final_stats['unique_phys_ids'],
            'phys_id_from_npi': final_stats['phys_id_from_npi'],
            'phys_id_from_provid': final_stats['phys_id_from_provid'],
            'output_file': output_path,
            'processing_time_minutes': processing_time/60
        }
        
        return summary_data
        
    except Exception as e:
        print(f"ERROR processing {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        try:
            conn.close()
        except:
            pass
        gc.collect()

def process_all_years_chunked():
    """
    Process all years and file types using chunked processing
    """
    print("CHUNKED PROVIDER DATA CLEANING - ALL YEARS")
    print("=" * 80)
    print(f"Patient chunk size: {PATIENT_CHUNK_SIZE:,}")
    print(f"Save intermediate results every: {SAVE_EVERY} chunks")
    print("=" * 80)
    
    # Track summary statistics
    all_summaries = []
    
    # Process S file (inpatient) - not partitioned by year
    s_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_S.parquet"
    
    if Path(s_file).exists():
        print(f"\n🔄 PROCESSING INPATIENT FILE (S) - ALL YEARS")
        print("=" * 50)
        
        for year in range(START_YEAR, END_YEAR + 1):
            print(f"\n📅 Processing INPATIENT year {year}")
            summary = clean_provider_mappings_chunked(s_file, "INPATIENT", year)
            if summary:
                all_summaries.append(summary)
    else:
        print(f"⚠️  INPATIENT FILE NOT FOUND: {s_file}")
    
    # Process O files (outpatient) - partitioned by year
    print(f"\n🔄 PROCESSING OUTPATIENT FILES (O) - BY YEAR")
    print("=" * 50)
    
    for year in range(START_YEAR, END_YEAR + 1):
        o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"
        
        if Path(o_file).exists():
            print(f"\n📅 Processing OUTPATIENT year {year}")
            summary = clean_provider_mappings_chunked(o_file, "OUTPATIENT", year)
            if summary:
                all_summaries.append(summary)
        else:
            print(f"⚠️  SKIPPING: {o_file} (file not found)")
    
    # Create overall summary report
    if all_summaries:
        print(f"\n{'='*80}")
        print("OVERALL SUMMARY REPORT")
        print(f"{'='*80}")
        
        summary_df = pd.DataFrame(all_summaries)
        
        # Overall totals
        total_patients = summary_df['total_patients'].sum()
        total_chunks = summary_df['chunks_processed'].sum()
        total_original = summary_df['original_records'].sum()
        total_cleaned = summary_df['cleaned_records'].sum()
        total_removed = summary_df['records_removed'].sum()
        overall_removal_rate = (total_removed / total_original) * 100
        total_time = summary_df['processing_time_minutes'].sum()
        
        print(f"\nOverall Statistics:")
        print(f"  Files processed: {len(all_summaries)}")
        print(f"  Total patients: {total_patients:,}")
        print(f"  Total chunks: {total_chunks:,}")
        print(f"  Total original records: {total_original:,}")
        print(f"  Total cleaned records: {total_cleaned:,}")
        print(f"  Total records removed: {total_removed:,} ({overall_removal_rate:.2f}%)")
        print(f"  Total processing time: {total_time:.1f} minutes")
        
        # Save detailed summary
        summary_file = f'provider_cleaning_chunked_summary_{START_YEAR}_{END_YEAR}.csv'
        summary_df.to_csv(summary_file, index=False)
        print(f"\nDetailed summary saved: {summary_file}")
        
        # Display year-by-year summary
        print(f"\nYear-by-Year Summary:")
        print("Year | File Type | Patients | Chunks | Original | Cleaned | Removed | Rate% | Time(min)")
        print("-" * 100)
        for _, row in summary_df.iterrows():
            print(f"{row['year']} | {row['file_type']:9} | {row['total_patients']:8,} | {row['chunks_processed']:6} | {row['original_records']:8,} | {row['cleaned_records']:7,} | {row['records_removed']:7,} | {row['removal_rate_pct']:5.1f}% | {row['processing_time_minutes']:8.1f}")
    
    print(f"\n{'='*80}")
    print("🎉 CHUNKED PROVIDER DATA CLEANING COMPLETE!")
    print(f"{'='*80}")

def main():
    """
    Main function to run the chunked provider data cleaning
    """
    print("MarketScan Provider Data Cleaning Script - CHUNKED VERSION")
    print("=" * 80)
    print(f"Processing years: {START_YEAR} to {END_YEAR}")
    print(f"Dataset: {DATASET_TYPE}/{DATABASE}")
    print(f"Memory constraint: 30GB (using 28GB with buffer)")
    print(f"Patient chunk size: {PATIENT_CHUNK_SIZE:,}")
    print("File structure:")
    print(f"  - S (Inpatient): Single file, filter by YEAR column")
    print(f"  - O (Outpatient): Separate files per year")
    print("=" * 80)
    
    # Process all years with chunking
    process_all_years_chunked()
    
    print("\n🎉 All chunked processing complete!")
    print("\nMemory-efficient processing complete!")
    print("Next steps:")
    print("  1. Use the cleaned files with '_cleaned' suffix for your analysis")
    print("  2. Use the PHYS_ID field instead of separate PROVID/NPI fields")
    print("  3. Check the summary CSV for detailed statistics")

if __name__ == "__main__":
    main()
