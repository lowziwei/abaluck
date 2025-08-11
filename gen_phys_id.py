import duckdb
import pandas as pd
import time
import gc
import glob
import os
from pathlib import Path

# Configuration
START_YEAR = 2018
END_YEAR = 2024  # Just 2018 first to test
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
PATIENT_CHUNK_SIZE = 100000  # Process 100k patients at a time (smaller for safety)
SAVE_EVERY = 5  # Save intermediate results every 5 chunks
OUTPUT_DIR = "/home/zl749/processed_marketscan"  # Recommended: Local filesystem with 442GB free

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

def identify_bad_provids(conn, file_path, file_type, year=None):
    """
    Identify truly ambiguous PROVIDs (map to 2+ different NPIs) across entire file
    """
    print(f"  Step 1: Identifying truly ambiguous PROVIDs across entire file...")

    year_filter = f"AND YEAR = {year}" if file_type == "INPATIENT" and year else ""

    # Find PROVIDs that map to multiple DIFFERENT NPIs (truly ambiguous)
    bad_provids_query = f"""
    WITH provid_npi_mapping AS (
        SELECT DISTINCT PROVID, NPI
        FROM '{file_path}'
        WHERE PROVID IS NOT NULL {year_filter}
    ),
    provid_npi_counts AS (
        SELECT 
            PROVID, 
            COUNT(DISTINCT NPI) FILTER (WHERE NPI IS NOT NULL) as distinct_npi_count
        FROM provid_npi_mapping
        GROUP BY PROVID
    )
    SELECT PROVID
    FROM provid_npi_counts
    WHERE distinct_npi_count > 1  -- Only problematic if maps to 2+ different NPIs
    """

    bad_provids_df = conn.execute(bad_provids_query).fetchdf()
    bad_provids = bad_provids_df['PROVID'].tolist() if len(bad_provids_df) > 0 else []

    print(f"    Found {len(bad_provids):,} truly ambiguous PROVIDs to exclude")
    print(f"    (PROVIDs mapping to 2+ different NPIs)")

    return bad_provids

def process_patient_chunk(conn, file_path, file_type, year, chunk_patients, chunk_idx, total_chunks, bad_provids):
    """
    Process a single chunk of patients with clean PHYS_ID logic
    """
    print(f"\n    📦 Processing chunk {chunk_idx + 1}/{total_chunks} ({len(chunk_patients):,} patients)")
    chunk_start = time.time()

    try:
        # Convert patient list to SQL format
enrolid_list = "', '".join(map(str, chunk_patients))

        # Add year filter for S files
        year_filter = f"AND YEAR = {year}" if file_type == "INPATIENT" and year else ""

        # Convert bad PROVIDs list to SQL format
        if bad_provids:
            bad_provids_sql = "', '".join(map(str, bad_provids))
            provid_filter = f"AND PROVID NOT IN ('{bad_provids_sql}')"
        else:
            provid_filter = ""

        # Get cleaned chunk data with PHYS_ID
        cleaned_query = f"""
        SELECT *,
               CASE 
                   WHEN NPI IS NOT NULL THEN CAST(NPI AS VARCHAR)
                   WHEN PROVID IS NOT NULL {provid_filter} THEN CAST(PROVID AS VARCHAR)
                   ELSE NULL 
               END as PHYS_ID
        FROM '{file_path}'
        WHERE ENROLID IN ('{enrolid_list}') {year_filter}
        """

        chunk_df = conn.execute(cleaned_query).fetchdf()

        chunk_time = time.time() - chunk_start

        # Calculate stats for this chunk
        total_records = len(chunk_df)
        records_with_phys_id = chunk_df['PHYS_ID'].notna().sum()
        phys_id_from_npi = (chunk_df['NPI'].notna()).sum()
        phys_id_from_provid = ((chunk_df['NPI'].isna()) & (chunk_df['PHYS_ID'].notna())).sum()

        print(f"      Total records: {total_records:,}")
        print(f"      Records with PHYS_ID: {records_with_phys_id:,} ({records_with_phys_id/total_records*100:.1f}%)")
        print(f"      PHYS_ID from NPI: {phys_id_from_npi:,}")
        print(f"      PHYS_ID from clean PROVID: {phys_id_from_provid:,}")
        print(f"      Chunk time: {chunk_time:.1f}s")

        # Clean up
        del chunk_patients
        gc.collect()

        return {
            'data': chunk_df,
            'stats': {
                'total_records': total_records,
                'records_with_phys_id': records_with_phys_id,
                'phys_id_from_npi': phys_id_from_npi,
                'phys_id_from_provid': phys_id_from_provid,
                'processing_time': chunk_time
            }
        }

    except Exception as e:
        print(f"      ERROR in chunk {chunk_idx + 1}: {e}")
        return None

def create_clean_phys_id_chunked(file_path, file_type, year=None):
    """
    Create clean PHYS_ID using patient chunking and selective PROVID removal
    """
    print(f"\n{'='*80}")
    print(f"CHUNKED CLEAN PHYS_ID CREATION - {file_type} {year if year else 'ALL'}")
    print(f"File: {file_path}")
    print(f"Chunk size: {PATIENT_CHUNK_SIZE:,} patients")
    print(f"Strategy: Drop only truly ambiguous PROVIDs, NPI > clean PROVID")
    print(f"{'='*80}")

    if not Path(file_path).exists():
        print(f"ERROR: File does not exist: {file_path}")
        return None

    # Initialize DuckDB
    conn = duckdb.connect()
    conn.execute("SET memory_limit='28GB'")  # Leave some buffer
    conn.execute("SET threads=4")

    start_time = time.time()

    try:
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"  Output directory: {OUTPUT_DIR}")

        # Step 1: Identify bad PROVIDs across entire file (small operation)
bad_provids = identify_bad_provids(conn, file_path, file_type, year)

        # Step 2: Get all patients for this file/year
        print(f"\nStep 2: Getting all patients...")
        all_patients = get_all_patients_for_year(conn, file_path, file_type, year)

        if not all_patients:
            print("No patients found!")
            return None

        # Step 3: Create patient chunks
        chunks = [all_patients[i:i + PATIENT_CHUNK_SIZE]
                 for i in range(0, len(all_patients), PATIENT_CHUNK_SIZE)]

        print(f"\nStep 3: Created {len(chunks)} chunks of {PATIENT_CHUNK_SIZE:,} patients each")
        print(f"  Total patients: {len(all_patients):,}")
        print(f"  Last chunk size: {len(chunks[-1]):,}")

        # Clean up patient list
        del all_patients
        gc.collect()

        # Step 4: Process chunks and save intermediate results
        print(f"\nStep 4: Processing {len(chunks)} chunks...")

        batch_results = []
        batch_number = 0
        total_stats = {
            'total_records': 0,
            'records_with_phys_id': 0,
            'phys_id_from_npi': 0,
            'phys_id_from_provid': 0
        }

        # Clean up any existing intermediate files
        base_name = f"{file_type.lower()}_{year if year else 'all'}"
        intermediate_pattern = f'{OUTPUT_DIR}/clean_phys_id_batch_{base_name}_*.parquet'
        existing_files = glob.glob(intermediate_pattern)
        for f in existing_files:
            os.remove(f)

        for chunk_idx, chunk_patients in enumerate(chunks):
            chunk_result = process_patient_chunk(
                conn, file_path, file_type, year, chunk_patients, chunk_idx, len(chunks), bad_provids
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
                        batch_file = f'{OUTPUT_DIR}/clean_phys_id_batch_{base_name}_{batch_number:03d}.parquet'
                        batch_df.to_parquet(batch_file, compression='snappy')

                        print(f"       Saved {len(batch_df):,} records to {batch_file}")

                        # Clear batch data and force cleanup
                        del batch_df, batch_results
                        batch_results = []
                        gc.collect()

            # Clean up chunk data
            if chunk_result:
                del chunk_result
            gc.collect()

        # Step 5: Generate final statistics from batch files
        print(f"\nStep 5: Generating final statistics from batch files...")

        intermediate_files = sorted(glob.glob(f'{OUTPUT_DIR}/clean_phys_id_batch_{base_name}_*.parquet'))
        print(f"  Found {len(intermediate_files)} batch files")
print(f"  Found {len(intermediate_files)} batch files")

        if not intermediate_files:
            print("ERROR: No intermediate files found!")
            return None

        # Calculate final statistics by reading batch files one at a time
        final_stats = {
            'total_records': 0,
            'records_with_phys_id': 0,
            'phys_id_from_npi': 0,
            'phys_id_from_provid': 0,
            'no_phys_id': 0,
            'unique_phys_ids': set(),
            'unique_npis': set(),
            'unique_provids': set(),
            'bad_provids_excluded': len(bad_provids)
        }

        print(f"  Calculating statistics from {len(intermediate_files)} batch files...")
        for i, batch_file in enumerate(intermediate_files):
            batch_df = pd.read_parquet(batch_file)

            final_stats['total_records'] += len(batch_df)
            final_stats['records_with_phys_id'] += batch_df['PHYS_ID'].notna().sum()
            final_stats['phys_id_from_npi'] += (batch_df['NPI'].notna()).sum()
            final_stats['phys_id_from_provid'] += ((batch_df['NPI'].isna()) & (batch_df['PHYS_ID'].notna())).sum()
            final_stats['no_phys_id'] += batch_df['PHYS_ID'].isna().sum()

            # Add to sets for unique counts
            final_stats['unique_phys_ids'].update(batch_df['PHYS_ID'].dropna().unique())
            final_stats['unique_npis'].update(batch_df['NPI'].dropna().unique())
            final_stats['unique_provids'].update(batch_df['PROVID'].dropna().unique())

            del batch_df
            gc.collect()
            print(f"    Processed batch {i+1}/{len(intermediate_files)}")

        # Convert sets to counts
        final_stats['unique_phys_ids'] = len(final_stats['unique_phys_ids'])
        final_stats['unique_npis'] = len(final_stats['unique_npis'])
        final_stats['unique_provids'] = len(final_stats['unique_provids'])

        print(f"  Final statistics:")
        print(f"    Total records: {final_stats['total_records']:,}")
        print(f"    Records with PHYS_ID: {final_stats['records_with_phys_id']:,} ({final_stats['records_with_phys_id']/final_stats['total_records']*100:.1f}%)")
        print(f"    PHYS_ID from NPI: {final_stats['phys_id_from_npi']:,} ({final_stats['phys_id_from_npi']/final_stats['total_records']*100:.1f}%)")
        print(f"    PHYS_ID from clean PROVID: {final_stats['phys_id_from_provid']:,} ({final_stats['phys_id_from_provid']/final_stats['total_records']*100:.1f}%)")
        print(f"    No PHYS_ID available: {final_stats['no_phys_id']:,} ({final_stats['no_phys_id']/final_stats['total_records']*100:.1f}%)")
        print(f"    Unique PHYS_IDs: {final_stats['unique_phys_ids']:,}")
        print(f"    Bad PROVIDs excluded: {final_stats['bad_provids_excluded']:,}")

        # Step 6: Keep batch files as final output (no combination needed)
        print(f"\nStep 6: Batch files kept as final output...")
        print(f"  Final output: {len(intermediate_files)} batch files in {OUTPUT_DIR}")
        print(f"  File pattern: clean_phys_id_batch_{base_name}_*.parquet")
        print(f"  No file combination - batch files can be used directly")

        processing_time = time.time() - start_time

        print(f"  Processing complete!")
        print(f"  Output: {len(intermediate_files)} batch files in {OUTPUT_DIR}")
        print(f"  Total processing time: {processing_time/60:.1f} minutes")

        # Create summary data
        summary_data = {
            'file_type': file_type,
            'year': year if year else 'ALL',
            'total_patients': len(all_patients) if 'all_patients' in locals() else 0,
            'chunks_processed': len(chunks),
            'output_files': len(intermediate_files),
            'output_pattern': f"clean_phys_id_batch_{base_name}_*.parquet",
            'processing_time_minutes': processing_time/60,
            **final_stats
        }

        return summary_data

    except Exception as e:
        print(f"ERROR processing {file_path}: {e}")
        import traceback
        traceback.print_exc()

finally:
        try:
            conn.close()
        except:
            pass
        gc.collect()

def analyze_provid_consistency_preview():
    """
    Quick analysis of PROVID→NPI consistency (before full processing)
    """
    print(f"\n{'='*60}")
    print("PROVID→NPI CONSISTENCY PREVIEW")
    print(f"{'='*60}")

    conn = duckdb.connect()

    # Just check one year from S file for preview
    s_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_S.parquet"
    if Path(s_file).exists():
        print(f"\nPreview from Inpatient {START_YEAR}:")

        try:
            consistency_query = f"""
            WITH provid_npi_mapping AS (
                SELECT DISTINCT PROVID, NPI
                FROM '{s_file}'
                WHERE PROVID IS NOT NULL AND YEAR = {START_YEAR}
            ),
            provid_consistency AS (
                SELECT 
                    PROVID,
                    COUNT(DISTINCT NPI) FILTER (WHERE NPI IS NOT NULL) as distinct_npi_count,
                    COUNT(*) FILTER (WHERE NPI IS NULL) as null_count
                FROM provid_npi_mapping
                GROUP BY PROVID
            )
            SELECT 
                'Consistent (1 NPI only)' as mapping_type,
                COUNT(*) as provid_count
            FROM provid_consistency 
            WHERE distinct_npi_count = 1 AND null_count = 0
            
            UNION ALL
            
            SELECT 
                'Mixed (1 NPI + missing)' as mapping_type,
                COUNT(*) as provid_count
            FROM provid_consistency 
            WHERE distinct_npi_count = 1 AND null_count > 0
            
            UNION ALL
            
            SELECT 
                'Missing NPI only' as mapping_type,
                COUNT(*) as provid_count
            FROM provid_consistency 
            WHERE distinct_npi_count = 0
            
            UNION ALL
            
            SELECT 
                'Ambiguous (Multiple NPIs)' as mapping_type,
                COUNT(*) as provid_count
            FROM provid_consistency 
            WHERE distinct_npi_count > 1
            
            ORDER BY mapping_type
            """

            result = conn.execute(consistency_query).fetchdf()
            for _, row in result.iterrows():
                status = "✅ KEPT" if "Ambiguous" not in row['mapping_type'] else "❌ DROPPED"
                print(f"    {row['mapping_type']}: {row['provid_count']:,} PROVIDs {status}")

        except Exception as e:
            print(f"  Error in preview: {e}")

    conn.close()

def process_all_files_chunked():
    """
    Process all files with chunked clean PHYS_ID creation
    """
    print("CHUNKED CLEAN PHYS_ID CREATION - ALL FILES")
    print("=" * 80)

# First, show preview of what will be kept vs dropped
    analyze_provid_consistency_preview()

    all_summaries = []

    # Process S file (inpatient)
    s_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_S.parquet"
    if Path(s_file).exists():
        print(f"\n🔄 PROCESSING INPATIENT FILE (S)")
        print("=" * 40)

        for year in range(START_YEAR, END_YEAR + 1):
            print(f"\n📅 Processing INPATIENT year {year}")
            summary = create_clean_phys_id_chunked(s_file, "INPATIENT", year)
            if summary:
                all_summaries.append(summary)

    # Process O files (outpatient)
    print(f"\n🔄 PROCESSING OUTPATIENT FILES (O)")
    print("=" * 40)

    for year in range(START_YEAR, END_YEAR + 1):
        o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"

        if Path(o_file).exists():
            print(f"\n📅 Processing OUTPATIENT year {year}")
            summary = create_clean_phys_id_chunked(o_file, "OUTPATIENT", year)
            if summary:
                all_summaries.append(summary)
        else:
            print(f"⚠️  SKIPPING: {o_file} (file not found)")

    # Summary report
    if all_summaries:
        print(f"\n{'='*80}")
        print("OVERALL SUMMARY")
        print(f"{'='*80}")

        summary_df = pd.DataFrame(all_summaries)

        total_patients = summary_df['total_patients'].sum()
        total_chunks = summary_df['chunks_processed'].sum()
        total_records = summary_df['total_records'].sum()
        total_with_phys_id = summary_df['records_with_phys_id'].sum()
        total_from_npi = summary_df['phys_id_from_npi'].sum()
        total_from_provid = summary_df['phys_id_from_provid'].sum()
        total_bad_provids = summary_df['bad_provids_excluded'].sum()
        total_time = summary_df['processing_time_minutes'].sum()

        print(f"\nOverall Statistics:")
        print(f"  Files processed: {len(all_summaries)}")
        print(f"  Total patients: {total_patients:,}")
        print(f"  Total chunks: {total_chunks:,}")
        print(f"  Total records: {total_records:,}")
        print(f"  ")
        print(f"  PROVID Cleaning Results:")
        print(f"    Total bad PROVIDs excluded: {total_bad_provids:,}")
        print(f"  ")
        print(f"  Final PHYS_ID Coverage:")
        print(f"    Records with PHYS_ID: {total_with_phys_id:,} ({total_with_phys_id/total_records*100:.1f}%)")
        print(f"    PHYS_ID from NPI: {total_from_npi:,} ({total_from_npi/total_records*100:.1f}%)")
        print(f"    PHYS_ID from clean PROVID: {total_from_provid:,} ({total_from_provid/total_records*100:.1f}%)")
        print(f"    No PHYS_ID: {total_records - total_with_phys_id:,} ({(total_records - total_with_phys_id)/total_records*100:.1f}%)")
        print(f"  ")
        print(f"  Total processing time: {total_time:.1f} minutes")

        # Save summary
        summary_file = f'{OUTPUT_DIR}/chunked_clean_phys_id_summary_{START_YEAR}_{END_YEAR}.csv'
        summary_df.to_csv(summary_file, index=False)
        print(f"\nSummary saved: {summary_file}")

        # Display year-by-year summary
        print(f"\nYear-by-Year Summary:")
        print("Year | File Type | Patients | Chunks | Records | PHYS_ID% | Time(min)")
        print("-" * 80)
        for _, row in summary_df.iterrows():
            phys_id_pct = row['records_with_phys_id'] / row['total_records'] * 100
            print(f"{row['year']} | {row['file_type']:9} | {row['total_patients']:8,} | {row['chunks_processed']:6} | {row['total_records']:8,} | {phys_id_pct:6.1f}% | {row['processing_time_minutes']:8.1f}")
print(f"\nOutput files have '_with_clean_phys_id' suffix")
        print(f"PHYS_ID interpretation:")
        print(f"  - All PHYS_IDs are unambiguous identifiers")
        print(f"  - NPI = individual provider (preferred)")
        print(f"  - PROVID = facility/department (only if consistent)")

def main():
    """
    Main function - chunked clean approach with selective PROVID removal
    """
    print("MarketScan Provider ID Script - CHUNKED CLEAN APPROACH")
    print("=" * 80)
    print("This script uses patient chunking to stay within memory limits")
    print("Removes only truly ambiguous PROVIDs (map to 2+ different NPIs)")
    print("PHYS_ID priority: NPI > consistent PROVID > null")
    print(f"Memory management: {PATIENT_CHUNK_SIZE:,} patients per chunk")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Processing years: {START_YEAR} to {END_YEAR}")
    print("=" * 80)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"✅ Output directory created/verified: {OUTPUT_DIR}")

    process_all_files_chunked()

    print(f"\n🎉 Chunked clean PHYS_ID creation complete!")
    print(f"\nKey benefits:")
    print(f"  ✅ Memory-efficient chunked processing")
    print(f"  ✅ Removes only truly ambiguous identifiers")
    print(f"  ✅ Keeps consistent facility/department identifiers")
    print(f"  ✅ All PHYS_IDs are unambiguous")
    print(f"  ✅ Much better data retention than aggressive approach")
    print(f"  ✅ Output saved to local filesystem: {OUTPUT_DIR}")

    print(f"\n📁 Output files:")
    output_files = glob.glob(f"{OUTPUT_DIR}/*.parquet")
    for f in sorted(output_files):
        size_mb = os.path.getsize(f) / (1024*1024)
        print(f"  {os.path.basename(f)} ({size_mb:.1f} MB)")

    if START_YEAR == END_YEAR:
        print(f"\n💡 To process all years, change:")
        print(f"  END_YEAR = 2023  # in the script configuration")

if __name__ == "__main__":
    main()

