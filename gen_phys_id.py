from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, count, countDistinct, when, isnan, isnull, 
    collect_list, collect_set, size, lit, broadcast
)
from pyspark.sql.types import StringType
import time
import pandas as pd
from pathlib import Path
import gc

# Configuration
START_YEAR = 2018
END_YEAR = 2023  # Adjust as needed
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"

def create_spark_session():
    """
    Create optimized Spark session for large dataset processing
    """
    spark = SparkSession.builder \
        .appName("MarketScan_Provider_Cleanup") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.executor.memoryFraction", "0.8") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def identify_bad_mappings(spark, df):
    """
    Identify PROVIDs and NPIs with inconsistent mappings using Spark
    
    Returns:
        tuple: (bad_provids_list, bad_npis_list)
    """
    print("    Identifying bad PROVID-NPI mappings...")
    
    # Find PROVIDs that map to multiple NPIs
    provid_npi_mapping = df.select("PROVID", "NPI") \
        .filter((col("PROVID").isNotNull()) & (col("NPI").isNotNull())) \
        .distinct()
    
    provid_npi_counts = provid_npi_mapping.groupBy("PROVID") \
        .agg(countDistinct("NPI").alias("npi_count")) \
        .filter(col("npi_count") > 1)
    
    bad_provids = [row.PROVID for row in provid_npi_counts.select("PROVID").collect()]
    
    # Find NPIs that map to multiple PROVIDs
    npi_provid_counts = provid_npi_mapping.groupBy("NPI") \
        .agg(countDistinct("PROVID").alias("provid_count")) \
        .filter(col("provid_count") > 1)
    
    bad_npis = [row.NPI for row in npi_provid_counts.select("NPI").collect()]
    
    print(f"      PROVIDs mapping to multiple NPIs: {len(bad_provids):,}")
    print(f"      NPIs mapping to multiple PROVIDs: {len(bad_npis):,}")
    
    return bad_provids, bad_npis

def clean_provider_mappings_spark(file_path, file_type, year=None):
    """
    Clean provider mappings for a single file using Spark
    """
    print(f"\n{'='*80}")
    print(f"SPARK CLEANING - {file_type} {year if year else 'ALL_YEARS'}")
    print(f"File: {file_path}")
    print(f"{'='*80}")
    
    if not Path(file_path).exists():
        print(f"ERROR: File does not exist: {file_path}")
        return None
    
    spark = create_spark_session()
    start_time = time.time()
    
    try:
        # Step 1: Load the data
        print(f"\nStep 1: Loading data from {file_type}...")
        df = spark.read.parquet(file_path)
        
        # Filter by year for S files (inpatient)
        if file_type == "INPATIENT" and year:
            df = df.filter(col("YEAR") == year)
            print(f"  Filtered to year {year}")
        
        # Get initial statistics
        total_records = df.count()
        print(f"  Total records: {total_records:,}")
        
        initial_stats = df.agg(
            count("*").alias("total_records"),
            countDistinct("PROVID").alias("unique_provids"),
            countDistinct("NPI").alias("unique_npis"),
            count(when(col("PROVID").isNotNull(), 1)).alias("records_with_provid"),
            count(when(col("NPI").isNotNull(), 1)).alias("records_with_npi"),
            count(when((col("PROVID").isNotNull()) & (col("NPI").isNotNull()), 1)).alias("records_with_both")
        ).collect()[0]
        
        print(f"  Initial statistics:")
        print(f"    Unique PROVIDs: {initial_stats.unique_provids:,}")
        print(f"    Unique NPIs: {initial_stats.unique_npis:,}")
        print(f"    Records with PROVID: {initial_stats.records_with_provid:,}")
        print(f"    Records with NPI: {initial_stats.records_with_npi:,}")
        print(f"    Records with both: {initial_stats.records_with_both:,}")
        
        # Step 2: Identify problematic mappings
        print(f"\nStep 2: Identifying problematic mappings...")
        bad_provids, bad_npis = identify_bad_mappings(spark, df)
        
        # Step 3: Filter out bad mappings and create PHYS_ID
        print(f"\nStep 3: Cleaning data and creating PHYS_ID...")
        
        cleaned_df = df
        
        # Remove records with bad PROVIDs
        if bad_provids:
            print(f"    Filtering out {len(bad_provids)} bad PROVIDs...")
            cleaned_df = cleaned_df.filter(~col("PROVID").isin(bad_provids))
        
        # Remove records with bad NPIs  
        if bad_npis:
            print(f"    Filtering out {len(bad_npis)} bad NPIs...")
            cleaned_df = cleaned_df.filter(~col("NPI").isin(bad_npis))
        
        # Create PHYS_ID field (cast both to string for consistency)
        print(f"    Creating PHYS_ID field...")
        cleaned_df = cleaned_df.withColumn(
            "PHYS_ID", 
            coalesce(col("NPI").cast(StringType()), col("PROVID").cast(StringType()))
        )
        
        # Cache the cleaned dataframe for multiple operations
        cleaned_df.cache()
        
        # Step 4: Calculate final statistics
        print(f"\nStep 4: Calculating final statistics...")
        
        final_records = cleaned_df.count()
        records_removed = total_records - final_records
        removal_rate = (records_removed / total_records) * 100
        
        final_stats = cleaned_df.agg(
            count("*").alias("total_records"),
            countDistinct("PROVID").alias("unique_provids"),
            countDistinct("NPI").alias("unique_npis"),
            countDistinct("PHYS_ID").alias("unique_phys_ids"),
            count(when(col("PROVID").isNotNull(), 1)).alias("records_with_provid"),
            count(when(col("NPI").isNotNull(), 1)).alias("records_with_npi"),
            count(when((col("PROVID").isNotNull()) & (col("NPI").isNotNull()), 1)).alias("records_with_both"),
            count(when(col("NPI").isNotNull(), 1)).alias("phys_id_from_npi"),
            count(when((col("NPI").isNull()) & (col("PROVID").isNotNull()), 1)).alias("phys_id_from_provid")
        ).collect()[0]
        
        print(f"  Final statistics:")
        print(f"    Total records: {final_stats.total_records:,}")
        print(f"    Records removed: {records_removed:,} ({removal_rate:.2f}%)")
        print(f"    Unique PROVIDs: {final_stats.unique_provids:,}")
        print(f"    Unique NPIs: {final_stats.unique_npis:,}")
        print(f"    Unique PHYS_IDs: {final_stats.unique_phys_ids:,}")
        print(f"    Records with PROVID: {final_stats.records_with_provid:,}")
        print(f"    Records with NPI: {final_stats.records_with_npi:,}")
        print(f"    Records with both: {final_stats.records_with_both:,}")
        print(f"    PHYS_ID from NPI: {final_stats.phys_id_from_npi:,}")
        print(f"    PHYS_ID from PROVID: {final_stats.phys_id_from_provid:,}")
        
        # Step 5: Save cleaned file
        print(f"\nStep 5: Saving cleaned file...")
        
        if file_type == "INPATIENT":
            output_path = file_path.replace('.parquet', f'_cleaned_{year}.parquet')
        else:
            output_path = file_path.replace('.parquet', '_cleaned.parquet')
        
        # Write with optimal partitioning
        cleaned_df.coalesce(50).write.mode("overwrite").parquet(output_path)
        
        processing_time = time.time() - start_time
        
        print(f"  Cleaned file saved: {output_path}")
        print(f"  Processing time: {processing_time/60:.1f} minutes")
        
        # Create summary data
        summary_data = {
            'file_type': file_type,
            'year': year if year else 'ALL',
            'original_records': total_records,
            'cleaned_records': final_stats.total_records,
            'records_removed': records_removed,
            'removal_rate_pct': removal_rate,
            'bad_provids_count': len(bad_provids),
            'bad_npis_count': len(bad_npis),
            'unique_phys_ids': final_stats.unique_phys_ids,
            'phys_id_from_npi': final_stats.phys_id_from_npi,
            'phys_id_from_provid': final_stats.phys_id_from_provid,
            'output_file': output_path,
            'processing_time_minutes': processing_time/60
        }
        
        # Clean up
        cleaned_df.unpersist()
        
        return summary_data
        
    except Exception as e:
        print(f"ERROR processing {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        spark.stop()
        gc.collect()

def process_all_years_spark():
    """
    Process all years and file types using Spark
    """
    print("SPARK PROVIDER DATA CLEANING - ALL YEARS")
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
            summary = clean_provider_mappings_spark(s_file, "INPATIENT", year)
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
            summary = clean_provider_mappings_spark(o_file, "OUTPATIENT", year)
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
        total_original = summary_df['original_records'].sum()
        total_cleaned = summary_df['cleaned_records'].sum()
        total_removed = summary_df['records_removed'].sum()
        overall_removal_rate = (total_removed / total_original) * 100
        total_time = summary_df['processing_time_minutes'].sum()
        
        print(f"\nOverall Statistics:")
        print(f"  Files processed: {len(all_summaries)}")
        print(f"  Total original records: {total_original:,}")
        print(f"  Total cleaned records: {total_cleaned:,}")
        print(f"  Total records removed: {total_removed:,} ({overall_removal_rate:.2f}%)")
        print(f"  Total processing time: {total_time:.1f} minutes")
        
        # Breakdown by file type
        print(f"\nBreakdown by File Type:")
        for file_type in ['INPATIENT', 'OUTPATIENT']:
            type_data = summary_df[summary_df['file_type'] == file_type]
            if not type_data.empty:
                type_original = type_data['original_records'].sum()
                type_cleaned = type_data['cleaned_records'].sum()
                type_removed = type_data['records_removed'].sum()
                type_removal_rate = (type_removed / type_original) * 100 if type_original > 0 else 0
                type_time = type_data['processing_time_minutes'].sum()
                
                print(f"  {file_type}:")
                print(f"    Years processed: {len(type_data)}")
                print(f"    Original records: {type_original:,}")
                print(f"    Cleaned records: {type_cleaned:,}")
                print(f"    Records removed: {type_removed:,} ({type_removal_rate:.2f}%)")
                print(f"    Processing time: {type_time:.1f} minutes")
        
        # Save detailed summary
        summary_file = f'provider_cleaning_spark_summary_{START_YEAR}_{END_YEAR}.csv'
        summary_df.to_csv(summary_file, index=False)
        print(f"\nDetailed summary saved: {summary_file}")
        
        # Display year-by-year summary
        print(f"\nYear-by-Year Summary:")
        print("Year | File Type | Original | Cleaned | Removed | Rate% | Time(min)")
        print("-" * 75)
        for _, row in summary_df.iterrows():
            print(f"{row['year']} | {row['file_type']:9} | {row['original_records']:8,} | {row['cleaned_records']:7,} | {row['records_removed']:7,} | {row['removal_rate_pct']:5.1f}% | {row['processing_time_minutes']:8.1f}")
    
    print(f"\n{'='*80}")
    print("🎉 SPARK PROVIDER DATA CLEANING COMPLETE!")
    print(f"{'='*80}")

def validate_cleaned_files_spark():
    """
    Validate that cleaned files have consistent PROVID-NPI mappings using Spark
    """
    print("\n" + "="*80)
    print("VALIDATING CLEANED FILES WITH SPARK")
    print("="*80)
    
    spark = create_spark_session()
    
    try:
        # Validate S files (inpatient) - one file per year
        for year in range(START_YEAR, END_YEAR + 1):
            s_file_cleaned = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_S_cleaned_{year}.parquet"
            
            if Path(s_file_cleaned).exists():
                print(f"\nValidating INPATIENT {year}...")
                
                df = spark.read.parquet(s_file_cleaned)
                
                # Check for inconsistent PROVID-NPI mappings
                provid_npi_mapping = df.select("PROVID", "NPI") \
                    .filter((col("PROVID").isNotNull()) & (col("NPI").isNotNull())) \
                    .distinct()
                
                # Check PROVIDs with multiple NPIs
                bad_provids_count = provid_npi_mapping.groupBy("PROVID") \
                    .agg(countDistinct("NPI").alias("npi_count")) \
                    .filter(col("npi_count") > 1) \
                    .count()
                
                # Check NPIs with multiple PROVIDs
                bad_npis_count = provid_npi_mapping.groupBy("NPI") \
                    .agg(countDistinct("PROVID").alias("provid_count")) \
                    .filter(col("provid_count") > 1) \
                    .count()
                
                file_name = Path(s_file_cleaned).name
                if bad_provids_count == 0 and bad_npis_count == 0:
                    print(f"  ✅ {file_name}: Clean")
                else:
                    print(f"  ❌ {file_name}: {bad_provids_count} bad PROVIDs, {bad_npis_count} bad NPIs")
        
        # Validate O files (outpatient) - one per year  
        for year in range(START_YEAR, END_YEAR + 1):
            o_file_cleaned = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}_cleaned.parquet"
            
            if Path(o_file_cleaned).exists():
                print(f"\nValidating OUTPATIENT {year}...")
                
                df = spark.read.parquet(o_file_cleaned)
                
                # Check for inconsistent PROVID-NPI mappings
                provid_npi_mapping = df.select("PROVID", "NPI") \
                    .filter((col("PROVID").isNotNull()) & (col("NPI").isNotNull())) \
                    .distinct()
                
                # Check PROVIDs with multiple NPIs
                bad_provids_count = provid_npi_mapping.groupBy("PROVID") \
                    .agg(countDistinct("NPI").alias("npi_count")) \
                    .filter(col("npi_count") > 1) \
                    .count()
                
                # Check NPIs with multiple PROVIDs
                bad_npis_count = provid_npi_mapping.groupBy("NPI") \
                    .agg(countDistinct("PROVID").alias("provid_count")) \
                    .filter(col("provid_count") > 1) \
                    .count()
                
                file_name = Path(o_file_cleaned).name
                if bad_provids_count == 0 and bad_npis_count == 0:
                    print(f"  ✅ {file_name}: Clean")
                else:
                    print(f"  ❌ {file_name}: {bad_provids_count} bad PROVIDs, {bad_npis_count} bad NPIs")
    
    finally:
        spark.stop()
    
    print("\nValidation complete!")

def main():
    """
    Main function to run the Spark provider data cleaning
    """
    print("MarketScan Provider Data Cleaning Script - SPARK VERSION")
    print("=" * 80)
    print(f"Processing years: {START_YEAR} to {END_YEAR}")
    print(f"Dataset: {DATASET_TYPE}/{DATABASE}")
    print("Using Apache Spark for distributed processing")
    print("Optimized for large datasets and efficient memory usage")
    print("File structure:")
    print(f"  - S (Inpatient): Single file, filter by YEAR column")
    print(f"  - O (Outpatient): Separate files per year")
    print("=" * 80)
    
    # Process all years with Spark
    process_all_years_spark()
    
    # Validate results with Spark
    validate_cleaned_files_spark()
    
    print("\n🎉 All Spark processing complete!")
    print("\nSpark Benefits:")
    print("  ✅ Distributed processing across multiple cores")
    print("  ✅ Automatic memory management and optimization") 
    print("  ✅ Lazy evaluation for efficient computation")
    print("  ✅ Built-in caching for repeated operations")
    print("  ✅ Adaptive query execution")
    print("\nNext steps:")
    print("  1. Use the cleaned files with '_cleaned' suffix for your analysis")
    print("  2. Use the PHYS_ID field instead of separate PROVID/NPI fields")
    print("  3. Check the summary CSV for detailed statistics")

if __name__ == "__main__":
    main()
