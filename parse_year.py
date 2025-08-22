import duckdb
import pandas as pd
import os
import time
from pathlib import Path

def setup_duckdb_connection(memory_limit='8GB'):
    """
    Create optimized DuckDB connection
    """
    conn = duckdb.connect(':memory:')
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.execute("SET max_temp_directory_size='20GB'")
    conn.execute("SET temp_directory='/data/MarketScan_data/temp'")
    conn.execute("SET threads=8")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute("SET enable_progress_bar=false")
    
    # Create temp directory if needed
    os.makedirs('/data/MarketScan_data/temp', exist_ok=True)
    
    return conn

def explore_medicare_file():
    """
    Quick exploration of the Medicare file structure
    """
    print("🔍 EXPLORING MEDICARE FILE STRUCTURE")
    print("=" * 50)
    
    input_file = "MEDICARE_SET_A/MDCR_D.parquet"
    
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        return
    
    conn = setup_duckdb_connection()
    
    try:
        # Check file info
        print(f"📁 File: {input_file}")
        
        # Get total record count
        count_query = f"SELECT COUNT(*) as total_records FROM '{input_file}'"
        total_records = conn.execute(count_query).fetchdf()['total_records'].iloc[0]
        print(f"📊 Total records: {total_records:,}")
        
        # Get column info
        columns_query = f"DESCRIBE SELECT * FROM '{input_file}'"
        columns_df = conn.execute(columns_query).fetchdf()
        print(f"📋 Columns ({len(columns_df)}):")
        for _, row in columns_df.iterrows():
            print(f"   {row['column_name']}: {row['column_type']}")
        
        # Check SVCDATE format and range
        date_query = f"""
        SELECT 
            MIN(SVCDATE) as min_date,
            MAX(SVCDATE) as max_date,
            COUNT(DISTINCT EXTRACT(year FROM CAST(SVCDATE AS DATE))) as unique_years
        FROM '{input_file}'
        WHERE SVCDATE IS NOT NULL
        """
        date_info = conn.execute(date_query).fetchdf()
        print(f"\n📅 SVCDATE INFO:")
        print(f"   Date range: {date_info['min_date'].iloc[0]} to {date_info['max_date'].iloc[0]}")
        print(f"   Unique years: {date_info['unique_years'].iloc[0]}")
        
        # Get year distribution
        year_query = f"""
        SELECT 
            EXTRACT(year FROM CAST(SVCDATE AS DATE)) as year,
            COUNT(*) as record_count
        FROM '{input_file}'
        WHERE SVCDATE IS NOT NULL
        GROUP BY year
        ORDER BY year
        """
        year_dist = conn.execute(year_query).fetchdf()
        print(f"\n📈 YEAR DISTRIBUTION:")
        for _, row in year_dist.iterrows():
            print(f"   {int(row['year'])}: {row['record_count']:,} records")
        
        # Sample data
        sample_query = f"""
        SELECT *
        FROM '{input_file}'
        LIMIT 5
        """
        sample_df = conn.execute(sample_query).fetchdf()
        print(f"\n👀 SAMPLE DATA:")
        print(sample_df.to_string(index=False))
        
        conn.close()
        return year_dist
        
    except Exception as e:
        print(f"💥 Error: {e}")
        conn.close()
        return None

def parse_medicare_by_year(year_list=None, output_dir="medicare_parsed"):
    """
    Parse Medicare prescription drug file by year
    """
    print("🔄 PARSING MEDICARE FILE BY YEAR")
    print("=" * 40)
    
    input_file = "MEDICARE_SET_A/MDCR_D.parquet"
    
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Output directory: {output_dir}")
    
    conn = setup_duckdb_connection()
    
    try:
        # If no year list provided, get all available years
        if year_list is None:
            year_query = f"""
            SELECT DISTINCT EXTRACT(year FROM CAST(SVCDATE AS DATE)) as year
            FROM '{input_file}'
            WHERE SVCDATE IS NOT NULL
            ORDER BY year
            """
            years_df = conn.execute(year_query).fetchdf()
            year_list = years_df['year'].astype(int).tolist()
        
        print(f"🎯 Processing years: {year_list}")
        
        results = []
        
        for year in year_list:
            print(f"\n📅 Processing year {year}...")
            start_time = time.time()
            
            # Extract data for this year
            year_query = f"""
            SELECT *
            FROM '{input_file}'
            WHERE EXTRACT(year FROM CAST(SVCDATE AS DATE)) = {year}
            """
            
            year_df = conn.execute(year_query).fetchdf()
            
            if len(year_df) == 0:
                print(f"   ⚠️  No records found for year {year}")
                continue
            
            # Save as parquet
            output_file = f"{output_dir}/MDCR_D_{year}.parquet"
            year_df.to_parquet(output_file, compression='snappy')
            
            # Calculate file size
            file_size_mb = os.path.getsize(output_file) / (1024*1024)
            processing_time = time.time() - start_time
            
            print(f"   ✅ Processed {len(year_df):,} records")
            print(f"   💾 Saved: {os.path.basename(output_file)} ({file_size_mb:.1f} MB)")
            print(f"   ⏱️  Time: {processing_time:.1f} seconds")
            
            results.append({
                'year': year,
                'records': len(year_df),
                'output_file': output_file,
                'file_size_mb': file_size_mb,
                'processing_time': processing_time
            })
            
            # Clean up
            del year_df
            import gc
            gc.collect()
        
        conn.close()
        
        # Summary
        print(f"\n📊 PARSING SUMMARY:")
        print("=" * 50)
        
        total_records = sum(r['records'] for r in results)
        total_size = sum(r['file_size_mb'] for r in results)
        total_time = sum(r['processing_time'] for r in results)
        
        print(f"Years processed: {len(results)}")
        print(f"Total records: {total_records:,}")
        print(f"Total file size: {total_size:.1f} MB")
        print(f"Total processing time: {total_time:.1f} seconds")
        
        print(f"\nYear-by-year breakdown:")
        for result in results:
            print(f"  {result['year']}: {result['records']:,} records, {result['file_size_mb']:.1f} MB")
        
        # Save summary
        summary_df = pd.DataFrame(results)
        summary_file = f"{output_dir}/parsing_summary.csv"
        summary_df.to_csv(summary_file, index=False)
        print(f"\n💾 Summary saved: {summary_file}")
        
        return results
        
    except Exception as e:
        print(f"💥 Error: {e}")
        conn.close()
        return None

def parse_specific_years(years, output_dir="medicare_parsed"):
    """
    Parse specific years only
    """
    print(f"🎯 PARSING SPECIFIC YEARS: {years}")
    return parse_medicare_by_year(year_list=years, output_dir=output_dir)

def main():
    """
    Main function with options
    """
    print("Medicare Prescription Drug File Parser")
    print("=" * 40)
    
    # Step 1: Explore the file
    print("Step 1: Exploring file structure...")
    year_dist = explore_medicare_file()
    
    if year_dist is None:
        return
    
    print(f"\n" + "="*50)
    print("PARSING OPTIONS:")
    print("1. Parse all years")
    print("2. Parse specific years")
    print("3. Parse recent years (2020-2024)")
    
    choice = input("\nChoose option (1/2/3): ").strip()
    
    if choice == "1":
        # Parse all years
        results = parse_medicare_by_year()
    elif choice == "2":
        # Parse specific years
        years_input = input("Enter years (comma-separated, e.g., 2018,2019,2020): ")
        try:
            years = [int(y.strip()) for y in years_input.split(',')]
            results = parse_specific_years(years)
        except ValueError:
            print("❌ Invalid year format. Please use numbers separated by commas.")
            return
    elif choice == "3":
        # Parse recent years
        recent_years = [2020, 2021, 2022, 2023, 2024]
        results = parse_specific_years(recent_years)
    else:
        print("❌ Invalid choice")
        return
    
    if results:
        print(f"\n🎉 PARSING COMPLETE!")
        print(f"Output files in: medicare_parsed/")

if __name__ == "__main__":
    main()
