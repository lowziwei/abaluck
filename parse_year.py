import duckdb
import pandas as pd
import os
import time
from pathlib import Path

def setup_duckdb_connection(memory_limit='4GB'):
    """
    Create optimized DuckDB connection with reduced memory
    """
    conn = duckdb.connect(':memory:')
    conn.execute(f"SET memory_limit='{memory_limit}'")  # Reduced from 8GB
    conn.execute("SET max_temp_directory_size='20GB'")
    conn.execute("SET temp_directory='/data/MarketScan_data/temp'")
    conn.execute("SET threads=4")  # Reduced threads
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
        
        # Get year distribution
        year_query = f"""
        SELECT 
            EXTRACT(year FROM CAST(SVCDATE AS DATE)) as year,
            COUNT(*) as record_count
        FROM '{input_file}'
        WHERE SVCDATE IS NOT NULL
        GROUP BY EXTRACT(year FROM CAST(SVCDATE AS DATE))
        ORDER BY year
        """
        year_dist = conn.execute(year_query).fetchdf()
        print(f"\n📈 YEAR DISTRIBUTION:")
        for _, row in year_dist.iterrows():
            print(f"   {int(row['year'])}: {row['record_count']:,} records")
        
        conn.close()
        return year_dist
        
    except Exception as e:
        print(f"💥 Error: {e}")
        conn.close()
        return None

def parse_year_chunked(year, chunk_size=50000, output_dir="medicare_parsed"):
    """
    Parse a single year using chunking to avoid memory issues
    """
    print(f"\n📅 Processing year {year} with chunking...")
    
    input_file = "MEDICARE_SET_A/MDCR_D.parquet"
    output_file = f"{output_dir}/MDCR_D_{year}.parquet"
    
    conn = setup_duckdb_connection()
    start_time = time.time()
    
    try:
        # First, get total count for this year
        count_query = f"""
        SELECT COUNT(*) as count
        FROM '{input_file}'
        WHERE EXTRACT(year FROM CAST(SVCDATE AS DATE)) = {year}
        """
        total_count = conn.execute(count_query).fetchdf()['count'].iloc[0]
        
        if total_count == 0:
            print(f"   ⚠️  No records for year {year}")
            conn.close()
            return None
        
        print(f"   📊 Found {total_count:,} records for year {year}")
        print(f"   🔄 Processing in chunks of {chunk_size:,} records...")
        
        # Process in chunks using DuckDB's built-in chunking
        chunk_num = 0
        offset = 0
        all_chunks = []
        
        while offset < total_count:
            chunk_query = f"""
            SELECT *
            FROM '{input_file}'
            WHERE EXTRACT(year FROM CAST(SVCDATE AS DATE)) = {year}
            ORDER BY SVCDATE
            LIMIT {chunk_size} OFFSET {offset}
            """
            
            chunk_df = conn.execute(chunk_query).fetchdf()
            
            if len(chunk_df) == 0:
                break
            
            all_chunks.append(chunk_df)
            chunk_num += 1
            offset += chunk_size
            
            print(f"   📦 Processed chunk {chunk_num}: {len(chunk_df):,} records ({offset:,}/{total_count:,})")
            
            # Clean up chunk from memory
            del chunk_df
            import gc
            gc.collect()
        
        # Combine all chunks and save
        if all_chunks:
            print(f"   🔗 Combining {len(all_chunks)} chunks...")
            final_df = pd.concat(all_chunks, ignore_index=True)
            
            # Save to parquet
            final_df.to_parquet(output_file, compression='snappy')
            
            # Get file size
            file_size_mb = os.path.getsize(output_file) / (1024*1024)
            processing_time = time.time() - start_time
            
            print(f"   ✅ Saved {len(final_df):,} records")
            print(f"   💾 File: {os.path.basename(output_file)} ({file_size_mb:.1f} MB)")
            print(f"   ⏱️  Time: {processing_time:.1f} seconds")
            
            # Clean up
            del final_df, all_chunks
            gc.collect()
            
            conn.close()
            
            return {
                'year': year,
                'records': total_count,
                'output_file': output_file,
                'file_size_mb': file_size_mb,
                'processing_time': processing_time
            }
        
        conn.close()
        return None
        
    except Exception as e:
        print(f"   💥 Error processing year {year}: {e}")
        conn.close()
        return None

def parse_medicare_chunked(year_list=None, output_dir="medicare_parsed", chunk_size=50000):
    """
    Parse Medicare file by year using memory-efficient chunking
    """
    print("🔄 PARSING MEDICARE FILE BY YEAR (CHUNKED)")
    print("=" * 50)
    
    input_file = "MEDICARE_SET_A/MDCR_D.parquet"
    
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Output directory: {output_dir}")
    print(f"🧩 Chunk size: {chunk_size:,} records")
    
    # Get available years if none specified
    if year_list is None:
        print("🔍 Getting available years...")
        year_dist = explore_medicare_file()
        if year_dist is None:
            return
        year_list = year_dist['year'].astype(int).tolist()
    
    print(f"🎯 Processing years: {year_list}")
    
    results = []
    
    for year in year_list:
        result = parse_year_chunked(year, chunk_size, output_dir)
        if result:
            results.append(result)
    
    # Summary
    if results:
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
    
    return None

def parse_single_year_test(year=2020):
    """
    Test parsing a single year to check memory usage
    """
    print(f"🧪 TEST: Parsing single year {year}")
    result = parse_year_chunked(year, chunk_size=25000)  # Smaller chunks for testing
    return result

def main():
    """
    Main function with chunked options
    """
    print("Medicare Prescription Drug File Parser (Memory-Efficient)")
    print("=" * 60)
    
    print("PARSING OPTIONS:")
    print("1. Test single year (2020)")
    print("2. Parse specific years") 
    print("3. Parse recent years (2020-2024)")
    print("4. Parse all years")
    
    choice = input("\nChoose option (1/2/3/4): ").strip()
    
    if choice == "1":
        # Test single year
        result = parse_single_year_test()
        if result:
            print(f"\n✅ Test successful! File size: {result['file_size_mb']:.1f} MB")
    elif choice == "2":
        # Parse specific years
        years_input = input("Enter years (comma-separated, e.g., 2018,2019,2020): ")
        try:
            years = [int(y.strip()) for y in years_input.split(',')]
            results = parse_medicare_chunked(year_list=years)
        except ValueError:
            print("❌ Invalid year format. Please use numbers separated by commas.")
            return
    elif choice == "3":
        # Parse recent years
        recent_years = [2020, 2021, 2022, 2023, 2024]
        results = parse_medicare_chunked(year_list=recent_years)
    elif choice == "4":
        # Parse all years
        results = parse_medicare_chunked()
    else:
        print("❌ Invalid choice")
        return
    
    print(f"\n🎉 PARSING COMPLETE!")
    print(f"Output files in: medicare_parsed/")

if __name__ == "__main__":
    main()
