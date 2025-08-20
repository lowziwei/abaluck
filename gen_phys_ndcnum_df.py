import pandas as pd
import duckdb
import glob
import os
import time
from pathlib import Path

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
    conn.execute("SET enable_progress_bar=false")
    
    return conn

def analyze_physician_semaglutide_prescribing():
    """
    Analyze semaglutide prescribing patterns by physician and service date
    """
    print("🔍 PHYSICIAN SEMAGLUTIDE PRESCRIBING ANALYSIS")
    print("=" * 50)
    
    # Find all processed parquet files
    parquet_files = glob.glob("prescription_events_*_with_ndcnum.parquet")
    
    if not parquet_files:
        print("❌ No processed parquet files found!")
        print("   Looking for files matching: prescription_events_*_with_ndcnum.parquet")
        return None
    
    print(f"📁 Found {len(parquet_files)} parquet files to analyze")
    for file in sorted(parquet_files):
        file_size = Path(file).stat().st_size / (1024*1024)
        print(f"   {os.path.basename(file)} ({file_size:.1f} MB)")
    
    print("\n🚀 Starting analysis...")
    start_time = time.time()
    
    # Setup DuckDB connection
    conn = setup_duckdb_connection()
    
    try:
        # Create file list for DuckDB query
        file_list = "', '".join(parquet_files)
        
        print("📊 Computing physician-level statistics...")
        
        # Main analysis query
        analysis_query = f"""
        WITH physician_stats AS (
            SELECT 
                phys_id,
                SVCDATE,
                -- Patient metrics
                COUNT(DISTINCT ENROLID) as total_patients,
                COUNT(DISTINCT CASE WHEN d_semaglutide = 1 THEN ENROLID END) as semaglutide_patients,
                
                -- Prescription metrics  
                COUNT(*) as total_prescriptions,
                SUM(d_semaglutide) as semaglutide_prescriptions,
                
                -- Calculate fractions
                CAST(SUM(d_semaglutide) AS FLOAT) / CAST(COUNT(*) AS FLOAT) as fraction_prescriptions_semaglutide,
                CAST(COUNT(DISTINCT CASE WHEN d_semaglutide = 1 THEN ENROLID END) AS FLOAT) / 
                CAST(COUNT(DISTINCT ENROLID) AS FLOAT) as fraction_patients_semaglutide
                
            FROM read_parquet(['{file_list}'])
            WHERE phys_id IS NOT NULL 
                AND SVCDATE IS NOT NULL
                AND ENROLID IS NOT NULL
            GROUP BY phys_id, SVCDATE
        )
        SELECT 
            phys_id,
            SVCDATE,
            total_patients,
            total_prescriptions,
            semaglutide_patients,
            semaglutide_prescriptions,
            COALESCE(fraction_prescriptions_semaglutide, 0.0) as fraction_prescriptions_semaglutide,
            COALESCE(fraction_patients_semaglutide, 0.0) as fraction_patients_semaglutide
        FROM physician_stats
        ORDER BY phys_id, SVCDATE
        """
        
        # Execute query and get results
        print("⚡ Executing analysis query...")
        results_df = conn.execute(analysis_query).fetchdf()
        
        conn.close()
        
        # Analysis summary
        total_time = time.time() - start_time
        
        print(f"\n✅ ANALYSIS COMPLETE!")
        print(f"⏱️  Processing time: {total_time:.1f} seconds")
        print(f"📊 Results summary:")
        print(f"   • Total physician-date combinations: {len(results_df):,}")
        print(f"   • Unique physicians: {results_df['phys_id'].nunique():,}")
        print(f"   • Date range: {results_df['SVCDATE'].min()} to {results_df['SVCDATE'].max()}")
        
        # Overall statistics
        total_prescriptions = results_df['total_prescriptions'].sum()
        total_semaglutide = results_df['semaglutide_prescriptions'].sum()
        overall_prescription_rate = (total_semaglutide / total_prescriptions * 100) if total_prescriptions > 0 else 0
        
        print(f"   • Total prescriptions analyzed: {total_prescriptions:,}")
        print(f"   • Total semaglutide prescriptions: {total_semaglutide:,}")
        print(f"   • Overall semaglutide prescription rate: {overall_prescription_rate:.2f}%")
        
        # Physician-level semaglutide prescribing distribution
        physicians_with_sema = (results_df['semaglutide_prescriptions'] > 0).sum()
        physician_sema_rate = (physicians_with_sema / len(results_df) * 100) if len(results_df) > 0 else 0
        
        print(f"   • Physician-date pairs with any semaglutide: {physicians_with_sema:,} ({physician_sema_rate:.1f}%)")
        
        # Top semaglutide prescribers
        if total_semaglutide > 0:
            print(f"\n🏆 TOP 10 PHYSICIAN-DATE COMBINATIONS BY SEMAGLUTIDE PRESCRIPTIONS:")
            top_prescribers = results_df.nlargest(10, 'semaglutide_prescriptions')[
                ['phys_id', 'SVCDATE', 'total_prescriptions', 'semaglutide_prescriptions', 'fraction_prescriptions_semaglutide']
            ]
            for idx, row in top_prescribers.iterrows():
                print(f"   {row['phys_id']} ({row['SVCDATE']}): {row['semaglutide_prescriptions']}/{row['total_prescriptions']} "
                      f"({row['fraction_prescriptions_semaglutide']*100:.1f}%)")
        
        return results_df
        
    except Exception as e:
        print(f"💥 Error during analysis: {e}")
        conn.close()
        return None

def save_results(results_df, output_filename="physician_semaglutide_analysis.parquet"):
    """
    Save the analysis results to a parquet file
    """
    if results_df is None:
        print("❌ No results to save")
        return
        
    print(f"\n💾 Saving results to {output_filename}...")
    
    try:
        results_df.to_parquet(output_filename, compression='snappy')
        
        file_size = Path(output_filename).stat().st_size / (1024*1024)
        print(f"✅ Results saved: {output_filename} ({file_size:.1f} MB)")
        
        # Also save as CSV for easy viewing
        csv_filename = output_filename.replace('.parquet', '.csv')
        results_df.to_csv(csv_filename, index=False)
        csv_size = Path(csv_filename).stat().st_size / (1024*1024)
        print(f"📄 Also saved as: {csv_filename} ({csv_size:.1f} MB)")
        
        return True
        
    except Exception as e:
        print(f"💥 Error saving results: {e}")
        return False

def display_sample_results(results_df, n_samples=20):
    """
    Display a sample of the results
    """
    if results_df is None:
        return
        
    print(f"\n👀 SAMPLE RESULTS (first {n_samples} rows):")
    print("=" * 100)
    
    # Format the display
    sample = results_df.head(n_samples).copy()
    
    # Round fractions for better display
    sample['fraction_prescriptions_semaglutide'] = sample['fraction_prescriptions_semaglutide'].round(4)
    sample['fraction_patients_semaglutide'] = sample['fraction_patients_semaglutide'].round(4)
    
    print(sample.to_string(index=False))

def main():
    """
    Main analysis function
    """
    print("Physician Semaglutide Prescribing Analysis")
    print("=" * 42)
    
    # Run the analysis
    results_df = analyze_physician_semaglutide_prescribing()
    
    if results_df is not None:
        # Display sample results
        display_sample_results(results_df)
        
        # Save results
        save_results(results_df)
        
        print(f"\n🎉 Analysis complete! The final dataframe contains:")
        print(f"   • phys_id: Physician identifier")
        print(f"   • SVCDATE: Service date") 
        print(f"   • total_patients: Number of unique patients")
        print(f"   • total_prescriptions: Total prescription count")
        print(f"   • semaglutide_patients: Patients receiving semaglutide")
        print(f"   • semaglutide_prescriptions: Semaglutide prescription count")
        print(f"   • fraction_prescriptions_semaglutide: Fraction of prescriptions that are semaglutide")
        print(f"   • fraction_patients_semaglutide: Fraction of patients receiving semaglutide")
        
        return results_df
    else:
        print("❌ Analysis failed")
        return None

if __name__ == "__main__":
    results = main()
