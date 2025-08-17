import pandas as pd
import os
from pathlib import Path

def display_processing_summary():
    """
    Display the processing summary CSV in a nicely formatted way
    """
    
    # File path
    summary_file = "histogram_results/processing_summary_2018_2024.csv"
    
    print("MarketScan Analysis - RESULTS DISPLAY")
    print("=" * 60)
    
    # Check if file exists
    if not Path(summary_file).exists():
        print(f"❌ Summary file not found: {summary_file}")
        print("\nAvailable files in histogram_results/:")
        if Path("histogram_results").exists():
            files = os.listdir("histogram_results")
            for file in sorted(files):
                if file.endswith('.csv'):
                    print(f"   📄 {file}")
        else:
            print("   📁 histogram_results/ directory not found")
        return
    
    try:
        # Load the CSV
        df = pd.read_csv(summary_file)
        
        print(f"📁 File: {summary_file}")
        print(f"📊 Records: {len(df)}")
        print(f"📅 Columns: {', '.join(df.columns.tolist())}")
        
        # Display the full table
        print(f"\n📋 PROCESSING SUMMARY TABLE:")
        print("=" * 120)
        
        # Configure pandas display options for better formatting
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 20)
        
        print(df.to_string(index=False))
        
        # Summary statistics
        if 'prescription_events' in df.columns:
            total_events = df['prescription_events'].sum()
            print(f"\n📊 SUMMARY STATISTICS:")
            print("=" * 50)
            print(f"Total files processed: {len(df)}")
            print(f"Total prescription events: {total_events:,}")
            print(f"Average events per file: {total_events/len(df):,.0f}")
            
            if 'processing_time' in df.columns:
                total_time = df['processing_time'].sum()
                print(f"Total processing time: {total_time/60:.1f} minutes")
                print(f"Average time per file: {df['processing_time'].mean():.1f} seconds")
            
            if 'out_of_thin_air' in df.columns:
                total_ota = df['out_of_thin_air'].sum()
                print(f"Total 'out of thin air' events: {total_ota:,}")
                print(f"Percentage 'out of thin air': {total_ota/total_events*100:.1f}%")
            
            if 'zero_physicians' in df.columns:
                total_zero = df['zero_physicians'].sum()
                print(f"Total zero physician events: {total_zero:,}")
                print(f"Percentage zero physicians: {total_zero/total_events*100:.1f}%")
        
        # Year-by-year breakdown
        if 'year' in df.columns:
            print(f"\n📅 YEAR-BY-YEAR BREAKDOWN:")
            print("=" * 70)
            print(f"{'Year':<6} {'Files':<6} {'Events':<12} {'Out of Thin Air':<15} {'Zero Physicians':<15} {'Time (min)':<10}")
            print("-" * 70)
            
            for year in sorted(df['year'].unique()):
                year_data = df[df['year'] == year]
                year_events = year_data['prescription_events'].sum() if 'prescription_events' in df.columns else 0
                year_files = len(year_data)
                year_ota = year_data['out_of_thin_air'].sum() if 'out_of_thin_air' in df.columns else 0
                year_zero = year_data['zero_physicians'].sum() if 'zero_physicians' in df.columns else 0
                year_time = year_data['processing_time'].sum()/60 if 'processing_time' in df.columns else 0
                
                print(f"{year:<6} {year_files:<6} {year_events:<12,} {year_ota:<15,} {year_zero:<15,} {year_time:<10.1f}")
        
        print(f"\n🔒 All data remains secure on server")
        
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")

def display_histogram_sample():
    """
    Display a sample of the histogram files
    """
    print(f"\n📊 SAMPLE HISTOGRAM DATA:")
    print("=" * 50)
    
    # Look for histogram files
    histogram_pattern = "histogram_results/unique_phys_*.parquet"
    import glob
    histogram_files = glob.glob(histogram_pattern)
    
    if histogram_files:
        # Show first histogram file as example
        sample_file = sorted(histogram_files)[0]
        print(f"📄 Sample from: {os.path.basename(sample_file)}")
        
        try:
            sample_df = pd.read_parquet(sample_file)
            print(sample_df.to_string(index=False))
            
            print(f"\n📈 This shows the distribution of physician counts per prescription")
            print(f"   - unique_phys: Number of physicians seen")
            print(f"   - prescription_events: Count of prescriptions with that many physicians")
            print(f"   - percentage: Percentage of total prescriptions")
            
        except Exception as e:
            print(f"❌ Error reading histogram file: {e}")
    else:
        print("❌ No histogram files found")

def main():
    """
    Main function to display all results
    """
    display_processing_summary()
    display_histogram_sample()

if __name__ == "__main__":
    main()
