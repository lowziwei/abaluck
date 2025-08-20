import pandas as pd
import glob
import os

def analyze_physician_semaglutide():
    """
    Simple analysis of semaglutide prescribing by physician and date
    """
    print("🔍 PHYSICIAN SEMAGLUTIDE ANALYSIS")
    print("=" * 40)
    
    # Find all parquet files matching the pattern
    parquet_files = glob.glob("prescription_events_*_*_with_ndcnum.parquet")
    
    if not parquet_files:
        print("❌ No parquet files found!")
        return None
    
    print(f"📁 Found {len(parquet_files)} files to process")
    
    all_results = []
    
    # Process each file
    for file in sorted(parquet_files):
        print(f"  📝 Processing: {os.path.basename(file)}")
        
        # Read the parquet file
        df = pd.read_parquet(file)
        
        # Group by phys_ids and SVCDATE
        results = df.groupby(['phys_ids', 'SVCDATE']).agg({
            'ENROLID': 'nunique',  # total patients
            'NDCNUM': 'count',     # total prescriptions
            'd_semaglutide': ['sum', lambda x: (x == 1).sum()]  # semaglutide prescriptions and patients
        }).reset_index()
        
        # Flatten column names
        results.columns = ['phys_ids', 'SVCDATE', 'total_patients', 'total_prescriptions', 
                          'semaglutide_prescriptions', 'semaglutide_patients_temp']
        
        # Calculate semaglutide patients properly
        sema_patients = df[df['d_semaglutide'] == 1].groupby(['phys_ids', 'SVCDATE'])['ENROLID'].nunique().reset_index()
        sema_patients.columns = ['phys_ids', 'SVCDATE', 'semaglutide_patients']
        
        # Merge back
        results = results.merge(sema_patients, on=['phys_ids', 'SVCDATE'], how='left')
        results['semaglutide_patients'] = results['semaglutide_patients'].fillna(0)
        results = results.drop('semaglutide_patients_temp', axis=1)
        
        # Calculate fractions
        results['fraction_prescriptions_semaglutide'] = results['semaglutide_prescriptions'] / results['total_prescriptions']
        results['fraction_patients_semaglutide'] = results['semaglutide_patients'] / results['total_patients']
        
        all_results.append(results)
        
        print(f"     Physician-date combinations: {len(results)}")
    
    # Combine all results
    final_df = pd.concat(all_results, ignore_index=True)
    
    print(f"\n✅ ANALYSIS COMPLETE!")
    print(f"📊 Total physician-date combinations: {len(final_df):,}")
    print(f"📊 Total prescriptions: {final_df['total_prescriptions'].sum():,}")
    print(f"📊 Total semaglutide prescriptions: {final_df['semaglutide_prescriptions'].sum():,}")
    
    return final_df

def save_results(df):
    """Save results to files"""
    if df is None:
        return
    
    # Save as parquet
    df.to_parquet("physician_semaglutide_analysis.parquet", compression='snappy')
    print(f"💾 Saved: physician_semaglutide_analysis.parquet")
    
    # Save as CSV
    df.to_csv("physician_semaglutide_analysis.csv", index=False)
    print(f"💾 Saved: physician_semaglutide_analysis.csv")

def main():
    """Main function"""
    results_df = analyze_physician_semaglutide()
    
    if results_df is not None:
        save_results(results_df)
        
        # Show sample
        print(f"\n👀 SAMPLE RESULTS:")
        print(results_df.head(10).to_string(index=False))
        
        return results_df
    
    return None

if __name__ == "__main__":
    results = main()
