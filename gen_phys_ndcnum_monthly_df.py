import pandas as pd
import os

def create_monthly_analysis():
    """
    Create monthly aggregated analysis from the physician-level data
    """
    print("📅 MONTHLY SEMAGLUTIDE ANALYSIS")
    print("=" * 35)
    
    # Read the physician-level analysis file
    input_file = "physician_semaglutide_analysis.parquet"
    
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        print("   Please run the physician analysis script first!")
        return None
    
    print(f"📁 Reading: {input_file}")
    df = pd.read_parquet(input_file)
    
    print(f"📊 Input data: {len(df):,} physician-date combinations")
    print(f"📅 Date range: {df['SVCDATE'].min()} to {df['SVCDATE'].max()}")
    
    # Convert SVCDATE to datetime and create year-month column
    df['SVCDATE'] = pd.to_datetime(df['SVCDATE'])
    df['year_month'] = df['SVCDATE'].dt.to_period('M')
    
    print(f"🗓️  Creating monthly buckets...")
    
    # Group by phys_ids and year_month
    monthly_stats = []
    
    for (phys_id, year_month), group in df.groupby(['phys_ids', 'year_month']):
        # Aggregate metrics for this physician-month
        total_patients = group['total_patients'].sum()
        total_prescriptions = group['total_prescriptions'].sum()
        semaglutide_prescriptions = group['semaglutide_prescriptions'].sum()
        semaglutide_patients = group['semaglutide_patients'].sum()
        
        # Calculate monthly fractions
        fraction_prescriptions_semaglutide = semaglutide_prescriptions / total_prescriptions if total_prescriptions > 0 else 0
        fraction_patients_semaglutide = semaglutide_patients / total_patients if total_patients > 0 else 0
        
        # Count number of service dates in this month for this physician
        service_dates_count = len(group)
        
        monthly_stats.append({
            'phys_ids': phys_id,
            'year_month': str(year_month),  # Convert to string for easier handling
            'year': year_month.year,
            'month': year_month.month,
            'service_dates_count': service_dates_count,
            'total_patients': total_patients,
            'total_prescriptions': total_prescriptions,
            'semaglutide_prescriptions': semaglutide_prescriptions,
            'semaglutide_patients': semaglutide_patients,
            'fraction_prescriptions_semaglutide': fraction_prescriptions_semaglutide,
            'fraction_patients_semaglutide': fraction_patients_semaglutide
        })
    
    # Convert to DataFrame
    monthly_df = pd.DataFrame(monthly_stats)
    
    # Sort by physician and date
    monthly_df = monthly_df.sort_values(['phys_ids', 'year', 'month']).reset_index(drop=True)
    
    # Summary statistics
    print(f"\n✅ MONTHLY ANALYSIS COMPLETE!")
    print(f"📊 Monthly physician combinations: {len(monthly_df):,}")
    print(f"👨‍⚕️ Unique physicians: {monthly_df['phys_ids'].nunique():,}")
    print(f"📅 Month range: {monthly_df['year_month'].min()} to {monthly_df['year_month'].max()}")
    print(f"📋 Total prescriptions: {monthly_df['total_prescriptions'].sum():,}")
    print(f"💊 Total semaglutide prescriptions: {monthly_df['semaglutide_prescriptions'].sum():,}")
    
    overall_sema_rate = (monthly_df['semaglutide_prescriptions'].sum() / monthly_df['total_prescriptions'].sum() * 100)
    print(f"📈 Overall semaglutide rate: {overall_sema_rate:.2f}%")
    
    return monthly_df

def save_monthly_results(df):
    """Save monthly results to files"""
    if df is None:
        return
    
    print(f"\n💾 Saving monthly results...")
    
    # Save as parquet
    output_parquet = "physician_monthly_semaglutide_analysis.parquet"
    df.to_parquet(output_parquet, compression='snappy')
    print(f"✅ Saved: {output_parquet}")
    
    # Save as CSV
    output_csv = "physician_monthly_semaglutide_analysis.csv"
    df.to_csv(output_csv, index=False)
    print(f"✅ Saved: {output_csv}")
    
    # Show file sizes
    parquet_size = os.path.getsize(output_parquet) / (1024*1024)
    csv_size = os.path.getsize(output_csv) / (1024*1024)
    print(f"📁 File sizes: parquet={parquet_size:.1f}MB, csv={csv_size:.1f}MB")

def show_sample_and_summary(df):
    """Show sample data and summary statistics"""
    if df is None:
        return
    
    print(f"\n👀 SAMPLE RESULTS (first 10 rows):")
    sample_cols = ['phys_ids', 'year_month', 'service_dates_count', 'total_patients', 
                   'total_prescriptions', 'semaglutide_prescriptions', 
                   'fraction_prescriptions_semaglutide']
    print(df[sample_cols].head(10).round(4).to_string(index=False))
    
    print(f"\n📈 MONTHLY TRENDS:")
    monthly_summary = df.groupby('year_month').agg({
        'phys_ids': 'nunique',
        'total_prescriptions': 'sum',
        'semaglutide_prescriptions': 'sum'
    }).reset_index()
    monthly_summary['sema_rate'] = monthly_summary['semaglutide_prescriptions'] / monthly_summary['total_prescriptions'] * 100
    monthly_summary.columns = ['month', 'physicians', 'total_rx', 'sema_rx', 'sema_rate_%']
    
    print("Recent months:")
    print(monthly_summary.tail(12).round(2).to_string(index=False))

def main():
    """Main function"""
    # Create monthly analysis
    monthly_df = create_monthly_analysis()
    
    if monthly_df is not None:
        # Save results
        save_monthly_results(monthly_df)
        
        # Show sample and trends
        show_sample_and_summary(monthly_df)
        
        print(f"\n🎉 MONTHLY ANALYSIS COMPLETE!")
        print(f"📄 Output files:")
        print(f"   • physician_monthly_semaglutide_analysis.parquet")
        print(f"   • physician_monthly_semaglutide_analysis.csv")
        
        return monthly_df
    
    return None

if __name__ == "__main__":
    results = main()
