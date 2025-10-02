import pandas as pd
import os
import glob

def create_monthly_analysis_with_eligibility():
    """
    Create monthly aggregated analysis from the physician-level data
    Includes eligibility metrics based on diagnosis codes
    """
    print("📅 MONTHLY SEMAGLUTIDE ANALYSIS WITH ELIGIBILITY")
    print("=" * 50)
    
    # Look for physician analysis files with diagnosis data
    # Try both possible filenames
    possible_files = [
        "physician_semaglutide_analysis_with_diagnosis.parquet",
        "physician_semaglutide_analysis.parquet"
    ]
    
    input_file = None
    for file in possible_files:
        if os.path.exists(file):
            input_file = file
            break
    
    if input_file is None:
        print(f"❌ No physician analysis file found!")
        print(f"   Looking for: {possible_files}")
        return None
    
    print(f"📁 Reading: {input_file}")
    df = pd.read_parquet(input_file)
    
    # Check if diagnosis columns exist
    if 'd_diagnosis_eligible_diab' not in df.columns or 'd_diagnosis_eligible_obes' not in df.columns:
        print(f"❌ Diagnosis eligibility columns not found in {input_file}")
        print(f"   Please run the diagnosis merge script first!")
        return None
    
    print(f"📊 Input data: {len(df):,} physician-date combinations")
    print(f"📅 Date range: {df['SVCDATE'].min()} to {df['SVCDATE'].max()}")
    
    # Convert SVCDATE to datetime and create year-month column
    df['SVCDATE'] = pd.to_datetime(df['SVCDATE'])
    df['year_month'] = df['SVCDATE'].dt.to_period('M')
    
    # Create eligibility flag: eligible if diabetes OR obesity diagnosis
    df['is_eligible'] = ((df['d_diagnosis_eligible_diab'] == 1) | 
                         (df['d_diagnosis_eligible_obes'] == 1)).astype(int)
    
    print(f"🗓️  Creating monthly buckets...")
    
    # Group by phys_ids and year_month
    monthly_stats = []
    
    for (phys_id, year_month), group in df.groupby(['phys_ids', 'year_month']):
        # Aggregate metrics for this physician-month
        total_patients = len(group)  # Each row is a unique patient-date prescription
        total_prescriptions = len(group)  # Same as patients in this context
        
        # Count eligible patients and prescriptions
        eligible_patients = group['is_eligible'].sum()
        eligible_prescriptions = group['is_eligible'].sum()
        
        # Count semaglutide prescriptions
        semaglutide_prescriptions = group['d_semaglutide'].sum()
        
        # Semaglutide among eligible patients only
        eligible_group = group[group['is_eligible'] == 1]
        semaglutide_in_eligible = eligible_group['d_semaglutide'].sum() if len(eligible_group) > 0 else 0
        
        # Calculate fractions
        fraction_patients_eligible = eligible_patients / total_patients if total_patients > 0 else 0
        fraction_prescriptions_semaglutide = semaglutide_prescriptions / total_prescriptions if total_prescriptions > 0 else 0
        fraction_eligible_get_semaglutide = semaglutide_in_eligible / eligible_patients if eligible_patients > 0 else 0
        
        # Count number of service dates in this month for this physician
        service_dates_count = group['SVCDATE'].nunique()
        
        monthly_stats.append({
            'phys_ids': phys_id,
            'year_month': str(year_month),
            'year': year_month.year,
            'month': year_month.month,
            'service_dates_count': service_dates_count,
            'total_patients': total_patients,
            'total_prescriptions': total_prescriptions,
            'eligible_patients': eligible_patients,
            'eligible_prescriptions': eligible_prescriptions,
            'semaglutide_prescriptions': semaglutide_prescriptions,
            'semaglutide_in_eligible': semaglutide_in_eligible,
            'fraction_patients_eligible': fraction_patients_eligible,
            'fraction_prescriptions_semaglutide': fraction_prescriptions_semaglutide,
            'fraction_eligible_get_semaglutide': fraction_eligible_get_semaglutide
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
    print(f"🎯 Eligible patients: {monthly_df['eligible_patients'].sum():,}")
    print(f"💊 Total semaglutide prescriptions: {monthly_df['semaglutide_prescriptions'].sum():,}")
    print(f"💊 Semaglutide in eligible patients: {monthly_df['semaglutide_in_eligible'].sum():,}")
    
    overall_eligible_rate = (monthly_df['eligible_patients'].sum() / monthly_df['total_patients'].sum() * 100)
    overall_sema_rate = (monthly_df['semaglutide_prescriptions'].sum() / monthly_df['total_prescriptions'].sum() * 100)
    overall_sema_in_eligible_rate = (monthly_df['semaglutide_in_eligible'].sum() / monthly_df['eligible_patients'].sum() * 100)
    
    print(f"📈 Overall eligibility rate: {overall_eligible_rate:.2f}%")
    print(f"📈 Overall semaglutide rate: {overall_sema_rate:.2f}%")
    print(f"📈 Semaglutide rate in eligible: {overall_sema_in_eligible_rate:.2f}%")
    
    return monthly_df

def save_monthly_results(df):
    """Save monthly results to files"""
    if df is None:
        return
    
    print(f"\n💾 Saving monthly results...")
    
    # Save as parquet
    output_parquet = "physician_monthly_semaglutide_with_eligibility.parquet"
    df.to_parquet(output_parquet, compression='snappy')
    print(f"✅ Saved: {output_parquet}")
    
    # Save as CSV
    output_csv = "physician_monthly_semaglutide_with_eligibility.csv"
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
    sample_cols = ['phys_ids', 'year_month', 'total_patients', 'eligible_patients',
                   'semaglutide_prescriptions', 'fraction_patients_eligible',
                   'fraction_eligible_get_semaglutide']
    print(df[sample_cols].head(10).round(4).to_string(index=False))
    
    print(f"\n📈 MONTHLY TRENDS:")
    monthly_summary = df.groupby('year_month').agg({
        'phys_ids': 'nunique',
        'total_prescriptions': 'sum',
        'eligible_prescriptions': 'sum',
        'semaglutide_prescriptions': 'sum',
        'semaglutide_in_eligible': 'sum'
    }).reset_index()
    
    monthly_summary['eligibility_rate_%'] = (monthly_summary['eligible_prescriptions'] / 
                                             monthly_summary['total_prescriptions'] * 100)
    monthly_summary['sema_rate_%'] = (monthly_summary['semaglutide_prescriptions'] / 
                                      monthly_summary['total_prescriptions'] * 100)
    monthly_summary['sema_in_eligible_%'] = (monthly_summary['semaglutide_in_eligible'] / 
                                            monthly_summary['eligible_prescriptions'] * 100)
    
    monthly_summary.columns = ['month', 'physicians', 'total_rx', 'eligible_rx', 'sema_rx', 
                               'sema_in_eligible', 'eligible_%', 'sema_%', 'sema_in_eligible_%']
    
    print("Recent months:")
    print(monthly_summary.tail(12).round(2).to_string(index=False))

def main():
    """Main function"""
    # Create monthly analysis with eligibility
    monthly_df = create_monthly_analysis_with_eligibility()
    
    if monthly_df is not None:
        # Save results
        save_monthly_results(monthly_df)
        
        # Show sample and trends
        show_sample_and_summary(monthly_df)
        
        print(f"\n🎉 MONTHLY ANALYSIS COMPLETE!")
        print(f"📄 Output files:")
        print(f"   • physician_monthly_semaglutide_with_eligibility.parquet")
        print(f"   • physician_monthly_semaglutide_with_eligibility.csv")
        
        return monthly_df
    
    return None

if __name__ == "__main__":
    results = main()
