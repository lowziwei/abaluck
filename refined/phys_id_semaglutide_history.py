import pandas as pd
import glob
import os

def process_single_file(file):
    """
    Process a single file and return both daily and monthly aggregations
    This ensures we only read each file once
    """
    print(f"  Processing: {os.path.basename(file)}")
    df = pd.read_parquet(file)
    
    # ===== DAILY AGGREGATION =====
    daily_results = df.groupby(['phys_ids', 'SVCDATE']).agg({
        'ENROLID': 'nunique',
        'NDCNUM': 'count',
        'd_semaglutide': 'sum'
    }).reset_index()
    daily_results.columns = ['phys_ids', 'SVCDATE', 'total_patients', 'total_prescriptions', 'semaglutide_prescriptions']
    
    # Daily eligible patients
    eligible_daily = df[
        (df['d_diagnosis_eligible_diab'] == 1) | (df['d_diagnosis_eligible_obes'] == 1)
    ].groupby(['phys_ids', 'SVCDATE'])['ENROLID'].nunique().reset_index()
    eligible_daily.columns = ['phys_ids', 'SVCDATE', 'eligible_patients']
    daily_results = daily_results.merge(eligible_daily, on=['phys_ids', 'SVCDATE'], how='left')
    daily_results['eligible_patients'] = daily_results['eligible_patients'].fillna(0).astype(int)
    
    # Daily semaglutide patients
    sema_daily = df[df['d_semaglutide'] == 1].groupby(['phys_ids', 'SVCDATE'])['ENROLID'].nunique().reset_index()
    sema_daily.columns = ['phys_ids', 'SVCDATE', 'semaglutide_patients']
    daily_results = daily_results.merge(sema_daily, on=['phys_ids', 'SVCDATE'], how='left')
    daily_results['semaglutide_patients'] = daily_results['semaglutide_patients'].fillna(0).astype(int)
    
    # ===== MONTHLY AGGREGATION =====
    df['SVCDATE'] = pd.to_datetime(df['SVCDATE'])
    df['year_month'] = df['SVCDATE'].dt.to_period('M')
    
    monthly_results = df.groupby(['phys_ids', 'year_month']).agg({
        'ENROLID': 'nunique',
        'NDCNUM': 'count',
        'd_semaglutide': 'sum',
        'SVCDATE': 'nunique'
    }).reset_index()
    monthly_results.columns = ['phys_ids', 'year_month', 'total_patients', 'total_prescriptions', 
                               'semaglutide_prescriptions', 'service_dates_count']
    
    # Monthly eligible patients
    eligible_monthly = df[
        (df['d_diagnosis_eligible_diab'] == 1) | (df['d_diagnosis_eligible_obes'] == 1)
    ].groupby(['phys_ids', 'year_month'])['ENROLID'].nunique().reset_index()
    eligible_monthly.columns = ['phys_ids', 'year_month', 'eligible_patients']
    monthly_results = monthly_results.merge(eligible_monthly, on=['phys_ids', 'year_month'], how='left')
    monthly_results['eligible_patients'] = monthly_results['eligible_patients'].fillna(0).astype(int)
    
    # Monthly semaglutide patients
    sema_monthly = df[df['d_semaglutide'] == 1].groupby(['phys_ids', 'year_month'])['ENROLID'].nunique().reset_index()
    sema_monthly.columns = ['phys_ids', 'year_month', 'semaglutide_patients']
    monthly_results = monthly_results.merge(sema_monthly, on=['phys_ids', 'year_month'], how='left')
    monthly_results['semaglutide_patients'] = monthly_results['semaglutide_patients'].fillna(0).astype(int)
    
    # Monthly eligible semaglutide patients
    eligible_sema = df[
        ((df['d_diagnosis_eligible_diab'] == 1) | (df['d_diagnosis_eligible_obes'] == 1)) & 
        (df['d_semaglutide'] == 1)
    ].groupby(['phys_ids', 'year_month'])['ENROLID'].nunique().reset_index()
    eligible_sema.columns = ['phys_ids', 'year_month', 'semaglutide_in_eligible']
    monthly_results = monthly_results.merge(eligible_sema, on=['phys_ids', 'year_month'], how='left')
    monthly_results['semaglutide_in_eligible'] = monthly_results['semaglutide_in_eligible'].fillna(0).astype(int)
    
    # Clear the dataframe from memory
    del df
    
    return daily_results, monthly_results

def main():
    """
    Memory-efficient aggregation pipeline:
    - Read each file only once
    - Compute both daily and monthly aggregations per file
    - Store only aggregated results (much smaller than raw data)
    """
    print("PHYSICIAN SEMAGLUTIDE ANALYSIS - MEMORY-EFFICIENT AGGREGATION")
    print("=" * 60)
    
    parquet_files = glob.glob("prescription_events_*_with_ndcnum_with_diagnosis.parquet")
    
    if not parquet_files:
        print("No files with diagnosis data found!")
        return
    
    print(f"Found {len(parquet_files)} files to process\n")
    
    all_daily = []
    all_monthly = []
    
    # Process each file once, computing both aggregations
    for file in sorted(parquet_files):
        daily, monthly = process_single_file(file)
        all_daily.append(daily)
        all_monthly.append(monthly)
    
    # ===== COMBINE DAILY RESULTS =====
    print("\nCombining daily results...")
    daily_combined = pd.concat(all_daily, ignore_index=True)
    del all_daily  # Free memory
    
    # Aggregate any physician-date combinations split across files
    daily_final = daily_combined.groupby(['phys_ids', 'SVCDATE']).agg({
        'total_patients': 'sum',
        'total_prescriptions': 'sum',
        'semaglutide_prescriptions': 'sum',
        'semaglutide_patients': 'sum',
        'eligible_patients': 'sum'
    }).reset_index()
    del daily_combined  # Free memory
    
    # Calculate fractions
    daily_final['fraction_prescriptions_semaglutide'] = (
        daily_final['semaglutide_prescriptions'] / daily_final['total_prescriptions']
    ).fillna(0)
    daily_final['fraction_patients_eligible'] = (
        daily_final['eligible_patients'] / daily_final['total_patients']
    ).fillna(0)
    
    print(f"Daily aggregation complete:")
    print(f"  Unique physician-date combinations: {len(daily_final):,}")
    print(f"  Total prescriptions: {daily_final['total_prescriptions'].sum():,}")
    
    # Save daily results
    daily_final.to_parquet("physician_semaglutide_analysis_with_diagnosis.parquet", compression='snappy')
    print(f"  Saved: physician_semaglutide_analysis_with_diagnosis.parquet")
    del daily_final  # Free memory
    
    # ===== COMBINE MONTHLY RESULTS =====
    print("\nCombining monthly results...")
    monthly_combined = pd.concat(all_monthly, ignore_index=True)
    del all_monthly  # Free memory
    
    # Aggregate any physician-month combinations split across files
    monthly_final = monthly_combined.groupby(['phys_ids', 'year_month']).agg({
        'total_patients': 'sum',
        'total_prescriptions': 'sum',
        'semaglutide_prescriptions': 'sum',
        'semaglutide_patients': 'sum',
        'eligible_patients': 'sum',
        'semaglutide_in_eligible': 'sum',
        'service_dates_count': 'sum'
    }).reset_index()
    del monthly_combined  # Free memory
    
    # Add year and month columns
    monthly_final['year'] = monthly_final['year_month'].apply(lambda x: x.year)
    monthly_final['month'] = monthly_final['year_month'].apply(lambda x: x.month)
    monthly_final['year_month'] = monthly_final['year_month'].astype(str)
    
    # Calculate fractions
    monthly_final['fraction_patients_eligible'] = (
        monthly_final['eligible_patients'] / monthly_final['total_patients']
    ).fillna(0)
    monthly_final['fraction_prescriptions_semaglutide'] = (
        monthly_final['semaglutide_prescriptions'] / monthly_final['total_prescriptions']
    ).fillna(0)
    monthly_final['fraction_eligible_get_semaglutide'] = (
        monthly_final['semaglutide_in_eligible'] / monthly_final['eligible_patients']
    ).fillna(0)
    
    # Reorder columns
    monthly_final = monthly_final[[
        'phys_ids', 'year_month', 'year', 'month', 'service_dates_count',
        'total_patients', 'total_prescriptions', 'eligible_patients',
        'semaglutide_prescriptions', 'semaglutide_patients', 'semaglutide_in_eligible',
        'fraction_patients_eligible', 'fraction_prescriptions_semaglutide',
        'fraction_eligible_get_semaglutide'
    ]]
    
    monthly_final = monthly_final.sort_values(['phys_ids', 'year', 'month']).reset_index(drop=True)
    
    print(f"Monthly aggregation complete:")
    print(f"  Monthly physician combinations: {len(monthly_final):,}")
    print(f"  Unique physicians: {monthly_final['phys_ids'].nunique():,}")
    print(f"  Total prescriptions: {monthly_final['total_prescriptions'].sum():,}")
    
    # Save monthly results
    monthly_final.to_parquet("physician_monthly_semaglutide_with_eligibility.parquet", compression='snappy')
    monthly_final.to_csv("physician_monthly_semaglutide_with_eligibility.csv", index=False)
    print(f"  Saved: physician_monthly_semaglutide_with_eligibility.parquet")
    print(f"  Saved: physician_monthly_semaglutide_with_eligibility.csv")
    
    print(f"\n✓ COMPLETE!")

if __name__ == "__main__":
    main()
