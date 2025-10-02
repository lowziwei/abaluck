import pandas as pd
import glob
import os

def aggregate_to_physician_level():
    """
    Step 1: Aggregate prescription events to physician-date level
    """
    print("STEP 1: PHYSICIAN-DATE AGGREGATION")
    print("=" * 60)
    
    parquet_files = glob.glob("prescription_events_*_with_ndcnum_with_diagnosis.parquet")
    
    if not parquet_files:
        print("No files with diagnosis data found!")
        return None, None
    
    print(f"Found {len(parquet_files)} files to process")
    
    all_results = []
    
    for file in sorted(parquet_files):
        print(f"  Processing: {os.path.basename(file)}")
        df = pd.read_parquet(file)
        
        # Group by physician and date
        results = df.groupby(['phys_ids', 'SVCDATE']).agg({
            'ENROLID': 'nunique',
            'NDCNUM': 'count',
            'd_semaglutide': 'sum'
        }).reset_index()
        
        results.columns = ['phys_ids', 'SVCDATE', 'total_patients', 'total_prescriptions', 'semaglutide_prescriptions']
        
        # Count eligible patients (diabetes OR obesity)
        eligible_patients = df[
            (df['d_diagnosis_eligible_diab'] == 1) | (df['d_diagnosis_eligible_obes'] == 1)
        ].groupby(['phys_ids', 'SVCDATE'])['ENROLID'].nunique().reset_index()
        eligible_patients.columns = ['phys_ids', 'SVCDATE', 'eligible_patients']
        
        results = results.merge(eligible_patients, on=['phys_ids', 'SVCDATE'], how='left')
        results['eligible_patients'] = results['eligible_patients'].fillna(0).astype(int)
        
        # Count semaglutide patients
        sema_patients = df[df['d_semaglutide'] == 1].groupby(['phys_ids', 'SVCDATE'])['ENROLID'].nunique().reset_index()
        sema_patients.columns = ['phys_ids', 'SVCDATE', 'semaglutide_patients']
        
        results = results.merge(sema_patients, on=['phys_ids', 'SVCDATE'], how='left')
        results['semaglutide_patients'] = results['semaglutide_patients'].fillna(0).astype(int)
        
        all_results.append(results)
    
    # Combine all files
    final_df = pd.concat(all_results, ignore_index=True)
    
    # Aggregate any physician-date combinations split across files
    combined_df = final_df.groupby(['phys_ids', 'SVCDATE']).agg({
        'total_patients': 'sum',
        'total_prescriptions': 'sum',
        'semaglutide_prescriptions': 'sum',
        'semaglutide_patients': 'sum',
        'eligible_patients': 'sum'
    }).reset_index()
    
    # Calculate fractions
    combined_df['fraction_prescriptions_semaglutide'] = combined_df['semaglutide_prescriptions'] / combined_df['total_prescriptions']
    combined_df['fraction_patients_eligible'] = combined_df['eligible_patients'] / combined_df['total_patients']
    
    print(f"\nPhysician-date level complete:")
    print(f"  Unique physician-date combinations: {len(combined_df):,}")
    print(f"  Total prescriptions: {combined_df['total_prescriptions'].sum():,}")
    
    return combined_df, parquet_files

def aggregate_to_monthly_level(parquet_files):
    """
    Step 2: Aggregate prescription events directly to physician-month level
    This avoids double-counting patients who appear on multiple dates
    """
    print("\nSTEP 2: PHYSICIAN-MONTH AGGREGATION")
    print("=" * 60)
    
    all_monthly = []
    
    for file in sorted(parquet_files):
        print(f"  Processing: {os.path.basename(file)}")
        df = pd.read_parquet(file)
        
        # Convert to datetime and create year-month period
        df['SVCDATE'] = pd.to_datetime(df['SVCDATE'])
        df['year_month'] = df['SVCDATE'].dt.to_period('M')
        
        # Basic aggregation - unique patients and counts per physician-month
        monthly = df.groupby(['phys_ids', 'year_month']).agg({
            'ENROLID': 'nunique',      # Unique patients per month
            'NDCNUM': 'count',          # Total prescriptions
            'd_semaglutide': 'sum',     # Semaglutide prescriptions
            'SVCDATE': 'nunique'        # Number of service dates
        }).reset_index()
        
        monthly.columns = ['phys_ids', 'year_month', 'total_patients', 'total_prescriptions', 
                          'semaglutide_prescriptions', 'service_dates_count']
        
        # Count unique eligible patients per month (diabetes OR obesity)
        eligible_patients = df[
            (df['d_diagnosis_eligible_diab'] == 1) | (df['d_diagnosis_eligible_obes'] == 1)
        ].groupby(['phys_ids', 'year_month'])['ENROLID'].nunique().reset_index()
        eligible_patients.columns = ['phys_ids', 'year_month', 'eligible_patients']
        
        monthly = monthly.merge(eligible_patients, on=['phys_ids', 'year_month'], how='left')
        monthly['eligible_patients'] = monthly['eligible_patients'].fillna(0).astype(int)
        
        # Count unique semaglutide patients per month
        sema_patients = df[df['d_semaglutide'] == 1].groupby(['phys_ids', 'year_month'])['ENROLID'].nunique().reset_index()
        sema_patients.columns = ['phys_ids', 'year_month', 'semaglutide_patients']
        
        monthly = monthly.merge(sema_patients, on=['phys_ids', 'year_month'], how='left')
        monthly['semaglutide_patients'] = monthly['semaglutide_patients'].fillna(0).astype(int)
        
        # Count unique patients who are both eligible AND got semaglutide
        eligible_sema_patients = df[
            ((df['d_diagnosis_eligible_diab'] == 1) | (df['d_diagnosis_eligible_obes'] == 1)) & 
            (df['d_semaglutide'] == 1)
        ].groupby(['phys_ids', 'year_month'])['ENROLID'].nunique().reset_index()
        eligible_sema_patients.columns = ['phys_ids', 'year_month', 'semaglutide_in_eligible']
        
        monthly = monthly.merge(eligible_sema_patients, on=['phys_ids', 'year_month'], how='left')
        monthly['semaglutide_in_eligible'] = monthly['semaglutide_in_eligible'].fillna(0).astype(int)
        
        all_monthly.append(monthly)
    
    # Combine all files
    final_monthly = pd.concat(all_monthly, ignore_index=True)
    
    # Aggregate any physician-month combinations split across files
    combined_monthly = final_monthly.groupby(['phys_ids', 'year_month']).agg({
        'total_patients': 'sum',
        'total_prescriptions': 'sum',
        'semaglutide_prescriptions': 'sum',
        'semaglutide_patients': 'sum',
        'eligible_patients': 'sum',
        'semaglutide_in_eligible': 'sum',
        'service_dates_count': 'sum'
    }).reset_index()
    
    # Add year and month columns
    combined_monthly['year'] = combined_monthly['year_month'].apply(lambda x: x.year)
    combined_monthly['month'] = combined_monthly['year_month'].apply(lambda x: x.month)
    combined_monthly['year_month'] = combined_monthly['year_month'].astype(str)
    
    # Calculate fractions
    combined_monthly['fraction_patients_eligible'] = (
        combined_monthly['eligible_patients'] / combined_monthly['total_patients']
    ).fillna(0)
    
    combined_monthly['fraction_prescriptions_semaglutide'] = (
        combined_monthly['semaglutide_prescriptions'] / combined_monthly['total_prescriptions']
    ).fillna(0)
    
    combined_monthly['fraction_eligible_get_semaglutide'] = (
        combined_monthly['semaglutide_in_eligible'] / combined_monthly['eligible_patients']
    ).fillna(0)
    
    # Reorder columns
    combined_monthly = combined_monthly[[
        'phys_ids', 'year_month', 'year', 'month', 'service_dates_count',
        'total_patients', 'total_prescriptions', 'eligible_patients',
        'semaglutide_prescriptions', 'semaglutide_patients', 'semaglutide_in_eligible',
        'fraction_patients_eligible', 'fraction_prescriptions_semaglutide',
        'fraction_eligible_get_semaglutide'
    ]]
    
    combined_monthly = combined_monthly.sort_values(['phys_ids', 'year', 'month']).reset_index(drop=True)
    
    print(f"\nPhysician-month level complete:")
    print(f"  Monthly physician combinations: {len(combined_monthly):,}")
    print(f"  Unique physicians: {combined_monthly['phys_ids'].nunique():,}")
    print(f"  Total prescriptions: {combined_monthly['total_prescriptions'].sum():,}")
    print(f"  Unique patients (total across all months): {combined_monthly['total_patients'].sum():,}")
    print(f"  Eligible patients (total across all months): {combined_monthly['eligible_patients'].sum():,}")
    
    return combined_monthly

def main():
    """
    Combined aggregation pipeline:
    1. Aggregate individual prescription events to physician-date level
    2. Aggregate prescription events directly to physician-month level
    """
    print("PHYSICIAN SEMAGLUTIDE ANALYSIS - COMBINED AGGREGATION")
    print("=" * 60)
    
    # Step 1: Physician-date level
    physician_date_df, parquet_files = aggregate_to_physician_level()
    
    if physician_date_df is None:
        return
    
    # Save physician-date level
    physician_date_df.to_parquet("physician_semaglutide_analysis_with_diagnosis.parquet", compression='snappy')
    print(f"\nSaved: physician_semaglutide_analysis_with_diagnosis.parquet")
    
    # Step 2: Physician-month level (directly from event data)
    monthly_df = aggregate_to_monthly_level(parquet_files)
    
    # Save monthly level
    monthly_df.to_parquet("physician_monthly_semaglutide_with_eligibility.parquet", compression='snappy')
    monthly_df.to_csv("physician_monthly_semaglutide_with_eligibility.csv", index=False)
    
    print(f"\nSaved: physician_monthly_semaglutide_with_eligibility.parquet")
    print(f"Saved: physician_monthly_semaglutide_with_eligibility.csv")
    
    print(f"\nCOMPLETE!")

if __name__ == "__main__":
    main()
