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
        return None
    
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
    
    return combined_df

def aggregate_to_monthly_level(physician_date_df):
    """
    Step 2: Aggregate physician-date data to physician-month level
    """
    print("\nSTEP 2: PHYSICIAN-MONTH AGGREGATION")
    print("=" * 60)
    
    df = physician_date_df.copy()
    df['SVCDATE'] = pd.to_datetime(df['SVCDATE'])
    df['year_month'] = df['SVCDATE'].dt.to_period('M')
    
    monthly_stats = []
    
    for (phys_id, year_month), group in df.groupby(['phys_ids', 'year_month']):
        total_patients = group['total_patients'].sum()
        total_prescriptions = group['total_prescriptions'].sum()
        eligible_patients = group['eligible_patients'].sum()
        semaglutide_prescriptions = group['semaglutide_prescriptions'].sum()
        semaglutide_patients = group['semaglutide_patients'].sum()
        
        # Semaglutide in eligible (approximate from aggregated data)
        # This is an approximation since we don't have individual patient eligibility at this level
        semaglutide_in_eligible = min(semaglutide_patients, eligible_patients)
        
        fraction_patients_eligible = eligible_patients / total_patients if total_patients > 0 else 0
        fraction_prescriptions_semaglutide = semaglutide_prescriptions / total_prescriptions if total_prescriptions > 0 else 0
        fraction_eligible_get_semaglutide = semaglutide_in_eligible / eligible_patients if eligible_patients > 0 else 0
        
        service_dates_count = len(group)
        
        monthly_stats.append({
            'phys_ids': phys_id,
            'year_month': str(year_month),
            'year': year_month.year,
            'month': year_month.month,
            'service_dates_count': service_dates_count,
            'total_patients': total_patients,
            'total_prescriptions': total_prescriptions,
            'eligible_patients': eligible_patients,
            'semaglutide_prescriptions': semaglutide_prescriptions,
            'semaglutide_patients': semaglutide_patients,
            'semaglutide_in_eligible': semaglutide_in_eligible,
            'fraction_patients_eligible': fraction_patients_eligible,
            'fraction_prescriptions_semaglutide': fraction_prescriptions_semaglutide,
            'fraction_eligible_get_semaglutide': fraction_eligible_get_semaglutide
        })
    
    monthly_df = pd.DataFrame(monthly_stats)
    monthly_df = monthly_df.sort_values(['phys_ids', 'year', 'month']).reset_index(drop=True)
    
    print(f"Physician-month level complete:")
    print(f"  Monthly physician combinations: {len(monthly_df):,}")
    print(f"  Unique physicians: {monthly_df['phys_ids'].nunique():,}")
    print(f"  Total prescriptions: {monthly_df['total_prescriptions'].sum():,}")
    print(f"  Eligible patients: {monthly_df['eligible_patients'].sum():,}")
    
    return monthly_df

def main():
    """
    Combined aggregation pipeline:
    1. Aggregate individual prescription events to physician-date level
    2. Aggregate physician-date to physician-month level
    """
    print("PHYSICIAN SEMAGLUTIDE ANALYSIS - COMBINED AGGREGATION")
    print("=" * 60)
    
    # Step 1: Physician-date level
    physician_date_df = aggregate_to_physician_level()
    
    if physician_date_df is None:
        return
    
    # Save physician-date level
    physician_date_df.to_parquet("physician_semaglutide_analysis_with_diagnosis.parquet", compression='snappy')
    print(f"\nSaved: physician_semaglutide_analysis_with_diagnosis.parquet")
    
    # Step 2: Physician-month level
    monthly_df = aggregate_to_monthly_level(physician_date_df)
    
    # Save monthly level
    monthly_df.to_parquet("physician_monthly_semaglutide_with_eligibility.parquet", compression='snappy')
    monthly_df.to_csv("physician_monthly_semaglutide_with_eligibility.csv", index=False)
    
    print(f"\nSaved: physician_monthly_semaglutide_with_eligibility.parquet")
    print(f"Saved: physician_monthly_semaglutide_with_eligibility.csv")
    
    print(f"\nCOMPLETE!")

if __name__ == "__main__":
    main()
