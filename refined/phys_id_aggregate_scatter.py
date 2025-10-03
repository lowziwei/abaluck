import pandas as pd
import numpy as np
from pathlib import Path

# Semaglutide NDC codes from your original script
SEMAGLUTIDE_NDCS = {
    # Ozempic
    '00169413001', '00169413013', '00169413211', '00169413212',
    '00169413290', '00169413297', '00169413602', '00169413611', 
    '00169418103', '00169418113', '00169418190', '00169418197', 
    '00169477211', '00169477212', '00169477290', '00169477297',
    '50090594900', '50090513800', '50090513900', '50090605100',
    
    # Rybelsus
    '00169430301', '00169430313', '00169430330', '00169430390', 
    '00169430393', '00169430399', '00169430701', '00169430713', 
    '00169430730', '00169431401', '00169431413', '00169431430', 
    '00169480430', '00169480930', '00169481530', '00169481590',
    
    # Wegovy  
    '00169450101', '00169450114', '00169450501', '00169450514',
    '00169451701', '00169451714', '00169452401', '00169452414', 
    '00169452501', '00169452514', '00169452590', '00169452594',
    '50090582400'
}

def process_physician_level_data(input_file, output_file, year):
    """
    Process prescription data to physician level with period-specific patient counts
    
    Parameters:
    -----------
    input_file : str
        Path to input parquet file with prescription and expenditure data
    output_file : str
        Path to save output parquet file
    year : int
        Year being processed (determines period boundaries)
    """
    
    print(f"{'='*70}")
    print(f"COLLAPSING TO PHYSICIAN LEVEL - YEAR {year}")
    print(f"{'='*70}")
    print(f"\nInput: {input_file}")
    
    # Define period boundaries based on year
    period1_start = pd.Timestamp(f'{year}-06-01')
    period1_end = pd.Timestamp(f'{year+1}-05-30')
    period2_start = pd.Timestamp(f'{year+1}-06-01')
    period2_end = pd.Timestamp(f'{year+2}-05-30')
    
    print(f"\nPeriod 1: {period1_start.date()} to {period1_end.date()}")
    print(f"Period 2: {period2_start.date()} to {period2_end.date()}")
    
    # Step 1: Load data
    print(f"\nStep 1: Loading data...")
    df = pd.read_parquet(input_file)
    print(f"  Initial records: {len(df):,}")
    print(f"  Unique patients: {df['ENROLID'].nunique():,}")
    print(f"  Unique physicians: {df['phys_ids'].nunique():,}")
    
    # Step 2: Sort by phys_ids
    print(f"\nStep 2: Sorting by phys_ids...")
    df = df.sort_values('phys_ids').reset_index(drop=True)
    
    # Step 3: Create semaglutide indicator if it doesn't exist
    print(f"\nStep 3: Creating semaglutide treatment indicator...")
    if 'd_semaglutide' not in df.columns:
        print(f"  Creating d_semaglutide from NDCNUM...")
        df['d_semaglutide'] = df['NDCNUM'].isin(SEMAGLUTIDE_NDCS).astype(int)
    else:
        print(f"  d_semaglutide already exists, using existing column")
    
    # Ensure SVCDATE is datetime
    df['SVCDATE'] = pd.to_datetime(df['SVCDATE'])
    
    # Step 4: Create period indicators based on prescription date
    print(f"\nStep 4: Identifying prescriptions by period...")
    df['in_period1'] = ((df['SVCDATE'] >= period1_start) & 
                        (df['SVCDATE'] <= period1_end)).astype(int)
    df['in_period2'] = ((df['SVCDATE'] >= period2_start) & 
                        (df['SVCDATE'] <= period2_end)).astype(int)
    
    period1_rx = df['in_period1'].sum()
    period2_rx = df['in_period2'].sum()
    print(f"  Prescriptions in Period 1: {period1_rx:,}")
    print(f"  Prescriptions in Period 2: {period2_rx:,}")
    
    # Step 5: Filter to patients with BOTH period expenditures
    print(f"\nStep 5: Filtering to patients with expenditures in BOTH periods...")
    patient_period_check = df.groupby('ENROLID').agg({
        'total_pay_period1': lambda x: (x > 0).any(),
        'total_pay_period2': lambda x: (x > 0).any()
    })
    
    patients_with_both = (patient_period_check['total_pay_period1'] & 
                          patient_period_check['total_pay_period2']).sum()
    print(f"  Patients with BOTH periods (>0): {patients_with_both:,}")
    
    patients_both_periods = patient_period_check[
        patient_period_check['total_pay_period1'] & 
        patient_period_check['total_pay_period2']
    ].index
    
    df_filtered = df[df['ENROLID'].isin(patients_both_periods)].copy()
    print(f"  Records after filtering: {len(df_filtered):,}")
    print(f"  Unique patients remaining: {df_filtered['ENROLID'].nunique():,}")
    print(f"  Unique physicians remaining: {df_filtered['phys_ids'].nunique():,}")
    
    # Step 6: Calculate physician-level metrics with period-specific counts
    print(f"\nStep 6: Calculating physician-level metrics...")
    
    # For each physician, we need:
    # - Unique patients in period 1
    # - Unique patients in period 2
    # - Unique patients receiving semaglutide
    # - Earliest semaglutide prescription date
    
    physician_metrics = []
    
    for phys_id, phys_df in df_filtered.groupby('phys_ids'):
        # Unique patients in period 1 (prescriptions during period 1)
        period1_patients = phys_df[phys_df['in_period1'] == 1]['ENROLID'].nunique()
        
        # Unique patients in period 2 (prescriptions during period 2)
        period2_patients = phys_df[phys_df['in_period2'] == 1]['ENROLID'].nunique()
        
        # Unique patients receiving semaglutide
        sema_patients = phys_df[phys_df['d_semaglutide'] == 1]['ENROLID'].nunique()
        
        # Total unique patients (across all periods)
        total_patients = phys_df['ENROLID'].nunique()
        
        # Earliest semaglutide prescription date
        sema_dates = phys_df[phys_df['d_semaglutide'] == 1]['SVCDATE']
        semaglutide_first = sema_dates.min() if len(sema_dates) > 0 else pd.NaT
        
        # Total prescriptions
        n_total_rx = len(phys_df)
        n_sema_rx = phys_df['d_semaglutide'].sum()
        n_period1_rx = phys_df['in_period1'].sum()
        n_period2_rx = phys_df['in_period2'].sum()
        
        # Expenditure sums (these are already aggregated by period at patient level)
        # We sum across all prescription records for this physician
        sum_total_pay_period1 = phys_df['total_pay_period1'].sum()
        sum_total_pay_period2 = phys_df['total_pay_period2'].sum()
        sum_inpatient_pay_period1 = phys_df['inpatient_pay_period1'].sum()
        sum_outpatient_pay_period1 = phys_df['outpatient_pay_period1'].sum()
        sum_drug_pay_period1 = phys_df['drug_pay_period1'].sum()
        sum_inpatient_pay_period2 = phys_df['inpatient_pay_period2'].sum()
        sum_outpatient_pay_period2 = phys_df['outpatient_pay_period2'].sum()
        sum_drug_pay_period2 = phys_df['drug_pay_period2'].sum()
        
        physician_metrics.append({
            'phys_ids': phys_id,
            'n_unique_patients_period1': period1_patients,
            'n_unique_patients_period2': period2_patients,
            'n_unique_patients_total': total_patients,
            'n_unique_patients_semaglutide': sema_patients,
            'n_prescriptions_period1': n_period1_rx,
            'n_prescriptions_period2': n_period2_rx,
            'n_prescriptions_total': n_total_rx,
            'n_prescriptions_semaglutide': n_sema_rx,
            'semaglutide_first': semaglutide_first,
            'sum_total_pay_period1': sum_total_pay_period1,
            'sum_total_pay_period2': sum_total_pay_period2,
            'sum_inpatient_pay_period1': sum_inpatient_pay_period1,
            'sum_outpatient_pay_period1': sum_outpatient_pay_period1,
            'sum_drug_pay_period1': sum_drug_pay_period1,
            'sum_inpatient_pay_period2': sum_inpatient_pay_period2,
            'sum_outpatient_pay_period2': sum_outpatient_pay_period2,
            'sum_drug_pay_period2': sum_drug_pay_period2
        })
    
    physician_final = pd.DataFrame(physician_metrics)
    
    # Calculate additional metrics
    physician_final['pct_prescriptions_semaglutide'] = (
        physician_final['n_prescriptions_semaglutide'] / 
        physician_final['n_prescriptions_total'] * 100
    )
    
    physician_final['pct_patients_semaglutide'] = (
        physician_final['n_unique_patients_semaglutide'] / 
        physician_final['n_unique_patients_total'] * 100
    )
    
    physician_final['has_semaglutide'] = (~physician_final['semaglutide_first'].isna()).astype(int)
    
    print(f"\n  Final physician-level dataset:")
    print(f"    Total physicians: {len(physician_final):,}")
    print(f"    Physicians with semaglutide: {physician_final['has_semaglutide'].sum():,}")
    
    # Step 7: Save output
    print(f"\nStep 7: Saving output to {output_file}...")
    physician_final.to_parquet(output_file, compression='snappy')
    
    file_size = Path(output_file).stat().st_size / (1024*1024)
    print(f"  Saved: {file_size:.2f} MB")
    
    # Display summary statistics
    print(f"\n{'='*70}")
    print("SUMMARY STATISTICS")
    print(f"{'='*70}")
    print(f"\nPhysician-level metrics:")
    print(f"  Total physicians: {len(physician_final):,}")
    print(f"  Physicians prescribing semaglutide: {physician_final['has_semaglutide'].sum():,}")
    
    print(f"\nPatient counts:")
    print(f"  Unique patients in Period 1: {physician_final['n_unique_patients_period1'].sum():,}")
    print(f"  Unique patients in Period 2: {physician_final['n_unique_patients_period2'].sum():,}")
    print(f"  Unique patients receiving semaglutide: {physician_final['n_unique_patients_semaglutide'].sum():,}")
    print(f"  Mean patients per physician (Period 1): {physician_final['n_unique_patients_period1'].mean():.1f}")
    print(f"  Mean patients per physician (Period 2): {physician_final['n_unique_patients_period2'].mean():.1f}")
    
    print(f"\nPrescription metrics:")
    print(f"  Total prescriptions (Period 1): {physician_final['n_prescriptions_period1'].sum():,}")
    print(f"  Total prescriptions (Period 2): {physician_final['n_prescriptions_period2'].sum():,}")
    print(f"  Semaglutide prescriptions: {physician_final['n_prescriptions_semaglutide'].sum():,}")
    
    print(f"\nExpenditure metrics (Period 1):")
    print(f"  Total expenditure: ${physician_final['sum_total_pay_period1'].sum():,.2f}")
    print(f"  Mean per physician: ${physician_final['sum_total_pay_period1'].mean():,.2f}")
    
    print(f"\nExpenditure metrics (Period 2):")
    print(f"  Total expenditure: ${physician_final['sum_total_pay_period2'].sum():,.2f}")
    print(f"  Mean per physician: ${physician_final['sum_total_pay_period2'].mean():,.2f}")
    
    return physician_final

def main():
    """
    Main function: Process all year files
    """
    print(f"{'='*70}")
    print("PHYSICIAN-LEVEL SEMAGLUTIDE ANALYSIS")
    print(f"{'='*70}")
    
    # Find input files
    years = [2021, 2022, 2023]
    input_files = []
    
    for year in years:
        pattern = f"prescription_events_{year}_with_expenditure_by_period_no_sema_eligible_only.parquet"
        if Path(pattern).exists():
            input_files.append((year, pattern))
        else:
            print(f"WARNING: File not found for {year}: {pattern}")
    
    if not input_files:
        print("ERROR: No input files found!")
        return
    
    print(f"\nFound {len(input_files)} files to process:")
    for year, filepath in input_files:
        file_size = Path(filepath).stat().st_size / (1024*1024)
        print(f"  {year}: {filepath} ({file_size:.1f} MB)")
    
    # Process each file
    all_results = []
    for year, input_file in input_files:
        print(f"\n{'='*70}")
        print(f"PROCESSING YEAR {year}")
        print(f"{'='*70}")
        
        output_file = f"physician_level_{year}_semaglutide_analysis.parquet"
        result = process_physician_level_data(input_file, output_file, year)
        all_results.append((year, result, output_file))
    
    # Final summary
    print(f"\n{'='*70}")
    print("ALL FILES PROCESSED")
    print(f"{'='*70}")
    print(f"\nOutput files created:")
    for year, result, output_file in all_results:
        file_size = Path(output_file).stat().st_size / (1024*1024)
        print(f"  {year}: {output_file} ({file_size:.2f} MB)")
        print(f"        {len(result):,} physicians")
        print(f"        Period 1 patients: {result['n_unique_patients_period1'].sum():,}")
        print(f"        Period 2 patients: {result['n_unique_patients_period2'].sum():,}")
        print(f"        Semaglutide patients: {result['n_unique_patients_semaglutide'].sum():,}")

if __name__ == "__main__":
    main()
