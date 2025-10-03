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

def process_physician_level_data(input_file, output_file):
    """
    Process prescription data to physician level:
    1. Sort by PHYS_ID
    2. Keep only patients with total_pay values for BOTH periods
    3. Create semaglutide indicator if needed
    4. Collapse to physician level with earliest semaglutide date
    
    Parameters:
    -----------
    input_file : str
        Path to input parquet file with prescription and expenditure data
    output_file : str
        Path to save output parquet file
    """
    
    print(f"{'='*70}")
    print(f"COLLAPSING TO PHYSICIAN LEVEL")
    print(f"{'='*70}")
    print(f"\nInput: {input_file}")
    
    # Step 1: Load data
    print(f"\nStep 1: Loading data...")
    df = pd.read_parquet(input_file)
    print(f"  Initial records: {len(df):,}")
    print(f"  Unique patients: {df['ENROLID'].nunique():,}")
    print(f"  Unique physicians: {df['PHYS_ID'].nunique():,}")
    
    # Step 2: Sort by PHYS_ID
    print(f"\nStep 2: Sorting by PHYS_ID...")
    df = df.sort_values('PHYS_ID').reset_index(drop=True)
    print(f"  Sorted!")
    
    # Step 3: Create semaglutide indicator if it doesn't exist
    print(f"\nStep 3: Creating semaglutide treatment indicator...")
    if 'd_semaglutide' not in df.columns:
        print(f"  Creating d_semaglutide from NDCNUM...")
        df['d_semaglutide'] = df['NDCNUM'].isin(SEMAGLUTIDE_NDCS).astype(int)
    else:
        print(f"  d_semaglutide already exists, using existing column")
    
    sema_count = df['d_semaglutide'].sum()
    total_count = len(df)
    print(f"  Semaglutide prescriptions: {sema_count:,} ({sema_count/total_count*100:.2f}%)")
    print(f"  Non-semaglutide prescriptions: {total_count - sema_count:,} ({(total_count-sema_count)/total_count*100:.2f}%)")
    
    # Step 4: Filter to patients with BOTH period expenditures
    print(f"\nStep 4: Filtering to patients with expenditures in BOTH periods...")
    print(f"  Checking which patients have total_pay > 0 for both periods...")
    
    # For each patient, check if they have non-zero total_pay in both periods
    patient_period_check = df.groupby('ENROLID').agg({
        'total_pay_period1': lambda x: (x > 0).any(),
        'total_pay_period2': lambda x: (x > 0).any()
    })
    
    # Get patients with BOTH periods
    patients_with_period1 = patient_period_check['total_pay_period1'].sum()
    patients_with_period2 = patient_period_check['total_pay_period2'].sum()
    patients_with_both = (patient_period_check['total_pay_period1'] & 
                          patient_period_check['total_pay_period2']).sum()
    
    print(f"  Patients with Period 1 expenditures (>0): {patients_with_period1:,}")
    print(f"  Patients with Period 2 expenditures (>0): {patients_with_period2:,}")
    print(f"  Patients with BOTH periods (>0): {patients_with_both:,}")
    
    # Get the set of patients with both
    patients_both_periods = patient_period_check[
        patient_period_check['total_pay_period1'] & 
        patient_period_check['total_pay_period2']
    ].index
    
    # Filter dataframe
    df_filtered = df[df['ENROLID'].isin(patients_both_periods)].copy()
    print(f"\n  Records after filtering:")
    print(f"    Before: {len(df):,} records")
    print(f"    After: {len(df_filtered):,} records")
    print(f"    Removed: {len(df) - len(df_filtered):,} records")
    print(f"    Unique patients remaining: {df_filtered['ENROLID'].nunique():,}")
    print(f"    Unique physicians remaining: {df_filtered['PHYS_ID'].nunique():,}")
    
    # Step 5: Collapse to physician level
    print(f"\nStep 5: Collapsing to physician level...")
    
    # Find earliest semaglutide prescription date per physician
    semaglutide_df = df_filtered[df_filtered['d_semaglutide'] == 1].copy()
    
    if len(semaglutide_df) > 0:
        # Ensure SVCDATE is datetime
        semaglutide_df['SVCDATE'] = pd.to_datetime(semaglutide_df['SVCDATE'])
        
        # Get earliest semaglutide date per physician
        physician_semaglutide = semaglutide_df.groupby('PHYS_ID')['SVCDATE'].min().reset_index()
        physician_semaglutide.columns = ['PHYS_ID', 'semaglutide_first']
        
        print(f"  Physicians with at least one semaglutide prescription: {len(physician_semaglutide):,}")
        
        # Show distribution of first prescription dates
        print(f"\n  First semaglutide prescription dates:")
        print(f"    Earliest: {physician_semaglutide['semaglutide_first'].min()}")
        print(f"    Latest: {physician_semaglutide['semaglutide_first'].max()}")
    else:
        physician_semaglutide = pd.DataFrame(columns=['PHYS_ID', 'semaglutide_first'])
        print(f"  WARNING: No semaglutide prescriptions found in filtered data!")
    
    # Aggregate other physician-level statistics
    print(f"\n  Aggregating physician-level statistics...")
    physician_stats = df_filtered.groupby('PHYS_ID').agg({
        'ENROLID': 'nunique',  # unique patients per physician
        'd_semaglutide': 'sum',  # total semaglutide prescriptions
        'SVCDATE': 'count',  # total prescriptions
        'total_pay_period1': 'sum',  # sum of all period1 expenditures
        'total_pay_period2': 'sum',  # sum of all period2 expenditures
        'inpatient_pay_period1': 'sum',
        'outpatient_pay_period1': 'sum',
        'drug_pay_period1': 'sum',
        'inpatient_pay_period2': 'sum',
        'outpatient_pay_period2': 'sum',
        'drug_pay_period2': 'sum'
    }).reset_index()
    
    # Rename columns for clarity
    physician_stats.columns = [
        'PHYS_ID',
        'n_unique_patients',
        'n_semaglutide_prescriptions',
        'n_total_prescriptions',
        'sum_total_pay_period1',
        'sum_total_pay_period2',
        'sum_inpatient_pay_period1',
        'sum_outpatient_pay_period1',
        'sum_drug_pay_period1',
        'sum_inpatient_pay_period2',
        'sum_outpatient_pay_period2',
        'sum_drug_pay_period2'
    ]
    
    # Merge with semaglutide_first dates
    physician_final = physician_stats.merge(
        physician_semaglutide,
        on='PHYS_ID',
        how='left'
    )
    
    # Calculate additional metrics
    physician_final['pct_prescriptions_semaglutide'] = (
        physician_final['n_semaglutide_prescriptions'] / 
        physician_final['n_total_prescriptions'] * 100
    )
    
    physician_final['has_semaglutide'] = (~physician_final['semaglutide_first'].isna()).astype(int)
    
    print(f"\n  Final physician-level dataset:")
    print(f"    Total physicians: {len(physician_final):,}")
    print(f"    Physicians with semaglutide: {physician_final['has_semaglutide'].sum():,}")
    print(f"    Physicians without semaglutide: {(physician_final['has_semaglutide'] == 0).sum():,}")
    
    # Step 6: Save output
    print(f"\nStep 6: Saving output to {output_file}...")
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
    print(f"\nPrescription metrics:")
    print(f"  Total patients: {physician_final['n_unique_patients'].sum():,}")
    print(f"  Total prescriptions: {physician_final['n_total_prescriptions'].sum():,}")
    print(f"  Semaglutide prescriptions: {physician_final['n_semaglutide_prescriptions'].sum():,}")
    print(f"  Mean prescriptions per physician: {physician_final['n_total_prescriptions'].mean():.1f}")
    print(f"  Median prescriptions per physician: {physician_final['n_total_prescriptions'].median():.1f}")
    
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
        result = process_physician_level_data(input_file, output_file)
        all_results.append((year, result, output_file))
    
    # Final summary
    print(f"\n{'='*70}")
    print("ALL FILES PROCESSED")
    print(f"{'='*70}")
    print(f"\nOutput files created:")
    for year, result, output_file in all_results:
        file_size = Path(output_file).stat().st_size / (1024*1024)
        print(f"  {year}: {output_file} ({file_size:.2f} MB)")
        print(f"        {len(result):,} physicians, {result['has_semaglutide'].sum():,} with semaglutide")

if __name__ == "__main__":
    main()
