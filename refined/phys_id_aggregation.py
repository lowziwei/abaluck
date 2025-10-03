import pandas as pd
from pathlib import Path

# Semaglutide NDC codes
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

# Fixed period definitions
PERIOD1_START = pd.Timestamp('2021-06-01')
PERIOD1_END = pd.Timestamp('2022-05-30')
PERIOD2_START = pd.Timestamp('2022-06-01')
PERIOD2_END = pd.Timestamp('2023-05-30')

def process_physician_level_data(input_file, output_file, year):
    """
    Aggregate prescription data to physician level with semaglutide indicators
    
    Note: Each file only contains prescriptions from that calendar year, so:
    - 2021 file: Can only detect semaglutide for June-Dec 2021 (partial Period 1)
    - 2022 file: Can detect for Jan-May 2022 (Period 1) + June-Dec 2022 (Period 2) 
    - 2023 file: Can only detect semaglutide for Jan-May 2023 (partial Period 2)
    """
    
    print(f"\n{'='*70}")
    print(f"PROCESSING YEAR {year}")
    print(f"{'='*70}")
    
    # Load data
    df = pd.read_parquet(input_file)
    df['SVCDATE'] = pd.to_datetime(df['SVCDATE'])
    
    print(f"Loaded {len(df):,} prescriptions")
    print(f"Date range: {df['SVCDATE'].min().date()} to {df['SVCDATE'].max().date()}")
    
    # Create semaglutide indicator
    df['is_semaglutide'] = df['NDCNUM'].isin(SEMAGLUTIDE_NDCS).astype(int)
    
    # Identify semaglutide prescriptions in each period (for this file's date range)
    df['sema_in_period1'] = ((df['is_semaglutide'] == 1) & 
                              (df['SVCDATE'] >= PERIOD1_START) & 
                              (df['SVCDATE'] <= PERIOD1_END)).astype(int)
    
    df['sema_in_period2'] = ((df['is_semaglutide'] == 1) & 
                              (df['SVCDATE'] >= PERIOD2_START) & 
                              (df['SVCDATE'] <= PERIOD2_END)).astype(int)
    
    print(f"Semaglutide Rx in Period 1 (this file): {df['sema_in_period1'].sum():,}")
    print(f"Semaglutide Rx in Period 2 (this file): {df['sema_in_period2'].sum():,}")
    
    # Aggregate to patient-physician level
    patient_phys = df.groupby(['ENROLID', 'phys_ids'], as_index=False).agg({
        'total_pay_period1': 'first',
        'total_pay_period2': 'first',
        'inpatient_pay_period1': 'first',
        'outpatient_pay_period1': 'first',
        'drug_pay_period1': 'first',
        'inpatient_pay_period2': 'first',
        'outpatient_pay_period2': 'first',
        'drug_pay_period2': 'first',
        'sema_in_period1': 'max',  # 1 if patient had ANY semaglutide Rx in period 1
        'sema_in_period2': 'max',  # 1 if patient had ANY semaglutide Rx in period 2
    })
    
    # Filter to patients with expenditures in BOTH periods
    both_periods = ((patient_phys['total_pay_period1'] > 0) & 
                    (patient_phys['total_pay_period2'] > 0))
    patient_phys = patient_phys[both_periods].copy()
    
    print(f"Patient-physician pairs (both periods): {len(patient_phys):,}")
    
    # Aggregate to physician level
    physician_stats = patient_phys.groupby('phys_ids', as_index=False).agg({
        'ENROLID': 'count',
        'sema_in_period1': 'sum',
        'sema_in_period2': 'sum',
        'total_pay_period1': 'sum',
        'total_pay_period2': 'sum',
        'inpatient_pay_period1': 'sum',
        'outpatient_pay_period1': 'sum',
        'drug_pay_period1': 'sum',
        'inpatient_pay_period2': 'sum',
        'outpatient_pay_period2': 'sum',
        'drug_pay_period2': 'sum',
    })
    
    # Rename for clarity
    physician_stats.columns = [
        'phys_ids', 'n_patients', 
        'n_sema_period1', 'n_sema_period2',
        'total_pay_p1', 'total_pay_p2',
        'inpatient_p1', 'outpatient_p1', 'drug_p1',
        'inpatient_p2', 'outpatient_p2', 'drug_p2'
    ]
    
    # Save
    physician_stats.to_parquet(output_file, compression='snappy')
    
    print(f"\nPhysician-level summary:")
    print(f"  Total physicians: {len(physician_stats):,}")
    print(f"  Physicians with semaglutide (Period 1): {(physician_stats['n_sema_period1'] > 0).sum():,}")
    print(f"  Physicians with semaglutide (Period 2): {(physician_stats['n_sema_period2'] > 0).sum():,}")
    print(f"  Total patients on semaglutide (Period 1): {physician_stats['n_sema_period1'].sum():,}")
    print(f"  Total patients on semaglutide (Period 2): {physician_stats['n_sema_period2'].sum():,}")
    print(f"\nSaved to: {output_file}")
    
    return physician_stats

def main():
    years = [2021, 2022, 2023]
    
    for year in years:
        input_file = f"prescription_events_{year}_with_expenditure_by_period_no_sema_eligible_only.parquet"
        output_file = f"physician_level_{year}_semaglutide.parquet"
        
        if Path(input_file).exists():
            process_physician_level_data(input_file, output_file, year)
        else:
            print(f"WARNING: {input_file} not found, skipping...")

if __name__ == "__main__":
    main()
