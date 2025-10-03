import pandas as pd
import duckdb
import os
import time
from pathlib import Path

# Configuration
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"
PERIOD1_START = '2021-06-01'
PERIOD1_END = '2022-05-31'
PERIOD2_START = '2022-06-01'
PERIOD2_END = '2023-05-31'

# Semaglutide NDC codes - all products (Ozempic, Rybelsus, Wegovy)
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

def setup_duckdb_connection(memory_limit='8GB'):
    """
    Create optimized DuckDB connection with performance settings
    """
    conn = duckdb.connect(':memory:')
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.execute("SET max_temp_directory_size='50GB'")
    conn.execute("SET temp_directory='/tmp'")
    conn.execute("SET threads=8")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute("SET enable_progress_bar=false")
    return conn

def extract_patient_expenditures(patient_ids, period_start, period_end, period_name):
    """
    Extract total expenditures per patient for a specific time period
    Aggregates ALL expenditures for each patient during the period
    EXCLUDES semaglutide drug costs
    
    Parameters:
    -----------
    patient_ids : set - Patient IDs to query
    period_start : str - Start date (YYYY-MM-DD)
    period_end : str - End date (YYYY-MM-DD)
    period_name : str - Name for logging (e.g., "Period 1")
    
    Returns:
    --------
    DataFrame with columns: ENROLID, inpatient_pay, outpatient_pay, drug_pay, total_pay
    """
    print(f"    Extracting expenditures for {len(patient_ids):,} patients")
    print(f"    Period: {period_start} to {period_end}")
    print(f"    NOTE: Excluding semaglutide drug costs")
    
    conn = setup_duckdb_connection()
    
    try:
        # Create patient lookup
        patients_df = pd.DataFrame({'ENROLID': list(patient_ids)})
        conn.register('target_patients', patients_df)
        
        # Register semaglutide NDC codes
        sema_df = pd.DataFrame({'NDCNUM': list(SEMAGLUTIDE_NDCS)})
        conn.register('semaglutide_ndcs', sema_df)
        
        data_path = f"/data/MarketScan_data/{DATASET_TYPE}"
        
        # Determine which years to query based on period
        period_start_dt = pd.to_datetime(period_start)
        period_end_dt = pd.to_datetime(period_end)
        years_to_query = list(range(period_start_dt.year, period_end_dt.year + 1))
        print(f"      Years to query: {years_to_query}")
        
        # Extract inpatient expenditures (TOTNET) - aggregate by patient
        print(f"      Querying inpatient...")
        inpatient_dfs = []
        inpatient_file = f"{data_path}/{DATABASE}_I.parquet"
        if Path(inpatient_file).exists():
            inpatient_query = f"""
            SELECT 
                ENROLID,
                SUM(COALESCE(TOTNET, 0)) as inpatient_pay
            FROM '{inpatient_file}'
            WHERE ENROLID IN (SELECT ENROLID FROM target_patients)
            AND ADMDATE >= DATE '{period_start}'
            AND ADMDATE <= DATE '{period_end}'
            GROUP BY ENROLID
            """
            inpatient_dfs.append(conn.execute(inpatient_query).fetchdf())
        
        if inpatient_dfs:
            inpatient_df = pd.concat(inpatient_dfs, ignore_index=True)
            inpatient_df = inpatient_df.groupby('ENROLID', as_index=False)['inpatient_pay'].sum()
            print(f"        {len(inpatient_df):,} patients with inpatient claims, ${inpatient_df['inpatient_pay'].sum():,.2f} total")
        else:
            inpatient_df = pd.DataFrame(columns=['ENROLID', 'inpatient_pay'])
            print(f"        No inpatient data found")
        
        # Extract outpatient expenditures (NETPAY) - aggregate by patient across years
        print(f"      Querying outpatient...")
        outpatient_dfs = []
        for year in years_to_query:
            outpatient_file = f"{data_path}/{DATABASE}_O_{year}.parquet"
            if Path(outpatient_file).exists():
                print(f"        Reading {DATABASE}_O_{year}.parquet...")
                outpatient_query = f"""
                SELECT 
                    ENROLID,
                    SUM(COALESCE(NETPAY, 0)) as outpatient_pay
                FROM '{outpatient_file}'
                WHERE ENROLID IN (SELECT ENROLID FROM target_patients)
                AND SVCDATE >= DATE '{period_start}'
                AND SVCDATE <= DATE '{period_end}'
                GROUP BY ENROLID
                """
                outpatient_dfs.append(conn.execute(outpatient_query).fetchdf())
        
        if outpatient_dfs:
            outpatient_df = pd.concat(outpatient_dfs, ignore_index=True)
            outpatient_df = outpatient_df.groupby('ENROLID', as_index=False)['outpatient_pay'].sum()
            print(f"        {len(outpatient_df):,} patients with outpatient claims, ${outpatient_df['outpatient_pay'].sum():,.2f} total")
        else:
            outpatient_df = pd.DataFrame(columns=['ENROLID', 'outpatient_pay'])
            print(f"        No outpatient data found")
        
        # Extract drug expenditures (NETPAY) - EXCLUDING SEMAGLUTIDE - aggregate by patient across years
        print(f"      Querying drug (excluding semaglutide)...")
        drug_dfs = []
        semaglutide_excluded_cost = 0
        semaglutide_excluded_claims = 0
        
        for year in years_to_query:
            drug_file = f"{data_path}/{DATABASE}_D_{year}.parquet"
            if Path(drug_file).exists():
                print(f"        Reading {DATABASE}_D_{year}.parquet...")
                
                # First, get semaglutide costs for reporting
                sema_cost_query = f"""
                SELECT 
                    COUNT(*) as sema_claims,
                    SUM(COALESCE(NETPAY, 0)) as sema_cost
                FROM '{drug_file}'
                WHERE ENROLID IN (SELECT ENROLID FROM target_patients)
                AND SVCDATE >= DATE '{period_start}'
                AND SVCDATE <= DATE '{period_end}'
                AND NDCNUM IN (SELECT NDCNUM FROM semaglutide_ndcs)
                """
                sema_stats = conn.execute(sema_cost_query).fetchdf()
                if len(sema_stats) > 0 and sema_stats['sema_cost'].iloc[0] is not None:
                    semaglutide_excluded_cost += sema_stats['sema_cost'].iloc[0]
                    semaglutide_excluded_claims += sema_stats['sema_claims'].iloc[0]
                
                # Query drug costs EXCLUDING semaglutide
                drug_query = f"""
                SELECT 
                    ENROLID,
                    SUM(COALESCE(NETPAY, 0)) as drug_pay
                FROM '{drug_file}'
                WHERE ENROLID IN (SELECT ENROLID FROM target_patients)
                AND SVCDATE >= DATE '{period_start}'
                AND SVCDATE <= DATE '{period_end}'
                AND NDCNUM NOT IN (SELECT NDCNUM FROM semaglutide_ndcs)
                GROUP BY ENROLID
                """
                drug_dfs.append(conn.execute(drug_query).fetchdf())
        
        if drug_dfs:
            drug_df = pd.concat(drug_dfs, ignore_index=True)
            drug_df = drug_df.groupby('ENROLID', as_index=False)['drug_pay'].sum()
            print(f"        {len(drug_df):,} patients with drug claims (non-semaglutide), ${drug_df['drug_pay'].sum():,.2f} total")
            if semaglutide_excluded_cost > 0:
                print(f"        EXCLUDED: {semaglutide_excluded_claims:,} semaglutide claims, ${semaglutide_excluded_cost:,.2f} total")
        else:
            drug_df = pd.DataFrame(columns=['ENROLID', 'drug_pay'])
            print(f"        No drug data found")
        
        # Merge all expenditures by patient
        conn.register('inpatient_exp', inpatient_df)
        conn.register('outpatient_exp', outpatient_df)
        conn.register('drug_exp', drug_df)
        
        merge_query = """
        SELECT 
            tp.ENROLID,
            COALESCE(i.inpatient_pay, 0) as inpatient_pay,
            COALESCE(o.outpatient_pay, 0) as outpatient_pay,
            COALESCE(d.drug_pay, 0) as drug_pay,
            COALESCE(i.inpatient_pay, 0) + COALESCE(o.outpatient_pay, 0) + COALESCE(d.drug_pay, 0) as total_pay
        FROM target_patients tp
        LEFT JOIN inpatient_exp i ON tp.ENROLID = i.ENROLID
        LEFT JOIN outpatient_exp o ON tp.ENROLID = o.ENROLID
        LEFT JOIN drug_exp d ON tp.ENROLID = d.ENROLID
        """
        
        result_df = conn.execute(merge_query).fetchdf()
        conn.close()
        
        # Summary
        patients_with_spend = (result_df['total_pay'] > 0).sum()
        print(f"      Summary: {patients_with_spend:,} patients with expenditures ({patients_with_spend/len(result_df)*100:.1f}%)")
        print(f"      Total expenditure (excluding semaglutide): ${result_df['total_pay'].sum():,.2f}")
        
        return result_df
        
    except Exception as e:
        print(f"      Error extracting expenditures: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.close()
        result_df = patients_df.copy()
        result_df['inpatient_pay'] = 0.0
        result_df['outpatient_pay'] = 0.0
        result_df['drug_pay'] = 0.0
        result_df['total_pay'] = 0.0
        return result_df

def process_file(input_file, year):
    """
    Process a single aggregated diagnosis file:
    1. Load the file
    2. Filter to semaglutide eligible patients
    3. Get unique patients
    4. Extract expenditures for two periods per patient
    5. Merge and save
    """
    print(f"\n{'='*70}")
    print(f"PROCESSING: {input_file}")
    print(f"{'='*70}")
    
    # Load file
    print(f"  Step 1: Loading file...")
    df = pd.read_parquet(input_file)
    print(f"    Total events: {len(df):,}")
    
    # Filter to semaglutide eligible patients only
    print(f"  Step 1b: Filtering to semaglutide eligible patients...")
    eligible_mask = (df['d_diagnosis_eligible_diab'] == 1) | (df['d_diagnosis_eligible_obes'] == 1)
    df_eligible = df[eligible_mask].copy()
    print(f"    Events before filtering: {len(df):,}")
    print(f"    Events after filtering to eligible: {len(df_eligible):,}")
    print(f"    Eligible patients with diabetes: {(df['d_diagnosis_eligible_diab'] == 1).sum():,}")
    print(f"    Eligible patients with obesity: {(df['d_diagnosis_eligible_obes'] == 1).sum():,}")
    
    # Get unique eligible patients
    unique_patients = set(df_eligible['ENROLID'].unique())
    print(f"    Unique eligible patients: {len(unique_patients):,}")
    
    # Extract expenditures for Period 1 (June 2021 - May 2022)
    print(f"\n  Step 2: Extracting Period 1 expenditures (June 2021 - May 2022)...")
    period1_exp = extract_patient_expenditures(unique_patients, PERIOD1_START, PERIOD1_END, "Period 1")
    period1_exp = period1_exp.rename(columns={
        'inpatient_pay': 'inpatient_pay_period1',
        'outpatient_pay': 'outpatient_pay_period1',
        'drug_pay': 'drug_pay_period1',
        'total_pay': 'total_pay_period1'
    })
    
    # Extract expenditures for Period 2 (June 2022 - May 2023)
    print(f"\n  Step 3: Extracting Period 2 expenditures (June 2022 - May 2023)...")
    period2_exp = extract_patient_expenditures(unique_patients, PERIOD2_START, PERIOD2_END, "Period 2")
    period2_exp = period2_exp.rename(columns={
        'inpatient_pay': 'inpatient_pay_period2',
        'outpatient_pay': 'outpatient_pay_period2',
        'drug_pay': 'drug_pay_period2',
        'total_pay': 'total_pay_period2'
    })
    
    # Merge with original data
    print(f"\n  Step 4: Merging expenditures with prescription data...")
    conn = setup_duckdb_connection()
    conn.register('events_df', df_eligible)
    conn.register('period1_exp', period1_exp)
    conn.register('period2_exp', period2_exp)
    
    merge_query = """
    SELECT 
        e.*,
        COALESCE(p1.inpatient_pay_period1, 0) as inpatient_pay_period1,
        COALESCE(p1.outpatient_pay_period1, 0) as outpatient_pay_period1,
        COALESCE(p1.drug_pay_period1, 0) as drug_pay_period1,
        COALESCE(p1.total_pay_period1, 0) as total_pay_period1,
        COALESCE(p2.inpatient_pay_period2, 0) as inpatient_pay_period2,
        COALESCE(p2.outpatient_pay_period2, 0) as outpatient_pay_period2,
        COALESCE(p2.drug_pay_period2, 0) as drug_pay_period2,
        COALESCE(p2.total_pay_period2, 0) as total_pay_period2
    FROM events_df e
    LEFT JOIN period1_exp p1 ON e.ENROLID = p1.ENROLID
    LEFT JOIN period2_exp p2 ON e.ENROLID = p2.ENROLID
    """
    
    merged_df = conn.execute(merge_query).fetchdf()
    conn.close()
    
    # Statistics
    print(f"\n  Results:")
    print(f"    Total events (eligible patients only): {len(merged_df):,}")
    print(f"\n    Period 1 (June 2021 - May 2022) - EXCLUDING SEMAGLUTIDE:")
    print(f"      Total expenditure: ${merged_df['total_pay_period1'].sum():,.2f}")
    print(f"      Inpatient: ${merged_df['inpatient_pay_period1'].sum():,.2f}")
    print(f"      Outpatient: ${merged_df['outpatient_pay_period1'].sum():,.2f}")
    print(f"      Drug (non-semaglutide): ${merged_df['drug_pay_period1'].sum():,.2f}")
    
    print(f"\n    Period 2 (June 2022 - May 2023) - EXCLUDING SEMAGLUTIDE:")
    print(f"      Total expenditure: ${merged_df['total_pay_period2'].sum():,.2f}")
    print(f"      Inpatient: ${merged_df['inpatient_pay_period2'].sum():,.2f}")
    print(f"      Outpatient: ${merged_df['outpatient_pay_period2'].sum():,.2f}")
    print(f"      Drug (non-semaglutide): ${merged_df['drug_pay_period2'].sum():,.2f}")
    
    # Save
    output_file = f"prescription_events_{year}_with_expenditure_by_period_no_sema_eligible_only.parquet"
    print(f"\n  Step 5: Saving to {output_file}...")
    merged_df.to_parquet(output_file, compression='snappy')
    
    output_size_mb = Path(output_file).stat().st_size / (1024*1024)
    print(f"  Saved: {output_size_mb:.1f} MB")
    
    # Cleanup
    del df, df_eligible, period1_exp, period2_exp, merged_df
    import gc
    gc.collect()
    
    return output_file

def main():
    """
    Main function: Add expenditures by period to aggregated diagnosis files
    """
    print("=" * 70)
    print("ADD EXPENDITURES BY PERIOD TO DIAGNOSIS FILES")
    print("EXCLUDING SEMAGLUTIDE DRUG COSTS")
    print("SEMAGLUTIDE ELIGIBLE PATIENTS ONLY")
    print("=" * 70)
    print(f"\nPeriod 1: {PERIOD1_START} to {PERIOD1_END}")
    print(f"Period 2: {PERIOD2_START} to {PERIOD2_END}")
    print(f"\nExpenditures aggregated per patient (not per date)")
    print(f"Semaglutide NDC codes excluded: {len(SEMAGLUTIDE_NDCS)} codes")
    print(f"Filtering to: d_diagnosis_eligible_diab == 1 OR d_diagnosis_eligible_obes == 1")
    
    # Find the 3 aggregated files
    years = [2021, 2022, 2023]
    input_files = {}
    
    for year in years:
        pattern = f"prescription_events_{year}_with_ndcnum_with_diagnosis.parquet"
        
        if Path(pattern).exists():
            input_files[year] = pattern
        else:
            print(f"\nWARNING: Could not find aggregated file for {year}")
            print(f"  Looking for: {pattern}")
    
    if not input_files:
        print(f"\nERROR: No aggregated diagnosis files found!")
        return
    
    print(f"\nFound {len(input_files)} aggregated files:")
    for year, filepath in sorted(input_files.items()):
        file_size = Path(filepath).stat().st_size / (1024*1024)
        print(f"  {year}: {filepath} ({file_size:.1f} MB)")
    
    print("=" * 70)
    
    # Process each file
    total_start_time = time.time()
    processed_files = []
    
    for year in sorted(input_files.keys()):
        file_start = time.time()
        result = process_file(input_files[year], year)
        if result:
            processed_files.append(result)
        file_time = time.time() - file_start
        print(f"  Processing time: {file_time/60:.1f} minutes")
    
    total_time = time.time() - total_start_time
    
    # Final summary
    print(f"\n{'='*70}")
    print("PROCESSING COMPLETE")
    print(f"{'='*70}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Files processed: {len(processed_files)}/{len(input_files)}")
    
    if processed_files:
        print(f"\nOutput files created:")
        for file in processed_files:
            file_size = Path(file).stat().st_size / (1024*1024)
            print(f"  {file} ({file_size:.1f} MB)")
        
        print(f"\nEach file contains 8 new expenditure columns (EXCLUDING SEMAGLUTIDE, ELIGIBLE PATIENTS ONLY):")
        print(f"  Period 1 (June 2021 - May 2022):")
        print(f"    - inpatient_pay_period1")
        print(f"    - outpatient_pay_period1")
        print(f"    - drug_pay_period1 (excludes semaglutide)")
        print(f"    - total_pay_period1 (excludes semaglutide)")
        print(f"  Period 2 (June 2022 - May 2023):")
        print(f"    - inpatient_pay_period2")
        print(f"    - outpatient_pay_period2")
        print(f"    - drug_pay_period2 (excludes semaglutide)")
        print(f"    - total_pay_period2 (excludes semaglutide)")
        print(f"\n  NOTE: Only includes patients with d_diagnosis_eligible_diab == 1 OR d_diagnosis_eligible_obes == 1")

if __name__ == "__main__":
    main()
