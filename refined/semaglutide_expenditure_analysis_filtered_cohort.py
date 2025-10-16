import pandas as pd
import duckdb
import os
import time
from pathlib import Path

# Configuration
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"

# Four time periods
PERIOD_M1_START = '2019-06-01'
PERIOD_M1_END = '2020-05-31'
PERIOD_0_START = '2020-06-01'
PERIOD_0_END = '2021-05-31'
PERIOD_1_START = '2021-06-01'
PERIOD_1_END = '2022-05-31'
PERIOD_2_START = '2022-06-01'
PERIOD_2_END = '2023-05-31'

# Semaglutide NDC codes
SEMAGLUTIDE_NDCS = {
    '00169413001', '00169413013', '00169413211', '00169413212',
    '00169413290', '00169413297', '00169413602', '00169413611', 
    '00169418103', '00169418113', '00169418190', '00169418197', 
    '00169477211', '00169477212', '00169477290', '00169477297',
    '50090594900', '50090513800', '50090513900', '50090605100',
    '00169430301', '00169430313', '00169430330', '00169430390', 
    '00169430393', '00169430399', '00169430701', '00169430713', 
    '00169430730', '00169431401', '00169431413', '00169431430', 
    '00169480430', '00169480930', '00169481530', '00169481590',
    '00169450101', '00169450114', '00169450501', '00169450514',
    '00169451701', '00169451714', '00169452401', '00169452414', 
    '00169452501', '00169452514', '00169452590', '00169452594',
    '50090582400'
}

def setup_duckdb_connection(memory_limit='8GB'):
    conn = duckdb.connect(':memory:')
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.execute("SET max_temp_directory_size='50GB'")
    conn.execute("SET temp_directory='/tmp'")
    conn.execute("SET threads=8")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute("SET enable_progress_bar=false")
    return conn

def get_patient_demographics():
    print("  Loading patient demographics...")
    data_path = f"/data/MarketScan_data/{DATASET_TYPE}"
    conn = setup_duckdb_connection()
    enroll_dfs = []
    for year in range(2019, 2024):
        enroll_file = f"{data_path}/{DATABASE}_T_{year}.parquet"
        if Path(enroll_file).exists():
            print(f"    Reading {DATABASE}_T_{year}.parquet...")
            # Calculate AGE from DOBYR (date of birth year)
            query = f"""
            SELECT DISTINCT
                ENROLID,
                ({year} - DOBYR) as AGE,
                SEX
            FROM '{enroll_file}'
            WHERE DOBYR IS NOT NULL
            """
            enroll_dfs.append(conn.execute(query).fetchdf())
    if not enroll_dfs:
        print("    WARNING: No enrollment files found!")
        return pd.DataFrame(columns=['ENROLID', 'AGE', 'SEX'])
    demographics = pd.concat(enroll_dfs, ignore_index=True)
    # Take most recent age for each patient
    demographics = demographics.sort_values('AGE', ascending=False).drop_duplicates('ENROLID', keep='first')
    print(f"    Loaded demographics for {len(demographics):,} patients")
    conn.close()
    return demographics

def identify_first_semaglutide_prescription():
    print("  Identifying first semaglutide prescriptions...")
    data_path = f"/data/MarketScan_data/{DATASET_TYPE}"
    conn = setup_duckdb_connection()
    sema_df = pd.DataFrame({'NDCNUM': list(SEMAGLUTIDE_NDCS)})
    conn.register('semaglutide_ndcs', sema_df)
    sema_prescriptions = []
    for year in range(2019, 2024):
        drug_file = f"{data_path}/{DATABASE}_D_{year}.parquet"
        if Path(drug_file).exists():
            print(f"    Reading {DATABASE}_D_{year}.parquet...")
            query = f"""
            SELECT ENROLID, SVCDATE, PROVID as phys_ids
            FROM '{drug_file}'
            WHERE NDCNUM IN (SELECT NDCNUM FROM semaglutide_ndcs)
            AND SVCDATE >= DATE '{PERIOD_M1_START}' AND SVCDATE <= DATE '{PERIOD_2_END}'
            """
            sema_prescriptions.append(conn.execute(query).fetchdf())
    if not sema_prescriptions:
        print("    WARNING: No semaglutide prescriptions found!")
        return pd.DataFrame(columns=['ENROLID', 'first_sema_date', 'first_sema_physician', 'first_sema_period'])
    all_sema = pd.concat(sema_prescriptions, ignore_index=True)
    print(f"    Total semaglutide prescriptions: {len(all_sema):,}")
    all_sema['SVCDATE'] = pd.to_datetime(all_sema['SVCDATE'])
    first_sema = all_sema.sort_values('SVCDATE').groupby('ENROLID').first().reset_index()
    first_sema = first_sema.rename(columns={'SVCDATE': 'first_sema_date', 'phys_ids': 'first_sema_physician'})
    def assign_period(date):
        if pd.isna(date):
            return None
        date_str = date.strftime('%Y-%m-%d')
        if PERIOD_M1_START <= date_str <= PERIOD_M1_END:
            return 'Period -1'
        elif PERIOD_0_START <= date_str <= PERIOD_0_END:
            return 'Period 0'
        elif PERIOD_1_START <= date_str <= PERIOD_1_END:
            return 'Period 1'
        elif PERIOD_2_START <= date_str <= PERIOD_2_END:
            return 'Period 2'
        else:
            return 'Outside periods'
    first_sema['first_sema_period'] = first_sema['first_sema_date'].apply(assign_period)
    print(f"    Patients with first semaglutide prescription: {len(first_sema):,}")
    print("\n    First prescriptions by period:")
    print(first_sema['first_sema_period'].value_counts().sort_index())
    conn.close()
    return first_sema

def extract_patient_expenditures(patient_ids, period_start, period_end, period_name):
    print(f"    Extracting expenditures for {len(patient_ids):,} patients")
    print(f"    Period: {period_start} to {period_end}")
    conn = setup_duckdb_connection()
    try:
        patients_df = pd.DataFrame({'ENROLID': list(patient_ids)})
        conn.register('target_patients', patients_df)
        sema_df = pd.DataFrame({'NDCNUM': list(SEMAGLUTIDE_NDCS)})
        conn.register('semaglutide_ndcs', sema_df)
        data_path = f"/data/MarketScan_data/{DATASET_TYPE}"
        period_start_dt = pd.to_datetime(period_start)
        period_end_dt = pd.to_datetime(period_end)
        years_to_query = list(range(period_start_dt.year, period_end_dt.year + 1))
        
        inpatient_dfs = []
        inpatient_file = f"{data_path}/{DATABASE}_I.parquet"
        if Path(inpatient_file).exists():
            inpatient_query = f"""
            SELECT ENROLID, SUM(COALESCE(TOTNET, 0)) as inpatient_pay
            FROM '{inpatient_file}'
            WHERE ENROLID IN (SELECT ENROLID FROM target_patients)
            AND ADMDATE >= DATE '{period_start}' AND ADMDATE <= DATE '{period_end}'
            GROUP BY ENROLID
            """
            inpatient_dfs.append(conn.execute(inpatient_query).fetchdf())
        inpatient_df = pd.concat(inpatient_dfs, ignore_index=True).groupby('ENROLID', as_index=False)['inpatient_pay'].sum() if inpatient_dfs else pd.DataFrame(columns=['ENROLID', 'inpatient_pay'])
        
        outpatient_dfs = []
        for year in years_to_query:
            outpatient_file = f"{data_path}/{DATABASE}_O_{year}.parquet"
            if Path(outpatient_file).exists():
                outpatient_query = f"""
                SELECT ENROLID, SUM(COALESCE(NETPAY, 0)) as outpatient_pay
                FROM '{outpatient_file}'
                WHERE ENROLID IN (SELECT ENROLID FROM target_patients)
                AND SVCDATE >= DATE '{period_start}' AND SVCDATE <= DATE '{period_end}'
                GROUP BY ENROLID
                """
                outpatient_dfs.append(conn.execute(outpatient_query).fetchdf())
        outpatient_df = pd.concat(outpatient_dfs, ignore_index=True).groupby('ENROLID', as_index=False)['outpatient_pay'].sum() if outpatient_dfs else pd.DataFrame(columns=['ENROLID', 'outpatient_pay'])
        
        drug_dfs = []
        for year in years_to_query:
            drug_file = f"{data_path}/{DATABASE}_D_{year}.parquet"
            if Path(drug_file).exists():
                drug_query = f"""
                SELECT ENROLID, SUM(COALESCE(NETPAY, 0)) as drug_pay
                FROM '{drug_file}'
                WHERE ENROLID IN (SELECT ENROLID FROM target_patients)
                AND SVCDATE >= DATE '{period_start}' AND SVCDATE <= DATE '{period_end}'
                AND NDCNUM NOT IN (SELECT NDCNUM FROM semaglutide_ndcs)
                GROUP BY ENROLID
                """
                drug_dfs.append(conn.execute(drug_query).fetchdf())
        drug_df = pd.concat(drug_dfs, ignore_index=True).groupby('ENROLID', as_index=False)['drug_pay'].sum() if drug_dfs else pd.DataFrame(columns=['ENROLID', 'drug_pay'])
        
        sema_drug_dfs = []
        for year in years_to_query:
            drug_file = f"{data_path}/{DATABASE}_D_{year}.parquet"
            if Path(drug_file).exists():
                sema_drug_query = f"""
                SELECT ENROLID, SUM(COALESCE(NETPAY, 0)) as semaglutide_pay
                FROM '{drug_file}'
                WHERE ENROLID IN (SELECT ENROLID FROM target_patients)
                AND SVCDATE >= DATE '{period_start}' AND SVCDATE <= DATE '{period_end}'
                AND NDCNUM IN (SELECT NDCNUM FROM semaglutide_ndcs)
                GROUP BY ENROLID
                """
                sema_drug_dfs.append(conn.execute(sema_drug_query).fetchdf())
        sema_drug_df = pd.concat(sema_drug_dfs, ignore_index=True).groupby('ENROLID', as_index=False)['semaglutide_pay'].sum() if sema_drug_dfs else pd.DataFrame(columns=['ENROLID', 'semaglutide_pay'])
        
        conn.register('inpatient_exp', inpatient_df)
        conn.register('outpatient_exp', outpatient_df)
        conn.register('drug_exp', drug_df)
        conn.register('sema_drug_exp', sema_drug_df)
        
        merge_query = """
        SELECT tp.ENROLID,
            COALESCE(i.inpatient_pay, 0) as inpatient_pay,
            COALESCE(o.outpatient_pay, 0) as outpatient_pay,
            COALESCE(d.drug_pay, 0) as drug_pay,
            COALESCE(s.semaglutide_pay, 0) as semaglutide_pay,
            COALESCE(i.inpatient_pay, 0) + COALESCE(o.outpatient_pay, 0) + COALESCE(d.drug_pay, 0) as total_pay_no_sema,
            COALESCE(i.inpatient_pay, 0) + COALESCE(o.outpatient_pay, 0) + COALESCE(d.drug_pay, 0) + COALESCE(s.semaglutide_pay, 0) as total_pay_with_sema
        FROM target_patients tp
        LEFT JOIN inpatient_exp i ON tp.ENROLID = i.ENROLID
        LEFT JOIN outpatient_exp o ON tp.ENROLID = o.ENROLID
        LEFT JOIN drug_exp d ON tp.ENROLID = d.ENROLID
        LEFT JOIN sema_drug_exp s ON tp.ENROLID = s.ENROLID
        """
        result_df = conn.execute(merge_query).fetchdf()
        conn.close()
        print(f"      Total expenditure (no sema): ${result_df['total_pay_no_sema'].sum():,.2f}")
        print(f"      Semaglutide expenditure: ${result_df['semaglutide_pay'].sum():,.2f}")
        print(f"      Total expenditure (with sema): ${result_df['total_pay_with_sema'].sum():,.2f}")
        return result_df
    except Exception as e:
        print(f"      Error: {e}")
        if conn:
            conn.close()
        result_df = patients_df.copy()
        for col in ['inpatient_pay', 'outpatient_pay', 'drug_pay', 'semaglutide_pay', 'total_pay_no_sema', 'total_pay_with_sema']:
            result_df[col] = 0.0
        return result_df

def main():
    print("=" * 70)
    print("MULTI-PERIOD SEMAGLUTIDE ANALYSIS")
    print("=" * 70)
    print(f"\nPeriod -1: {PERIOD_M1_START} to {PERIOD_M1_END}")
    print(f"Period 0:  {PERIOD_0_START} to {PERIOD_0_END}")
    print(f"Period 1:  {PERIOD_1_START} to {PERIOD_1_END}")
    print(f"Period 2:  {PERIOD_2_START} to {PERIOD_2_END}")
    
    print("\n" + "="*70)
    print("STEP 1: Load prescription event files")
    print("="*70)
    years = [2019, 2020, 2021, 2022, 2023]
    event_dfs = []
    for year in years:
        pattern = f"prescription_events_{year}_with_ndcnum_with_diagnosis.parquet"
        if Path(pattern).exists():
            print(f"  Loading {pattern}...")
            df = pd.read_parquet(pattern)
            event_dfs.append(df)
            print(f"    Rows: {len(df):,}")
    if not event_dfs:
        print("ERROR: No prescription event files found!")
        return
    all_events = pd.concat(event_dfs, ignore_index=True)
    print(f"\n  Total prescription events: {len(all_events):,}")
    print(f"  Unique patients: {all_events['ENROLID'].nunique():,}")
    print(f"  Unique physicians: {all_events['phys_ids'].nunique():,}")
    
    print("\n" + "="*70)
    print("STEP 2: Load patient demographics")
    print("="*70)
    demographics = get_patient_demographics()
    
    print("\n" + "="*70)
    print("STEP 3: Identify first semaglutide prescriptions")
    print("="*70)
    first_sema = identify_first_semaglutide_prescription()
    
    print("\n" + "="*70)
    print("STEP 4: Filter to 18+ and obese prior to June 2021")
    print("="*70)
    all_events = all_events.merge(demographics[['ENROLID', 'AGE', 'SEX']], on='ENROLID', how='left')
    patients_18plus = all_events[all_events['AGE'] >= 18]['ENROLID'].unique()
    print(f"  Patients 18+: {len(patients_18plus):,}")
    obese_before_june2021 = all_events[
        (all_events['ENROLID'].isin(patients_18plus)) &
        (all_events['d_diagnosis_eligible_obes'] == 1) &
        (pd.to_datetime(all_events['SVCDATE']) < '2021-06-01')
    ]['ENROLID'].unique()
    print(f"  Patients obese before June 2021: {len(obese_before_june2021):,}")
    filtered_events = all_events[all_events['ENROLID'].isin(obese_before_june2021)].copy()
    print(f"  Events after filtering: {len(filtered_events):,}")
    print(f"  Unique patients remaining: {filtered_events['ENROLID'].nunique():,}")
    print(f"  Unique physicians remaining: {filtered_events['phys_ids'].nunique():,}")
    
    print("\n" + "="*70)
    print("STEP 5: Physician patient counts")
    print("="*70)
    physician_patient_counts = filtered_events.groupby('phys_ids')['ENROLID'].nunique().reset_index()
    physician_patient_counts.columns = ['phys_ids', 'patient_count']
    physicians_10plus = physician_patient_counts[physician_patient_counts['patient_count'] >= 10]
    print(f"  Total physicians: {len(physician_patient_counts):,}")
    print(f"  Physicians with ≥10 patients: {len(physicians_10plus):,}")
    print(f"  % with ≥10 patients: {len(physicians_10plus)/len(physician_patient_counts)*100:.1f}%")
    
    print("\n" + "="*70)
    print("STEP 6: Merge first semaglutide prescription info")
    print("="*70)
    filtered_events = filtered_events.merge(
        first_sema[['ENROLID', 'first_sema_date', 'first_sema_physician', 'first_sema_period']], 
        on='ENROLID', how='left'
    )
    print("\n  First semaglutide prescriptions by period (filtered cohort):")
    first_sema_filtered = filtered_events[['ENROLID', 'first_sema_period']].drop_duplicates()
    period_counts = first_sema_filtered['first_sema_period'].value_counts().sort_index()
    print(period_counts)
    print("\n  Unique physicians associated with first prescriptions by period:")
    for period in ['Period -1', 'Period 0', 'Period 1', 'Period 2']:
        patients_in_period = first_sema_filtered[first_sema_filtered['first_sema_period'] == period]['ENROLID']
        if len(patients_in_period) > 0:
            phys_in_period = filtered_events[
                (filtered_events['ENROLID'].isin(patients_in_period)) &
                (filtered_events['phys_ids'].notna())
            ]['phys_ids'].nunique()
            print(f"    {period}: {phys_in_period:,} physicians")
    
    print("\n" + "="*70)
    print("STEP 7: Extract expenditures for all periods")
    print("="*70)
    unique_patients = set(filtered_events['ENROLID'].unique())
    
    print(f"\n  Period -1: {PERIOD_M1_START} to {PERIOD_M1_END}")
    period_m1_exp = extract_patient_expenditures(unique_patients, PERIOD_M1_START, PERIOD_M1_END, "Period -1")
    period_m1_exp = period_m1_exp.rename(columns={
        'inpatient_pay': 'inpatient_pay_period_m1', 'outpatient_pay': 'outpatient_pay_period_m1',
        'drug_pay': 'drug_pay_period_m1', 'semaglutide_pay': 'semaglutide_pay_period_m1',
        'total_pay_no_sema': 'total_pay_no_sema_period_m1', 'total_pay_with_sema': 'total_pay_with_sema_period_m1'
    })
    
    print(f"\n  Period 0: {PERIOD_0_START} to {PERIOD_0_END}")
    period_0_exp = extract_patient_expenditures(unique_patients, PERIOD_0_START, PERIOD_0_END, "Period 0")
    period_0_exp = period_0_exp.rename(columns={
        'inpatient_pay': 'inpatient_pay_period_0', 'outpatient_pay': 'outpatient_pay_period_0',
        'drug_pay': 'drug_pay_period_0', 'semaglutide_pay': 'semaglutide_pay_period_0',
        'total_pay_no_sema': 'total_pay_no_sema_period_0', 'total_pay_with_sema': 'total_pay_with_sema_period_0'
    })
    
    print(f"\n  Period 1: {PERIOD_1_START} to {PERIOD_1_END}")
    period_1_exp = extract_patient_expenditures(unique_patients, PERIOD_1_START, PERIOD_1_END, "Period 1")
    period_1_exp = period_1_exp.rename(columns={
        'inpatient_pay': 'inpatient_pay_period_1', 'outpatient_pay': 'outpatient_pay_period_1',
        'drug_pay': 'drug_pay_period_1', 'semaglutide_pay': 'semaglutide_pay_period_1',
        'total_pay_no_sema': 'total_pay_no_sema_period_1', 'total_pay_with_sema': 'total_pay_with_sema_period_1'
    })
    
    print(f"\n  Period 2: {PERIOD_2_START} to {PERIOD_2_END}")
    period_2_exp = extract_patient_expenditures(unique_patients, PERIOD_2_START, PERIOD_2_END, "Period 2")
    period_2_exp = period_2_exp.rename(columns={
        'inpatient_pay': 'inpatient_pay_period_2', 'outpatient_pay': 'outpatient_pay_period_2',
        'drug_pay': 'drug_pay_period_2', 'semaglutide_pay': 'semaglutide_pay_period_2',
        'total_pay_no_sema': 'total_pay_no_sema_period_2', 'total_pay_with_sema': 'total_pay_with_sema_period_2'
    })
    
    print("\n" + "="*70)
    print("STEP 8: Merge all data")
    print("="*70)
    conn = setup_duckdb_connection()
    conn.register('events', filtered_events)
    conn.register('exp_m1', period_m1_exp)
    conn.register('exp_0', period_0_exp)
    conn.register('exp_1', period_1_exp)
    conn.register('exp_2', period_2_exp)
    
    final_query = """
    SELECT e.*, 
        COALESCE(em1.inpatient_pay_period_m1, 0) as inpatient_pay_period_m1,
        COALESCE(em1.outpatient_pay_period_m1, 0) as outpatient_pay_period_m1,
        COALESCE(em1.drug_pay_period_m1, 0) as drug_pay_period_m1,
        COALESCE(em1.semaglutide_pay_period_m1, 0) as semaglutide_pay_period_m1,
        COALESCE(em1.total_pay_no_sema_period_m1, 0) as total_pay_no_sema_period_m1,
        COALESCE(em1.total_pay_with_sema_period_m1, 0) as total_pay_with_sema_period_m1,
        COALESCE(e0.inpatient_pay_period_0, 0) as inpatient_pay_period_0,
        COALESCE(e0.outpatient_pay_period_0, 0) as outpatient_pay_period_0,
        COALESCE(e0.drug_pay_period_0, 0) as drug_pay_period_0,
        COALESCE(e0.semaglutide_pay_period_0, 0) as semaglutide_pay_period_0,
        COALESCE(e0.total_pay_no_sema_period_0, 0) as total_pay_no_sema_period_0,
        COALESCE(e0.total_pay_with_sema_period_0, 0) as total_pay_with_sema_period_0,
        COALESCE(e1.inpatient_pay_period_1, 0) as inpatient_pay_period_1,
        COALESCE(e1.outpatient_pay_period_1, 0) as outpatient_pay_period_1,
        COALESCE(e1.drug_pay_period_1, 0) as drug_pay_period_1,
        COALESCE(e1.semaglutide_pay_period_1, 0) as semaglutide_pay_period_1,
        COALESCE(e1.total_pay_no_sema_period_1, 0) as total_pay_no_sema_period_1,
        COALESCE(e1.total_pay_with_sema_period_1, 0) as total_pay_with_sema_period_1,
        COALESCE(e2.inpatient_pay_period_2, 0) as inpatient_pay_period_2,
        COALESCE(e2.outpatient_pay_period_2, 0) as outpatient_pay_period_2,
        COALESCE(e2.drug_pay_period_2, 0) as drug_pay_period_2,
        COALESCE(e2.semaglutide_pay_period_2, 0) as semaglutide_pay_period_2,
        COALESCE(e2.total_pay_no_sema_period_2, 0) as total_pay_no_sema_period_2,
        COALESCE(e2.total_pay_with_sema_period_2, 0) as total_pay_with_sema_period_2
    FROM events e
    LEFT JOIN exp_m1 em1 ON e.ENROLID = em1.ENROLID
    LEFT JOIN exp_0 e0 ON e.ENROLID = e0.ENROLID
    LEFT JOIN exp_1 e1 ON e.ENROLID = e1.ENROLID
    LEFT JOIN exp_2 e2 ON e.ENROLID = e2.ENROLID
    """
    
    final_df = conn.execute(final_query).fetchdf()
    conn.close()
    print(f"  Final dataset rows: {len(final_df):,}")
    print(f"  Final dataset columns: {len(final_df.columns)}")
    
    print("\n" + "="*70)
    print("STEP 9: Save results")
    print("="*70)
    output_file = "prescription_events_filtered_18plus_obese_4periods_expenditure.parquet"
    final_df.to_parquet(output_file, compression='snappy')
    output_size_mb = Path(output_file).stat().st_size / (1024*1024)
    print(f"  Saved: {output_file} ({output_size_mb:.1f} MB)")
    
    summary_file = "analysis_summary_filtered_cohort.csv"
    summary_data = {
        'metric': [
            'Total patients', 'Total physicians', 'Physicians with >=10 patients',
            'First sema Period -1', 'First sema Period 0', 'First sema Period 1', 'First sema Period 2',
            'Total expenditure (no sema) Period -1', 'Total expenditure (no sema) Period 0',
            'Total expenditure (no sema) Period 1', 'Total expenditure (no sema) Period 2',
            'Semaglutide expenditure Period -1', 'Semaglutide expenditure Period 0',
            'Semaglutide expenditure Period 1', 'Semaglutide expenditure Period 2',
            'Total expenditure (with sema) Period -1', 'Total expenditure (with sema) Period 0',
            'Total expenditure (with sema) Period 1', 'Total expenditure (with sema) Period 2'
        ],
        'value': [
            final_df['ENROLID'].nunique(), final_df['phys_ids'].nunique(), len(physicians_10plus),
            (first_sema_filtered['first_sema_period'] == 'Period -1').sum(),
            (first_sema_filtered['first_sema_period'] == 'Period 0').sum(),
            (first_sema_filtered['first_sema_period'] == 'Period 1').sum(),
            (first_sema_filtered['first_sema_period'] == 'Period 2').sum(),
            final_df['total_pay_no_sema_period_m1'].sum(), final_df['total_pay_no_sema_period_0'].sum(),
            final_df['total_pay_no_sema_period_1'].sum(), final_df['total_pay_no_sema_period_2'].sum(),
            final_df['semaglutide_pay_period_m1'].sum(), final_df['semaglutide_pay_period_0'].sum(),
            final_df['semaglutide_pay_period_1'].sum(), final_df['semaglutide_pay_period_2'].sum(),
            final_df['total_pay_with_sema_period_m1'].sum(), final_df['total_pay_with_sema_period_0'].sum(),
            final_df['total_pay_with_sema_period_1'].sum(), final_df['total_pay_with_sema_period_2'].sum()
        ]
    }
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(summary_file, index=False)
    print(f"  Saved: {summary_file}")
    
    print("\n" + "="*70)
    print("ANALYSIS COMPLETE!")
    print("="*70)

if __name__ == "__main__":
    main()
