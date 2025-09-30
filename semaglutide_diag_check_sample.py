import duckdb
import pandas as pd
import numpy as np
from datetime import datetime

def sample_and_analyze_diagnosis_codes():
    """Sample 10% of 2021 semaglutide patients and track specific diagnosis codes"""
    
    print("="*80)
    print("DIAGNOSIS CODE FREQUENCY ANALYSIS")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Configuration
    semaglutide_ndcs = [
        '00169413001', '00169413013', '00169413211', '00169413212',
        '00169413290', '00169413297', '00169413602', '00169413611',
        '00169418103', '00169418113', '00169418190', '00169418197',
        '00169477211', '00169477212', '00169477290', '00169477297',
        '50090594900', '50090513800', '50090513900', '50090605100'
    ]
    
    diabetes_codes = [
        # ICD-9 codes
        '25000', '25002', '25010', '25012', '25020', '25022', '25030', '25032',
        '25040', '25042', '25050', '25052', '25060', '25062', '25070', '25072',
        '25080', '25082', '25090', '25092',
        # ICD-10 codes  
        'E1100', 'E1101', 'E1110', 'E1111', 'E1121', 'E1122', 'E1129',
        'E1131', 'E1132', 'E1133', 'E1134', 'E1135', 'E1136', 'E1137', 'E1138', 'E1139',
        'E1140', 'E1141', 'E1142', 'E1143', 'E1144', 'E1149',
        'E1151', 'E1152', 'E1159', 'E1161', 'E1162', 'E1163', 'E1164', 'E1165', 'E1169',
        'E118', 'E119'
    ]
    
    obesity_codes = [
        # ICD-9 codes
        '278', '27800', '27801', '27802', '27803',
        # ICD-10 codes
        'E660', 'E6601', 'E6609', 'E661', 'E662', 'E663', 'E668', 'E669',
        'Z6825', 'Z6826', 'Z6827', 'Z6828', 'Z6829',
        'Z6830', 'Z6831', 'Z6832', 'Z6833', 'Z6834',
        'Z6835', 'Z6836', 'Z6837', 'Z6838', 'Z6839',
        'Z684', 'Z6841', 'Z6842', 'Z6843', 'Z6844', 'Z6845'
    ]
    
    all_codes = diabetes_codes + obesity_codes
    code_type_map = {code: 'diabetes' for code in diabetes_codes}
    code_type_map.update({code: 'obesity' for code in obesity_codes})
    
    conn = duckdb.connect()
    
    try:
        # Step 1: Get all 2021 semaglutide patients (after June 1)
        print("\nStep 1: Getting 2021 semaglutide patients (after June 1, 2021)...")
        ndc_list = "', '".join(semaglutide_ndcs)
        
        all_patients = conn.execute(f"""
            SELECT DISTINCT ENROLID
            FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_2021.parquet'
            WHERE NDCNUM IN ('{ndc_list}')
            AND SVCDATE >= '2021-06-01'
        """).df()
        
        print(f"  Total unique patients: {len(all_patients):,}")
        
        if len(all_patients) == 0:
            print("  No patients found!")
            return
        
        # Step 2: Randomly sample 10%
        sample_size = int(len(all_patients) * 0.10)
        sampled_patients = all_patients.sample(n=sample_size, random_state=42)
        sampled_patient_ids = sampled_patients['ENROLID'].tolist()
        
        print(f"\nStep 2: Randomly sampled 10% of patients")
        print(f"  Sample size: {len(sampled_patient_ids):,} patients")
        
        # Step 3: Track code frequencies
        print(f"\nStep 3: Analyzing diagnostic history for sampled patients...")
        print(f"  Checking {len(all_codes)} diagnosis codes...")
        
        # Dictionary to store code frequencies
        code_frequencies = {code: 0 for code in all_codes}
        patients_with_code = {code: set() for code in all_codes}
        
        # Process in chunks for memory efficiency
        chunk_size = 100
        patient_chunks = [sampled_patient_ids[i:i + chunk_size] 
                         for i in range(0, len(sampled_patient_ids), chunk_size)]
        
        print(f"  Processing {len(patient_chunks)} chunks of up to {chunk_size} patients each")
        
        for chunk_idx, patient_chunk in enumerate(patient_chunks):
            print(f"    Chunk {chunk_idx + 1}/{len(patient_chunks)}...", end='')
            
            patient_list = ', '.join(map(str, patient_chunk))
            
            # Query all diagnosis columns for these patients
            for code in all_codes:
                try:
                    result = conn.execute(f"""
                        SELECT ENROLID, 
                               SUM(CASE WHEN PDX = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX1 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX2 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX3 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX4 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX5 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX6 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX7 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX8 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX9 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX10 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX11 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX12 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX13 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX14 = '{code}' THEN 1 ELSE 0 END +
                                   CASE WHEN DX15 = '{code}' THEN 1 ELSE 0 END) as code_count
                        FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_I.parquet'
                        WHERE ENROLID IN ({patient_list})
                        GROUP BY ENROLID
                        HAVING code_count > 0
                    """).df()
                    
                    if len(result) > 0:
                        total_occurrences = result['code_count'].sum()
                        code_frequencies[code] += total_occurrences
                        patients_with_code[code].update(result['ENROLID'].tolist())
                    
                    del result
                    
                except Exception as e:
                    pass  # Skip errors
            
            print(" done")
        
        # Step 4: Create summary dataframe
        print(f"\nStep 4: Creating summary...")
        
        summary_data = []
        for code in all_codes:
            summary_data.append({
                'diagnosis_code': code,
                'code_type': code_type_map[code],
                'total_occurrences': code_frequencies[code],
                'patients_with_code': len(patients_with_code[code]),
                'percent_of_sample': (len(patients_with_code[code]) / len(sampled_patient_ids)) * 100
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df = summary_df.sort_values('total_occurrences', ascending=False)
        
        # Step 5: Display and save results
        print("\n" + "="*80)
        print("RESULTS SUMMARY")
        print("="*80)
        print(f"Sample size: {len(sampled_patient_ids):,} patients (10% of 2021 post-June semaglutide users)")
        print(f"\nTop 20 Most Common Diagnosis Codes:")
        print(summary_df.head(20).to_string(index=False))
        
        print(f"\n\nDIABETES CODE SUMMARY:")
        diabetes_summary = summary_df[summary_df['code_type'] == 'diabetes']
        print(f"  Total diabetes codes found: {len(diabetes_summary[diabetes_summary['total_occurrences'] > 0])}")
        print(f"  Total diabetes occurrences: {diabetes_summary['total_occurrences'].sum():,}")
        print(f"  Patients with any diabetes code: {len(set.union(*[patients_with_code[c] for c in diabetes_codes if len(patients_with_code[c]) > 0], set())):,}")
        
        print(f"\nOBESITY CODE SUMMARY:")
        obesity_summary = summary_df[summary_df['code_type'] == 'obesity']
        print(f"  Total obesity codes found: {len(obesity_summary[obesity_summary['total_occurrences'] > 0])}")
        print(f"  Total obesity occurrences: {obesity_summary['total_occurrences'].sum():,}")
        print(f"  Patients with any obesity code: {len(set.union(*[patients_with_code[c] for c in obesity_codes if len(patients_with_code[c]) > 0], set())):,}")
        
        # Save results
        output_file = '/data/MarketScan_data/diagnosis_code_frequency_2021_sample.csv'
        summary_df.to_csv(output_file, index=False)
        print(f"\nFull results saved to: {output_file}")
        
        # Also save a detailed breakdown
        detailed_file = '/data/MarketScan_data/diagnosis_code_frequency_2021_sample_nonzero.csv'
        summary_df[summary_df['total_occurrences'] > 0].to_csv(detailed_file, index=False)
        print(f"Non-zero codes saved to: {detailed_file}")
        
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()

if __name__ == "__main__":
    sample_and_analyze_diagnosis_codes()
