import duckdb
import pandas as pd

conn = duckdb.connect()

# Set pandas to show all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)

# Semaglutide NDC codes
semaglutide_ndcs = [
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
]

ndc_list = "', '".join(semaglutide_ndcs)

print("Finding earliest Semaglutide prescription in 2018...")

try:
    # Find earliest semaglutide prescription by SVCDATE in 2018
    earliest_data = conn.execute(f"""
        SELECT * 
        FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_2018.parquet'
        WHERE NDCNUM IN ('{ndc_list}')
        ORDER BY SVCDATE ASC
        LIMIT 1
    """).df()
    
    if len(earliest_data) > 0:
        print(f"✓ Found earliest Semaglutide prescription in 2018")
        print(f"Earliest SVCDATE: {earliest_data['SVCDATE'].iloc[0]}")
        print(f"NDC: {earliest_data['NDCNUM'].iloc[0]}")
        print(f"Patient ID: {earliest_data['ENROLID'].iloc[0]}")
        
        print(f"\nFull record details:")
        print(earliest_data.to_string())
        
        # Also get count and date range for context
        summary_data = conn.execute(f"""
            SELECT 
                COUNT(*) as total_prescriptions,
                MIN(SVCDATE) as earliest_date,
                MAX(SVCDATE) as latest_date,
                COUNT(DISTINCT ENROLID) as unique_patients,
                COUNT(DISTINCT NDCNUM) as unique_ndcs
            FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_2018.parquet'
            WHERE NDCNUM IN ('{ndc_list}')
        """).df()
        
        print(f"\n2018 Semaglutide Summary:")
        print(summary_data.to_string())
        
    else:
        print("No Semaglutide records found in 2018")
        
except Exception as e:
    print(f"Error processing 2018 file: {e}")

conn.close()
