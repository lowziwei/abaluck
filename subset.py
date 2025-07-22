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

print("Searching for ANY Semaglutide NDC match across all years...")

years = range(2014, 2025)
all_semaglutide_data = []

for year in years:
    print(f"Processing {year}...")
    
    try:
        # This finds rows where NDCNUM matches ANY one NDC in the list
        year_data = conn.execute(f"""
            SELECT * 
            FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_{year}.parquet'
            WHERE NDCNUM IN ('{ndc_list}')
        """).df()
        
        if len(year_data) > 0:
            print(f"  ✓ Found {len(year_data):,} records in {year}")
            all_semaglutide_data.append(year_data)
        else:
            print(f"  No records in {year}")
            
    except Exception as e:
        print(f"  Error with {year}: {e}")

# Combine and display results
if all_semaglutide_data:
    combined_data = pd.concat(all_semaglutide_data, ignore_index=True)
    
    print(f"\nTOTAL: {len(combined_data):,} Semaglutide prescriptions found")
    print(f"Years with data: {sorted(combined_data['YEAR'].unique())}")
    print(f"Unique NDCs found: {len(combined_data['NDCNUM'].unique())}")
    
    print(f"\nFirst 10 records:")
    print(combined_data.head(10))
    
else:
    print("No Semaglutide records found.")
