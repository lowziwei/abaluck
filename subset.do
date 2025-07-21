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
    '0169413001', '0169413013', '0169413211', '0169413212',
    '0169413290', '0169413297', '0169413602', '0169413611', 
    '0169418103', '0169418113', '0169418190', '0169418197', 
    '0169477211', '0169477212', '0169477290', '0169477297',
    
    # Rybelsus
    '0169430301', '0169430313', '0169430330', '0169430390', 
    '0169430393', '0169430399', '0169430701', '0169430713', 
    '0169430730', '0169431401', '0169431413', '0169431430', 
    '0169480430', '0169480930', '0169481530', '0169481590',
    
    # Wegovy  
    '0169450101', '0169450114', '0169450501', '0169450514',
    '0169451701', '0169451714', '0169452401', '0169452414', 
    '0169452501', '0169452514', '0169452590', '0169452594'
]

ndc_list = "', '".join(semaglutide_ndcs)

# Get first 100 rows where NDCNUM matches Semaglutide NDCs
semaglutide_data = conn.execute(f"""
    SELECT * 
    FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_2014.parquet'
    WHERE NDCNUM IN ('{ndc_list}')
    LIMIT 100
""").df()

print(f"Found {len(semaglutide_data)} Semaglutide records")
print("\nFirst 100 Semaglutide prescription records:")
print(semaglutide_data)
