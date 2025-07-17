import duckdb

conn = duckdb.connect()

# First, let's see what we're working with
print("Getting file info...")
result = conn.execute("""
    SELECT COUNT(*) as total_rows 
    FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_2014.parquet'
""").fetchone()

print(f"Total rows in file: {result[0]:,}")

# Get column info
columns = conn.execute("""
    DESCRIBE SELECT * FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_2014.parquet' LIMIT 1
""").df()
print("Columns available:")
print(columns)

# Get a 1% sample (should be manageable)
print("Loading 1% sample...")
df_sample = conn.execute("""
    SELECT * FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_2014.parquet' 
    USING SAMPLE 1 PERCENT
""").df()

print(f"Sample loaded: {len(df_sample):,} rows")
print(df_sample.head())

# Get just 10 rows with all columns, displayed nicely
df_small = conn.execute("""
    SELECT * FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_2014.parquet' 
    LIMIT 10
""").df()

# Display all columns without truncation
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

print("First 10 rows with all columns:")
print(df_small)

# Also get a summary of the data types and non-null counts
print("\n" + "="*50)
print("DATA SUMMARY:")
print("="*50)
print(df_small.info())

print("\n" + "="*50)
print("SAMPLE VALUES FOR KEY COLUMNS:")
print("="*50)
print("Unique years:", sorted(df_small['YEAR'].dropna().unique()))
print("Unique sexes:", df_small['SEX'].dropna().unique())
print("Date range:", df_small['SVCDATE'].min(), "to", df_small['SVCDATE'].max())
print("Cost columns (PAY, NETPAY) sample:")
print(df_small[['PAY', 'NETPAY', 'COPAY', 'DEDUCT']].describe())
