import duckdb

conn = duckdb.connect()

for year in range(2014, 2025):
   try:
       conn.execute(f"""
           CREATE OR REPLACE TABLE merged_{year} AS
           SELECT d.*, o.* EXCLUDE (SEQNUM, ENROLID, SVCDATE)
           FROM '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_D_{year}.parquet' d
           INNER JOIN '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_O_{year}.parquet' o 
               ON d.SEQNUM = o.SEQNUM 
               AND d.ENROLID = o.ENROLID 
               AND d.SVCDATE = o.SVCDATE
       """)
       
       conn.execute(f"""
           COPY merged_{year} TO '/data/MarketScan_data/COMMERCIAL_SET_A/CCAE_MERGED_{year}.parquet'
       """)
       
       print(f"Merged {year}")
       
   except Exception as e:
       print(f"Error {year}: {e}")
