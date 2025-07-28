import duckdb
import pandas as pd
from pathlib import Path

def create_prescription_flags_duckdb(dataset_type="COMMERCIAL_SET_A", database="CCAE", 
                                   table_code="D", start_year=2018, end_year=2024):
    """
    Create FLAG column for prescriptions with REFILL = 0, indicating how many years back
    the same ENROLID + NDCNUM combination was found.
    
    Uses DuckDB for efficient processing of large parquet files.
    
    Parameters:
    dataset_type: str, dataset identifier
    database: str, database identifier  
    table_code: str, table code (D for prescriptions)
    start_year: int, starting year for analysis (default 2018)
    end_year: int, ending year for analysis (default 2024)
    
    Returns:
    pandas DataFrame with FLAG column added
    """
    
    conn = duckdb.connect()
    
    # Build list of available files
    data_path = Path(f"/data/MarketScan_data/{dataset_type}")
    available_files = []
    
    for year in range(2014, end_year + 1):  # Check from 2014 to have lookback data
        file_path = data_path / f"{database}_{table_code}_{year}.parquet"
        if file_path.exists():
            available_files.append((year, str(file_path)))
            print(f"Found file for year {year}")
    
    if not available_files:
        raise FileNotFoundError("No prescription files found")
    
    # Create a temporary table with all years of data - only necessary columns
    print("Loading prescription data (ENROLID, NDCNUM, REFILL, YEAR only)...")
    
    # First, create the combined dataset with only necessary columns
    union_queries = []
    for year, file_path in available_files:
        union_queries.append(f"SELECT ENROLID, NDCNUM, REFILL, YEAR FROM '{file_path}'")
    
    combined_query = " UNION ALL ".join(union_queries)
    
    # Create temporary view
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW all_prescriptions AS (
            {combined_query}
        )
    """)
    
    print("Data loaded. Creating flags...")
    
    # Get the flagged results using SQL - using exact column names from your data
    flag_query = f"""
    WITH refill_zero AS (
        -- Get all REFILL = 0 prescriptions from {start_year} onwards
        SELECT *
        FROM all_prescriptions 
        WHERE REFILL = 0 AND YEAR >= {start_year}
    ),
    
    enrol_ndc_years AS (
        -- Create lookup of all ENROLID + NDCNUM combinations with their years
        SELECT ENROLID, NDCNUM, ARRAY_AGG(DISTINCT YEAR ORDER BY YEAR) as available_years
        FROM all_prescriptions
        GROUP BY ENROLID, NDCNUM
    ),
    
    flagged_prescriptions AS (
        SELECT r.*,
               CASE 
                   -- Check if previous year exists (t-1)
                   WHEN list_contains(e.available_years, r.YEAR - 1) THEN -1
                   -- Check if 2 years back exists (t-2) 
                   WHEN list_contains(e.available_years, r.YEAR - 2) THEN -2
                   -- Check if 3 years back exists (t-3)
                   WHEN list_contains(e.available_years, r.YEAR - 3) THEN -3
                   -- Check if 4 years back exists (t-4)
                   WHEN list_contains(e.available_years, r.YEAR - 4) THEN -4
                   -- Check if 5 years back exists (t-5)
                   WHEN list_contains(e.available_years, r.YEAR - 5) THEN -5
                   -- Check if 6 years back exists (t-6)
                   WHEN list_contains(e.available_years, r.YEAR - 6) THEN -6
                   -- Check if 7 years back exists (t-7)
                   WHEN list_contains(e.available_years, r.YEAR - 7) THEN -7
                   -- Check if 8 years back exists (t-8)
                   WHEN list_contains(e.available_years, r.YEAR - 8) THEN -8
                   -- Check if 9 years back exists (t-9)
                   WHEN list_contains(e.available_years, r.YEAR - 9) THEN -9
                   -- Check if 10 years back exists (t-10)
                   WHEN list_contains(e.available_years, r.YEAR - 10) THEN -10
                   ELSE NULL
               END as FLAG
        FROM refill_zero r
        LEFT JOIN enrol_ndc_years e ON r.ENROLID = e.ENROLID AND r.NDCNUM = e.NDCNUM
    )
    
    SELECT * FROM flagged_prescriptions
    WHERE FLAG IS NOT NULL  -- Only return rows that have a flag
    ORDER BY ENROLID, NDCNUM, YEAR
    """
    
    # Execute and return results
    result_df = conn.execute(flag_query).df()
    
    print(f"Analysis complete. Found {len(result_df):,} prescriptions with flags.")
    print(f"Flag distribution:")
    print(result_df['FLAG'].value_counts().sort_index())
    
    return result_df

def analyze_prescription_patterns(dataset_type="COMMERCIAL_SET_A", database="CCAE", 
                                table_code="D", start_year=2018):
    """
    Comprehensive analysis of prescription patterns with flags
    """
    
    # Get flagged prescriptions
    flagged_df = create_prescription_flags_duckdb(dataset_type, database, table_code, start_year)
    
    print("\n" + "="*60)
    print("PRESCRIPTION PATTERN ANALYSIS")
    print("="*60)
    
    # Summary statistics
    print(f"\nTotal flagged prescriptions: {len(flagged_df):,}")
    print(f"Unique patients (ENROLID): {flagged_df['ENROLID'].nunique():,}")
    print(f"Unique drugs (NDCNUM): {flagged_df['NDCNUM'].nunique():,}")
    
    # Flag distribution
    print(f"\nFlag Distribution (years back):")
    flag_counts = flagged_df['FLAG'].value_counts().sort_index()
    for flag, count in flag_counts.items():
        print(f"  {flag:2d} year(s) back: {count:6,} prescriptions ({count/len(flagged_df)*100:.1f}%)")
    
    # Year distribution
    print(f"\nPrescriptions by Year:")
    year_counts = flagged_df['YEAR'].value_counts().sort_index()
    for year, count in year_counts.items():
        print(f"  {year}: {count:6,} prescriptions")
    
    # Top drugs with patterns
    print(f"\nTop 10 Drugs with Most Pattern Matches:")
    drug_patterns = flagged_df.groupby('NDCNUM').agg({
        'FLAG': 'count',
        'ENROLID': 'nunique'
    }).rename(columns={'FLAG': 'total_patterns', 'ENROLID': 'unique_patients'})
    drug_patterns = drug_patterns.sort_values('total_patterns', ascending=False).head(10)
    
    for ndcnum, row in drug_patterns.iterrows():
        print(f"  {ndcnum}: {row['total_patterns']:,} patterns, {row['unique_patients']:,} patients")
    
    return flagged_df

# Example usage for your current setup
def run_analysis():
    """
    Run the analysis with your current configuration
    """
    
    # Your current settings
    DATASET_TYPE = "COMMERCIAL_SET_A"
    DATABASE = "CCAE" 
    TABLE_CODE = "D"
    
    print("Starting prescription pattern analysis...")
    print(f"Dataset: {DATASET_TYPE}")
    print(f"Database: {DATABASE}")
    print(f"Table: {TABLE_CODE}")
    
    try:
        # Run the analysis
        results = analyze_prescription_patterns(DATASET_TYPE, DATABASE, TABLE_CODE, 2018)
        
        # Show sample of results
        print(f"\nSample of flagged prescriptions:")
        print(results[['ENROLID', 'NDCNUM', 'YEAR', 'REFILL', 'FLAG']].head(10))
        
        # Save results if needed
        output_file = f"prescription_flags_{DATABASE}_{TABLE_CODE}_2018plus.parquet"
        results.to_parquet(output_file)
        print(f"\nResults saved to: {output_file}")
        
        return results
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        return None

# Quick test with single year
def test_single_year():
    """
    Test the logic with just 2018 data to verify it works
    """
    
    conn = duckdb.connect()
    
    DATASET_TYPE = "COMMERCIAL_SET_A"
    DATABASE = "CCAE"
    TABLE_CODE = "D"
    
    # Load 2017 and 2018 data for testing - only necessary columns
    file_2017 = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_2017.parquet"
    file_2018 = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_{TABLE_CODE}_2018.parquet"
    
    test_query = f"""
    WITH combined_data AS (
        SELECT ENROLID, NDCNUM, REFILL, YEAR FROM '{file_2017}'
        UNION ALL
        SELECT ENROLID, NDCNUM, REFILL, YEAR FROM '{file_2018}'
    ),
    
    refill_zero_2018 AS (
        SELECT * FROM combined_data 
        WHERE REFILL = 0 AND YEAR = 2018
        LIMIT 1000  -- Test with just 1000 rows
    ),
    
    flagged AS (
        SELECT r.*,
               CASE WHEN EXISTS (
                   SELECT 1 FROM combined_data c2 
                   WHERE c2.ENROLID = r.ENROLID 
                   AND c2.NDCNUM = r.NDCNUM 
                   AND c2.YEAR = 2017
               ) THEN -1 
               ELSE NULL END as FLAG
        FROM refill_zero_2018 r
    )
    
    SELECT * FROM flagged WHERE FLAG IS NOT NULL
    """
    
    test_results = conn.execute(test_query).df()
    print(f"Test results: {len(test_results)} rows with flags")
    print(test_results[['ENROLID', 'NDCNUM', 'YEAR', 'REFILL', 'FLAG']].head())
    
    return test_results

if __name__ == "__main__":
    # Run the full analysis
    results = run_analysis()
