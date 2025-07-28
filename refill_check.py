import duckdb
import pandas as pd

def create_2018_prescription_flags(dataset_type="COMMERCIAL_SET_A", database="CCAE", table_code="D"):
    """
    Process 2018 data and check against previous years
    
    Steps:
    1. Load 2018 file with only necessary columns
    2. Filter for REFILL = 0
    3. Create FLAG column by checking previous years for same ENROLID + NDCNUM
    """
    
    conn = duckdb.connect()
    
    # File paths
    data_path = f"/data/MarketScan_data/{dataset_type}"
    file_2018 = f"{data_path}/{database}_{table_code}_2018.parquet"
    
    # Check which previous year files exist
    previous_years = []
    for year in [2017, 2016, 2015, 2014]:
        file_path = f"{data_path}/{database}_{table_code}_{year}.parquet"
        try:
            # Test if file exists and is readable
            conn.execute(f"SELECT COUNT(*) FROM '{file_path}' LIMIT 1")
            previous_years.append(year)
            print(f"Found data for year {year}")
        except:
            print(f"No data found for year {year}")
    
    if not previous_years:
        print("No previous year data found!")
        return pd.DataFrame()
    
    print(f"Will check against years: {previous_years}")
    
    # Step 1: Load 2018 data with only necessary columns and filter for REFILL = 0
    print("Loading 2018 REFILL=0 prescriptions...")
    
    refill_2018_query = f"""
    SELECT ENROLID, NDCNUM, REFILL, YEAR, SVCDATE
    FROM '{file_2018}'
    WHERE REFILL = 0
    """
    
    df_2018_refill0 = conn.execute(refill_2018_query).df()
    print(f"Found {len(df_2018_refill0):,} prescriptions with REFILL=0 in 2018")
    
    if len(df_2018_refill0) == 0:
        print("No REFILL=0 prescriptions found in 2018")
        return pd.DataFrame()
    
    # Step 2: Create the FLAG column by checking previous years
    print("Checking previous years for same ENROLID + NDCNUM combinations...")
    
    # Build the SQL query to check previous years AND same year
    case_conditions = []
    
    # First check within same year (2018) - only flag if REFILL=0 is NOT the first chronological instance
    case_conditions.append(f"""
        WHEN EXISTS (
            SELECT 1 FROM '{file_2018}' same_year
            WHERE same_year.ENROLID = r.ENROLID 
            AND same_year.NDCNUM = r.NDCNUM
            AND same_year.SVCDATE < r.SVCDATE
        ) THEN 0
    """)
    
    # Then check previous years
    for i, year in enumerate(sorted(previous_years, reverse=True)):  # Start with most recent year
        years_back = 2018 - year
        file_path = f"{data_path}/{database}_{table_code}_{year}.parquet"
        
        case_conditions.append(f"""
            WHEN EXISTS (
                SELECT 1 FROM '{file_path}' p{year}
                WHERE p{year}.ENROLID = r.ENROLID 
                AND p{year}.NDCNUM = r.NDCNUM
            ) THEN -{years_back}
        """)
    
    # Create the full query with FLAG column
    flag_query = f"""
    WITH refill_zero_2018 AS (
        SELECT ENROLID, NDCNUM, REFILL, YEAR, SVCDATE
        FROM '{file_2018}'
        WHERE REFILL = 0
    )
    SELECT r.*,
           CASE 
               {''.join(case_conditions)}
               ELSE NULL
           END as FLAG
    FROM refill_zero_2018 r
    """
    
    print("Executing flag assignment query...")
    result_df = conn.execute(flag_query).df()
    
    # Show results
    flagged_count = result_df['FLAG'].notna().sum()
    coding_error_count = (result_df['FLAG'] == 0).sum()
    previous_year_count = flagged_count - coding_error_count
    
    print(f"\nResults:")
    print(f"Total REFILL=0 prescriptions in 2018: {len(result_df):,}")
    print(f"Potential coding errors (FLAG=0, REFILL=0 but NOT the first chronological instance): {coding_error_count:,}")
    print(f"Prescriptions with previous year patterns: {previous_year_count:,}")
    print(f"Total flagged: {flagged_count:,}")
    print(f"Percentage with any pattern: {flagged_count/len(result_df)*100:.1f}%")
    
    if flagged_count > 0:
        print(f"\nFlag distribution:")
        flag_counts = result_df['FLAG'].value_counts().sort_index()
        for flag, count in flag_counts.items():
            if flag == 0:
                print(f"  FLAG  0 (not first chronological instance): {count:6,} prescriptions ({count/flagged_count*100:.1f}%)")
            else:
                years_back = abs(int(flag))
                check_year = 2018 - years_back
                print(f"  FLAG {int(flag):2d} (found in {check_year}): {count:6,} prescriptions ({count/flagged_count*100:.1f}%)")
    
    # Show sample
    print(f"\nSample of results:")
    print(result_df[['ENROLID', 'NDCNUM', 'YEAR', 'REFILL', 'FLAG']].head(10))
    
    return result_df

def quick_analysis_2018():
    """
    Quick analysis of 2018 prescription flags
    """
    
    DATASET_TYPE = "COMMERCIAL_SET_A"
    DATABASE = "CCAE" 
    TABLE_CODE = "D"
    
    print("="*60)
    print("2018 PRESCRIPTION FLAG ANALYSIS")
    print("="*60)
    print(f"Dataset: {DATASET_TYPE}")
    print(f"Database: {DATABASE}")
    print(f"Table: {TABLE_CODE}")
    print()
    
    try:
        # Run the analysis
        results = create_2018_prescription_flags(DATASET_TYPE, DATABASE, TABLE_CODE)
        
        if len(results) > 0:
            # Save results
            output_file = f"prescription_flags_2018_{DATABASE}_{TABLE_CODE}.parquet"
            results.to_parquet(output_file)
            print(f"\nResults saved to: {output_file}")
            
            # Additional analysis
            print(f"\nAdditional Analysis:")
            print(f"Unique patients with flagged prescriptions: {results[results['FLAG'].notna()]['ENROLID'].nunique():,}")
            print(f"Unique drugs with flagged prescriptions: {results[results['FLAG'].notna()]['NDCNUM'].nunique():,}")
            
            # Show coding errors specifically
            coding_errors = results[results['FLAG'] == 0]
            if len(coding_errors) > 0:
                print(f"\nCoding Error Analysis (FLAG=0):")
                print(f"Patients with potential coding errors: {coding_errors['ENROLID'].nunique():,}")
                print(f"Drugs with potential coding errors: {coding_errors['NDCNUM'].nunique():,}")
            
            # Show most common drug patterns (excluding coding errors)
            previous_patterns = results[results['FLAG'] < 0]
            if len(previous_patterns) > 0:
                print(f"\nTop 10 drugs with most previous year pattern matches:")
                drug_patterns = previous_patterns.groupby('NDCNUM').size().sort_values(ascending=False).head(10)
                for ndcnum, count in drug_patterns.items():
                    print(f"  {ndcnum}: {count:,} pattern matches")
        
        return results
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    results = quick_analysis_2018()
