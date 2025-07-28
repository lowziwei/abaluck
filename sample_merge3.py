#Uses Impatient Services S file

print(f"\nStep 2: Processing {total_patients:,} patients...")
step2_start = time.time()

# Create a temporary table for patient IDs instead of string concatenation
conn.execute("CREATE TEMP TABLE test_patients AS SELECT unnest($1) as ENROLID", [test_enrolids])
conn.execute("CREATE INDEX idx_test_patients ON test_patients(ENROLID)")

# Step 2a: Get prescription data
prescription_query = f"""
    SELECT p.ENROLID, p.SVCDATE
    FROM '{d_file}' p
    INNER JOIN test_patients tp ON p.ENROLID = tp.ENROLID
    WHERE p.ENROLID IS NOT NULL 
      AND p.SVCDATE IS NOT NULL
"""
prescriptions_df = conn.execute(prescription_query).fetchdf()
print(f"  Found {len(prescriptions_df):,} prescription events")

# Remove duplicates in pandas (faster than SQL for this)
prescriptions_df = prescriptions_df.drop_duplicates(['ENROLID', 'SVCDATE'])
print(f"  Unique prescription events: {len(prescriptions_df):,}")

# Step 2b: Create temp table for prescriptions
conn.register('prescriptions_temp', prescriptions_df)

# Step 2c: Get outpatient data (separate query)
outpatient_query = f"""
    SELECT o.ENROLID, o.SVCDATE, o.NPI, 'Outpatient' as service_type
    FROM '{o_file}' o
    INNER JOIN test_patients tp ON o.ENROLID = tp.ENROLID
    WHERE o.ENROLID IS NOT NULL 
      AND o.SVCDATE IS NOT NULL
"""
outpatient_df = conn.execute(outpatient_query).fetchdf()
print(f"  Found {len(outpatient_df):,} outpatient visits")

# Step 2d: Get inpatient data (separate query)
inpatient_query = f"""
    SELECT s.ENROLID, s.SVCDATE, s.NPI, 'Inpatient' as service_type
    FROM '{s_file}' s
    INNER JOIN test_patients tp ON s.ENROLID = tp.ENROLID
    WHERE s.ENROLID IS NOT NULL 
      AND s.SVCDATE IS NOT NULL
      AND s.YEAR = {year}
"""
inpatient_df = conn.execute(inpatient_query).fetchdf()
print(f"  Found {len(inpatient_df):,} inpatient visits")

# Step 2e: Combine services and register
all_services_df = pd.concat([outpatient_df, inpatient_df], ignore_index=True)
conn.register('all_services_temp', all_services_df)
print(f"  Total service visits: {len(all_services_df):,}")

# Step 2f: Final matching query (much simpler now)
final_query = """
    SELECT 
        p.ENROLID,
        p.SVCDATE as prescription_date,
        COUNT(DISTINCT CASE WHEN s.NPI IS NOT NULL THEN s.NPI END) as unique_npi_count,
        COUNT(*) as total_visits,
        COUNT(CASE WHEN s.NPI IS NULL THEN 1 END) as null_npi_visits,
        COUNT(CASE WHEN s.service_type = 'Outpatient' THEN 1 END) as outpatient_visits,
        COUNT(CASE WHEN s.service_type = 'Inpatient' THEN 1 END) as inpatient_visits,
        COUNT(DISTINCT CASE WHEN s.NPI IS NOT NULL AND s.service_type = 'Outpatient' THEN s.NPI END) as unique_outpatient_npis,
        COUNT(DISTINCT CASE WHEN s.NPI IS NOT NULL AND s.service_type = 'Inpatient' THEN s.NPI END) as unique_inpatient_npis
    FROM prescriptions_temp p
    LEFT JOIN all_services_temp s 
        ON p.ENROLID = s.ENROLID 
        AND s.SVCDATE BETWEEN DATE_SUB(p.SVCDATE, INTERVAL 3 DAY) 
                          AND DATE_ADD(p.SVCDATE, INTERVAL 3 DAY)
    GROUP BY p.ENROLID, p.SVCDATE
    ORDER BY p.ENROLID, p.SVCDATE
"""

final_df = conn.execute(final_query).fetchdf()
step2_time = time.time() - step2_start
