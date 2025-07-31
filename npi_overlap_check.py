import duckdb
import pandas as pd
import time
import gc
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
import tempfile
import os

def get_memory_usage():
    """Get current memory usage in GB using system info"""
    try:
        with open('/proc/meminfo', 'r') as f:
            meminfo = f.read()
        
        for line in meminfo.split('\n'):
            if 'MemAvailable:' in line:
                available_kb = int(line.split()[1])
                available_gb = available_kb / 1024 / 1024
                total_gb = 30  # Assume 30GB total
                used_gb = total_gb - available_gb
                return used_gb
        return 5.0
    except:
        return 5.0

# Configuration
START_YEAR = 2018
DATASET_TYPE = "COMMERCIAL_SET_A"
DATABASE = "CCAE"

def analyze_npi_provid_with_temp_files(year):
    """
    Memory-safe analysis using temporary files to avoid RAM overflow
    Save intermediate results to disk instead of keeping in memory
    """
    print(f"\n{'='*80}")
    print(f"MEMORY-SAFE NPI vs PROVID ANALYSIS - YEAR {year}")
    print(f"Using temporary files to manage memory usage")
    print(f"{'='*80}")

    # File paths
    o_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_O_{year}.parquet"
    s_file = f"/data/MarketScan_data/{DATASET_TYPE}/{DATABASE}_S.parquet"

    # Verify files exist
    for file_type, file_path in [('Outpatient', o_file), ('Inpatient', s_file)]:
        if not Path(file_path).exists():
            print(f"ERROR: Missing {file_type} file: {file_path}")
            return None

    # Create temporary directory for intermediate results
    temp_dir = tempfile.mkdtemp(prefix='npi_analysis_')
    print(f"Using temporary directory: {temp_dir}")

    # Initialize DuckDB
    conn = duckdb.connect()
    conn.execute("SET memory_limit='15GB'")  # More conservative
    conn.execute("SET threads=2")
    
    total_start_time = time.time()
    temp_files = []  # Track temporary files for cleanup

    try:
        total_prescriptions_processed = 0
        
        # Process each flag file one at a time, saving results to temp files
        for file_idx in range(1, 13):  # 12 files
            file_path = f"prescription_flags_2018_{DATABASE}_D_part{file_idx:02d}.parquet"
            
            if not Path(file_path).exists():
                print(f"File {file_idx}/12: {file_path} not found, skipping...")
                continue
                
            print(f"\nProcessing File {file_idx}/12: {file_path}")
            file_start_time = time.time()
            
            # Load truly new prescriptions from this file
            try:
                df = pd.read_parquet(file_path, columns=['ENROLID', 'SVCDATE', 'FLAG'])
            except:
                df = pd.read_parquet(file_path)
                df = df[['ENROLID', 'SVCDATE', 'FLAG']].copy()
            
            # Filter for truly new prescriptions (FLAG is NULL)
            truly_new = df[df['FLAG'].isna()].copy()
            
            if len(truly_new) == 0:
                print(f"  No truly new prescriptions in this file")
                del df, truly_new
                gc.collect()
                continue
            
            print(f"  Found {len(truly_new):,} truly new prescriptions")
            
            # Get unique patients for this file
            file_patients = truly_new['ENROLID'].unique()
            file_patient_list = "', '".join(map(str, file_patients))
            
            print(f"  Analyzing healthcare visits for {len(file_patients):,} patients...")
            
            # MEMORY-SAFE: Process the analysis and immediately save to temp file
            visit_analysis_query = f"""
            WITH all_visits AS (
                -- Outpatient visits
                SELECT 
                    ENROLID, SVCDATE, NPI, PROVID, 'Outpatient' as visit_type,
                    CASE 
                        WHEN NPI IS NOT NULL AND UPPER(TRIM(CAST(NPI as VARCHAR))) != 'NULL' AND TRIM(CAST(NPI as VARCHAR)) != ''
                        THEN 'HAS_NPI' 
                        ELSE 'MISSING_NPI' 
                    END as npi_status,
                    CASE 
                        WHEN PROVID IS NOT NULL AND UPPER(TRIM(CAST(PROVID as VARCHAR))) != 'NULL' AND TRIM(CAST(PROVID as VARCHAR)) != ''
                        THEN 'HAS_PROVID' 
                        ELSE 'MISSING_PROVID' 
                    END as provid_status
                FROM '{o_file}'
                WHERE ENROLID IN ('{file_patient_list}')
                  AND ENROLID IS NOT NULL 
                  AND SVCDATE IS NOT NULL
                
                UNION ALL
                
                -- Inpatient visits  
                SELECT 
                    ENROLID, SVCDATE, NPI, PROVID, 'Inpatient' as visit_type,
                    CASE 
                        WHEN NPI IS NOT NULL AND UPPER(TRIM(CAST(NPI as VARCHAR))) != 'NULL' AND TRIM(CAST(NPI as VARCHAR)) != ''
                        THEN 'HAS_NPI' 
                        ELSE 'MISSING_NPI' 
                    END as npi_status,
                    CASE 
                        WHEN PROVID IS NOT NULL AND UPPER(TRIM(CAST(PROVID as VARCHAR))) != 'NULL' AND TRIM(CAST(PROVID as VARCHAR)) != ''
                        THEN 'HAS_PROVID' 
                        ELSE 'MISSING_PROVID' 
                    END as provid_status
                FROM '{s_file}'
                WHERE ENROLID IN ('{file_patient_list}')
                  AND ENROLID IS NOT NULL 
                  AND SVCDATE IS NOT NULL
                  AND YEAR = {year}
            )
            SELECT 
                visit_type,
                npi_status,
                provid_status,
                -- Combined status - key question: can PROVID fill NPI gaps?
                CASE 
                    WHEN npi_status = 'HAS_NPI' AND provid_status = 'HAS_PROVID' THEN 'BOTH_AVAILABLE'
                    WHEN npi_status = 'HAS_NPI' AND provid_status = 'MISSING_PROVID' THEN 'ONLY_NPI'
                    WHEN npi_status = 'MISSING_NPI' AND provid_status = 'HAS_PROVID' THEN 'ONLY_PROVID'
                    ELSE 'NEITHER_AVAILABLE'
                END as provider_availability,
                COUNT(*) as visit_count
            FROM all_visits
            GROUP BY visit_type, npi_status, provid_status
            """
            
            visit_results = conn.execute(visit_analysis_query).fetchdf()
            
            if len(visit_results) > 0:
                # SAVE TO TEMPORARY FILE instead of keeping in memory
                temp_file = os.path.join(temp_dir, f'file_{file_idx:02d}_results.parquet')
                visit_results.to_parquet(temp_file)
                temp_files.append(temp_file)
                
                print(f"    Healthcare visits analyzed: {visit_results['visit_count'].sum():,}")
                print(f"    Results saved to: {temp_file}")
            
            total_prescriptions_processed += len(truly_new)
            
            # AGGRESSIVE CLEANUP
            del df, truly_new, visit_results, file_patients
            gc.collect()
            
            file_time = time.time() - file_start_time
            current_memory = get_memory_usage()
            print(f"    File {file_idx} completed in {file_time:.1f}s, Memory: {current_memory:.1f} GB")
            
            # Safety check
            if current_memory > 20:
                print(f"    ⚠️  High memory usage detected")
        
        # MEMORY-SAFE: Load and combine temp files one at a time
        print(f"\nCombining results from {len(temp_files)} temporary files...")
        combine_start = time.time()
        
        if not temp_files:
            print("No results to combine!")
            return None
        
        # Initialize aggregation dictionary
        final_aggregation = {}
        
        for i, temp_file in enumerate(temp_files):
            print(f"  Loading temp file {i+1}/{len(temp_files)}: {os.path.basename(temp_file)}")
            
            # Load one temp file at a time
            temp_data = pd.read_parquet(temp_file)
            
            # Aggregate into final results
            for _, row in temp_data.iterrows():
                key = (row['visit_type'], row['npi_status'], row['provid_status'], row['provider_availability'])
                if key in final_aggregation:
                    final_aggregation[key] += row['visit_count']
                else:
                    final_aggregation[key] = row['visit_count']
            
            # Clean up temp data immediately
            del temp_data
            gc.collect()
        
        # Convert aggregation back to DataFrame
        final_results = pd.DataFrame([
            {
                'visit_type': key[0],
                'npi_status': key[1], 
                'provid_status': key[2],
                'provider_availability': key[3],
                'visit_count': count
            }
            for key, count in final_aggregation.items()
        ])
        
        combine_time = time.time() - combine_start
        print(f"  Results combined in {combine_time:.1f} seconds")
        print(f"  Final memory usage: {get_memory_usage():.1f} GB")
        
        # ANALYSIS - Answer the key questions
        print(f"\n{'='*80}")
        print(f"ANALYSIS RESULTS")
        print(f"{'='*80}")
        
        print(f"Total prescriptions analyzed: {total_prescriptions_processed:,}")
        print(f"Total healthcare visits analyzed: {final_results['visit_count'].sum():,}")
        
        # Separate outpatient and inpatient results
        outpatient_data = final_results[final_results['visit_type'] == 'Outpatient']
        inpatient_data = final_results[final_results['visit_type'] == 'Inpatient']
        
        for visit_type, data in [('OUTPATIENT', outpatient_data), ('INPATIENT', inpatient_data)]:
            if len(data) == 0:
                continue
                
            total_visits = data['visit_count'].sum()
            print(f"\n{visit_type} VISITS ({total_visits:,} total):")
            
            # Group by provider availability
            availability_summary = data.groupby('provider_availability')['visit_count'].sum().sort_values(ascending=False)
            
            for status, count in availability_summary.items():
                pct = (count / total_visits) * 100
                print(f"  {status}: {count:,} visits ({pct:.1f}%)")
            
            # Key metrics
            both_available = availability_summary.get('BOTH_AVAILABLE', 0)
            only_npi = availability_summary.get('ONLY_NPI', 0)  
            only_provid = availability_summary.get('ONLY_PROVID', 0)
            neither = availability_summary.get('NEITHER_AVAILABLE', 0)
            
            npi_missing = only_provid + neither  # Visits where NPI is missing
            provid_missing = only_npi + neither  # Visits where PROVID is missing
            
            print(f"\n  KEY METRICS:")
            print(f"  - NPI missing in {npi_missing:,} visits ({npi_missing/total_visits*100:.1f}%)")
            print(f"  - PROVID missing in {provid_missing:,} visits ({provid_missing/total_visits*100:.1f}%)")
            print(f"  - PROVID available when NPI missing: {only_provid:,} visits ({only_provid/npi_missing*100 if npi_missing > 0 else 0:.1f}% of NPI-missing visits)")
        
        # OVERALL ASSESSMENT
        print(f"\n{'='*60}")
        print(f"OVERALL ASSESSMENT")
        print(f"{'='*60}")
        
        # Combine outpatient and inpatient for overall stats
        overall_summary = final_results.groupby('provider_availability')['visit_count'].sum()
        total_all_visits = overall_summary.sum()
        
        both_available = overall_summary.get('BOTH_AVAILABLE', 0)
        only_npi = overall_summary.get('ONLY_NPI', 0)
        only_provid = overall_summary.get('ONLY_PROVID', 0)
        neither = overall_summary.get('NEITHER_AVAILABLE', 0)
        
        npi_missing_total = only_provid + neither
        provid_fills_gap = only_provid
        
        print(f"Across ALL healthcare visits for truly new prescription patients:")
        print(f"  - Total visits: {total_all_visits:,}")
        print(f"  - NPI missing: {npi_missing_total:,} visits ({npi_missing_total/total_all_visits*100:.1f}%)")
        print(f"  - PROVID available when NPI missing: {provid_fills_gap:,} visits")
        
        if npi_missing_total > 0:
            provid_fill_rate = (provid_fills_gap / npi_missing_total) * 100
            print(f"  - PROVID fill rate: {provid_fill_rate:.1f}% of NPI-missing visits have PROVID")
            
            if provid_fill_rate > 50:
                print(f"\n✅ RECOMMENDATION: Use PROVID as complement to NPI")
                print(f"   - PROVID can fill significant gaps when NPI is missing")
                print(f"   - {provid_fills_gap:,} additional visits can be provider-matched")
            elif provid_fill_rate > 10:
                print(f"\n🟡 MODERATE VALUE: PROVID provides some complement to NPI")
                print(f"   - PROVID fills {provid_fill_rate:.1f}% of NPI gaps")
                print(f"   - Consider if {provid_fills_gap:,} additional matches justify implementation")
            else:
                print(f"\n🔴 LIMITED VALUE: PROVID provides minimal complement to NPI")
                print(f"   - Only {provid_fill_rate:.1f}% of NPI-missing visits have PROVID")
        
        # Create simple visualization
        create_simple_visualization(final_results, year)
        
        total_time = time.time() - total_start_time
        
        print(f"\n{'='*80}")
        print(f"ANALYSIS COMPLETE - {total_time/60:.1f} minutes")
        print(f"Peak memory usage: {get_memory_usage():.1f} GB")
        print(f"{'='*80}")
        
        return {
            'results': final_results,
            'total_prescriptions': total_prescriptions_processed,
            'total_time': total_time
        }

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        # CLEANUP: Remove all temporary files
        print(f"\nCleaning up temporary files...")
        for temp_file in temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    print(f"  Removed: {os.path.basename(temp_file)}")
            except:
                print(f"  Failed to remove: {os.path.basename(temp_file)}")
        
        try:
            os.rmdir(temp_dir)
            print(f"  Removed temporary directory: {temp_dir}")
        except:
            print(f"  Failed to remove temporary directory: {temp_dir}")
        
        try:
            conn.close()
        except:
            pass
        gc.collect()

def create_simple_visualization(results_df, year):
    """Create simple visualization showing NPI vs PROVID availability"""
    print("\nCreating visualization...")
    
    plt.style.use('default')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: Overall Provider Availability
    overall_summary = results_df.groupby('provider_availability')['visit_count'].sum()
    total_visits = overall_summary.sum()
    
    # Calculate percentages
    percentages = (overall_summary / total_visits * 100).round(1)
    
    # Create pie chart
    colors = ['lightgreen', 'skyblue', 'orange', 'lightcoral']
    wedges, texts, autotexts = ax1.pie(overall_summary.values, 
                                      labels=overall_summary.index, 
                                      colors=colors,
                                      autopct='%1.1f%%',
                                      startangle=90)
    ax1.set_title('Provider ID Availability in Healthcare Visits\n(For Truly New Prescription Patients)')
    
    # Plot 2: Outpatient vs Inpatient Comparison
    outpatient_data = results_df[results_df['visit_type'] == 'Outpatient'].groupby('provider_availability')['visit_count'].sum()
    inpatient_data = results_df[results_df['visit_type'] == 'Inpatient'].groupby('provider_availability')['visit_count'].sum()
    
    categories = ['BOTH_AVAILABLE', 'ONLY_NPI', 'ONLY_PROVID', 'NEITHER_AVAILABLE']
    
    # Convert to percentages
    outpatient_total = outpatient_data.sum()
    inpatient_total = inpatient_data.sum()
    
    outpatient_pcts = [(outpatient_data.get(cat, 0) / outpatient_total * 100) for cat in categories]
    inpatient_pcts = [(inpatient_data.get(cat, 0) / inpatient_total * 100) for cat in categories]
    
    x = np.arange(len(categories))
    width = 0.35
    
    bars1 = ax2.bar(x - width/2, outpatient_pcts, width, label='Outpatient', alpha=0.8, color='skyblue')
    bars2 = ax2.bar(x + width/2, inpatient_pcts, width, label='Inpatient', alpha=0.8, color='lightcoral')
    
    ax2.set_xlabel('Provider ID Availability')
    ax2.set_ylabel('Percentage of Visits')
    ax2.set_title('Provider ID Availability: Outpatient vs Inpatient')
    ax2.set_xticks(x)
    ax2.set_xticklabels([cat.replace('_', '\n') for cat in categories])
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Add percentage labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0.5:  # Only label if bar is tall enough
                ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5, 
                        f'{height:.1f}%', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    
    # Save the plot
    filename = f'npi_provid_simple_analysis_{year}.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Visualization saved as: {filename}")
    plt.show()

def main():
    print("MarketScan Analysis - MEMORY-SAFE NPI vs PROVID ANALYSIS")
    print("=" * 80)
    print("MEMORY MANAGEMENT:")
    print("- Process one flag file at a time")
    print("- Save intermediate results to temporary files") 
    print("- Combine results at the end")
    print("- Aggressive memory cleanup after each file")
    print("=" * 80)
    print("ANALYSIS QUESTIONS:")
    print("1. How often is NPI missing in healthcare visits?")
    print("2. How often is PROVID missing in healthcare visits?") 
    print("3. Can PROVID be used as complement/replacement when NPI is missing?")
    print("=" * 80)
    
    start_time = time.time()
    result = analyze_npi_provid_with_temp_files(2018)
    total_runtime = time.time() - start_time
    
    if result:
        print(f"\n🎉 Memory-safe NPI vs PROVID analysis completed!")
        print(f"Runtime: {total_runtime/60:.1f} minutes")
        print(f"Prescriptions analyzed: {result['total_prescriptions']:,}")
        
        # Quick summary
        results_df = result['results']
        overall_summary = results_df.groupby('provider_availability')['visit_count'].sum()
        
        only_provid = overall_summary.get('ONLY_PROVID', 0)
        neither = overall_summary.get('NEITHER_AVAILABLE', 0)
        npi_missing = only_provid + neither
        
        if npi_missing > 0:
            fill_rate = (only_provid / npi_missing) * 100
            print(f"\n📊 BOTTOM LINE:")
            print(f"   - PROVID can fill {fill_rate:.1f}% of NPI gaps")
            print(f"   - That's {only_provid:,} additional provider-identifiable visits")
            
            if fill_rate > 50:
                print(f"   - ✅ STRONG case for using PROVID as NPI complement")
            elif fill_rate > 10:
                print(f"   - 🟡 MODERATE case for using PROVID as NPI complement")
            else:
                print(f"   - 🔴 WEAK case for using PROVID as NPI complement")
        
    else:
        print("\n❌ Analysis failed")

if __name__ == "__main__":
    main()
