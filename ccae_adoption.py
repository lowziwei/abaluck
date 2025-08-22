import pandas as pd
import numpy as np
import os
import json

def analyze_top100_random_groups_wide():
    """
    Split top 100 physicians (by patients) into random groups of 10 and analyze each group
    OUTPUT: Wide format (months as rows, groups as columns)
    """
    print("🎲 RANDOM GROUPS ANALYSIS - TOP 100 PHYSICIANS (WIDE FORMAT)")
    print("=" * 65)
    
    # Input files
    monthly_file = "physician_monthly_semaglutide_analysis.parquet"
    
    # Check if file exists
    if not os.path.exists(monthly_file):
        print(f"❌ File not found: {monthly_file}")
        return
    
    print(f"📁 Reading: {monthly_file}")
    df = pd.read_parquet(monthly_file)
    
    # Step 1: Get top 100 physicians by total patients
    print(f"🔄 Identifying top 100 physicians by patient count...")
    
    physician_totals = df.groupby('phys_ids').agg({
        'total_patients': 'sum',
        'total_prescriptions': 'sum'
    }).reset_index()
    
    top_100_phys_ids = physician_totals.nlargest(100, 'total_patients')['phys_ids'].tolist()
    
    print(f"✅ Identified top 100 physicians")
    print(f"   Patient range: {physician_totals.nlargest(100, 'total_patients')['total_patients'].min():,} to {physician_totals.nlargest(100, 'total_patients')['total_patients'].max():,}")
    
    # Step 2: Filter monthly data for top 100 physicians
    top_100_monthly = df[df['phys_ids'].isin(top_100_phys_ids)].copy()
    
    print(f"📊 Monthly records for top 100: {len(top_100_monthly):,}")
    
    # Step 3: Randomly split into 10 groups of 10 physicians each
    print(f"\n🎲 Randomly splitting into groups of 10...")
    
    RANDOM_SEED = 42  # For reproducibility
    np.random.seed(RANDOM_SEED)
    shuffled_phys_ids = np.random.permutation(top_100_phys_ids)
    
    # Create 10 groups of 10 physicians each
    groups = []
    for i in range(10):
        start_idx = i * 10
        end_idx = (i + 1) * 10
        group_phys_ids = shuffled_phys_ids[start_idx:end_idx].tolist()
        groups.append({
            'group_id': i + 1,
            'phys_ids': group_phys_ids
        })
    
    print(f"✅ Created 10 groups of 10 physicians each")
    
    # Save randomization token
    randomization_token = {
        'random_seed': RANDOM_SEED,
        'timestamp': pd.Timestamp.now().isoformat(),
        'total_physicians': len(top_100_phys_ids),
        'groups_created': 10,
        'original_phys_ids_order': top_100_phys_ids,
        'shuffled_order': shuffled_phys_ids.tolist(),
        'group_assignments': {}
    }
    
    # Add group assignments to token
    for group in groups:
        randomization_token['group_assignments'][f'group_{group["group_id"]}'] = group['phys_ids']
    
    # Step 4: Analyze each group - month by month
    print(f"\n📈 ANALYZING EACH GROUP BY MONTH:")
    print("=" * 60)
    
    # Get all possible months from the data
    all_months = sorted(top_100_monthly['year_month'].unique())
    print(f"📅 Date range: {all_months[0]} to {all_months[-1]} ({len(all_months)} months)")
    
    all_monthly_results = []
    group_summaries = []
    
    for group in groups:
        group_id = group['group_id']
        group_phys_ids = group['phys_ids']
        
        print(f"\n🔢 GROUP {group_id}:")
        
        # Filter data for this group
        group_monthly = top_100_monthly[top_100_monthly['phys_ids'].isin(group_phys_ids)]
        
        # Calculate metrics by month for this group
        monthly_metrics = group_monthly.groupby('year_month').agg({
            'total_prescriptions': 'sum',  # Total prescriptions for the group that month
            'fraction_patients_semaglutide': 'mean'  # Average across physicians in group
        }).reset_index()
        
        # Create complete month series with 0s for missing months
        complete_months = pd.DataFrame({'year_month': all_months})
        monthly_metrics = complete_months.merge(monthly_metrics, on='year_month', how='left')
        
        # Fill missing values with 0
        monthly_metrics['total_prescriptions'] = monthly_metrics['total_prescriptions'].fillna(0)
        monthly_metrics['fraction_patients_semaglutide'] = monthly_metrics['fraction_patients_semaglutide'].fillna(0)
        
        # Add group identifier
        monthly_metrics['group_id'] = group_id
        monthly_metrics['physician_count'] = len(group_phys_ids)
        
        # Store all monthly data for plotting
        all_monthly_results.append(monthly_metrics)
        
        # Calculate summary stats for this group (excluding 0 months for averages)
        non_zero_months = monthly_metrics[monthly_metrics['total_prescriptions'] > 0]
        avg_prescriptions = monthly_metrics['total_prescriptions'].mean()  # Include 0s in average
        avg_semaglutide_fraction = monthly_metrics['fraction_patients_semaglutide'].mean()  # Include 0s
        active_months = len(non_zero_months)
        
        print(f"   Physicians: {len(group_phys_ids)}")
        print(f"   Total months: {len(monthly_metrics)}")
        print(f"   Active months: {active_months}")
        print(f"   Avg prescriptions per month: {avg_prescriptions:,.1f} (including 0s)")
        print(f"   Avg semaglutide fraction: {avg_semaglutide_fraction:.4f} ({avg_semaglutide_fraction*100:.2f}%)")
        
        # Store summary
        group_summaries.append({
            'group_id': group_id,
            'physician_count': len(group_phys_ids),
            'total_months': len(monthly_metrics),
            'active_months': active_months,
            'avg_prescriptions_per_month': avg_prescriptions,
            'avg_semaglutide_fraction': avg_semaglutide_fraction,
            'phys_ids': group_phys_ids
        })
    
    # Combine all monthly data (LONG format)
    combined_monthly_long = pd.concat(all_monthly_results, ignore_index=True)
    combined_monthly_long = combined_monthly_long.sort_values(['group_id', 'year_month']).reset_index(drop=True)
    
    # Step 5: Create WIDE format datasets
    print(f"\n🔄 Converting to WIDE format...")
    
    # Wide format for prescriptions (months x groups)
    prescriptions_wide = combined_monthly_long.pivot(
        index='year_month', 
        columns='group_id', 
        values='total_prescriptions'
    )
    prescriptions_wide.columns = [f'group_{col}_prescriptions' for col in prescriptions_wide.columns]
    prescriptions_wide = prescriptions_wide.reset_index()
    
    # Wide format for semaglutide fractions (months x groups)
    semaglutide_wide = combined_monthly_long.pivot(
        index='year_month', 
        columns='group_id', 
        values='fraction_patients_semaglutide'
    )
    semaglutide_wide.columns = [f'group_{col}_semaglutide_fraction' for col in semaglutide_wide.columns]
    semaglutide_wide = semaglutide_wide.reset_index()
    
    # Combined wide format (both metrics)
    combined_wide = prescriptions_wide.merge(semaglutide_wide, on='year_month')
    
    print(f"✅ Created wide format datasets:")
    print(f"   Wide prescriptions: {prescriptions_wide.shape[0]} months × {prescriptions_wide.shape[1]-1} group columns")
    print(f"   Wide semaglutide: {semaglutide_wide.shape[0]} months × {semaglutide_wide.shape[1]-1} group columns") 
    print(f"   Combined wide: {combined_wide.shape[0]} months × {combined_wide.shape[1]-1} total columns")
    
    # Step 6: Create summary dataframe  
    summary_df = pd.DataFrame(group_summaries)
    
    # Step 7: Show sample of wide data
    print(f"\n👀 SAMPLE WIDE DATA (first 10 months):")
    print("PRESCRIPTIONS BY GROUP:")
    print(prescriptions_wide.head(10).to_string(index=False))
    
    print(f"\nSEMAGLUTIDE FRACTIONS BY GROUP:")
    print(semaglutide_wide.head(10).round(4).to_string(index=False))
    
    # Step 8: Summary statistics
    print(f"\n📊 OVERALL GROUP SUMMARY:")
    print("=" * 40)
    
    print(f"Average across all groups:")
    print(f"   Prescriptions per month: {summary_df['avg_prescriptions_per_month'].mean():,.1f} (±{summary_df['avg_prescriptions_per_month'].std():,.1f})")
    print(f"   Semaglutide fraction: {summary_df['avg_semaglutide_fraction'].mean():.4f} (±{summary_df['avg_semaglutide_fraction'].std():.4f})")
    
    # Step 9: Save results
    print(f"\n💾 Saving results...")
    
    # Save randomization token
    with open('randomization_token.json', 'w') as f:
        json.dump(randomization_token, f, indent=2)
    
    # Save LONG format (original)
    combined_monthly_long.to_parquet("top100_groups_monthly_timeseries_long.parquet", compression='snappy')
    combined_monthly_long.to_csv("top100_groups_monthly_timeseries_long.csv", index=False)
    
    # Save WIDE format files
    prescriptions_wide.to_parquet("top100_groups_prescriptions_wide.parquet", compression='snappy')
    prescriptions_wide.to_csv("top100_groups_prescriptions_wide.csv", index=False)
    
    semaglutide_wide.to_parquet("top100_groups_semaglutide_wide.parquet", compression='snappy')
    semaglutide_wide.to_csv("top100_groups_semaglutide_wide.csv", index=False)
    
    combined_wide.to_parquet("top100_groups_combined_wide.parquet", compression='snappy')
    combined_wide.to_csv("top100_groups_combined_wide.csv", index=False)
    
    # Save summary and assignments
    summary_df.to_parquet("top100_random_groups_summary.parquet", compression='snappy')
    summary_df.to_csv("top100_random_groups_summary.csv", index=False)
    
    group_assignments = []
    for result in group_summaries:
        for phys_id in result['phys_ids']:
            group_assignments.append({
                'phys_id': phys_id,
                'group_id': result['group_id']
            })
    
    assignments_df = pd.DataFrame(group_assignments)
    assignments_df.to_parquet("top100_group_assignments.parquet", compression='snappy')
    assignments_df.to_csv("top100_group_assignments.csv", index=False)
    
    print(f"✅ Saved: randomization_token.json (RANDOMIZATION TOKEN)")
    print(f"✅ Saved: LONG format - top100_groups_monthly_timeseries_long.parquet/csv")
    print(f"✅ Saved: WIDE format - top100_groups_prescriptions_wide.parquet/csv")
    print(f"✅ Saved: WIDE format - top100_groups_semaglutide_wide.parquet/csv")
    print(f"✅ Saved: WIDE format - top100_groups_combined_wide.parquet/csv (MAIN WIDE FILE)")
    print(f"✅ Saved: top100_random_groups_summary.parquet/csv")
    print(f"✅ Saved: top100_group_assignments.parquet/csv")
    
    print(f"\n📊 OUTPUT FORMATS:")
    print(f"   LONG format: Each row = one group-month combination")
    print(f"   WIDE format: Each row = one month, columns = groups")
    print(f"   Main wide file: top100_groups_combined_wide.csv")
    
    return {
        'monthly_data_long': combined_monthly_long,
        'prescriptions_wide': prescriptions_wide,
        'semaglutide_wide': semaglutide_wide,
        'combined_wide': combined_wide,
        'summary': summary_df,
        'assignments': assignments_df,
        'randomization_token': randomization_token
    }

if __name__ == "__main__":
    results = analyze_top100_random_groups_wide()
