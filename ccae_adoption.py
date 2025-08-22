import pandas as pd
import numpy as np
import os
import json

def analyze_top100_random_groups():
    """
    Split top 100 physicians (by patients) into random groups of 10 and analyze each group
    """
    print("🎲 RANDOM GROUPS ANALYSIS - TOP 100 PHYSICIANS")
    print("=" * 55)
    
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
    
    # Combine all monthly data
    combined_monthly = pd.concat(all_monthly_results, ignore_index=True)
    
    # Sort by month for plotting
    combined_monthly = combined_monthly.sort_values(['group_id', 'year_month']).reset_index(drop=True)
    
    print(f"\n📊 COMBINED MONTHLY DATA:")
    print(f"   Total month-group combinations: {len(combined_monthly):,}")
    print(f"   Date range: {combined_monthly['year_month'].min()} to {combined_monthly['year_month'].max()}")
    print(f"   Zero prescription months: {(combined_monthly['total_prescriptions'] == 0).sum():,}")
    
    # Step 5: Create summary dataframe  
    summary_df = pd.DataFrame(group_summaries)
    
    # Step 6: Show sample of monthly data
    print(f"\n👀 SAMPLE MONTHLY DATA (first 15 rows):")
    sample_cols = ['group_id', 'year_month', 'total_prescriptions', 'fraction_patients_semaglutide']
    print(combined_monthly[sample_cols].head(15).round(4).to_string(index=False))
    
    # Step 7: Overall summary statistics
    print(f"\n📊 OVERALL GROUP SUMMARY:")
    print("=" * 40)
    
    print(f"Average across all groups:")
    print(f"   Prescriptions per month: {summary_df['avg_prescriptions_per_month'].mean():,.1f} (±{summary_df['avg_prescriptions_per_month'].std():,.1f})")
    print(f"   Semaglutide fraction: {summary_df['avg_semaglutide_fraction'].mean():.4f} (±{summary_df['avg_semaglutide_fraction'].std():.4f})")
    
    print(f"\nRange across groups:")
    print(f"   Prescriptions per month: {summary_df['avg_prescriptions_per_month'].min():,.1f} to {summary_df['avg_prescriptions_per_month'].max():,.1f}")
    print(f"   Semaglutide fraction: {summary_df['avg_semaglutide_fraction'].min():.4f} to {summary_df['avg_semaglutide_fraction'].max():.4f}")
    
    # Step 8: Display summary table
    print(f"\n📋 GROUP SUMMARY TABLE:")
    display_df = summary_df[['group_id', 'physician_count', 'total_months', 'active_months', 'avg_prescriptions_per_month', 'avg_semaglutide_fraction']].copy()
    display_df['avg_prescriptions_per_month'] = display_df['avg_prescriptions_per_month'].round(1)
    display_df['avg_semaglutide_fraction'] = display_df['avg_semaglutide_fraction'].round(4)
    print(display_df.to_string(index=False))
    
    # Step 9: Save results
    print(f"\n💾 Saving results...")
    
    # Save randomization token first
    with open('randomization_token.json', 'w') as f:
        json.dump(randomization_token, f, indent=2)
    
    # Save monthly data (MAIN FILE FOR PLOTTING)
    combined_monthly.to_parquet("top100_groups_monthly_timeseries.parquet", compression='snappy')
    combined_monthly.to_csv("top100_groups_monthly_timeseries.csv", index=False)
    
    # Save summary
    summary_df.to_parquet("top100_random_groups_summary.parquet", compression='snappy')
    summary_df.to_csv("top100_random_groups_summary.csv", index=False)
    
    # Save group assignments
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
    print(f"✅ Saved: top100_groups_monthly_timeseries.parquet/csv (MAIN PLOTTING FILE)")
    print(f"✅ Saved: top100_random_groups_summary.parquet/csv")
    print(f"✅ Saved: top100_group_assignments.parquet/csv")
    
    print(f"\n🔐 RANDOMIZATION TOKEN:")
    print(f"   Random seed: {RANDOM_SEED}")
    print(f"   Token file: randomization_token.json")
    print(f"   Use this to reproduce exact same groups later")
    
    print(f"\n📊 FOR PLOTTING:")
    print(f"   Main file: top100_groups_monthly_timeseries.csv")
    print(f"   Columns: group_id, year_month, total_prescriptions, fraction_patients_semaglutide")
    print(f"   Each row = one group in one month")
    
    return {
        'monthly_data': combined_monthly,
        'summary': summary_df,
        'assignments': assignments_df,
        'randomization_token': randomization_token
    }

if __name__ == "__main__":
    results = analyze_top100_random_groups()
