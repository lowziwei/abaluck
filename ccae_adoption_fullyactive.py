import pandas as pd
import numpy as np
import os
import json

def analyze_top100_random_selection():
    """
    Filter to physicians active in all periods, get top 100 by patients, 
    randomly select 10, and analyze them individually
    OUTPUT: Wide format (months as rows, physicians as columns)
    """
    print("🎲 RANDOM SELECTION ANALYSIS - 10 FROM TOP 100 ALWAYS-ACTIVE PHYSICIANS")
    print("=" * 70)
    
    # Input files
    monthly_file = "physician_monthly_semaglutide_analysis.parquet"
    
    # Check if file exists
    if not os.path.exists(monthly_file):
        print(f"❌ File not found: {monthly_file}")
        return
    
    print(f"📁 Reading: {monthly_file}")
    df = pd.read_parquet(monthly_file)
    
    # Step 1: Filter to physicians active in ALL periods first
    print(f"🔄 Filtering to physicians active in all periods...")
    
    physician_month_counts = df.groupby('phys_ids')['year_month'].nunique()
    total_months = df['year_month'].nunique()
    always_active_phys_ids = physician_month_counts[physician_month_counts == total_months].index.tolist()
    
    print(f"📊 Total months in dataset: {total_months}")
    print(f"📊 Total physicians: {df['phys_ids'].nunique():,}")
    print(f"📊 Physicians active in all {total_months} months: {len(always_active_phys_ids):,}")
    
    if len(always_active_phys_ids) < 100:
        print(f"⚠️  Warning: Only {len(always_active_phys_ids)} physicians active in all periods (less than 100)")
        print(f"   Proceeding with available physicians...")
    
    # DEBUG: Check always-active physicians
    print(f"\n🔍 DEBUG INFO:")
    print(f"   Always active physicians count: {len(always_active_phys_ids)}")
    print(f"   First 10 always active (sorted): {sorted(always_active_phys_ids)[:10]}")
    
    # Step 2: From always-active physicians, get top 100 by patient count
    print(f"\n🔄 Getting top physicians by patient count from always-active set...")
    
    always_active_df = df[df['phys_ids'].isin(always_active_phys_ids)]
    physician_totals = always_active_df.groupby('phys_ids').agg({
        'total_patients': 'sum',
        'total_prescriptions': 'sum'
    }).reset_index()
    
    # Get top N (up to 100) from always-active physicians
    n_top = min(100, len(physician_totals))
    top_stats = physician_totals.nlargest(n_top, 'total_patients')
    
    # DEBUG: Check for ties and ordering
    print(f"\n🔍 DEBUG - TOP PHYSICIANS ANALYSIS:")
    print(f"   Top physicians count: {len(top_stats)}")
    print(f"   Patient count range: {top_stats['total_patients'].min():,} to {top_stats['total_patients'].max():,}")
    print(f"   Any duplicate patient counts? {top_stats['total_patients'].duplicated().any()}")
    if top_stats['total_patients'].duplicated().any():
        print(f"   Duplicate counts found - this could cause non-deterministic ordering!")
        duplicate_counts = top_stats[top_stats['total_patients'].duplicated(keep=False)]['total_patients'].unique()
        print(f"   Duplicate patient counts: {duplicate_counts}")
    
    # Sort to ensure consistent ordering when there are ties
    top_phys_ids = top_stats.sort_values(['total_patients', 'phys_ids'], ascending=[False, True])['phys_ids'].tolist()
    
    print(f"\n✅ Identified top {len(top_phys_ids)} always-active physicians")
    print(f"   Patient range: {top_stats['total_patients'].min():,} to {top_stats['total_patients'].max():,}")
    
    # DEBUG: Show top physicians list
    print(f"\n🔍 DEBUG - TOP PHYSICIANS LIST:")
    print(f"   First 10 top physicians (ordered): {top_phys_ids[:10]}")
    print(f"   Last 10 top physicians (ordered): {top_phys_ids[-10:]}")
    
    # Step 3: Randomly select 10 physicians from the top always-active physicians
    print(f"\n🎲 Randomly selecting 10 physicians from top {len(top_phys_ids)} always-active...")
    
    # Set random seed for reproducibility
    RANDOM_SEED = 42
    np.random.seed(RANDOM_SEED)
    
    print(f"🔍 DEBUG - BEFORE RANDOM SELECTION:")
    print(f"   Random seed: {RANDOM_SEED}")
    print(f"   Input list length: {len(top_phys_ids)}")
    print(f"   Input list first 5: {top_phys_ids[:5]}")
    
    n_select = min(10, len(top_phys_ids))
    selected_phys_ids = np.random.choice(top_phys_ids, size=n_select, replace=False).tolist()
    
    print(f"✅ Selected {len(selected_phys_ids)} physicians: {selected_phys_ids}")
    print(f"🔍 DEBUG - AFTER RANDOM SELECTION:")
    print(f"   Selected physicians (sorted): {sorted(selected_phys_ids)}")
    
    # Verify reproducibility by re-running the selection
    np.random.seed(RANDOM_SEED)  # Reset seed
    verification_selection = np.random.choice(top_phys_ids, size=n_select, replace=False).tolist()
    print(f"   Verification run (should match): {verification_selection}")
    print(f"   Selections match: {set(selected_phys_ids) == set(verification_selection)}")
    
    # Step 4: Filter monthly data for selected physicians
    selected_monthly = df[df['phys_ids'].isin(selected_phys_ids)].copy()
    
    print(f"📊 Monthly records for selected physicians: {len(selected_monthly):,}")
    
    # Save randomization token
    randomization_token = {
        'random_seed': RANDOM_SEED,
        'timestamp': pd.Timestamp.now().isoformat(),
        'total_physicians_in_data': df['phys_ids'].nunique(),
        'total_months': total_months,
        'always_active_physicians': len(always_active_phys_ids),
        'top_n_considered': len(top_phys_ids),
        'selected_count': len(selected_phys_ids),
        'always_active_phys_ids': always_active_phys_ids,
        'top_phys_ids': top_phys_ids,
        'selected_phys_ids': selected_phys_ids
    }
    
    # Step 5: Analyze each physician - month by month
    print(f"\n📈 ANALYZING EACH PHYSICIAN BY MONTH:")
    print("=" * 50)
    
    # Get all possible months from the data (should be same for all selected physicians)
    all_months = sorted(selected_monthly['year_month'].unique())
    print(f"📅 Date range: {all_months[0]} to {all_months[-1]} ({len(all_months)} months)")
    
    all_monthly_results = []
    physician_summaries = []
    
    for phys_id in selected_phys_ids:
        print(f"\n👨‍⚕️ PHYSICIAN {phys_id}:")
        
        # Filter data for this physician
        physician_monthly = selected_monthly[selected_monthly['phys_ids'] == phys_id]
        
        # Get monthly data (should already be by month)
        monthly_metrics = physician_monthly[['year_month', 'total_prescriptions', 'fraction_patients_semaglutide']].copy()
        
        # Verify we have all months (since we filtered to always-active)
        if len(monthly_metrics) != len(all_months):
            print(f"   ⚠️  Warning: Expected {len(all_months)} months, found {len(monthly_metrics)}")
        
        # Create complete month series (should not be needed, but for safety)
        complete_months = pd.DataFrame({'year_month': all_months})
        monthly_metrics = complete_months.merge(monthly_metrics, on='year_month', how='left')
        
        # Fill missing values with 0 (should not be needed for always-active physicians)
        monthly_metrics['total_prescriptions'] = monthly_metrics['total_prescriptions'].fillna(0)
        monthly_metrics['fraction_patients_semaglutide'] = monthly_metrics['fraction_patients_semaglutide'].fillna(0)
        
        # Add physician identifier
        monthly_metrics['phys_id'] = phys_id
        
        # Store all monthly data
        all_monthly_results.append(monthly_metrics)
        
        # Calculate summary stats for this physician
        avg_prescriptions = monthly_metrics['total_prescriptions'].mean()
        avg_semaglutide_fraction = monthly_metrics['fraction_patients_semaglutide'].mean()
        total_prescriptions = monthly_metrics['total_prescriptions'].sum()
        
        print(f"   Total months: {len(monthly_metrics)}")
        print(f"   Total prescriptions: {total_prescriptions:,.0f}")
        print(f"   Avg prescriptions per month: {avg_prescriptions:,.1f}")
        print(f"   Avg semaglutide fraction: {avg_semaglutide_fraction:.4f} ({avg_semaglutide_fraction*100:.2f}%)")
        
        # Store summary
        physician_summaries.append({
            'phys_id': phys_id,
            'total_months': len(monthly_metrics),
            'total_prescriptions': total_prescriptions,
            'avg_prescriptions_per_month': avg_prescriptions,
            'avg_semaglutide_fraction': avg_semaglutide_fraction
        })
    
    # Combine all monthly data (LONG format)
    combined_monthly_long = pd.concat(all_monthly_results, ignore_index=True)
    combined_monthly_long = combined_monthly_long.sort_values(['phys_id', 'year_month']).reset_index(drop=True)
    
    # Step 6: Create WIDE format datasets
    print(f"\n🔄 Converting to WIDE format...")
    
    # Wide format for prescriptions (months x physicians)
    prescriptions_wide = combined_monthly_long.pivot(
        index='year_month', 
        columns='phys_id', 
        values='total_prescriptions'
    )
    prescriptions_wide.columns = [f'physician_{col}_prescriptions' for col in prescriptions_wide.columns]
    prescriptions_wide = prescriptions_wide.reset_index()
    
    # Wide format for semaglutide fractions (months x physicians)
    semaglutide_wide = combined_monthly_long.pivot(
        index='year_month', 
        columns='phys_id', 
        values='fraction_patients_semaglutide'
    )
    semaglutide_wide.columns = [f'physician_{col}_semaglutide_fraction' for col in semaglutide_wide.columns]
    semaglutide_wide = semaglutide_wide.reset_index()
    
    # Combined wide format (both metrics)
    combined_wide = prescriptions_wide.merge(semaglutide_wide, on='year_month')
    
    print(f"✅ Created wide format datasets:")
    print(f"   Wide prescriptions: {prescriptions_wide.shape[0]} months × {prescriptions_wide.shape[1]-1} physician columns")
    print(f"   Wide semaglutide: {semaglutide_wide.shape[0]} months × {semaglutide_wide.shape[1]-1} physician columns") 
    print(f"   Combined wide: {combined_wide.shape[0]} months × {combined_wide.shape[1]-1} total columns")
    
    # Step 7: Create summary dataframe  
    summary_df = pd.DataFrame(physician_summaries)
    
    # Step 8: Show sample of wide data
    print(f"\n👀 SAMPLE WIDE DATA (first 10 months):")
    print("PRESCRIPTIONS BY PHYSICIAN:")
    display_cols = min(6, prescriptions_wide.shape[1])  # Show max 6 columns to fit screen
    print(prescriptions_wide.iloc[:10, :display_cols].to_string(index=False))
    
    print(f"\nSEMAGLUTIDE FRACTIONS BY PHYSICIAN:")
    print(semaglutide_wide.iloc[:10, :display_cols].round(4).to_string(index=False))
    
    # Step 9: Summary statistics
    print(f"\n📊 OVERALL PHYSICIAN SUMMARY:")
    print("=" * 40)
    
    print(f"Average across all selected physicians:")
    print(f"   Prescriptions per month: {summary_df['avg_prescriptions_per_month'].mean():,.1f} (±{summary_df['avg_prescriptions_per_month'].std():,.1f})")
    print(f"   Semaglutide fraction: {summary_df['avg_semaglutide_fraction'].mean():.4f} (±{summary_df['avg_semaglutide_fraction'].std():.4f})")
    print(f"   Total prescriptions: {summary_df['total_prescriptions'].sum():,.0f}")
    
    # Step 10: Save results
    print(f"\n💾 Saving results...")
    
    # Save randomization token
    with open('randomization_token_10physicians.json', 'w') as f:
        json.dump(randomization_token, f, indent=2)
    
    # Save LONG format
    combined_monthly_long.to_parquet("selected10_physicians_monthly_timeseries_long.parquet", compression='snappy')
    combined_monthly_long.to_csv("selected10_physicians_monthly_timeseries_long.csv", index=False)
    
    # Save WIDE format files
    prescriptions_wide.to_parquet("selected10_physicians_prescriptions_wide.parquet", compression='snappy')
    prescriptions_wide.to_csv("selected10_physicians_prescriptions_wide.csv", index=False)
    
    semaglutide_wide.to_parquet("selected10_physicians_semaglutide_wide.parquet", compression='snappy')
    semaglutide_wide.to_csv("selected10_physicians_semaglutide_wide.csv", index=False)
    
    combined_wide.to_parquet("selected10_physicians_combined_wide.parquet", compression='snappy')
    combined_wide.to_csv("selected10_physicians_combined_wide.csv", index=False)
    
    # Save summary
    summary_df.to_parquet("selected10_physicians_summary.parquet", compression='snappy')
    summary_df.to_csv("selected10_physicians_summary.csv", index=False)
    
    print(f"✅ Saved: randomization_token_10physicians.json (RANDOMIZATION TOKEN)")
    print(f"✅ Saved: LONG format - selected10_physicians_monthly_timeseries_long.parquet/csv")
    print(f"✅ Saved: WIDE format - selected10_physicians_prescriptions_wide.parquet/csv")
    print(f"✅ Saved: WIDE format - selected10_physicians_semaglutide_wide.parquet/csv")
    print(f"✅ Saved: WIDE format - selected10_physicians_combined_wide.csv (MAIN WIDE FILE)")
    print(f"✅ Saved: selected10_physicians_summary.parquet/csv")
    
    print(f"\n📊 OUTPUT FORMATS:")
    print(f"   LONG format: Each row = one physician-month combination")
    print(f"   WIDE format: Each row = one month, columns = individual physicians")
    print(f"   Main wide file: selected10_physicians_combined_wide.csv")
    print(f"   Key insight: All selected physicians are active in ALL {total_months} months")
    
    return {
        'monthly_data_long': combined_monthly_long,
        'prescriptions_wide': prescriptions_wide,
        'semaglutide_wide': semaglutide_wide,
        'combined_wide': combined_wide,
        'summary': summary_df,
        'randomization_token': randomization_token
    }

if __name__ == "__main__":
    results = analyze_top100_random_selection()
