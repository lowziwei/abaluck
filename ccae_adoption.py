import pandas as pd
import numpy as np
import os
import json

def analyze_top100_random_selection():
    """
    Randomly select 10 physicians from top 100 (by patients) and analyze them individually
    OUTPUT: Wide format (months as rows, physicians as columns)
    """
    print("🎲 RANDOM SELECTION ANALYSIS - 10 FROM TOP 100 PHYSICIANS")
    print("=" * 60)
    
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
    
    # Step 2: Randomly select 10 physicians from the top 100
    print(f"\n🎲 Randomly selecting 10 physicians from top 100...")
    
    RANDOM_SEED = 42  # For reproducibility
    np.random.seed(RANDOM_SEED)
    selected_phys_ids = np.random.choice(top_100_phys_ids, size=10, replace=False).tolist()
    
    print(f"✅ Selected 10 physicians: {selected_phys_ids}")
    
    # Step 3: Filter monthly data for selected physicians
    selected_monthly = df[df['phys_ids'].isin(selected_phys_ids)].copy()
    
    print(f"📊 Monthly records for selected physicians: {len(selected_monthly):,}")
    
    # Save randomization token
    randomization_token = {
        'random_seed': RANDOM_SEED,
        'timestamp': pd.Timestamp.now().isoformat(),
        'total_top_physicians': len(top_100_phys_ids),
        'selected_count': 10,
        'top_100_phys_ids': top_100_phys_ids,
        'selected_phys_ids': selected_phys_ids
    }
    
    # Step 4: Analyze each physician - month by month
    print(f"\n📈 ANALYZING EACH PHYSICIAN BY MONTH:")
    print("=" * 50)
    
    # Get all possible months from the data
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
        
        # Create complete month series with 0s for missing months
        complete_months = pd.DataFrame({'year_month': all_months})
        monthly_metrics = complete_months.merge(monthly_metrics, on='year_month', how='left')
        
        # Fill missing values with 0
        monthly_metrics['total_prescriptions'] = monthly_metrics['total_prescriptions'].fillna(0)
        monthly_metrics['fraction_patients_semaglutide'] = monthly_metrics['fraction_patients_semaglutide'].fillna(0)
        
        # Add physician identifier
        monthly_metrics['phys_id'] = phys_id
        
        # Store all monthly data for plotting
        all_monthly_results.append(monthly_metrics)
        
        # Calculate summary stats for this physician (excluding 0 months for averages)
        non_zero_months = monthly_metrics[monthly_metrics['total_prescriptions'] > 0]
        avg_prescriptions = monthly_metrics['total_prescriptions'].mean()  # Include 0s in average
        avg_semaglutide_fraction = monthly_metrics['fraction_patients_semaglutide'].mean()  # Include 0s
        active_months = len(non_zero_months)
        
        print(f"   Total months: {len(monthly_metrics)}")
        print(f"   Active months: {active_months}")
        print(f"   Avg prescriptions per month: {avg_prescriptions:,.1f} (including 0s)")
        print(f"   Avg semaglutide fraction: {avg_semaglutide_fraction:.4f} ({avg_semaglutide_fraction*100:.2f}%)")
        
        # Store summary
        physician_summaries.append({
            'phys_id': phys_id,
            'total_months': len(monthly_metrics),
            'active_months': active_months,
            'avg_prescriptions_per_month': avg_prescriptions,
            'avg_semaglutide_fraction': avg_semaglutide_fraction
        })
    
    # Combine all monthly data (LONG format)
    combined_monthly_long = pd.concat(all_monthly_results, ignore_index=True)
    combined_monthly_long = combined_monthly_long.sort_values(['phys_id', 'year_month']).reset_index(drop=True)
    
    # Step 5: Create WIDE format datasets
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
    
    # Step 6: Create summary dataframe  
    summary_df = pd.DataFrame(physician_summaries)
    
    # Step 7: Show sample of wide data
    print(f"\n👀 SAMPLE WIDE DATA (first 10 months):")
    print("PRESCRIPTIONS BY PHYSICIAN:")
    print(prescriptions_wide.head(10).to_string(index=False))
    
    print(f"\nSEMAGLUTIDE FRACTIONS BY PHYSICIAN:")
    print(semaglutide_wide.head(10).round(4).to_string(index=False))
    
    # Step 8: Summary statistics
    print(f"\n📊 OVERALL PHYSICIAN SUMMARY:")
    print("=" * 40)
    
    print(f"Average across all selected physicians:")
    print(f"   Prescriptions per month: {summary_df['avg_prescriptions_per_month'].mean():,.1f} (±{summary_df['avg_prescriptions_per_month'].std():,.1f})")
    print(f"   Semaglutide fraction: {summary_df['avg_semaglutide_fraction'].mean():.4f} (±{summary_df['avg_semaglutide_fraction'].std():.4f})")
    
    # Step 9: Save results
    print(f"\n💾 Saving results...")
    
    # Save randomization token
    with open('randomization_token_10physicians.json', 'w') as f:
        json.dump(randomization_token, f, indent=2)
    
    # Save LONG format (original)
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
