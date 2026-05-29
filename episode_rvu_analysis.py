import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import warnings
warnings.filterwarnings('ignore')

# Settings
SAMPLE_SIZE = 15000  # Should match what you used in the main script
YEARS = [str(year) for year in range(2014, 2025)]  # 2014-2024

# Directories
home_dir = os.path.expanduser('~')
input_dir = os.path.join(home_dir, 'episode_outputs')
output_dir = os.path.join(home_dir, 'episode_outputs', 'rvu_analysis')

os.makedirs(output_dir, exist_ok=True)

print("="*100)
print("RVU ANALYSIS - SEPARATE ANALYSIS OF EPISODE DATA")
print("="*100)
print(f"Reading episode files from: {input_dir}")
print(f"Saving analysis to: {output_dir}")
print("="*100)

def create_rvu_analysis(df, year):
    """
    Create RVU analysis graphs:
    1. Binscatter: Total RVU vs (Work + MP) RVU (full and truncated)
    2. Binscatter: PAY vs Total RVU (full and truncated)
    3. Histograms of allowed amount (PAY)
    4. Histograms of (Work RVU + MP RVU)
    5. Histograms of Total RVU
    """
    if df is None or len(df) == 0:
        print(f"[{year}] WARNING: No data for RVU analysis")
        return
    
    # Check if RVU columns exist
    required_cols = ['WORK_RVU', 'MP_RVU', 'PE_RVU_actualized', 'PAY']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"[{year}] WARNING: Missing RVU columns for analysis: {missing_cols}")
        print(f"[{year}] Available columns: {df.columns.tolist()}")
        return
    
    print(f"\n[{year}] Creating RVU analysis graphs...")
    
    # Calculate derived variables
    df_analysis = df.copy()
    df_analysis['Work_MP_RVU'] = df_analysis['WORK_RVU'] + df_analysis['MP_RVU']
    df_analysis['Total_RVU'] = df_analysis['WORK_RVU'] + df_analysis['MP_RVU'] + df_analysis['PE_RVU_actualized']
    
    # Remove any rows with missing RVU data
    df_analysis = df_analysis.dropna(subset=['Work_MP_RVU', 'Total_RVU', 'PAY'])
    
    if len(df_analysis) == 0:
        print(f"[{year}] WARNING: No valid RVU data after removing missing values")
        return
    
    print(f"  [{year}] Episodes with complete RVU data: {len(df_analysis):,}")
    
    # Calculate 95th percentiles for truncation
    p95_work_mp = np.percentile(df_analysis['Work_MP_RVU'], 95)
    p95_total = np.percentile(df_analysis['Total_RVU'], 95)
    p95_pay = np.percentile(df_analysis['PAY'], 95)
    
    # Create truncated dataset
    df_truncated = df_analysis[
        (df_analysis['Work_MP_RVU'] <= p95_work_mp) & 
        (df_analysis['Total_RVU'] <= p95_total)
    ].copy()
    
    print(f"  [{year}] Episodes after 95th percentile truncation: {len(df_truncated):,}")
    
    # ===== FIGURE 1: Binscatter - Work + MP RVU =====
    fig1, axes1 = plt.subplots(1, 2, figsize=(16, 6))
    
    n_bins = 50
    
    # Panel A: Full data binscatter (Work + MP)
    df_analysis_sorted = df_analysis.sort_values('Work_MP_RVU')
    df_analysis_sorted['bin_workmp'] = pd.qcut(df_analysis_sorted['Work_MP_RVU'], 
                                                 q=n_bins, 
                                                 labels=False, 
                                                 duplicates='drop')
    
    bin_means_workmp = df_analysis_sorted.groupby('bin_workmp').agg({
        'Work_MP_RVU': 'mean',
        'Total_RVU': 'mean'
    }).reset_index()
    
    axes1[0].scatter(bin_means_workmp['Work_MP_RVU'], bin_means_workmp['Total_RVU'], 
                     s=50, c='blue', alpha=0.7)
    axes1[0].set_xlabel('Work RVU + Malpractice RVU', fontsize=11)
    axes1[0].set_ylabel('Total RVU (Work + MP + PE)', fontsize=11)
    axes1[0].set_title(f'{year}: Total RVU vs (Work + MP) RVU - Full Data\n{len(bin_means_workmp)} bins, n={len(df_analysis):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes1[0].grid(True, alpha=0.3)
    
    max_val = max(bin_means_workmp['Work_MP_RVU'].max(), bin_means_workmp['Total_RVU'].max())
    axes1[0].plot([0, max_val], [0, max_val], 'r--', linewidth=1, alpha=0.5, 
                  label='Total = Work + MP (PE = 0)')
    axes1[0].legend()
    
    # Panel B: Truncated at 95th percentile (Work + MP)
    df_truncated_sorted = df_truncated.sort_values('Work_MP_RVU')
    df_truncated_sorted['bin_workmp'] = pd.qcut(df_truncated_sorted['Work_MP_RVU'], 
                                                  q=n_bins, 
                                                  labels=False, 
                                                  duplicates='drop')
    
    bin_means_workmp_trunc = df_truncated_sorted.groupby('bin_workmp').agg({
        'Work_MP_RVU': 'mean',
        'Total_RVU': 'mean'
    }).reset_index()
    
    axes1[1].scatter(bin_means_workmp_trunc['Work_MP_RVU'], bin_means_workmp_trunc['Total_RVU'], 
                     s=50, c='green', alpha=0.7)
    axes1[1].set_xlabel('Work RVU + Malpractice RVU', fontsize=11)
    axes1[1].set_ylabel('Total RVU (Work + MP + PE)', fontsize=11)
    axes1[1].set_title(f'{year}: Total RVU vs (Work + MP) RVU - 95th Percentile Truncated\n{len(bin_means_workmp_trunc)} bins, n={len(df_truncated):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes1[1].grid(True, alpha=0.3)
    
    max_val_trunc = max(bin_means_workmp_trunc['Work_MP_RVU'].max(), bin_means_workmp_trunc['Total_RVU'].max())
    axes1[1].plot([0, max_val_trunc], [0, max_val_trunc], 'r--', linewidth=1, alpha=0.5,
                  label='Total = Work + MP (PE = 0)')
    axes1[1].legend()
    
    plt.tight_layout()
    scatter_file_workmp = os.path.join(output_dir, f'rvu_binscatter_workmp_{year}.png')
    plt.savefig(scatter_file_workmp, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Work+MP binscatters saved to: {scatter_file_workmp}")
    
    # ===== FIGURE 2: Binscatter - Total RVU =====
    fig2, axes2 = plt.subplots(1, 2, figsize=(16, 6))
    
    # Panel A: Full data binscatter (Total RVU)
    df_analysis_sorted_total = df_analysis.sort_values('Total_RVU')
    df_analysis_sorted_total['bin_total'] = pd.qcut(df_analysis_sorted_total['Total_RVU'], 
                                                      q=n_bins, 
                                                      labels=False, 
                                                      duplicates='drop')
    
    bin_means_total = df_analysis_sorted_total.groupby('bin_total').agg({
        'Total_RVU': 'mean',
        'PAY': 'mean'
    }).reset_index()
    
    axes2[0].scatter(bin_means_total['Total_RVU'], bin_means_total['PAY'], 
                     s=50, c='purple', alpha=0.7)
    axes2[0].set_xlabel('Total RVU (Work + MP + PE)', fontsize=11)
    axes2[0].set_ylabel('Allowed Amount (PAY) ($)', fontsize=11)
    axes2[0].set_title(f'{year}: PAY vs Total RVU - Full Data\n{len(bin_means_total)} bins, n={len(df_analysis):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes2[0].grid(True, alpha=0.3)
    
    # Panel B: Truncated at 95th percentile (Total RVU)
    df_truncated_sorted_total = df_truncated.sort_values('Total_RVU')
    df_truncated_sorted_total['bin_total'] = pd.qcut(df_truncated_sorted_total['Total_RVU'], 
                                                       q=n_bins, 
                                                       labels=False, 
                                                       duplicates='drop')
    
    bin_means_total_trunc = df_truncated_sorted_total.groupby('bin_total').agg({
        'Total_RVU': 'mean',
        'PAY': 'mean'
    }).reset_index()
    
    axes2[1].scatter(bin_means_total_trunc['Total_RVU'], bin_means_total_trunc['PAY'], 
                     s=50, c='orange', alpha=0.7)
    axes2[1].set_xlabel('Total RVU (Work + MP + PE)', fontsize=11)
    axes2[1].set_ylabel('Allowed Amount (PAY) ($)', fontsize=11)
    axes2[1].set_title(f'{year}: PAY vs Total RVU - 95th Percentile Truncated\n{len(bin_means_total_trunc)} bins, n={len(df_truncated):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes2[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    scatter_file_total = os.path.join(output_dir, f'rvu_binscatter_total_{year}.png')
    plt.savefig(scatter_file_total, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Total RVU binscatters saved to: {scatter_file_total}")
    
    # ===== FIGURE 3: Histograms of Allowed Amount (PAY) =====
    fig3, axes3 = plt.subplots(1, 2, figsize=(14, 6))
    
    # Panel A: Full distribution (0-95th percentile)
    pay_95 = df_analysis[df_analysis['PAY'] <= p95_pay]['PAY']
    axes3[0].hist(pay_95, bins=100, edgecolor='black', alpha=0.7, color='steelblue')
    axes3[0].set_xlabel('Allowed Amount (PAY) per Episode ($)', fontsize=11)
    axes3[0].set_ylabel('Frequency', fontsize=11)
    axes3[0].set_title(f'{year}: Allowed Amount Distribution (0-95th percentile)\nn={len(pay_95):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes3[0].axvline(np.median(pay_95), color='red', linestyle='--', linewidth=2,
                     label=f'Median: ${np.median(pay_95):,.2f}')
    axes3[0].legend()
    axes3[0].grid(True, alpha=0.3)
    
    # Panel B: Lower tail (0-10th percentile)
    p10_pay = np.percentile(df_analysis['PAY'], 10)
    pay_10 = df_analysis[df_analysis['PAY'] <= p10_pay]['PAY']
    axes3[1].hist(pay_10, bins=50, edgecolor='black', alpha=0.7, color='coral')
    axes3[1].set_xlabel('Allowed Amount (PAY) per Episode ($)', fontsize=11)
    axes3[1].set_ylabel('Frequency', fontsize=11)
    axes3[1].set_title(f'{year}: Allowed Amount - Lower Tail (0-10th percentile)\nn={len(pay_10):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes3[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    pay_hist_file = os.path.join(output_dir, f'allowed_amount_histogram_{year}.png')
    plt.savefig(pay_hist_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Allowed amount histograms saved to: {pay_hist_file}")
    
    # ===== FIGURE 4: Histograms of Work + MP RVU =====
    fig4, axes4 = plt.subplots(1, 2, figsize=(14, 6))
    
    # Panel A: Full distribution (0-95th percentile)
    work_mp_95 = df_analysis[df_analysis['Work_MP_RVU'] <= p95_work_mp]['Work_MP_RVU']
    axes4[0].hist(work_mp_95, bins=100, edgecolor='black', alpha=0.7, color='purple')
    axes4[0].set_xlabel('Work RVU + Malpractice RVU per Episode', fontsize=11)
    axes4[0].set_ylabel('Frequency', fontsize=11)
    axes4[0].set_title(f'{year}: Work + MP RVU Distribution (0-95th percentile)\nn={len(work_mp_95):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes4[0].axvline(np.median(work_mp_95), color='red', linestyle='--', linewidth=2,
                     label=f'Median: {np.median(work_mp_95):.2f}')
    axes4[0].legend()
    axes4[0].grid(True, alpha=0.3)
    
    # Panel B: Lower tail (0-10th percentile)
    p10_work_mp = np.percentile(df_analysis['Work_MP_RVU'], 10)
    work_mp_10 = df_analysis[df_analysis['Work_MP_RVU'] <= p10_work_mp]['Work_MP_RVU']
    axes4[1].hist(work_mp_10, bins=50, edgecolor='black', alpha=0.7, color='orange')
    axes4[1].set_xlabel('Work RVU + Malpractice RVU per Episode', fontsize=11)
    axes4[1].set_ylabel('Frequency', fontsize=11)
    axes4[1].set_title(f'{year}: Work + MP RVU - Lower Tail (0-10th percentile)\nn={len(work_mp_10):,} episodes', 
                       fontsize=12, fontweight='bold')
    
    # Mark the minimum value
    min_work_mp = work_mp_10.min()
    axes4[1].axvline(min_work_mp, color='red', linestyle='--', linewidth=2,
                     label=f'Min: {min_work_mp:.2f}')
    axes4[1].legend()
    axes4[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    work_mp_hist_file = os.path.join(output_dir, f'work_mp_rvu_histogram_{year}.png')
    plt.savefig(work_mp_hist_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Work + MP RVU histograms saved to: {work_mp_hist_file}")
    
    # ===== FIGURE 5: Histograms of Total RVU =====
    fig5, axes5 = plt.subplots(1, 2, figsize=(14, 6))
    
    # Panel A: Full distribution (0-95th percentile)
    total_rvu_95 = df_analysis[df_analysis['Total_RVU'] <= p95_total]['Total_RVU']
    axes5[0].hist(total_rvu_95, bins=100, edgecolor='black', alpha=0.7, color='teal')
    axes5[0].set_xlabel('Total RVU (Work + MP + PE) per Episode', fontsize=11)
    axes5[0].set_ylabel('Frequency', fontsize=11)
    axes5[0].set_title(f'{year}: Total RVU Distribution (0-95th percentile)\nn={len(total_rvu_95):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes5[0].axvline(np.median(total_rvu_95), color='red', linestyle='--', linewidth=2,
                     label=f'Median: {np.median(total_rvu_95):.2f}')
    axes5[0].legend()
    axes5[0].grid(True, alpha=0.3)
    
    # Panel B: Lower tail (0-10th percentile)
    p10_total = np.percentile(df_analysis['Total_RVU'], 10)
    total_rvu_10 = df_analysis[df_analysis['Total_RVU'] <= p10_total]['Total_RVU']
    axes5[1].hist(total_rvu_10, bins=50, edgecolor='black', alpha=0.7, color='darkgreen')
    axes5[1].set_xlabel('Total RVU (Work + MP + PE) per Episode', fontsize=11)
    axes5[1].set_ylabel('Frequency', fontsize=11)
    axes5[1].set_title(f'{year}: Total RVU - Lower Tail (0-10th percentile)\nn={len(total_rvu_10):,} episodes', 
                       fontsize=12, fontweight='bold')
    
    # Mark the minimum value
    min_total = total_rvu_10.min()
    axes5[1].axvline(min_total, color='red', linestyle='--', linewidth=2,
                     label=f'Min: {min_total:.2f}')
    axes5[1].legend()
    axes5[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    total_rvu_hist_file = os.path.join(output_dir, f'total_rvu_histogram_{year}.png')
    plt.savefig(total_rvu_hist_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Total RVU histograms saved to: {total_rvu_hist_file}")
    
    # ===== BUNCHING ANALYSIS =====
    print(f"\n[{year}] BUNCHING ANALYSIS (Work + MP RVU):")
    print(f"  Minimum Work + MP RVU: {df_analysis['Work_MP_RVU'].min():.3f}")
    print(f"  5th percentile: {np.percentile(df_analysis['Work_MP_RVU'], 5):.3f}")
    print(f"  10th percentile: {p10_work_mp:.3f}")
    print(f"  Median: {np.median(df_analysis['Work_MP_RVU']):.3f}")
    
    # Check for bunching at common visit RVU values
    # Typical office visit Work RVUs: 99211=0.18, 99212=0.48, 99213=0.97, 99214=1.50, 99215=2.11
    # Adding typical MP RVU (roughly 0.04-0.20 depending on code)
    common_visit_rvus = [0.22, 0.52, 1.01, 1.54, 2.31]  # Approximate Work+MP for 99211-99215
    
    print(f"\n  Checking for bunching near common office visit RVU values:")
    for visit_rvu in common_visit_rvus:
        nearby = df_analysis[
            (df_analysis['Work_MP_RVU'] >= visit_rvu - 0.1) & 
            (df_analysis['Work_MP_RVU'] <= visit_rvu + 0.1)
        ]
        if len(nearby) > 0:
            pct = len(nearby) / len(df_analysis) * 100
            print(f"    Episodes near {visit_rvu:.2f} RVU (±0.1): {len(nearby):,} ({pct:.2f}%)")
    
    # Check for bunching at the very minimum (visit lower bound)
    min_rvu = df_analysis['Work_MP_RVU'].min()
    within_01 = df_analysis[
        (df_analysis['Work_MP_RVU'] >= min_rvu) & 
        (df_analysis['Work_MP_RVU'] <= min_rvu + 0.1)
    ]
    within_05 = df_analysis[
        (df_analysis['Work_MP_RVU'] >= min_rvu) & 
        (df_analysis['Work_MP_RVU'] <= min_rvu + 0.5)
    ]
    
    print(f"\n  Bunching at minimum (visit lower bound):")
    print(f"    Episodes within 0.1 RVU of min: {len(within_01):,} ({len(within_01)/len(df_analysis)*100:.2f}%)")
    print(f"    Episodes within 0.5 RVU of min: {len(within_05):,} ({len(within_05)/len(df_analysis)*100:.2f}%)")
    
    # Summary statistics
    print(f"\n[{year}] RVU SUMMARY STATISTICS:")
    print(f"  Mean Total RVU: {df_analysis['Total_RVU'].mean():.2f}")
    print(f"  Median Total RVU: {np.median(df_analysis['Total_RVU']):.2f}")
    print(f"  Mean Work + MP RVU: {df_analysis['Work_MP_RVU'].mean():.2f}")
    print(f"  Median Work + MP RVU: {np.median(df_analysis['Work_MP_RVU']):.2f}")
    print(f"  Mean PE RVU (actualized): {df_analysis['PE_RVU_actualized'].mean():.2f}")
    print(f"  Median PE RVU (actualized): {np.median(df_analysis['PE_RVU_actualized']):.2f}")
    print(f"  Mean Allowed Amount (PAY): ${df_analysis['PAY'].mean():,.2f}")
    print(f"  Median Allowed Amount (PAY): ${np.median(df_analysis['PAY']):,.2f}")
    
    # Correlation
    correlation = df_analysis[['Work_MP_RVU', 'Total_RVU', 'PE_RVU_actualized', 'PAY']].corr()
    print(f"\n[{year}] CORRELATIONS:")
    print(f"  Work+MP RVU vs Total RVU: {correlation.loc['Work_MP_RVU', 'Total_RVU']:.3f}")
    print(f"  Work+MP RVU vs PE RVU: {correlation.loc['Work_MP_RVU', 'PE_RVU_actualized']:.3f}")
    print(f"  Work+MP RVU vs PAY: {correlation.loc['Work_MP_RVU', 'PAY']:.3f}")
    print(f"  Total RVU vs PAY: {correlation.loc['Total_RVU', 'PAY']:.3f}")
    print(f"  PE RVU vs PAY: {correlation.loc['PE_RVU_actualized', 'PAY']:.3f}")
    
    # Save summary statistics to CSV
    summary_stats = pd.DataFrame({
        'year': [year],
        'n_episodes': [len(df_analysis)],
        'mean_total_rvu': [df_analysis['Total_RVU'].mean()],
        'median_total_rvu': [np.median(df_analysis['Total_RVU'])],
        'mean_work_mp_rvu': [df_analysis['Work_MP_RVU'].mean()],
        'median_work_mp_rvu': [np.median(df_analysis['Work_MP_RVU'])],
        'mean_pe_rvu': [df_analysis['PE_RVU_actualized'].mean()],
        'median_pe_rvu': [np.median(df_analysis['PE_RVU_actualized'])],
        'mean_pay': [df_analysis['PAY'].mean()],
        'median_pay': [np.median(df_analysis['PAY'])],
        'min_work_mp_rvu': [df_analysis['Work_MP_RVU'].min()],
        'p5_work_mp_rvu': [np.percentile(df_analysis['Work_MP_RVU'], 5)],
        'p10_work_mp_rvu': [np.percentile(df_analysis['Work_MP_RVU'], 10)],
        'corr_work_mp_total': [correlation.loc['Work_MP_RVU', 'Total_RVU']],
        'corr_work_mp_pay': [correlation.loc['Work_MP_RVU', 'PAY']],
        'corr_total_pay': [correlation.loc['Total_RVU', 'PAY']]
    })
    
    summary_file = os.path.join(output_dir, f'rvu_summary_stats_{year}.csv')
    summary_stats.to_csv(summary_file, index=False)
    print(f"\n  ✓ Summary statistics saved to: {summary_file}")
    
    return summary_stats

# Main execution
if __name__ == '__main__':
    all_summaries = []
    
    for year in YEARS:
        print(f"\n{'='*100}")
        print(f"ANALYZING YEAR: {year}")
        print(f"{'='*100}")
        
        # Construct filename
        sample_suffix = f"_sample{SAMPLE_SIZE}" if SAMPLE_SIZE else "_all"
        episode_file = os.path.join(input_dir, f'episodes_{year}{sample_suffix}.csv')
        
        # Check if file exists
        if not os.path.exists(episode_file):
            print(f"[{year}] ERROR: File not found: {episode_file}")
            continue
        
        try:
            # Load episode data
            print(f"[{year}] Loading data from: {episode_file}")
            df = pd.read_csv(episode_file)
            print(f"[{year}] Loaded {len(df):,} episodes")
            
            # Create analysis
            summary = create_rvu_analysis(df, year)
            if summary is not None:
                all_summaries.append(summary)
            
            # Clean up
            del df
            
        except Exception as e:
            print(f"[{year}] ERROR: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Combine all summary statistics
    if len(all_summaries) > 0:
        print(f"\n{'='*100}")
        print("COMBINING SUMMARY STATISTICS")
        print(f"{'='*100}")
        
        combined_summary = pd.concat(all_summaries, ignore_index=True)
        combined_file = os.path.join(output_dir, f'rvu_summary_stats_all_years.csv')
        combined_summary.to_csv(combined_file, index=False)
        
        print(f"\n✓ Combined summary statistics saved to: {combined_file}")
        
        # Print summary table
        print("\n" + "="*100)
        print("SUMMARY TABLE - RVU STATISTICS ACROSS YEARS")
        print("="*100)
        print(combined_summary[['year', 'n_episodes', 'mean_total_rvu', 'mean_work_mp_rvu', 
                                'mean_pe_rvu', 'mean_pay', 'min_work_mp_rvu']].to_string(index=False))
    
    print(f"\n{'='*100}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*100}")
    print(f"Output directory: {output_dir}")
    print(f"\nFiles generated per year:")
    print(f"  ✓ rvu_binscatter_workmp_YYYY.png (Total RVU vs Work+MP, full & truncated)")
    print(f"  ✓ rvu_binscatter_total_YYYY.png (PAY vs Total RVU, full & truncated)")
    print(f"  ✓ allowed_amount_histogram_YYYY.png (PAY distribution)")
    print(f"  ✓ work_mp_rvu_histogram_YYYY.png (Work+MP RVU distribution)")
    print(f"  ✓ total_rvu_histogram_YYYY.png (Total RVU distribution)")
    print(f"  ✓ rvu_summary_stats_YYYY.csv (summary statistics)")
    print(f"\nCombined file:")
    print(f"  ✓ rvu_summary_stats_all_years.csv")
    print("="*100)
