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
    1. Scatterplot: Total RVU vs (Work RVU + MP RVU)
    2. Same plot truncated at 95th percentile
    3. Histograms of allowed amount (PAY)
    4. Histograms of (Work RVU + MP RVU) - checking for bunching at lower bound
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
    
    # ===== FIGURE 1: Full scatterplot + Truncated scatterplot =====
    fig1, axes1 = plt.subplots(1, 2, figsize=(16, 6))
    
    # Panel A: Full data
    axes1[0].scatter(df_analysis['Work_MP_RVU'], df_analysis['Total_RVU'], 
                     alpha=0.3, s=1, c='blue')
    axes1[0].set_xlabel('Work RVU + Malpractice RVU', fontsize=11)
    axes1[0].set_ylabel('Total RVU (Work + MP + PE)', fontsize=11)
    axes1[0].set_title(f'{year}: Total RVU vs (Work + MP) RVU - Full Data\nn={len(df_analysis):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes1[0].grid(True, alpha=0.3)
    
    # Add 45-degree line (where Total = Work + MP, i.e., PE = 0)
    max_val = max(df_analysis['Work_MP_RVU'].max(), df_analysis['Total_RVU'].max())
    axes1[0].plot([0, max_val], [0, max_val], 'r--', linewidth=1, alpha=0.5, 
                  label='Total = Work + MP (PE = 0)')
    axes1[0].legend()
    
    # Panel B: Truncated at 95th percentile
    axes1[1].scatter(df_truncated['Work_MP_RVU'], df_truncated['Total_RVU'], 
                     alpha=0.3, s=1, c='green')
    axes1[1].set_xlabel('Work RVU + Malpractice RVU', fontsize=11)
    axes1[1].set_ylabel('Total RVU (Work + MP + PE)', fontsize=11)
    axes1[1].set_title(f'{year}: Total RVU vs (Work + MP) RVU - 95th Percentile Truncated\nn={len(df_truncated):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes1[1].grid(True, alpha=0.3)
    
    # Add 45-degree line
    max_val_trunc = max(df_truncated['Work_MP_RVU'].max(), df_truncated['Total_RVU'].max())
    axes1[1].plot([0, max_val_trunc], [0, max_val_trunc], 'r--', linewidth=1, alpha=0.5,
                  label='Total = Work + MP (PE = 0)')
    axes1[1].legend()
    
    plt.tight_layout()
    scatter_file = os.path.join(output_dir, f'rvu_scatterplot_{year}.png')
    plt.savefig(scatter_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Scatterplots saved to: {scatter_file}")
    
    # ===== FIGURE 2: Histograms of Allowed Amount (PAY) =====
    fig2, axes2 = plt.subplots(1, 2, figsize=(14, 6))
    
    # Panel A: Full distribution (0-95th percentile)
    pay_95 = df_analysis[df_analysis['PAY'] <= p95_pay]['PAY']
    axes2[0].hist(pay_95, bins=100, edgecolor='black', alpha=0.7, color='steelblue')
    axes2[0].set_xlabel('Allowed Amount (PAY) per Episode ($)', fontsize=11)
    axes2[0].set_ylabel('Frequency', fontsize=11)
    axes2[0].set_title(f'{year}: Allowed Amount Distribution (0-95th percentile)\nn={len(pay_95):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes2[0].axvline(np.median(pay_95), color='red', linestyle='--', linewidth=2,
                     label=f'Median: ${np.median(pay_95):,.2f}')
    axes2[0].legend()
    axes2[0].grid(True, alpha=0.3)
    
    # Panel B: Lower tail (0-10th percentile)
    p10_pay = np.percentile(df_analysis['PAY'], 10)
    pay_10 = df_analysis[df_analysis['PAY'] <= p10_pay]['PAY']
    axes2[1].hist(pay_10, bins=50, edgecolor='black', alpha=0.7, color='coral')
    axes2[1].set_xlabel('Allowed Amount (PAY) per Episode ($)', fontsize=11)
    axes2[1].set_ylabel('Frequency', fontsize=11)
    axes2[1].set_title(f'{year}: Allowed Amount - Lower Tail (0-10th percentile)\nn={len(pay_10):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes2[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    pay_hist_file = os.path.join(output_dir, f'allowed_amount_histogram_{year}.png')
    plt.savefig(pay_hist_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Allowed amount histograms saved to: {pay_hist_file}")
    
    # ===== FIGURE 3: Histograms of Work + MP RVU (checking for bunching) =====
    fig3, axes3 = plt.subplots(1, 2, figsize=(14, 6))
    
    # Panel A: Full distribution (0-95th percentile)
    work_mp_95 = df_analysis[df_analysis['Work_MP_RVU'] <= p95_work_mp]['Work_MP_RVU']
    axes3[0].hist(work_mp_95, bins=100, edgecolor='black', alpha=0.7, color='purple')
    axes3[0].set_xlabel('Work RVU + Malpractice RVU per Episode', fontsize=11)
    axes3[0].set_ylabel('Frequency', fontsize=11)
    axes3[0].set_title(f'{year}: Work + MP RVU Distribution (0-95th percentile)\nn={len(work_mp_95):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes3[0].axvline(np.median(work_mp_95), color='red', linestyle='--', linewidth=2,
                     label=f'Median: {np.median(work_mp_95):.2f}')
    axes3[0].legend()
    axes3[0].grid(True, alpha=0.3)
    
    # Panel B: Lower tail (0-10th percentile) - CHECKING FOR BUNCHING
    p10_work_mp = np.percentile(df_analysis['Work_MP_RVU'], 10)
    work_mp_10 = df_analysis[df_analysis['Work_MP_RVU'] <= p10_work_mp]['Work_MP_RVU']
    axes3[1].hist(work_mp_10, bins=50, edgecolor='black', alpha=0.7, color='orange')
    axes3[1].set_xlabel('Work RVU + Malpractice RVU per Episode', fontsize=11)
    axes3[1].set_ylabel('Frequency', fontsize=11)
    axes3[1].set_title(f'{year}: Work + MP RVU - Lower Tail (0-10th percentile)\nChecking for bunching at visit cost\nn={len(work_mp_10):,} episodes', 
                       fontsize=12, fontweight='bold')
    
    # Mark the minimum value (potential bunching point)
    min_work_mp = work_mp_10.min()
    axes3[1].axvline(min_work_mp, color='red', linestyle='--', linewidth=2,
                     label=f'Min: {min_work_mp:.2f}')
    axes3[1].legend()
    axes3[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    work_mp_hist_file = os.path.join(output_dir, f'work_mp_rvu_histogram_{year}.png')
    plt.savefig(work_mp_hist_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Work + MP RVU histograms saved to: {work_mp_hist_file}")
    
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
    print(f"  ✓ rvu_scatterplot_YYYY.png (Total RVU vs Work+MP, full & truncated)")
    print(f"  ✓ allowed_amount_histogram_YYYY.png (PAY distribution)")
    print(f"  ✓ work_mp_rvu_histogram_YYYY.png (Work+MP RVU with bunching analysis)")
    print(f"  ✓ rvu_summary_stats_YYYY.csv (summary statistics)")
    print(f"\nCombined file:")
    print(f"  ✓ rvu_summary_stats_all_years.csv")
    print("="*100)
