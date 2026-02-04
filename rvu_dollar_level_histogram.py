import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import warnings
warnings.filterwarnings('ignore')

# Settings
SAMPLE_SIZE = 15000
YEARS = [str(year) for year in range(2014, 2025)]

# Directories
home_dir = os.path.expanduser('~')
input_dir = os.path.join(home_dir, 'episode_outputs')
output_dir = os.path.join(home_dir, 'episode_outputs', 'payment_analysis')

os.makedirs(output_dir, exist_ok=True)

# Conversion rates (Medicare fee schedule conversion factors)
CONVERSION_RATES = {
    2014: 35.8228,
    2015: 35.9335,
    2016: 35.8043,
    2017: 35.8887,
    2018: 35.9996,
    2019: 36.0391,
    2020: 36.0896,
    2021: 34.8931,
    2022: 34.6062,
    2023: 33.8872,
    2024: 33.2875
}

print("="*100)
print("PAYMENT DISTRIBUTION ANALYSIS: TOTPAY vs Medicare-based Estimate (m)")
print("="*100)
print(f"Reading episode files from: {input_dir}")
print(f"Saving analysis to: {output_dir}")
print("="*100)

def create_payment_analysis(df, year):
    """
    Create payment histograms:
    1. Histograms of TOTPAY (full and lower tail)
    2. Histograms of m (full and lower tail)
    """
    if df is None or len(df) == 0:
        print(f"[{year}] WARNING: No data for payment analysis")
        return
    
    # Check required columns
    required_cols = ['PAY', 'DEDUCT', 'COINS', 'COPAY', 'WORK_RVU', 'MP_RVU']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"[{year}] WARNING: Missing columns: {missing_cols}")
        return
    
    print(f"\n[{year}] Creating payment histograms...")
    
    # Calculate TOTPAY and m
    df_analysis = df.copy()
    df_analysis['TOTPAY'] = df_analysis['PAY'] + df_analysis['DEDUCT'] + df_analysis['COINS'] + df_analysis['COPAY']
    
    conversion_rate = CONVERSION_RATES[int(year)]
    df_analysis['m'] = (df_analysis['WORK_RVU'] * conversion_rate) + (df_analysis['MP_RVU'] * conversion_rate)
    
    # Remove missing values
    df_analysis = df_analysis.dropna(subset=['TOTPAY', 'm'])
    
    if len(df_analysis) == 0:
        print(f"[{year}] WARNING: No valid payment data after removing missing values")
        return
    
    print(f"  [{year}] Episodes with complete payment data: {len(df_analysis):,}")
    
    # Calculate percentiles
    p95_totpay = np.percentile(df_analysis['TOTPAY'], 95)
    p10_totpay = np.percentile(df_analysis['TOTPAY'], 10)
    p95_m = np.percentile(df_analysis['m'], 95)
    p10_m = np.percentile(df_analysis['m'], 10)
    
    # ===== FIGURE 1: Histograms of TOTPAY =====
    fig1, axes1 = plt.subplots(1, 2, figsize=(14, 6))
    
    # Panel A: Full distribution (0-95th percentile)
    totpay_95 = df_analysis[df_analysis['TOTPAY'] <= p95_totpay]['TOTPAY']
    axes1[0].hist(totpay_95, bins=100, edgecolor='black', alpha=0.7, color='steelblue')
    axes1[0].set_xlabel('TOTPAY per Episode ($)', fontsize=11)
    axes1[0].set_ylabel('Frequency', fontsize=11)
    axes1[0].set_title(f'{year}: TOTPAY Distribution (0-95th percentile)\nn={len(totpay_95):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes1[0].axvline(np.median(totpay_95), color='red', linestyle='--', linewidth=2,
                     label=f'Median: ${np.median(totpay_95):,.2f}')
    axes1[0].legend()
    axes1[0].grid(True, alpha=0.3)
    
    # Panel B: Lower tail (0-10th percentile)
    totpay_10 = df_analysis[df_analysis['TOTPAY'] <= p10_totpay]['TOTPAY']
    axes1[1].hist(totpay_10, bins=50, edgecolor='black', alpha=0.7, color='coral')
    axes1[1].set_xlabel('TOTPAY per Episode ($)', fontsize=11)
    axes1[1].set_ylabel('Frequency', fontsize=11)
    axes1[1].set_title(f'{year}: TOTPAY - Lower Tail (0-10th percentile)\nn={len(totpay_10):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes1[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    totpay_hist_file = os.path.join(output_dir, f'totpay_histogram_{year}.png')
    plt.savefig(totpay_hist_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ TOTPAY histograms saved to: {totpay_hist_file}")
    
    # ===== FIGURE 2: Histograms of m =====
    fig2, axes2 = plt.subplots(1, 2, figsize=(14, 6))
    
    # Panel A: Full distribution (0-95th percentile)
    m_95 = df_analysis[df_analysis['m'] <= p95_m]['m']
    axes2[0].hist(m_95, bins=100, edgecolor='black', alpha=0.7, color='purple')
    axes2[0].set_xlabel('Medicare-based Estimate (m) per Episode ($)', fontsize=11)
    axes2[0].set_ylabel('Frequency', fontsize=11)
    axes2[0].set_title(f'{year}: Medicare Estimate (m) Distribution (0-95th percentile)\nn={len(m_95):,} episodes', 
                       fontsize=12, fontweight='bold')
    axes2[0].axvline(np.median(m_95), color='red', linestyle='--', linewidth=2,
                     label=f'Median: ${np.median(m_95):,.2f}')
    axes2[0].legend()
    axes2[0].grid(True, alpha=0.3)
    
    # Panel B: Lower tail (0-10th percentile)
    m_10 = df_analysis[df_analysis['m'] <= p10_m]['m']
    axes2[1].hist(m_10, bins=50, edgecolor='black', alpha=0.7, color='orange')
    axes2[1].set_xlabel('Medicare-based Estimate (m) per Episode ($)', fontsize=11)
    axes2[1].set_ylabel('Frequency', fontsize=11)
    axes2[1].set_title(f'{year}: Medicare Estimate (m) - Lower Tail (0-10th percentile)\nn={len(m_10):,} episodes', 
                       fontsize=12, fontweight='bold')
    
    min_m = m_10.min()
    axes2[1].axvline(min_m, color='red', linestyle='--', linewidth=2,
                     label=f'Min: ${min_m:.2f}')
    axes2[1].legend()
    axes2[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    m_hist_file = os.path.join(output_dir, f'medicare_estimate_histogram_{year}.png')
    plt.savefig(m_hist_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Medicare estimate histograms saved to: {m_hist_file}")
    
    # ===== STATISTICS =====
    print(f"\n[{year}] PAYMENT STATISTICS:")
    print(f"  Mean TOTPAY: ${df_analysis['TOTPAY'].mean():,.2f}")
    print(f"  Median TOTPAY: ${np.median(df_analysis['TOTPAY']):,.2f}")
    print(f"  Mean Medicare estimate (m): ${df_analysis['m'].mean():,.2f}")
    print(f"  Median Medicare estimate (m): ${np.median(df_analysis['m']):,.2f}")
    
    # Save summary statistics
    summary_stats = pd.DataFrame({
        'year': [year],
        'n_episodes': [len(df_analysis)],
        'mean_totpay': [df_analysis['TOTPAY'].mean()],
        'median_totpay': [np.median(df_analysis['TOTPAY'])],
        'mean_m': [df_analysis['m'].mean()],
        'median_m': [np.median(df_analysis['m'])],
        'conversion_rate': [conversion_rate]
    })
    
    summary_file = os.path.join(output_dir, f'payment_summary_stats_{year}.csv')
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
        episode_file = os.path.join(input_dir, f'episodes_level1_{year}{sample_suffix}.csv')
        
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
            summary = create_payment_analysis(df, year)
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
        combined_file = os.path.join(output_dir, f'payment_summary_stats_all_years.csv')
        combined_summary.to_csv(combined_file, index=False)
        
        print(f"\n✓ Combined summary statistics saved to: {combined_file}")
        
        # Print summary table
        print("\n" + "="*100)
        print("SUMMARY TABLE - PAYMENT STATISTICS ACROSS YEARS")
        print("="*100)
        print(combined_summary[['year', 'n_episodes', 'mean_totpay', 'mean_m']].to_string(index=False))
    
    print(f"\n{'='*100}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*100}")
    print(f"Output directory: {output_dir}")
    print(f"\nFiles generated per year:")
    print(f"  ✓ totpay_histogram_YYYY.png (TOTPAY distribution: full & lower tail)")
    print(f"  ✓ medicare_estimate_histogram_YYYY.png (m distribution: full & lower tail)")
    print(f"  ✓ payment_summary_stats_YYYY.csv (summary statistics)")
    print(f"\nCombined file:")
    print(f"  ✓ payment_summary_stats_all_years.csv")
    print("="*100)
