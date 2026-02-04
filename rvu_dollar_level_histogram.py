import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Settings
output_dir = os.path.expanduser('~/episode_outputs')
SAMPLE_SIZE = 15000

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

# Load all episode files
all_episodes = []

for year in range(2014, 2025):
    sample_suffix = f"_sample{SAMPLE_SIZE}"
    level1_file = os.path.join(output_dir, f'episodes_level1_{year}{sample_suffix}.csv')
    
    if os.path.exists(level1_file):
        print(f"\nLoading {year}...")
        df = pd.read_csv(level1_file)
        df['year'] = year
        
        # Calculate TOTPAY
        df['TOTPAY'] = df['PAY'] + df['DEDUCT'] + df['COINS'] + df['COPAY']
        
        # Calculate Medicare-based payment estimate (m)
        conversion_rate = CONVERSION_RATES[year]
        df['m'] = (df['WORK_RVU'] * conversion_rate) + (df['MP_RVU'] * conversion_rate)
        
        print(f"  Loaded {len(df):,} episodes")
        print(f"  TOTPAY - Mean: ${df['TOTPAY'].mean():,.2f}, Median: ${df['TOTPAY'].median():,.2f}")
        print(f"  m      - Mean: ${df['m'].mean():,.2f}, Median: ${df['m'].median():,.2f}")
        
        all_episodes.append(df)
    else:
        print(f"\nWARNING: File not found: {level1_file}")

if len(all_episodes) == 0:
    print("\nERROR: No episode files found!")
    exit(1)

# Combine all years
combined_df = pd.concat(all_episodes, ignore_index=True)
print(f"\n{'='*100}")
print(f"COMBINED DATASET: {len(combined_df):,} total episodes across {len(all_episodes)} years")
print("="*100)

# Remove extreme outliers for visualization (keep 99th percentile)
totpay_99 = combined_df['TOTPAY'].quantile(0.99)
m_99 = combined_df['m'].quantile(0.99)
max_value = max(totpay_99, m_99)

print(f"\nDistribution statistics:")
print(f"  TOTPAY - Mean: ${combined_df['TOTPAY'].mean():,.2f}, Median: ${combined_df['TOTPAY'].median():,.2f}, 99th pct: ${totpay_99:,.2f}")
print(f"  m      - Mean: ${combined_df['m'].mean():,.2f}, Median: ${combined_df['m'].median():,.2f}, 99th pct: ${m_99:,.2f}")

# Filter for visualization
viz_df = combined_df[(combined_df['TOTPAY'] <= max_value) & (combined_df['m'] <= max_value)].copy()
print(f"\nUsing {len(viz_df):,} episodes for visualization (capped at 99th percentile: ${max_value:,.2f})")

# Create figure with multiple subplots
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Payment Distribution Comparison: TOTPAY vs Medicare-based Estimate (m)', 
             fontsize=16, fontweight='bold', y=0.995)

# Color scheme
color_totpay = '#2E86AB'  # Blue
color_m = '#A23B72'        # Purple

# 1. Overlaid histograms
ax1 = axes[0, 0]
bins = np.linspace(0, max_value, 50)

ax1.hist(viz_df['TOTPAY'], bins=bins, alpha=0.6, label='TOTPAY', color=color_totpay, edgecolor='black', linewidth=0.5)
ax1.hist(viz_df['m'], bins=bins, alpha=0.6, label='Medicare estimate (m)', color=color_m, edgecolor='black', linewidth=0.5)

ax1.set_xlabel('Payment Amount ($)', fontsize=11, fontweight='bold')
ax1.set_ylabel('Frequency', fontsize=11, fontweight='bold')
ax1.set_title('A. Overlaid Histograms', fontsize=12, fontweight='bold', pad=10)
ax1.legend(fontsize=10)
ax1.grid(True, alpha=0.3)
ax1.ticklabel_format(style='plain', axis='x')

# Add summary stats to plot
totpay_mean = viz_df['TOTPAY'].mean()
m_mean = viz_df['m'].mean()
ax1.axvline(totpay_mean, color=color_totpay, linestyle='--', linewidth=2, alpha=0.8, label=f'TOTPAY mean: ${totpay_mean:,.0f}')
ax1.axvline(m_mean, color=color_m, linestyle='--', linewidth=2, alpha=0.8, label=f'm mean: ${m_mean:,.0f}')

# 2. Side-by-side boxplots
ax2 = axes[0, 1]
box_data = [viz_df['TOTPAY'], viz_df['m']]
bp = ax2.boxplot(box_data, labels=['TOTPAY', 'Medicare\nestimate (m)'], 
                 patch_artist=True, widths=0.6, showfliers=False)

# Color the boxes
for patch, color in zip(bp['boxes'], [color_totpay, color_m]):
    patch.set_facecolor(color)
    patch.set_alpha(0.6)

ax2.set_ylabel('Payment Amount ($)', fontsize=11, fontweight='bold')
ax2.set_title('B. Distribution Comparison (Box Plots)', fontsize=12, fontweight='bold', pad=10)
ax2.grid(True, alpha=0.3, axis='y')
ax2.ticklabel_format(style='plain', axis='y')

# 3. Scatter plot (TOTPAY vs m)
ax3 = axes[1, 0]
# Sample for scatter plot if too many points
if len(viz_df) > 10000:
    scatter_df = viz_df.sample(n=10000, random_state=42)
else:
    scatter_df = viz_df

ax3.scatter(scatter_df['m'], scatter_df['TOTPAY'], alpha=0.3, s=10, color='#06A77D')
ax3.plot([0, max_value], [0, max_value], 'r--', linewidth=2, alpha=0.7, label='Perfect agreement')

ax3.set_xlabel('Medicare estimate (m) ($)', fontsize=11, fontweight='bold')
ax3.set_ylabel('TOTPAY ($)', fontsize=11, fontweight='bold')
ax3.set_title('C. TOTPAY vs Medicare Estimate', fontsize=12, fontweight='bold', pad=10)
ax3.legend(fontsize=10)
ax3.grid(True, alpha=0.3)
ax3.ticklabel_format(style='plain')

# Calculate correlation
correlation = viz_df['TOTPAY'].corr(viz_df['m'])
ax3.text(0.05, 0.95, f'Correlation: {correlation:.3f}', 
         transform=ax3.transAxes, fontsize=10, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

# 4. Distribution by year
ax4 = axes[1, 1]
years = sorted(viz_df['year'].unique())
totpay_by_year = [viz_df[viz_df['year']==y]['TOTPAY'].mean() for y in years]
m_by_year = [viz_df[viz_df['year']==y]['m'].mean() for y in years]

x = np.arange(len(years))
width = 0.35

bars1 = ax4.bar(x - width/2, totpay_by_year, width, label='TOTPAY', color=color_totpay, alpha=0.8)
bars2 = ax4.bar(x + width/2, m_by_year, width, label='Medicare estimate (m)', color=color_m, alpha=0.8)

ax4.set_xlabel('Year', fontsize=11, fontweight='bold')
ax4.set_ylabel('Mean Payment Amount ($)', fontsize=11, fontweight='bold')
ax4.set_title('D. Mean Payments by Year', fontsize=12, fontweight='bold', pad=10)
ax4.set_xticks(x)
ax4.set_xticklabels(years, rotation=45)
ax4.legend(fontsize=10)
ax4.grid(True, alpha=0.3, axis='y')
ax4.ticklabel_format(style='plain', axis='y')

plt.tight_layout()

# Save figure
output_file = os.path.join(output_dir, f'payment_comparison_histograms_sample{SAMPLE_SIZE}.png')
plt.savefig(output_file, dpi=300, bbox_inches='tight')
print(f"\n✓ Histogram saved to: {output_file}")

# Create additional detailed comparison plot
fig2, axes2 = plt.subplots(1, 2, figsize=(16, 6))
fig2.suptitle('Detailed Payment Distribution Analysis', fontsize=16, fontweight='bold')

# Left: Kernel Density Estimation
ax_left = axes2[0]
from scipy import stats

totpay_kde = stats.gaussian_kde(viz_df['TOTPAY'])
m_kde = stats.gaussian_kde(viz_df['m'])

x_range = np.linspace(0, max_value, 1000)
totpay_density = totpay_kde(x_range)
m_density = m_kde(x_range)

ax_left.plot(x_range, totpay_density, label='TOTPAY', color=color_totpay, linewidth=2)
ax_left.plot(x_range, m_density, label='Medicare estimate (m)', color=color_m, linewidth=2)
ax_left.fill_between(x_range, totpay_density, alpha=0.3, color=color_totpay)
ax_left.fill_between(x_range, m_density, alpha=0.3, color=color_m)

ax_left.set_xlabel('Payment Amount ($)', fontsize=11, fontweight='bold')
ax_left.set_ylabel('Density', fontsize=11, fontweight='bold')
ax_left.set_title('Kernel Density Estimation', fontsize=12, fontweight='bold', pad=10)
ax_left.legend(fontsize=10)
ax_left.grid(True, alpha=0.3)
ax_left.ticklabel_format(style='plain', axis='x')

# Right: Difference distribution
ax_right = axes2[1]
viz_df['difference'] = viz_df['TOTPAY'] - viz_df['m']
difference_median = viz_df['difference'].median()
difference_mean = viz_df['difference'].mean()

ax_right.hist(viz_df['difference'], bins=50, color='#F18F01', alpha=0.7, edgecolor='black', linewidth=0.5)
ax_right.axvline(0, color='red', linestyle='-', linewidth=2, label='Zero difference', alpha=0.7)
ax_right.axvline(difference_mean, color='darkblue', linestyle='--', linewidth=2, 
                 label=f'Mean diff: ${difference_mean:,.0f}', alpha=0.7)
ax_right.axvline(difference_median, color='darkgreen', linestyle='--', linewidth=2, 
                 label=f'Median diff: ${difference_median:,.0f}', alpha=0.7)

ax_right.set_xlabel('Difference (TOTPAY - m) ($)', fontsize=11, fontweight='bold')
ax_right.set_ylabel('Frequency', fontsize=11, fontweight='bold')
ax_right.set_title('Distribution of Payment Differences', fontsize=12, fontweight='bold', pad=10)
ax_right.legend(fontsize=10)
ax_right.grid(True, alpha=0.3)
ax_right.ticklabel_format(style='plain', axis='x')

plt.tight_layout()

output_file2 = os.path.join(output_dir, f'payment_difference_analysis_sample{SAMPLE_SIZE}.png')
plt.savefig(output_file2, dpi=300, bbox_inches='tight')
print(f"✓ Difference analysis saved to: {output_file2}")

# Print summary statistics
print(f"\n{'='*100}")
print("SUMMARY STATISTICS")
print("="*100)

summary_stats = pd.DataFrame({
    'Metric': ['Mean', 'Median', 'Std Dev', '25th Percentile', '75th Percentile', 'Min', 'Max'],
    'TOTPAY': [
        combined_df['TOTPAY'].mean(),
        combined_df['TOTPAY'].median(),
        combined_df['TOTPAY'].std(),
        combined_df['TOTPAY'].quantile(0.25),
        combined_df['TOTPAY'].quantile(0.75),
        combined_df['TOTPAY'].min(),
        combined_df['TOTPAY'].max()
    ],
    'Medicare estimate (m)': [
        combined_df['m'].mean(),
        combined_df['m'].median(),
        combined_df['m'].std(),
        combined_df['m'].quantile(0.25),
        combined_df['m'].quantile(0.75),
        combined_df['m'].min(),
        combined_df['m'].max()
    ],
    'Difference (TOTPAY - m)': [
        combined_df['TOTPAY'].mean() - combined_df['m'].mean(),
        (combined_df['TOTPAY'] - combined_df['m']).median(),
        (combined_df['TOTPAY'] - combined_df['m']).std(),
        (combined_df['TOTPAY'] - combined_df['m']).quantile(0.25),
        (combined_df['TOTPAY'] - combined_df['m']).quantile(0.75),
        (combined_df['TOTPAY'] - combined_df['m']).min(),
        (combined_df['TOTPAY'] - combined_df['m']).max()
    ]
})

# Format as currency
for col in ['TOTPAY', 'Medicare estimate (m)', 'Difference (TOTPAY - m)']:
    summary_stats[col] = summary_stats[col].apply(lambda x: f'${x:,.2f}')

print(summary_stats.to_string(index=False))

print(f"\n✓ Analysis complete!")
print(f"✓ Total episodes analyzed: {len(combined_df):,}")
print(f"✓ Correlation between TOTPAY and m: {correlation:.3f}")
