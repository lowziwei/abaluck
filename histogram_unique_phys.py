import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
from pathlib import Path

# Configuration
OUTPUT_DIR = "histogram_results"

def create_combined_histogram():
    """
    Create combined histograms showing count and percentage of unique phys_id
    """
    print("Creating combined histogram from all processed files...")
    
    # Find all histogram files
    histogram_pattern = os.path.join(OUTPUT_DIR, "unique_phys_*.parquet")
    histogram_files = glob.glob(histogram_pattern)
    
    if not histogram_files:
        print(f"❌ No histogram files found in {OUTPUT_DIR}")
        return
    
    print(f"Found {len(histogram_files)} histogram files")
    
    # Load and combine all data
    all_histograms = []
    for file_path in histogram_files:
        df = pd.read_parquet(file_path)
        all_histograms.append(df)
    
    combined_df = pd.concat(all_histograms, ignore_index=True)
    
    # Aggregate by unique_phys across all years
    aggregated = combined_df.groupby('unique_phys')['prescription_events'].sum().reset_index()
    
    # Calculate percentages
    total_events = aggregated['prescription_events'].sum()
    aggregated['percentage'] = (aggregated['prescription_events'] / total_events * 100).round(2)
    
    # Sort by physician count
    aggregated = aggregated.sort_values('unique_phys').reset_index(drop=True)
    
    print(f"Total prescription events: {total_events:,}")
    
    # Create the two histograms
    plot_data = aggregated.head(12)  # Show first 12 categories
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Histogram 1: Count
    bars1 = ax1.bar(plot_data['unique_phys'], plot_data['prescription_events'], 
                   color='skyblue', edgecolor='navy', alpha=0.8)
    ax1.set_xlabel('Number of Unique Physicians per Prescription')
    ax1.set_ylabel('Number of Prescription Events')
    ax1.set_title('Count of Prescription Events by Physician Count')
    ax1.grid(True, alpha=0.3)
    
    # Add value labels
    for bar, value in zip(bars1, plot_data['prescription_events']):
        height = bar.get_height()
        if value >= 1000000:
            label = f'{value/1000000:.1f}M'
        elif value >= 1000:
            label = f'{value/1000:.0f}K'
        else:
            label = f'{value:,.0f}'
        ax1.text(bar.get_x() + bar.get_width()/2., height + total_events * 0.01,
                label, ha='center', va='bottom', fontsize=9)
    
    # Histogram 2: Percentage
    bars2 = ax2.bar(plot_data['unique_phys'], plot_data['percentage'], 
                   color='lightcoral', edgecolor='darkred', alpha=0.8)
    ax2.set_xlabel('Number of Unique Physicians per Prescription')
    ax2.set_ylabel('Percentage of Prescription Events')
    ax2.set_title('Percentage Distribution by Physician Count')
    ax2.grid(True, alpha=0.3)
    
    # Add percentage labels
    for bar, value in zip(bars2, plot_data['percentage']):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + max(plot_data['percentage']) * 0.02,
                f'{value:.1f}%', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    
    # Save the plot
    years = combined_df['year'].unique()
    plot_file = os.path.join(OUTPUT_DIR, f'combined_histogram_{min(years)}_{max(years)}.png')
    plt.savefig(plot_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {os.path.basename(plot_file)}")
    
    plt.show()

def main():
    if not Path(OUTPUT_DIR).exists():
        print(f"❌ Directory not found: {OUTPUT_DIR}")
        return
    
    create_combined_histogram()
    print("✅ Done!")

if __name__ == "__main__":
    main()
