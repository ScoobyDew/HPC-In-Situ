import time
import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd
import logging
import matplotlib.patches as mpatches

# Configure logging
logging.basicConfig(level=logging.INFO, filename='NSEWPlotting.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def interpolate_and_average(dfs):
    # Define a common set of bin midpoints for interpolation
    common_bins = np.linspace(min(df['bin_mid'].min() for df in dfs if not df.empty),
                              max(df['bin_mid'].max() for df in dfs if not df.empty), 100)
    interpolated_means = []
    for df in dfs:
        if not df.empty:
            interpolated_mean = np.interp(common_bins, df['bin_mid'], df['mean'], left=np.nan, right=np.nan)
            interpolated_means.append(interpolated_mean)
    # Average across all interpolated means
    mean_values = np.nanmean(interpolated_means, axis=0)
    return common_bins, mean_values

def read_and_plot(directory):
    logging.info(f'Reading data from {directory}')
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))
    quadrants = ['North', 'South', 'East', 'West']
    colors = {'North': 'red', 'South': 'blue', 'East': 'green', 'West': 'orange'}

    quadrant_data = {quad: [] for quad in quadrants}

    all_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    for file in all_files:
        quadrant = file.split('_')[0]
        if quadrant in quadrants:
            df = pd.read_csv(os.path.join(directory, file))
            quadrant_data[quadrant].append(df)

    logging.info('Read data from CSV files')

    # Create patches for the legend based on quadrant colors
    patches = [mpatches.Patch(color=color, label=quadrant) for quadrant, color in colors.items()]

    # Plot North and South with normalization and average
    for quadrant in ['North', 'South']:
        dfs = quadrant_data[quadrant]
        common_bins, avg_values = interpolate_and_average(dfs)
        normalized_avg_values = (avg_values - avg_values.min()) / (avg_values.max() - avg_values.min())
        axs[0].plot(common_bins, normalized_avg_values, color='k', linewidth=2)
        for df in dfs:
            normalized_values = (df['mean'] - df['mean'].min()) / (df['mean'].max() - df['mean'].min())
            axs[0].plot(df['bin_mid'], normalized_values, color=colors[quadrant], alpha=0.2)
        axs[0].set_title('North/South Quadrants')
        axs[0].set_xlabel('Distance')
        axs[0].set_ylabel('Normalized Mean Value of Pyro2')

    axs[0].legend(handles=patches[:2])  # North and South patches

    # Plot East and West with normalization and average
    for quadrant in ['East', 'West']:
        dfs = quadrant_data[quadrant]
        common_bins, avg_values = interpolate_and_average(dfs)
        normalized_avg_values = (avg_values - avg_values.min()) / (avg_values.max() - avg_values.min())
        axs[1].plot(common_bins, normalized_avg_values, color='k', linewidth=2)
        for df in dfs:
            normalized_values = (df['mean'] - df['mean'].min()) / (df['mean'].max() - df['mean'].min())
            axs[1].plot(df['bin_mid'], normalized_values, color=colors[quadrant], alpha=0.2)
        axs[1].set_title('East/West Quadrants')
        axs[1].set_xlabel('Distance')

    axs[1].legend(handles=patches[2:])  # East and West patches

    plt.tight_layout()

    # Save the plot to a file with a timestamp
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    plt.savefig(f'NSEWPlot_{timestamp}.png')
    logging.info(f'Plot saved as NSEWPlot_{timestamp}.png')

if __name__ == '__main__':
    directory = '/users/eia19od/in_situ/HPC-In-Situ/NSEW/binned_data'
    read_and_plot(directory)
