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
    # Filter out empty dataframes to avoid errors in interpolation
    dfs = [df for df in dfs if not df.empty]
    if not dfs:
        logging.warning("No data available for averaging.")
        return np.array([]), np.array([])  # Return empty arrays if no data frames are available

    # Define a common set of bin midpoints for interpolation, considering all available data
    common_bins = np.linspace(min(df['bin_mid'].min() for df in dfs),
                              max(df['bin_mid'].max() for df in dfs), 100)
    interpolated_means = []

    for df in dfs:
        # Ensure 'bin_mid' and 'mean' are sorted in ascending order
        df.sort_values('bin_mid', inplace=True)
        interpolated_mean = np.interp(common_bins, df['bin_mid'], df['mean'], left=np.nan, right=np.nan)
        interpolated_means.append(interpolated_mean)

    # Average across all interpolated means, ignoring NaNs
    mean_values = np.nanmean(np.array(interpolated_means), axis=0)
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
        else:
            logging.warning(f"Unexpected quadrant name '{quadrant}' in file {file}")

    logging.info('Data loaded and organized by quadrant')

    # Create patches for the legend based on quadrant colors
    patches = [mpatches.Patch(color=color, label=quadrant) for quadrant, color in colors.items()]

    # Interpolate and average for each quadrant and plot
    for quadrant_group, ax in [('North', 'South'), ('East', 'West')]:
        for quadrant in quadrant_group:
            try:
                common_bins, avg_values = interpolate_and_average(quadrant_data[quadrant])
                if common_bins.size > 0:  # Ensure there's data to plot
                    normalized_avg_values = (avg_values - np.nanmin(avg_values)) / (np.nanmax(avg_values) - np.nanmin(avg_values))
                    ax.plot(common_bins, normalized_avg_values, color=colors[quadrant], linewidth=2, label=f"Avg {quadrant}")
            except KeyError:
                logging.error(f"No data for quadrant {quadrant}. This might be due to missing files.")
                continue  # Skip this quadrant if data is missing
            # Plot individual normalized traces
            for df in quadrant_data.get(quadrant, []):
                normalized_values = (df['mean'] - np.nanmin(df['mean'])) / (np.nanmax(df['mean']) - np.nanmin(df['mean']))
                ax.plot(df['bin_mid'], normalized_values, color=colors[quadrant], alpha=0.2)
            ax.set_title(f'{quadrant_group} Quadrants')
            ax.set_xlabel('Distance')
            ax.set_ylabel('Normalized Mean Value of Pyro2')
            ax.legend()

    plt.tight_layout()
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    plt.savefig(f'NSEWPlot_{timestamp}.png')
    logging.info(f'Plot saved as NSEWPlot_{timestamp}.png')

if __name__ == '__main__':
    directory = '/users/eia19od/in_situ/HPC-In-Situ/NSEW/binned_data'
    read_and_plot(directory)

