import time

import matplotlib.pyplot as plt
import os
import pandas as pd
import logging
import matplotlib.patches as mpatches

# Configure logging
logging.basicConfig(level=logging.INFO, filename='NSEWPlotting.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_and_plot(directory):
    logging.info(f'Reading data from {directory}')
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))
    quadrants = ['North', 'South', 'East', 'West']
    colors = {'North': 'red', 'South': 'blue', 'East': 'green', 'West': 'orange'}

    quadrant_data = {quad: [] for quad in quadrants}

    all_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    for file in all_files:
        quadrant = file.split('_')[0]
        df = pd.read_csv(os.path.join(directory, file))
        if df.empty or 'instantaneous_distance' not in df.columns:
            logging.warning(f"File {file} does not contain 'instantaneous_distance'. Skipping...")
            continue  # Skip files that do not contain the required column
        if quadrant in quadrants:
            quadrant_data[quadrant].append(df)

    logging.info('Data loaded and organized by quadrant')

    # Bin and plot data for each quadrant
    for quadrant, color in colors.items():
        for df in quadrant_data[quadrant]:
            try:
                bins = pd.cut(df['instantaneous_distance'], bins=20, include_lowest=True, right=True)
                df['bin'] = bins
                df['bin_mid'] = df['bin'].apply(lambda b: b.mid if not pd.isna(b) else np.nan)
                grouped = df.groupby('bin', as_index=False).agg({'pyro2': ['mean', 'std']})
                grouped.columns = ['bin', 'mean', 'std']  # Simplify multi-level columns
                grouped['bin_mid'] = grouped['bin'].apply(lambda b: b.mid)
                quadrant_data[quadrant] = grouped
            except KeyError as e:
                logging.error(f"Error processing {quadrant} data: {e}")
                continue

    logging.info('Binned data for each quadrant prepared')

    # Plot the average values for each quadrant
    plt.figure(figsize=(12, 6))

    plt.subplot(1, 2, 1)
    for quadrant in ['North', 'South']:
        for df in quadrant_data[quadrant]:
            normalized_values = (df['mean'] - df['mean'].min()) / (df['mean'].max() - df['mean'].min())
            plt.plot(df['bin_mid'], normalized_values, color=colors[quadrant], alpha=0.2)
        plt.title('North/South Quadrants')
        plt.xlabel('Distance')
        plt.ylabel('Normalized Mean Value of Pyro2')

    plt.legend(handles=[mpatches.Patch(color=color, label=quadrant) for quadrant, color in colors.items()][:2])

    plt.subplot(1, 2, 2)

    for quadrant in ['East', 'West']:
        for df in quadrant_data[quadrant]:
            normalized_values = (df['mean'] - df['mean'].min()) / (df['mean'].max() - df['mean'].min())
            plt.plot(df['bin_mid'], normalized_values, color=colors[quadrant], alpha=0.2)
        plt.title('East/West Quadrants')
        plt.xlabel('Distance')

    plt.legend(handles=[mpatches.Patch(color=color, label=quadrant) for quadrant, color in colors.items()][2:])

    plt.tight_layout()
    # Save the plot to a file with a timestamp
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    plt.savefig(f'NSEWavg_{timestamp}.png')

    # Create patches for the legend based on quadrant colors
    patches = [mpatches.Patch(color=color, label=quadrant) for quadrant, color in colors.items()]

    # Plot North and South with normalization
    for quadrant in ['North', 'South']:
        for df in quadrant_data[quadrant]:
            normalized_values = (df['mean'] - df['mean'].min()) / (df['mean'].max() - df['mean'].min())
            axs[0].plot(df['bin_mid'], normalized_values, color=colors[quadrant], alpha=0.2)
        axs[0].set_title('North/South Quadrants')
        axs[0].set_xlabel('Distance')
        axs[0].set_ylabel('Normalized Mean Value of Pyro2')

    axs[0].legend(handles=patches[:2])  # North and South patches
    # Set xlim to 7500 for both plots
    axs[0].set_xlim(0, 7500)

    # Plot East and West with normalization
    for quadrant in ['East', 'West']:
        for df in quadrant_data[quadrant]:
            normalized_values = (df['mean'] - df['mean'].min()) / (df['mean'].max() - df['mean'].min())
            axs[1].plot(df['bin_mid'], normalized_values, color=colors[quadrant], alpha=0.2)
        axs[1].set_title('East/West Quadrants')
        axs[1].set_xlabel('Distance')

    logging.info('Plotting North/South and East/West quadrants')

    axs[1].legend(handles=patches[2:])  # East and West patches
    # Set xlim to 7500 for both plots
    axs[1].set_xlim(0, 7500)

    plt.tight_layout()

    # Save the plot to a file with a timestamp
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    plt.savefig(f'NSEWPlot_{timestamp}.png')
    logging.info(f'Plot saved as NSEWPlot_{timestamp}.png')



if __name__ == '__main__':
    directory = '/users/eia19od/in_situ/HPC-In-Situ/NSEW/binned_data'
    read_and_plot(directory)