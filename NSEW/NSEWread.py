import time

import matplotlib.pyplot as plt
import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, filename='NSEWPlotting.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_and_plot(directory):
    logging.info(f"Reading and plotting data from {directory}")

    fig, axs = plt.subplots(1, 2, figsize=(12, 6))  # Two subplots on one row
    quadrants = ['North', 'South', 'East', 'West']
    colors = {'North': 'red', 'South': 'blue', 'East': 'green', 'West': 'orange'}  # Unique colors for each quadrant

    # Dictionary to organize data by quadrant
    quadrant_data = {quad: [] for quad in quadrants}

    # Load all CSV files and sort them into the dictionary by quadrant
    all_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    for file in all_files:
        quadrant = file.split('_')[0]  # Extract the quadrant from the filename
        if quadrant in quadrants:
            df = pd.read_csv(os.path.join(directory, file))
            quadrant_data[quadrant].append(df)

    logging.info(f"Loaded {len(all_files)} files from {directory}")

    # Plotting North and South on the first subplot with normalization
    for quadrant in ['North', 'South']:
        for df in quadrant_data[quadrant]:
            normalized_values = (df['mean'] - df['mean'].min()) / (df['mean'].max() - df['mean'].min())
            axs[0].plot(df['bin_mid'], normalized_values, label=f"{quadrant} File", color=colors[quadrant])
        axs[0].set_title('North/South Quadrants')
        axs[0].set_xlabel('Distance')
        axs[0].set_ylabel('Normalized Mean Value of Pyro2')
        axs[0].legend()

    # Plotting East and West on the second subplot with normalization
    for quadrant in ['East', 'West']:
        for df in quadrant_data[quadrant]:
            normalized_values = (df['mean'] - df['mean'].min()) / (df['mean'].max() - df['mean'].min())
            axs[1].plot(df['bin_mid'], normalized_values, label=f"{quadrant} File", color=colors[quadrant])
        axs[1].set_title('East/West Quadrants')
        axs[1].set_xlabel('Distance')
        axs[1].legend()

    plt.tight_layout()

    #
    date = time.strftime('%Y-%m-%d_%H-%M-%S')
    plt.savefig(f'binned_data_plot.png')
    logging.info(f"Plot saved as binned_data_plot.png at {date}")

# Example usage assuming the binned data is stored in the 'binned_data' directory
read_and_plot('binned_data')

if __name__ == '__main__':
    directory = '/users/eia19od/in_situ/HPC-In-Situ/NSEW/binned_data'
    read_and_plot(directory)