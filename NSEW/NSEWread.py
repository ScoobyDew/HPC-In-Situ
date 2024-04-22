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
        if 'instantaneous_distance' in df.columns and quadrant in quadrants:
            quadrant_data[quadrant].append(df)
        else:
            logging.warning(f"Missing 'instantaneous_distance' in {file} or incorrect quadrant {quadrant}")

    logging.info('Data loaded and organized by quadrant')

    for quadrant, color in colors.items():
        for df in quadrant_data[quadrant]:
            bins = pd.cut(df['instantaneous_distance'], bins=20, include_lowest=True, right=True)
            df['bin'] = bins
            df['bin_mid'] = df['bin'].apply(lambda b: b.mid if not pd.isna(b) else np.nan)
            grouped = df.groupby('bin_mid').agg({'pyro2': ['mean', 'std']}).reset_index()
            grouped.columns = ['bin_mid', 'mean', 'std']
            quadrant_data[quadrant] = grouped
            logging.info(f"Processed data for {quadrant}: {grouped.head()}")  # Log some of the processed data

    # Plot the average values for each quadrant
    for quadrant_group, ax in [('North', 'South'), ('East', 'West')]:
        for quadrant in quadrant_group:
            df = quadrant_data[quadrant]
            if not df.empty:
                normalized_values = (df['mean'] - df['mean'].min()) / (df['mean'].max() - df['mean'].min())
                ax.plot(df['bin_mid'], normalized_values, color=colors[quadrant], label=f"{quadrant} Average", alpha=0.7)
            else:
                logging.warning(f"No data to plot for {quadrant}")

        ax.set_title(f'{quadrant_group} Quadrants')
        ax.set_xlabel('Distance')
        ax.set_ylabel('Normalized Mean Value of Pyro2')
        ax.legend()

    plt.tight_layout()
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    plt.savefig(f'NSEWavg_{timestamp}.png')
    logging.info(f'Plot saved as NSEWavg_{timestamp}.png')

if __name__ == '__main__':
    directory = '/users/eia19od/in_situ/HPC-In-Situ/NSEW/binned_data'
    read_and_plot(directory)
