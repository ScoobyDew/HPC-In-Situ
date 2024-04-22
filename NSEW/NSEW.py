import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import dask.dataframe as dd
from dask import delayed
import time
import logging

logging.basicConfig(level=logging.INFO, filename='/users/eia19od/in_situ/HPC-In-Situ/NSEW/NSEW.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_random_files(n, exclude, max_val):
    file_numbers = [num for num in range(1, max_val + 1) if num not in exclude]

    # Randomly select n files from the list of file numbers
    np.random.seed(42)
    selected_files = np.random.choice(file_numbers, size=n, replace=False)
    return selected_files


def assign_quadrant(df):
    df['90angle'] = np.round(df['hatch_angle'] / 90) * 90

    def get_quadrant(angle):
        if angle == 0:
            return 'East'
        elif angle == 90:
            return 'North'
        elif angle == 180 or angle == -180:
            return 'West'
        elif angle == -90 or angle == 270:
            return 'South'

    df['quadrant'] = df['90angle'].apply(get_quadrant)
    return df

def plot_quadrant(dfs, quadrants, bins, signal, colors, x='instantaneous_distance'):
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))  # Adjust subplot layout to horizontal
    for quadrant, color, ax in zip(quadrants, colors, axs.flatten()):
        for idx, df in enumerate(dfs):
            quad_df = df[df['quadrant'] == quadrant].compute()  # Ensure Dask dataframe is fully computed
            # Convert categorical column to regular Pandas Series
            quad_df['bin'] = pd.cut(quad_df[x], bins=bins, include_lowest=True, right=True).astype('object')
            # Calculate bin midpoints directly from IntervalIndex
            bin_edges = quad_df['bin'].values
            midpoints = [(interval.left + interval.right) / 2 for interval in bin_edges]
            quad_df['bin_mid'] = midpoints
            grouped = quad_df.groupby('bin_mid', observed=True)[signal].agg(['mean', 'std']).reset_index()
            valid_mask = np.isfinite(grouped['mean']) & np.isfinite(grouped['std'])
            midpoints = grouped['bin_mid'][valid_mask]
            means = grouped['mean'][valid_mask]
            minmax_norm = (means - means.min()) / (means.max() - means.min())
            ax.plot(midpoints, minmax_norm, color=color, label=f'File {idx + 1} - {quadrant}', alpha=0.5)
        ax.set_title(f'{quadrant} Quadrant')
        ax.set_xlabel('x (mm)')
        ax.set_ylabel(f'$V_{{p}}\\prime$')
        ax.legend()
    plt.tight_layout()

    # Save the plot to directory if it does not exist
    if not os.path.exists('/mnt/parscratch/users/eia19od/Quadrants'):
        os.makedirs('/mnt/parscratch/users/eia19od/Quadrants')

    # Create a string with the current date and time
    end_time = time.time()
    date_time = time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime(end_time))
    plt.savefig(f'/mnt/parscratch/users/eia19od/Quadrants/Quadrants_{date_time}.png')


def main():
    time_start = time.time()
    directory = '/mnt/parscratch/users/eia19od/Cleaned'
    n_files = 25
    max_val = 124
    exclude = [1, 4, 64, 67]
    selected_files = get_random_files(n_files, exclude, max_val)
    dfs = []
    for file_number in selected_files:
        file_path = f'{directory}/{file_number}.parquet'
        if os.path.exists(file_path):
            df = dd.read_parquet(file_path)  # Using Dask to lazily load data
            df = df.map_partitions(assign_quadrant)  # Apply assign_quadrant function to each partition
            dfs.append(df)
            logging.info(f"Loaded {file_path}")
        else:
            logging.warning(f"File {file_path} does not exist.")
    if not dfs:
        logging.warning("No data files were loaded.")
        return

    bins = np.linspace(min(df['instantaneous_distance'].min().compute() for df in dfs),
                       max(df['instantaneous_distance'].max().compute() for df in dfs), 20)
    logging.info(f"Bins: {bins}")

    signal = 'pyro2'
    logging.info(f"Plotting quadrants.")
    plot_quadrant(dfs, ['North', 'South'], bins, signal, ['red', 'blue'])
    plot_quadrant(dfs, ['East', 'West'], bins, signal, ['green', 'orange'])
    logging.info(f"Total processing time: {time.time() - time_start} seconds.")


if __name__ == '__main__':
    main()
