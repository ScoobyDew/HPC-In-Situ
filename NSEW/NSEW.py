import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings
import logging
import time
import matplotlib.patches as mpatches

logging.basicConfig(level=logging.INFO, filename='/users/eia19od/in_situ/HPC-In-Situ/NSEW/NSEW.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_random_files(n, exclude, max_val):
    file_numbers = [num for num in range(1, max_val + 1) if num not in exclude]
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

def shift_to_zero(df):
    df['instantaneous_distance'] -= df['instantaneous_distance'].min()
    return df

def bin_signal(df, signal, bins, x='instantaneous_distance', quadrant=None):
    quad_df = df[df['quadrant'] == quadrant].copy()
    quad_df['bin'] = pd.cut(quad_df[x], bins=bins, include_lowest=True, right=True)
    quad_df['bin_mid'] = quad_df['bin'].apply(lambda b: b.mid if not pd.isna(b) else np.nan)
    grouped = quad_df.groupby('bin_mid', observed=True)[signal].agg(['mean', 'std']).reset_index()
    return grouped


# N
def plot_quadrant(dfs, quadrants, bins, signal, colors, x='instantaneous_distance'):
    color_dict = dict(zip(quadrants, colors))
    patches = [mpatches.Patch(color=color, label=quadrant) for quadrant, color in color_dict.items()]

    fig, axs = plt.subplots(1, 2, figsize=(12, 6))  # Adjust subplot layout to horizontal
    for quadrant, color, ax in zip(quadrants, colors, axs.flatten()):
        means_list = []
        for idx, df in enumerate(dfs):
            quad_df = df[df['quadrant'] == quadrant].copy()
            # Convert categorical column to regular Pandas Series
            quad_df['bin'] = pd.cut(quad_df[x].compute(), bins=bins, include_lowest=True, right=True).astype('object')
            grouped = quad_df.groupby('bin', observed=True)[signal].agg(['mean', 'std']).reset_index()
            valid_mask = np.isfinite(grouped['mean']) & np.isfinite(grouped['std'])
            means = grouped['mean'][valid_mask]
            means_list.append(means)

        # Ensure all arrays in means_list have the same length by padding with NaNs if necessary
        max_length = max(len(means) for means in means_list)
        padded_means_list = [np.concatenate([means, np.full(max_length - len(means), np.nan)]) for means in means_list]

        # Calculate the mean across dataframes
        avg_means = np.nanmean(padded_means_list, axis=0)
        ax.plot(bins[:-1], avg_means, color=color, label=f'{quadrant} Quadrant', alpha=0.5)

        ax.set_title(f'{"/".join(quadrants)} Quadrants')
        ax.set_xlabel('x (mm)')
        ax.set_ylabel(f'$V^*_{{pyro}}$ mV')
        ax.legend(handles=patches)  # Use the patches for the legend
    plt.tight_layout()

    # Save the plot to directory if it does not exist
    if not os.path.exists('/mnt/parscratch/users/eia19od/Quadrants'):
        os.makedirs('/mnt/parscratch/users/eia19od/Quadrants')

    # Create a string with the current date and time
    end_time = time.time()
    date_time = time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime(end_time))
    plt.savefig(f'/mnt/parscratch/users/eia19od/Quadrants/Quadrants_{date_time}.png')

def plot_quadrants(dfs, bins, signal, x='instantaneous_distance'):
    quadrant_pairs = [('North', 'South'), ('East', 'West')]
    color_pairs = [('red', 'blue'), ('green', 'orange')]
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))  # Adjust subplot layout to horizontal
    for pair, colors, ax in zip(quadrant_pairs, color_pairs, axs.flatten()):
        plot_quadrant(dfs, pair, bins, signal, colors, x=x, ax=ax)
    plt.tight_layout()

    # Save to 'quadrants' directory
    if not os.path.exists('quadrants'):
        os.makedirs('quadrants')
    date = time.strftime('%Y-%m-%d_%H-%M-%S')
    plt.savefig(f'quadrants/quadrants_{date}.png')


def main():
    start_time = time.time()
    directory = '/mnt/parscratch/users/eia19od/Cleaned'
    n_files = 4
    max_val = 124
    exclude = [1, 4, 64, 67]
    selected_files = get_random_files(n_files, exclude, max_val)
    dfs = []
    for file_number in selected_files:
        file_path = f'{directory}/{file_number}.parquet'
        if os.path.exists(file_path):
            df = pd.read_parquet(file_path)
            df = assign_quadrant(df)

            # Shift the instantaneous_distance to start from 0
            logging.info(f"Shifting instantaneous_distance to start from 0 for {file_path}")
            df = shift_to_zero(df)

            dfs.append(df)
            logging.info(f"Loaded {file_path}")



        else:
            logging.warning(f"File {file_path} does not exist.")
    if not dfs:
        logging.error("No files loaded. Exiting.")
        return
    bins = np.linspace(min(df['instantaneous_distance'].min() for df in dfs),
                       max(df['instantaneous_distance'].max() for df in dfs), 20)

    signal = 'pyro2'
    logging.info("Plotting quadrants.")
    plot_quadrants(dfs, bins, signal)
    logging.info(f"Time taken: {time.time() - start_time:.2f} seconds.")


if __name__ == '__main__':
    main()
