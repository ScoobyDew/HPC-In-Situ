import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import logging
import time
import matplotlib.patches as mpatches

# Configure logging
logging.basicConfig(level=logging.INFO, filename='NSEW.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_random_files(n, exclude, max_val):
    file_numbers = np.setdiff1d(np.arange(1, max_val + 1), exclude)
    selected_files = np.random.choice(file_numbers, size=n, replace=False)
    return selected_files

def assign_quadrant(df):
    df['quadrant'] = np.select(
        [df['hatch_angle'] == 0, df['hatch_angle'] == 90, df['hatch_angle'] == 180,
         df['hatch_angle'] == -180, df['hatch_angle'] == -90, df['hatch_angle'] == 270],
        ['East', 'North', 'West', 'West', 'South', 'South'],
        default='Unknown'
    )
    return df

def preprocess_data(df):
    df['instantaneous_distance'] -= df['instantaneous_distance'].min()
    return assign_quadrant(df)

def bin_signal(df, signal, bins, x='instantaneous_distance', quadrant=None):
    quad_df = df[df['quadrant'] == quadrant].copy()
    quad_df['bin'] = pd.cut(quad_df[x], bins=bins, include_lowest=True, right=True)
    quad_df['bin_mid'] = quad_df['bin'].apply(lambda b: b.mid if not pd.isna(b) else np.nan)
    grouped = quad_df.groupby('bin', as_index=False, observed=True).agg({signal: ['mean', 'std']})
    grouped.columns = ['bin', 'mean', 'std']  # Flatten the MultiIndex for easier access
    grouped['bin_mid'] = grouped['bin'].apply(lambda b: b.mid)
    return grouped


def plot_quadrants(dfs, bins, signal):
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))
    colors = [['red', 'blue'], ['green', 'orange']]  # Each sub-plot gets two colors
    quadrant_pairs = [('North', 'South'), ('East', 'West')]

    for ax, (quadrants, color_pair) in zip(axs, zip(quadrant_pairs, colors)):
        for quadrant, color in zip(quadrants, color_pair):
            all_means = []
            all_midpoints = []
            for df in dfs:
                grouped = bin_signal(df, signal, bins, quadrant=quadrant)
                normalized_mean = (grouped['mean'] - grouped['mean'].min()) / (
                            grouped['mean'].max() - grouped['mean'].min())
                all_means.append(normalized_mean)
                all_midpoints.append(grouped['bin_mid'])

            for mean, midpoint in zip(all_means, all_midpoints):
                ax.plot(midpoint, mean, color=color, label=f'{quadrant} Mean', alpha=0.5)

        ax.set_title(f'{"/".join(quadrants)} Quadrants')
        ax.set_xlabel('x (mm)')
        ax.set_ylabel(f'$V^*_{{pyro}}$ mV')
        ax.legend(
            handles=[mpatches.Patch(color=color, label=quadrant) for quadrant, color in zip(quadrants, color_pair)])

    plt.tight_layout()
    plt.savefig('quadrants_plot.png')


def main():
    directory = '/mnt/parscratch/users/eia19od/Cleaned'
    n_files = 15
    max_val = 124
    exclude = [1, 4, 64, 67]
    selected_files = get_random_files(n_files, exclude, max_val)
    dfs = []

    for file_number in selected_files:
        file_path = f'{directory}/{file_number}.parquet'
        if os.path.exists(file_path):
            df = pd.read_parquet(file_path)
            df = preprocess_data(df)
            dfs.append(df)
            logging.info(f"Loaded and processed {file_path}")
        else:
            logging.warning(f"File {file_path} does not exist.")

    if not dfs:
        logging.error("No data files were loaded. Exiting.")
        return

    bins = np.linspace(min(df['instantaneous_distance'].min() for df in dfs),
                       max(df['instantaneous_distance'].max() for df in dfs), 30)
    signal = 'pyro2'
    plot_quadrants(dfs, bins, signal)
    logging.info("Finished plotting.")

if __name__ == '__main__':
    main()
