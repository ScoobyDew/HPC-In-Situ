import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, filename='NSEW.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_random_files(n, exclude, max_val):
    file_numbers = np.setdiff1d(np.arange(1, max_val + 1), exclude)
    selected_files = np.random.choice(file_numbers, size=n, replace=False)
    return selected_files


def assign_quadrant(df):
    # Directly update quadrant based on conditions
    df['quadrant'] = np.select(
        [df['hatch_angle'] == 0, df['hatch_angle'] == 90, df['hatch_angle'] == 180,
         df['hatch_angle'] == -180, df['hatch_angle'] == -90, df['hatch_angle'] == 270],
        ['East', 'North', 'West', 'West', 'South', 'South'],
        default='Unknown'
    )
    return df


def preprocess_data(df):
    # Shift the 'instantaneous_distance' to start from zero and assign quadrants
    df['instantaneous_distance'] -= df['instantaneous_distance'].min()
    df = assign_quadrant(df)
    # Swap directions for visualization purposes
    df['quadrant'] = df['quadrant'].replace({'North': 'West', 'West': 'North', 'South': 'East', 'East': 'South'})
    return df


def bin_signal(df, signal, bins, x='instantaneous_distance', quadrant=None):
    # Filter and bin the data for a given quadrant
    quad_df = df[df['quadrant'] == quadrant]
    quad_df['bin'] = pd.cut(quad_df[x], bins=bins, include_lowest=True, right=True)
    grouped = quad_df.groupby('bin')['mean', 'std'].agg({'mean': 'mean', 'std': 'std'}).reset_index()
    return grouped['mean'], quad_df['bin'].apply(lambda b: b.mid)


def plot_quadrants(dfs, bins, signal):
    # Set up plotting
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))
    colors = ['red', 'blue', 'green', 'orange']
    quadrant_pairs = [('North', 'South'), ('East', 'West')]

    for ax, (quadrants, color) in zip(axs, zip(quadrant_pairs, colors)):
        for quadrant in quadrants:
            means, midpoints = [], []
            for df in dfs:
                mean, midpoint = bin_signal(df, signal, bins, quadrant=quadrant)
                means.append(mean)
                midpoints.append(midpoint)
            # Normalize and plot
            for mean, midpoint in zip(means, midpoints):
                normalized_mean = (mean - np.min(mean)) / (np.max(mean) - np.min(mean))
                ax.plot(midpoint, normalized_mean, color=color, label=f'{quadrant} Mean')

        ax.set_title(f'{"/".join(quadrants)} Quadrants')
        ax.set_xlabel('x (mm)')
        ax.set_ylabel(f'$V^*_{{pyro}}$ mV')
        ax.legend()

    plt.tight_layout()
    plt.savefig('quadrants_plot.png')


def main():
    directory = 'D:/Cleaned'
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
                       max(df['instantaneous_distance'].max() for df in dfs), 20)
    signal = 'pyro2'
    plot_quadrants(dfs, bins, signal)
    logging.info("Finished plotting.")


if __name__ == '__main__':
    main()
