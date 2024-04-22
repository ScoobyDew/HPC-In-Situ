import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings
import os
import time
import logging

logging.basicConfig(level=logging.INFO, filename='NSEW.log', filemode='w',
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


def bin_signal(df, signal, bins, x='instantaneous_distance', quadrant=None):
    quad_df = df[df['quadrant'] == quadrant].copy()
    quad_df['bin'] = pd.cut(quad_df[x], bins=bins, include_lowest=True, right=True)
    quad_df['bin_mid'] = quad_df['bin'].apply(lambda b: b.mid if not pd.isna(b) else np.nan)
    grouped = quad_df.groupby('bin_mid', observed=True)[signal].agg(['mean', 'std']).reset_index()
    return grouped


def plot_quadrant(df_list, quadrants, bins, signal, colors, x='instantaneous_distance', ax=None):
    if ax is None:
        raise ValueError("Axis not provided")

    for quadrant, color in zip(quadrants, colors):
        for idx, df in enumerate(df_list):
            grouped = bin_signal(df, signal, bins, x=x, quadrant=quadrant)
            valid_mask = np.isfinite(grouped['mean']) & np.isfinite(grouped['std'])

            midpoints = grouped['bin_mid'][valid_mask]
            means = grouped['mean'][valid_mask]

            # Normalize each quadrant's means within each plot call
            minmax_norm = (means - means.min()) / (means.max() - means.min())

            ax.plot(midpoints, minmax_norm, color=color,
                    label=f'File {idx + 1} - {quadrant}', alpha=0.5)

    ax.set_title(f'{"/".join(quadrants)} Quadrants')
    ax.set_xlabel('x (mm)')
    ax.set_ylabel(f'$V_{{p}}\\prime$')
    # ax.legend()


def plot_quadrants(dfs, bins, signal, x='instantaneous_distance'):
    quadrant_pairs = [('North', 'South'), ('East', 'West')]
    color_pairs = [('red', 'blue'), ('green', 'orange')]
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))  # Adjust subplot layout to horizontal
    for pair, colors, ax in zip(quadrant_pairs, color_pairs, axs.flatten()):
        plot_quadrant(dfs, pair, bins, signal, colors, x=x, ax=ax)
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
    n_files = 15
    max_val = 124
    exclude = [1, 4, 64, 67]
    selected_files = get_random_files(n_files, exclude, max_val)
    dfs = []
    for file_number in selected_files:
        file_path = f'{directory}/{file_number}.parquet'
        if os.path.exists(file_path):
            df = pd.read_parquet(file_path)
            df = assign_quadrant(df)
            dfs.append(df)
            print(f"Loaded {file_path}")
        else:
            print(f"File {file_path} does not exist.")
    if not dfs:
        print("No data files were loaded.")
        return
    bins = np.linspace(min(df['instantaneous_distance'].min() for df in dfs),
                       max(df['instantaneous_distance'].max() for df in dfs), 20)
    signal = 'pyro2'
    plot_quadrants(dfs, bins, signal)


if __name__ == '__main__':
    main()
