import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import logging
import time

# Setting up logging
logging.basicConfig(level=logging.INFO, filename='NSEW.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def assign_quadrant(df):
    df['90angle'] = np.round(df['hatch_angle'] / 90) * 90
    df['quadrant'] = df['90angle'].map({0: 'East', 90: 'North', 180: 'West', -180: 'West', -90: 'South', 270: 'South'})
    return df


def shift_to_zero(df):
    df['instantaneous_distance'] -= df['instantaneous_distance'].min()
    return df


def bin_signal(df, signal, bins, quadrant, file_number):
    output_folder = "binned_data"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    quad_df = df[df['quadrant'] == quadrant].copy()
    quad_df['bin'] = pd.cut(quad_df['instantaneous_distance'], bins=bins, include_lowest=True, right=True)
    quad_df['bin_mid'] = quad_df['bin'].apply(lambda b: b.mid if not pd.isna(b) else np.nan)
    grouped = quad_df.groupby('bin_mid')[signal].agg(['mean', 'std']).reset_index()

    # Save the feature name in the filename for clarity
    file_path = os.path.join(output_folder, f"{quadrant}_{signal}_{file_number}.csv")
    grouped.to_csv(file_path, index=False)
    logging.info(f"Saved binned data for {signal} to {file_path}")


def read_and_plot(directory):
    fig, axs = plt.subplots(2, 2, figsize=(12, 12))
    quadrants = ['North', 'East', 'South', 'West']
    for i, quadrant in enumerate(quadrants):
        files = sorted([f for f in os.listdir(directory) if f.startswith(quadrant)])
        for file in files:
            df = pd.read_csv(os.path.join(directory, file))
            axs[i // 2, i % 2].plot(df['bin_mid'], df['mean'], label=f"{file.split('_')[1].split('.')[0]}")
            axs[i // 2, i % 2].set_title(quadrant)
            axs[i // 2, i % 2].set_xlabel('Distance')
            axs[i // 2, i % 2].set_ylabel('Mean Value')
            axs[i // 2, i % 2].legend()
    plt.tight_layout()

    # Save the plot
    plt.savefig('binned_data_plot.png')
    logging.info("Saved binned data plot to binned_data_plot.png")


def main():
    directory = '/mnt/parscratch/users/eia19od/Cleaned'
    signal = 'pyro2'
    # Retrieve all parquet files in the directory
    all_files = [f for f in os.listdir(directory) if f.endswith('.parquet')]

    for filename in all_files:
        file_path = os.path.join(directory, filename)
        df = pd.read_parquet(file_path)
        df = assign_quadrant(df)
        df = shift_to_zero(df)
        bins = np.linspace(df['instantaneous_distance'].min(), df['instantaneous_distance'].max(), 40)

        # Extract file number or identifier from filename
        file_number = filename.split('.')[0]  # Assuming the filename format is 'number.parquet'

        for quadrant in ['North', 'East', 'South', 'West']:
            bin_signal(df, signal, bins, quadrant, file_number)

    logging.info("Plotting binned data.")
    read_and_plot('binned_data')


if __name__ == '__main__':
    main()

if __name__ == '__main__':
    main()
