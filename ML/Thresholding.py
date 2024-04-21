import os
import logging
import time
import numpy as np
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt

# Setup logging
logging.basicConfig(level=logging.INFO, filename='dbscancpu.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    time_start = time.time()
    logging.info("Starting processing...")
    # Load the dataset
    df = dd.read_parquet('/mnt/parscratch/users/eia19od/combined_params.parquet',
                         engine='pyarrow', columns=['mp_width', 'mp_length'])
    logging.info("Successfully read the parquet file.")

    # Remove any 0 or negative values
    logging.info("Removing any 0 or negative values.")
    df = df[(df['mp_width'] > 0)]
    df = df[(df['mp_length'] > 0)]
    logging.info("0 or negative values removed.")

    # Convert to Pandas DataFrame for processing
    logging.info("Converting to Pandas DataFrame.")
    X = df.compute()

    # Compute the 2D histogram
    logging.info("Computing 2D histogram.")
    histogram, x_edges, y_edges = np.histogram2d(
        X['mp_width'], X['mp_length'],
        bins=(np.arange(X['mp_width'].min(), X['mp_width'].max() + 1), np.arange(X['mp_length'].min(), X['mp_length'].max() + 1)))

    # Plotting the histogram
    logging.info("Plotting the histogram.")
    plt.figure(figsize=(10, 8))
    plt.imshow(histogram.T, origin='lower', aspect='auto', extent=[x_edges[0], x_edges[-1], y_edges[0], y_edges[-1]], interpolation='nearest')
    plt.colorbar(label='Counts')
    plt.title('2D Histogram of Meltpool Width and Length')
    plt.xlabel('Melt Pool Width')
    plt.ylabel('Melt Pool Length')
    plt.grid(True)
    plt.savefig(f'/mnt/parscratch/users/eia19od/histogram_{time.strftime("%Y%m%d%H%M%S")}.png')
    plt.close()
    logging.info("Histogram plot saved successfully.")

    # Save histogram data to CSV
    histogram_data = pd.DataFrame(histogram, index=np.arange(y_edges[0], y_edges[-1]), columns=np.arange(x_edges[0], x_edges[-1]))
    histogram_data.to_csv(f'/mnt/parscratch/users/eia19od/histogram_data_{time.strftime("%Y%m%d%H%M%S")}.csv')
    logging.info("Histogram data saved successfully.")

    logging.info(f"Total processing time: {time.time() - time_start} seconds.")

if __name__ == "__main__":
    main()
