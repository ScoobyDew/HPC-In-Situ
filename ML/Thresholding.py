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
    df = df[(df['mp_width'] > 0) & (df['mp_length'] > 0)]
    logging.info("0 or negative values removed.")

    # Convert to Pandas DataFrame for processing
    logging.info("Converting to Pandas DataFrame.")
    X = df.compute()

    # Define the bin edges so that every possible value of mp_width and mp_length is a bin edge
    x_edges = np.arange(X['mp_width'].min(), X['mp_width'].max() + 2)  # +2 to ensure the max value is included in a bin
    y_edges = np.arange(X['mp_length'].min(), X['mp_length'].max() + 2)

    # Compute the 2D histogram
    logging.info("Computing 2D histogram.")
    histogram, x_edges, y_edges = np.histogram2d(
        X['mp_width'], X['mp_length'],
        bins=(x_edges, y_edges))

    # Calculate density
    total_datapoints = np.sum(histogram)
    density = histogram / total_datapoints

    # Create meshgrid of mp_width and mp_length values
    mp_width, mp_length = np.meshgrid(x_edges[:-1], y_edges[:-1], indexing='ij')

    # Flatten the arrays and create a DataFrame
    histogram_data = pd.DataFrame({
        'mp_width': mp_width.ravel().astype(int),
        'mp_length': mp_length.ravel().astype(int),
        'density': density.ravel().astype(int)
    })

    # Save histogram data to CSV
    output_path = f'/mnt/parscratch/users/eia19od/histogram_density_data_{time.strftime("%Y%m%d%H%M%S")}.csv'
    histogram_data.to_csv(output_path, index=False)
    logging.info(f"Histogram data saved successfully at {output_path}.")

    logging.info(f"Total processing time: {time.time() - time_start} seconds.")

if __name__ == "__main__":
    main()
