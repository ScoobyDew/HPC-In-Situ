import os
import logging
import time
import numpy as np
import dask.dataframe as dd
import pandas as pd

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

    # Apply robust filtering to remove any 0 or negative values effectively
    logging.info("Applying robust filtering to remove any 0 or negative values.")
    df = df[df['mp_width'] > 0]
    df = df[df['mp_length'] > 0]
    logging.info("0 or negative values removed.")

    # Convert to Pandas DataFrame for processing
    logging.info("Converting to Pandas DataFrame.")
    X = df.compute()

    # Define the bin edges so that every possible value of mp_width and mp_length is a bin edge
    x_edges = np.arange(X['mp_width'].min(), X['mp_width'].max() + 2)  # +2 to include the max value
    y_edges = np.arange(X['mp_length'].min(), X['mp_length'].max() + 2)

    # Compute the 2D histogram
    logging.info("Computing 2D histogram for counts.")
    histogram, x_edges, y_edges = np.histogram2d(
        X['mp_width'], X['mp_length'],
        bins=(x_edges, y_edges))

    # Create meshgrid of mp_width and mp_length values
    mp_width, mp_length = np.meshgrid(x_edges[:-1], y_edges[:-1], indexing='ij')

    # Flatten the arrays and create a DataFrame
    histogram_data = pd.DataFrame({
        'mp_width': mp_width.ravel().astype(int),
        'mp_length': mp_length.ravel().astype(int),
        'counts': histogram.ravel().astype(int)
    })

    # Save histogram data to CSV
    output_path = f'/mnt/parscratch/users/eia19od/histogram_counts_data_{time.strftime("%Y%m%d%H%M%S")}.csv'
    histogram_data.to_csv(output_path, index=False)
    logging.info(f"Counts data saved successfully at {output_path}.")

    logging.info(f"Total processing time: {time.time() - time_start} seconds.")

if __name__ == "__main__":
    main()
