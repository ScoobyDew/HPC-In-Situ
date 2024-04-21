import os
import pickle
import logging
import time
import numpy as np
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.ndimage import label

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

    # Sample to reduce the size of the dataset
    df = df.sample(frac=0.00001)

    # Select the columns to be used
    logging.info("Selecting columns.")
    df = df[['mp_width', 'mp_length']]

    # Drop invalid rows
    logging.info("Dropping rows with invalid values.")
    df = df[(df['mp_width'] > 0) & (df['mp_length'] > 0)]

    # Convert to Pandas DataFrame for processing
    logging.info("Converting to Pandas DataFrame.")
    X = df.compute()

    # Generate a 2D histogram
    logging.info("Generating 2D histogram.")
    hist, xedges, yedges = np.histogram2d(
        X['mp_width'], X['mp_length'],
        bins=300,
        range=[[X['mp_width'].min(), X['mp_width'].max()], [X['mp_length'].min(), X['mp_length'].max()]]
    )


    # Apply a density threshold
    threshold = 2e4
    dense_areas = hist > threshold

    # Label the dense areas
    labeled_array, num_features = label(dense_areas)
    logging.info(f"Number of dense areas identified: {num_features}")

    # Plotting
    logging.info("Plotting the results.")
    plt.figure(figsize=(10, 8))
    plt.imshow(labeled_array, interpolation='nearest', origin='low',
               extent=[xedges[0], xedges[-1], yedges[0], yedges[-1]])
    plt.colorbar()
    plt.title('Dense Areas in Meltpool Width and Length')
    plt.xlabel('Melt Pool Width')
    plt.ylabel('Melt Pool Length')
    plt.savefig(f'/mnt/parscratch/users/eia19od/dense_areas_plot_{time.strftime("%Y%m%d%H%M%S")}.png')
    plt.close()
    logging.info("Plot saved successfully.")
    logging.info(f"Total processing time: {time.time() - time_start} seconds.")

if __name__ == "__main__":
    main()
