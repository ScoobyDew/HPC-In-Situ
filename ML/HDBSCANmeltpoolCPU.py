import os
import pickle
import logging
import time
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import HDBSCAN  # Corrected import

# Setup logging
logging.basicConfig(level=logging.INFO, filename='hdbscancpu.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Starting processing...")
    try:
        # Load the dataset
        df = dd.read_parquet('/mnt/parscratch/users/eia19od/combined_params.parquet')
        logging.info("Successfully read the parquet file.")

        # Sample to reduce the size of the dataset to 0.01% of the original size
        df = dd.sample(df, frac=0.001)

        # Select the columns to be used for clustering
        logging.info("Selecting columns for clustering.")
        df = df[['mp_width', 'mp_length']]

        # Convert to Pandas DataFrame for clustering
        logging.info("Converting to Pandas DataFrame for clustering.")
        X = df.compute()  # Ensure enough memory is available before computing

        # Perform HDBSCAN clustering
        logging.info("Performing HDBSCAN clustering.")
        clusterer = HDBSCAN(
            min_samples=10,
            min_cluster_size=10,
        )
        labels = clusterer.fit_predict(X)
        logging.info("Clustering complete.")

        # Assign cluster labels to the original Pandas DataFrame
        logging.info("Assigning cluster labels to the DataFrame.")
        X['cluster'] = labels

        # Plotting the data
        logging.info("Plotting the cluster distribution.")
        plt.figure(figsize=(10, 8))
        sns.histplot(
            data=X,
            x='mp_width',
            y='mp_length',
            hue='cluster',
            palette='viridis',
            bins=30,  # Adjust based on the range and spread of your data
            kde=False
        )
        plt.title('2D Histogram of Meltpool Width and Length by Cluster')
        plt.xlabel('Melt Pool Width')
        plt.ylabel('Melt Pool Length')
        plt.legend(title='Cluster')
        plt.grid(True)
        plt.savefig(f'/mnt/parscratch/users/eia19od/cluster_plot_{time.strftime("%Y-%m-%d-%H%M%S")}.png')
        plt.close()
        logging.info("Plot saved successfully.")

    except Exception as e:
        logging.error("An error occurred during processing.", exc_info=True)

if __name__ == "__main__":
    main()
