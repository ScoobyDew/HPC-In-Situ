import os
import pickle
import logging
import time
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import DBSCAN  # Corrected import

# Setup logging
logging.basicConfig(level=logging.INFO, filename='dbscancpu.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Starting processing...")
    try:
        # Load the dataset
        df = dd.read_parquet('/mnt/parscratch/users/eia19od/combined_params.parquet',
                             engine='pyarrow', columns=['mp_width', 'mp_length'])
        logging.info("Successfully read the parquet file.")

        # Sample to reduce the size of the dataset to 0.01% of the original size
        df = df.sample(frac=0.00001)

        # Select the columns to be used for clustering
        logging.info("Selecting columns for clustering.")
        df = df[['mp_width', 'mp_length']]

        # Drop rows with mp_width or mp_length as zero, negative or infinite
        logging.info("Dropping rows with invalid values.")
        df = df[(df['mp_width'] > 0) & (df['mp_length'] > 0) & (df['mp_width'] != float('inf')) & (df['mp_length'] != float('inf'))]
        logging.info("Invalid rows dropped.")


        # Drop

        # Convert to Pandas DataFrame for clustering
        logging.info("Converting to Pandas DataFrame for clustering.")
        X = df.compute()  # Ensure enough memory is available before computing

        # Perform DBSCAN clustering
        logging.info("Performing DBSCAN clustering.")
        clusterer = DBSCAN(
            eps=5,  # Example value; adjust based on domain knowledge and experimentation
            min_samples=2
        )
        labels = clusterer.fit_predict(X)
        logging.info("Clustering complete.")

        # Assign cluster labels to the original Pandas DataFrame
        logging.info("Assigning cluster labels to the DataFrame.")
        X['cluster'] = labels.astype(str)

        # Plotting the data
        logging.info("Plotting the cluster distribution.")
        plt.figure(figsize=(10, 8))
        plot = sns.histplot(
            data=X,
            x='mp_width',
            y='mp_length',
            hue='cluster',
            palette='viridis',
            style='cluster',
            binwidth= (1,1),
            bins=30,  # Adjust based on the range and spread of your data
            kde=False
        )
        plt.title('2D Histogram of Meltpool Width and Length by Cluster')
        plt.xlabel('Melt Pool Width')
        plt.ylabel('Melt Pool Length')

        # Check if any labels were picked up for the legend
        if plot.get_legend() is None:
            logging.warning("Legend is not generated - no labels found.")
        else:
            logging.info("Legend generated successfully.")

        plt.legend(title='Cluster')
        plt.grid(True)
        plt.savefig(f'/mnt/parscratch/users/eia19od/cluster_plot_{time.strftime("%Y%m%d%H%M%S")}.png')
        plt.close()
        logging.info("Plot saved successfully.")

    except Exception as e:
        logging.error("An error occurred during processing.", exc_info=True)

if __name__ == "__main__":
    main()
