"""
This file performs DBSCAN clustering on the combined meltpool data.
This will be used to indentify the different clusters of meltpools and the
process parameters that are associated with them.
"""

import os
import pickle
import logging
import time

import dask
import dask.dataframe as dd

import cudf.pandas
cudf.pandas.install()

from cuml.cluster import hdbscan

import pandas as pd

import seaborn
seaborn.set_style("white")

dask.config.set({"dataframe.backend": "cudf"})

# Find time at the start of the processing (date and time)
start_time = time.strftime('%Y-%m-%d-%H%M%S')
# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """load the data from parquet file"""
    logging.info("Starting processing...")
    df = dd.read_parquet('/mnt/parscratch/users/eia19od/combined_params.parquet')
    logging.info(f"Successfully read parquet file")

    # Select the columns to be used for clustering
    logging.info(f"Selecting columns for clustering and converting to cuDF...")
    X = df[['mp_width', 'mp_length']].compute()
    logging.info(f"Successfully converted to cuDF")

    # Perform DBSCAN clustering
    clusterer = hdbscan.HDBSCAN(
        min_samples=10,
        min_cluster_size=2
    )
    logging.info(f"Starting clustering...")
    clusterer.fit(X)
    logging.info(f"Clustering complete")

    # Save the model and all associated data
    logging.info(f"Saving model and data...")
    with open(f'/mnt/parscratch/users/eia19od/hdbscan_model_{start_time}.pkl', 'wb') as f:
        pickle.dump(clusterer, f)
    logging.info(f"Model saved as pickle")

    def assign_cluster_labels(df, labels):
        df['cluster'] = labels
        return df


    # Assign the cluster labels to the original DataFrame
    logging.info(f"Assigning cluster labels to the data...")
    df = df.map_partitions(assign_cluster_labels, clusterer.labels_)
    logging.info(f"Cluster labels assigned")

    # Save the DataFrame with Dask
    logging.info(f"Saving the DataFrame with cluster labels...")
    df.to_parquet(f'/mnt/parscratch/users/eia19od/combined_params_clustered_{start_time}.parquet')
    logging.info(f"DataFrame saved as parquet")

    # Plot clusters in 2D histogram for meltpool width and length
    # Plotting using Seaborn
    logging.info(f"Plotting the data...")
    for