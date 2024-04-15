import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCUDACluster
from cuml.cluster import DBSCAN as cuDBSCAN
from cuml.preprocessing import StandardScaler
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Setting Dask to use cuDF as the backend for DataFrame operations.")
    dask.config.set({"dataframe.backend": "cudf"})

    # Setup Dask CUDA cluster
    try:
        cluster = LocalCUDACluster()
        client = Client(cluster)
        logging.info("Using GPU Dask cluster.")
        logging.info("Cluster dashboard link: " + client.dashboard_link)
    except Exception as e:
        logging.error("Error setting up GPU Dask cluster: %s", e)
        return

    # Load the dataset using Dask with cuDF backend
    file_path = ''  # Path to your Parquet file
    logging.info("Loading data from %s", file_path)
    df = dd.read_parquet(file_path)
    logging.info("Data loaded successfully.")

    # Focus on 'mp_width' and 'mp_length' for clustering
    features = ['mp_width', 'mp_length']


    logging.info("Standardizing features...")
    scaler = StandardScaler()
    df[features] = df.map_partitions(lambda part: scaler.fit_transform(part))
    logging.info("Features standardized.")

    logging.info("Compurting the scaling and applying DBSCAN...")
    # Compute the scaling and apply DBSCAN
    df = df.compute()  # Convert to cuDF DataFrame for further processing

    # Parameters for DBSCAN may need tuning
    eps = 0.5  # Maximum distance between two samples for one to be considered as in the neighborhood of the other
    min_samples = 5
    clustering = cuDBSCAN(eps=eps, min_samples=min_samples)
    df['cluster'] = clustering.fit_predict(df[features])

    logging.info("Clustering completed.")

    # Save the results back to Parquet
    output_path = '/mnt/parscratch/users/eia19od/clustered.parquet'  # Define your output file path
    logging.info("Saving clustered data to %s", output_path)
    df.to_parquet(output_path)

    logging.info("Clustering completed successfully.")

if __name__ == "__main__":
    main()
