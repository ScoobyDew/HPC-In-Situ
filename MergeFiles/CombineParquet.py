import os
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import logging
from dask import delayed

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_parquet_file(file):
    # This function reads a single Parquet file and returns a DataFrame.
    df = pd.read_parquet(file)
    df['Part Number'] = os.path.splitext(os.path.basename(file))[0]  # Assuming part number is the file name without extension
    return df

def read_parquet_directory(directory):
    # List all Parquet files in the directory
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
    # Create delayed tasks for each file
    delayed_frames = [delayed(read_parquet_file)(file) for file in files]

    # Define explicit metadata for the DataFrame expected from read_parquet_file
    meta = {'t': float, 'x': float, 'y': float, 'z': float, 'pyro1': float, 'pyro2': float,
            'laser_state': float, 'defect': float, 'hatch': float, 'mp_width': float,
            'mp_length': float, 'mp_intensity': float, 'layer': int, 't_hatch': float,
            'v': float, 'hatch_length': float, 'hatch_angle': float,
            'instantaneous_distance': float, 'Part Number': str}

    # Use from_delayed to construct a Dask DataFrame
    ddf = dd.from_delayed(delayed_frames, meta=meta)
    return ddf

def attach_parameters(ddf, parameters_file):
    parameters = pd.read_excel(parameters_file)
    parameters['Part Number'] = parameters['Part Number'].astype(str)  # Ensure matching data types for merging
    parameters_ddf = dd.from_pandas(parameters, npartitions=32)
    ddf = ddf.merge(parameters_ddf, on='Part Number', how='left')
    return ddf

def main():
    cluster = LocalCluster(
        memory_limit='64GB',
        n_workers=32,
        threads_per_worker=1
    )
    client = Client(cluster)
    try:
        directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
        logging.info(f"Processing data in directory: {directory}")
        ddf = read_parquet_directory(directory)
        ddf = attach_parameters(ddf, 'parameters.xlsx')
        df = ddf.compute()

        if df.empty:
            logging.error("Processed DataFrame is empty. No data to process.")
        else:
            logging.info('Processing completed successfully')
            print(df.head())
            print(df.describe())

    except Exception as e:
        logging.error(f'An error occurred during processing: {e}')
    finally:
        client.close()

if __name__ == "__main__":
    main()
