import os
import dask.dataframe as dd
import pandas as pd
import logging
from dask.distributed import Client, LocalCluster
from dask import delayed

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_parquet_file(file):
    try:
        # Read each file as a delayed Dask DataFrame
        df = dd.read_parquet(file, engine='pyarrow')
        part_number = os.path.basename(file).split('.')[0]
        df['Part Number'] = part_number
        return df
    except Exception as e:
        logging.error(f'Error reading file {file}: {e}', exc_info=True)
        return dd.from_pandas(pd.DataFrame(), npartitions=32)

def read_parquet_directory(directory):
    logging.info(f"Reading parquet files from directory: {directory}")
    try:
        files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
        # Process each file in parallel using Dask
        dataframes = [delayed(read_parquet_file)(file) for file in files]
        ddf = dd.from_delayed(dataframes)
        return ddf
    except Exception as e:
        logging.error(f'Error reading parquet files: {e}', exc_info=True)
        return dd.from_pandas(pd.DataFrame(), npartitions=32)

def attach_parameters(ddf, parameters_file):
    try:
        parameters = pd.read_excel(parameters_file)
        parameters['Part Number'] = parameters['Part Number'].astype(str)
        parameters_ddf = dd.from_pandas(parameters, npartitions=1)
        ddf = ddf.merge(parameters_ddf, on='Part Number', how='left')
        return ddf
    except Exception as e:
        logging.error(f'Failed to attach parameters: {e}', exc_info=True)
        return ddf

def calculate_NVED(ddf):
    try:
        A = 0.3
        l = 7.5
        rho = 8.395e-3
        Cp = 0.43
        Tm = 1300
        T0 = 300
        h = 75

        ddf['E*'] = (A * ddf['Power (W)'] / (2 * ddf['Speed (mm/s)'] * l * ddf['beam_radius'] * 1e3) *
                     1 / (0.67 * rho * Cp * (Tm - T0)))
        ddf['1/h*'] = ddf['beam_radius'] * 1e3 / h
        return ddf
    except Exception as e:
        logging.error(f'Failed to calculate NVED: {e}', exc_info=True)
        return ddf

def main():
    cluster = LocalCluster(memory_limit='2GB', n_workers=4, threads_per_worker=1)
    client = Client(cluster)
    try:
        directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
        logging.info(f"Processing data in directory: {directory}")

        ddf = read_parquet_directory(directory)
        ddf = attach_parameters(ddf, 'part_parameters.xlsx')
        ddf = calculate_NVED(ddf)

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
