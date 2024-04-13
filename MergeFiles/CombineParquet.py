import os
import dask.dataframe as dd
import logging
from dask.distributed import Client
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_parquet_directory(directory):
    logging.info(f"Reading parquet files from directory: {directory}")
    try:
        # List all parquet files in the directory
        files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
        # Read files using Dask
        ddf = dd.read_parquet(files, engine='pyarrow')

        # Process file names to extract part numbers
        meta = {'Part Number': str}
        ddf['Part Number'] = ddf.map_partitions(lambda df: df.apply(lambda row: os.path.basename(row.name).split('.')[0], axis=1), meta=meta)

        logging.info(f"Files processed: {len(files)}")
        return ddf
    except Exception as e:
        logging.error(f'Error reading parquet files: {e}', exc_info=True)
        return dd.from_pandas(pd.DataFrame(), npartitions=32)  # Return an empty Dask DataFrame if an error occurs

def attach_parameters(ddf, parameters_file):
    try:
        # Parameters are still read with Pandas due to the usual smaller size and complexity of Excel files
        parameters = pd.read_excel(parameters_file)
        features = ['Power (W)', 'Speed (mm/s)', 'Focus', 'Beam radius (um)']
        necessary_columns = ['Part Number'] + features
        parameters_ddf = dd.from_pandas(pd.DataFrame(parameters, columns=necessary_columns), npartitions=32)

        # Merge parameters with the main Dask DataFrame
        ddf = ddf.merge(parameters_ddf, on='Part Number', how='left')
        return ddf
    except Exception as e:
        logging.error(f'Failed to attach parameters: {e}', exc_info=True)
        return ddf  # Return the input if failure occurs

def calculate_NVED(ddf):
    try:
        A = 0.3
        l = 7.5
        rho = 8.395e-3
        Cp = 0.43
        Tm = 1300
        T0 = 300
        h = 75

        # Calculation using Dask operations
        ddf['E*'] = (A * ddf['Power (W)'] / (2 * ddf['Speed (mm/s)'] * l * ddf['beam_radius'] * 1e3) *
                     1 / (0.67 * rho * Cp * (Tm - T0)))
        ddf['1/h*'] = ddf['beam_radius'] * 1e3 / h
        return ddf
    except Exception as e:
        logging.error(f'Failed to calculate NVED: {e}', exc_info=True)
        return ddf

def main():
    client = Client()  # Starts a Dask client to manage workers
    try:
        directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
        logging.info(f"Processing data in directory: {directory}")

        # Read and process data
        ddf = read_parquet_directory(directory)
        ddf = attach_parameters(ddf, 'part_parameters.xlsx')
        ddf = calculate_NVED(ddf)

        # Compute the result to get final DataFrame
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
