import os
import dask.dataframe as dd
import pandas as pd
import logging
from dask.distributed import Client

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_parquet_directory(directory):
    logging.info(f"Reading parquet files from directory: {directory}")
    try:
        files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
        # Read all parquet files using Dask
        ddf = dd.read_parquet(files, engine='pyarrow')
        # Extracting 'Part Number' from filenames within each partition
        ddf['Part Number'] = ddf.map_partitions(
            lambda df, filename: df.assign(part_number=os.path.basename(filename).split('.')[0]),
            meta=('part_number', 'object'),  # Specifying meta to ensure dtype understanding
            filename=files
        )
        logging.info(f"Processed files: {len(files)}")
        return ddf
    except Exception as e:
        logging.error(f'Error reading parquet files: {e}', exc_info=True)
        return dd.from_pandas(pd.DataFrame(), npartitions=1)  # Return an empty DataFrame if error occurs

def attach_parameters(ddf, parameters_file):
    try:
        parameters = pd.read_excel(parameters_file)
        features = ['Power (W)', 'Speed (mm/s)', 'Focus', 'Beam radius (um)']
        parameters = parameters[['Part Number'] + features]
        parameters['Part Number'] = parameters['Part Number'].astype(str)  # Ensuring 'Part Number' is string

        # Convert pandas DataFrame to Dask DataFrame
        parameters_ddf = dd.from_pandas(parameters, npartitions=1)
        ddf = ddf.merge(parameters_ddf, on='Part Number', how='left')
        return ddf
    except Exception as e:
        logging.error(f'Failed to attach parameters: {e}', exc_info=True)
        return ddf

def calculate_NVED(ddf):
    try:
        # Constants
        A = 0.3
        l = 7.5
        rho = 8.395e-3
        Cp = 0.43
        Tm = 1300
        T0 = 300
        h = 75

        # Ensure all required columns are present
        for column in ['Power (W)', 'Speed (mm/s)', 'beam_radius']:
            if column not in ddf.columns:
                ddf[column] = 0  # Assigning default value of 0

        ddf['E*'] = (A * ddf['Power (W)'] / (2 * ddf['Speed (mm/s)'] * l * ddf['beam_radius'] * 1e3) *
                     1 / (0.67 * rho * Cp * (Tm - T0)))
        ddf['1/h*'] = ddf['beam_radius'] * 1e3 / h
        return ddf
    except Exception as e:
        logging.error(f'Failed to calculate NVED: {e}', exc_info=True)
        return ddf

def main():
    client = Client()  # Start a Dask client to manage workers
    try:
        directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
        logging.info(f"Processing data in directory: {directory}")

        ddf = read_parquet_directory(directory)
        ddf = attach_parameters(ddf, 'parameters.xlsx')
        ddf = calculate_NVED(ddf)

        df = ddf.compute()  # Compute the Dask DataFrame to a Pandas DataFrame

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
