import os
import dask_cudf
import cudf
import logging
from dask.distributed import Client

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def read_parquet_directory(directory):
    logging.info(f"Reading parquet files from directory: {directory}")
    try:
        # Using dask_cudf to read parquet files in parallel
        ddf = dask_cudf.read_parquet(os.path.join(directory, '*.parquet'))
        return ddf
    except Exception as e:
        logging.error(f'Error reading parquet files: {e}', exc_info=True)
        return dask_cudf.from_cudf(cudf.DataFrame(), npartitions=1)  # Return an empty dask_cudf DataFrame


def attach_parameters(ddf, parameters_file):
    try:
        parameters = cudf.read_excel(parameters_file)
        features = ['Power (W)', 'Speed (mm/s)', 'Focus', 'Beam radius (um)']
        necessary_columns = ['Part Number'] + features
        parameters = parameters[necessary_columns]

        # Merging using dask_cudf merge function
        ddf = ddf.merge(parameters, on='Part Number', how='left')
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
        ddf = attach_parameters(ddf, 'parameters.xlsx')
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
        logging.error(f'An error occurred during processing: {e}', exc_info=True)
    finally:
        client.close()


if __name__ == "__main__":
    main()
