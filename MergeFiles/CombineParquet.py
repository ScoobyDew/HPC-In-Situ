import os
import dask.dataframe as dd
import pandas as pd
import logging
from dask.distributed import Client

logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_parquet_directory(directory):
    logging.info(f"Reading parquet files from directory: {directory}")
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
    if not files:
        logging.info("No parquet files found.")
        return dd.from_pandas(pd.DataFrame(), npartitions=32)

    ddf = dd.read_parquet(files, engine='pyarrow')
    return ddf

def attach_parameters(ddf, parameters_file):
    try:
        parameters = pd.read_excel(parameters_file)
        parameters_ddf = dd.from_pandas(parameters, npartitions=32)
        parameters_ddf['Part Number'] = parameters_ddf['Part Number'].astype(str)
        ddf['Part Number'] = ddf['Part Number'].astype(str)
        ddf = ddf.merge(parameters_ddf, on='Part Number', how='left')
        return ddf
    except Exception as e:
        logging.error(f'Failed to attach parameters: {e}')
        return ddf

def calculate_NVED(ddf):
    ddf['E*'] = (0.3 * ddf['Power (W)'] / (2 * ddf['Speed (mm/s)'] * 7.5 * ddf['beam_radius'] * 1e3) *
                 1 / (0.67 * 8.395e-3 * 0.43 * (1300 - 300)))
    ddf['1/h*'] = ddf['beam_radius'] * 1e3 / 75
    return ddf

def main():
    #Allow client to use 64G
    client = Client(memory_limit='64GB')
    try:
        directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
        ddf = read_parquet_directory(directory)
        ddf = attach_parameters(ddf, 'part_parameters.xlsx')
        ddf = calculate_NVED(ddf)
        df = ddf.compute()
        if df.empty:
            logging.error("Processed DataFrame is empty.")
        else:
            print(df.head())
    except Exception as e:
        logging.error(f'An error occurred: {e}')
    finally:
        client.close()

if __name__ == "__main__":
    main()
