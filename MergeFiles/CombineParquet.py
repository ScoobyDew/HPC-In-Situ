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
    try:
        df = pd.read_parquet(file)
        if 'Part Number' not in df.columns:
            df['Part Number'] = os.path.splitext(os.path.basename(file))[0]  # Assuming part number is the file name without extension
        return df
    except Exception as e:
        logging.error(f"Error reading {file}: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame in case of error

def read_parquet_directory(directory):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
    delayed_frames = [delayed(read_parquet_file)(file) for file in files]
    meta = pd.read_parquet(files[0]).dtypes.to_dict() if files else {}
    if 'Part Number' not in meta:
        meta['Part Number'] = str
    ddf = dd.from_delayed(delayed_frames, meta=meta)
    return ddf

def attach_parameters(ddf, parameters_file):
    try:
        parameters = pd.read_excel(parameters_file)
        parameters['Part Number'] = parameters['Part Number'].astype(str)
        parameters_ddf = dd.from_pandas(parameters, npartitions=1)
        ddf = ddf.merge(parameters_ddf, on='Part Number', how='left')
        return ddf
    except Exception as e:
        logging.error(f"Error merging parameters: {str(e)}")
        return ddf  # Proceed with original data in case of error

def main():
    cluster = LocalCluster(memory_limit='2GB', n_workers=4, threads_per_worker=1)
    client = Client(cluster)
    try:
        directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
        logging.info(f"Processing data in directory: {directory}")
        ddf = read_parquet_directory(directory)
        ddf = attach_parameters(ddf, 'part_parameters.xlsx')
        df = ddf.compute()
        if df.empty:
            logging.error("Processed DataFrame is empty. No data to process.")
        else:
            logging.info('Processing completed successfully')
            print(df.head())
            print(df.describe())
    except Exception as e:
        logging.error(f"An error occurred during processing: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    main()
