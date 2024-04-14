import os
import logging
import dask.dataframe as dd

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

try:
    import cudf
    from dask_cuda import LocalCUDACluster
    from dask.distributed import Client

    cluster = LocalCUDACluster()
    client = Client(cluster)
    logging.info("Using cuDF and Dask")
except ImportError:
    import pandas as pd
    logging.info("Using Pandas")
    pass


def main():
    logging.info("Starting processing")
    try: # Read the merged parquet file
        filepath = '/mnt/parscratch/users/eia19od/combined_data.parquet'
        df = dd.read_parquet(filepath)
        df['Part Number'] = df['Part Number'].astype(int)
        logging.info(f"Read parquet file: {filepath}")

    except Exception as e:
        logging.error(f"Error reading {filepath}: {str(e)}")
        df = pd.DataFrame()

    try: # Read the part parameters file
        parameters_file = 'part_parameters.xlsx'

        features = [
            'Part Number',
            'Power (W)',
            'Speed (mm/s)',
            'Focus',
            'Beam radius (um)'
        ]
        logging.info(f"Reading parameters file: {parameters_file}")
        parameters = pd.read_excel(parameters_file)
        parameters['Part Number'] = parameters['Part Number'].astype(int)
        logging.info(f"Read parameters file: {parameters_file}")
    except Exception as e:
        logging.error(f"Error reading {parameters_file}: {str(e)}")
        parameters = pd.DataFrame()

    # Merge the data with the parameters
    try:
        if not df.empty and not parameters.empty:
            logging.info("Merging parameters with data")
            df = df.merge(parameters, on='Part Number', how='left')
            logging.info("Merged parameters with data")
        else:
            logging.error("No data to merge")
    except Exception as e:
        logging.error(f"An error occurred during merging: {str(e)}")

    try: # Save the processed DataFrame to a new parquet file
        output_file = '/mnt/parscratch/users/eia19od/combined_data_with_parameters.parquet'
        df.to_parquet(output_file, index=False)
        if df.empty:
            logging.error("Processed DataFrame is empty. No data to process.")
        else:
            logging.info('Processing completed with no errors')
            print(df.head())
            print(df.describe())
    except Exception as e:
        logging.error(f"An error occurred during processing: {str(e)}")

if __name__ == "__main__":
    main()
