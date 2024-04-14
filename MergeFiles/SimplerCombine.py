import pandas as pd
import pyarrow
import os
import logging
from multiprocessing import Pool
import gc

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_and_process_file(file):
    try:
        df = pd.read_parquet(file, engine='pyarrow')
        if 'Part Number' not in df.columns:
            df['Part Number'] = os.path.splitext(os.path.basename(file))[0]
        logging.info(f"Processed file: {os.path.splitext(os.path.basename(file))[0]}")
        gc.collect()

        return df
    except Exception as e:
        logging.error(f"Error processing {file}: {e}")
        return pd.DataFrame()  # Return empty DataFrame in case of error

def main():
    # Create an empty DataFrame to store the combined data
    combined_df = pd.DataFrame()

    # Obtain a list of parquet files in the specified directory
    directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]

    # Create a process pool and process the files in parallel
    with Pool(2) as p:
        dataframes = p.map(read_and_process_file, files)

    logging.info('Processing completed successfully')
    logging.info('Concatenating DataFrames...')
    # Concatenate the resulting DataFrames
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Save the combined DataFrame to a new parquet file
    output_file = '/mnt/parscratch/users/eia19od/combined_data.parquet'
    combined_df.to_parquet(output_file, index=False)

    # Log completion message
    logging.info('Successfully saved combined data to parquet file')

if __name__ == "__main__":
    main()

