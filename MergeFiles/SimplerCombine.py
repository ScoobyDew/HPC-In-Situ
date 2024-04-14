import pandas as pd
import os
from multiprocessing import Pool
import logging
import gc


# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_and_process_file(file):
    try:
        df = pd.read_parquet(file)
        if 'Part Number' not in df.columns:
            df['Part Number'] = os.path.splitext(os.path.basename(file))[0]
        logging.info(f"Processed file: {file}")
        return df
    except Exception as e:
        logging.error(f"Error processing {file}: {e}")
        return pd.DataFrame()  # Return empty DataFrame in case of error

def main():
    # Create an empty DataFrame to store the combined data
    combined_df = pd.DataFrame()

    # Obtain a list of parquet files in the specified directory
    directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')

    # Over 4 cores, read the parquet files and append them to the combined DataFrame
    with Pool(4) as p:
        dataframes = p.map(read_and_process_file, [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')])
        combined_df = pd.concat(dataframes, ignore_index=True)

    # Save the combined DataFrame to a new parquet file
    output_file = '/mnt/parscratch/users/eia19od/combined_data.parquet'

    combined_df.to_parquet(output_file, index=False)

    # Log completion message
    logging.info('Processing completed successfully')

if __name__ == "__main__":
    main()