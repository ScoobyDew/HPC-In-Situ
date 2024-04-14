import pandas as pd
import os
from multiprocessing import Pool
import logging

logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')
def read_and_process_file(file):
    df = pd.read_parquet(file)
    if 'Part Number' not in df.columns:
        df['Part Number'] = os.path.splitext(os.path.basename(file))[0]
        # log the file name
        logging.info(f"Processing file: {file}")
        return df

def main():
    directory = '/mnt/parscratch/users/eia19od/Cleaned'

    # Get a list of all Parquet files in the directory
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]

    # Use a multiprocessing Pool to read and process the files in parallel
    with Pool(4) as p:
        dataframes = p.map(read_and_process_file, files)

    logging.info(f"Processed {len(dataframes)} files")
    logging.info(f"Combining {len(dataframes)} DataFrames...")

    # Combine all DataFrames into a single DataFrame
    df = pd.concat(dataframes, ignore_index=True)
    logging.info(f"Combined DataFrame shape: {df.shape}")
    # Save the processed DataFrame to a new parquet file
    output_file = '/mnt/parscratch/users/eia19od/combined.parquet'

    logging.info(f"Saving combined DataFrame to {output_file}...")
    df.to_parquet(output_file, index=False)
    logging.info("Processing completed successfully")


if __name__ == "__main__":
    main()
    logging.info("Done")