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
    with Pool(40) as p:
        dataframes = p.map(read_and_process_file, files)

    # Combine all DataFrames into a single DataFrame
    df = pd.concat(dataframes, ignore_index=True)

    # Save the processed DataFrame to a new parquet file
    output_file = '/users/eia19od/in_situ/HPC-In-Situ/MergeFiles/combined.parquet'
    df.to_parquet(output_file, index=False)



if __name__ == "__main__":
    main()
    print("Done")