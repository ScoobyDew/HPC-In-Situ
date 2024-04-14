import pandas as pd
import os
from multiprocessing import Pool
import logging

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

def process_files(files):
    dataframes = []
    for file in files:
        df = read_and_process_file(file)
        if not df.empty:
            dataframes.append(df)
    return dataframes

def main():
    directory = '/mnt/parscratch/users/eia19od/Cleaned'
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]

    # Number of processes
    num_processes = 8

    # Split files into batches to control resource usage
    files_per_batch = len(files) // num_processes + (len(files) % num_processes > 0)
    batches = [files[i:i + files_per_batch] for i in range(0, len(files), files_per_batch)]

    # Use multiprocessing Pool to read and process the files in parallel, batch by batch
    with Pool(num_processes) as p:
        results = p.map(process_files, batches)

    # Flatten the list of dataframes
    dataframes = [df for sublist in results for df in sublist]

    logging.info(f"Processed {len(dataframes)} files")
    logging.info("Combining DataFrames...")

    # Combine all DataFrames into a single DataFrame
    if dataframes:
        df = pd.concat(dataframes, ignore_index=True)
        logging.info(f"Combined DataFrame shape: {df.shape}")

        # Save the processed DataFrame to a new parquet file
        output_file = '/mnt/parscratch/users/eia19od/combined.parquet'
        df.to_parquet(output_file, index=False)
        logging.info(f"Saved combined DataFrame to {output_file}")

if __name__ == "__main__":
    main()
    print("Done")
