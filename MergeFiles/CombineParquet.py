import os
import pandas as pd
import logging
from multiprocessing import Pool

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
    with Pool(32) as p:  # Utilizing 32 cores
        dataframes = p.map(read_parquet_file, files)
    return pd.concat(dataframes, ignore_index=True)

def attach_parameters(df, parameters_file):
    try:
        parameters = pd.read_excel(parameters_file)
        parameters['Part Number'] = parameters['Part Number'].astype(str)
        df = df.merge(parameters, on='Part Number', how='left')
        return df
    except Exception as e:
        logging.error(f"Error merging parameters: {str(e)}")
        return df  # Proceed with original data in case of error

def main():
    try:
        directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
        logging.info(f"Processing data in directory: {directory}")
        df = read_parquet_directory(directory)
        df = attach_parameters(df, 'part_parameters.xlsx')

        # Save the processed DataFrame to a new parquet file
        output_file = '/users/eia19od/in_situ/HPC-In-Situ/MergeFiles'
        df.to_parquet(output_file, index=False)
        if df.empty:
            logging.error("Processed DataFrame is empty. No data to process.")
        else:
            logging.info('Processing completed successfully')
            print(df.head())
            print(df.describe())

    except Exception as e:
        logging.error(f"An error occurred during processing: {str(e)}")

if __name__ == "__main__":
    main()
