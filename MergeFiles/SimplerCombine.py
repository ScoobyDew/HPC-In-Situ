import pandas as pd
import os
import logging
from multiprocessing import Pool
import gc

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_and_process_file(file):
    # Define columns to read - adjust this list based on your actual data schema
    columns_to_read = ['Part Number', 'AnotherColumn', 'YetAnotherColumn']

    try:
        # Only read specified columns
        df = pd.read_parquet(file, engine='pyarrow', columns=columns_to_read)

        # Convert columns to float32 if needed
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = df[col].astype('float32')

        if 'Part Number' not in df.columns:
            df['Part Number'] = os.path.splitext(os.path.basename(file))[0]

        logging.info(f"Processed file: {os.path.splitext(os.path.basename(file))[0]}")
        gc.collect()
        return df
    except Exception as e:
        logging.error(f"Error processing {file}: {e}")
        return None  # Return None in case of error to avoid breaking concatenation logic

def main():
    # Obtain a list of parquet files in the specified directory
    directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]

    # Initialize an empty DataFrame or use None for first concatenation
    combined_df = None

    # Process each file individually to reduce memory usage
    for file in files:
        df = read_and_process_file(file)
        if df is not None:
            if combined_df is None:
                combined_df = df  # First DataFrame sets the combined_df
            else:
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            gc.collect()  # Optionally force garbage collection after each concat

    if combined_df is not None:
        # Save the combined DataFrame to a new parquet file
        output_file = '/mnt/parscratch/users/eia19od/combined_data.parquet'
        combined_df.to_parquet(output_file, index=False)
        logging.info('Successfully saved combined data to parquet file')

    logging.info('Processing completed successfully')

if __name__ == "__main__":
    main()
    logging.info("Done")