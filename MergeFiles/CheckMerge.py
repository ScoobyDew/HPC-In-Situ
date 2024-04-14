# Create a script which checks the collumns of a merged parquet file

import os
import pandas as pd
import logging
from multiprocessing import Pool

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

filepath = '/mnt/parscratch/users/eia19od/combined_data.parquet'

def main():
    # Read the merged parquet file
    try:
        df = pd.read_parquet(filepath)
        logging.info(f"Read parquet file: {filepath}")
    except Exception as e:
        logging.error(f"Error reading {filepath}: {str(e)}")
        df = pd.DataFrame()

    # Check the columns of the merged DataFrame
    try:
        if not df.empty:
            logging.info(f"Columns in merged DataFrame: {df.columns}")
        else:
            logging.error("Merged DataFrame is empty. No data to process.")

    except Exception as e:
        logging.error(f"An error occurred during column check: {str(e)}")

    # Check the first few rows of the merged DataFrame
    try:
        if not df.empty:
            logging.info(f"First few rows of merged DataFrame: {df.head()}")
        else:
            logging.error("Merged DataFrame is empty. No data to process.")

    except Exception as e:
        logging.error(f"An error occurred during row check: {str(e)}")

if __name__ == "__main__":
    main()
