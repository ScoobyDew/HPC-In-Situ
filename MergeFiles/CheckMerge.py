# Create a script which checks the collumns of a merged parquet file

import os
import pandas as pd
import logging
from multiprocessing import Pool

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

filepath = '/mnt/parscratch/users/eia19od/combined_data_with_parameters.parquet'

def main():
    # Read the merged parquet file
    try:
        df = pd.read_parquet(filepath)
        logging.info(f"Read parquet file: {filepath}")
    except Exception as e:
        logging.error(f"Error reading {filepath}: {str(e)}")
        df = pd.DataFrame()


    # Try to get the columns of the dataframe
    try:
        columns = df.columns
        logging.info(f"Columns: {columns}")
    except Exception as e:
        logging.error(f"Error getting columns: {str(e)}")
        columns = []

    # Try to get .head() of the dataframe
    try:
        head = df.head()
        logging.info(f"Head: {head}")
    except Exception as e:
        logging.error(f"Error getting head: {str(e)}")


if __name__ == "__main__":
    main()
