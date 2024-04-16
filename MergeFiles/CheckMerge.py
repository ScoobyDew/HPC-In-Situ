"""
Create a script which checks the collumns of a merged parquet file
- Successful 11:00 15-04-24
"""

import os
import pandas as pd
import logging
from multiprocessing import Pool
import dask.dataframe as dd
client = Client(memory_limit='2GB')

# Import a garbage collector
import gc
# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

filepath = '/mnt/parscratch/users/eia19od/combined_derived.parquet'

def main():
    logging.info("Starting processing")
    logging.info(f"Reading parquet file: {filepath}")
    # Read the merged parquet file
    try:
        df = dd.read_parquet(filepath)
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

    # Try to print the first 5 rows of ['Part Number', 'Power (W)', 'Speed (mm/s)', 'Focus', 'Beam radius (um)']
    try:
        head = df[['Power (W)', 'mp_width']].head()
        logging.info(f"Head: {head}")
    except Exception as e:
        logging.error(f"Error getting head merged params: {str(e)}")

    # Try to get .head() of the dataframe
    try:
        head = df.head()
        logging.info(f"Head: {head}")
    except Exception as e:
        logging.error(f"Error getting head: {str(e)}")


if __name__ == "__main__":
    main()
