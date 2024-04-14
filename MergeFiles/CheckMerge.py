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

    # Convert pyro2 column to int8 and change to "Pyro"
    try:
        if 'pyro2' in df.columns:
            df['pyro2'] = df['pyro2'].astype('int8')
            df.rename(columns={'pyro2': 'Pyro'}, inplace=True)
            logging.info("Converted pyro2 column to int8 and renamed to Pyro")
        else:
            logging.error("pyro2 column not found in DataFrame. No changes made.")
    except Exception as e:
        logging.error(f"An error occurred during column conversion: {str(e)}")

    # Convert pyro and part number to int8
    try:
        if 'Pyro' in df.columns:
            df['Pyro'] = df['Pyro'].astype('int8')
            logging.info("Converted Pyro column to int8")
        else:
            logging.error("Pyro column not found in DataFrame. No changes made.")
    except Exception as e:
        logging.error(f"An error occurred during column conversion: {str(e)}")

    try:
        if 'Part Number' in df.columns:
            df['Part Number'] = df['Part Number'].astype('int8')
            logging.info("Converted Part Number column to int8")
        else:
            logging.error("Part Number column not found in DataFrame. No changes made.")
    except Exception as e:
        logging.error(f"An error occurred during column conversion: {str(e)}")


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
