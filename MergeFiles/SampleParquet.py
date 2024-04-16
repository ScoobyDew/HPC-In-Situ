import logging
import dask.dataframe as dd
import pandas as pd
import os
import gc

# Setup logging
logging.basicConfig(level=logging.INFO, filename='sample.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def sample_data(filepath, frac=0.0001):
    """Sample the data and save to a new parquet file."""
    logging.info(f"Reading parquet file: {filepath}")
    df = dd.read_parquet(filepath)  # Loading with Dask
    logging.info(f"Read parquet file: {filepath}")

    logging.info(f"Sampling data with fraction: {frac}")
    df_sampled = df.sample(frac=frac).compute()  # Computing to convert to Pandas DataFrame

    # Ensure the type after compute() is indeed a Pandas DataFrame
    if isinstance(df_sampled, pd.DataFrame):
        logging.info("Confirmed sampled data is a Pandas DataFrame")
    else:
        logging.error("Data is not a Pandas DataFrame")
        return  # Exit if not a Pandas DataFrame

    logging.info(f"Columns: {df_sampled.columns}")
    logging.info(f"Data types: {df_sampled.dtypes}")

    # Prepare to save the sampled data to a new parquet file
    new_filepath = '/mnt/parscratch/users/eia19od/combined_sampled.parquet'
    logging.info(f"Saving sampled data to: {new_filepath}")
    df_sampled.to_parquet(new_filepath, engine='pyarrow', index=False)  # Saving with Pandas
    logging.info(f"Saved sampled data to: {new_filepath}")

    # Clean up to free memory
    del df_sampled
    gc.collect()

def main():
    filepath = '/mnt/parscratch/users/eia19od/combined_derived.parquet'
    sample_data(filepath)

if __name__ == "__main__":
    main()
