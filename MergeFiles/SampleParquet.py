"""
Load the full parquet file and sample the data to make a reduced size parquet file.
"""


import logging
import dask.dataframe as dd
import os
import time
import numpy as np
import gc
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, filename='sample_parquet.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def sample_data(filepath, frac=0.0001):
    """Sample the data and save to a new parquet file."""
    logging.info(f"Reading parquet file: {filepath}")
    df = dd.read_parquet(filepath)
    logging.info(f"Read parquet file: {filepath}")

    logging.info(f"Sampling data with fraction: {frac}")
    df_sampled = df.sample(frac=frac)

    # Save the sampled data to a new parquet file
    new_filepath = os.path.splitext(filepath)[0] + f"_sampled_{frac}.parquet"
    logging.info(f"Saving sampled data to: {new_filepath}")
    df_sampled.to_parquet(new_filepath, engine='pyarrow')
    logging.info(f"Saved sampled data to: {new_filepath}")

    return new_filepath