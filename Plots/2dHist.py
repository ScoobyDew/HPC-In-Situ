"""
Plots two of the parameteres of the data from combined_params.parquet against each other
"""
import logging
import cudf.pandas
cudf.pandas.install()

import pandas as pd
import matplotlib.pyplot as plt
import dask
import dask.dataframe as dd
dask.config.set({"dataframe.backend": "cudf"})

def main():
    # Read the merged parquet file
    filepath = '/mnt/parscratch/users/eia19od/combined_params.parquet'
    try:
        df = dd.read_parquet(filepath)
        logging.info(f"Read parquet file: {filepath}")
    except Exception as e:
        logging.error(f"Error reading {filepath}: {str(e)}")
        return

    # try to plot mp_width against mp_length in 2d histogram
    try:
        plt.hist2d(df['mp_width'], df['mp_length'], bins=(100, 100), cmap=plt.cm.jet)
        plt.show()
    except Exception as e:
        logging.error(f"Error plotting: {str(e)}")
        return

if __name__ == "__main__":
    main()