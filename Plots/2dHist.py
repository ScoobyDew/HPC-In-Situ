"""
Plots two of the parameteres of the data from combined_params.parquet against each other
"""
import logging
import os
import dask
import plotly.express as px

import pandas as pd
import matplotlib.pyplot as plt

import dask.dataframe as dd
# dask.config.set({"dataframe.backend": "cudf"})

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting processing")
    filepath = '/mnt/parscratch/users/eia19od/combined_params.parquet'

    try:
        # Read the merged parquet file using dask_cudf
        df = dd.read_parquet(filepath)
        logging.info(f"Read parquet file: {filepath}")

        # Plotting using Plotly Express
        fig = px.density_contour(
            df,
            x='mp_width',
            y='mp_length',
            nbinsx=100,
            nbinsy=100,
            title='Density Contour of mp_width vs mp_length'
        )
        fig.update_layout(
            xaxis_title='melt pool width',
            yaxis_title='melt pool length'
        )

        # Save the plot to a file
        if not os.path.exists("images"):
            os.mkdir("images")
        fig.write_image("images/fig1.png")
    except Exception as e:
        logging.error(f"Error encountered: {str(e)}")

if __name__ == "__main__":
    main()