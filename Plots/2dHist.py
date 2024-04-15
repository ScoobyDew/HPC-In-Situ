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

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def main():

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting processing")
    filepath = '/mnt/parscratch/users/eia19od/combined_params.parquet'

    try:
        # Read the merged parquet file using dask_cudf
        df = dd.read_parquet(filepath, columns=['mp_width', 'mp_length'])
        logging.info(f"Successfully read parquet file: {filepath}")
        df_sample = df.sample(frac=0.1).compute()  # Adjust the fraction as needed
        logging.info(f"Successfully sampled the data")

        logging.info(f"Plotting the data")
        # Plotting using Plotly Express
        fig = px.density_contour(
            df_sample,
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
        fig.write_html("images/fig1.html")  # To save as HTML

    except Exception as e:
        logging.error(f"Error encountered: {str(e)}")

    logging.info("Processing complete")

    # save the plot to a file


if __name__ == "__main__":
    main()