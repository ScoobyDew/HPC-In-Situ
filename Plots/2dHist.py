"""
Plots two of the parameteres of the data from combined_params.parquet against each other
"""
import logging
import os
import dask
import plotly.express as px
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# use a clear background for seaborn plots
sns.set_style("whitegrid")

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

        # Plotting using Seaborn
        plt.figure(figsize=(10, 8))
        sns.kdeplot(data=df_sample, x='mp_width', y='mp_length', fill=True, thresh=0, levels=100, cmap="mako")
        plt.title('Density Contour of mp_width vs mp_length')
        plt.xlabel('Melt Pool Width')
        plt.ylabel('Melt Pool Length')

        # Ensure the 'images' directory exists
        if not os.path.exists("images"):
            os.mkdir("images")

        # Save the plot as PNG
        plt.savefig("images/density_contour.png")
        logging.info("Plot saved as PNG.")

        # Save the plot as a pickle file
        with open("images/density_contour.pkl", 'wb') as f:
            pickle.dump(plt.gcf(), f)
        logging.info("Plot saved as pickle.")

    finally:
        # Clear the plotting area for any further plots
        plt.clf()


    logging.info("Processing Finished")

    # save the plot to a file


if __name__ == "__main__":
    main()