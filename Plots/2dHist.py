import logging
import os
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import time
import pickle
from matplotlib.colors import LogNorm

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

# Find time at the start of the processing (date and time)
start_time = time.strftime('%Y-%m-%d %H:%M:%S')

def main():
    logging.info("Starting processing")
    filepath = '/mnt/parscratch/users/eia19od/combined_params.parquet'

    try:
        # Read the merged parquet file using dask
        df = dd.read_parquet(filepath, columns=['mp_width', 'mp_length'])
        logging.info(f"Successfully read parquet file: {filepath}")

        # Replace all zero values with NaN
        df = df.replace(0, np.nan)
        
        # Check for duplicate indices
        if df.index.duplicated().any().compute():
            logging.error("Duplicate indices found in the DataFrame.")
            raise ValueError("Duplicate indices found in the DataFrame.")

        # Convert to pandas dataframe
        df_pd = df.compute()
        logging.info("Converted to pandas dataframe")

        # Plotting the data
        plt.figure(figsize=(10, 8))
        sns.histplot(
            data=df_pd,
            x='mp_width',
            y='mp_length',
            cmap='cividis',
            binwidth=(1, 1),
            cbar=True,
            norm=LogNorm(vmin=1, vmax=7.1e6),
        )
        plt.title('2D Histogram of meltpool width and length', fontsize=16)
        plt.xlabel('Melt Pool Width (pixels)')
        plt.ylabel('Melt Pool Length (pixels)')

        # Ensure the 'images' directory exists
        if not os.path.exists("images"):
            os.mkdir("images")

        # Save the plot as PNG
        plt.savefig(f"images/density_contour_{start_time}.png")
        logging.info("Plot saved as PNG.")

        # Save the plot as a pickle file
        with open(f"images/density_contour_{start_time}.pkl", 'wb') as f:
            pickle.dump(plt.gcf(), f)
        logging.info("Plot saved as pickle.")

    except Exception as e:
        logging.error(f"Error during processing: {e}", exc_info=True)
    finally:
        plt.clf()  # Clear the plotting area

    logging.info("Processing Finished")

if __name__ == "__main__":
    main()
