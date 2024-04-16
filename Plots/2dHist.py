import logging
import os
import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns
import time
import pickle
from matplotlib.colors import LogNorm, Normalize
from matplotlib.ticker import MaxNLocator

# Setup logging
logging.basicConfig(level=logging.INFO, filename='hist.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Starting processing")
    filepath = '/mnt/parscratch/users/eia19od/combined_params.parquet'

    try:
        # Read the merged parquet file using dask, filtering out zeros in 'mp_width' and 'mp_length'
        filters = [('mp_width', '>', 0), ('mp_length', '>', 0)]
        df = dd.read_parquet(filepath, columns=['mp_width', 'mp_length'], filters=filters)
        logging.info(f"Successfully read parquet file: {filepath}")

        # Convert Dask DataFrame to Pandas DataFrame
        df_pd = df.compute()
        logging.info("Converted to pandas dataframe")

        # Prepare the normalization
        norm = LogNorm(vmin=1, vmax=7.1e6)

        # Plotting
        plt.figure(figsize=(10, 8))
        sns.histplot(
            data=df_pd,
            x='mp_width',
            y='mp_length',
            cmap='cividis',
            binwidth=(1, 1),
            cbar_kws={
                'ticks': MaxNLocator(2),
                'format': '%.e'
            },
            norm=norm
        )
        plt.title('2D Histogram of meltpool width and length', fontsize=16)
        plt.xlabel('Melt Pool Width (pixels)')
        plt.ylabel('Melt Pool Length (pixels)')

        # Ensure the 'images' directory exists
        if not os.path.exists("images"):
            os.mkdir("images")

        # Save the plot as PNG
        plt.savefig(f"images/density_contour_{time.strftime('%Y_%m_%d %H_%M_%S')}.png")
        logging.info("Plot saved as PNG.")

        # Save the plot as a pickle file
        with open(f"images/density_contour_{time.strftime('%Y_%m_%d %H_%M_%S')}.pkl", 'wb') as f:
            pickle.dump(plt.gcf(), f)
        logging.info("Plot saved as pickle.")

    except Exception as e:
        logging.error(f"Error during processing: {e}", exc_info=True)
    finally:
        plt.clf()  # Clear the plotting area

    logging.info("Processing Finished")

if __name__ == "__main__":
    main()
