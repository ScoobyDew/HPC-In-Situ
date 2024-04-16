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

        x = df_pd['mp_width']
        y = df_pd['mp_length']

        # Create figure and axis
        fig, ax = plt.subplots(figsize=(10, 8))

        # Determine the range of data
        x_min, x_max = x.min() - 0.5, x.max() + 0.5
        y_min, y_max = y.min() - 0.5, y.max() + 0.5

        # Define bins such that each integer value falls into its own bin
        x_bins = np.arange(x_min, x_max + 1, 1)
        y_bins = np.arange(y_min, y_max + 1, 1)

        # Using matplotlib's hist2d function with the cividis colormap
        h = ax.hist2d(x, y, bins=[x_bins, y_bins], norm=LogNorm(), cmap='cividis')

        # Adding color bar
        cbar = fig.colorbar(h[3], ax=ax)
        cbar.set_label('Counts')

        # Adding titles and labels
        ax.set_title('2D Histogram of Melt Pool Width and Length with Log Scaling and Cividis Colormap')
        ax.set_xlabel('Melt Pool Width')
        ax.set_ylabel('Melt Pool Length')



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
