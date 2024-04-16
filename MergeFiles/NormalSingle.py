import os
import numpy as np
import dask.dataframe as dd
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from scipy.stats import pearsonr
import time
import gc
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, filename='plot.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def plot_subplot(df, x_var, y_var, x_label, y_label, title, ax, cmap, fig):
    """Function to plot each subplot with histogram, regression, and annotations."""
    plt.switch_backend('agg')  # Use non-interactive backend suitable for scripts
    h = ax.hist2d(df[x_var], df[y_var], bins=50, norm=LogNorm(), cmap=cmap)
    cbar = fig.colorbar(h[3], ax=ax)
    cbar.set_label('Counts')
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

    # Linear regression and correlation
    slope, intercept = np.polyfit(df[x_var], df[y_var], 1)
    r_value, _ = pearsonr(df[x_var], df[y_var])
    line_eq = f'y = {slope:.2f}x + {intercept:.2f}'
    corr_info = f'Pearson r = {r_value:.2f}'

    # Plot regression line
    x_vals = np.array(ax.get_xlim())
    y_vals = intercept + slope * x_vals
    ax.plot(x_vals, y_vals, 'r--')

    # Annotate with line equation and Pearson's r
    ax.annotate(f'{line_eq}\n{corr_info}', xy=(0.05, 0.95), xycoords='axes fraction',
                verticalalignment='top', fontsize=10, color='red')

def load_data(filepath, columns):
    """Load only specified columns to conserve memory."""
    df = dd.read_parquet(filepath, columns=columns)
    return df.compute()  # Convert to Pandas DataFrame for easier plotting

def main():
    filepath = '/mnt/parscratch/users/eia19od/combined_derived.parquet'
    plot_vars = [
        (['mp_intensity', 'Normalised Enthalpy'], 'Meltpool Intensity', '$\\frac{\\Delta{H}}{h}$', '$\\frac{\\Delta{H}}{h}$ vs Meltpool Intensity', 'cividis')
    ]

    fig, axs = plt.subplots(2, 2, figsize=(15, 15))

    for ax, (columns, x_label, y_label, title, cmap) in zip(axs.flat, plot_vars):
        df = load_data(filepath, columns)
        plot_subplot(df, columns[0], columns[1], x_label, y_label, title, ax, cmap, fig)
        del df  # Delete the dataframe to free memory
        gc.collect()  # Collect garbage to free memory

    fig.suptitle('2D Histograms of Insitu Parameters and Associated Dimensionless Quantities')
    plt.tight_layout(rect=[0, 0, 1, 0.95])

    # Save the plot
    if not os.path.exists("images"):
        os.mkdir("images")
    plt.savefig(f"images/2d_hist_{time.strftime('%Y_%m_%d_%H_%M_%S')}.png", dpi=500)
    plt.close(fig)  # Close the figure to free memory
    logging.info("Plot saved as PNG.")
    logging.info("Processing Finished")

if __name__ == '__main__':
    main()
