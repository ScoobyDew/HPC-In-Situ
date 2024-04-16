import logging
import os
import numpy as np
import dask.dataframe as dd
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from scipy.stats import pearsonr
import time
import multiprocessing as mp

# Setup logging
logging.basicConfig(level=logging.INFO, filename='plot.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def plot_subplot(args):
    df, x_var, y_var, x_label, y_label, title = args
    # Create the plot with non-interactive backend to prevent it from trying to show in an HPC environment
    plt.switch_backend('agg')
    fig, ax = plt.subplots(figsize=(8, 8))

    # Plotting histogram
    h = ax.hist2d(df[x_var], df[y_var], bins=50, norm=LogNorm(), cmap='cividis')
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

    # Save each subplot as an individual image
    if not os.path.exists("images"):
        os.mkdir("images")
    fig.savefig(f"images/{title.replace(' ', '_').replace('$', '').replace('\\', '')}_{time.strftime('%Y_%m_%d_%H_%M_%S')}.png", dpi=300)
    plt.close(fig)

def main():
    filepath = '/mnt/parscratch/users/eia19od/combined_derived.parquet'
    filters = [
        ('pyro2', '>', 0),
        ('mp_intensity', '>', 0),
        ('Normalised Enthalpy', '>', 0),
        ('E*0', '>', 0)
    ]
    df = dd.read_parquet(filepath, filters=filters).compute()

    plot_vars = [
        (df, 'mp_intensity', 'Normalised Enthalpy', 'Meltpool Intensity', '$\\frac{\\Delta{H}}{h}$', '$\\frac{\\Delta{H}}{h}$ vs Meltpool Intensity'),
        (df, 'pyro2', 'Normalised Enthalpy', 'Pyrometer Voltage (mV)', '$\\frac{\\Delta{H}}{h}$', '$\\frac{\\Delta{H}}{h}$ vs Pyrometer Voltage'),
        (df, 'mp_intensity', 'E*0', 'Meltpool Pixel Count', '$E_0^*$', '$E_0^*$ vs Meltpool Pixel Count'),
        (df, 'pyro2', 'E*0', 'Pyrometer Voltage (mV)', '$E_0^*$', '$E_0^*$ vs Pyrometer Voltage')
    ]

    # Set up a pool of processes
    pool = mp.Pool(processes=mp.cpu_count())

    # Map plot_subplot function to each set of arguments
    pool.map(plot_subplot, plot_vars)

    pool.close()
    pool.join()
    logging.info("Processing Finished")

if __name__ == '__main__':
    main()
