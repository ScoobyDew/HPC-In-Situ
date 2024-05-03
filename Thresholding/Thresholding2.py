import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import time



# Set up logging
logging.basicConfig(level=logging.INFO, filename='/users/eia19od/in_situ/HPC-In-Situ/Thresholding/thresh2.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')
def plot_violin(data, x_col, y_col, title, filename, palette=None):
    """
    Function to create a violin plot for specified columns and save it as an image.

    Args:
    data (DataFrame): The data to plot.
    x_col (str): The column name for the x-axis.
    y_col (str): The column name for the y-axis.
    title (str): Title of the plot.
    filename (str): Path to save the plot image.
    palette (dict, optional): A dictionary mapping categories to colors.
    """
    plt.figure(figsize=(12, 8))
    sns.violinplot(x=x_col, y=y_col, data=data, palette=palette)
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.savefig(filename)
    plt.close()  # Close the plot to free up memory

def plot_bar(data, x_col, y_col, title, filename, ci=None, palette=None):
    """
    Function to create a bar plot for specified columns and save it as an image.

    Args:
    data (DataFrame): The data to plot.
    x_col (str): The column name for the x-axis.
    y_col (str): The column name for the y-axis.
    title (str): Title of the plot.
    filename (str): Path to save the plot image.
    estimator (function): Aggregation function (e.g., numpy.mean).
    ci (float, optional): Confidence interval size for error bars.
    palette (dict, optional): A dictionary mapping categories to colors.
    """
    plt.figure(figsize=(12, 8))
    sns.barplot(x=x_col, y=y_col, data=data, ci=ci, palette=palette)
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.savefig(filename)
    plt.close()  # Close the plot to free up memory

def main():
    time_start = time.time()

    main_data_path = '/mnt/parscratch/users/eia19od/combined_data.parquet'
    labeled_data_path = '/mnt/parscratch/users/eia19od/labelled.csv'
    parameters_path = '/mnt/parscratch/users/eia19od/merged_data.xlsx'

    logging.info("Reading the main and labeled datasets using Dask.")
    main_data = dd.read_parquet(main_data_path)

    # downsample main_data to .01%
    main_data = main_data.sample(frac=0.01)
    labeled_data = dd.read_csv(labeled_data_path)

    # Ensure 'mp_length' and 'mp_width' are integer for merging consistency
    main_data['mp_length'] = main_data['mp_length'].astype(int)
    main_data['mp_width'] = main_data['mp_width'].astype(int)
    labeled_data['mp_length'] = labeled_data['mp_length'].astype(int)
    labeled_data['mp_width'] = labeled_data['mp_width'].astype(int)

    # Merge main data with labeled data on 'mp_width' and 'mp_length'
    logging.info("Merging main data with labeled data.")
    merged_data = dd.merge(main_data, labeled_data, on=['mp_width', 'mp_length'], how='left')

    # Use pandas to read Excel file (Processing parameters)
    logging.info("Reading processing parameters.")
    processing_parameters = pd.read_excel(parameters_path)
    processing_parameters['Part Number'] = processing_parameters['Part Number'].astype(str)  # Convert Part Number to string
    processing_parameters = dd.from_pandas(processing_parameters, npartitions=2)

    # Ensure 'Part Number' is a string in main data for consistency
    merged_data['Part Number'] = merged_data['Part Number'].astype(str)

    # Merge processing parameters with merged_data on 'Part Number'
    logging.info("Merging with processing parameters.")
    final_merged_data = dd.merge(merged_data, processing_parameters, on='Part Number', how='left')
    logging.info(f"Columns in final merged data: {final_merged_data.columns}")

    # Log the column names in the final merged data
    logging.info(f"Columns in final merged data: {final_merged_data.columns}")

    # Remove RegionLabel == 0
    logging.info("Removing RegionLabel == 0")
    final_merged_data = final_merged_data[final_merged_data['RegionLabel'] != 0]

    # Make 'RegionLabel' a string for consistency
    final_merged_data['RegionLabel'] = final_merged_data['RegionLabel'].astype(str)

    # if region label is neither 1, 2, or 3, set it to 'Background'



    # Compute necessary data for plotting
    logging.info("Computing necessary data for plotting.")
    computed_data = final_merged_data.compute()

    # Plotting
    logging.info("Plotting the violin plot.")

    # Plotting
    logging.info("Plotting the violin plot with custom colors.")
    plt.figure(figsize=(12, 8))
    palette = {'1': "tab:blue", '2': "tab:green", '3': "tab:olive", 'Background': "tab:gray"}  # Custom color palette

    sns.violinplot(x='RegionLabel', y='Normalised Enthalpy', data=computed_data, palette=palette)
    plt.title('Violin Plot of Normalized Enthalpy by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Normalised Enthalpy')
    plt.savefig('/mnt/parscratch/users/eia19od/violins/NormH_colored.png')

    plt.figure(figsize=(12, 8))
    sns.violinplot(x='RegionLabel', y='E*0', hue='RegionLabel', data=computed_data, palette=palette, legend=False)
    plt.title('Violin Plot of E*0 by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('E*0')
    plt.savefig('/mnt/parscratch/users/eia19od/violins/Estar_colored.png')

    plt.figure(figsize=(12, 8))
    sns.violinplot(x='RegionLabel', y='Power (W)', hue='RegionLabel', data=computed_data, palette=palette, legend=False)
    plt.title('Violin Plot of Power by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Power (W)')
    plt.savefig('/mnt/parscratch/users/eia19od/violins/Power_colored.png')

    plt.figure(figsize=(12, 8))
    sns.violinplot(x='RegionLabel', y='Speed (mm/s)', hue='RegionLabel', data=computed_data, palette=palette, legend=False)
    plt.title('Violin Plot of E*0 by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Speed (mm/s)')
    plt.savefig('/mnt/parscratch/users/eia19od/violins/Speed_colored.png')

    plt.figure(figsize=(12, 8))
    sns.violinplot(x='RegionLabel', y='Focus', hue='RegionLabel', data=computed_data, palette=palette, legend=False)
    plt.title('Violin Plot of E*0 by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Focus')
    plt.savefig('/mnt/parscratch/users/eia19od/violins/Focus_colored.png')

    # Ensure 'Keyhole' is a boolean for plotting
    final_merged_data['Keyhole'] = final_merged_data['Keyhole'].astype(bool)

    # Plotting count plot for Keyhole variable across different RegionLabels
    logging.info("Plotting count plot for Keyhole variable.")
    plt.figure(figsize=(10, 6))
    ax = sns.countplot(x='RegionLabel', hue='Keyhole', data=computed_data, palette="pastel")
    ax.set_title('Count of Keyhole States by Region Label')
    ax.set_xlabel('Region Label')
    ax.set_ylabel('Count')
    plt.legend(title='Keyhole State', loc='upper right')
    plt.savefig('/mnt/parscratch/users/eia19od/bargraphs/Keyhole_Counts.png')
    plt.close()

    # Plotting bar plot for 'Power (W)' by 'RegionLabel'
    logging.info("Plotting bar plot for Power (W) by RegionLabel.")
    plot_bar(computed_data, 'RegionLabel', 'Power (W)', 'Bar Plot of Power by Region Label',
             '/mnt/parscratch/users/eia19od/bargraphs/Power_Bar.png', estimator=pd.np.sum, ci='sd')



    logging.info("Colored violin plots created and saved successfully.")

    logging.info(f"Total processing time: {time.time() - time_start} seconds.")

    logging.info("Violin plot created and saved successfully.")

    logging.info(f"Total processing time: {time.time() - time_start} seconds.")



if __name__ == "__main__":
    main()