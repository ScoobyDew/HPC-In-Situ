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


def plot_unique_power_bars(data, filename):
    """
    Function to create a bar plot with unique bars for each unique power value per region label.
    """
    plt.figure(figsize=(12, 8))
    sns.barplot(x='RegionLabel', y='Power (W)', data=data)
    plt.title('Unique Power Values by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Power (W)')
    plt.savefig(filename)
    plt.close()


def plot_grouped_power_bars(data, filename):
    """
    Function to create a grouped bar plot with bars for each unique power value per region label,
    sorted in ascending order and colored using the cividis colormap.
    """
    # Ensure Power (W) is numeric and drop any rows where it's missing
    data['Power (W)'] = pd.to_numeric(data['Power (W)'], errors='coerce')
    data = data.dropna(subset=['Power (W)'])

    # Count frequencies for each power value in each region, sorted by Power
    data_freq = data.groupby(['RegionLabel', 'Power (W)']).size().reset_index(name='Frequency')
    data_freq = data_freq.sort_values(by='Power (W)')

    plt.figure(figsize=(12, 8))
    palette = sns.color_palette("cividis", n_colors=len(data_freq['Power (W)'].unique()))
    sns.barplot(x='RegionLabel', y='Frequency', hue='Power (W)', data=data_freq, palette=palette)
    plt.title('Frequency of Power Values by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Frequency')
    plt.legend(title='Power (W)', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()


def plot_grouped_focus_bars(data, filename):
    """
    Function to create a grouped bar plot with bars for each unique focus value per region label,
    sorted in ascending order and colored using the cividis colormap.
    """
    # Ensure Focus is numeric and drop any rows where it's missing
    data['Focus'] = pd.to_numeric(data['Focus'], errors='coerce')
    data = data.dropna(subset=['Focus'])

    # Count frequencies for each focus value in each region, sorted by Focus
    data_freq = data.groupby(['RegionLabel', 'Focus']).size().reset_index(name='Frequency')
    data_freq = data_freq.sort_values(by='Focus')

    plt.figure(figsize=(12, 8))
    # Dynamically assign colors from the cividis colormap based on unique Focus values
    palette = sns.color_palette("cividis", n_colors=len(data_freq['Focus'].unique()))
    sns.barplot(x='RegionLabel', y='Frequency', hue='Focus', data=data_freq, palette=palette)
    plt.title('Frequency of Focus Values by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Frequency')
    plt.legend(title='Focus', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()


def plot_grouped_speed_bars(data, filename):
    """
    Function to create a grouped bar plot with bars for each unique speed value per region label,
    sorted in ascending order and colored using the cividis colormap.
    """
    # Ensure Speed (mm/s) is numeric and drop any rows where it's missing
    data['Speed (mm/s)'] = pd.to_numeric(data['Speed (mm/s)'], errors='coerce')
    data = data.dropna(subset=['Speed (mm/s)'])

    # Count frequencies for each speed value in each region, sorted by Speed
    data_freq = data.groupby(['RegionLabel', 'Speed (mm/s)']).size().reset_index(name='Frequency')
    data_freq = data_freq.sort_values(by='Speed (mm/s)')

    plt.figure(figsize=(12, 8))
    # Assign colors from the cividis colormap based on unique Speed values
    palette = sns.color_palette("cividis", n_colors=len(data_freq['Speed (mm/s)'].unique()))
    sns.barplot(x='RegionLabel', y='Frequency', hue='Speed (mm/s)', data=data_freq, palette=palette)
    plt.title('Frequency of Speed Values by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Frequency')
    plt.legend(title='Speed (mm/s)', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()


# Create bar plots for PV for each region
def plot_grouped_pv_bars(data, filename):
    """
    Function to create a grouped bar plot with bars for each unique PV value per region label,
    sorted in ascending order and colored using the cividis colormap.
    """
    # Ensure PV is numeric and drop any rows where it's missing
    data['PV'] = pd.to_numeric(data['PV'], errors='coerce')
    data = data.dropna(subset=['PV'])

    # Count frequencies for each PV value in each region, sorted by PV
    data_freq = data.groupby(['RegionLabel', 'PV']).size().reset_index(name='Frequency')
    data_freq = data_freq.sort_values(by='PV')

    plt.figure(figsize=(12, 8))
    # Dynamically assign colors from the cividis colormap based on unique PV values
    palette = sns.color_palette("cividis", n_colors=len(data_freq['PV'].unique()))
    sns.barplot(x='RegionLabel', y='Frequency', hue='PV', data=data_freq, palette=palette)
    plt.title('Frequency of PV Values by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Frequency')
    plt.legend(title='PV', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()


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

    # Create feature P/V
    logging.info("Creating feature P/V.")
    final_merged_data['PV'] = final_merged_data['Power (W)'] / final_merged_data['Speed (mm/s)']
    logging.info("P/V feature created.")



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

    # Call PV function
    logging.info("Creating grouped bar plot for P/V.")
    plot_grouped_pv_bars(computed_data[computed_data['PV'].notnull()], '/mnt/parscratch/users/eia19od/bargraphs/Unique_PV_Bar.png')
    logging.info("Grouped bar plot for P/V created and saved successfully.")

    logging.info("Colored violin plots created and saved successfully.")

    logging.info(f"Total processing time: {time.time() - time_start} seconds.")

    logging.info("Violin plot created and saved successfully.")

    logging.info(f"Total processing time: {time.time() - time_start} seconds.")


def plot_normalized_speed_bars(data, filename):
    """
    Function to create a grouped bar plot with normalized bars for each unique speed value per region label,
    scaled by the total counts per region and colored using the cividis colormap.
    """
    # Ensure Speed (mm/s) is numeric and drop any rows where it's missing
    data['Speed (mm/s)'] = pd.to_numeric(data['Speed (mm/s)'], errors='coerce')
    data = data.dropna(subset=['Speed (mm/s)'])

    # Count frequencies for each speed value in each region
    data_freq = data.groupby(['RegionLabel', 'Speed (mm/s)']).size().reset_index(name='Frequency')

    # Calculate total counts per region to normalize
    total_counts_per_region = data_freq.groupby('RegionLabel')['Frequency'].transform('sum')
    data_freq['Normalized Frequency'] = data_freq['Frequency'] / total_counts_per_region

    # Sort data for better visualization
    data_freq = data_freq.sort_values(by=['RegionLabel', 'Speed (mm/s)'])

    plt.figure(figsize=(12, 8))
    # Assign colors from the cividis colormap based on unique Speed values
    palette = sns.color_palette("cividis", n_colors=len(data_freq['Speed (mm/s)'].unique()))
    sns.barplot(x='RegionLabel', y='Normalized Frequency', hue='Speed (mm/s)', data=data_freq, palette=palette)
    plt.title('Normalized Frequency of Speed Values by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Proportion (by group)')
    plt.legend(title='Speed (mm/s)', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_normalized_focus_bars(data, filename):
    """
    Function to create a grouped bar plot with normalized bars for each unique focus value per region label,
    scaled by the total counts per region and colored using the cividis colormap.
    """
    # Ensure Focus is numeric and drop any rows where it's missing
    data['Focus'] = pd.to_numeric(data['Focus'], errors='coerce')
    data = data.dropna(subset=['Focus'])

    # Count frequencies for each focus value in each region
    data_freq = data.groupby(['RegionLabel', 'Focus']).size().reset_index(name='Frequency')

    # Calculate total counts per region to normalize
    total_counts_per_region = data_freq.groupby('RegionLabel')['Frequency'].transform('sum')
    data_freq['Normalized Frequency'] = data_freq['Frequency'] / total_counts_per_region

    # Sort data for better visualization
    data_freq = data_freq.sort_values(by=['RegionLabel', 'Focus'])

    plt.figure(figsize=(12, 8))
    # Assign colors from the cividis colormap based on unique Focus values
    palette = sns.color_palette("cividis", n_colors=len(data_freq['Focus'].unique()))
    sns.barplot(x='RegionLabel', y='Normalized Frequency', hue='Focus', data=data_freq, palette=palette)
    plt.title('Normalized Frequency of Focus Values by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Proportion (by group)')
    plt.legend(title='Focus', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_normalized_power_bars(data, filename):
    """
    Function to create a grouped bar plot with normalized bars for each unique power value per region label,
    scaled by the total counts per region and colored using the cividis colormap.
    """
    # Ensure Power (W) is numeric and drop any rows where it's missing
    data['Power (W)'] = pd.to_numeric(data['Power (W)'], errors='coerce')
    data = data.dropna(subset=['Power (W)'])

    # Count frequencies for each power value in each region
    data_freq = data.groupby(['RegionLabel', 'Power (W)']).size().reset_index(name='Frequency')

    # Calculate total counts per region to normalize
    total_counts_per_region = data_freq.groupby('RegionLabel')['Frequency'].transform('sum')
    data_freq['Normalized Frequency'] = data_freq['Frequency'] / total_counts_per_region

    # Sort data for better visualization
    data_freq = data_freq.sort_values(by=['RegionLabel', 'Power (W)'])

    plt.figure(figsize=(12, 8))
    # Assign colors from the cividis colormap based on unique Power values
    palette = sns.color_palette("cividis", n_colors=len(data_freq['Power (W)'].unique()))
    sns.barplot(x='RegionLabel', y='Normalized Frequency', hue='Power (W)', data=data_freq, palette=palette)
    plt.title('Normalized Frequency of Power Values by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Proportion (by group)')
    plt.legend(title='Power (W)', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_grouped_pv_bars(data, filename):
    """
    Function to create a grouped bar plot with bars for each unique PV value per region label,
    sorted in ascending order and colored using the cividis colormap.
    """
    # Ensure PV is numeric and drop any rows where it's missing
    data['PV'] = pd.to_numeric(data['PV'], errors='coerce')
    data = data.dropna(subset=['PV'])

    # Count frequencies for each PV value in each region, sorted by PV
    data_freq = data.groupby(['RegionLabel', 'PV']).size().reset_index(name='Frequency')
    data_freq = data_freq.sort_values(by='PV')

    plt.figure(figsize=(12, 8))
    # Dynamically assign colors from the cividis colormap based on unique PV values
    palette = sns.color_palette("cividis", n_colors=len(data_freq['PV'].unique()))
    sns.barplot(x='RegionLabel', y='Frequency', hue='PV', data=data_freq, palette=palette)
    plt.title('Frequency of PV Values by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Frequency')
    plt.legend(title='PV', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()


# Plot normalised PV
def plot_normalized_pv_bars(data, filename):
    """
    Function to create a grouped bar plot with normalized bars for each unique PV value per region label,
    scaled by the total counts per region and colored using the cividis colormap.
    """
    # Ensure PV is numeric and drop any rows where it's missing
    data['PV'] = pd.to_numeric(data['PV'], errors='coerce')
    data = data.dropna(subset=['PV'])

    # Count frequencies for each PV value in each region
    data_freq = data.groupby(['RegionLabel', 'PV']).size().reset_index(name='Frequency')

    # Calculate total counts per region to normalize
    total_counts_per_region = data_freq.groupby('RegionLabel')['Frequency'].transform('sum')
    data_freq['Normalized Frequency'] = data_freq['Frequency'] / total_counts_per_region

    # Sort data for better visualization
    data_freq = data_freq.sort_values(by=['RegionLabel', 'PV'])

    plt.figure(figsize=(12, 8))
    # Assign colors from the cividis colormap based on unique PV values
    palette = sns.color_palette("cividis", n_colors=len(data_freq['PV'].unique()))
    sns.barplot(x='RegionLabel', y='Normalized Frequency', hue='PV', data=data_freq, palette=palette)
    plt.title('Normalized Frequency of PV Values by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Proportion (by group)')
    plt.legend(title='PV', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

# Plot violin plots for each of the regions and E*0
def plot_violin(data, filename):
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
    sns.violinplot(x='RegionLabel', y='Normalised Enthalpy', data=data)
    plt.title('Violin Plot of Normalized Enthalpy by Region Label')
    plt.xlabel('Region Label')
    plt.ylabel('Normalised Enthalpy')
    plt.savefig(filename)
    plt.close()  # Close the plot to free up memory

def main2():
    start_time = time.time()
    main_data_path = '/mnt/parscratch/users/eia19od/combined_data.parquet'
    labeled_data_path = '/mnt/parscratch/users/eia19od/labelled.csv'
    parameters_path = '/mnt/parscratch/users/eia19od/merged_data.xlsx'

    logging.info("Reading the main and labeled datasets using Dask.")
    main_data = dd.read_parquet(main_data_path)
    main_data = main_data.sample(frac=0.01)
    labeled_data = dd.read_csv(labeled_data_path)

    main_data['mp_length'] = main_data['mp_length'].astype(int)
    main_data['mp_width'] = main_data['mp_width'].astype(int)
    labeled_data['mp_length'] = labeled_data['mp_length'].astype(int)
    labeled_data['mp_width'] = labeled_data['mp_width'].astype(int)

    logging.info("Merging main data with labeled data.")
    merged_data = dd.merge(main_data, labeled_data, on=['mp_width', 'mp_length'], how='left')

    processing_parameters = pd.read_excel(parameters_path)
    processing_parameters['Part Number'] = processing_parameters['Part Number'].astype(str)
    processing_parameters = dd.from_pandas(processing_parameters, npartitions=2)

    merged_data['Part Number'] = merged_data['Part Number'].astype(str)
    final_merged_data = dd.merge(merged_data, processing_parameters, on='Part Number', how='left')
    logging.info(f"Columns in final merged data: {final_merged_data.columns}")

    final_merged_data = final_merged_data[final_merged_data['RegionLabel'] != 0]
    final_merged_data['RegionLabel'] = final_merged_data['RegionLabel'].astype(str)

    logging.info("Creating feature P/V.")
    final_merged_data['PV'] = final_merged_data['Power (W)'] / final_merged_data['Speed (mm/s)']
    logging.info("P/V feature created.")
    computed_data = final_merged_data.compute()

    # Adjust this plot call to match your needs
    plot_grouped_power_bars(computed_data[computed_data['Power (W)'].notnull()], '/mnt/parscratch/users/eia19od/bargraphs/Unique_Power_Bar.png')
    plot_grouped_focus_bars(computed_data[computed_data['Focus'].notnull()], '/mnt/parscratch/users/eia19od/bargraphs/Unique_Focus_Bar.png')
    plot_grouped_speed_bars(computed_data[computed_data['Speed (mm/s)'].notnull()], '/mnt/parscratch/users/eia19od/bargraphs/Unique_Speed_Bar.png')
    plot_normalized_speed_bars(computed_data[computed_data['Speed (mm/s)'].notnull()], '/mnt/parscratch/users/eia19od/bargraphs/Normalized_Speed_Bar.png')
    plot_normalized_focus_bars(computed_data[computed_data['Focus'].notnull()], '/mnt/parscratch/users/eia19od/bargraphs/Normalized_Focus_Bar.png')
    plot_normalized_power_bars(computed_data[computed_data['Power (W)'].notnull()], '/mnt/parscratch/users/eia19od/bargraphs/Normalized_Power_Bar.png')
    plot_violin(computed_data, '/mnt/parscratch/users/eia19od/violins/NormH_colored.png')

    plot_normalized_pv_bars(computed_data[computed_data['PV'].notnull()], '/mnt/parscratch/users/eia19od/bargraphs/Normalized_PV_Bar.png')

    # Plot violin plots for Normalised Enthalpy
    logging.info("Plotting the violin plot with custom colors.")
    plot_violin(computed_data, '/mnt/parscratch/users/eia19od/violins/NormH_colored.png')
    logging.info("Violin plot created and saved successfully.")

    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"Total processing time {total_time} seconds.")

if __name__ == "__main__":
    main2()