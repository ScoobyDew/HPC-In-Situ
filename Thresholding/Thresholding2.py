import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import time

time_start = time.time()

# Set up logging
logging.basicConfig(level=logging.INFO, filename='/users/eia19od/in_situ/HPC-In-Situ/Thresholding/thresh2.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

main_data_path = '/mnt/parscratch/users/eia19od/combined_data.parquet'
labeled_data_path = '/mnt/parscratch/users/eia19od/labelled.csv'
parameters_path = '/mnt/parscratch/users/eia19od/merged_data.xlsx'

logging.info("Reading the main and labeled datasets using Dask.")
main_data = dd.read_parquet(main_data_path)
# downsample main_data to .01%
main_data = main_data.sample(frac=0.0001)
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

# Compute necessary data for plotting
logging.info("Computing necessary data for plotting.")
computed_data = final_merged_data.compute()

# Ensure 'RegionLabel' is available as integer if it's supposed to be
# Filtering the data to include only regions 1, 2, and 3
filtered_data = computed_data[computed_data['RegionLabel'].isin([1, 2, 3])]
filtered_data['RegionLabel'] = filtered_data['RegionLabel'].astype(int)

# Create a color palette
palette = {1: 'blue', 2: 'green', 3: 'yellow'}

# Plotting
logging.info("Plotting the violin plot.")

# Create the violin plot for Normalized Enthalpy
plt.figure(figsize=(12, 8))
sns.violinplot(x='RegionLabel', y='Normalised Enthalpy', data=filtered_data, palette=palette)
plt.title('Violin Plot of Normalized Enthalpy by Region Label')
plt.xlabel('Region')
plt.ylabel(r'$\frac{\Delta H}{h_s}$')  # Correct LaTeX formatting
plt.savefig('/mnt/parscratch/users/eia19od/NENTH_violin_plot.png')
plt.close()

# Create the violin plot for E*0
plt.figure(figsize=(12, 8))
sns.violinplot(x='RegionLabel', y='E*0', data=filtered_data, palette=palette)
plt.title('Violin Plot of E*0 by Region Label')
plt.xlabel('Region')
plt.ylabel(r'$E^{*}_0$')  # Correct LaTeX formatting
plt.savefig('/mnt/parscratch/users/eia19od/E0_violin_plot.png')
plt.close()

# Logging completion
logging.info("Violin plots created and saved successfully.")
logging.info(f"Total processing time: {time.time() - time_start} seconds.")
