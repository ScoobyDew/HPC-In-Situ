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
main_data = main_data.sample(frac=0.0001)  # downsample main_data to .01%
labeled_data = dd.read_csv(labeled_data_path)

# Convert 'mp_length' and 'mp_width' to integer for merging consistency
main_data['mp_length'] = main_data['mp_length'].astype(int)
main_data['mp_width'] = main_data['mp_width'].astype(int)
labeled_data['mp_length'] = labeled_data['mp_length'].astype(int)
labeled_data['mp_width'] = labeled_data['mp_width'].astype(int)

logging.info("Merging main data with labeled data.")
merged_data = dd.merge(main_data, labeled_data, on=['mp_width', 'mp_length'], how='left')

logging.info("Reading processing parameters.")
processing_parameters = pd.read_excel(parameters_path)
processing_parameters['Part Number'] = processing_parameters['Part Number'].astype(str)
processing_parameters = dd.from_pandas(processing_parameters, npartitions=2)

merged_data['Part Number'] = merged_data['Part Number'].astype(str)
final_merged_data = dd.merge(merged_data, processing_parameters, on='Part Number', how='left')

logging.info("Computing necessary data for plotting.")
computed_data = final_merged_data.compute()

filtered_data = computed_data[computed_data['RegionLabel'].isin([1, 2, 3])]
filtered_data['RegionLabel'] = filtered_data['RegionLabel'].astype(int)

unique_labels = filtered_data['RegionLabel'].unique()
logging.info(f"Unique Region Labels: {unique_labels}")

# Create a color palette with integer keys matching the RegionLabel data
palette = {1: 'blue', 2: 'green', 3: 'yellow'}
logging.info("Creating the violin plots.")

plt.figure(figsize=(12, 8))
sns.violinplot(x='RegionLabel', y='Normalised Enthalpy', data=filtered_data, palette=palette)
plt.title('Violin Plot of Normalized Enthalpy by Region Label')
plt.xlabel('Region')
plt.ylabel(r'$\frac{\Delta H}{h_s}$')
plt.savefig('/mnt/parscratch/users/eia19od/NENTH_violin_plot.png')
plt.close()

plt.figure(figsize=(12, 8))
sns.violinplot(x='RegionLabel', y='E*0', data=filtered_data, palette=palette)
plt.title('Violin Plot of E*0 by Region Label')
plt.xlabel('Region')
plt.ylabel(r'$E^{*}_0$')
plt.savefig('/mnt/parscratch/users/eia19od/E0_violin_plot.png')
plt.close()

logging.info("Violin plots created and saved successfully.")
logging.info(f"Total processing time: {time.time() - time_start} seconds.")
