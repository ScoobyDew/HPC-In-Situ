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
labeled_data = dd.read_csv(labeled_data_path)

# Inspecting columns
logging.info(f"Main data columns: {main_data.columns.tolist()}")
logging.info(f"Labeled data columns: {labeled_data.columns.tolist()}")

# Ensure 'Part Number' is present and correctly typed
if 'Part Number' not in main_data.columns:
    logging.error("Part Number column missing in main data.")
    raise KeyError("Part Number column missing in main data.")
if 'Part Number' not in labeled_data.columns:
    logging.error("Part Number column missing in labeled data.")
    raise KeyError("Part Number column missing in labeled data.")

# Using pandas to read Excel file (Processing parameters)
processing_parameters = pd.read_excel(parameters_path)
logging.info("Parameters read successfully with pandas.")

# Convert pandas DataFrame to Dask DataFrame
processing_parameters = dd.from_pandas(processing_parameters, npartitions=2)

# Merge the datasets on 'Part Number'
logging.info("Merging the datasets.")
merged_data = dd.merge(main_data, processing_parameters, on='Part Number', how='left')
final_merged_data = dd.merge(merged_data, labeled_data, on=['mp_length', 'mp_width'], how='left')
logging.info("Datasets merged successfully.")

# Compute necessary part for plotting
logging.info("Computing necessary data for plotting.")
computed_data = final_merged_data.compute()

# Plotting
plt.figure(figsize=(12, 8))
sns.violinplot(x='RegionLabel', y='Normalized Enthalpy', data=computed_data)
plt.title('Violin Plot of Normalized Enthalpy by Region Label')
plt.xlabel('Region Label')
plt.ylabel('Normalized Enthalpy')
plt.savefig('/mnt/parscratch/users/eia19od/violin_plot.png')
logging.info("Violin plot created and saved successfully.")
logging.info(f"Total processing time: {time.time() - time_start} seconds.")
