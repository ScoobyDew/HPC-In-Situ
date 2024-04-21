import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import time

time_start = time.time()

# Set up logging
logging.basicConfig(level=logging.INFO, filename='/mnt/parscratch/users/eia19od/thresh2.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

main_data_path = '/mnt/parscratch/users/eia19od/combined_data_with_parameters.parquet'
labeled_data_path = '/mnt/parscratch/users/eia19od/labelled.csv'

logging.info("Reading the main and labeled datasets using Dask.")
main_data = dd.read_parquet(main_data_path)

# Convert data types
main_data['mp_length'] = main_data['mp_length'].astype(int)
main_data['mp_width'] = main_data['mp_width'].astype(int)

# Downsample the data to .1% of the original size
main_data = main_data.sample(frac=0.001)

# Print column names and data types form the main dataset
logging.info("Column names and data types from the main dataset:")
logging.info(main_data.dtypes)


labeled_data = dd.read_csv(labeled_data_path)

# Print column names and data types from the labeled dataset
logging.info("Column names and data types from the labeled dataset:")
logging.info(labeled_data.dtypes)

logging.info("Datasets read successfully.")

# Merge the datasets
logging.info("Merging the datasets.")
merged_data = dd.merge(main_data, labeled_data, on=['mp_length', 'mp_width'], how='left')
logging.info("Datasets merged successfully.")

# Compute necessary part for plotting
logging.info("Computing necessary data for plotting.")
computed_data = merged_data.compute()  # This triggers the actual computation

# Plotting
logging.info("Plotting the violin plot.")
plt.figure(figsize=(12, 8))
sns.violinplot(x='RegionLabel', y='Normalized Enthalpy', data=computed_data)
plt.title('Violin Plot of Normalized Energy by Cluster')
plt.xlabel('Cluster')
plt.ylabel('Normalized Energy')
logging.info("Violin plot created successfully.")

# Save the plot
output_path = '/mnt/parscratch/users/eia19od/violin_plot.png'
plt.savefig(output_path)
logging.info(f"Violin plot saved successfully at {output_path}.")
logging.info(f"Total processing time: {time.time() - time_start} seconds.")
