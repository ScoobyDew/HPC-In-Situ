# Load both the main dataset and the labeled dataset
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, filename='thresh2.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

main_data_path = '/mnt/parscratch/users/eia19od/combined_data_with_parameters.parquet'
labeled_data_path = '/mnt/parscratch/users/eia19od/labelled.csv'

logging.info("Reading the main and labeled datasets.")
main_data = pd.read_parquet(main_data_path)
labeled_data = pd.read_csv(labeled_data_path)
logging.info("Datasets read successfully.")

# Merge the datasets
# Assuming there's a common column like 'mp_length' and 'mp_width' in both datasets
logging.info("Merging the datasets.")
merged_data = pd.merge(main_data, labeled_data, on=['mp_length', 'mp_width'], how='left')
logging.info("Datasets merged successfully.")

# Plot violin plot of 'Normalized Energy' by 'cluster' (1, 2, 3)
import matplotlib.pyplot as plt
import seaborn as sns

logging.info("Plotting the violin plot.")
plt.figure(figsize=(12, 8))
sns.violinplot(x='RegionLabel', y='Normalized Enthalpy', data=merged_data)
plt.title('Violin Plot of Normalized Energy by Cluster')
plt.xlabel('Cluster')
plt.ylabel('Normalized Energy')
logging.info("Violin plot created successfully.")

# Save the plot in \mnt\parscratch\users\eia19od
output_path = '/mnt/parscratch/users/eia19od/violin_plot.png'
plt.savefig(output_path)
logging.info(f"Violin plot saved successfully at {output_path}.")