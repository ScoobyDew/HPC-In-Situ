import os
import logging
import pandas as pd
import dask.dataframe as dd

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Starting processing")
    filepath = '/mnt/parscratch/users/eia19od/combined_data.parquet'
    columns = [
        'Part Number',
        'pyro2',
        'mp_width',
        'mp_length',
        'mp_intensity'
    ]

    # Read the merged parquet file
    try:
        df = dd.read_parquet(filepath, columns=columns)
        df['Part Number'] = df['Part Number'].astype('category')
        logging.info(f"Read parquet file: {filepath}")
    except Exception as e:
        logging.error(f"Error reading {filepath}: {str(e)}")

    # Read excel part_parameters file
    parameters = pd.read_excel('part_parameters.xlsx')
    logging.info("Read excel file")

    # Select only the required columns from the parameters dataframe
    parameters = parameters[['Part Number', 'Power (W)', 'Speed (mm/s)', 'Focus', 'Beam radius (um)']]

    # Merge the dataframes on 'Part Number'
    try:
        logging.info(f"DataFrame shape before merge: {df.shape.compute()}")
        logging.info(f"Parameters DataFrame shape: {parameters.shape}")
        df = dd.merge(df, parameters, on='Part Number', how='left')
        logging.info(f"DataFrame shape after merge: {df.shape.compute()}")
        logging.info("Merged dataframes")
    except Exception as e:
        logging.error(f"Error merging dataframes: {str(e)}")

    # Reset the index before saving to Parquet
    df = df.reset_index(drop=True)

    # Save the processed DataFrame to a new parquet file
    output_file = '/mnt/parscratch/users/eia19od/combined_params.parquet'
    df.to_parquet(output_file)

    logging.info("Processing completed successfully")

if __name__ == "__main__":
    main()
