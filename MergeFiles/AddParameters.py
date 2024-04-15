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
        df['Part Number'] = df['Part Number'].astype('str')  # Use string for matching
        logging.info(f"Read parquet file: {filepath}")
    except Exception as e:
        logging.error(f"Error reading {filepath}: {str(e)}")
        return

    # Read excel part_parameters file
    try:
        parameters = pd.read_excel('part_parameters.xlsx')
        parameters['Part Number'] = parameters['Part Number'].astype('str')  # Ensure matching data type
        logging.info("Read excel file")
    except Exception as e:
        logging.error(f"Error reading Excel file: {str(e)}")
        return

    # Select only the required columns from the parameters dataframe
    parameters = parameters[['Part Number', 'Power (W)', 'Speed (mm/s)', 'Focus', 'Beam radius (um)']]
    logging.info(f"Columns selected from parameters dataframe: {list(parameters.columns)}")

    # Merge the dataframes on 'Part Number'
    try:
        df_shape_before = df.shape
        parameters_shape = parameters.shape
        logging.info(f"DataFrame shape before merge: {(df_shape_before[0].compute(), df_shape_before[1])}")
        logging.info(f"Parameters DataFrame shape: {parameters_shape}")
        logging.info("Merging dataframes")
        df = dd.merge(df, parameters, on='Part Number', how='left')
        df_shape_after = df.shape

        logging.info(f"DataFrame shape before merge: {(df_shape_before[0].compute(), df_shape_before[1])}")
        logging.info(f"Parameters DataFrame shape: {parameters_shape}")
        logging.info(f"DataFrame shape after merge: {(df_shape_after[0].compute(), df_shape_after[1])}")
        logging.info("Merged dataframes")
    except Exception as e:
        logging.error(f"Error merging dataframes: {str(e)}")
        return

    # Reset the index before saving to Parquet
    df = df.reset_index(drop=True)

    # Save the processed DataFrame to a new parquet file
    output_file = '/mnt/parscratch/users/eia19od/combined_params.parquet'
    try:
        df.to_parquet(output_file)
        logging.info(f"Saved processed data to {output_file}")
    except Exception as e:
        logging.error(f"Error saving data to Parquet: {str(e)}")

    logging.info("Processing completed successfully")

if __name__ == "__main__":
    main()
