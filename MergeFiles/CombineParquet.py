import os
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_parquet_directory(directory):
    logging.info(f"Reading parquet files from directory: {directory}")
    try:
        files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
        # Initialize an empty DataFrame
        df = pd.DataFrame()

        # Read each file as a DataFrame and concatenate it to the existing DataFrame
        for file in files:
            temp_df = pd.read_parquet(file)
            df = pd.concat([df, temp_df], ignore_index=True)
            logging.info(f"Processed file: {file}")

        return df
    except Exception as e:
        logging.error(f'Error reading parquet files: {e}', exc_info=True)
        return pd.DataFrame()  # Return an empty DataFrame if an error occurs

def attach_parameters(df, parameters_file):
    try:
        parameters = pd.read_excel(parameters_file)
        features = ['Power (W)', 'Speed (mm/s)', 'Focus', 'Beam radius (um)']
        necessary_columns = ['Part Number'] + features
        parameters = pd.DataFrame(parameters, columns=necessary_columns)

        # Merge parameters with the main data
        df = df.merge(parameters, on='Part Number', how='left')
        return df
    except Exception as e:
        logging.error(f'Failed to attach parameters: {e}', exc_info=True)
        return df  # Return the input if failure occurs

def calculate_NVED(df):
    try:
        A = 0.3
        l = 7.5
        rho = 8.395e-3
        Cp = 0.43
        Tm = 1300
        T0 = 300
        h = 75

        df['E*'] = (A * df['Power (W)'] / (2 * df['Speed (mm/s)'] * l * df['beam_radius'] * 1e3) *
                     1 / (0.67 * rho * Cp * (Tm - T0)))
        df['1/h*'] = df['beam_radius'] * 1e3 / h
        return df
    except Exception as e:
        logging.error(f'Failed to calculate NVED: {e}', exc_info=True)
        return df

def main():
    try:
        directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
        logging.info(f"Processing data in directory: {directory}")

        # Read and process data
        df = read_parquet_directory(directory)
        df = attach_parameters(df, 'part_parameters.xlsx')
        df = calculate_NVED(df)

        if df.empty:
            logging.error("Processed DataFrame is empty. No data to process.")
        else:
            logging.info('Processing completed successfully')
            print(df.head())
            print(df.describe())

    except Exception as e:
        logging.error(f'An error occurred during processing: {e}')

if __name__ == "__main__":
    main()
