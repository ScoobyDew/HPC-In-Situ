import os
import sys
import pandas as pd
import numpy as np
import logging
from multiprocessing import Pool

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def read_parquet_file(filepath):
    logging.info(f"Trying to read file: {filepath}")
    try:
        sample_number = int(os.path.basename(filepath).split('.')[0])
        df = pd.read_parquet(filepath)
        df['sample_number'] = sample_number
        if df.empty:
            logging.warning(f"Read empty DataFrame from {filepath}")
        return df
    except Exception as e:
        logging.error(f'Error reading {filepath}: {e}', exc_info=True)
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def combine_on_name(directory, num_processes=4):
    """
    Combine all .parquet files in a directory into a single DataFrame using multiprocessing.
    """
    filepaths = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith(".parquet")]
    logging.info(f"Found {len(filepaths)} .parquet files.")

    with Pool(processes=num_processes) as pool:
        dataframes = pool.map(read_parquet_file, filepaths)

    # Filter out any empty DataFrames returned due to errors
    dataframes = [df for df in dataframes if not df.empty]

    if dataframes:
        combined_df = pd.concat(dataframes)
    else:
        logging.error('No dataframes to combine, returning empty DataFrame')
        return pd.DataFrame()

    return combined_df


def attach_parameters(df):
    try:
        parameters = pd.read_excel('parameters.xlsx')
        features = ['Power (W)', 'Speed (mm/s)', 'Focus', 'Beam radius (um)']
        necessary_columns = ['Part Number'] + features
        parameters = parameters[necessary_columns]
        df = df.merge(parameters, on='Part Number', how='left')
    except Exception as e:
        logging.error(f'Failed to attach parameters: {e}', exc_info=True)
    return df


def calculate_NVED(df):
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


def calculate_normalized_enthalpy(df):
    A = 0.3
    D = 3.25
    rho = 8.395e-3
    Cp = 0.43
    Tsol = 1300
    df['dH/dh'] = (A * df['Power (W)']) / (
                rho * Cp * Tsol * np.sqrt(D * df['Speed (mm/s)'] * (df['beam_radius'] * 1e3) ** 3))
    return df


def main():
    try:
        directory = os.getenv('DATA_DIRECTORY', '/mnt/parscratch/users/eia19od/Cleaned')
        logging.info(f"Processing data in directory: {directory}")

        # Utilize multiprocessing to handle multiple files
        combined_df = combine_on_name(directory, num_processes=16)

        if combined_df.empty:
            logging.error("Combined DataFrame is empty. No data to process.")
            return combined_df  # Ensure returning a DataFrame even when empty

        combined_df = attach_parameters(combined_df)
        combined_df[['mp_width', 'mp_length', 'mp_intensity']] = combined_df[
            ['mp_width', 'mp_length', 'mp_intensity']].replace(0, np.nan)
        combined_df = calculate_NVED(combined_df)
        combined_df = calculate_normalized_enthalpy(combined_df)
        logging.info('Processing completed successfully')
    except Exception as e:
        logging.error(f'An error occurred during processing: {e}', exc_info=True)
        return pd.DataFrame()  # Return an empty DataFrame on exception

    return combined_df


if __name__ == "__main__":
    df = main()
    if df is not None and not df.empty:
        print(df.columns)
        print(df.head())
        print(df.describe())
        print(df.info())
    else:
        print("No data to display.")
