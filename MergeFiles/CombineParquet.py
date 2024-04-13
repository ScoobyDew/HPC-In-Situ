import os
import sys
import pandas as pd
import numpy as np
import logging
from multiprocessing import Pool

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def combine_on_name(directory):
    """
    Combine all .parquet files in a directory into a single DataFrame.
    The files are combined based on the 'name' column and only includes files
    where the filename begins with an integer (sample number).
    """
    dfs = []
    for file in os.listdir(directory):
        if file.endswith(".parquet"):
            try:
                sample_number = int(file.split('.')[0])
            except ValueError:
                logging.warning(f'Skipped: {file} due to invalid sample number')
                continue

            filepath = os.path.join(directory, file)
            try:
                df = pd.read_parquet(filepath)
                df['sample_number'] = sample_number
                dfs.append(df)
            except Exception as e:
                logging.error(f'Failed to read {filepath}: {e}', exc_info=True)

    if dfs:
        combined_df = pd.concat(dfs)
    else:
        logging.error('No dataframes to combine, returning empty DataFrame')
        combined_df = pd.DataFrame()

    return combined_df

def attach_parameters(df):
    try:
        parameters = pd.read_excel('parameters.xlsx')
        features = ['Power (W)', 'Speed (mm/s)', 'Focus']
        necessary_columns = ['Part Number'] + features
        parameters = parameters[necessary_columns]
        df = df.merge(parameters, on='Part Number', how='left')
    except Exception as e:
        logging.error('Failed to attach parameters: {e}', exc_info=True)
    return df

def calculatate_NVED(df):
    A = 0.3
    l = 7.5
    rho = 8.395e-3
    Cp = 0.43
    Tm = 1300
    T0 = 300
    h = 75

    df['E*'] = (A * df['Power (W)'] / (2 * df['Speed (mm/s)'] * l * df['beam_radius']) *
                1 / (0.67 * rho * Cp * (Tm - T0)))
    df['1/h*'] = df['beam_radius'] / h
    return df

def calc_nomalised_enthalpy(df):
    A = 0.3
    D = 3.25
    rho = 8.395e-3
    Cp = 0.43
    Tsol = 1300
    df['dH/dh'] = (A * df['Power (W)']) / (rho * Cp * Tsol * np.sqrt(D * df['Speed (mm/s)'] * df['beam_radius']**3))
    return df

def main():
    try:
        directory = '/users/eia19od/sample_files'

        combined_df = combine_on_name(directory)
        combined_df = attach_parameters(combined_df)
        combined_df[['mp_width', 'mp_length', 'mp_intensity']] =\
            combined_df[['mp_width', 'mp_length', 'mp_intensity']].replace(0, np.nan)
        combined_df = calculatate_NVED(combined_df)
        combined_df = calc_nomalised_enthalpy(combined_df)
        logging.info('Processing completed successfully')
    except Exception as e:
        logging.error(f'An error occurred during processing: {e}', exc_info=True)
        return pd.DataFrame()  # Return empty DataFrame in case of failure

    return combined_df

if __name__ == "__main__":
    df = main()
    print(df.columns)
    print("")
    print(df.head())
    print("")
    print(df.describe())
    print("")
    print(df.info())
    print("")