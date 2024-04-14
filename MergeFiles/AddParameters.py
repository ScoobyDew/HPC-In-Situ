import os
import logging
import pandas as pd
import dask.dataframe as dd

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# try:
#     import cudf.pandas
#     cudf.pandas.install()
#     import pandas as pd
#     logging.info("Using cuDF")
#
# except ImportError:
#     import pandas as pd
#     logging.info("Using Pandas")
#     pass


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

    '''Read the merged parquet file'''
    try:
        df = dd.read_parquet(filepath, columns=columns)
        df['Part Number'] = df['Part Number'].astype('category')
        logging.info(f"Read parquet file: {filepath}")
    except Exception as e:
        logging.error(f"Error reading {filepath}: {str(e)}")


    '''Read excel part_parameters file'''
    parameters = pd.read_excel('part_parameters.xlsx')
    param_columns = [
        'Part Number',
        'Power (W)',
        'Speed (mm/s)',
        'Focus',
        'Beam radius (um)',
    ]
    for col in param_columns:
        df[col] = df[col].astype('category')

    '''Merge the dataframes'''
    try:
        df = dd.merge(parameters, on='Part Number', how='left')
        logging.info("Merged dataframes")
    except Exception as e:
        logging.error(f"Error merging dataframes: {str(e)}")

    '''Save the processed DataFrame to a new parquet file'''
    output_file = '/mnt/parscratch/users/eia19od/combined_params.parquet'
    df.to_parquet(output_file, index=False)

    logging.info("Processing completed successfully")

if __name__ == "__main__":
    main()
