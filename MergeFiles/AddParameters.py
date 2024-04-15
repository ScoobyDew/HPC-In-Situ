import os
import logging
import pandas as pd
import dask.dataframe as dd
import pyarrow.parquet as pq

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

    '''Read the merged parquet file'''
    try:
        dataset = pq.ParquetDataset(filepath)
        table = dataset.read(columns=columns)
        df = dd.from_pandas(table.to_pandas(), npartitions=4)
        df['Part Number'] = df['Part Number'].astype('category')
        logging.info(f"Read parquet file: {filepath}")
    except Exception as e:
        logging.error(f"Error reading {filepath}: {str(e)}")

    '''Read excel part_parameters file'''
    try:
        parameters = pd.read_excel('part_parameters.xlsx')
        logging.info("Read excel file")
        logging.info(f"Parameters: {parameters.columns}")
        for col in parameters.columns:
            df[col] = df[col].astype('category')
    except Exception as e:
        logging.error(f"Error reading part_parameters.xlsx: {str(e)}")

    '''Merge the dataframes'''
    try:
        df = dd.merge(df, parameters, on='Part Number', how='left')
        logging.info("Merged dataframes")
    except Exception as e:
        logging.error(f"Error merging dataframes: {str(e)}")

    '''Save the processed DataFrame to a new parquet file'''
    output_file = '/mnt/parscratch/users/eia19od/combined_params.parquet'
    df.to_parquet(output_file, index=False)

    logging.info("Processing completed successfully")

if __name__ == "__main__":
    main()
