from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import os

# Initialize Dask Client
client = Client(memory_limit='2GB')

def process_files(directory):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
    ddf = dd.read_parquet(files, engine='pyarrow')

    # Ensure 'Part Number' is treated as string
    ddf['Part Number'] = ddf['Part Number'].astype(str)

    # Assume operations...
    ddf = ddf[ddf['Part Number'] != '']  # Example operation

    return ddf

def main():
    directory = '/path/to/directory'
    result = process_files(directory)
    result.compute()

    # Close the client after computation
    client.close()

if __name__ == '__main__':
    main()
