import dask.dataframe as dd
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def read_and_process_file(file):
    # Use Dask to read the Parquet file
    ddf = dd.read_parquet(file)

    # Example processing: ensure 'Part Number' is a column, derived from the file name if absent
    if 'Part Number' not in ddf.columns:
        part_number = os.path.splitext(os.path.basename(file))[0]
        ddf['Part Number'] = part_number

    logging.info(f"Processed file: {file}")
    return ddf


def main():
    directory = '/mnt/parscratch/users/eia19od/Cleaned'
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]

    # Read and process all files using Dask
    dataframes = [read_and_process_file(file) for file in files]
    big_dataframe = dd.concat(dataframes, axis=0, ignore_index=True)

    # Compute the result to perform actual computations
    final_dataframe = big_dataframe.compute()

    # Log the shape of the combined DataFrame
    logging.info(f"Combined DataFrame shape: {final_dataframe.shape}")

    # Save the processed DataFrame to a new parquet file
    output_file = '/mnt/parscratch/users/eia19od/combined.parquet'
    final_dataframe.to_parquet(output_file, write_index=False)
    logging.info(f"Saved combined DataFrame to {output_file}")


if __name__ == "__main__":
    main()
    logging.info("Done")
