"""
Plots a 2D histogram of the data in the file from combined parquet with parameters
follows from SimplerCombine.py and AddParameters.py to make create a dataset with parameters:
- Power
- Speed
- Focus
- Beam radius
- pyro2
- mp_width
- mp_length
- mp_intensity
"""

import cudf as cf
import cuml

import os
import logging
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt

# Setup logging
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Reads the combined parquet file with parameters and plots a 2D histogram of the data"""
