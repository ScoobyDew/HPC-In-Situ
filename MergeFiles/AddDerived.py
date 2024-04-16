"""
This Script is used to add derived parameters to the combined DataFrame including:
- Normalised volumetric energy density (VED)
- Normalised Enthalpy
"""

import os
import pandas as pd
import logging
import time
import numpy as np
import dask.dataframe as dd
import matplotlib.pyplot as plt

# Setup logging
logging.basicConfig(level=logging.INFO, filename='add_derived.log',
                    filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )


def add_normenthalpy(dd, absorptivity=0.3, T_m=1323, T_0=298, r_B=0.1, rho=0.008395, C_p=0.43):
    """
    Add normalised enthalpy to the Dask DataFrame

    Normalised enthalpy can be given by the eqation:
    $\frac{\Delta H}{h_s}=\frac{n Q}{h_s \sqrt{a v \sigma^3}}$
    $h_s=\rho C_p T_{\text {sol }}$

        where:
        - $\frac{\Delta H}{h_s}$ is the normalised enthalpy
        - $n$ is the absorptive efficiency
        - $Q$ is the laser power
        - $h_s$ is the specific enthalpy
        - $a$ is the thermal diffusivity
        - $v$ is the laser scanning speed
        - $\sigma$ is the laser spot size

        and
        - $\rho$ is the density
        - $C_p$ is the specific heat capacity
        - $T_{\text {sol }}$ is the solidus temperature

        for IN718:
        - $n$ is 0.3
        - $a$ is 3.25 mm^2/s
        - $rho$ is 0.008395 g/mm^3
        - $C_p$ is 0.43 J/gK
        - $T_{\text {sol }}$ is assumed to be 1260 C


    Parameters:
    - dd: Dask DataFrame
        Contains the columns:
        - 'Power (W)'
        - 'Laser Speed (mm/s)'
        - 'Beam radius (um)'
    """

    # Constants (move parameters inside the function if they vary)
    n = 0.3  # Absorptivity
    a = 3.25  # mm^2/s, thermal diffusivity
    rho = 0.008395  # g/mm^3
    Cp = 0.43  # J/gK
    T_sol = 1260  # C

    # Calculate the specific enthalpy (J/mm³)
    hs = rho * Cp * T_sol

    # Calculate the normalized enthalpy using the correct formula with square root
    dd['Normalised Enthalpy'] = (n * dd['Power (W)']) / (
                hs * np.sqrt(a * dd['Speed (mm/s)'] * (dd['Beam radius (um)'] / 1000) ** 3))

    logging.info("Added Normalised Enthalpy to the DataFrame")
    return dd
def add_NVED(dd, absorptivity=0.3, T_m=1323, T_0=298, r_B=0.1, rho=0.008395, C_p=0.43):
    """
    Add nomalised energy density to the Dask DataFrame
    Add normalised hatch

    normalised volumetric energy:
    $E^*=q^* / v^* l^*=\left[A q /\left(2 v l r_B\right)\right]\left[1 / 0.67 \rho C_p\left(T_m-T_0\right)\right]$

        where:
        - $E^*$ is the normalised volumetric energy density
        - $q^*$ is the normalised heat input
        - $v^*$ is the normalised scanning speed
        - $l^*$ is the normalised hatch
        - $A$ is the absorptivity
        - $q$ is the laser power
        - $v$ is the laser scanning speed
        - $l$ is the hatch distance
        - $r_B$ is the laser beam radius
        - $\rho$ is the density
        - $C_p$ is the specific heat capacity
        - $T_m$ is the melting temperature
        - $T_0$ is the initial temperature

        for IN718:
        - $A$ is 0.3
        - $T_m$ is 1323 K
        - $T_0$ is 298 K
        - $r_B$ is 0.1 mm
        - $\rho$ is 0.008395 g/mm^3
        - $C_p$ is 0.43 J/gK
        - $l$ is 30um


    Parameters:
    - dd: Dask DataFrame
        Contains the columns:
        - 'Power (W)'
        - 'Laser Speed (mm/s)'
        - 'Hatch (mm)'
        - 'Beam radius (um)'
    """

    # Constants
    A = 0.3
    T_m = 1323
    T_0 = 298
    rho = 0.008395
    C_p = 0.43
    l = 30

    # Calculate the normalised volumetric energy density
    dd['E*'] = (A * dd['Power (W)']) / (2 * dd['Speed (mm/s)'] * l * (dd['Beam radius (um)'] / 1000)) * \
               (1 / (0.67 * rho * C_p * (T_m - T_0)))

    dd['1/h*'] = (dd['Beam radius (um)'] * 1e-3) / dd['Speed (mm/s)']

    dd['E*0'] = dd['E*'] * dd['1/h*']
    logging.info("Added Normalised Volumetric Energy Density to the DataFrame")

    return dd

def main():
    # Read Parquet file
    filepath = '/mnt/parscratch/users/eia19od/combined_params.parquet'

    # Try to read the parquet file
    logging.info("Loading parquet file")
    df = dd.read_parquet(filepath)
    logging.info(f"Successfully read parquet file: {filepath}")
    logging.info(f"Columns: {df.columns}")

    # Add derived parameters
    logging.info("Adding normalised enthalpy")
    df = add_normenthalpy(df)
    logging.info("Successfully added normalised enthalpy")


    logging.info("Adding normalised volumetric energy density")
    df = add_NVED(df)
    logging.info("Successfully added normalised volumetric energy density")



    # Save the DataFrame as a parquet file
    logging.info("Saving DataFrame as a parquet file")
    df.to_parquet('/mnt/parscratch/users/eia19od/combined_derived.parquet')
    logging.info("Successfully saved DataFrame as a parquet file")

    logging.info("Processing complete")

    # Open the parquet file and read the first 5 rows
    df = pd.read_parquet('/mnt/parscratch/users/eia19od/combined_derived.parquet')
    logging.info(f"First 5 rows: {df.head()}")

if __name__ == '__main__':
    main()

