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


def add_normenthalpy(dd):
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

    # Constants
    n = 0.3
    a = 3.25
    rho = 0.008395
    Cp = 0.43
    T_sol = 1260

    # Calculate the specific enthalpy
    hs = rho * Cp * T_sol

    # Calculate the normalised enthalpy
    dd['Normalised Enthalpy'] =(n * dd['Power (W)']) /\
                               (hs * (a * dd['Laser Speed (mm/s)'] * (dd['Beam radius (um)'] / 1000) ** 3)

    logging.info("Added Normalised Enthalpy to the DataFrame")

    return dd

def add_NED(dd):

