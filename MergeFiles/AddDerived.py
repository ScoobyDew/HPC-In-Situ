"""
This Script is used to add derived parameters to the combined DataFrame including:
- Normalised volumetric energy density (VED)
- Normalised Enthalpy
"""

import os
import pandas as pd
import logging

def add_normenthalpy(dd):
    """
    Add normalised enthalpy to the Dask DataFrame

    Normalised enthalpy can be given by the eqation:
    $\frac{\Delta H}{h_s}=\frac{n Q}{h_s \sqrt{a v \sigma^3}}$

        where:
        - $\Delta H$ is the enthalpy
        - $h_s$ is the specific enthalpy of the material
        - $n$ is the number of layers
        - $Q$ is the heat input
        - $a$ is the laser absorptivity
        - $v$ is the scanning speed
        - $\sigma$ is the laser beam diameter

    Parameters:
    - dd: Dask DataFrame
    """
    df['norm_enthalpy'] = df['enthalpy'] / df['mass']
    return df