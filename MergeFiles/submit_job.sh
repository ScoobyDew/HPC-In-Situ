#!/bin/bash

SBATCH --job-name=Parquet merging  # Job name
SBATCH --nodes=1                   # Number of nodes
SBATCH --ntasks-per-node=32        # Number of cores
SBATCH --mem=32G               # Total memory per node
SBATCH --time=01:00:00             # Time limit hrs:min:sec
SBATCH --output=result_%j.log      # Standard output and error log
SBATCH --error=error_%j.log        # Error log
SBATCH --mail-eia19od=odew1@sheffield.ac.uk

module load python3                 # Load Python module, adjust as needed based on your HPC environment
export DATA_DIRECTORY=/users/eia19od/in_situ/HPC-In-Situ/MergeFiles

python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/CombineParquet.py      # Adjust the path to your Python script
