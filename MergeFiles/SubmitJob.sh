#!/bin/bash

## Generic SLURM script for a serial job...


#SBATCH --job-name=Parquet_merging  # Job name
#SBATCH --nodes=1                   # Number of nodes
#SBATCH --ntasks-per-node=1         # Number of cores
#SBATCH --cpus-per-task=2
#SBATCH --mem=256G                  # Total memory per node
#SBATCH --time=00:20:00             # Time limit hrs:min:sec
#SBATCH --output=result.log         # Standard output and error log
#SBATCH --error=error.log           # Error log


source /users/eia19od/in_situ/HPC-In-Situ/venv/bin/activate
date  # Print the current date and time
python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/AddParameters.py
date  # Print the current date and time
seff $SLURM_JOBID
deactivate
