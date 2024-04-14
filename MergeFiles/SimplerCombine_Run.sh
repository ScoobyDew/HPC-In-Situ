#!/bin/bash

#SBATCH --job-name=Parquet_merging  # Job name
#SBATCH --nodes=1                   # Number of nodes
#SBATCH --ntasks-per-node=1         # Number of cores
#SBATCH --cpus-per-task=2
#SBATCH --mem=256G                  # Total memory per node
#SBATCH --time=00:10:00             # Time limit hrs:min:sec
#SBATCH --output=result.log         # Standard output and error log
#SBATCH --error=error.log           # Error log
#SBATCH --mail-user=odew1@sheffield.ac.uk


source /users/eia19od/in_situ/HPC-In-Situ/venv/bin/activate


date  # Print the current date and time

# Run your Python script
python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/SimplerCombine.py

# end time of the job
date  # Print the current date and time

# Print out resource usage
seff $SLURM_JOBID

# Deactivate the virtual environment
deactivate
