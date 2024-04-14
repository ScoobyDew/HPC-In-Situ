#!/bin/bash

## Generic SLURM script for a serial job...

# Delete any previous logs


#SBATCH --job-name=Parquet_merging  # Job name

#!/bin/bash
#SBATCH --partition=gpu
#SBATCH --qos=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=2      # Number of CPU cores per task
#SBATCH --gres=gpu:2           # 2 GPUs for job


#SBATCH --mem=256G                  # Total memory per node
#SBATCH --time=00:20:00             # Time limit hrs:min:sec
#SBATCH --output=result.log         # Standard output and error log
#SBATCH --error=error.log           # Error log


source activate /users/eia19od/in_situ/HPC-In-Situ/conda_env
conda install openpyxl -y
date  # Print the current date and time

python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/AddParameters.py

date  # Print the current date and time
seff $SLURM_JOBID
deactivate
