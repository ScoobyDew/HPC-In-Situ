#!/bin/bash

#SBATCH --job-name=Parquet_merging  # Job name
#SBATCH --nodes=1                   # Number of nodes
#SBATCH --ntasks-per-node=32        # Number of cores
#SBATCH --mem=32G                   # Total memory per node
#SBATCH --time=01:00:00             # Time limit hrs:min:sec
#SBATCH --output=result_%j.log      # Standard output and error log
#SBATCH --error=error_%j.log        # Error log
#SBATCH --mail-user=odew1@sheffield.ac.uk

module load Python/3.10.8-GCCcore-12.2.0
export DATA_DIRECTORY=/users/eia19od/in_situ/HPC-In-Situ/MergeFiles


pip install numpy
pip install pandas
pip install os
pip install sys
pip install logging
pip install multiprocessing
pip install fastparquet

# start time of the job
date  # Print the current date and time
python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/CombineParquet.py      # Adjust the path to your Python script

# end time of the job
date  # Print the current date and time

# Print out resource usage
seff $SLURM_JOBID