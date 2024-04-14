#!/bin/bash


#SBATCH --job-name=Parquet_merging  # Job name
#SBATCH --nodes=1                   # Number of nodes
#SBATCH --ntasks-per-node=1        # Number of cores
#SBATCH --cpus-per-task=6
#SBATCH --mem=128G                  # Total memory per node
#SBATCH --time=06:00:00             # Time limit hrs:min:sec
#SBATCH --output=result.log      # Standard output and error log
#SBATCH --error=error.log        # Error log
#SBATCH --mail-user=odew1@sheffield.ac.uk

module load Python/3.10.8-GCCcore-12.2.0
export DATA_DIRECTORY=/mnt/parscratch/users/eia19od/Cleaned

pip install pandas
pip install numpy
pip install fastparquet

# start time of the job
date  # Print the current date and time
python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/SimplerCombine.py      # Adjust the path to your Python script

# end time of the job
date  # Print the current date and time

# Print out resource usage
seff $SLURM_JOBID

