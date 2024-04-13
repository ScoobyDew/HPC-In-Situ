#!/bin/bash

#SBATCH --job-name=Parquet_merging  # Job name
#SBATCH --nodes=1                   # Number of nodes
#SBATCH --ntasks-per-node=32        # Number of cores
#SBATCH --cpus-per-task=1
#SBATCH --gres=gpu:1
#SBATCH --mem=80G                   # Total memory per node
#SBATCH --time=01:00:00             # Time limit hrs:min:sec
#SBATCH --output=result_%j.log      # Standard output and error log
#SBATCH --error=error_%j.log        # Error log
#SBATCH --mail-user=odew1@sheffield.ac.uk

module load Anaconda3/2022.10

# Activate the Conda environment
source activate rapids-0.19

export DATA_DIRECTORY=/mnt/parscratch/users/eia19od/Cleaned
conda create -n rapids-env -c rapidsai -c nvidia -c conda-forge cudf=21.06 dask-cudf=21.06 python=3.8 cudatoolkit=11.0

pip install dask[complete]
pip install numpy
pip install pandas
pip install os
pip install sys

pip install fastparquet


# start time of the job
date  # Print the current date and time
python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/CombineParquet.py      # Adjust the path to your Python script

# end time of the job
date  # Print the current date and time

# Print out resource usage
seff $SLURM_JOBID