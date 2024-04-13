#!/bin/bash


#SBATCH --job-name=Parquet_merging  # Job name
#SBATCH --nodes=1                   # Number of nodes
#SBATCH --ntasks-per-node=1        # Number of cores
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G                  # Total memory per node
#SBATCH --time=01:00:00             # Time limit hrs:min:sec
#SBATCH --output=result_%j.log      # Standard output and error log
#SBATCH --error=error_%j.log        # Error log
#SBATCH --mail-user=odew1@sheffield.ac.uk

module load Python/3.10.8-GCCcore-12.2.0
export DATA_DIRECTORY=/mnt/parscratch/users/eia19od/Cleaned

pip install dask[complete]
pip install pandas
pip install numpy
pip install fastparquet
pip install openpyxl

# start time of the job
date  # Print the current date and time
python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/CombineParquet.py      # Adjust the path to your Python script

# end time of the job
date  # Print the current date and time

# Print out resource usage
seff $SLURM_JOBID

# Set your GitHub token as an environment variable
export GITHUB_TOKEN="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"

# Configure your Git username and email
git config --global user.name "ScoobyDew"
git config --global user.email "oliver.e.dew@gmail.com"

# Add all new and changed files to the git
git add .

# Commit the changes
git commit -m "Automated commit from script"

# Push the changes to your remote repository
git push https://$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@github.com//ScoobyDew/HPC-In-Situ.git
