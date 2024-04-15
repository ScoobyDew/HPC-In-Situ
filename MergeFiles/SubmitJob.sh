#!/bin/bash

#SBATCH --job-name=insitu_generic

#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16

#SBATCH --mem=180G
#SBATCH --time=00:30:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=odew1@sheffield.ac.uk
#SBATCH --output=result.log
#SBATCH --error=error.log

# Source the Conda initialization script
source /users/eia19od/in_situ/HPC-In-Situ/venv/bin/activate
python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/AddParameters.py
date

seff $SLURM_JOBID

deactivate
