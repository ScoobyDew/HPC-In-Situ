#!/bin/bash

#SBATCH --job-name=insitu_generic

#SBATCH --nodes=1
#SBATCH --ntasks=1

#SBATCH --nodes=1
#SBATCH --ntasks=16
#SBATCH --cpus-per-task=1
#SBATCH --mem=80G
#SBATCH --time=00:45:00
#SBATCH --output=result.log
#SBATCH --error=error.log

# Source the Conda initialization script

source /users/eia19od/in_situ/HPC-In-Situ/venv/bin/activate
python /users/eia19od/in_situ/HPC-In-Situ/Thresholding/Thresholding2.py
date
seff $SLURM_JOBID

deactivate
