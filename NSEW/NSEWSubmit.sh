#!/bin/bash

#SBATCH --job-name=insitu_generic

#SBATCH --nodes=1
#SBATCH --ntasks=1

#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=4
#SBATCH --mem=60G
#SBATCH --time=00:45:00
#SBATCH --output=result.log
#SBATCH --error=error.log

# Source the Conda initialization script

source /users/eia19od/in_situ/HPC-In-Situ/venv/bin/activate
python /users/eia19od/in_situ/HPC-In-Situ/NSEW/NSEWread.py
date
seff $SLURM_JOBID

deactivate
