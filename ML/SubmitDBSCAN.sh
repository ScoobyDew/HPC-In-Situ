#!/bin/bash

#SBATCH --job-name=insitu_generic

#SBATCH --nodes=1
#SBATCH --ntasks=1

#SBATCH --nodes=1
#SBATCH --ntasks=16
#SBATCH --cpus-per-task=2
#SBATCH --mem=120G
#SBATCH --time=00:45:00
#SBATCH --output=result_%j.log
#SBATCH --error=error_%j.log

# Source the Conda initialization script

source /users/eia19od/in_situ/HPC-In-Situ/venv/bin/activate
python /users/eia19od/in_situ/HPC-In-Situ/ML/DBSCANmeltpool.py
date
seff $SLURM_JOBID

deactivate
