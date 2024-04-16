#!/bin/bash

#SBATCH --job-name=insitu_generic

#SBATCH --nodes=1
#SBATCH --ntasks=1

#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=240G
#SBATCH --time=00:45:00
#SBATCH --output=result_%j.log
#SBATCH --error=error_%j.log

# Source the Conda initialization script

source /users/eia19od/in_situ/HPC-In-Situ/venv/bin/activate
python /users/eia19od/in_situ/HPC-In-Situ/Plots/plotnormal.py
date
seff $SLURM_JOBID

deactivate
