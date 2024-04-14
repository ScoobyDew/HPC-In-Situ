#!/bin/bash

#SBATCH --job-name=Parquet_merging
#SBATCH --partition=gpu
#SBATCH --qos=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=2
#SBATCH --gres=gpu:2
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=result.log
#SBATCH --error=error.log

# Source the Conda initialization script
source /users/eia19od/in_situ/HPC-In-Situ/venv/bin/activate
module load CUDA/12.0.0

python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/AddParameters.py
date

seff $SLURM_JOBID

deactivate
