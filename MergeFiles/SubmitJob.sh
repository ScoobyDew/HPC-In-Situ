#!/bin/bash

#SBATCH --job-name=insitu_generic
#SBATCH --partition=gpu
#SBATCH --qos=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --gres=gpu:6
#SBATCH --mem=180G
#SBATCH --mem-per-cpu=16
#SBATCH --time=00:30:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=odew1@sheffield.ac.uk
#SBATCH --output=result.log
#SBATCH --error=error.log

# Source the Conda initialization script
source /users/eia19od/in_situ/HPC-In-Situ/venv/bin/activate
module load CUDA/12.0.0
sacct -j jobid --format=JobID,MaxRSS
python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/AddParameters.py
sacct -j jobid --format=JobID,MaxRSS
date

seff $SLURM_JOBID

deactivate
