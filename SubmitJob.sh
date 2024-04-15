#!/bin/bash

#SBATCH --job-name=insitu_generic

#SBATCH --nodes=1
#SBATCH --ntasks=1

#SBATCH --partition=gpu
#SBATCH --qos=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=4
#SBATCH --gres=gpu:4       # 4 GPUs for job
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=result.log
#SBATCH --error=error.log

# Source the Conda initialization script
module load CUDA/12.0.0

source /users/eia19od/in_situ/HPC-In-Situ/venv/bin/activate
python /users/eia19od/in_situ/HPC-In-Situ/Plots/2dHist.py
date
seff $SLURM_JOBID

deactivate
