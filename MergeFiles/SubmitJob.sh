#!/bin/bash

#SBATCH --job-name=Parquet_merging
#SBATCH --partition=gpu
#SBATCH --qos=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=2
#SBATCH --gres=gpu:2
#SBATCH --mem=256G
#SBATCH --time=00:20:00
#SBATCH --output=result.log
#SBATCH --error=error.log

# Source the Conda initialization script
source /opt/apps/testapps/common/software/staging/Anaconda3/2022.05/etc/profile.d/conda.sh

conda activate conda_env

date
python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/AddParameters.py
date

seff $SLURM_JOBID

conda deactivate
