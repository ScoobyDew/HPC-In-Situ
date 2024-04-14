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
source /opt/apps/testapps/common/software/staging/Anaconda3/2022.05/etc/profile.d/conda.sh
module load CUDA/12.0.0
conda activate conda_env

# Install libmamba solver
conda install -n base conda-libmamba-solver

# Create the RAPIDS environment
conda create --solver=libmamba -n rapids-24.04 -c rapidsai -c conda-forge -c nvidia  \
    rapids=24.04 python=3.11 cuda-version=12.0

# Activate the RAPIDS environment
conda activate rapids-24.04

date
python /users/eia19od/in_situ/HPC-In-Situ/MergeFiles/AddParameters.py
date

seff $SLURM_JOBID

conda deactivate
