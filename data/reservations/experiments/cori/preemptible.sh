#!/bin/bash
#SBATCH -N 1
#SBATCH -A nstaff
#SBATCH -C knl
#SBATCH -q regular
#SBATCH -t 00:05:00
#SBATCH --signal=R:INT@60
#SBATCH --job-name=preem_1_60


#OpenMP settings:
export OMP_NUM_THREADS=1
export OMP_PLACES=threads
export OMP_PROC_BIND=spread


#run the application:
srun -n 1 ./patient_app
