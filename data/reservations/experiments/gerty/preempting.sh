#!/bin/bash
#SBATCH -N 1
#SBATCH -A nstaff
#SBATCH -C knl
#SBATCH -q regular
#SBATCH -t 00:02:00
#SBATCH --reservation=preemption_test_cori
#SBATCH --job-name=urgent_1


#OpenMP settings:
export OMP_NUM_THREADS=1
export OMP_PLACES=threads
export OMP_PROC_BIND=spread


#run the application:
sleep 1m
