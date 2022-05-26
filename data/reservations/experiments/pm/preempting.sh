#!/bin/bash
#SBATCH -N 4
#SBATCH -A nstaff_g
#SBATCH -C gpu
#SBATCH -q regular
#SBATCH -t 00:02:00
#SBATCH --reservation=preemption_test_perlmutter
#SBATCH --job-name=urgent_4


#OpenMP settings:
export OMP_NUM_THREADS=1
export OMP_PLACES=threads
export OMP_PROC_BIND=spread


#run the application:
sleep 1m
