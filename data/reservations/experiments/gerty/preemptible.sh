#!/bin/bash
#SBATCH -N 1
#SBATCH -A nstaff
#SBATCH -C knl
#SBATCH -q regular
#SBATCH -t 00:10:00
#SBATCH --signal=R:INT@300
#SBATCH --job-name=preem_1_300
#SBATCH --exclude=nid[00041,00045-00051,00200-00202,00225-00235,00249-00251,00392-00401,00403,00406-00419,00432-00435]


#OpenMP settings:
export OMP_NUM_THREADS=1
export OMP_PLACES=threads
export OMP_PROC_BIND=spread


#run the application:
srun -n 1 ./patient_app
