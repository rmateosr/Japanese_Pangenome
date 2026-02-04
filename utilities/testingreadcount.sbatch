#!/usr/bin/env bash
#SBATCH -J testingreadcount
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH -o /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log/%x.%j.out
#SBATCH -e /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log/%x.%j.err

set -euo pipefail

SCRIPT="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/testingreadcount.sh"

mkdir -p /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log

# Use all allocated CPUs unless explicitly overridden
export SAMTOOLS_THREADS="${SAMTOOLS_THREADS:-$SLURM_CPUS_PER_TASK}"

# Prefer node-local scratch if available
export WORKDIR="${WORKDIR:-${SLURM_TMPDIR:-/tmp}/samtools_fastq_compare_${USER}_${SLURM_JOB_ID}}"

bash "$SCRIPT"
