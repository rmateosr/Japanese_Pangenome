#!/usr/bin/env bash
#SBATCH --job-name=fai_maker
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=06:00:00
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err

set -euo pipefail

# Work from the directory where you submit the job
cd "$SLURM_SUBMIT_DIR"

mkdir -p logs

# If samtools/bgzip are from a module system, uncomment and adjust:
# module purge
# module load samtools
# module load htslib

# Or if you rely on conda:
# source ~/miniconda3/etc/profile.d/conda.sh
# conda activate base

# Run
./fai_maker.sh /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest.tsv


