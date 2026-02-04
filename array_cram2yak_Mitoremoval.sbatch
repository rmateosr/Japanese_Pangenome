#!/usr/bin/env bash
#SBATCH --job-name=mito_removal
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/logs/%x_%A_%a.out
#SBATCH --error=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/logs/%x_%A_%a.err

set -euo pipefail

LOG_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/logs"
mkdir -p "$LOG_DIR"

: "${MANIFEST:?MANIFEST env var is required}"
: "${TABLE:?TABLE env var is required}"

export THREADS="${THREADS:-${SLURM_CPUS_PER_TASK:-16}}"

# Base temp location; per-task subdir is created inside the run script.
export WORKDIR="${WORKDIR:-${SLURM_TMPDIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/tmp_mito_rm_work}}"

# Optional: tool environment
# module purge
# module load minimap2 samtools htslib 2>/dev/null || true

RUN_SCRIPT="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/run_one_mito_removal.sh"
bash "$RUN_SCRIPT" "$MANIFEST" "$TABLE"
