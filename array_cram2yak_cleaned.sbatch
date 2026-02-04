#!/usr/bin/env bash
#SBATCH --job-name=cram2yak
#SBATCH --output=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log/%x_%A_%a.out
#SBATCH --error=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log/%x_%A_%a.err

set -euo pipefail

MANIFEST="${MANIFEST:?MANIFEST env var is required}"
RUN_TAG="${RUN_TAG:?RUN_TAG env var is required}"

# Optional, passed from outer script:
LIM_L="${LIM_L:-100k}"
CONTIG_MIN_BP="${CONTIG_MIN_BP:-0}"
COUNTS_BASE="${COUNTS_BASE:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/counts_sample_1000G}"
QV_BASE="${QV_BASE:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/qv_sample_counts_sample_1000G}"

# Map array index -> ID (first column), assuming header row.
ID="$(
  awk -v n="$SLURM_ARRAY_TASK_ID" 'BEGIN{FS="\t"} NR==n+1{print $1; exit}' "$MANIFEST"
)"
if [[ -z "$ID" ]]; then
  echo "ERROR: could not map SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID to an ID in $MANIFEST" >&2
  exit 10
fi

# Call the generic inner runner (single script, parameters via env)
bash /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/run_one_cram2yak_cleaned.sh \
  "$MANIFEST" "$ID"
