#!/usr/bin/env bash
#SBATCH --job-name=cram2yak
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log/%x_%A_%a.out
#SBATCH --error=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log/%x_%A_%a.err

set -euo pipefail

LOG_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log"
mkdir -p "$LOG_DIR"

MANIFEST="${MANIFEST:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/manifest.tsv}"

# Map array index -> ID (first column), assuming header row.
ID="$(
  awk -v n="$SLURM_ARRAY_TASK_ID" 'BEGIN{FS="\t"} NR==n+1{print $1; exit}' "$MANIFEST"
)"

if [[ -z "$ID" ]]; then
  echo "ERROR: could not map SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID to an ID in $MANIFEST" >&2
  exit 10
fi

bash /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/run_one_cram2yak_100kremoval_andl100k.sh \
  "$MANIFEST" "$ID"
