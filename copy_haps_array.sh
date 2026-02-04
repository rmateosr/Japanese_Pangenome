#!/usr/bin/env bash
#SBATCH -J copy_haps
#SBATCH -t 06:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH -o /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies/logs/%x_%A_%a.out
#SBATCH -e /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies/logs/%x_%A_%a.err

set -euo pipefail

OUT_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies"
LOG_DIR="${OUT_DIR}/logs"
MANIFEST="${OUT_DIR}/hap_manifest.tsv"

mkdir -p "$OUT_DIR" "$LOG_DIR"

if [[ ! -s "$MANIFEST" ]]; then
  echo "ERROR: manifest not found or empty: $MANIFEST" >&2
  echo "Run: sbatch prep_hap_manifest.sh" >&2
  exit 1
fi

# Manifest has a header on line 1; array tasks are 1..N for data lines.
line_no=$((SLURM_ARRAY_TASK_ID + 1))
line="$(sed -n "${line_no}p" "$MANIFEST" || true)"

if [[ -z "$line" ]]; then
  echo "ERROR: No data line for SLURM_ARRAY_TASK_ID=${SLURM_ARRAY_TASK_ID} (line ${line_no}) in $MANIFEST" >&2
  exit 1
fi

ID="$(awk -F $'\t' '{print $1}' <<< "$line")"
FASTA1="$(awk -F $'\t' '{print $2}' <<< "$line")"
FASTA2="$(awk -F $'\t' '{print $3}' <<< "$line")"

dest1="${OUT_DIR}/${ID}.hap1.fa.gz"
dest2="${OUT_DIR}/${ID}.hap2.fa.gz"

# Skip if already present and non-empty
if [[ -s "$dest1" && -s "$dest2" ]]; then
  echo "[$ID] already present, skipping"
  exit 0
fi

# Validate sources
if [[ ! -s "$FASTA1" ]]; then
  echo "[$ID] ERROR: fasta1 source missing/empty: $FASTA1" >&2
  exit 2
fi
if [[ ! -s "$FASTA2" ]]; then
  echo "[$ID] ERROR: fasta2 source missing/empty: $FASTA2" >&2
  exit 2
fi

cp -f "$FASTA1" "$dest1"
cp -f "$FASTA2" "$dest2"

echo "[$ID] copied:"
ls -lh "$dest1" "$dest2"
