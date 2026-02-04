#!/usr/bin/env bash
set -euo pipefail

MANIFEST="${1:?usage: submit_fai_array.sh /path/to/hap_manifest.tsv}"

# Adjust defaults here if you want
OUTDIR="${OUTDIR:-/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/fai}"
JOBS_PER_BATCH="${JOBS_PER_BATCH:-20}"   # concurrency cap, e.g. 20
CPUS="${CPUS:-8}"
MEM="${MEM:-16G}"

[[ -f "$MANIFEST" ]] || { echo "ERROR: manifest not found: $MANIFEST" >&2; exit 1; }

mkdir -p "$OUTDIR/log"

# Number of data rows (exclude header)
n=$(( $(wc -l < "$MANIFEST") - 1 ))
(( n >= 1 )) || { echo "ERROR: manifest has no data rows: $MANIFEST" >&2; exit 1; }

sbatch \
  --job-name=fai_dp \
  --cpus-per-task="$CPUS" \
  --mem="$MEM" \
  --output="$OUTDIR/log/%x_%A_%a.out" \
  --error="$OUTDIR/log/%x_%A_%a.err" \
  --array=1-"$n"%${JOBS_PER_BATCH} \
  --export=ALL,MANIFEST="$MANIFEST",OUTDIR="$OUTDIR" \
  make_fai_array.sbatch
