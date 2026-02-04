#!/usr/bin/env bash
set -euo pipefail

MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest.tsv"
TABLE="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/visc_fastq_table.txt"

# Sanity checks
[[ -r "$MANIFEST" ]] || { echo "ERROR: MANIFEST not readable: $MANIFEST" >&2; exit 1; }
[[ -r "$TABLE" ]]    || { echo "ERROR: TABLE not readable: $TABLE" >&2; exit 1; }

# Number of array tasks = number of data lines (exclude header)
n=$(( $(wc -l < "$MANIFEST") - 1 ))
if (( n <= 0 )); then
  echo "ERROR: MANIFEST has no data rows: $MANIFEST" >&2
  exit 1
fi

sbatch --array=1-"$n" \
  --export=ALL,MANIFEST="$MANIFEST",TABLE="$TABLE" \
  /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/array_cram2yak_Mitoremoval.sbatch
