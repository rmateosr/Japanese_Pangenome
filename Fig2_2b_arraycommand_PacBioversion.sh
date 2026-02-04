#!/usr/bin/env bash
set -euo pipefail

MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifestPacBio.tsv"

# number of data lines (exclude header)
n=$(($(wc -l < "$MANIFEST") - 1))
if [[ "$n" -le 0 ]]; then
  echo "ERROR: manifest has no data rows: $MANIFEST" >&2
  exit 1
fi

sbatch --array=1-"$n" \
  --export=ALL,MANIFEST="$MANIFEST" \
  /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/array_hifi2yak_PacBioversion.sbatch
