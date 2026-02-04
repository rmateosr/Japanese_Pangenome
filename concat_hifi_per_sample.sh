#!/usr/bin/env bash
set -euo pipefail

MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest.tsv"
TABLE="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/visc_fastq_table.txt"
OUTDIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Concatenated_HiFi"

mkdir -p "$OUTDIR"

# Loop over manifest (tab-delimited). Only the first column (ID) is required.
while IFS=$'\t' read -r ID _rest; do
  # Skip header / blank lines
  [[ -z "${ID:-}" ]] && continue
  [[ "$ID" == "ID" ]] && continue

  # Collect HiFi fastq.gz paths for this sample
  mapfile -t PATHS < <(awk -F $'\t' -v id="$ID" '$1=="HiFi" && $2==id {print $3}' "$TABLE")

  if (( ${#PATHS[@]} == 0 )); then
    echo "WARN: No HiFi rows found for sample=$ID in $TABLE" >&2
    continue
  fi

  # Optional: sanity check existence (will fail fast if any path is missing)
  for p in "${PATHS[@]}"; do
    if [[ ! -r "$p" ]]; then
      echo "ERROR: Missing/unreadable HiFi FASTQ for $ID: $p" >&2
      exit 1
    fi
  done

  OUT="${OUTDIR}/${ID}.HiFi.fastq.gz"
  TMP="${OUT}.tmp"

  echo "INFO: $ID -> ${#PATHS[@]} file(s) -> $OUT"
  # Concatenate gz members safely (keeps gzip stream valid for most tools)
  cat "${PATHS[@]}" > "$TMP"
  mv -f "$TMP" "$OUT"
done < "$MANIFEST"
