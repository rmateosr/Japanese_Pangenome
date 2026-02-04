#!/usr/bin/env bash
set -euo pipefail

MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest.tsv"

FASTQ_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Concatenated_HiFi"
OUT_FLAGSTAT_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Flagstat_HiFi"

# Temporary BAM location (will be created; BAMs removed after flagstat)
OUT_BAM_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/tmp_mm2_bam_hifi"

THREADS_MM2=16
THREADS_SORT=8
THREADS_FLAGSTAT=16

mkdir -p "$OUT_FLAGSTAT_DIR" "$OUT_BAM_DIR"

# Loop over manifest (tab-delimited)
while IFS=$'\t' read -r ID FASTA1 FASTA2 _CRAM; do
  # Skip header / blank lines
  [[ -z "${ID:-}" ]] && continue
  [[ "$ID" == "ID" ]] && continue

  FASTQ="${FASTQ_DIR}/${ID}.HiFi.fastq.gz"
  if [[ ! -r "$FASTQ" ]]; then
    echo "WARN: Missing concatenated HiFi FASTQ for $ID: $FASTQ (skipping)" >&2
    continue
  fi

  # Validate fasta paths exist; skip mapping for missing ones
  if [[ -z "${FASTA1:-}" || ! -r "$FASTA1" ]]; then
    echo "WARN: Missing/unreadable fasta1_path for $ID: ${FASTA1:-<empty>} (skipping hap1)" >&2
  fi
  if [[ -z "${FASTA2:-}" || ! -r "$FASTA2" ]]; then
    echo "WARN: Missing/unreadable fasta2_path for $ID: ${FASTA2:-<empty>} (skipping hap2)" >&2
  fi

  # Function to map + flagstat + cleanup
  run_one() {
    local hap="$1"
    local fasta="$2"
    local bam="${OUT_BAM_DIR}/${ID}.${hap}.mm2.sorted.bam"
    local flagout="${OUT_FLAGSTAT_DIR}/${ID}.${hap}.mm2.sorted.flagstat.txt"
    local tmpbam="${bam}.tmp"

    # Skip if fasta missing
    [[ -r "$fasta" ]] || return 0

    echo "INFO: Mapping $ID ($hap) FASTQ -> $fasta"
    minimap2 --cs -L -Y -t "$THREADS_MM2" -ax map-hifi "$fasta" "$FASTQ" \
      | samtools sort -@ "$THREADS_SORT" -o "$tmpbam" -

    mv -f "$tmpbam" "$bam"
    samtools index "$bam"

    samtools flagstat -@ "$THREADS_FLAGSTAT" "$bam" > "$flagout"

    # Cleanup BAM + index after flagstat
    rm -f "$bam" "${bam}.bai"
  }

  run_one "hap1" "$FASTA1"
  run_one "hap2" "$FASTA2"

done < "$MANIFEST"

echo "DONE: flagstats in $OUT_FLAGSTAT_DIR"
