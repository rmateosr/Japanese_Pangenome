#!/usr/bin/env bash
set -euo pipefail

: "${MANIFEST:?MANIFEST is required}"
: "${TABLE:?TABLE is required}"
: "${SLURM_ARRAY_TASK_ID:?SLURM_ARRAY_TASK_ID is required}"

# Final outputs: per-sample TSV files with BAM sizes
OUT_BAMSIZE_DIR="${OUT_BAMSIZE_DIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/BamSizes_HiFi}"

# Intermediates go in a NEW folder (different from the one used previously).
# Prefer node-local scratch; otherwise fall back to a NEW Lustre tmp dir.
if [[ -n "${SLURM_TMPDIR:-}" ]]; then
  WORKDIR="${WORKDIR:-$SLURM_TMPDIR/hifi_mm2_bamsize_work}"
else
  WORKDIR="${WORKDIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/tmp_hifi_mm2_bamsize_work}"
fi

THREADS="${THREADS:-${SLURM_CPUS_PER_TASK:-16}}"

mkdir -p "$OUT_BAMSIZE_DIR" "$WORKDIR"

# Array index 1 corresponds to first data row (line 2) because line 1 is header.
line="$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$MANIFEST" || true)"
if [[ -z "$line" ]]; then
  echo "ERROR: No manifest line for SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID" >&2
  exit 1
fi

IFS=$'\t' read -r ID FASTA1 FASTA2 _CRAM <<< "$line"

if [[ -z "${ID:-}" || "$ID" == "ID" ]]; then
  echo "ERROR: Parsed invalid ID from manifest line: $line" >&2
  exit 1
fi

# Collect HiFi fastq.gz paths for this sample
mapfile -t PATHS < <(awk -F $'\t' -v id="$ID" '$1=="HiFi" && $2==id {print $3}' "$TABLE")

if (( ${#PATHS[@]} == 0 )); then
  echo "WARN: No HiFi rows found for sample=$ID in $TABLE (skipping)" >&2
  exit 0
fi

# Sanity check existence/readability
for p in "${PATHS[@]}"; do
  if [[ ! -r "$p" ]]; then
    echo "ERROR: Missing/unreadable HiFi FASTQ for $ID: $p" >&2
    exit 1
  fi
done

# Temporary concatenated FASTQ (removed at end)
FASTQ="${WORKDIR}/${ID}.HiFi.fastq.gz"
FASTQ_TMP="${FASTQ}.tmp"

# Per-sample output (safe for SLURM arrays; no write contention)
OUT_TSV="${OUT_BAMSIZE_DIR}/${ID}.bam_sizes.tsv"

cleanup() {
  rm -f "$FASTQ_TMP" "$FASTQ"
}
trap cleanup EXIT

echo "INFO: [$ID] Concatenating ${#PATHS[@]} HiFi FASTQ(s) -> $FASTQ"
cat "${PATHS[@]}" > "$FASTQ_TMP"
mv -f "$FASTQ_TMP" "$FASTQ"

# Write header once per sample output
printf "ID\thap\tbam_path\tsize_bytes\tsize_GiB\n" > "$OUT_TSV"

run_one() {
  local hap="$1"
  local fasta="$2"

  if [[ -z "${fasta:-}" || ! -r "$fasta" ]]; then
    echo "WARN: Missing/unreadable ${hap} fasta for $ID: ${fasta:-<empty>} (skipping ${hap})" >&2
    return 0
  fi

  local bam="${WORKDIR}/${ID}.${hap}.mm2.sorted.bam"
  local tmpbam="${bam}.tmp"

  echo "INFO: [$ID][$hap] minimap2/samtools sort threads=$THREADS"
  minimap2 --cs -L -Y -t "$THREADS" -ax map-hifi "$fasta" "$FASTQ" \
    | samtools sort -@ "$THREADS" -o "$tmpbam" -

  mv -f "$tmpbam" "$bam"

  # Record size (bytes + GiB)
  local size_bytes size_gib
  size_bytes="$(stat -c '%s' "$bam")"
  size_gib="$(awk -v b="$size_bytes" 'BEGIN{printf "%.3f", b/1024/1024/1024}')"

  printf "%s\t%s\t%s\t%s\t%s\n" "$ID" "$hap" "$bam" "$size_bytes" "$size_gib" >> "$OUT_TSV"

  # Remove BAM after size capture (scriptÅfs only purpose is size listing)
  #rm -f "$bam"
}

run_one "hap1" "$FASTA1"
run_one "hap2" "$FASTA2"

echo "DONE: $ID (BAM sizes written to $OUT_TSV; intermediates cleaned)"
