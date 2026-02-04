#!/usr/bin/env bash
set -euo pipefail

MANIFEST="${1:?manifest path required}"
TASK_ID="${2:?task id (ID column) required}"

# ----------------------------
# Config (override via env)
# ----------------------------
REF="${REF:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/References/GRCh38_full_analysis_set_plus_decoy_hla.fa}"
YAK_BIN="${YAK_BIN:-/lustre10/home/raulnmateos/Japanese_Pangenome/tools/yak/yak/yak}"

COUNTS_DIR="${COUNTS_DIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/counts_sample_1000G}"
QV_DIR="${QV_DIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/qv_sample_counts_sample_1000G}"

# Temporary FASTQs: prefer node-local scratch if available; otherwise fall back to your existing folder.
FASTQ_TMP_DIR="${FASTQ_TMP_DIR:-${SLURM_TMPDIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/samples_fastq}}"

CPUS="${SLURM_CPUS_PER_TASK:-16}"
SAMTOOLS_THREADS="${SAMTOOLS_THREADS:-$CPUS}"
YAK_THREADS="${YAK_THREADS:-$CPUS}"

# yak qv parameters
KMER_K="${KMER_K:-3.2g}"
LIM_L="${LIM_L:-100k}"

# If you ever want to keep counts for debugging: export KEEP_COUNTS=1
KEEP_COUNTS="${KEEP_COUNTS:-0}"

DEBUG="${DEBUG:-0}"
if [[ "$DEBUG" == "1" ]]; then
  set -x
fi

mkdir -p "$COUNTS_DIR" "$QV_DIR" "$FASTQ_TMP_DIR"

# ----------------------------
# Read manifest row for TASK_ID
# Manifest header: ID  fasta1_path  fasta2_path  cram
# ----------------------------
row="$(
  awk -v id="$TASK_ID" 'BEGIN{FS=OFS="\t"} NR==1{next} $1==id{print; exit}' "$MANIFEST"
)"
if [[ -z "$row" ]]; then
  echo "ERROR: ID '$TASK_ID' not found in manifest: $MANIFEST" >&2
  exit 2
fi

IFS=$'\t' read -r ID fasta1_path fasta2_path cram_uri <<< "$row"

if [[ -z "${ID:-}" ]]; then
  echo "ERROR: malformed row for ID=$TASK_ID (empty ID)" >&2
  exit 3
fi

# CRAM may be NA -> skip (cannot compute yak count / qv)
if [[ -z "${cram_uri:-}" || "$cram_uri" == "NA" || "$cram_uri" == "na" || "$cram_uri" == "." ]]; then
  echo "INFO: ID=$ID has cram=NA (or empty). Skipping."
  exit 0
fi

# Assemblies are required by manifest (fail fast if broken manifest)
if [[ -z "${fasta1_path:-}" || "$fasta1_path" == "NA" || "$fasta1_path" == "." ]]; then
  echo "ERROR: fasta1_path missing/NA for ID=$ID" >&2
  exit 4
fi
if [[ -z "${fasta2_path:-}" || "$fasta2_path" == "NA" || "$fasta2_path" == "." ]]; then
  echo "ERROR: fasta2_path missing/NA for ID=$ID" >&2
  exit 5
fi
if [[ ! -f "$fasta1_path" ]]; then
  echo "ERROR: fasta1_path not found for ID=$ID: $fasta1_path" >&2
  exit 6
fi
if [[ ! -f "$fasta2_path" ]]; then
  echo "ERROR: fasta2_path not found for ID=$ID: $fasta2_path" >&2
  exit 7
fi

# Tools / reference checks
if [[ ! -f "$REF" ]]; then
  echo "ERROR: reference FASTA not found: $REF" >&2
  exit 8
fi
if [[ ! -x "$YAK_BIN" ]]; then
  echo "ERROR: yak binary not executable: $YAK_BIN" >&2
  exit 9
fi

yak_out="${COUNTS_DIR}/${ID}_counts.pb.yak"
out_hap1="${QV_DIR}/${ID}_hap1_QV_Yak.txt"
out_hap2="${QV_DIR}/${ID}_hap2_QV_Yak.txt"

# If both QV outputs already exist, we consider QV complete; delete counts if present (unless KEEP_COUNTS=1)
if [[ -s "$out_hap1" && -s "$out_hap2" ]]; then
  echo "INFO: ID=$ID QV outputs already exist."
  if [[ "$KEEP_COUNTS" != "1" && -s "$yak_out" ]]; then
    echo "CLEAN: removing yak counts (QV complete): $yak_out"
    rm -f "$yak_out"
  fi
  exit 0
fi

# ----------------------------
# Step A: Ensure yak counts exist (create if missing)
# ----------------------------
if [[ -s "$yak_out" ]]; then
  echo "INFO: ID=$ID yak counts exist, reusing: $yak_out"
else
  tmp1="$(mktemp --tmpdir="$FASTQ_TMP_DIR" "${ID}_R1.XXXXXX.fastq.gz")"
  tmp2="$(mktemp --tmpdir="$FASTQ_TMP_DIR" "${ID}_R2.XXXXXX.fastq.gz")"
  tmp_yak="$(mktemp --tmpdir="$COUNTS_DIR" "${ID}_counts.XXXXXX.pb.yak")"

  cleanup_counts() {
    rm -f "$tmp1" "$tmp2"
    rm -f "$tmp_yak" 2>/dev/null || true
  }
  trap cleanup_counts EXIT

  echo "ID=$ID"
  echo "CRAM(S3)=$cram_uri"
  echo "REF=$REF"
  echo "COUNTS_DIR=$COUNTS_DIR"
  echo "FASTQ_TMP_DIR=$FASTQ_TMP_DIR"
  echo "CPUS=$CPUS (samtools=$SAMTOOLS_THREADS yak=$YAK_THREADS)"

  echo "Running samtools fastq (streaming from S3)..."
  samtools fastq -c 6 -@ "$SAMTOOLS_THREADS" \
    --reference "$REF" \
    -1 "$tmp1" \
    -2 "$tmp2" \
    "$cram_uri"

  gzip -t "$tmp1"
  gzip -t "$tmp2"

  echo "Running yak count..."
  "$YAK_BIN" count -b37 -t"$YAK_THREADS" -o "$tmp_yak" "$tmp1" "$tmp2"

  if [[ ! -s "$tmp_yak" ]]; then
    echo "ERROR: yak count output is empty for ID=$ID" >&2
    exit 10
  fi

  mv -f "$tmp_yak" "$yak_out"
  echo "DONE (yak count): $yak_out"

  # Remove FASTQs after successful count
  rm -f "$tmp1" "$tmp2"
  trap - EXIT
fi

# ----------------------------
# Step B: yak qv for hap1 and hap2 (from manifest)
# ----------------------------
run_qv() {
  local hap="$1"
  local assembly="$2"
  local out="$3"

  if [[ -s "$out" ]]; then
    echo "INFO: ID=$ID $hap QV exists, skipping: $out"
    return 0
  fi

  local tmp_out
  tmp_out="$(mktemp --tmpdir="$QV_DIR" "${ID}_${hap}_QV_Yak.XXXXXX.tmp")"

  echo "Running yak qv for ID=$ID $hap"
  if ! "$YAK_BIN" qv -t"$YAK_THREADS" -p -K"$KMER_K" -l"$LIM_L" \
        "$yak_out" "$assembly" > "$tmp_out"
  then
    rm -f "$tmp_out"
    echo "ERROR: yak qv failed for ID=$ID $hap" >&2
    return 1
  fi

  mv -f "$tmp_out" "$out"
  echo "DONE (yak qv): $out"
}

run_qv "hap1" "$fasta1_path" "$out_hap1"
run_qv "hap2" "$fasta2_path" "$out_hap2"

# ----------------------------
# Step C: remove yak counts after QV complete
# ----------------------------
if [[ -s "$out_hap1" && -s "$out_hap2" ]]; then
  if [[ "$KEEP_COUNTS" != "1" ]]; then
    echo "CLEAN: QV complete, removing yak counts: $yak_out"
    rm -f "$yak_out"
  else
    echo "INFO: KEEP_COUNTS=1, not deleting: $yak_out"
  fi
else
  echo "ERROR: QV outputs missing/empty after run for ID=$ID; not deleting counts." >&2
  exit 11
fi

echo "DONE: ID=$ID (yak count + yak qv + cleanup)"
