#!/usr/bin/env bash
#SBATCH --job-name=cram2yak_test1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --output=log/%x_%j.out
#SBATCH --error=log/%x_%j.err

set -euo pipefail

# Set DEBUG=1 to enable command tracing
DEBUG="${DEBUG:-0}"
if [[ "$DEBUG" == "1" ]]; then
  set -x
fi

mkdir -p log

#module use /usr/local/package/modulefiles
#module load samtools/1.19
# module load yak/<version>

# ----------------------------
# User configuration
# ----------------------------
CRAM_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/high_coverage_data"
FASTQ_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/samples_fastq"
REF="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/References/GRCh38_full_analysis_set_plus_decoy_hla.fa"

YAK_BIN="/lustre10/home/raulnmateos/Japanese_Pangenome/tools/yak/yak/yak"

COUNTS_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/counts_sample_1000G"
QV_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/qv_sample_counts_sample_1000G"

mkdir -p "$FASTQ_DIR" "$COUNTS_DIR" "$QV_DIR"

CPUS="${SLURM_CPUS_PER_TASK:-16}"
SAMTOOLS_THREADS="${SAMTOOLS_THREADS:-$CPUS}"
YAK_THREADS="${YAK_THREADS:-$CPUS}"

shopt -s nullglob

# ----------------------------
# Pick ONLY the first CRAM (lexicographic order) [test mode]
# ----------------------------
mapfile -t CRAMS < <(ls -1 "$CRAM_DIR"/*.cram 2>/dev/null | sort -V)
if (( ${#CRAMS[@]} == 0 )); then
  echo "[ERROR] No CRAM files found in: $CRAM_DIR" >&2
  exit 1
fi

cram="${CRAMS[0]}"
base="$(basename "$cram")"
id="${base%%.*}"

echo "[TEST] Running only first CRAM"
echo "       CRAM: $cram"
echo "       ID:   $id"

out1="$FASTQ_DIR/${id}_R1.fastq.gz"
out2="$FASTQ_DIR/${id}_R2.fastq.gz"
yak_out="$COUNTS_DIR/${id}_counts.pb.yak"

# ----------------------------
# 1) Ensure CRAM index exists
# ----------------------------
if [[ ! -s "${cram}.crai" ]]; then
  echo "[INDEX] Building index for $base"
  samtools index -@ "$SAMTOOLS_THREADS" -c "$cram"
fi

# ----------------------------
# 2) CRAM -> paired FASTQ.gz (only if needed)
# ----------------------------
if [[ ! -s "$out1" || ! -s "$out2" ]]; then
  echo "[FASTQ] Generating FASTQ.gz for $id"

  tmp1="$(mktemp --tmpdir="$FASTQ_DIR" "${id}_R1.XXXXXX.fastq.gz")"
  tmp2="$(mktemp --tmpdir="$FASTQ_DIR" "${id}_R2.XXXXXX.fastq.gz")"

  if ! samtools fastq -c 6 -@ "$SAMTOOLS_THREADS" \
        --reference "$REF" \
        -1 "$tmp1" \
        -2 "$tmp2" \
        "$cram"
  then
    rm -f "$tmp1" "$tmp2"
    echo "[ERROR] samtools fastq failed for $id" >&2
    exit 1
  fi

  # Sanity check: ensure they are valid gzip streams
  if ! gzip -t "$tmp1" || ! gzip -t "$tmp2"; then
    rm -f "$tmp1" "$tmp2"
    echo "[ERROR] FASTQ outputs are not valid gzip for $id" >&2
    exit 1
  fi

  mv -f "$tmp1" "$out1"
  mv -f "$tmp2" "$out2"
else
  echo "[FASTQ] $id: FASTQs already exist, reusing."
fi

# ----------------------------
# 3) yak count
# ----------------------------
if [[ -s "$yak_out" ]]; then
  echo "[YAK] $id: output exists ($yak_out). Skipping."
else
  echo "[YAK] Counting k-mers for $id"
  "$YAK_BIN" count -b37 -t"$YAK_THREADS" -o "$yak_out" "$out1" "$out2"
  echo "[YAK] Done: $yak_out"
fi

# ----------------------------
# 4) Remove FASTQ.gz only after yak output is present
# ----------------------------
if [[ -s "$yak_out" ]]; then
  echo "[CLEAN] Removing intermediates for $id: $out1 $out2"
  rm -f "$out1" "$out2"
else
  echo "[CLEAN] Not removing FASTQs for $id because yak output is missing/empty."
fi

echo "[DONE] Test sample complete."
