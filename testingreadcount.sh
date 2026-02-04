#!/usr/bin/env bash
set -euo pipefail

# -----------------------
# Inputs
# -----------------------
CRAM_S3="s3://1000genomes/1000G_2504_high_coverage/data/ERR3239557/NA18939.final.cram"
REF="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/References/GRCh38_full_analysis_set_plus_decoy_hla.fa"
SAMTOOLS_THREADS="${SAMTOOLS_THREADS:-8}"

# Working dir (node-local if available)
WORKDIR="${WORKDIR:-${SLURM_TMPDIR:-/tmp}/samtools_fastq_compare_${USER}_$$}"
mkdir -p "$WORKDIR"

echo "WORKDIR=$WORKDIR"
echo "CRAM_S3=$CRAM_S3"
echo "REF=$REF"
echo "SAMTOOLS_THREADS=$SAMTOOLS_THREADS"

# -----------------------
# Prep reference index (required by samtools --reference)
# -----------------------
if [[ ! -s "${REF}.fai" ]]; then
  echo "Indexing reference with samtools faidx..."
  samtools faidx "$REF"
fi

# -----------------------
# Fetch CRAM locally (important: samtools fastq needs normal file I/O; streaming CRAM can be fragile)
# -----------------------
CRAM_LOCAL="${WORKDIR}/NA18939.final.cram"
echo "Downloading CRAM..."
aws s3 cp --no-sign-request "$CRAM_S3" "$CRAM_LOCAL"

# CRAI may or may not exist publicly; try but don't fail.
aws s3 cp --no-sign-request "${CRAM_S3}.crai" "${CRAM_LOCAL}.crai" 2>/dev/null || true

# -----------------------
# Method 1: view -> collate -> fastq split R1/R2; discard singleton/orphans
# -----------------------
tmp1="${WORKDIR}/m1_R1.fq.gz"
tmp2="${WORKDIR}/m1_R2.fq.gz"

echo "Running method 1 (split R1/R2; discard -0/-s)..."
samtools view -u -@ "$SAMTOOLS_THREADS" --reference "$REF" "$CRAM_LOCAL" \
  | samtools collate -u -O - \
  | samtools fastq -c 6 -@ "$SAMTOOLS_THREADS" --reference "$REF" \
      -1 >(gzip -c > "$tmp1") \
      -2 >(gzip -c > "$tmp2") \
      -0 /dev/null \
      -s /dev/null \
      -n \
      -

gzip -t "$tmp1"
gzip -t "$tmp2"

# -----------------------
# Method 2: samtools fastq single stream (keeps everything: pairs + singletons)
# -----------------------
tmp_fq="${WORKDIR}/m2_all.fq.gz"

echo "Running method 2 (single FASTQ stream; no split)..."
samtools fastq -@ "$SAMTOOLS_THREADS" --reference "$REF" "$CRAM_LOCAL" \
  | gzip -c > "$tmp_fq"
gzip -t "$tmp_fq"

# -----------------------
# Optional control: method 2b = direct fastq split + discard, to compare apples-to-apples with method 1
# (This isolates whether collate changes counts.)
# -----------------------
tmp1b="${WORKDIR}/m2b_R1.fq.gz"
tmp2b="${WORKDIR}/m2b_R2.fq.gz"

echo "Running method 2b (direct split R1/R2; discard -0/-s; matches method 1 semantics)..."
samtools fastq -c 6 -@ "$SAMTOOLS_THREADS" --reference "$REF" \
    -1 >(gzip -c > "$tmp1b") \
    -2 >(gzip -c > "$tmp2b") \
    -0 /dev/null \
    -s /dev/null \
    -n \
    "$CRAM_LOCAL"

gzip -t "$tmp1b"
gzip -t "$tmp2b"

# -----------------------
# Counting: number of FASTQ records = lines/4
# -----------------------
count_fastq_records() {
  local fqgz="$1"
  # wc -l is integer; NR/4 is safe if file is valid FASTQ
  zcat "$fqgz" | wc -l | awk '{print int($1/4)}'
}

m1_r1=$(count_fastq_records "$tmp1")
m1_r2=$(count_fastq_records "$tmp2")
m1_total=$((m1_r1 + m1_r2))

m2_total=$(count_fastq_records "$tmp_fq")

m2b_r1=$(count_fastq_records "$tmp1b")
m2b_r2=$(count_fastq_records "$tmp2b")
m2b_total=$((m2b_r1 + m2b_r2))

echo
echo "================ RESULTS ================"
echo "Method 1: R1=$m1_r1  R2=$m1_r2  TOTAL_READS(R1+R2)=$m1_total"
echo "Method 2: ALL_STREAM_TOTAL_READS=$m2_total"
echo "Method 2b: R1=$m2b_r1  R2=$m2b_r2  TOTAL_READS(R1+R2)=$m2b_total"
echo "========================================"
echo

# Comparisons / verdicts
if [[ "$m1_r1" -ne "$m1_r2" ]]; then
  echo "WARNING: Method 1 R1 != R2 (unexpected if only proper pairs were emitted)."
fi

if [[ "$m2b_total" -eq "$m1_total" ]]; then
  echo "OK: Method 1 and Method 2b match exactly (collate did not change read counts under paired-only semantics)."
else
  echo "MISMATCH: Method 1 and Method 2b differ. This suggests differences in how reads are grouped/selected (unexpected) or an execution issue."
fi

if [[ "$m2_total" -eq "$m1_total" ]]; then
  echo "OK: Method 2 total reads equals Method 1 total reads (this implies essentially no singletons/orphans in output)."
else
  delta=$((m2_total - m1_total))
  if [[ "$delta" -gt 0 ]]; then
    echo "EXPECTED (usually): Method 2 has $delta more reads than Method 1 because Method 2 includes singletons/orphans that Method 1 discards (-0/-s -> /dev/null)."
  else
    echo "UNEXPECTED: Method 2 has fewer reads than Method 1 (delta=$delta). Investigate."
  fi
fi

echo
echo "Outputs are in: $WORKDIR"

