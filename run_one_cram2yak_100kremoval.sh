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

# Temporary FASTQs: prefer node-local scratch if available; otherwise fall back.
FASTQ_TMP_DIR="${FASTQ_TMP_DIR:-${SLURM_TMPDIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/samples_fastq}}"

CPUS="${SLURM_CPUS_PER_TASK:-16}"
SAMTOOLS_THREADS="${SAMTOOLS_THREADS:-$CPUS}"
YAK_THREADS="${YAK_THREADS:-$CPUS}"

# yak qv parameters
KMER_K="${KMER_K:-3.2g}"
LIM_L="${LIM_L:-0}"     # default 0

# Contig length filter threshold
CONTIG_MIN_BP="${CONTIG_MIN_BP:-100000}"  # 100k

# If you ever want to keep counts for debugging: export KEEP_COUNTS=1
KEEP_COUNTS="${KEEP_COUNTS:-0}"

DEBUG="${DEBUG:-0}"
if [[ "$DEBUG" == "1" ]]; then
  set -x
fi

# ----------------------------
# Tag output folders with _100kremoval
# ----------------------------
COUNTS_DIR="${COUNTS_DIR%/}_100kremoval"
QV_DIR="${QV_DIR%/}_100kremoval"

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

# CRAM may be NA -> skip
if [[ -z "${cram_uri:-}" || "$cram_uri" == "NA" || "$cram_uri" == "na" || "$cram_uri" == "." ]]; then
  echo "INFO: ID=$ID has cram=NA (or empty). Skipping."
  exit 0
fi

# Assemblies required
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
if ! command -v samtools >/dev/null 2>&1; then
  echo "ERROR: samtools not found in PATH" >&2
  exit 12
fi
if ! command -v bgzip >/dev/null 2>&1; then
  echo "ERROR: bgzip not found in PATH (required for BGZF FASTA handling)" >&2
  exit 13
fi

yak_out="${COUNTS_DIR}/${ID}_counts.pb.yak"
out_hap1="${QV_DIR}/${ID}_hap1_QV_Yak.txt"
out_hap2="${QV_DIR}/${ID}_hap2_QV_Yak.txt"

# If both QV outputs already exist, consider complete; delete counts if present (unless KEEP_COUNTS=1)
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

  rm -f "$tmp1" "$tmp2"
  trap - EXIT
fi

# ----------------------------
# Step B0: Filter assemblies to contigs >= CONTIG_MIN_BP for yak qv
#
# Key fix:
#   - samtools faidx cannot index plain gzip FASTA; it needs uncompressed or BGZF.
# Strategy:
#   - stage the assembly into QV_DIR (symlink first)
#   - attempt samtools faidx; if it errors with "Cannot index files compressed with gzip",
#     convert gzip -> BGZF in QV_DIR and retry
#   - then use the .fai to list contigs >= threshold and produce filtered BGZF FASTA
# ----------------------------

# Stage an assembly into QV_DIR so we can create .fai locally.
# Echoes staged path.
stage_fasta_for_faidx() {
  local in_fa="$1"
  local tag="$2"   # hap1_src / hap2_src
  local staged err

  if [[ "$in_fa" == *.gz ]]; then
    staged="${QV_DIR}/${ID}_${tag}.fa.gz"

    # Fast path: symlink, then test faidx on the staged file.
    ln -sf "$in_fa" "$staged"
    rm -f "${staged}.fai"

    err="$(mktemp --tmpdir="$QV_DIR" "${ID}_${tag}.faidx_err.XXXXXX.txt")"
    if ! samtools faidx "$staged" 2> "$err"; then
      # If the failure is because it's plain gzip, convert to BGZF and retry.
      if grep -q "Cannot index files compressed with gzip" "$err"; then
        rm -f "$staged" "${staged}.fai"
        gzip -dc "$in_fa" | bgzip -c > "$staged"
        rm -f "$err"
        samtools faidx "$staged"
      else
        cat "$err" >&2
        rm -f "$err"
        echo "ERROR: samtools faidx failed for staged file: $staged" >&2
        return 1
      fi
    fi
    rm -f "$err" 2>/dev/null || true

  else
    staged="${QV_DIR}/${ID}_${tag}.fa"
    ln -sf "$in_fa" "$staged"
    if [[ ! -s "${staged}.fai" ]]; then
      samtools faidx "$staged"
    fi
  fi

  if [[ ! -s "${staged}.fai" ]]; then
    echo "ERROR: missing fai after staging: ${staged}.fai" >&2
    return 1
  fi

  echo "$staged"
}

filter_assembly_ge_minbp_to_gz() {
  local in_fa="$1"
  local out_gz="$2"
  local tag="$3"

  local staged_in
  staged_in="$(stage_fasta_for_faidx "$in_fa" "$tag")"

  local keep_list
  keep_list="$(mktemp --tmpdir="$QV_DIR" "${ID}_keepcontigs.XXXXXX.txt")"
  awk -v minbp="$CONTIG_MIN_BP" 'BEGIN{FS=OFS="\t"} $2>=minbp{print $1}' "${staged_in}.fai" > "$keep_list"

  if [[ ! -s "$keep_list" ]]; then
    rm -f "$keep_list"
    echo "ERROR: no contigs >= ${CONTIG_MIN_BP} bp in assembly: $in_fa" >&2
    return 1
  fi

  # Fetch kept contigs and write BGZF-compressed FASTA
  # shellcheck disable=SC2046
  if ! samtools faidx "$staged_in" $(cat "$keep_list") | bgzip -c > "$out_gz"; then
    rm -f "$keep_list" "$out_gz"
    echo "ERROR: failed to create filtered assembly: $out_gz" >&2
    return 1
  fi
  rm -f "$keep_list"

  # Index filtered FASTA (it is BGZF, so faidx works)
  if [[ ! -s "${out_gz}.fai" ]]; then
    samtools faidx "$out_gz"
  fi
}

tmp_hap1_100k="${QV_DIR}/${ID}_hap1.contigs_ge${CONTIG_MIN_BP}.fa.gz"
tmp_hap2_100k="${QV_DIR}/${ID}_hap2.contigs_ge${CONTIG_MIN_BP}.fa.gz"

if [[ ! -s "$tmp_hap1_100k" ]]; then
  echo "Filtering hap1 contigs < ${CONTIG_MIN_BP} bp (temporary, originals untouched)..."
  filter_assembly_ge_minbp_to_gz "$fasta1_path" "$tmp_hap1_100k" "hap1_src"
else
  echo "INFO: filtered hap1 exists, reusing: $tmp_hap1_100k"
fi

if [[ ! -s "$tmp_hap2_100k" ]]; then
  echo "Filtering hap2 contigs < ${CONTIG_MIN_BP} bp (temporary, originals untouched)..."
  filter_assembly_ge_minbp_to_gz "$fasta2_path" "$tmp_hap2_100k" "hap2_src"
else
  echo "INFO: filtered hap2 exists, reusing: $tmp_hap2_100k"
fi

# ----------------------------
# Step B: yak qv for hap1 and hap2 (use filtered assemblies)
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

  echo "Running yak qv for ID=$ID $hap (assembly filtered >=${CONTIG_MIN_BP} bp)"
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

run_qv "hap1" "$tmp_hap1_100k" "$out_hap1"
run_qv "hap2" "$tmp_hap2_100k" "$out_hap2"

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

echo "DONE: ID=$ID (yak count + filtered-assembly yak qv + cleanup)"
