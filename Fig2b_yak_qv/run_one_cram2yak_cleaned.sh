#!/usr/bin/env bash
set -euo pipefail

MANIFEST="${1:?manifest path required}"
TASK_ID="${2:?task id (ID column) required}"

# ----------------------------
# Config (override via env)
# ----------------------------
REF="${REF:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/References/GRCh38_full_analysis_set_plus_decoy_hla.fa}"
YAK_BIN="${YAK_BIN:-/lustre10/home/raulnmateos/Japanese_Pangenome/tools/yak/yak/yak}"

# Base output dirs (we will suffix them with a RUN_TAG)
COUNTS_DIR="${COUNTS_DIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/counts_sample_1000G}"
QV_DIR="${QV_DIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/qv_sample_counts_sample_1000G}"

# Temporary FASTQs: prefer node-local scratch if available; otherwise fall back.
FASTQ_TMP_DIR="${FASTQ_TMP_DIR:-${SLURM_TMPDIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/samples_fastq}}"

CPUS="${SLURM_CPUS_PER_TASK:-16}"
SAMTOOLS_THREADS="${SAMTOOLS_THREADS:-$CPUS}"
YAK_THREADS="${YAK_THREADS:-$CPUS}"

# yak qv parameters
KMER_K="${KMER_K:-3.2g}"
LIM_L="${LIM_L:-100k}"          # yak qv -l (0 means disable the -l filter)

# Contig length filter threshold in bp (0 = keep all; no filtering).
# If >0, we will create a filtered copy of the assembly for yak qv (original untouched).
CONTIG_MIN_BP="${CONTIG_MIN_BP:-0}"

# If you ever want to keep counts for debugging: export KEEP_COUNTS=1
KEEP_COUNTS="${KEEP_COUNTS:-0}"

# NEW: remove staged intermediates so QV_DIR ends with only QV txt files
CLEAN_STAGE="${CLEAN_STAGE:-1}"  # 1 = delete staged FASTA/index/locks; 0 = keep for debugging

DEBUG="${DEBUG:-0}"
if [[ "$DEBUG" == "1" ]]; then
  set -x
fi

# ----------------------------
# Derive RUN_TAG and tag output folders
# ----------------------------
if [[ -z "${RUN_TAG:-}" ]]; then
  if [[ "$CONTIG_MIN_BP" -eq 0 ]]; then
    contig_tag="0removal"
  elif [[ "$CONTIG_MIN_BP" -eq 100000 ]]; then
    contig_tag="100kremoval"
  else
    contig_tag="${CONTIG_MIN_BP}bpremoval"
  fi

  RUN_TAG="${contig_tag}_l${LIM_L}_collated"
fi

TAG="_${RUN_TAG}"
COUNTS_DIR="${COUNTS_DIR%/}${TAG}"
QV_DIR="${QV_DIR%/}${TAG}"

mkdir -p "$COUNTS_DIR" "$QV_DIR" "$FASTQ_TMP_DIR"

# ----------------------------
# NEW: Stage directory for intermediates
# IMPORTANT: must be per-task to avoid races in Slurm arrays.
# ----------------------------
STAGE_ROOT="${QV_DIR}/.stage"
STAGE_DIR="${STAGE_ROOT}/${TASK_ID}"
mkdir -p "$STAGE_DIR"

# ----------------------------
# Concurrency guard (per sample, per RUN_TAG)
# IMPORTANT: locks must NOT live under STAGE_DIR, because STAGE_DIR is deleted per task.
# ----------------------------
if ! command -v flock >/dev/null 2>&1; then
  echo "ERROR: flock not found in PATH. Load util-linux (or equivalent) to use locking." >&2
  exit 14
fi

lockdir="${QV_DIR}/.locks"
mkdir -p "$lockdir"
lock="${lockdir}/${TASK_ID}.lock"

exec 9>"$lock"
flock -n 9 || { echo "INFO: $TASK_ID already running for RUN_TAG=$RUN_TAG; exiting."; exit 0; }

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

  # If there is any leftover stage dir from a prior run, remove it (optional)
  if [[ "$CLEAN_STAGE" == "1" && -d "$STAGE_DIR" ]]; then
    echo "CLEAN: removing staged intermediates: $STAGE_DIR"
    rm -rf "$STAGE_DIR"
  fi

  exit 0
fi

# ----------------------------
# Step A: Ensure yak counts exist (create if missing)
#
# MODIFIED: do NOT split into R1/R2.
# We generate a single FASTQ stream from the CRAM and then run yak count on that.
# ----------------------------
if [[ -s "$yak_out" ]]; then
  echo "INFO: ID=$ID yak counts exist, reusing: $yak_out"
else
  tmp_fq="$(mktemp --tmpdir="$FASTQ_TMP_DIR" "${ID}.XXXXXX.fastq.gz")"
  tmp_yak="$(mktemp --tmpdir="$COUNTS_DIR" "${ID}_counts.XXXXXX.pb.yak")"

  cleanup_counts() {
    rm -f "$tmp_fq"
    rm -f "$tmp_yak" 2>/dev/null || true
  }
  trap cleanup_counts EXIT

  echo "ID=$ID"
  echo "CRAM(S3)=$cram_uri"
  echo "REF=$REF"
  echo "RUN_TAG=$RUN_TAG"
  echo "COUNTS_DIR=$COUNTS_DIR"
  echo "QV_DIR=$QV_DIR"
  echo "STAGE_DIR=$STAGE_DIR"
  echo "FASTQ_TMP_DIR=$FASTQ_TMP_DIR"
  echo "CPUS=$CPUS (samtools=$SAMTOOLS_THREADS yak=$YAK_THREADS)"
  echo "CONTIG_MIN_BP=$CONTIG_MIN_BP (0 means keep all contigs)"
  echo "yak qv -l=$LIM_L"

  echo "Running samtools fastq (single FASTQ stream, no R1/R2 split)..."
  samtools fastq -@ "$SAMTOOLS_THREADS" --reference "$REF" "$cram_uri" \
    | gzip -c > "$tmp_fq"
  gzip -t "$tmp_fq"

  echo "Running yak count on single FASTQ..."
  "$YAK_BIN" count -b37 -t"$YAK_THREADS" -o "$tmp_yak" "$tmp_fq"

  if [[ ! -s "$tmp_yak" ]]; then
    echo "ERROR: yak count output is empty for ID=$ID" >&2
    exit 10
  fi

  mv -f "$tmp_yak" "$yak_out"
  echo "DONE (yak count): $yak_out"

  rm -f "$tmp_fq"
  trap - EXIT
fi

# ----------------------------
# Step B0: Stage assemblies for yak qv and ensure faidx works
# All staged FASTA and indices go under STAGE_DIR.
# ----------------------------
stage_fasta_for_faidx() {
  local in_fa="$1"
  local tag="$2"   # hap1_src / hap2_src
  local staged err

  if [[ "$in_fa" == *.gz ]]; then
    staged="${STAGE_DIR}/${ID}_${tag}.fa.gz"

    # Symlink first, try faidx on the staged path
    ln -sf "$in_fa" "$staged"
    rm -f "${staged}.fai" "${staged}.gzi" 2>/dev/null || true

    err="$(mktemp --tmpdir="$STAGE_DIR" "${ID}_${tag}.faidx_err.XXXXXX.txt")"
    if ! samtools faidx "$staged" 2> "$err"; then
      # If it is plain gzip, convert to BGZF into the staged file and retry
      if grep -q "Cannot index files compressed with gzip" "$err"; then
        rm -f "$staged" "${staged}.fai" "${staged}.gzi"
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
    staged="${STAGE_DIR}/${ID}_${tag}.fa"
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

# If CONTIG_MIN_BP > 0, write filtered copy under STAGE_DIR and return its path.
maybe_filter_contigs_minlen() {
  local staged="$1"
  local tag="$2"
  local minbp="$3"
  local out_fa

  if [[ "$minbp" -le 0 ]]; then
    echo "$staged"
    return 0
  fi

  out_fa="${STAGE_DIR}/${ID}_${tag}.min${minbp}.fa.gz"
  if [[ -s "$out_fa" && -s "${out_fa}.fai" ]]; then
    echo "$out_fa"
    return 0
  fi

  echo "Filtering contigs for ID=$ID $tag: keep length >= ${minbp} bp (writing copy, original untouched)"
  rm -f "$out_fa" "${out_fa}.fai" "${out_fa}.gzi" 2>/dev/null || true

  if [[ "$staged" == *.gz ]]; then
    gzip -dc "$staged"
  else
    cat "$staged"
  fi | awk -v min="$minbp" '
    BEGIN { RS=">"; ORS=""; }
    NR==1 { next; }
    {
      n = index($0, "\n");
      if (n == 0) next;
      hdr = substr($0, 1, n-1);
      seq = substr($0, n+1);
      gsub(/\n/, "", seq);
      if (length(seq) >= min) {
        print ">" hdr "\n";
        for (i=1; i<=length(seq); i+=60) {
          print substr(seq, i, 60) "\n";
        }
      }
    }
  ' | bgzip -c > "$out_fa"

  samtools faidx "$out_fa"

  if [[ ! -s "$out_fa" || ! -s "${out_fa}.fai" ]]; then
    echo "ERROR: contig-filtered FASTA or index missing for ID=$ID $tag: $out_fa" >&2
    return 1
  fi

  echo "$out_fa"
}

staged_hap1="$(stage_fasta_for_faidx "$fasta1_path" "hap1_src")"
staged_hap2="$(stage_fasta_for_faidx "$fasta2_path" "hap2_src")"

qv_hap1="$(maybe_filter_contigs_minlen "$staged_hap1" "hap1_qv" "$CONTIG_MIN_BP")"
qv_hap2="$(maybe_filter_contigs_minlen "$staged_hap2" "hap2_qv" "$CONTIG_MIN_BP")"

# ----------------------------
# Step B: yak qv for hap1 and hap2
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

  echo "Running yak qv for ID=$ID $hap (CONTIG_MIN_BP=${CONTIG_MIN_BP}; -l ${LIM_L})"
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

run_qv "hap1" "$qv_hap1" "$out_hap1"
run_qv "hap2" "$qv_hap2" "$out_hap2"

# ----------------------------
# Step C: cleanup after QV complete
# ----------------------------
if [[ -s "$out_hap1" && -s "$out_hap2" ]]; then
  # Remove yak counts unless told otherwise
  if [[ "$KEEP_COUNTS" != "1" ]]; then
    echo "CLEAN: QV complete, removing yak counts: $yak_out"
    rm -f "$yak_out"
  else
    echo "INFO: KEEP_COUNTS=1, not deleting: $yak_out"
  fi

  # NEW: remove staged FASTA/index files and locks so QV_DIR only has QV txt
  if [[ "$CLEAN_STAGE" == "1" ]]; then
    echo "CLEAN: removing staged intermediates: $STAGE_DIR"
    rm -rf "$STAGE_DIR"
  else
    echo "INFO: CLEAN_STAGE=0, keeping staged intermediates in: $STAGE_DIR"
  fi
else
  echo "ERROR: QV outputs missing/empty after run for ID=$ID; not deleting counts or stage." >&2
  exit 11
fi

echo "DONE: ID=$ID (yak count + yak qv (CONTIG_MIN_BP=${CONTIG_MIN_BP}, -l ${LIM_L}) + cleanup) RUN_TAG=$RUN_TAG"
