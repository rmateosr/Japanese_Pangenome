#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# Remove mitochondria-related contigs from hap assemblies using HiFi reads
#
# Inputs:
#   MANIFEST: TSV with header: ID  fasta1_path  fasta2_path  cram
#   TABLE:    TSV where HiFi rows are: "HiFi <tab> sampleID <tab> /path/to/*.fastq.gz"
#
# Invocation options:
#   A) Explicit ID:
#      bash run_one_mito_removal.sh MANIFEST TABLE TASK_ID
#   B) Slurm array:
#      bash run_one_mito_removal.sh MANIFEST TABLE
#      (then SLURM_ARRAY_TASK_ID selects the Nth data row in MANIFEST)
#
# Output:
#   MITO_OUT_DIR/<ID>/<ID>.hap1.mitochondria_removed.fa.gz (+ .fai)
#   MITO_OUT_DIR/<ID>/<ID>.hap2.mitochondria_removed.fa.gz (+ .fai)
###############################################################################

MANIFEST="${1:?manifest path required}"
TABLE="${2:?TABLE path required}"
TASK_ID="${3:-}"

# ----------------------------
# Config (override via env)
# ----------------------------
MT_REF="${MT_REF:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/References/CHM13v2.0.mt.fa}"
MINIMAP2_BIN="${MINIMAP2_BIN:-minimap2}"
MT_MM2_OPTS="${MT_MM2_OPTS:--L --eqx}"

MITO_OUT_DIR="${MITO_OUT_DIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Assemblies_mitochondria_removed}"

THREADS="${THREADS:-${SLURM_CPUS_PER_TASK:-16}}"

MT_MIN_FRAC="${MT_MIN_FRAC:-0.80}"
MT_MIN_BP="${MT_MIN_BP:-5000}"
MT_MAX_CONTIG_BP="${MT_MAX_CONTIG_BP:-200000}"

DEBUG="${DEBUG:-0}"
KEEP_TMP="${KEEP_TMP:-0}"

if [[ "$DEBUG" == "1" ]]; then
  set -x
fi

# ----------------------------
# Tool checks
# ----------------------------
command -v awk >/dev/null 2>&1 || { echo "ERROR: awk not found" >&2; exit 2; }
command -v samtools >/dev/null 2>&1 || { echo "ERROR: samtools not found in PATH" >&2; exit 2; }
command -v bgzip >/dev/null 2>&1 || { echo "ERROR: bgzip not found in PATH (htslib)" >&2; exit 2; }
command -v gzip >/dev/null 2>&1 || { echo "ERROR: gzip not found in PATH" >&2; exit 2; }
command -v "$MINIMAP2_BIN" >/dev/null 2>&1 || { echo "ERROR: minimap2 not found (MINIMAP2_BIN=$MINIMAP2_BIN)" >&2; exit 2; }

[[ -r "$MT_REF" ]] || { echo "ERROR: MT_REF not readable: $MT_REF" >&2; exit 2; }
[[ -r "$MANIFEST" ]] || { echo "ERROR: MANIFEST not readable: $MANIFEST" >&2; exit 2; }
[[ -r "$TABLE" ]] || { echo "ERROR: TABLE not readable: $TABLE" >&2; exit 2; }

mkdir -p "$MITO_OUT_DIR"

# ----------------------------
# Resolve TASK_ID
# If not provided, use SLURM_ARRAY_TASK_ID to select manifest row.
# ----------------------------
if [[ -z "$TASK_ID" ]]; then
  if [[ -z "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    echo "ERROR: Provide TASK_ID as arg3 or run under Slurm array (SLURM_ARRAY_TASK_ID set)." >&2
    exit 3
  fi

  # Pick Nth data row: SLURM_ARRAY_TASK_ID=1 => first non-header line
  TASK_ID="$(
    awk -v n="$SLURM_ARRAY_TASK_ID" 'BEGIN{FS="\t"} NR==1{next} {i++; if(i==n){print $1; exit}}' "$MANIFEST"
  )"

  if [[ -z "$TASK_ID" ]]; then
    echo "ERROR: SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID out of range for manifest: $MANIFEST" >&2
    exit 3
  fi
fi

# ----------------------------
# WORKDIR (unique per task)
# ----------------------------
BASE_TMP="${WORKDIR:-}"
if [[ -z "$BASE_TMP" ]]; then
  if [[ -n "${SLURM_TMPDIR:-}" ]]; then
    BASE_TMP="$SLURM_TMPDIR"
  else
    BASE_TMP="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/tmp_mito_rm_work"
  fi
fi

JOBTAG="${SLURM_JOB_ID:-nojob}"
TASKTAG="${SLURM_ARRAY_TASK_ID:-notask}"
WORKDIR="${BASE_TMP%/}/mito_rm_${JOBTAG}_${TASKTAG}_${TASK_ID}"
mkdir -p "$WORKDIR"

# ----------------------------
# Read manifest row for TASK_ID
# ----------------------------
row="$(
  awk -v id="$TASK_ID" 'BEGIN{FS=OFS="\t"} NR==1{next} $1==id{print; exit}' "$MANIFEST"
)"
if [[ -z "$row" ]]; then
  echo "ERROR: ID '$TASK_ID' not found in manifest: $MANIFEST" >&2
  exit 3
fi

IFS=$'\t' read -r ID FASTA1 FASTA2 _CRAM <<< "$row"

if [[ -z "${ID:-}" || "$ID" == "ID" ]]; then
  echo "ERROR: Parsed invalid ID from manifest row: $row" >&2
  exit 3
fi

for f in "$FASTA1" "$FASTA2"; do
  [[ -r "$f" ]] || { echo "ERROR: assembly not readable for $ID: $f" >&2; exit 4; }
done

# ----------------------------
# Collect HiFi fastq.gz for this sample
# TABLE format: HiFi <tab> sampleID <tab> path
# ----------------------------
mapfile -t HIFI_PATHS < <(awk -F $'\t' -v id="$ID" '$1=="HiFi" && $2==id {print $3}' "$TABLE")
if (( ${#HIFI_PATHS[@]} == 0 )); then
  echo "ERROR: No HiFi rows found for sample=$ID in TABLE=$TABLE" >&2
  exit 5
fi
for p in "${HIFI_PATHS[@]}"; do
  [[ -r "$p" ]] || { echo "ERROR: missing/unreadable HiFi FASTQ for $ID: $p" >&2; exit 5; }
done

HIFI_CAT="${WORKDIR}/${ID}.HiFi.concat.fastq.gz"
HIFI_CAT_TMP="${HIFI_CAT}.tmp"

# ----------------------------
# Cleanup policy
# ----------------------------
cleanup() {
  if [[ "$KEEP_TMP" == "1" ]]; then
    echo "INFO: [$ID] KEEP_TMP=1, not deleting WORKDIR: $WORKDIR"
    return 0
  fi
  rm -f "$HIFI_CAT_TMP" "$HIFI_CAT" \
        "${WORKDIR}/${ID}.mt.bam" \
        "${WORKDIR}/${ID}.mt.fastq.gz" \
        "${WORKDIR}/${ID}.hap1.mt_to_asm.paf" \
        "${WORKDIR}/${ID}.hap2.mt_to_asm.paf" \
        "${WORKDIR}/${ID}.hap1.remove.list" \
        "${WORKDIR}/${ID}.hap2.remove.list" \
        "${WORKDIR}/${ID}.hap1.keep.list" \
        "${WORKDIR}/${ID}.hap2.keep.list" \
        "${WORKDIR}/${ID}.hap1.asm.bgz" \
        "${WORKDIR}/${ID}.hap2.asm.bgz" \
        "${WORKDIR}/${ID}.hap1.asm.bgz.fai" \
        "${WORKDIR}/${ID}.hap2.asm.bgz.fai"
  rmdir "$WORKDIR" 2>/dev/null || true
}
trap cleanup EXIT

echo "INFO: [$ID] WORKDIR=$WORKDIR"
echo "INFO: [$ID] Concatenating ${#HIFI_PATHS[@]} HiFi FASTQ(s) -> $HIFI_CAT"
cat "${HIFI_PATHS[@]}" > "$HIFI_CAT_TMP"
mv -f "$HIFI_CAT_TMP" "$HIFI_CAT"
gzip -t "$HIFI_CAT"

# ----------------------------
# Step 1: Identify mitochondrial reads
# ----------------------------
MT_BAM="${WORKDIR}/${ID}.mt.bam"
MT_FQ="${WORKDIR}/${ID}.mt.fastq.gz"

echo "INFO: [$ID] Mapping HiFi -> MT_REF to identify mitochondrial reads"
echo "INFO: minimap2 opts: ${MT_MM2_OPTS} preset: -ax map-hifi"
"$MINIMAP2_BIN" ${MT_MM2_OPTS} -t "$THREADS" -ax map-hifi "$MT_REF" "$HIFI_CAT" \
  | samtools view -@ "$THREADS" -b -F 4 -o "$MT_BAM" -

echo "INFO: [$ID] Extracting mitochondrial reads to FASTQ.gz"
samtools fastq -@ "$THREADS" "$MT_BAM" | bgzip -@ "$THREADS" -c > "$MT_FQ"
gzip -t "$MT_FQ"

mt_n_reads="$(zcat "$MT_FQ" | awk 'END{print int(NR/4)}')"
echo "INFO: [$ID] MT reads extracted: $mt_n_reads"
if [[ "$mt_n_reads" -eq 0 ]]; then
  echo "WARN: [$ID] No MT reads found. No contigs will be removed."
fi

# ----------------------------
# Helper: create BGZF copy + faidx
# ----------------------------
bgzf_and_faidx() {
  local in_fa="$1"
  local out_bgz="$2"

  echo "INFO: BGZF: $in_fa -> $out_bgz"
  if [[ "$in_fa" == *.gz ]]; then
    gzip -cd "$in_fa" | bgzip -@ "$THREADS" -c > "$out_bgz"
  else
    bgzip -@ "$THREADS" -c "$in_fa" > "$out_bgz"
  fi
  samtools faidx "$out_bgz"
}

# ----------------------------
# Helper: compute contigs to remove
# ----------------------------
compute_remove_list() {
  local hap="$1"
  local asm_in="$2"
  local asm_bgz="$3"
  local paf_out="$4"
  local remove_list="$5"
  local keep_list="$6"

  bgzf_and_faidx "$asm_in" "$asm_bgz"

  if [[ "$mt_n_reads" -eq 0 ]]; then
    cut -f1 "${asm_bgz}.fai" > "$keep_list"
    : > "$remove_list"
    return 0
  fi

  echo "INFO: [$ID][$hap] Mapping MT reads -> assembly (PAF)"
  "$MINIMAP2_BIN" ${MT_MM2_OPTS} --cs -t "$THREADS" -x map-hifi "$asm_bgz" "$MT_FQ" > "$paf_out"

  awk -v FS='\t' -v minfrac="$MT_MIN_FRAC" -v minbp="$MT_MIN_BP" -v maxbp="$MT_MAX_CONTIG_BP" '
    FNR==NR { len[$1]=$2; next }
    { t=$6; aln=$11; sum[t]+=aln }
    END{
      for(t in len){
        a = (t in sum) ? sum[t] : 0
        L = len[t]
        frac = (L>0) ? a/L : 0
        if(L <= maxbp && a >= minbp && frac >= minfrac){
          print t > "'"$remove_list"'"
        } else {
          print t > "'"$keep_list"'"
        }
      }
    }
  ' "${asm_bgz}.fai" "$paf_out"

  echo "INFO: [$ID][$hap] remove_n=$(wc -l < "$remove_list" | tr -d " ") keep_n=$(wc -l < "$keep_list" | tr -d " ")"
}

# ----------------------------
# Step 2: Build mitochondria-removed assemblies
# ----------------------------
SAMPLE_OUT="${MITO_OUT_DIR}/${ID}"
mkdir -p "$SAMPLE_OUT"

H1_BGZ="${WORKDIR}/${ID}.hap1.asm.bgz"
H2_BGZ="${WORKDIR}/${ID}.hap2.asm.bgz"

H1_PAF="${WORKDIR}/${ID}.hap1.mt_to_asm.paf"
H2_PAF="${WORKDIR}/${ID}.hap2.mt_to_asm.paf"

H1_RM="${WORKDIR}/${ID}.hap1.remove.list"
H2_RM="${WORKDIR}/${ID}.hap2.remove.list"

H1_KEEP="${WORKDIR}/${ID}.hap1.keep.list"
H2_KEEP="${WORKDIR}/${ID}.hap2.keep.list"

compute_remove_list "hap1" "$FASTA1" "$H1_BGZ" "$H1_PAF" "$H1_RM" "$H1_KEEP"
compute_remove_list "hap2" "$FASTA2" "$H2_BGZ" "$H2_PAF" "$H2_RM" "$H2_KEEP"

OUT_H1="${SAMPLE_OUT}/${ID}.hap1.mitochondria_removed.fa.gz"
OUT_H2="${SAMPLE_OUT}/${ID}.hap2.mitochondria_removed.fa.gz"

echo "INFO: [$ID][hap1] Writing mitochondria_removed assembly -> $OUT_H1"
samtools faidx "$H1_BGZ" -r "$H1_KEEP" | bgzip -@ "$THREADS" -c > "$OUT_H1"
samtools faidx "$OUT_H1"

echo "INFO: [$ID][hap2] Writing mitochondria_removed assembly -> $OUT_H2"
samtools faidx "$H2_BGZ" -r "$H2_KEEP" | bgzip -@ "$THREADS" -c > "$OUT_H2"
samtools faidx "$OUT_H2"

# Summary
SUMMARY="${SAMPLE_OUT}/mito_removal.summary.tsv"
{
  echo -e "ID\tmt_reads\tMT_MIN_FRAC\tMT_MIN_BP\tMT_MAX_CONTIG_BP\thap1_removed\thap2_removed"
  echo -e "${ID}\t${mt_n_reads}\t${MT_MIN_FRAC}\t${MT_MIN_BP}\t${MT_MAX_CONTIG_BP}\t$(wc -l < "$H1_RM" | tr -d ' ')\t$(wc -l < "$H2_RM" | tr -d ' ')"
} > "$SUMMARY"

echo "DONE: [$ID] mitochondria_removed assemblies written:"
echo "  $OUT_H1"
echo "  $OUT_H2"
echo "  Summary: $SUMMARY"
