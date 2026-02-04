#!/usr/bin/env bash
set -euo pipefail

# ----------------------------
# Defaults
# ----------------------------
FIG2="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2"
SCRIPTS="${FIG2}/scripts"
LOG_DIR="${FIG2}/log"

MANIFEST="${FIG2}/required_data/Manifest/hap_manifest.tsv"
ARRAY_RANGE=""                     # if empty, compute from manifest
LIM_L="100k"                        # yak qv -l
CONTIG_MIN_BP="0"                   # contig length filter threshold
CPUS="16"
MEM="64G"
TIME="12:00:00"

COUNTS_BASE="${FIG2}/required_data/counts_sample_1000G"
QV_BASE="${FIG2}/required_data/qv_sample_counts_sample_1000G"

SBATCH_SCRIPT="${SCRIPTS}/array_cram2yak_cleaned.sbatch"   # generic sbatch

usage() {
  cat <<EOF
Usage:
  $0 [--manifest PATH] [--array 1-N] [--lim-l 0|100k|...] [--contig-min-bp 0|100000|100k|...]
     [--cpus N] [--mem 64G] [--time HH:MM:SS]

Examples:
  $0 --lim-l 100k --contig-min-bp 0
  $0 --lim-l 0 --contig-min-bp 0 --array 1-5
  $0 --lim-l 100k --contig-min-bp 100k
EOF
}

# ----------------------------
# Parse args
# ----------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest)        MANIFEST="$2"; shift 2;;
    --array)           ARRAY_RANGE="$2"; shift 2;;
    --lim-l)           LIM_L="$2"; shift 2;;
    --contig-min-bp)   CONTIG_MIN_BP="$2"; shift 2;;
    --cpus)            CPUS="$2"; shift 2;;
    --mem)             MEM="$2"; shift 2;;
    --time)            TIME="$2"; shift 2;;
    -h|--help)         usage; exit 0;;
    *) echo "ERROR: unknown arg: $1" >&2; usage; exit 2;;
  esac
done

mkdir -p "$LOG_DIR"

if [[ ! -s "$MANIFEST" ]]; then
  echo "ERROR: manifest not found/empty: $MANIFEST" >&2
  exit 3
fi
if [[ ! -s "$SBATCH_SCRIPT" ]]; then
  echo "ERROR: sbatch script not found/empty: $SBATCH_SCRIPT" >&2
  exit 4
fi

# ----------------------------
# Normalize contig min for tagging (accept 100k, 5m, etc.)
# (Only for TAG + filtering logic; yak -l keeps original LIM_L string.)
# ----------------------------
norm_bp() {
  local x="$1"
  if [[ "$x" =~ ^[0-9]+$ ]]; then
    echo "$x"; return 0
  fi
  if [[ "$x" =~ ^([0-9]+)([kKmMgG])$ ]]; then
    local n="${BASH_REMATCH[1]}"
    local u="${BASH_REMATCH[2]}"
    case "$u" in
      k|K) echo $((n*1000));;
      m|M) echo $((n*1000000));;
      g|G) echo $((n*1000000000));;
    esac
    return 0
  fi
  echo "ERROR: invalid bp value: $x (use 0, 100000, 100k, 5m, ...)" >&2
  exit 5
}

CONTIG_MIN_BP_NORM="$(norm_bp "$CONTIG_MIN_BP")"

contig_tag() {
  local bp="$1"
  if [[ "$bp" -eq 0 ]]; then
    echo "0removal"
  elif [[ "$bp" -eq 100000 ]]; then
    echo "100kremoval"
  else
    echo "${bp}bpremoval"
  fi
}

l_tag() {
  local l="$1"
  # LIM_L is passed to yak as-is (yak accepts 100k style). Tag just prefixes with 'l'.
  echo "l${l}"
}

RUN_TAG="$(contig_tag "$CONTIG_MIN_BP_NORM")_$(l_tag "$LIM_L")_cleaned"

# ----------------------------
# Array range default: 1-(lines-1)
# ----------------------------
if [[ -z "$ARRAY_RANGE" ]]; then
  n=$(($(wc -l < "$MANIFEST") - 1))
  if [[ "$n" -le 0 ]]; then
    echo "ERROR: manifest has no data rows: $MANIFEST" >&2
    exit 6
  fi
  ARRAY_RANGE="1-${n}"
fi

JOB_NAME="cram2yak_${RUN_TAG}"

echo "Submitting:"
echo "  JOB_NAME=$JOB_NAME"
echo "  MANIFEST=$MANIFEST"
echo "  ARRAY=$ARRAY_RANGE"
echo "  LIM_L=$LIM_L"
echo "  CONTIG_MIN_BP=$CONTIG_MIN_BP (norm=$CONTIG_MIN_BP_NORM)"
echo "  COUNTS_BASE=$COUNTS_BASE"
echo "  QV_BASE=$QV_BASE"
echo

sbatch \
  -J "$JOB_NAME" \
  --cpus-per-task="$CPUS" \
  --mem="$MEM" \
  --time="$TIME" \
  --array="$ARRAY_RANGE" \
  --output="${LOG_DIR}/%x_%A_%a.out" \
  --error="${LOG_DIR}/%x_%A_%a.err" \
  --export=ALL,MANIFEST="$MANIFEST",LIM_L="$LIM_L",CONTIG_MIN_BP="$CONTIG_MIN_BP_NORM",RUN_TAG="$RUN_TAG",COUNTS_BASE="$COUNTS_BASE",QV_BASE="$QV_BASE" \
  "$SBATCH_SCRIPT"
