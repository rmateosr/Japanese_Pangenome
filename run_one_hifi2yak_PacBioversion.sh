#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# HiFi-based yak QV workflow
#
# Inputs:
#   MANIFEST (TSV with header): must contain columns:
#     ID  fasta1_path  fasta2_path  HiFi_paths
#   TASK_ID: sample ID (matches ID column)
#
# HiFi_paths:
#   - one or more gzipped FASTQ paths
#   - if multiple, they are comma-separated in the cell
#
# Output:
#   COUNTS_DIR_HiFibased/<ID>_counts.pb.yak
#   QV_DIR_HiFibased/<ID>_hap1_QV_Yak.txt
#   QV_DIR_HiFibased/<ID>_hap2_QV_Yak.txt
#
# Cleanup:
#   - concatenated HiFi temp FASTQ removed after yak count completes
#   - counts removed after both QV files exist unless KEEP_COUNTS=1
###############################################################################

MANIFEST="${1:?manifest path required}"
TASK_ID="${2:?task id (ID column) required}"

# ----------------------------
# Config (override via env)
# ----------------------------
YAK_BIN="${YAK_BIN:-/lustre10/home/raulnmateos/Japanese_Pangenome/tools/yak/yak/yak}"

COUNTS_DIR="${COUNTS_DIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/counts_sample_1000G_HiFibased}"
QV_DIR="${QV_DIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/qv_sample_counts_sample_1000G_HiFibased}"

# Temporary FASTQs: prefer node-local scratch if available; otherwise fallback
FASTQ_TMP_DIR="${FASTQ_TMP_DIR:-${SLURM_TMPDIR:-/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/samples_fastq}}"

CPUS="${SLURM_CPUS_PER_TASK:-16}"
YAK_THREADS="${YAK_THREADS:-$CPUS}"

# yak qv parameters (keep your existing defaults)
KMER_K="${KMER_K:-3.2g}"
LIM_L="${LIM_L:-100k}"

# Keep counts for debugging: export KEEP_COUNTS=1
KEEP_COUNTS="${KEEP_COUNTS:-0}"

DEBUG="${DEBUG:-0}"
if [[ "$DEBUG" == "1" ]]; then
  set -x
fi

mkdir -p "$COUNTS_DIR" "$QV_DIR" "$FASTQ_TMP_DIR"

# ----------------------------
# Read manifest row for TASK_ID
# Requires header with columns: ID, fasta1_path, fasta2_path, HiFi_paths
# ----------------------------
row="$(
  awk -v id="$TASK_ID" 'BEGIN{FS=OFS="\t"}
    NR==1{
      for(i=1;i<=NF;i++) col[$i]=i
      # Hard fail early if needed columns missing
      if(!("ID" in col) || !("fasta1_path" in col) || !("fasta2_path" in col) || !("HiFi_paths" in col)){
        print "ERROR: manifest header must contain: ID, fasta1_path, fasta2_path, HiFi_paths" > "/dev/stderr"
        exit 20
      }
      next
    }
    $col["ID"]==id{
      print $col["ID"], $col["fasta1_path"], $col["fasta2_path"], $col["HiFi_paths"]
      exit
    }' "$MANIFEST"
)"

if [[ -z "$row" ]]; then
  echo "ERROR: ID '$TASK_ID' not found in manifest: $MANIFEST" >&2
  exit 2
fi

IFS=$'\t' read -r ID fasta1_path fasta2_path hifi_paths <<< "$row"

if [[ -z "${ID:-}" ]]; then
  echo "ERROR: malformed row for ID=$TASK_ID (empty ID)" >&2
  exit 3
fi

# HiFi paths required
if [[ -z "${hifi_paths:-}" || "$hifi_paths" == "NA" || "$hifi_paths" == "na" || "$hifi_paths" == "." ]]; then
  echo "INFO: ID=$ID has HiFi_paths=NA (or empty). Skipping."
  exit 0
fi

# Assemblies required
for asm in "$fasta1_path" "$fasta2_path"; do
  if [[ -z "${asm:-}" || "$asm" == "NA" || "$asm" == "." ]]; then
    echo "ERROR: assembly path missing/NA for ID=$ID" >&2
    exit 4
  fi
  if [[ ! -f "$asm" ]]; then
    echo "ERROR: assembly not found for ID=$ID: $asm" >&2
    exit 5
  fi
done

# Tool check
if [[ ! -x "$YAK_BIN" ]]; then
  echo "ERROR: yak binary not executable: $YAK_BIN" >&2
  exit 9
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
# HiFi is single-end: yak count gets ONE FASTQ input (concatenated if needed).
# Paired-end would use two inputs/streams; we are not doing that here. :contentReference[oaicite:2]{index=2}
# ----------------------------
if [[ -s "$yak_out" ]]; then
  echo "INFO: ID=$ID yak counts exist, reusing: $yak_out"
else
  # Build a concatenated gz FASTQ if multiple HiFi files exist.
  # Note: concatenating .gz by `cat` produces a valid concatenated gzip stream.
  tmp_hifi="$(mktemp --tmpdir="$FASTQ_TMP_DIR" "${ID}.HiFi.concat.XXXXXX.fastq.gz")"
  tmp_yak="$(mktemp --tmpdir="$COUNTS_DIR" "${ID}_counts.XXXXXX.pb.yak")"

  cleanup_counts() {
    rm -f "$tmp_hifi" 2>/dev/null || true
    rm -f "$tmp_yak" 2>/dev/null || true
  }
  trap cleanup_counts EXIT

  # Split comma-separated HiFi paths into an array
  IFS=',' read -r -a HIFI_ARR <<< "$hifi_paths"

  # Validate inputs exist
  if [[ "${#HIFI_ARR[@]}" -lt 1 ]]; then
    echo "ERROR: could not parse HiFi_paths for ID=$ID: $hifi_paths" >&2
    exit 21
  fi

  for f in "${HIFI_ARR[@]}"; do
    # Trim possible whitespace
    f="${f#"${f%%[![:space:]]*}"}"
    f="${f%"${f##*[![:space:]]}"}"

    if [[ ! -f "$f" ]]; then
      echo "ERROR: HiFi FASTQ not found for ID=$ID: $f" >&2
      exit 22
    fi
  done

  echo "ID=$ID"
  echo "HiFi_paths=$hifi_paths"
  echo "COUNTS_DIR=$COUNTS_DIR"
  echo "FASTQ_TMP_DIR=$FASTQ_TMP_DIR"
  echo "CPUS=$CPUS (yak=$YAK_THREADS)"
  echo "Concatenating HiFi FASTQs -> $tmp_hifi"

  # Concatenate (compressed) FASTQs
  cat "${HIFI_ARR[@]}" > "$tmp_hifi"

  # Validate gzip stream
  gzip -t "$tmp_hifi"

  echo "Running yak count (HiFi single-end)..."
  "$YAK_BIN" count -b37 -t"$YAK_THREADS" -o "$tmp_yak" "$tmp_hifi"

  if [[ ! -s "$tmp_yak" ]]; then
    echo "ERROR: yak count output is empty for ID=$ID" >&2
    exit 10
  fi

  mv -f "$tmp_yak" "$yak_out"
  echo "DONE (yak count): $yak_out"

  # Remove concatenated HiFi after successful count
  rm -f "$tmp_hifi"
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

echo "DONE: ID=$ID (HiFi-based yak count + yak qv + cleanup)"
