#!/usr/bin/env bash
set -euo pipefail

: "${DIR:=/rshare1/ZETTAI_path_WA_slash_home_KARA/home/rmateosr/Pangenome_Japan/HPRC_Data/JPT/assembly/zipped_concat}"
: "${SUFFIX:=_concatenated_hprc_r2_v1.0.1.fa.gz}"
: "${SBATCH_SCRIPT:=./flagger_per_sample.sbatch}"
: "${SAMPLE_LIST:=./samples.flagger.list}"
: "${DRY_RUN:=0}"

shopt -s nullglob
files=("${DIR}"/*"${SUFFIX}")
if (( ${#files[@]} == 0 )); then
  echo "ERROR: No files matching *${SUFFIX} in: ${DIR}" >&2
  exit 1
fi

# Extract samples, keep only NA\d+ (same logic as your SGE submitter)
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

for f in "${files[@]}"; do
  base="$(basename "$f")"
  sample="${base%${SUFFIX}}"
  if [[ "$sample" =~ ^NA[0-9]+$ ]]; then
    echo "$sample"
  else
    echo "WARN: skipping unexpected filename: $base" >&2
  fi
done | sort -u > "$tmp"

mv -f "$tmp" "${SAMPLE_LIST}"

N="$(wc -l < "${SAMPLE_LIST}" | tr -d ' ')"
if [[ "${N}" -lt 1 ]]; then
  echo "ERROR: SAMPLE_LIST is empty: ${SAMPLE_LIST}" >&2
  exit 1
fi

cmd=(sbatch --array=1-"${N}" --export=ALL,SAMPLE_LIST="${SAMPLE_LIST}",FASTA_DIR="${DIR}" "${SBATCH_SCRIPT}")

if [[ "${DRY_RUN}" == "1" ]]; then
  printf '[DRY_RUN] %q ' "${cmd[@]}"; echo
else
  echo "Submitting array with ${N} samples using SAMPLE_LIST=${SAMPLE_LIST}"
  "${cmd[@]}"
fi
