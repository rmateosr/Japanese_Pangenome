#!/usr/bin/env bash
#SBATCH --job-name=mm2_map
#SBATCH --output=log/%x.%A.%a.out
#SBATCH --error=log/%x.%A.%a.err

set -euo pipefail

: "${MANIFEST:?MANIFEST not set}"
: "${CONCAT_DIR:?CONCAT_DIR not set}"
: "${CONCAT_SUFFIX:?CONCAT_SUFFIX not set}"
: "${OUT_BAM_DIR:?OUT_BAM_DIR not set}"
: "${OUT_FLAGSTAT_DIR:?OUT_FLAGSTAT_DIR not set}"

threads="${SLURM_CPUS_PER_TASK:-1}"
sort_threads=$(( threads > 1 ? threads - 1 : 1 ))

mkdir -p log "$OUT_BAM_DIR" "$OUT_FLAGSTAT_DIR"

row="$(
  awk -v task="$SLURM_ARRAY_TASK_ID" 'BEGIN{FS=OFS="\t"}
    NR==1{
      for(i=1;i<=NF;i++) col[$i]=i
      if(!("ID" in col) || !("HiFi_paths" in col)){
        print "ERROR: manifest header must contain: ID, HiFi_paths" > "/dev/stderr"
        exit 20
      }
      next
    }
    (NR-1)==task{
      print $col["ID"], $col["HiFi_paths"]
      exit
    }' "$MANIFEST"
)"

if [[ -z "$row" ]]; then
  echo "ERROR: could not read manifest row for SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID" >&2
  exit 2
fi

IFS=$'\t' read -r ID hifi_paths <<< "$row"

if [[ -z "${ID:-}" ]]; then
  echo "ERROR: empty ID for task=$SLURM_ARRAY_TASK_ID" >&2
  exit 3
fi

# Skip semantics (match your yak pattern)
if [[ -z "${hifi_paths:-}" || "$hifi_paths" == "NA" || "$hifi_paths" == "na" || "$hifi_paths" == "." ]]; then
  echo "INFO: ID=$ID has HiFi_paths=NA (or empty). Skipping mapping."
  exit 0
fi

fasta="${CONCAT_DIR}/${ID}${CONCAT_SUFFIX}"
if [[ ! -s "$fasta" ]]; then
  echo "ERROR: missing concatenated reference FASTA for ID=$ID: $fasta" >&2
  exit 1
fi

bam="${OUT_BAM_DIR}/${ID}.mm2.sorted.bam"
bai="${bam}.bai"
flagstat="${OUT_FLAGSTAT_DIR}/${ID}.mm2.sorted.flagstat.txt"

if [[ -s "$bam" && -s "$bai" && -s "$flagstat" ]]; then
  echo "SKIP: mapping outputs exist for ID=$ID"
  exit 0
fi

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

IFS=',' read -r -a HIFI_ARR <<< "$hifi_paths"

tmp_list=()
for f in "${HIFI_ARR[@]}"; do
  f="$(trim "$f")"
  [[ -z "$f" ]] && continue
  if [[ ! -f "$f" ]]; then
    echo "ERROR: HiFi FASTQ not found for ID=$ID: $f" >&2
    exit 22
  fi
  tmp_list+=("$f")
done
HIFI_ARR=("${tmp_list[@]}")

if [[ "${#HIFI_ARR[@]}" -lt 1 ]]; then
  echo "ERROR: could not parse any HiFi FASTQs for ID=$ID from HiFi_paths=$hifi_paths" >&2
  exit 21
fi

echo "ID=$ID"
echo "Reference=$fasta"
echo "HiFi_paths_count=${#HIFI_ARR[@]}"
printf 'HiFi_paths:\n'; printf '  %s\n' "${HIFI_ARR[@]}"

MAX_DIRECT="${MAX_DIRECT_FASTQS:-40}"

if (( ${#HIFI_ARR[@]} <= MAX_DIRECT )); then
  minimap2 --cs -L -Y -t "$threads" -ax map-hifi "$fasta" "${HIFI_ARR[@]}" \
    | samtools sort -@ "$sort_threads" -o "$bam" -
else
  echo "INFO: ${#HIFI_ARR[@]} FASTQs > MAX_DIRECT=${MAX_DIRECT}, concatenating to scratch for mapping."
  tmpdir="${SLURM_TMPDIR:-/tmp}"
  tmp_hifi="$(mktemp --tmpdir="$tmpdir" "${ID}.HiFi.concat.XXXXXX.fastq.gz")"
  trap 'rm -f "$tmp_hifi" 2>/dev/null || true' EXIT

  cat "${HIFI_ARR[@]}" > "$tmp_hifi"
  gzip -t "$tmp_hifi"

  minimap2 --cs -L -Y -t "$threads" -ax map-hifi "$fasta" "$tmp_hifi" \
    | samtools sort -@ "$sort_threads" -o "$bam" -

  rm -f "$tmp_hifi"
  trap - EXIT
fi

samtools index -@ "$threads" "$bam"
samtools flagstat -@ "$threads" "$bam" > "$flagstat"
