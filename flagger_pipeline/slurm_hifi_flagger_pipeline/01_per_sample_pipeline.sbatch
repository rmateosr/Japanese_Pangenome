#!/usr/bin/env bash
#SBATCH --job-name=per_sample_pipeline
#SBATCH --output=log/%x.%A.%a.out
#SBATCH --error=log/%x.%A.%a.err

set -euo pipefail

: "${MANIFEST:?MANIFEST not set}"
: "${OUT_BAM_DIR:?OUT_BAM_DIR not set}"
: "${OUT_FLAGSTAT_DIR:?OUT_FLAGSTAT_DIR not set}"
: "${OUT_CRAM_DIR:?OUT_CRAM_DIR not set}"
: "${FLAGGER_WORK_ROOT:?FLAGGER_WORK_ROOT not set}"
: "${FLAGGER_SIF:?FLAGGER_SIF not set}"

threads="${SLURM_CPUS_PER_TASK:-1}"
sort_threads=$(( threads > 1 ? threads - 1 : 1 ))

mkdir -p log "$OUT_BAM_DIR" "$OUT_FLAGSTAT_DIR" "$OUT_CRAM_DIR" "$FLAGGER_WORK_ROOT"

# ----------------------------
# Read manifest row by array index; locate columns by header name.
# Required: ID, fasta1_path, fasta2_path, HiFi_paths
# ----------------------------
row="$(
  awk -v task="$SLURM_ARRAY_TASK_ID" 'BEGIN{FS=OFS="\t"}
    NR==1{
      for(i=1;i<=NF;i++) col[$i]=i
      if(!("ID" in col) || !("fasta1_path" in col) || !("fasta2_path" in col) || !("HiFi_paths" in col)){
        print "ERROR: manifest header must contain: ID, fasta1_path, fasta2_path, HiFi_paths" > "/dev/stderr"
        exit 20
      }
      next
    }
    (NR-1)==task{
      print $col["ID"], $col["fasta1_path"], $col["fasta2_path"], $col["HiFi_paths"]
      exit
    }' "$MANIFEST"
)"
if [[ -z "$row" ]]; then
  echo "ERROR: could not read manifest row for task=$SLURM_ARRAY_TASK_ID" >&2
  exit 2
fi

IFS=$'\t' read -r ID hap1_fa hap2_fa hifi_paths <<< "$row"
if [[ -z "${ID:-}" ]]; then
  echo "ERROR: empty ID for task=$SLURM_ARRAY_TASK_ID" >&2
  exit 3
fi

# Skip if no HiFi
if [[ -z "${hifi_paths:-}" || "$hifi_paths" == "NA" || "$hifi_paths" == "na" || "$hifi_paths" == "." ]]; then
  echo "INFO: ID=$ID has HiFi_paths=NA (or empty). Skipping."
  exit 0
fi

# Validate assemblies exist
for asm in "$hap1_fa" "$hap2_fa"; do
  if [[ -z "${asm:-}" || "$asm" == "NA" || "$asm" == "." ]]; then
    echo "ERROR: assembly path missing/NA for ID=$ID" >&2
    exit 4
  fi
  if [[ ! -f "$asm" ]]; then
    echo "ERROR: assembly not found for ID=$ID: $asm" >&2
    exit 5
  fi
done

# Parse HiFi FASTQ paths from comma-separated string
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

# Outputs (shared filesystem)
bam="${OUT_BAM_DIR}/${ID}.mm2.sorted.bam"
bai="${bam}.bai"
flagstat="${OUT_FLAGSTAT_DIR}/${ID}.mm2.sorted.flagstat.txt"
cram="${OUT_CRAM_DIR}/${ID}.mm2.sorted.cram"
crai="${cram}.crai"

# Flagger outputs (final products for this pipeline)
work="${FLAGGER_WORK_ROOT}/${ID}"
ref_bed_dir="${work}/reference_bed"
ann_dir="${work}/annotations_json"
hap_bam_dir="${work}/bam_hap"
cov_dir="${work}/covfilesHap_direct"
mkdir -p "$work" "$ref_bed_dir" "$ann_dir" "$hap_bam_dir" "$cov_dir"

cov1="${cov_dir}/${ID}.hap1.cov.gz"
cov2="${cov_dir}/${ID}.hap2.cov.gz"

# If both cov outputs exist with indexes, consider the sample complete
if [[ -s "$cov1" && -s "${cov1}.index" && -s "$cov2" && -s "${cov2}.index" ]]; then
  echo "SKIP: ID=$ID cov outputs already exist."
  exit 0
fi

# ----------------------------
# Build TEMP concatenated reference FASTA in job scratch
# This MUST live for the whole job because it's used for:
# - mapping
# - CRAM encoding/decoding
# - building hap BEDs from .fai
# It is deleted on exit.
# ----------------------------
scratch="${SLURM_TMPDIR:-/tmp}"
tmp_ref="${scratch}/${ID}.ref.concat.fa.gz"
tmp_ref_fai="${tmp_ref}.fai"
tmp_ref_gzi="${tmp_ref}.gzi"

rm -f "$tmp_ref" "$tmp_ref_fai" "$tmp_ref_gzi"

cleanup_tmp() {
  rm -f "$tmp_ref" "$tmp_ref_fai" "$tmp_ref_gzi" 2>/dev/null || true
  rm -f "${scratch}/${ID}.decoded.XXXXXX.bam" 2>/dev/null || true
}
trap cleanup_tmp EXIT

stream_with_marker() {
  local fasta="$1"
  local marker="$2"  # "#1#" or "#2#"
  if [[ "$fasta" == *.gz ]]; then
    zcat "$fasta"
  else
    cat "$fasta"
  fi | awk -v m="$marker" '
    /^>/{
      h=substr($0,2)
      if (index(h,"#1#")>0 || index(h,"#2#")>0) { print ">"h; next }
      print ">"h m
      next
    }
    { print }
  '
}

echo "ID=$ID"
echo "hap1_fa=$hap1_fa"
echo "hap2_fa=$hap2_fa"
echo "HiFi_paths_count=${#HIFI_ARR[@]}"
echo "scratch=$scratch"
echo "tmp_ref=$tmp_ref"

# Requires bgzip and samtools on PATH
(
  stream_with_marker "$hap1_fa" "#1#"
  stream_with_marker "$hap2_fa" "#2#"
) | bgzip -@ "$threads" -c > "$tmp_ref"

# Index reference
samtools faidx "$tmp_ref"
if [[ ! -s "$tmp_ref" || ! -s "$tmp_ref_fai" ]]; then
  echo "ERROR: failed to create temp reference for ID=$ID" >&2
  exit 6
fi

# ----------------------------
# Mapping (only if BAM missing)
# Uses multiple FASTQs directly; fallback to temp FASTQ concat if too many inputs.
# ----------------------------
MAX_DIRECT="${MAX_DIRECT_FASTQS:-40}"

if [[ ! -s "$bam" ]]; then
  if (( ${#HIFI_ARR[@]} <= MAX_DIRECT )); then
    minimap2 --cs -L -Y -t "$threads" -ax map-hifi "$tmp_ref" "${HIFI_ARR[@]}" \
      | samtools sort -@ "$sort_threads" -o "$bam" -
  else
    echo "INFO: ${#HIFI_ARR[@]} FASTQs > MAX_DIRECT=${MAX_DIRECT}, concatenating to scratch for mapping."
    tmp_hifi="$(mktemp --tmpdir="$scratch" "${ID}.HiFi.concat.XXXXXX.fastq.gz")"
    trap 'rm -f "$tmp_hifi" 2>/dev/null || true; cleanup_tmp' EXIT
    cat "${HIFI_ARR[@]}" > "$tmp_hifi"
    gzip -t "$tmp_hifi"

    minimap2 --cs -L -Y -t "$threads" -ax map-hifi "$tmp_ref" "$tmp_hifi" \
      | samtools sort -@ "$sort_threads" -o "$bam" -

    rm -f "$tmp_hifi"
    trap cleanup_tmp EXIT
  fi

  samtools index -@ "$threads" "$bam"
else
  echo "INFO: BAM exists, reusing: $bam"
  [[ -s "$bai" ]] || samtools index -@ "$threads" "$bam"
fi

# Flagstat (QC)
if [[ ! -s "$flagstat" ]]; then
  samtools flagstat -@ "$threads" "$bam" > "$flagstat"
fi

# ----------------------------
# BAM -> CRAM (optional artifact)
# ----------------------------
if [[ ! -s "$cram" ]]; then
  samtools view -@ "$threads" -C -T "$tmp_ref" -o "$cram" "$bam"
  samtools index -@ "$threads" "$cram"
else
  echo "INFO: CRAM exists, reusing: $cram"
  [[ -s "$crai" ]] || samtools index -@ "$threads" "$cram"
fi

# ----------------------------
# Build hap BEDs from temp reference .fai
# ----------------------------
hap1_bed="${ref_bed_dir}/${ID}.hap1.bed"
hap2_bed="${ref_bed_dir}/${ID}.hap2.bed"

if [[ ! -s "$hap1_bed" ]]; then
  awk 'BEGIN{OFS="\t"} $1 ~ /#1#/ {print $1, 0, $2}' "$tmp_ref_fai" > "$hap1_bed"
fi
if [[ ! -s "$hap2_bed" ]]; then
  awk 'BEGIN{OFS="\t"} $1 ~ /#2#/ {print $1, 0, $2}' "$tmp_ref_fai" > "$hap2_bed"
fi

if [[ ! -s "$hap1_bed" || ! -s "$hap2_bed" ]]; then
  echo "ERROR: hap BEDs empty for ID=$ID. Contig names must contain '#1#' and '#2#'." >&2
  exit 11
fi

# ----------------------------
# Prepare hap BAMs (in shared work dir)
# ----------------------------
hap1_bam="${hap_bam_dir}/${ID}.hap1.bam"
hap2_bam="${hap_bam_dir}/${ID}.hap2.bam"

if [[ ! -s "$hap1_bam" ]]; then
  samtools view -@ "$threads" -b -L "$hap1_bed" -o "$hap1_bam" "$bam"
  samtools index -@ "$threads" "$hap1_bam"
fi
if [[ ! -s "$hap2_bam" ]]; then
  samtools view -@ "$threads" -b -L "$hap2_bed" -o "$hap2_bam" "$bam"
  samtools index -@ "$threads" "$hap2_bam"
fi

# Annotations JSON for flagger
ann_json="${ann_dir}/${ID}_annotations_path.json"
if [[ ! -s "$ann_json" ]]; then
  cat > "$ann_json" <<EOF
{
  "hap1": "${hap1_bed}",
  "hap2": "${hap2_bed}"
}
EOF
fi

# ----------------------------
# Flagger bam2cov per hap
# Everything needed by the container lives inside $work, so bind only that.
# ----------------------------
if [[ ! -s "$cov1" || ! -s "${cov1}.index" ]]; then
  rm -f "$cov1" "${cov1}.index"
  apptainer exec --cleanenv \
    --bind "${work}:${work}" \
    "$FLAGGER_SIF" \
    bam2cov \
      --bam "$hap1_bam" \
      --output "$cov1" \
      --annotationJson "$ann_json" \
      --threads "$threads" \
      --baselineAnnotation hap1
else
  echo "INFO: hap1 cov exists, skipping: $cov1"
fi

if [[ ! -s "$cov2" || ! -s "${cov2}.index" ]]; then
  rm -f "$cov2" "${cov2}.index"
  apptainer exec --cleanenv \
    --bind "${work}:${work}" \
    "$FLAGGER_SIF" \
    bam2cov \
      --bam "$hap2_bam" \
      --output "$cov2" \
      --annotationJson "$ann_json" \
      --threads "$threads" \
      --baselineAnnotation hap2
else
  echo "INFO: hap2 cov exists, skipping: $cov2"
fi

echo "DONE: ID=$ID"
echo "Temp reference removed automatically on exit: $tmp_ref"
