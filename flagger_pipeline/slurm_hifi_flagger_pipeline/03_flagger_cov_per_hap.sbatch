#!/usr/bin/env bash
#SBATCH --job-name=flagger_cov
#SBATCH --output=log/%x.%A.%a.out
#SBATCH --error=log/%x.%A.%a.err

set -euo pipefail

: "${MANIFEST:?MANIFEST not set}"
: "${CONCAT_DIR:?CONCAT_DIR not set}"
: "${CONCAT_SUFFIX:?CONCAT_SUFFIX not set}"
: "${OUT_BAM_DIR:?OUT_BAM_DIR not set}"
: "${OUT_CRAM_DIR:?OUT_CRAM_DIR not set}"
: "${FLAGGER_WORK_ROOT:?FLAGGER_WORK_ROOT not set}"
: "${FLAGGER_SIF:?FLAGGER_SIF not set}"

threads="${SLURM_CPUS_PER_TASK:-1}"
mkdir -p log

row="$(
  awk -v task="$SLURM_ARRAY_TASK_ID" 'BEGIN{FS=OFS="\t"}
    NR==1{
      for(i=1;i<=NF;i++) col[$i]=i
      if(!("ID" in col)){
        print "ERROR: manifest header must contain: ID" > "/dev/stderr"
        exit 20
      }
      next
    }
    (NR-1)==task{
      print $col["ID"]
      exit
    }' "$MANIFEST"
)"

if [[ -z "$row" ]]; then
  echo "ERROR: could not read manifest row for SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID" >&2
  exit 2
fi

ID="$row"
if [[ -z "${ID:-}" ]]; then
  echo "ERROR: empty ID for task=$SLURM_ARRAY_TASK_ID" >&2
  exit 3
fi

fasta="${CONCAT_DIR}/${ID}${CONCAT_SUFFIX}"
if [[ ! -s "$fasta" ]]; then
  echo "INFO: missing concatenated FASTA for ID=$ID, skipping flagger: $fasta"
  exit 0
fi

# Workspace layout per sample
work="${FLAGGER_WORK_ROOT}/${ID}"
bam_dir="${work}/bam"
ref_bed_dir="${work}/reference_bed"
ann_dir="${work}/annotations_json"
hap_bam_dir="${work}/bam_hap"
cov_dir="${work}/covfilesHap_direct"

mkdir -p "$work" "$bam_dir" "$ref_bed_dir" "$ann_dir" "$hap_bam_dir" "$cov_dir"

# Prefer BAM from mapping stage. If absent, try CRAM decode.
bam="${OUT_BAM_DIR}/${ID}.mm2.sorted.bam"
if [[ ! -s "$bam" ]]; then
  cram="${OUT_CRAM_DIR}/${ID}.mm2.sorted.cram"
  bam="${bam_dir}/${ID}.mm2.sorted.bam"
  if [[ ! -s "$bam" ]]; then
    if [[ ! -s "$cram" ]]; then
      echo "INFO: no BAM/CRAM for ID=$ID, skipping flagger."
      exit 0
    fi
    echo "Decoding CRAM to BAM for ID=$ID"
    samtools view -@ "$threads" -b -T "$fasta" -o "$bam" "$cram"
    samtools index -@ "$threads" "$bam"
  fi
fi

# Ensure FASTA index exists
if [[ ! -s "${fasta}.fai" ]]; then
  samtools faidx "$fasta"
fi

hap1_bed="${ref_bed_dir}/${ID}.hap1.bed"
hap2_bed="${ref_bed_dir}/${ID}.hap2.bed"

if [[ ! -s "$hap1_bed" ]]; then
  awk 'BEGIN{OFS="\t"} $1 ~ /#1#/ {print $1, 0, $2}' "${fasta}.fai" > "$hap1_bed"
fi
if [[ ! -s "$hap2_bed" ]]; then
  awk 'BEGIN{OFS="\t"} $1 ~ /#2#/ {print $1, 0, $2}' "${fasta}.fai" > "$hap2_bed"
fi

if [[ ! -s "$hap1_bed" || ! -s "$hap2_bed" ]]; then
  echo "ERROR: hap BEDs empty for ID=$ID. Contig names must contain '#1#' and '#2#'." >&2
  exit 11
fi

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

ann_json="${ann_dir}/${ID}_annotations_path.json"
if [[ ! -s "$ann_json" ]]; then
  cat > "$ann_json" <<EOF
{
  "hap1": "${hap1_bed}",
  "hap2": "${hap2_bed}"
}
EOF
fi

cov1="${cov_dir}/${ID}.hap1.cov.gz"
cov2="${cov_dir}/${ID}.hap2.cov.gz"

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
  echo "SKIP: hap1 cov exists for ID=$ID"
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
  echo "SKIP: hap2 cov exists for ID=$ID"
fi

echo "DONE: ID=$ID"
