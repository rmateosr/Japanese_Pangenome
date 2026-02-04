#!/usr/bin/env bash
#SBATCH --job-name=bam2cram
#SBATCH --output=log/%x.%A.%a.out
#SBATCH --error=log/%x.%A.%a.err

set -euo pipefail

: "${MANIFEST:?MANIFEST not set}"
: "${CONCAT_DIR:?CONCAT_DIR not set}"
: "${CONCAT_SUFFIX:?CONCAT_SUFFIX not set}"
: "${OUT_BAM_DIR:?OUT_BAM_DIR not set}"
: "${OUT_CRAM_DIR:?OUT_CRAM_DIR not set}"

threads="${SLURM_CPUS_PER_TASK:-1}"
mkdir -p log "$OUT_CRAM_DIR"

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
bam="${OUT_BAM_DIR}/${ID}.mm2.sorted.bam"

cram="${OUT_CRAM_DIR}/${ID}.mm2.sorted.cram"
crai="${cram}.crai"

if [[ -s "$cram" && -s "$crai" ]]; then
  echo "SKIP: CRAM exists for ID=$ID"
  exit 0
fi

# If mapping was skipped, BAM may not exist. Treat as a skip, not a hard error.
if [[ ! -s "$bam" ]]; then
  echo "INFO: missing BAM for ID=$ID, skipping CRAM step: $bam"
  exit 0
fi
if [[ ! -s "$fasta" ]]; then
  echo "ERROR: missing reference FASTA for ID=$ID: $fasta" >&2
  exit 1
fi

samtools view -@ "$threads" -C -T "$fasta" -o "$cram" "$bam"
samtools index -@ "$threads" "$cram"
