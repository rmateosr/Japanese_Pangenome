#!/usr/bin/env bash
#SBATCH --job-name=flagger
#SBATCH --time=24:00:00
#SBATCH --mem=40G
#SBATCH --cpus-per-task=4
#SBATCH --export=ALL
#SBATCH --output=log/%x.%A.%a.out
#SBATCH --error=log/%x.%A.%a.err

set -euo pipefail
set -x

mkdir -p log

# ----------------------------
# Inputs: prefer env overrides
# ----------------------------
: "${SAMPLE_LIST:?Set SAMPLE_LIST to a file with one SAMPLE per line}"
: "${INPUT_DIR:=/rshare1/ZETTAI_path_WA_slash_home_KARA/home/rmateosr/Pangenome_Japan/HPRC_Data/JPT/assembly/HiFi_fastq_preassembly/minimap2_output_cram}"
: "${WORKING_DIR:=/rshare1/ZETTAI_path_WA_slash_home_KARA/home/rmateosr/Pangenome_Japan/HPRC_Data/JPT/assembly/assembly_qc/my_flagger}"
: "${REF:=/rshare1/ZETTAI_path_WA_slash_home_KARA/home/rmateosr/Pangenome_Japan/HPRC_Data/JPT/illumina_short_reads/GRCh38_full_analysis_set_plus_decoy_hla.fa}"
: "${FASTA_DIR:=/rshare1/ZETTAI_path_WA_slash_home_KARA/home/rmateosr/Pangenome_Japan/HPRC_Data/JPT/assembly/zipped_concat}"
: "${FASTA_SUFFIX:=_concatenated_hprc_r2_v1.0.1.fa.gz}"
: "${SIF:=flagger_v1.1.0.sif}"
: "${ALPHA_TSV:=/home/programs/config/alpha_optimum_trunc_exp_gaussian_w_4000_n_50.tsv}"

THREADS="${SLURM_CPUS_PER_TASK:-4}"

# Resolve sample from array index (1-based)
: "${SLURM_ARRAY_TASK_ID:?This script is intended for sbatch --array=...}"
SAMPLE="$(sed -n "${SLURM_ARRAY_TASK_ID}p" "${SAMPLE_LIST}" | tr -d '\r' | xargs)"
: "${SAMPLE:?Failed to read SAMPLE from SAMPLE_LIST at line ${SLURM_ARRAY_TASK_ID}}"

cd "${WORKING_DIR}"

BAM_DIR="${WORKING_DIR}/bam"
COV_DIR="${WORKING_DIR}/covfiles"
BED_DIR="${WORKING_DIR}/reference_bed"
ANN_DIR="${WORKING_DIR}/annotations_json"
HMM_DIR="${WORKING_DIR}/hmm_flagger_outputs/${SAMPLE}_hmm_flagger_outputs"

mkdir -p "${BAM_DIR}" "${COV_DIR}" "${BED_DIR}" "${ANN_DIR}"

IN_CRAM="${INPUT_DIR}/${SAMPLE}.mm2.sorted.cram"
OUT_BAM="${BAM_DIR}/${SAMPLE}.mm2.sorted.bam"
COV_FILE="${COV_DIR}/${SAMPLE}_coverage_file.cov.gz"
FASTA_GZ="${FASTA_DIR}/${SAMPLE}${FASTA_SUFFIX}"
OUT_BED="${BED_DIR}/${SAMPLE}.whole_genome.bed"
ANN_JSON="${ANN_DIR}/${SAMPLE}_annotations_path.json"

# ----------------------------
# Sanity checks
# ----------------------------
[[ -f "${IN_CRAM}" ]] || { echo "ERROR: Input CRAM not found: ${IN_CRAM}" >&2; exit 2; }
[[ -f "${FASTA_GZ}" ]] || { echo "ERROR: FASTA_GZ not found: ${FASTA_GZ}" >&2; exit 2; }
[[ -f "${REF}" ]] || { echo "ERROR: REF not found: ${REF}" >&2; exit 2; }
[[ -f "${ALPHA_TSV}" ]] || { echo "ERROR: ALPHA_TSV not found: ${ALPHA_TSV}" >&2; exit 2; }
[[ -f "${SIF}" ]] || { echo "ERROR: SIF not found: ${SIF}" >&2; exit 2; }

# ----------------------------
# Build BED + annotation JSON (idempotent)
# ----------------------------
samtools faidx "${FASTA_GZ}"
awk 'BEGIN{OFS="\t"} {print $1, 0, $2}' "${FASTA_GZ}.fai" > "${OUT_BED}"

cat > "${ANN_JSON}" <<EOF
{
  "whole_genome" : "${OUT_BED}"
}
EOF

# ----------------------------
# Step 1: CRAM -> BAM (skip if already done+indexed)
# ----------------------------
if [[ -s "${OUT_BAM}" && -s "${OUT_BAM}.bai" ]]; then
  echo "[INFO] BAM+BAI exist, skipping CRAM->BAM: ${OUT_BAM}"
else
  echo "[INFO] Converting ${IN_CRAM} -> ${OUT_BAM}"
  samtools view -@ "${THREADS}" -b -T "${REF}" -o "${OUT_BAM}" "${IN_CRAM}"
  samtools index "${OUT_BAM}"
fi

# ----------------------------
# Step 2: bam2cov (skip if cov exists)
# ----------------------------
if [[ -s "${COV_FILE}" ]]; then
  echo "[INFO] COV exists, skipping bam2cov: ${COV_FILE}"
else
  apptainer exec --cleanenv \
    --bind "${WORKING_DIR}:${WORKING_DIR}" \
    "${SIF}" \
    bam2cov \
      --bam "${OUT_BAM}" \
      --output "${COV_FILE}" \
      --annotationJson "${ANN_JSON}" \
      --threads "${THREADS}" \
      --baselineAnnotation whole_genome
fi

# ----------------------------
# Step 3: hmm_flagger (skip if output looks complete)
# ----------------------------
# Heuristic: skip if output dir exists and non-empty
if [[ -d "${HMM_DIR}" ]] && find "${HMM_DIR}" -mindepth 1 -maxdepth 1 | read -r _; then
  echo "[INFO] HMM output dir non-empty, skipping hmm_flagger: ${HMM_DIR}"
else
  mkdir -p "${HMM_DIR}"
  apptainer exec --cleanenv \
    --bind "${WORKING_DIR}:${WORKING_DIR}" \
    "${SIF}" \
    hmm_flagger \
      --input "${COV_FILE}" \
      --outputDir "${HMM_DIR}" \
      --alphaTsv "${ALPHA_TSV}" \
      --labelNames Err,Dup,Hap,Col \
      --threads "${THREADS}"
fi

echo "[INFO] Flagger pipeline completed for ${SAMPLE}"

# ----------------------------
# Cleanup (matches your current behavior)
# ----------------------------
echo "[INFO] Removing intermediate BAM for ${SAMPLE}: ${OUT_BAM} and index"
rm -f "${OUT_BAM}" "${OUT_BAM}.bai"
