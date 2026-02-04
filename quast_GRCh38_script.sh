#!/usr/bin/env bash
#SBATCH --job-name=quast_GRCh38
#SBATCH --cpus-per-task=16
#SBATCH --mem=90G
#SBATCH --chdir=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2
#SBATCH --output=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log/%x_%j.out
#SBATCH --error=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log/%x_%j.err

set -euo pipefail

OUTPUT_ROOT="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/quast/GRCh38"
OUTPUT_DIR="${OUTPUT_ROOT}/quast_out_oneforall_GRCh38"

ASSEMBLIES_FOLDER="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies"
REFERENCE_FA="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/References/GCF_000001405.40_GRCh38.p14_genomic.fna.gz"
QUAST_BIN="/home/raulnmateos/miniconda/envs/quast/bin/quast.py"

mkdir -p "/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/log" "${OUTPUT_DIR}"

# Prefer node-local temp if available
TMPBASE="${SLURM_TMPDIR:-/tmp/${USER}/quast_${SLURM_JOB_ID}}"
mkdir -p "${TMPBASE}"
export TMPDIR="${TMPBASE}"

# Sanity checks
[[ -x "${QUAST_BIN}" ]] || { echo "ERROR: QUAST not executable: ${QUAST_BIN}" >&2; exit 1; }
[[ -r "${REFERENCE_FA}" ]] || { echo "ERROR: Reference not readable: ${REFERENCE_FA}" >&2; exit 1; }
[[ -d "${ASSEMBLIES_FOLDER}" ]] || { echo "ERROR: Assemblies folder missing: ${ASSEMBLIES_FOLDER}" >&2; exit 1; }

# Collect inputs safely (and fail if none)
shopt -s nullglob
ASSEMBLIES=( "${ASSEMBLIES_FOLDER}"/*.fa.gz )
shopt -u nullglob

echo "Found ${#ASSEMBLIES[@]} assembly files in: ${ASSEMBLIES_FOLDER}"
if [[ "${#ASSEMBLIES[@]}" -eq 0 ]]; then
  echo "ERROR: No input contigs matched ${ASSEMBLIES_FOLDER}/*.fa.gz" >&2
  echo "DEBUG: Listing top of folder:" >&2
  ls -lah "${ASSEMBLIES_FOLDER}" | head -200 >&2
  exit 1
fi

# QUAST run (comments are on separate lines to keep bash parsing correct)
"${QUAST_BIN}" \
  -o "${OUTPUT_DIR}" \
  -r "${REFERENCE_FA}" \
  --large \
  -t 16 \
  --memory-efficient \
  --no-snps \
  --no-plots \
  --no-html \
  --space-efficient \
  "${ASSEMBLIES[@]}"
