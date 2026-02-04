#!/usr/bin/env bash
#SBATCH --job-name=concat_mm2_hifi_flagstat
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/logs/%x_%A_%a.out
#SBATCH --error=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/logs/%x_%A_%a.err

set -euo pipefail

#module load minimap2 samtools 2>/dev/null || true

mkdir -p /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/logs

export MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest.tsv"
export TABLE="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/visc_fastq_table.txt"
export OUT_FLAGSTAT_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Flagstat_HiFi"

# Use a single thread count everywhere
export THREADS="${SLURM_CPUS_PER_TASK}"

bash /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/run_one_concat_mm2_bamsize.sh
