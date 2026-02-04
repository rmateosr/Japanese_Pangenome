#!/usr/bin/env bash
#SBATCH --job-name=quast_GRCh38
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --output=log/%x_%j.out
#SBATCH --error=log/%x_%j.err

set -euo pipefail

# Do not ssh inside an sbatch script
mkdir -p log

OUTPUT_FOLDER="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/quast/GRCh38"
ASSEMBLIES_FOLDER="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies"
REFERENCE_FOLDER="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/References"

# Create output dir
mkdir -p "${OUTPUT_FOLDER}"


# Load conda function into this non-interactive shell, then activate env
conda init
conda activate quast
