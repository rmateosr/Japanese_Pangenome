#!/usr/bin/env bash
#SBATCH --job-name=concat_hifi
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=06:00:00
#SBATCH --output=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/logs/%x_%j.out
#SBATCH --error=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/logs/%x_%j.err

set -euo pipefail

mkdir -p /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/logs

SCRIPT="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/concat_hifi_per_sample.sh"

bash "$SCRIPT"

