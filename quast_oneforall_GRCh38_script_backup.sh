#!/usr/bin/env bash
#SBATCH --job-name=quast_GRCh38
#SBATCH --cpus-per-task=8
#SBATCH --mem=90G
#SBATCH --output=log/%x_%j.out
#SBATCH --error=log/%x_%j.err
ssh a001

OUTPUT_FOLDER="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/quast/GRCh38"
ASSEMBLIES_FOLDER="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies"
REFERENCE_FOLDER="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/References"

conda init
conda activate quast

mkdir -p ${OUTPUT_FOLDER}

quast.py \
  -o ${OUTPUT_FOLDER}/quast_out_oneforall_GRCh38 \
  -r ${REFERENCE_FOLDER}/GCF_000001405.40_GRCh38.p14_genomic.fna.gz \
  -t 8 --large -e \
  ${ASSEMBLIES_FOLDER}/*.fa.gz


