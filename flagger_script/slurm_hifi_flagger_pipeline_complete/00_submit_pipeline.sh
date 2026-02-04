#!/usr/bin/env bash
set -euo pipefail

# SLURM submission wrapper for the per-sample HiFi->(BAM/CRAM)->Flagger cov pipeline.
# Key property:
#   - hap1/hap2 FASTAs are concatenated into a TEMP reference in $SLURM_TMPDIR (or /tmp)
#   - the TEMP reference is removed at the end of each array task
#
# Usage:
#   bash 00_submit_pipeline.sh /path/to/hap_manifestPacBio.tsv
#
# Manifest (TSV with header) must contain columns:
#   ID  fasta1_path  fasta2_path  HiFi_paths
#
# HiFi_paths:
#   - one or more gzipped FASTQ paths
#   - if multiple, comma-separated (spaces allowed)

MANIFEST="${1:?Provide manifest TSV path}"

# ----------------------------
# Outputs on shared filesystem (CHANGE THESE)
# ----------------------------
OUT_ROOT="/path/to/output_root"

OUT_BAM_DIR="${OUT_ROOT}/minimap2_output"
OUT_FLAGSTAT_DIR="${OUT_ROOT}/minimap2_output_samtools_flagstat"
OUT_CRAM_DIR="${OUT_ROOT}/minimap2_output_cram"
FLAGGER_WORK_ROOT="${OUT_ROOT}/flagger_work"

# Flagger container (Apptainer SIF) (CHANGE THIS)
FLAGGER_SIF="/path/to/flagger_v1.1.0.sif"

# ----------------------------
# Resources (tune later)
# ----------------------------
CPUS="${CPUS:-16}"
MEM="${MEM:-96G}"
TIME="${TIME:-36:00:00}"

mkdir -p log "$OUT_BAM_DIR" "$OUT_FLAGSTAT_DIR" "$OUT_CRAM_DIR" "$FLAGGER_WORK_ROOT"

# Count rows (assume header is first line)
n="$(($(wc -l < "$MANIFEST") - 1))"
if (( n <= 0 )); then
  echo "ERROR: manifest seems empty: $MANIFEST" >&2
  exit 1
fi

jid="$(
  sbatch --parsable \
    --export=ALL,MANIFEST="$MANIFEST",OUT_BAM_DIR="$OUT_BAM_DIR",OUT_FLAGSTAT_DIR="$OUT_FLAGSTAT_DIR",OUT_CRAM_DIR="$OUT_CRAM_DIR",FLAGGER_WORK_ROOT="$FLAGGER_WORK_ROOT",FLAGGER_SIF="$FLAGGER_SIF" \
    --array=1-"$n" \
    --cpus-per-task="$CPUS" --mem="$MEM" --time="$TIME" \
    01_per_sample_pipeline.sbatch
)"

echo "Submitted per-sample pipeline array job_id=$jid"
