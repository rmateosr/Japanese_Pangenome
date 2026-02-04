#!/usr/bin/env bash
#SBATCH --job-name=cram2yak
#SBATCH --cpus-per-task=24
#SBATCH --mem=4G
#SBATCH --output=log/%x_%j.out
#SBATCH --error=log/%x_%j.err

set -xv
set -euo pipefail

mkdir -p log

module use /usr/local/package/modulefiles
module load samtools/1.19
# If yak is available as a module on your system, load it here:
# module load yak/<version>

# ----------------------------
# User configuration
# ----------------------------
CRAM_DIR="/rshare1/ZETTAI_path_WA_slash_home_KARA/home/rmateosr/Pangenome_Japan/HPRC_Data/JPT/illumina_short_reads/samples_cram"
FASTQ_DIR="/rshare1/ZETTAI_path_WA_slash_home_KARA/home/rmateosr/Pangenome_Japan/HPRC_Data/JPT/illumina_short_reads/samples_fastq"
REF="/rshare1/ZETTAI_path_WA_slash_home_KARA/home/rmateosr/Pangenome_Japan/HPRC_Data/JPT/illumina_short_reads/GRCh38_full_analysis_set_plus_decoy_hla.fa"

# yak binary (adjust if needed)
YAK_BIN="../yak/yak"

# Threads: by default, use what Slurm allocated
CPUS="${SLURM_CPUS_PER_TASK:-24}"
SAMTOOLS_THREADS="${SAMTOOLS_THREADS:-$CPUS}"
YAK_THREADS="${YAK_THREADS:-$CPUS}"

COUNTS_DIR="counts_sample_1000G"
QV_DIR="qv_sample_counts_sample_1000G"   # kept for compatibility (unused here unless you add steps)

mkdir -p "$FASTQ_DIR" "$COUNTS_DIR" "$QV_DIR"

shopt -s nullglob

for cram in "$CRAM_DIR"/*.cram; do
  (
    base="$(basename "$cram")"   # e.g. NA18983.final.cram
    id="${base%%.*}"             # -> NA18983

    out1="$FASTQ_DIR/${id}_R1.fastq.gz"
    out2="$FASTQ_DIR/${id}_R2.fastq.gz"
    yak_out="$COUNTS_DIR/${id}_counts.pb.yak"

    # 1) Ensure CRAM index exists (preserve CRAM and CRAI)
    if [[ ! -s "${cram}.crai" ]]; then
      echo "[INDEX] Building index for $base"
      samtools index -@ "$SAMTOOLS_THREADS" -c "$cram"
    fi

    # 2) Convert CRAM -> paired FASTQ (only if needed)
    if [[ ! -s "$out1" || ! -s "$out2" ]]; then
      echo "[FASTQ] Generating FASTQ for $id"
      tmp1="${out1}.tmp"
      tmp2="${out2}.tmp"

      # Clean temp files if anything fails in this subshell
      trap 'rm -f "$tmp1" "$tmp2"' EXIT

      samtools fastq -@ "$SAMTOOLS_THREADS" \
        --reference="$REF" \
        -1 "$tmp1" \
        -2 "$tmp2" \
        -s /dev/null \
        -0 /dev/null \
        "$cram"

      mv "$tmp1" "$out1"
      mv "$tmp2" "$out2"

      trap - EXIT
    else
      echo "[FASTQ] $id: FASTQs already exist, will reuse them."
    fi

    # 3) Run yak count
    if [[ -s "$yak_out" ]]; then
      echo "[YAK] $id: output exists ($yak_out). Skipping yak count."
    else
      echo "[YAK] Counting k-mers for $id"
      "$YAK_BIN" count -b37 -t"$YAK_THREADS" \
        -o "$yak_out" \
        <(zcat "$out1") <(zcat "$out2")
      echo "[YAK] Done: $yak_out"
    fi

    # 4) Remove intermediate FASTQ.gz only after yak output is present
    if [[ -s "$yak_out" ]]; then
      echo "[CLEAN] Removing intermediates for $id: $out1 $out2"
      rm -f "$out1" "$out2"
    else
      echo "[CLEAN] Not removing FASTQs for $id because yak output is missing/empty."
    fi

    echo "[DONE] $id"
  )
done

echo "All samples processed."
