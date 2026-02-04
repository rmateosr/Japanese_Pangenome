#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./make_fai_only.sh /path/to/hap_manifest_polished.tsv
#
# Input manifest columns (tab-delimited):
#   1: ID
#   2: fasta1_path
#   3: fasta2_path
#   4: cram (ignored)
#
# Output:
#   OUTDIR/<ID>/hap1.fai
#   OUTDIR/<ID>/hap2.fai
#   OUTDIR/fai_manifest.tsv  (records what was produced)
#
# IMPORTANT:
# - This script does NOT create any copied FASTA/BGZF files (no hap1.fa.gz, etc).
# - It avoids writing .fai next to the original FASTA by indexing a temporary symlink in OUTDIR,
#   then deleting the symlink and keeping only the .fai.

IN_MANIFEST="${1:?ERROR: provide input manifest TSV as arg1}"
OUTDIR="${OUTDIR:-/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/fai_DeepPolisher}"
OUT_MANIFEST="${OUT_MANIFEST:-${OUTDIR}/fai_manifest.tsv}"

command -v samtools >/dev/null 2>&1 || { echo "ERROR: samtools not found in PATH" >&2; exit 1; }
[[ -f "$IN_MANIFEST" ]] || { echo "ERROR: input manifest not found: $IN_MANIFEST" >&2; exit 1; }

mkdir -p "$OUTDIR"
printf "sample\thap\torig_fasta\tfai\n" > "$OUT_MANIFEST"

# Skip header; read first 3 columns, ignore the rest (cram etc)
tail -n +2 "$IN_MANIFEST" | while IFS=$'\t' read -r sample hap1 hap2 rest; do
  [[ -n "${sample:-}" ]] || continue

  sdir="$OUTDIR/$sample"
  mkdir -p "$sdir"

  for hap in 1 2; do
    orig=""
    if [[ "$hap" == "1" ]]; then
      orig="${hap1:-}"
    else
      orig="${hap2:-}"
    fi

    # Treat empty / NA as missing
    if [[ -z "$orig" || "$orig" == "NA" ]]; then
      echo "WARN: missing fasta path for sample=$sample hap$hap" >&2
      continue
    fi

    if [[ ! -e "$orig" ]]; then
      echo "WARN: fasta not found for sample=$sample hap$hap: $orig" >&2
      continue
    fi

    # Create a temporary symlink in OUTDIR, index *that*, then keep only the .fai
    link="${sdir}/hap${hap}.fasta"
    tmp_fai="${link}.fai"
    out_fai="${sdir}/hap${hap}.fai"

    rm -f "$link" "$tmp_fai" "$out_fai"

    ln -s "$orig" "$link"
    samtools faidx "$link"

    if [[ ! -f "$tmp_fai" ]]; then
      echo "WARN: samtools faidx did not produce: $tmp_fai (sample=$sample hap$hap)" >&2
      rm -f "$link"
      continue
    fi

    mv -f "$tmp_fai" "$out_fai"
    rm -f "$link"

    printf "%s\thap%s\t%s\t%s\n" "$sample" "$hap" "$orig" "$out_fai" >> "$OUT_MANIFEST"
  done
done

echo "Done."
echo "FAI root: $OUTDIR"
echo "Output manifest: $OUT_MANIFEST"
