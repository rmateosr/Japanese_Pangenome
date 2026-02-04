#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./make_fai_only.sh /path/to/hap_manifest.tsv
#
# Input manifest (TSV) columns:
#   1: ID
#   2: fasta1_path  (plain FASTA or .gz/.bgz; .gz may be gzip or BGZF)
#   3: fasta2_path  (plain FASTA or .gz/.bgz; .gz may be gzip or BGZF)
#   4+: ignored
#
# Output (ONLY .fai files are kept):
#   OUTDIR/<ID>/hap1.fai
#   OUTDIR/<ID>/hap2.fai
#   OUTDIR/fai_manifest.tsv
#
# Notes:
# - If input is plain FASTA: index via symlink in OUTDIR, keep only .fai.
# - If input is .gz/.bgz and already BGZF: index via symlink, keep only .fai.
# - If input is .gz but NOT BGZF: convert stream -> BGZF temp, index temp, keep only .fai, delete temp + .gzi.

IN_MANIFEST="${1:?ERROR: provide input manifest TSV as arg1}"

OUTDIR="${OUTDIR:-/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/fai}"
OUT_MANIFEST="${OUT_MANIFEST:-${OUTDIR}/fai_manifest.tsv}"

# Tools
command -v samtools >/dev/null 2>&1 || { echo "ERROR: samtools not found in PATH" >&2; exit 1; }
command -v bgzip   >/dev/null 2>&1 || { echo "ERROR: bgzip not found in PATH (htslib)" >&2; exit 1; }
command -v gzip    >/dev/null 2>&1 || { echo "ERROR: gzip not found in PATH" >&2; exit 1; }

[[ -f "$IN_MANIFEST" ]] || { echo "ERROR: input manifest not found: $IN_MANIFEST" >&2; exit 1; }

mkdir -p "$OUTDIR"
printf "sample\thap\torig_fasta\tfai\n" > "$OUT_MANIFEST"

# Create .fai in OUTDIR, leaving no copied FASTA behind.
make_fai_only() {
  local orig="$1"
  local sdir="$2"
  local hap="$3"

  local out_fai="${sdir}/hap${hap}.fai"

  # For direct-index path (plain FASTA or already-BGZF):
  local link="${sdir}/hap${hap}.fasta"

  # For re-BGZF path (gzip but not BGZF):
  local tmp_bgzf="${sdir}/hap${hap}.tmp.fa.gz"

  rm -f \
    "$out_fai" \
    "$link" "${link}.fai" "${link}.gzi" \
    "$tmp_bgzf" "${tmp_bgzf}.fai" "${tmp_bgzf}.gzi"

  # Compressed input: decide whether it's BGZF already.
  if [[ "$orig" =~ \.(gz|bgz)$ ]]; then
    # bgzip -t exits 0 if BGZF; non-zero if not BGZF (or unreadable)
    if bgzip -t "$orig" >/dev/null 2>&1; then
      # Already BGZF -> index directly via symlink (no temp BGZF file)
      ln -s "$orig" "$link"
      samtools faidx "$link"

      if [[ ! -f "${link}.fai" ]]; then
        echo "WARN: failed to create fai for BGZF input: $orig" >&2
        rm -f "$link"
        return 1
      fi

      mv -f "${link}.fai" "$out_fai"
      rm -f "$link"
      # ensure we do not leave any gzi from this path (unlikely here)
      rm -f "${link}.gzi"
      return 0
    fi

    # Not BGZF (plain gzip) -> convert stream to BGZF temp, index, keep only .fai
    gzip -dc "$orig" | bgzip -c > "$tmp_bgzf"
    samtools faidx "$tmp_bgzf"

    if [[ ! -f "${tmp_bgzf}.fai" ]]; then
      echo "WARN: failed to create fai after BGZF conversion: $orig" >&2
      rm -f "$tmp_bgzf" "${tmp_bgzf}.gzi"
      return 1
    fi

    mv -f "${tmp_bgzf}.fai" "$out_fai"
    rm -f "$tmp_bgzf" "${tmp_bgzf}.gzi"
    return 0
  fi

  # Plain FASTA input -> index via symlink (no temp FASTA copies)
  ln -s "$orig" "$link"
  samtools faidx "$link"

  if [[ ! -f "${link}.fai" ]]; then
    echo "WARN: failed to create fai for plain FASTA input: $orig" >&2
    rm -f "$link"
    return 1
  fi

  mv -f "${link}.fai" "$out_fai"
  rm -f "$link"
  rm -f "${link}.gzi"
  return 0
}

# Skip header; read first 3 columns, ignore rest
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

    if make_fai_only "$orig" "$sdir" "$hap"; then
      printf "%s\thap%s\t%s\t%s\n" "$sample" "$hap" "$orig" "${sdir}/hap${hap}.fai" >> "$OUT_MANIFEST"
    fi
  done
done

echo "Done."
echo "FAI root: $OUTDIR"
echo "Output manifest: $OUT_MANIFEST"
