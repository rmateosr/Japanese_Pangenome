#!/usr/bin/env bash
set -euo pipefail

TSV="${1:-haplotypes.tsv}"

OUTDIR="/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/fai"

# Tools
command -v samtools >/dev/null 2>&1 || { echo "ERROR: samtools not found in PATH" >&2; exit 1; }
command -v bgzip   >/dev/null 2>&1 || { echo "ERROR: bgzip not found in PATH (install htslib)" >&2; exit 1; }
command -v gzip    >/dev/null 2>&1 || { echo "ERROR: gzip not found in PATH" >&2; exit 1; }

[[ -f "$TSV" ]] || { echo "ERROR: TSV not found: $TSV" >&2; exit 1; }

mkdir -p "$OUTDIR"

MANIFEST="$OUTDIR/manifest.tsv"
printf "sample\thap\torig_fasta\tbgzf_fasta\tfai\tgzi\n" > "$MANIFEST"

# Convert to BGZF in output dir (always produces BGZF, regardless of input being gzip/bgzip/plain)
bgzf_copy() {
  local orig="$1"
  local dest="$2"

  if [[ ! -e "$orig" ]]; then
    echo "WARN: missing input: $orig" >&2
    return 1
  fi

  # Stream-decompress if gz, otherwise stream raw
  if [[ "$orig" =~ \.gz$ ]]; then
    gzip -dc "$orig" | bgzip -c > "$dest"
  else
    bgzip -c "$orig" > "$dest"
  fi
}

# Skip header line; expected columns: sample \t hap1_path \t hap2_path
tail -n +2 "$TSV" | while IFS=$'\t' read -r sample hap1 hap2; do
  [[ -n "${sample:-}" ]] || continue

  sdir="$OUTDIR/$sample"
  mkdir -p "$sdir"

  for hap in 1 2; do
    orig=""
    [[ "$hap" == "1" ]] && orig="${hap1:-}" || orig="${hap2:-}"

    if [[ -z "$orig" ]]; then
      echo "WARN: empty path for sample=$sample hap$hap" >&2
      continue
    fi

    bgzf_fa="$sdir/hap${hap}.fa.gz"   # this will be BGZF-compressed, extension kept as .gz
    rm -f "$bgzf_fa" "$bgzf_fa.fai" "$bgzf_fa.gzi"

    bgzf_copy "$orig" "$bgzf_fa" || continue

    # Index (creates .fai; for bgzf FASTA samtools may also create .gzi)
    samtools faidx "$bgzf_fa"

    fai="${bgzf_fa}.fai"
    gzi="${bgzf_fa}.gzi"
    if [[ -f "$gzi" ]]; then
      printf "%s\thap%s\t%s\t%s\t%s\t%s\n" "$sample" "$hap" "$orig" "$bgzf_fa" "$fai" "$gzi" >> "$MANIFEST"
    else
      printf "%s\thap%s\t%s\t%s\t%s\t\n" "$sample" "$hap" "$orig" "$bgzf_fa" "$fai" >> "$MANIFEST"
    fi
  done
done

echo "Done. Output root: $OUTDIR"
echo "Manifest: $MANIFEST"

