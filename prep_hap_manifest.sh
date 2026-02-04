#!/usr/bin/env bash
#SBATCH -J prep_hap_manifest
#SBATCH -t 01:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH -o /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies/logs/%x_%j.out
#SBATCH -e /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies/logs/%x_%j.err

set -euo pipefail

RESULTS_DIR="/lustre9/open/shared_data/visc/data/hpp_workflow/results"
OUT_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/"
LOG_DIR="${OUT_DIR}/logs"

EXCLUDE_RE='^(NA18939_test|NA18939_test2|NA18943_test)$'

mkdir -p "$OUT_DIR" "$LOG_DIR"

manifest_tmp="$(mktemp)"
missing_tmp="$(mktemp)"
: > "$manifest_tmp"
: > "$missing_tmp"

# Build data rows: ID \t fasta1_path \t fasta2_path
for d in "$RESULTS_DIR"/NA[0-9]*; do
  [[ -d "$d" ]] || continue
  s="$(basename "$d")"

  [[ "$s" =~ $EXCLUDE_RE ]] && continue

  fasta1="$(find "$d" -type f -name "${s}.hap1.fa.gz" -print -quit 2>/dev/null || true)"
  fasta2="$(find "$d" -type f -name "${s}.hap2.fa.gz" -print -quit 2>/dev/null || true)"

  if [[ -n "$fasta1" && -n "$fasta2" ]]; then
    printf "%s\t%s\t%s\n" "$s" "$fasta1" "$fasta2" >> "$manifest_tmp"
  else
    miss=""
    [[ -z "$fasta1" ]] && miss="${miss}fasta1 "
    [[ -z "$fasta2" ]] && miss="${miss}fasta2 "
    printf "%s\tmissing:%s\n" "$s" "${miss% }" >> "$missing_tmp"
  fi
done

MANIFEST="${OUT_DIR}/hap_manifest.tsv"

# Write header + sorted data rows
{
  printf "ID\tfasta1_path\tfasta2_path\n"
  sort -t $'\t' -k1,1V "$manifest_tmp"
} > "$MANIFEST"

# Convenience outputs
tail -n +2 "$MANIFEST" | cut -f1 > "${OUT_DIR}/samples.txt"
sort -V "$missing_tmp" > "${OUT_DIR}/missing_haps.tsv"

rm -f "$manifest_tmp" "$missing_tmp"

echo "Wrote:"
echo "  $MANIFEST (3 columns: ID, fasta1_path, fasta2_path)"
echo "  ${OUT_DIR}/samples.txt"
echo "  ${OUT_DIR}/missing_haps.tsv"
echo
echo "Samples with both FASTAs: $(( $(wc -l < "$MANIFEST") - 1 ))"
