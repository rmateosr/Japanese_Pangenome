#!/usr/bin/env bash
#SBATCH -J prep_hap_manifest
#SBATCH -t 01:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH -o /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies/logs/%x_%j.out
#SBATCH -e /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies/logs/%x_%j.err

set -euo pipefail

# ---- NEW: input results root ----
# Expected per-sample files like:
# /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Polishing_test/Mitochondria_removal/NA19081/NA19081.hap2.noMT.fa.gz
RESULTS_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Polishing_test/Mitochondria_removal"

OUT_DIR="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest"
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

  # Expected files:
  #   $d/${ID}.hap1.noMT.fa.gz
  #   $d/${ID}.hap2.noMT.fa.gz
  fasta1="$d/${s}.hap1.noMT.fa.gz"
  fasta2="$d/${s}.hap2.noMT.fa.gz"

  if [[ -s "$fasta1" && -s "$fasta2" ]]; then
    printf "%s\t%s\t%s\n" "$s" "$fasta1" "$fasta2" >> "$manifest_tmp"
  else
    miss=""
    [[ ! -s "$fasta1" ]] && miss="${miss}fasta1 "
    [[ ! -s "$fasta2" ]] && miss="${miss}fasta2 "
    printf "%s\tmissing:%s\n" "$s" "${miss% }" >> "$missing_tmp"
  fi
done

# ---- Output names now use "ChrM_removed" ----
MANIFEST="${OUT_DIR}/hap_manifest_ChrM_removed.tsv"
SAMPLES_OUT="${OUT_DIR}/samples_ChrM_removed.txt"
MISSING_OUT="${OUT_DIR}/missing_haps_ChrM_removed.tsv"

# Write header + sorted data rows
{
  printf "ID\tfasta1_path\tfasta2_path\n"
  sort -t $'\t' -k1,1V "$manifest_tmp"
} > "$MANIFEST"

# Convenience outputs
tail -n +2 "$MANIFEST" | cut -f1 > "$SAMPLES_OUT"
sort -V "$missing_tmp" > "$MISSING_OUT"

rm -f "$manifest_tmp" "$missing_tmp"

echo "Wrote:"
echo "  $MANIFEST (3 columns: ID, fasta1_path, fasta2_path)"
echo "  $SAMPLES_OUT"
echo "  $MISSING_OUT"
echo
echo "Samples with both FASTAs: $(( $(wc -l < "$MANIFEST") - 1 ))"
