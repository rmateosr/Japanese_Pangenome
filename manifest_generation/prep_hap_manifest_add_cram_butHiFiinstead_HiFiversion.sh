#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# Build hap_manifestPacBio.tsv by joining:
#  - hap_manifest.tsv (ID, fasta1_path, fasta2_path, cram)   [TAB-separated]
#  - visc_fastq_table.txt (type, sample, path)               [space or tab]
#
# Output columns:
#   ID  fasta1_path  fasta2_path  HiFi_paths
#
# HiFi_paths: comma-joined list of all "HiFi" paths for that sample ID.
# The CRAM column is dropped.
###############################################################################

MANIFEST_IN="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest.tsv"
TABLE="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/visc_fastq_table.txt"
OUT="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifestPacBio.tsv"

mkdir -p "$(dirname "$OUT")"

awk -v OFS='\t' '
  ###########################################################################
  # PASS 1: read fastq table and build hifi[sample] = path1,path2,...
  # Table may be space- or tab-delimited, so split on whitespace.
  ###########################################################################
  FNR==NR {
    if ($0 ~ /^[[:space:]]*$/) next

    # split by whitespace to tolerate spaces/tabs
    n = split($0, a, /[[:space:]]+/)

    # Expect: type sample path (path has no spaces in your data)
    # Skip header if present
    if (n >= 3 && a[1] == "type" && a[2] == "sample") next

    type  = a[1]
    sample= a[2]
    path  = a[3]

    if (type == "HiFi") {
      if (sample != "" && path != "") {
        if (hifi[sample] == "") hifi[sample] = path
        else hifi[sample] = hifi[sample] "," path
      }
    }
    next
  }

  ###########################################################################
  # PASS 2: process manifest (tab-separated) and write output
  ###########################################################################
  FNR==1 {
    # Always write our own header (drops cram, adds HiFi_paths)
    print "ID","fasta1_path","fasta2_path","HiFi_paths"
    next
  }

  {
    id = $1
    f1 = $2
    f2 = $3
    print id, f1, f2, (id in hifi ? hifi[id] : "")
  }
' "$TABLE" "$MANIFEST_IN" > "$OUT"

# Optional sanity checks (lightweight)
{
  echo "[OK] Wrote: $OUT"
  echo "[INFO] Header:"
  head -n 1 "$OUT"
  echo "[INFO] Rows with HiFi paths: $(awk -F'\t' 'NR>1 && $4!=""{c++} END{print c+0}' "$OUT")"
  echo "[INFO] Total rows (excluding header): $(awk 'NR>1{c++} END{print c+0}' "$OUT")"
} >&2
