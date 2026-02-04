#!/usr/bin/env bash
#SBATCH --job-name=concat_haps
#SBATCH --output=log/%x.%A.%a.out
#SBATCH --error=log/%x.%A.%a.err

set -euo pipefail

: "${MANIFEST:?MANIFEST not set}"
: "${CONCAT_DIR:?CONCAT_DIR not set}"
: "${CONCAT_SUFFIX:?CONCAT_SUFFIX not set}"

threads="${SLURM_CPUS_PER_TASK:-1}"
mkdir -p "$CONCAT_DIR" log

# Read row by array index, but map columns by header name.
row="$(
  awk -v task="$SLURM_ARRAY_TASK_ID" 'BEGIN{FS=OFS="\t"}
    NR==1{
      for(i=1;i<=NF;i++) col[$i]=i
      if(!("ID" in col) || !("fasta1_path" in col) || !("fasta2_path" in col)){
        print "ERROR: manifest header must contain: ID, fasta1_path, fasta2_path" > "/dev/stderr"
        exit 20
      }
      next
    }
    (NR-1)==task{
      print $col["ID"], $col["fasta1_path"], $col["fasta2_path"]
      exit
    }' "$MANIFEST"
)"

if [[ -z "$row" ]]; then
  echo "ERROR: could not read manifest row for SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID" >&2
  exit 2
fi

IFS=$'\t' read -r ID hap1_fa hap2_fa <<< "$row"

if [[ -z "${ID:-}" ]]; then
  echo "ERROR: empty ID for task=$SLURM_ARRAY_TASK_ID" >&2
  exit 3
fi

out_fa="${CONCAT_DIR}/${ID}${CONCAT_SUFFIX}"
out_fai="${out_fa}.fai"

# Idempotency
if [[ -s "$out_fa" && -s "$out_fai" ]]; then
  echo "SKIP: concatenated FASTA exists for ID=$ID"
  exit 0
fi

# Validate hap assemblies
for asm in "$hap1_fa" "$hap2_fa"; do
  if [[ -z "${asm:-}" || "$asm" == "NA" || "$asm" == "." ]]; then
    echo "ERROR: missing assembly path for ID=$ID (got: $hap1_fa , $hap2_fa)" >&2
    exit 4
  fi
  if [[ ! -f "$asm" ]]; then
    echo "ERROR: assembly not found for ID=$ID: $asm" >&2
    exit 5
  fi
done

tmp="${out_fa}.tmp"
rm -f "$tmp" "$out_fai"

stream_with_marker() {
  local fasta="$1"
  local marker="$2"  # "#1#" or "#2#"

  if [[ "$fasta" == *.gz ]]; then
    zcat "$fasta"
  else
    cat "$fasta"
  fi | awk -v m="$marker" '
    /^>/{
      h=substr($0,2)
      if (index(h,"#1#")>0 || index(h,"#2#")>0) { print ">"h; next }
      print ">"h m
      next
    }
    { print }
  '
}

echo "ID=$ID"
echo "hap1_fa=$hap1_fa"
echo "hap2_fa=$hap2_fa"
echo "out_fa=$out_fa"

# Requires bgzip and samtools on PATH
(
  stream_with_marker "$hap1_fa" "#1#"
  stream_with_marker "$hap2_fa" "#2#"
) | bgzip -@ "$threads" -c > "$tmp"

mv -f "$tmp" "$out_fa"
samtools faidx "$out_fa"

if [[ ! -s "$out_fa" || ! -s "$out_fai" ]]; then
  echo "ERROR: failed to create concatenated FASTA or index for ID=$ID" >&2
  exit 6
fi

echo "DONE: $out_fa"
