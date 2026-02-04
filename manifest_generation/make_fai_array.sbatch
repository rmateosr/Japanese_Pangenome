#!/usr/bin/env bash
#SBATCH --time=02:00:00
set -euo pipefail

: "${MANIFEST:?MANIFEST env var is required (set by submit wrapper)}"
: "${OUTDIR:?OUTDIR env var is required (set by submit wrapper)}"

command -v samtools >/dev/null 2>&1 || { echo "ERROR: samtools not found in PATH" >&2; exit 1; }
command -v bgzip   >/dev/null 2>&1 || { echo "ERROR: bgzip not found in PATH (htslib)" >&2; exit 1; }
command -v gzip    >/dev/null 2>&1 || { echo "ERROR: gzip not found in PATH" >&2; exit 1; }

TASK_ID="${SLURM_ARRAY_TASK_ID:?SLURM_ARRAY_TASK_ID not set}"
THREADS="${SLURM_CPUS_PER_TASK:-1}"

# Prefer node-local scratch for temp BGZF
SCRATCH="${SLURM_TMPDIR:-${TMPDIR:-/tmp}}"

# Pull the TASK_ID-th data line (skip header => +1)
line_no=$((TASK_ID + 1))
line="$(sed -n "${line_no}p" "$MANIFEST" || true)"
[[ -n "$line" ]] || { echo "ERROR: no line for task $TASK_ID (line $line_no) in $MANIFEST" >&2; exit 1; }

# Expect: ID \t fasta1_path \t fasta2_path \t cram ...
IFS=$'\t' read -r sample hap1 hap2 cram rest <<< "$line"

if [[ -z "${sample:-}" || "$sample" == "ID" ]]; then
  echo "ERROR: parsed empty/invalid sample for task $TASK_ID from: $line" >&2
  exit 1
fi

sdir="$OUTDIR/$sample"
mkdir -p "$sdir"

# Build .fai only, leaving no copied FASTA/BGZF behind in OUTDIR.
# For compressed input:
#   1) Attempt direct indexing via symlink (works if BGZF)
#   2) If it fails (e.g. plain gzip), convert to BGZF temp in scratch and index that
make_fai_only_one() {
  local orig="$1"
  local hap="$2"

  local out_fai="${sdir}/hap${hap}.fai"
  local link="${sdir}/hap${hap}.fasta"

  # scratch temp names (unique per task + hap)
  local tmp_bgzf="${SCRATCH}/${sample}.hap${hap}.tmp.fa.gz"
  local tmp_fai="${tmp_bgzf}.fai"
  local tmp_gzi="${tmp_bgzf}.gzi"

  local errlog="${SCRATCH}/${sample}.hap${hap}.faidx.stderr"

  rm -f "$out_fai" "$link" "${link}.fai" "${link}.gzi" \
        "$tmp_bgzf" "$tmp_fai" "$tmp_gzi" "$errlog"

  # If input is compressed, try direct indexing first (BGZF works), then fallback.
  if [[ "$orig" =~ \.(gz|bgz)$ ]]; then
    ln -s "$orig" "$link"
    set +e
    samtools faidx "$link" 2> "$errlog"
    rc=$?
    set -e

    if [[ $rc -eq 0 && -f "${link}.fai" ]]; then
      mv -f "${link}.fai" "$out_fai"
      rm -f "$link" "${link}.gzi" "$errlog"
      return 0
    fi

    # Direct indexing failed -> most commonly plain gzip.
    # Cleanup symlink attempt and fallback to BGZF conversion.
    rm -f "$link" "${link}.fai" "${link}.gzi"

    gzip -dc "$orig" | bgzip -@ "$THREADS" -c > "$tmp_bgzf"
    samtools faidx "$tmp_bgzf"

    if [[ ! -f "$tmp_fai" ]]; then
      echo "WARN: failed to create fai after conversion for sample=$sample hap$hap: $orig" >&2
      if [[ -s "$errlog" ]]; then
        echo "---- samtools faidx stderr (direct attempt) ----" >&2
        cat "$errlog" >&2
        echo "---------------------------------------------" >&2
      fi
      rm -f "$tmp_bgzf" "$tmp_gzi" "$errlog"
      return 1
    fi

    mv -f "$tmp_fai" "$out_fai"
    rm -f "$tmp_bgzf" "$tmp_gzi" "$errlog"
    return 0
  fi

  # Plain FASTA input -> index via symlink (no temp FASTA copies)
  ln -s "$orig" "$link"
  samtools faidx "$link"

  if [[ ! -f "${link}.fai" ]]; then
    echo "WARN: failed to create fai for sample=$sample hap$hap: $orig" >&2
    rm -f "$link"
    return 1
  fi

  mv -f "${link}.fai" "$out_fai"
  rm -f "$link" "${link}.gzi"
  return 0
}

# Process hap1/hap2
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

  make_fai_only_one "$orig" "$hap"
done

echo "OK: $sample -> $(ls -1 "$sdir"/hap*.fai 2>/dev/null || true)"
