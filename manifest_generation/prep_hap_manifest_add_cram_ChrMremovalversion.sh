#!/usr/bin/env bash
set -euo pipefail

# Manifest produced by the previous step (ChrM removal context)
MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest_ChrM_removed.tsv"

# 1000G high coverage CRAMs (keep as-is unless you changed your CRAM source)
S3_ROOT="s3://1000genomes/1000G_2504_high_coverage/data"

# 1 = strictest matching (prefer "<ID>.final.cram", then any CRAM containing the ID in path)
STRICT_MATCH=1

AWS_LS_FLAGS=(--no-sign-request --recursive)

tmp_out="$(mktemp)"
tmp_log="$(mktemp)"
tmp_crams="$(mktemp)"
cleanup() { rm -f "$tmp_out" "$tmp_crams"; }  # keep tmp_log until we move it at the end
trap cleanup EXIT

if [[ ! -f "$MANIFEST" ]]; then
  echo "ERROR: MANIFEST not found: $MANIFEST" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: aws CLI not found in PATH." >&2
  exit 1
fi

# Derive bucket + prefix so we can safely de-duplicate keys returned by `aws s3 ls --recursive`
S3_BUCKET="${S3_ROOT#s3://}"
S3_BUCKET="${S3_BUCKET%%/*}"
S3_PREFIX="${S3_ROOT#s3://${S3_BUCKET}/}"

header="$(head -n 1 "$MANIFEST")"

# If header already includes "cram", do nothing
has_cram_col=0
IFS=$'\t' read -r -a hdr_fields <<< "$header"
for f in "${hdr_fields[@]}"; do
  if [[ "$f" == "cram" ]]; then
    has_cram_col=1
    break
  fi
done

if [[ "$has_cram_col" -eq 1 ]]; then
  echo "INFO: 'cram' column already exists in header. No changes made."
  exit 0
fi

escape_regex() {
  # Escape ERE metacharacters so IDs are safe in grep -E
  sed 's/[][(){}.^$*+?|\\]/\\&/g' <<<"$1"
}

normalize_key_to_uri() {
  # Input: key as returned by `aws s3 ls` ($4) (bucket key, usually includes our prefix)
  # Output: fully qualified s3:// URI rooted at S3_ROOT without duplicated prefix
  local key="$1"

  if [[ -z "$key" || "$key" == "NA" ]]; then
    printf "NA\n"
    return 0
  fi

  # If it is already a URI, just return it
  if [[ "$key" == s3://* ]]; then
    printf "%s\n" "$key"
    return 0
  fi

  # If aws returned a key that already contains our prefix, strip it
  if [[ "$key" == "${S3_PREFIX}/"* ]]; then
    key="${key#${S3_PREFIX}/}"
  fi

  printf "%s/%s\n" "$S3_ROOT" "$key"
}

# Build a one-time CRAM key index (avoid re-running recursive listing per sample)
echo "INFO: Indexing CRAM keys under ${S3_ROOT} ..."
if ! aws s3 ls "${S3_ROOT}/" "${AWS_LS_FLAGS[@]}" 2>>"$tmp_log" \
  | awk '{print $4}' \
  | grep -E '\.cram$' > "$tmp_crams"; then
  echo "ERROR: Failed to list CRAMs from S3_ROOT: ${S3_ROOT}" >&2
  echo "See log for details: $tmp_log" >&2
  exit 1
fi

# Find best CRAM match for an ID from the pre-built index
find_cram_uri() {
  local id="$1"
  local id_re key

  id_re="$(escape_regex "$id")"

  if [[ "$STRICT_MATCH" -eq 1 ]]; then
    # Prefer exact-ish filename match: ".../<ID>.final.cram"
    key="$(
      grep -E "(^|/)${id_re}\.final\.cram$" "$tmp_crams" | head -n 1 || true
    )"

    # Fallback: any CRAM whose path contains the ID
    if [[ -z "$key" ]]; then
      key="$(
        grep -E "(^|/)${id_re}([^/]*|/).*\.cram$" "$tmp_crams" | head -n 1 || true
      )"
    fi
  else
    key="$(
      grep -F "$id" "$tmp_crams" | head -n 1 || true
    )"
  fi

  if [[ -n "$key" ]]; then
    normalize_key_to_uri "$key"
    return 0
  fi

  printf "NA\n"
  return 0
}

# Write new header
printf "%s\tcram\n" "$header" > "$tmp_out"

# Process rows (no subshell)
while IFS=$'\t' read -r id fasta1 fasta2 rest; do
  [[ -z "${id:-}" ]] && continue

  orig_line="${id}"$'\t'"${fasta1}"$'\t'"${fasta2}"
  if [[ -n "${rest:-}" ]]; then
    orig_line+=$'\t'"${rest}"
  fi

  cram_uri="$(find_cram_uri "$id")"
  printf "%s\t%s\n" "$orig_line" "$cram_uri" >> "$tmp_out"

  if [[ "$cram_uri" == "NA" ]]; then
    echo "WARN: No CRAM found for ${id}" >> "$tmp_log"
  else
    echo "INFO: ${id} -> ${cram_uri}" >> "$tmp_log"
  fi
done < <(tail -n +2 "$MANIFEST")

# Backup + replace manifest atomically
cp -a "$MANIFEST" "${MANIFEST}.bak.$(date +%Y%m%dT%H%M%S)"
mv "$tmp_out" "$MANIFEST"

# Persist the log next to the manifest
log_path="${MANIFEST}.cram_add.$(date +%Y%m%dT%H%M%S).log"
mv "$tmp_log" "$log_path"

echo "DONE: Updated manifest written to: $MANIFEST"
echo "LOG:  Details in: $log_path"
echo "NOTE: Backup created at: ${MANIFEST}.bak.*"
