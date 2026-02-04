#!/usr/bin/env bash
set -euo pipefail

# Polished manifest produced by the previous script
MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest_polished.tsv"
S3_ROOT="s3://1000genomes/1000G_2504_high_coverage/data"

# If you want the strictest matching, keep this as 1.
STRICT_MATCH=1

AWS_LS_FLAGS=(--no-sign-request --recursive)

tmp_out="$(mktemp)"
tmp_log="$(mktemp)"
cleanup() { rm -f "$tmp_out"; }   # keep tmp_log until we move it to a stable path at the end
trap cleanup EXIT

if [[ ! -f "$MANIFEST" ]]; then
  echo "ERROR: MANIFEST not found: $MANIFEST" >&2
  exit 1
fi

# Derive bucket + prefix so we can safely de-duplicate keys returned by `aws s3 ls --recursive`
S3_BUCKET="${S3_ROOT#s3://}"
S3_BUCKET="${S3_BUCKET%%/*}"
S3_PREFIX="${S3_ROOT#s3://${S3_BUCKET}/}"

header="$(head -n 1 "$MANIFEST")"

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
  # Escape ERE metacharacters so IDs like "NA18939" are safe in grep -E
  sed 's/[][(){}.^$*+?|\\]/\\&/g' <<<"$1"
}

normalize_key_to_uri() {
  # Input: key as returned by `aws s3 ls` ($4), which may already include the prefix
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

find_cram_uri() {
  local id="$1"
  local key=""
  local id_re
  id_re="$(escape_regex "$id")"

  # Try a targeted prefix first (only helps if the layout is .../data/<ID>/...)
  if aws s3 ls "${S3_ROOT}/${id}/" "${AWS_LS_FLAGS[@]}" >/dev/null 2>&1; then
    key="$(
      aws s3 ls "${S3_ROOT}/${id}/" "${AWS_LS_FLAGS[@]}" \
        | awk '{print $4}' \
        | grep -E '\.cram$' \
        | head -n 1
    )"
    if [[ -n "$key" ]]; then
      normalize_key_to_uri "$key"
      return 0
    fi
  fi

  # Fallback: recursive list from root (can be slow if repeated many times)
  if [[ "$STRICT_MATCH" -eq 1 ]]; then
    # Prefer exact-ish filename matches first (common in 1000G: NAxxxxx.final.cram)
    key="$(
      aws s3 ls "${S3_ROOT}/" "${AWS_LS_FLAGS[@]}" \
        | awk '{print $4}' \
        | grep -E "(^|/)${id_re}\.final\.cram$" \
        | head -n 1
    )"
    # If not found, allow any CRAM containing the ID somewhere in the path
    if [[ -z "$key" ]]; then
      key="$(
        aws s3 ls "${S3_ROOT}/" "${AWS_LS_FLAGS[@]}" \
          | awk '{print $4}' \
          | grep -E "(^|/)${id_re}([^/]*|/).*\.cram$" \
          | head -n 1
      )"
    fi
  else
    key="$(
      aws s3 ls "${S3_ROOT}/" "${AWS_LS_FLAGS[@]}" \
        | awk '{print $4}' \
        | grep -F "$id" \
        | grep -E '\.cram$' \
        | head -n 1
    )"
  fi

  if [[ -n "$key" ]]; then
    normalize_key_to_uri "$key"
    return 0
  fi

  printf "NA\n"
  return 0
}

printf "%s\tcram\n" "$header" > "$tmp_out"

# Avoid subshell so the script behaves predictably
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

cp -a "$MANIFEST" "${MANIFEST}.bak.$(date +%Y%m%dT%H%M%S)"
mv "$tmp_out" "$MANIFEST"

# Persist the log next to the manifest
log_path="${MANIFEST}.cram_add.$(date +%Y%m%dT%H%M%S).log"
mv "$tmp_log" "$log_path"

echo "DONE: Updated manifest written to: $MANIFEST"
echo "LOG:  Warnings/info in: $log_path"
echo "NOTE: Backup created at: ${MANIFEST}.bak.*"
