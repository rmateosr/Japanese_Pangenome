#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash fix_permissions_by_type.sh /path/to/scripts
# If no argument is provided, uses current directory.
TARGET_DIR="${1:-.}"

if [[ ! -d "$TARGET_DIR" ]]; then
  echo "ERROR: Not a directory: $TARGET_DIR" >&2
  exit 1
fi

cd "$TARGET_DIR"

echo "Target directory: $(pwd)"
echo

# Helper: print perms in a compact way
perm_line() {
  # Linux stat format; should work on your cluster
  stat -c '%a %U:%G %n' "$1" 2>/dev/null || true
}

# Record changes
changed=0

# 1) Ensure directories are traversable and listable by you
while IFS= read -r -d '' d; do
  before="$(perm_line "$d")"
  chmod u+rwx "$d"
  after="$(perm_line "$d")"
  if [[ "$before" != "$after" ]]; then
    echo "DIR  : $before  ->  $after"
    changed=$((changed+1))
  fi
done < <(find . -type d -print0)

# 2) Script-like files: make readable + executable by you
# Add/remove patterns here if you use other extensions.
script_patterns=(
  -name '*.sh'
  -o -name '*.bash'
  -o -name '*.sbatch'
)

while IFS= read -r -d '' f; do
  before="$(perm_line "$f")"
  chmod u+rwx "$f"
  after="$(perm_line "$f")"
  if [[ "$before" != "$after" ]]; then
    echo "EXEC : $before  ->  $after"
    changed=$((changed+1))
  fi
done < <(find . -type f \( "${script_patterns[@]}" \) -print0)

# 3) Data/text files: ensure readable+writable by you, remove execute bit
data_patterns=(
  -name '*.tsv'
  -o -name '*.txt'
  -o -name '*.csv'
  -o -name '*.bed'
  -o -name '*.fa'   -o -name '*.fna' -o -name '*.fasta'
  -o -name '*.gz'
  -o -name '*.json'
  -o -name '*.yaml' -o -name '*.yml'
  -o -name '*.log'  -o -name '*.out' -o -name '*.err'
)

while IFS= read -r -d '' f; do
  before="$(perm_line "$f")"
  chmod u+rw,u-x "$f"
  after="$(perm_line "$f")"
  if [[ "$before" != "$after" ]]; then
    echo "DATA : $before  ->  $after"
    changed=$((changed+1))
  fi
done < <(find . -type f \( "${data_patterns[@]}" \) -print0)

echo
echo "Done. Items changed: $changed"
echo
echo "Sanity checks (should now work):"
echo "  file quast_oneforall_CHM13_script.sh quast_oneforall_GRCh38_script.sh quast_command.sh"
echo "  bash -n quast_oneforall_CHM13_script.sh"

