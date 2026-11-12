#!/usr/bin/env bash
# =============================================================================
#  DEMO - RUN ON: VM2  (Anjanaa)
#  Proves "raw data is preserved" and shows the rejects location in HDFS.
#     demo/11_vm2_show_raw_and_rejects.sh            # all batches
#     demo/11_vm2_show_raw_and_rejects.sh day2       # one batch
# =============================================================================
source "$(dirname "$0")/../scripts/lib/common.sh"
expect_vm 2
ONLY="${1:-}"
[[ -n "$ONLY" ]] && validate_batch "$ONLY"

banner "HDFS layout under ${HDFS_URI}${HDFS_ROOT}"
run hdfs dfs -ls "$HDFS_ROOT"
run hdfs dfs -ls -R "$HDFS_ROOT/raw" "$HDFS_ROOT/rejects"
pause

for dir in $(hdfs dfs -ls "$HDFS_ROOT/raw" 2>/dev/null | grep -oE 'batch_id=[a-z0-9_]+$'); do
  b="${dir#batch_id=}"
  [[ -n "$ONLY" && "$b" != "$ONLY" ]] && continue
  src="$(batch_file "$b")"
  banner "Batch $b - raw file is byte-for-byte identical to the source"
  manifest="$(hdfs dfs -cat "$HDFS_ROOT/raw/$dir/_MANIFEST.json")"
  echo "$manifest" | sed 's/^/  /'
  f="$(echo "$manifest" | python3 -c 'import json,sys; print(json.load(sys.stdin)["source_file"])')"
  hdfs_sha="$(hdfs dfs -cat "$HDFS_ROOT/raw/$dir/$f" | sha256sum | cut -d' ' -f1)"
  hdfs_lines="$(hdfs dfs -cat "$HDFS_ROOT/raw/$dir/$f" | wc -l)"
  src_sha="$(sha256sum "$src" | cut -d' ' -f1)"
  src_lines="$(wc -l < "$src")"
  printf "\n  %-28s lines=%-5s sha256=%s\n" "source data/$(basename "$src")" "$src_lines" "$src_sha"
  printf "  %-28s lines=%-5s sha256=%s\n" "HDFS raw/$dir/$f" "$hdfs_lines" "$hdfs_sha"
  if [[ "$hdfs_sha" == "$src_sha" ]]; then ok "identical - duplicates, broken JSON and invalid rows are all preserved"; else err "DIFFERENT"; fi
  echo
  echo "  examples of preserved 'bad' lines in HDFS raw:"
  hdfs dfs -cat "$HDFS_ROOT/raw/$dir/$f" | grep -n -E '"yesterday"|"clickk"|":-[0-9]|[^}]$' | head -6 | cut -c1-150 | sed 's/^/    line /'
  pause

  banner "Batch $b - rejects stored separately in HDFS (${HDFS_ROOT}/rejects/batch_id=$b)"
  hdfs dfs -cat "$HDFS_ROOT/rejects/batch_id=$b/*.json" 2>/dev/null | python3 -c "$(cat <<'PY'
import json, sys
from collections import Counter
rows = [json.loads(l) for l in sys.stdin if l.strip()]
print("  %d rejected lines (every occurrence counted)\n" % len(rows))
for r in rows:
    print("  line %4s  %-20s %-16s %s" % (r["line_no"], r["primary_reason"], r.get("event_id"), r["raw_line"][:70]))
print()
for k, v in sorted(Counter(r["primary_reason"] for r in rows).items()):
    print("  %-22s %d" % (k, v))
PY
)"
  pause
done
