#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM2 (Anjanaa) - ONCE, right after the final Day 2 run is verified.
#  Writes /clickpulse/_control/FINAL_RUN_DONE on HDFS. From then on
#  reset_for_rehearsal.sh refuses to run, so nobody can wipe the final data by accident.
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
expect_vm 2

MARK="$HDFS_ROOT/_control/FINAL_RUN_DONE"
if hdfs dfs -test -e "$MARK"; then
  hdfs dfs -cat "$MARK" | sed 's/^/  /'; ok "Final run is already locked"; exit 0
fi
for b in day1 day2; do
  hdfs dfs -test -e "$HDFS_ROOT/_control/runs/batch_id=$b/latest.json" \
    || die "No completed Spark run for $b yet - run the pipeline for day1 and day2 first."
  hdfs dfs -test -e "$HDFS_ROOT/raw/batch_id=$b/_MANIFEST.json" \
    || die "Raw data for $b is not on HDFS - run the pipeline for day1 and day2 first."
done
hdfs dfs -test -e "$HDFS_ROOT/_control/druid_synced.json" || die "Druid was never synced - run the pipeline first."

printf 'final run locked at %s by %s on %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')" "$USER" "$(hostname)" \
  | hdfs dfs -put - "$MARK"
hdfs dfs -cat "$MARK" | sed 's/^/  /'
ok "Final run locked. reset_for_rehearsal.sh will now refuse to run."
