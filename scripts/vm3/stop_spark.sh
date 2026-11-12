#!/usr/bin/env bash
# RUN ON: VM3 (Gagan R) - stops Spark worker + master (also ones started from another Spark install).
source "$(dirname "$0")/../lib/common.sh"
"$SPARK_HOME/sbin/stop-worker.sh" || true
"$SPARK_HOME/sbin/stop-master.sh" || true
for cls in org.apache.spark.deploy.worker.Worker org.apache.spark.deploy.master.Master; do
  if pgrep -f "$cls" >/dev/null; then
    pkill -f "$cls" 2>/dev/null || true
    sleep 3
    pgrep -f "$cls" >/dev/null && pkill -9 -f "$cls" 2>/dev/null || true
  fi
  if pgrep -f "$cls" >/dev/null; then
    warn "$cls is still running (started by another user, e.g. root). Stop it with:  sudo pkill -f $cls"
  fi
done
ok "Spark stopped"
