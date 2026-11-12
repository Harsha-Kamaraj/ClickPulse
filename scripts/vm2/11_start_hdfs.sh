#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM2  (Anjanaa)
#  Starts the HDFS NameNode + DataNode and creates the ClickPulse directories.
#  Safe to re-run.
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
require_not_root
expect_vm 2

banner "Starting HDFS on VM2 (NameNode + DataNode)"
for d in namenode datanode; do
  if pgrep -f "proc_${d}" >/dev/null; then
    info "$d already running"
  else
    run "$HADOOP_HOME/bin/hdfs" --daemon start "$d"
  fi
done
wait_for_port "$VM2_HOST" "$HDFS_PORT" 90 "NameNode RPC"
wait_for_port "$VM2_HOST" 9870 60 "NameNode web UI"

step "Waiting for safe mode to end / DataNode to register"
"$HADOOP_HOME/bin/hdfs" dfsadmin -safemode wait
for _ in $(seq 1 30); do
  live="$("$HADOOP_HOME/bin/hdfs" dfsadmin -report 2>/dev/null | grep -oE 'Live datanodes \([0-9]+\)' | grep -oE '[0-9]+' || echo 0)"
  [[ "$live" -ge 1 ]] && break
  sleep 2
done
ok "Live DataNodes: ${live:-0}"

step "Creating ClickPulse directories"
for d in raw rejects warehouse export/druid _control/runs; do
  "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "${HDFS_ROOT}/$d"
done
"$HADOOP_HOME/bin/hdfs" dfs -chmod -R 777 "$HDFS_ROOT"
"$HADOOP_HOME/bin/hdfs" dfs -ls "$HDFS_ROOT"

banner "HDFS is up: ${HDFS_URI}   UI: http://${VM2_HOST}:9870"
