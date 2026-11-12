#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM3  (Gagan R)
#  Starts the Spark standalone master + worker and checks HDFS access.
#  Safe to re-run.   Master UI: http://cp-vm3:8080
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
require_not_root
expect_vm 3

banner "Starting Spark standalone on VM3"
# a master/worker started from another Spark install (e.g. an older lab setup) must not be reused
for cls in org.apache.spark.deploy.master.Master org.apache.spark.deploy.worker.Worker; do
  for pid in $(pgrep -f "java.*$cls"); do
    # the jars a running JVM has open resolve symlinks, so this catches an install reached via a link
    if ! ls -l "/proc/$pid/fd" 2>/dev/null | grep -q -- "-> $(readlink -f "$SPARK_HOME")/jars/spark-core_"; then
      die "A Spark ${cls##*.} (pid $pid) from a different Spark install (or another user) is running. Run scripts/vm3/stop_spark.sh first."
    fi
  done
done
if pgrep -f "org.apache.spark.deploy.master.Master" >/dev/null; then
  info "Spark master already running"
else
  run "$SPARK_HOME/sbin/start-master.sh"
fi
wait_for_port "$VM3_HOST" 7077 60 "Spark master"

if pgrep -f "org.apache.spark.deploy.worker.Worker" >/dev/null; then
  info "Spark worker already running"
else
  run "$SPARK_HOME/sbin/start-worker.sh" "$SPARK_MASTER_URL"
fi
sleep 3
curl -s "http://${VM3_HOST}:8080/json/" | python3 -c \
  'import json,sys; d=json.load(sys.stdin); print("  master status=%s workers=%d cores=%s memory=%sMB" % (d["status"], len(d["workers"]), d["cores"], d["memory"]))' \
  || warn "could not read master UI JSON"

step "HDFS reachable from VM3?"
if "$HADOOP_HOME/bin/hdfs" dfs -ls "${HDFS_URI}${HDFS_ROOT}" ; then
  ok "VM3 sees the shared HDFS (${HDFS_URI})"
else
  warn "HDFS not reachable - is scripts/vm2/11_start_hdfs.sh running on VM2?"
fi

banner "Spark master: ${SPARK_MASTER_URL}   UI: http://${VM3_HOST}:8080"
