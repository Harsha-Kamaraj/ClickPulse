#!/usr/bin/env bash
# =============================================================================
#  DEMO - RUN ON: VM1 (Harsha) and VM3 (Gagan)   [VM2 can run it too]
#  Proves this VM talks to the SAME HDFS cluster whose NameNode+DataNode are on VM2.
# =============================================================================
source "$(dirname "$0")/../scripts/lib/common.sh"

banner "HDFS client check on $(hostname) ($(hostname -I 2>/dev/null | tr ' ' '\n' | grep -E "^($VM1_IP|$VM2_IP|$VM3_IP|$VM4_IP)$" || echo '?'))"
step "Configured filesystem"
run "$HADOOP_HOME/bin/hdfs" getconf -confKey fs.defaultFS
fs="$("$HADOOP_HOME/bin/hdfs" getconf -confKey fs.defaultFS 2>/dev/null)"
if [[ "$fs" != "$HDFS_URI" ]]; then
  err "fs.defaultFS is '$fs' but must be '$HDFS_URI' (the NameNode on VM2)."
  info "Config file in use: $HADOOP_CONF_DIR/core-site.xml"
  env | grep -E '^HADOOP_(CONF_DIR|CLASSPATH|HOME|OPTS)=' | sed 's/^/  your shell has: /'
  die "Re-run this VM's install script (it rewrites the Hadoop config), then run this demo again."
fi

step "Cluster report (served by the NameNode on VM2)"
"$HADOOP_HOME/bin/hdfs" dfsadmin -report 2>/dev/null | grep -E "Configured Capacity|DFS Used:|Live datanodes|^Name:|^Hostname:" | head -8
step "ClickPulse directories"
run "$HADOOP_HOME/bin/hdfs" dfs -ls "$HDFS_ROOT" || die "Cannot list $HDFS_ROOT on $HDFS_URI - is HDFS running on VM2 (scripts/vm2/11_start_hdfs.sh)?"
run "$HADOOP_HOME/bin/hdfs" dfs -ls -R "$HDFS_ROOT/raw"
step "Write + read test from this VM"
f="$HDFS_ROOT/_control/hello_from_$(hostname).txt"
echo "hello from $(hostname) at $(date -u +%H:%M:%S) UTC" | "$HADOOP_HOME/bin/hdfs" dfs -put -f - "$f" \
  || die "Write to HDFS failed"
run "$HADOOP_HOME/bin/hdfs" dfs -cat "$f" || die "Read from HDFS failed"
run "$HADOOP_HOME/bin/hdfs" dfs -ls "$HDFS_ROOT/_control"
ok "$(hostname) is a client of ${HDFS_URI}"
