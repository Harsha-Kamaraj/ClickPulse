#!/usr/bin/env bash
# RUN ON: VM3 (Gagan R) - stops Spark worker + master.
source "$(dirname "$0")/../lib/common.sh"
"$SPARK_HOME/sbin/stop-worker.sh" || true
"$SPARK_HOME/sbin/stop-master.sh" || true
ok "Spark stopped"
