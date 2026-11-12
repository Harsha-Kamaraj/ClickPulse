#!/usr/bin/env bash
# RUN ON: VM2 (Anjanaa) - stops Airflow and HDFS (data is kept).
source "$(dirname "$0")/../lib/common.sh"
for name in webserver scheduler; do
  pidf="$CP_RUN/airflow-$name.pid"
  if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    kill "$(cat "$pidf")" && ok "stopped airflow $name"
  fi
  rm -f "$pidf"
done
pkill -f "airflow scheduler" 2>/dev/null || true
pkill -f "gunicorn.*airflow-webserver" 2>/dev/null || true
"$HADOOP_HOME/bin/hdfs" --daemon stop datanode || true
"$HADOOP_HOME/bin/hdfs" --daemon stop namenode || true
ok "VM2 services stopped"
