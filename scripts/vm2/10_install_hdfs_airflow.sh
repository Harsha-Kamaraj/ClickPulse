#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM2  (Anjanaa)  - once, after scripts/all/00_bootstrap_vm.sh
#
#  Installs:
#   * Hadoop HDFS  - NameNode + DataNode for the whole cluster (formats it once)
#   * Apache Airflow (own venv, SQLite + SequentialExecutor - enough for 1 DAG)
#   * Python venv with confluent-kafka for the Kafka -> HDFS consumer
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
source "$CP_ROOT/scripts/lib/hadoop.sh"
set -e  # stop at the first error
require_not_root
expect_vm 2

banner "VM2 install: HDFS (NameNode+DataNode) + Airflow ${AIRFLOW_VERSION} + Kafka consumer"

step "Hadoop ${HADOOP_VERSION}"
install_hadoop
mkdir -p "$CP_DATA/hadoop/namenode" "$CP_DATA/hadoop/datanode" "$CP_DATA/hadoop/tmp"
if [[ -f "$CP_DATA/hadoop/namenode/current/VERSION" ]]; then
  info "NameNode already formatted - NOT formatting again (keeps all HDFS data)"
else
  "$HADOOP_HOME/bin/hdfs" namenode -format -nonInteractive -force -clusterId clickpulse
  ok "NameNode formatted"
fi

step "Python venv for the Kafka consumer + Druid sync"
python3 -m venv "$CP_VENV"
"$CP_VENV/bin/pip" install -q --upgrade pip
"$CP_VENV/bin/pip" install -q -r "$CP_ROOT/pipeline/requirements-kafka.txt"
ok "venv ready: $CP_VENV"

step "Apache Airflow ${AIRFLOW_VERSION}"
PYV="$(python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
python3 -m venv "$AIRFLOW_VENV"
"$AIRFLOW_VENV/bin/pip" install -q --upgrade pip
"$AIRFLOW_VENV/bin/pip" install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYV}.txt"
mkdir -p "$AIRFLOW_HOME"
source "$CP_ROOT/scripts/lib/airflow_env.sh"
"$AIRFLOW_VENV/bin/airflow" db migrate
"$AIRFLOW_VENV/bin/airflow" users create -r Admin -u "$AIRFLOW_ADMIN_USER" -p "$AIRFLOW_ADMIN_PASSWORD" \
  -e admin@clickpulse.local -f Click -l Pulse 2>/dev/null || info "Airflow admin user already exists"
ok "Airflow installed in $AIRFLOW_VENV (AIRFLOW_HOME=$AIRFLOW_HOME)"

banner "VM2 install done" \
       "Next: scripts/vm2/11_start_hdfs.sh, then 12_setup_ssh.sh, then 13_start_airflow.sh"
