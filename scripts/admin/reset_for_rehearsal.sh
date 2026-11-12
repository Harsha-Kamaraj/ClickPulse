#!/usr/bin/env bash
# =============================================================================
#  !!! DANGER - REHEARSALS ONLY !!!   RUN ON: VM2 (Anjanaa)
#
#  Wipes ALL ClickPulse data so you can practise the demo from scratch:
#    Kafka topics (recreated empty), HDFS /clickpulse (recreated empty),
#    Iceberg tables, Druid datasource, Airflow run history of the DAG.
#
#  The project rules say: after the FINAL Day 1 + Day 2 run, do NOT reset
#  Kafka, HDFS, the curated table or Druid before submission.
#  So never run this after your final run.
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
source "$CP_ROOT/scripts/lib/airflow_env.sh"
expect_vm 2

banner "!!! This deletes every ClickPulse message, file, table row and Druid segment !!!"
read -r -p "Type RESET to continue: " ans
[[ "$ans" == "RESET" ]] || die "aborted"

step "Kafka topics (VM1)"
vm_ssh 1 "bash -lc '
  source $CP_REPO/scripts/lib/common.sh
  for t in $KAFKA_TOPIC $KAFKA_CONTROL_TOPIC; do \$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server \$KAFKA_BOOTSTRAP --delete --topic \$t --if-exists; done
  sleep 5
  \$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server \$KAFKA_BOOTSTRAP --create --if-not-exists --topic $KAFKA_TOPIC --partitions $KAFKA_PARTITIONS --replication-factor $KAFKA_REPLICATION
  \$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server \$KAFKA_BOOTSTRAP --create --if-not-exists --topic $KAFKA_CONTROL_TOPIC --partitions 1 --replication-factor $KAFKA_REPLICATION --config cleanup.policy=compact
'"

step "HDFS ${HDFS_ROOT}"
hdfs dfs -rm -r -f -skipTrash "$HDFS_ROOT"
for d in raw rejects warehouse export/druid _control/runs; do hdfs dfs -mkdir -p "$HDFS_ROOT/$d"; done
hdfs dfs -chmod -R 777 "$HDFS_ROOT"

step "Druid datasource $DRUID_DATASOURCE (VM4)"
curl -s -X DELETE "${DRUID_ROUTER}/druid/coordinator/v1/datasources/${DRUID_DATASOURCE}" >/dev/null || true
sleep 5
curl -s -X DELETE "${DRUID_ROUTER}/druid/coordinator/v1/datasources/${DRUID_DATASOURCE}/intervals/1000-01-01_3000-01-01" >/dev/null || true
curl -s -X POST -H 'Content-Type: application/json' \
  -d "{\"type\":\"kill\",\"dataSource\":\"${DRUID_DATASOURCE}\",\"interval\":\"1000-01-01/3000-01-01\"}" \
  "${DRUID_ROUTER}/druid/indexer/v1/task" >/dev/null || true

step "Airflow run history"
airflow dags delete -y clickpulse_pipeline >/dev/null 2>&1 || true
sleep 20; airflow dags unpause clickpulse_pipeline >/dev/null 2>&1 || true

banner "Reset done - start again with demo/10_vm2_run_pipeline.sh day1"
