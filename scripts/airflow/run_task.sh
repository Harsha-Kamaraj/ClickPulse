#!/usr/bin/env bash
# =============================================================================
#  Executed BY AIRFLOW on VM2 (one call per DAG task). Can also be run by hand:
#     scripts/airflow/run_task.sh <preflight|produce|consume|spark|druid_sync|verify> <batch>
#
#   preflight   both Kafka brokers, HDFS, Spark master/worker, Druid, SSH all OK
#   produce     ssh VM1 -> producer: file lines -> Kafka           (idempotent)
#   consume     VM2     -> consumer: Kafka -> HDFS raw/            (idempotent)
#   spark       ssh VM3 -> spark-submit (driver on VM3): HDFS -> Iceberg MERGE -> Parquet export
#   druid_sync  VM2     -> Druid native batch ingest of the export (skipped if already mirrored)
#   verify      VM2     -> Druid SQL totals == curated table totals
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
TASK="${1:?task}"; BATCH="${2:?batch}"
validate_batch "$BATCH"
echo "[$(date -u +%H:%M:%S)] task=$TASK batch=$BATCH host=$(hostname)"

case "$TASK" in
  preflight)
    fail=0
    "$CP_VENV/bin/python" - "$KAFKA_BOOTSTRAP" "$KAFKA_TOPIC" "$KAFKA_CONTROL_TOPIC" <<'EOF' || fail=1
import sys
from confluent_kafka.admin import AdminClient
bs, topic, ctl = sys.argv[1:4]
md = AdminClient({"bootstrap.servers": bs}).list_topics(timeout=15)
brokers = sorted(f"{b.id}@{b.host}:{b.port}" for b in md.brokers.values())
print(f"  kafka brokers: {brokers}  controller id: {md.controller_id}")
assert len(brokers) >= 2, "both Kafka brokers must be running"
for t in (topic, ctl):
    assert t in md.topics, f"topic {t} missing"
    parts = md.topics[t].partitions
    print(f"  topic {t}: {len(parts)} partitions, replicas={[list(p.replicas) for p in parts.values()]}")
EOF
    hdfs dfs -test -d "$HDFS_ROOT" && echo "  hdfs: ${HDFS_URI}${HDFS_ROOT} OK" || { err "HDFS not reachable"; fail=1; }
    curl -sf "http://${VM3_HOST}:8080/json/" | python3 -c 'import json,sys; d=json.load(sys.stdin); w=[x for x in d["workers"] if x["state"]=="ALIVE"]; print("  spark master %s, alive workers=%d" % (d["status"], len(w))); sys.exit(0 if d["status"]=="ALIVE" and w else 1)' \
      || { err "Spark master/worker not ready on VM3"; fail=1; }
    [[ "$(curl -s "${DRUID_ROUTER}/status/health")" == "true" ]] && echo "  druid router OK" || { err "Druid not healthy on VM4"; fail=1; }
    vm_ssh 1 "test -d $CP_REPO" && echo "  ssh vm1 OK" || { err "ssh to VM1 failed"; fail=1; }
    vm_ssh 3 "test -d $CP_REPO" && echo "  ssh vm3 OK" || { err "ssh to VM3 failed"; fail=1; }
    exit $fail
    ;;
  produce)
    vm_ssh 1 "bash $CP_REPO/scripts/vm1/run_producer.sh $BATCH"
    ;;
  consume)
    bash "$CP_ROOT/scripts/vm2/run_consumer.sh" "$BATCH"
    ;;
  spark)
    vm_ssh 3 "bash $CP_REPO/scripts/vm3/run_spark_job.sh $BATCH"
    ;;
  druid_sync)
    python3 "$CP_ROOT/pipeline/druid/sync_to_druid.py" \
      --router "$DRUID_ROUTER" --datasource "$DRUID_DATASOURCE" \
      --hdfs-uri "$HDFS_URI" --hdfs-root "$HDFS_ROOT" \
      --mode "$DRUID_INPUT_MODE" --vm4-ssh "${VM4_USER}@${VM4_HOST}" --vm4-dir "$CP_BASE/druid-input"
    ;;
  verify)
    echo "---- batch summary written by Spark (${HDFS_ROOT}/_control/runs/batch_id=$BATCH/latest.json)"
    hdfs dfs -cat "${HDFS_ROOT}/_control/runs/batch_id=$BATCH/latest.json"
    echo
    python3 "$CP_ROOT/pipeline/druid/sync_to_druid.py" --verify-only \
      --router "$DRUID_ROUTER" --datasource "$DRUID_DATASOURCE" \
      --hdfs-uri "$HDFS_URI" --hdfs-root "$HDFS_ROOT"
    ;;
  *)
    die "unknown task '$TASK'"
    ;;
esac
