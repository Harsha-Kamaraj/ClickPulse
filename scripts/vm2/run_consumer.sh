#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM2. Normally called by Airflow; can be run by hand.
#  Usage: scripts/vm2/run_consumer.sh day1
#  Reads the batch from Kafka and lands it unchanged in HDFS raw/.
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
BATCH="${1:?usage: run_consumer.sh <batch e.g. day1>}"
validate_batch "$BATCH"
exec "$CP_VENV/bin/python" "$CP_ROOT/pipeline/consumer/consume_to_hdfs.py" \
  --bootstrap "$KAFKA_BOOTSTRAP" \
  --topic "$KAFKA_TOPIC" \
  --control-topic "$KAFKA_CONTROL_TOPIC" \
  --batch "$BATCH" \
  --hdfs-root "$HDFS_ROOT"
