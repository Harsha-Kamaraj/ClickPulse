#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM1. Normally called by Airflow (VM2 -> ssh -> VM1); can be run by hand.
#  Usage: scripts/vm1/run_producer.sh day1
#  Publishes every line of data/clicks_<batch>.jsonl to Kafka, unchanged.
#  Idempotent: a batch that is already fully published is skipped.
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
BATCH="${1:?usage: run_producer.sh <batch e.g. day1>}"
validate_batch "$BATCH"
FILE="$(batch_file "$BATCH")"
[[ -f "$FILE" ]] || die "Input file not found: $FILE"

exec "$CP_VENV/bin/python" "$CP_ROOT/pipeline/producer/produce_batch.py" \
  --bootstrap "$KAFKA_BOOTSTRAP" \
  --topic "$KAFKA_TOPIC" \
  --control-topic "$KAFKA_CONTROL_TOPIC" \
  --batch "$BATCH" \
  --file "$FILE"
