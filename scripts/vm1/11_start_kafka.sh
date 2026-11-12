#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM1  (Harsha K)
#  Starts BOTH Kafka brokers (KRaft) and creates the ClickPulse topics.
#  Safe to re-run: already-running brokers / existing topics are left alone.
#  The brokers must stay up for the whole project - Airflow never stops them.
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
require_not_root
expect_vm 1

KCONF="$CP_BASE/kafka-config"
[[ -f "$KCONF/broker1.properties" ]] || die "Run scripts/vm1/10_install_kafka.sh first"

broker_running() { pgrep -f "kafka.Kafka .*broker$1.properties" >/dev/null 2>&1; }

start_broker() {
  local b="$1"
  if broker_running "$b"; then info "broker$b already running"; return 0; fi
  info "starting broker$b"
  LOG_DIR="$CP_LOGS/kafka-broker$b" KAFKA_HEAP_OPTS="$KAFKA_HEAP" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$KCONF/broker$b.properties"
}

banner "Starting Kafka (KRaft) on VM1"
start_broker 1
wait_for_port "$VM1_HOST" 9093 90 "KRaft controller (broker1)"
wait_for_port "$VM1_HOST" 9092 90 "Kafka broker 1"
start_broker 2
wait_for_port "$VM1_HOST" 9094 90 "Kafka broker 2"

step "Waiting until both brokers are registered"
for _ in $(seq 1 30); do
  n="$("$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" 2>/dev/null | grep -c '(id: ' || true)"
  [[ "$n" -ge 2 ]] && break
  sleep 2
done
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" 2>/dev/null | grep "(id:" || warn "could not list brokers yet"

step "Creating topics (if missing)"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --create --if-not-exists \
  --topic "$KAFKA_TOPIC" --partitions "$KAFKA_PARTITIONS" --replication-factor "$KAFKA_REPLICATION"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --create --if-not-exists \
  --topic "$KAFKA_CONTROL_TOPIC" --partitions 1 --replication-factor "$KAFKA_REPLICATION" \
  --config cleanup.policy=compact
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic "$KAFKA_TOPIC"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic "$KAFKA_CONTROL_TOPIC"

step "KRaft quorum"
"$KAFKA_HOME/bin/kafka-metadata-quorum.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" describe --status || true

banner "Kafka is up: broker1=${VM1_HOST}:9092 (controller :9093)  broker2=${VM1_HOST}:9094"
