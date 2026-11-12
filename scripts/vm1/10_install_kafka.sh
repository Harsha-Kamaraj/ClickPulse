#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM1  (Harsha K)  - once, after scripts/all/00_bootstrap_vm.sh
#
#  Installs:
#   * Apache Kafka (KRaft) with TWO brokers on this VM
#       broker 1 : node.id=1, roles=broker,controller  PLAINTEXT :9092  CONTROLLER :9093
#       broker 2 : node.id=2, roles=broker              PLAINTEXT :9094
#   * Hadoop client (VM1 must talk to the same HDFS as VM2/VM3)
#   * Python venv with confluent-kafka for the producer
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
source "$CP_ROOT/scripts/lib/hadoop.sh"
set -e  # stop at the first error
require_not_root
expect_vm 1

banner "VM1 install: Kafka ${KAFKA_VERSION} (2 brokers, KRaft) + HDFS client"

step "Kafka binaries"
TGZ="$CP_DOWNLOADS/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
download "kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz" "$TGZ"
install_tarball "$TGZ" "$KAFKA_HOME"

step "Kafka broker configs"
KCONF="$CP_BASE/kafka-config"
mkdir -p "$KCONF" "$CP_DATA/kafka/broker1" "$CP_DATA/kafka/broker2" "$CP_LOGS/kafka-broker1" "$CP_LOGS/kafka-broker2"

common_props() {
  cat <<EOF
controller.quorum.voters=1@${VM1_HOST}:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

num.partitions=${KAFKA_PARTITIONS}
default.replication.factor=${KAFKA_REPLICATION}
min.insync.replicas=1
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0

# keep every raw message forever - the project must not reset Kafka
log.retention.ms=-1
log.retention.bytes=-1
EOF
}

cat > "$KCONF/broker1.properties" <<EOF
# ---- ClickPulse Kafka broker 1  (broker + KRaft controller) ----
process.roles=broker,controller
node.id=1
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://${VM1_HOST}:9092
log.dirs=${CP_DATA}/kafka/broker1
$(common_props)
EOF

cat > "$KCONF/broker2.properties" <<EOF
# ---- ClickPulse Kafka broker 2  (broker only) ----
process.roles=broker
node.id=2
listeners=PLAINTEXT://0.0.0.0:9094
advertised.listeners=PLAINTEXT://${VM1_HOST}:9094
log.dirs=${CP_DATA}/kafka/broker2
$(common_props)
EOF
ok "Wrote $KCONF/broker1.properties and broker2.properties"

step "Formatting KRaft storage (only the first time)"
CID_FILE="$CP_DATA/kafka/cluster.id"
if [[ ! -s "$CID_FILE" ]]; then
  "$KAFKA_HOME/bin/kafka-storage.sh" random-uuid > "$CID_FILE"
fi
CID="$(cat "$CID_FILE")"
for b in 1 2; do
  if [[ -f "$CP_DATA/kafka/broker$b/meta.properties" ]]; then
    info "broker$b storage already formatted (cluster.id=$CID)"
  else
    "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CID" -c "$KCONF/broker$b.properties" --ignore-formatted
  fi
done
ok "KRaft cluster id: $CID"

step "Hadoop client"
install_hadoop

step "Python venv for the producer"
python3 -m venv "$CP_VENV"
"$CP_VENV/bin/pip" install -q --upgrade pip
"$CP_VENV/bin/pip" install -q -r "$CP_ROOT/pipeline/requirements-kafka.txt"
ok "venv ready: $CP_VENV"

banner "VM1 install done" "Next: scripts/vm1/11_start_kafka.sh"
