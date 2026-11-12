#!/usr/bin/env bash
# =============================================================================
#  DEMO - RUN ON: VM1  (Harsha K)
#  Shows the 2-broker KRaft Kafka cluster and, after a run, what is in the topics.
#     demo/01_vm1_show_kafka.sh          # cluster + topics + messages per batch
# =============================================================================
source "$(dirname "$0")/../scripts/lib/common.sh"
expect_vm 1
KB="$KAFKA_HOME/bin"

banner "Kafka on VM1 - two brokers in KRaft mode (no ZooKeeper)"

step "Broker processes on this VM"
pgrep -af "kafka.Kafka" | grep -oE "\S*broker[12]\.properties" | sed 's/^/  running with: /'
for b in 1 2; do
  echo "  broker$b: $(grep -E '^(process.roles|node.id|advertised.listeners)' "$CP_BASE/kafka-config/broker$b.properties" | tr '\n' ' ')"
done
pause

step "KRaft metadata quorum (broker 1 is the controller)"
run "$KB/kafka-metadata-quorum.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" describe --status
run "$KB/kafka-metadata-quorum.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" describe --replication
pause

step "Topics - 3 partitions, replication factor 2 (a copy on each broker)"
run "$KB/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic "$KAFKA_TOPIC"
run "$KB/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic "$KAFKA_CONTROL_TOPIC"
pause

step "Messages per batch in '$KAFKA_TOPIC' + producer markers"
"$CP_VENV/bin/python" - "$KAFKA_BOOTSTRAP" "$KAFKA_TOPIC" "$KAFKA_CONTROL_TOPIC" "$CP_ROOT/data" <<'EOF'
import json, os, sys, hashlib
from collections import Counter, defaultdict
from confluent_kafka import Consumer, TopicPartition
bs, topic, ctl, data_dir = sys.argv[1:5]
c = Consumer({"bootstrap.servers": bs, "group.id": f"clickpulse-demo-{os.getpid()}", "enable.auto.commit": False})
def scan(t):
    md = c.list_topics(t, timeout=10)
    for p in sorted(md.topics[t].partitions):
        lo, hi = c.get_watermark_offsets(TopicPartition(t, p), timeout=10)
        print(f"  {t}[{p}] offsets {lo}..{hi}")
        if hi <= lo: continue
        c.assign([TopicPartition(t, p, lo)])
        pos = lo
        while pos < hi:
            m = c.poll(5)
            if m is None: break
            pos = m.offset() + 1
            yield m
per_batch, sample = Counter(), {}
lines_by_batch = defaultdict(dict)
for m in scan(topic):
    h = {k: v.decode() for k, v in (m.headers() or [])}
    b = h.get("batch_id", "?"); per_batch[b] += 1
    lines_by_batch[b][int(h.get("line_no", 0))] = m.value() or b""
    if b not in sample and int(h.get("line_no", 0)) == 1:
        sample[b] = (m.partition(), m.offset(), m.key().decode(), h, (m.value() or b"")[:110].decode(errors="replace"))
print()
for b, n in sorted(per_batch.items()):
    src = os.path.join(data_dir, f"clicks_{b}.jsonl")
    ok = ""
    if os.path.exists(src):
        raw = open(src, "rb").read().split(b"\n")[:-1]
        same = all(lines_by_batch[b].get(i + 1) == raw[i] for i in range(len(raw)))
        ok = f"(source file has {len(raw)} lines; every line byte-identical in Kafka: {same})"
    print(f"  batch {b}: {n} messages {ok}")
for b, (p, o, k, h, v) in sorted(sample.items()):
    print(f"\n  sample {b}: partition={p} offset={o} key={k}\n    headers={h}\n    value={v}...")
print("\n  control markers (compacted topic):")
for m in scan(ctl):
    print(f"    {m.key().decode()}: {m.value().decode()}")
c.close()
EOF
banner "Kafka keeps every raw line (retention = forever); brokers stay up for the whole project"
