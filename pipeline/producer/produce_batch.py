#!/usr/bin/env python3
"""
ClickPulse producer  (runs on VM1)

Publishes every line of a clickstream file to Kafka *byte-for-byte unchanged*
(including duplicates, broken JSON and blank lines). Each message carries
headers with its lineage so the consumer can rebuild the exact file:

    key     = "<batch>:<line_no>"
    value   = raw line bytes (no trailing newline)
    headers = batch_id, source_file, line_no, total_lines, file_sha256

After all lines are acknowledged by the brokers (acks=all, idempotent producer)
a COMPLETE marker is written to the compacted control topic (key = batch id).

Idempotent: if the control topic already has a COMPLETE marker for this batch
with the same file checksum, nothing is sent again.
"""
import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition


def read_lines(path):
    """Split on b'\\n' only, keeping every line's exact bytes."""
    with open(path, "rb") as fh:
        data = fh.read()
    sha = hashlib.sha256(data).hexdigest()
    parts = data.split(b"\n")
    trailing_newline = data.endswith(b"\n")
    if trailing_newline:
        parts = parts[:-1]          # the empty string after the final '\n' is not a line
    return data, sha, parts, trailing_newline


def read_control_markers(bootstrap, control_topic):
    """Return {batch_id: latest marker dict} from the compacted control topic."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"clickpulse-control-reader-{os.getpid()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    markers = {}
    try:
        md = consumer.list_topics(control_topic, timeout=15)
        if control_topic not in md.topics or md.topics[control_topic].error:
            raise SystemExit(f"Control topic {control_topic} missing - run scripts/vm1/11_start_kafka.sh")
        for p in md.topics[control_topic].partitions:
            tp = TopicPartition(control_topic, p)
            low, high = consumer.get_watermark_offsets(tp, timeout=15)
            if high <= low:
                continue
            consumer.assign([TopicPartition(control_topic, p, low)])
            pos = low
            while pos < high:
                msg = consumer.poll(5.0)
                if msg is None:
                    break
                if msg.error():
                    raise KafkaException(msg.error())
                pos = msg.offset() + 1
                if msg.key() and msg.value():
                    markers[msg.key().decode()] = json.loads(msg.value())
    finally:
        consumer.close()
    return markers


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--control-topic", required=True)
    ap.add_argument("--batch", required=True)
    ap.add_argument("--file", required=True)
    args = ap.parse_args()

    data, sha, lines, trailing_nl = read_lines(args.file)
    source_file = os.path.basename(args.file)
    total = len(lines)
    print(f"[producer] batch={args.batch} file={source_file} lines={total} bytes={len(data)} sha256={sha}")

    marker = read_control_markers(args.bootstrap, args.control_topic).get(args.batch)
    if marker and marker.get("status") == "COMPLETE":
        if marker.get("file_sha256") == sha:
            print(f"[producer] batch '{args.batch}' already published at {marker.get('published_at')} "
                  f"({marker.get('total_lines')} lines, same checksum) -> SKIP (idempotent re-run)")
            return 0
        print(f"[producer] ERROR: batch '{args.batch}' was already published with a DIFFERENT file "
              f"(sha {marker.get('file_sha256')}). Use a new batch id for new data.", file=sys.stderr)
        return 2

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "compression.type": "none",
        "client.id": "clickpulse-producer",
    })

    delivered, failures = Counter(), []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(str(err))
        else:
            delivered[msg.partition()] += 1

    t0 = time.time()
    for i, raw in enumerate(lines, start=1):
        headers = [
            ("batch_id", args.batch.encode()),
            ("source_file", source_file.encode()),
            ("line_no", str(i).encode()),
            ("total_lines", str(total).encode()),
            ("file_sha256", sha.encode()),
        ]
        while True:
            try:
                producer.produce(args.topic, key=f"{args.batch}:{i:06d}".encode(), value=raw,
                                 headers=headers, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
    remaining = producer.flush(60)

    if failures or remaining or sum(delivered.values()) != total:
        print(f"[producer] ERROR: delivered={sum(delivered.values())}/{total} "
              f"pending={remaining} failures={failures[:3]}", file=sys.stderr)
        return 1

    marker = {
        "batch_id": args.batch,
        "status": "COMPLETE",
        "source_file": source_file,
        "file_sha256": sha,
        "total_lines": total,
        "bytes": len(data),
        "trailing_newline": trailing_nl,
        "topic": args.topic,
        "published_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    producer.produce(args.control_topic, key=args.batch.encode(), value=json.dumps(marker).encode())
    if producer.flush(30):
        print("[producer] ERROR: control marker not acknowledged", file=sys.stderr)
        return 1

    per_part = ", ".join(f"p{p}={n}" for p, n in sorted(delivered.items()))
    print(f"[producer] published {total} lines to '{args.topic}' in {time.time() - t0:.1f}s ({per_part})")
    print(f"[producer] control marker written to '{args.control_topic}' key={args.batch}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
