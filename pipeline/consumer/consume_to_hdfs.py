#!/usr/bin/env python3
"""
ClickPulse Kafka -> HDFS raw sink  (runs on VM2, the HDFS NameNode)

1. Waits for the producer's COMPLETE marker for the batch (control topic).
2. Reads the raw topic from the beginning up to the current end offsets and
   keeps the messages of this batch (headers batch_id + file_sha256).
   Kafka-level redeliveries (same line_no twice) are collapsed, *input*
   duplicates are kept because they are different line numbers.
3. Re-assembles the file in line order and checks that its SHA-256 equals the
   source file's checksum -> proves every input line landed unchanged.
4. Writes it atomically to   /clickpulse/raw/batch_id=<batch>/<source_file>
   plus a _MANIFEST.json (Spark ignores files starting with '_').

Idempotent: if the manifest already exists with the same checksum -> skip.
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException, TopicPartition


def hdfs(*args, check=True, capture=False):
    cmd = ["hdfs", "dfs", *args]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {res.stderr.strip()}")
    return res.stdout if capture else res.returncode


def read_partition_range(consumer, topic, partition, low, high, on_msg):
    if high <= low:
        return
    consumer.assign([TopicPartition(topic, partition, low)])
    pos = low
    idle = 0
    while pos < high:
        msg = consumer.poll(2.0)
        if msg is None:
            idle += 1
            if idle > 15:
                raise RuntimeError(f"Timed out reading {topic}[{partition}] at offset {pos}/{high}")
            continue
        idle = 0
        if msg.error():
            raise KafkaException(msg.error())
        pos = msg.offset() + 1
        on_msg(msg)


def scan_topic(consumer, topic, on_msg):
    md = consumer.list_topics(topic, timeout=15)
    if topic not in md.topics or md.topics[topic].error:
        raise SystemExit(f"Topic {topic} does not exist")
    for p in sorted(md.topics[topic].partitions):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
        read_partition_range(consumer, topic, p, low, high, on_msg)


def get_marker(consumer, control_topic, batch, wait_s):
    deadline = time.time() + wait_s
    while True:
        found = {}

        def keep(msg):
            if msg.key() and msg.key().decode() == batch and msg.value():
                found["m"] = json.loads(msg.value())

        scan_topic(consumer, control_topic, keep)
        m = found.get("m")
        if m and m.get("status") == "COMPLETE":
            return m
        if time.time() > deadline:
            raise SystemExit(f"No COMPLETE marker for batch '{batch}' in {control_topic} - did the producer run?")
        print(f"[consumer] waiting for producer marker for '{batch}' ...")
        time.sleep(5)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--control-topic", required=True)
    ap.add_argument("--batch", required=True)
    ap.add_argument("--hdfs-root", required=True, help="e.g. /clickpulse")
    ap.add_argument("--wait", type=int, default=120)
    args = ap.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": "clickpulse-hdfs-sink",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "client.id": "clickpulse-consumer",
    })
    try:
        marker = get_marker(consumer, args.control_topic, args.batch, args.wait)
        sha, total, source_file = marker["file_sha256"], int(marker["total_lines"]), marker["source_file"]
        print(f"[consumer] marker: batch={args.batch} file={source_file} lines={total} sha256={sha}")

        raw_dir = f"{args.hdfs_root}/raw/batch_id={args.batch}"
        manifest_path = f"{raw_dir}/_MANIFEST.json"
        if hdfs("-test", "-e", manifest_path, check=False) == 0:
            existing = json.loads(hdfs("-cat", manifest_path, capture=True))
            if existing.get("file_sha256") == sha:
                print(f"[consumer] {raw_dir}/{source_file} already landed with the same checksum "
                      f"({existing.get('lines')} lines) -> SKIP (idempotent re-run)")
                return 0
            raise SystemExit(f"HDFS already has a DIFFERENT file for batch '{args.batch}'. Refusing to overwrite raw data.")

        lines, per_partition, kafka_msgs, redelivered = {}, Counter(), 0, 0

        def collect(msg):
            nonlocal kafka_msgs, redelivered
            h = {k: v.decode() for k, v in (msg.headers() or [])}
            if h.get("batch_id") != args.batch or h.get("file_sha256") != sha:
                return
            kafka_msgs += 1
            n = int(h["line_no"])
            if n in lines:
                redelivered += 1         # same line sent twice (producer retry/re-run) - not an input duplicate
                return
            lines[n] = msg.value() or b""
            per_partition[msg.partition()] += 1

        scan_topic(consumer, args.topic, collect)
    finally:
        consumer.close()

    missing = [n for n in range(1, total + 1) if n not in lines]
    if missing:
        raise SystemExit(f"[consumer] ERROR: {len(missing)} lines missing from Kafka (first: {missing[:5]})")

    body = b"\n".join(lines[n] for n in range(1, total + 1))
    if marker.get("trailing_newline", True):
        body += b"\n"
    got_sha = hashlib.sha256(body).hexdigest()
    if got_sha != sha:
        raise SystemExit(f"[consumer] ERROR: rebuilt file checksum {got_sha} != source {sha}")
    print(f"[consumer] rebuilt {total} lines from {kafka_msgs} Kafka messages "
          f"({', '.join(f'p{p}={c}' for p, c in sorted(per_partition.items()))}; redelivered={redelivered}); "
          f"checksum matches source")

    with tempfile.TemporaryDirectory() as td:
        local = os.path.join(td, source_file)
        with open(local, "wb") as fh:
            fh.write(body)
        manifest = {
            "batch_id": args.batch, "source_file": source_file, "file_sha256": sha,
            "lines": total, "bytes": len(body), "kafka_messages": kafka_msgs,
            "kafka_redelivered": redelivered, "topic": args.topic,
            "landed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        mpath = os.path.join(td, "_MANIFEST.json")
        with open(mpath, "w") as fh:
            json.dump(manifest, fh, indent=2)

        hdfs("-mkdir", "-p", raw_dir)
        tmp_target = f"{raw_dir}/.{source_file}.inprogress"
        hdfs("-put", "-f", local, tmp_target)
        hdfs("-rm", "-f", "-skipTrash", f"{raw_dir}/{source_file}", check=False)
        hdfs("-mv", tmp_target, f"{raw_dir}/{source_file}")
        hdfs("-put", "-f", mpath, manifest_path)       # manifest last = commit point

    print(f"[consumer] landed raw file -> {raw_dir}/{source_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
