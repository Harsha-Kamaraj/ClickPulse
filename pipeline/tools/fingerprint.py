#!/usr/bin/env python3
"""
State fingerprint of the whole ClickPulse system (run on VM2 with the venv python).
Used by demo/20_vm2_rerun_idempotency.sh to prove that re-runs change nothing.

  fingerprint.py snapshot <out.json>
  fingerprint.py compare <before.json> <after.json>
"""
import hashlib
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "druid"))
from druid_query import query  # noqa: E402

E = os.environ
ROOT = E["HDFS_ROOT"]
TABLES = ["events", "rejects", "upsert_log", "batch_audit"]


def sh(*cmd):
    r = subprocess.run(cmd, capture_output=True)
    return r.stdout if r.returncode == 0 else b""


def kafka_offsets():
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": E["KAFKA_BOOTSTRAP"], "group.id": "clickpulse-fingerprint"})
    out = {}
    for t in (E["KAFKA_TOPIC"], E["KAFKA_CONTROL_TOPIC"]):
        for p in sorted(c.list_topics(t, timeout=10).topics[t].partitions):
            out[f"{t}[{p}]"] = c.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1]
    c.close()
    return out


def snapshot():
    fp = {}
    for k, v in kafka_offsets().items():
        fp[f"kafka end offset {k}"] = v
    for line in sh("hdfs", "dfs", "-ls", f"{ROOT}/raw").decode().splitlines():
        if "batch_id=" in line:
            d = line.split()[-1]
            data = sh("hdfs", "dfs", "-cat", f"{d}/*.jsonl")
            fp[f"hdfs raw {d.split('/')[-1]} sha256"] = hashlib.sha256(data).hexdigest()[:16]
    listing = sh("hdfs", "dfs", "-ls", "-R", f"{ROOT}/rejects").decode()
    fp["hdfs rejects/ files (name,size,mtime)"] = hashlib.sha256(listing.encode()).hexdigest()[:16]
    for t in TABLES:
        v = sh("hdfs", "dfs", "-cat", f"{ROOT}/warehouse/clickpulse/{t}/metadata/version-hint.text").decode().strip()
        fp[f"iceberg {t} metadata version"] = v or "-"
    exp = json.loads(sh("hdfs", "dfs", "-cat", f"{ROOT}/_control/druid_export_latest.json") or b"{}")
    fp["iceberg events current snapshot"] = exp.get("snapshot_id")
    ds = '"%s"' % E["DRUID_DATASOURCE"]
    r = query(E["DRUID_ROUTER"], f"SELECT COUNT(*) AS n, ROUND(SUM(revenue),2) AS rev, SUM(is_purchase) AS p FROM {ds}")[0]
    fp["druid rows"], fp["druid revenue"], fp["druid purchases"] = r["n"], r["rev"], r["p"]
    segs = query(E["DRUID_ROUTER"], "SELECT segment_id FROM sys.segments WHERE datasource = '%s' "
                 "AND is_published = 1 AND is_overshadowed = 0 ORDER BY segment_id" % E["DRUID_DATASOURCE"])
    fp["druid active segment versions"] = hashlib.sha256(
        "|".join(s["segment_id"] for s in segs).encode()).hexdigest()[:16] + f" ({len(segs)} segments)"
    return fp


def compare(a, b):
    expected_change = {"iceberg batch_audit metadata version"}
    bad = 0
    print(f"  {'item':<44}{'before':>22}{'after':>22}")
    print("  " + "-" * 96)
    for k in sorted(set(a) | set(b)):
        va, vb = a.get(k), b.get(k)
        if k in expected_change:
            mark = "changed (expected: +1 audit row per run)" if va != vb else "same"
        elif va == vb:
            mark = "UNCHANGED"
        else:
            mark = "CHANGED !"
            bad += 1
        print(f"  {k:<44}{str(va):>22}{str(vb):>22}   {mark}")
    print()
    print("  RESULT: " + ("re-runs changed NOTHING in Kafka, HDFS raw/rejects, the curated table or Druid"
                          if not bad else f"{bad} item(s) changed"))
    return 1 if bad else 0


if __name__ == "__main__":
    if sys.argv[1] == "snapshot":
        json.dump(snapshot(), open(sys.argv[2], "w"), indent=2)
    else:
        sys.exit(compare(json.load(open(sys.argv[2])), json.load(open(sys.argv[3]))))
