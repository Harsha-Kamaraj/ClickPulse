#!/usr/bin/env python3
"""
ClickPulse curated table -> Druid sync  (runs on VM2, called by Airflow).  stdlib only.

After every Spark upsert, the Spark job exports the curated Iceberg table's
CURRENT state to Parquet on HDFS and writes  <root>/_control/druid_export_latest.json.
This script makes Druid mirror that snapshot:

  * native batch ingestion (index_parallel) of the Parquet files
  * appendToExisting=false + dropExisting=true over an interval that covers both
    the new data and everything Druid held before -> the datasource is REPLACED,
    so updated events that moved to another date appear exactly once and no stale
    rows survive.
  * waits until Druid SQL totals equal the curated table (rows, ids, revenue,
    purchases and per-date numbers).

Idempotent: if Druid already mirrors the exported snapshot -> nothing is ingested.

  --verify-only   just compare Druid with the curated export (exit 1 on mismatch)
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone


# ----------------------------------------------------------------------------- HDFS
def hdfs_cat(path):
    r = subprocess.run(["hdfs", "dfs", "-cat", path], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def hdfs_put_text(path, text):
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as fh:
        fh.write(text)
        tmp = fh.name
    try:
        subprocess.run(["hdfs", "dfs", "-put", "-f", tmp, path], check=True, capture_output=True)
    finally:
        os.unlink(tmp)


# ----------------------------------------------------------------------------- Druid HTTP
class Druid:
    def __init__(self, router, datasource):
        self.router = router.rstrip("/")
        self.ds = datasource

    def _req(self, method, path, body=None, timeout=60):
        data = None if body is None else json.dumps(body).encode()
        url = self.router + path
        for _ in range(5):
            req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    raw = resp.read().decode()
                    return json.loads(raw) if raw else None
            except urllib.error.HTTPError as e:
                # the overlord answers 307 -> its leader URL; urllib won't re-POST, so follow it ourselves
                if e.code in (307, 308) and e.headers.get("Location"):
                    url = urllib.parse.urljoin(url, e.headers["Location"])
                    time.sleep(1)
                    continue
                raise RuntimeError(f"Druid {method} {path} -> HTTP {e.code}: {e.read().decode()[:500]}") from None
        raise RuntimeError(f"Druid {method} {path}: too many redirects")

    def sql(self, query):
        return self._req("POST", "/druid/v2/sql", {
            "query": query, "resultFormat": "object",
            "context": {"sqlTimeZone": "Etc/UTC", "useApproximateCountDistinct": False, "useCache": False,
                        "populateCache": False},
        })

    def exists(self):
        # NB: Druid returns *no row* (not 0) for COUNT(*) over an empty INFORMATION_SCHEMA match
        rows = self.sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                        f"WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '{self.ds}'")
        return len(rows) > 0

    def stats(self):
        if not self.exists():
            return None
        t = self.sql(f"""SELECT COUNT(*) AS "rows", COUNT(DISTINCT event_id) AS ids,
                         ROUND(SUM(revenue), 2) AS revenue, SUM(is_purchase) AS purchases,
                         TIMESTAMP_TO_MILLIS(MIN(__time)) AS min_ms, TIMESTAMP_TO_MILLIS(MAX(__time)) AS max_ms
                         FROM "{self.ds}" """)[0]
        if not t["rows"]:
            return None
        by_date = {r["event_date"]: {"events": r["events"], "purchases": r["purchases"], "revenue": r["revenue"]}
                   for r in self.sql(f"""SELECT event_date, COUNT(*) AS events, SUM(is_purchase) AS purchases,
                                          ROUND(SUM(revenue), 2) AS revenue FROM "{self.ds}"
                                          GROUP BY event_date ORDER BY event_date""")}
        return {"rows": t["rows"], "distinct_event_ids": t["ids"], "revenue": float(t["revenue"] or 0),
                "purchases": int(t["purchases"] or 0), "min_event_time_ms": t["min_ms"],
                "max_event_time_ms": t["max_ms"], "by_date": by_date}

    def submit(self, spec):
        return self._req("POST", "/druid/indexer/v1/task", spec)["task"]

    def task_status(self, task_id):
        return self._req("GET", f"/druid/indexer/v1/task/{task_id}/status")["status"]


# ----------------------------------------------------------------------------- comparison
def compare(export, druid):
    """Return list of (check, curated, druid, ok)."""
    rows = []
    d = druid or {"rows": 0, "distinct_event_ids": 0, "revenue": 0.0, "purchases": 0, "by_date": {}}
    rows.append(("rows", export["rows"], d["rows"], export["rows"] == d["rows"]))
    rows.append(("distinct event_ids", export["distinct_event_ids"], d["distinct_event_ids"],
                 export["distinct_event_ids"] == d["distinct_event_ids"]))
    rows.append(("duplicates in Druid", 0, d["rows"] - d["distinct_event_ids"], d["rows"] == d["distinct_event_ids"]))
    rows.append(("purchases", export["purchases"], d["purchases"], export["purchases"] == d["purchases"]))
    rows.append(("revenue", f"{export['revenue']:.2f}", f"{d['revenue']:.2f}", abs(export["revenue"] - d["revenue"]) < 0.005))
    for day in sorted(set(export["by_date"]) | set(d["by_date"])):
        e = export["by_date"].get(day, {"events": 0, "purchases": 0, "revenue": 0.0})
        g = d["by_date"].get(day, {"events": 0, "purchases": 0, "revenue": 0.0})
        same = (e["events"] == g["events"] and e["purchases"] == g["purchases"]
                and abs(e["revenue"] - (g["revenue"] or 0)) < 0.005)
        rows.append((f"  {day} events/purch/rev", f"{e['events']}/{e['purchases']}/{e['revenue']:.2f}",
                     f"{g['events']}/{g['purchases']}/{(g['revenue'] or 0):.2f}", same))
    return rows


def print_compare(rows, title):
    print(f"\n  {title}")
    print(f"  {'check':<30} {'curated (Iceberg)':>22} {'Druid':>22}")
    print("  " + "-" * 78)
    for name, a, b, ok in rows:
        print(f"  {name:<30} {str(a):>22} {str(b):>22}   {'OK' if ok else 'MISMATCH'}")
    print()


def all_ok(rows):
    return all(r[3] for r in rows)


# ----------------------------------------------------------------------------- spec
def day_floor(ms):
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)


def build_spec(datasource, input_source, interval):
    return {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": {"column": "event_time_ms", "format": "millis"},
                "dimensionsSpec": {"dimensions": [
                    "event_id", "user_id", "session_id", "event_type", "product_id", "category", "device",
                    "event_date", "event_time", "updated_at", "source_batch",
                    {"type": "double", "name": "price"},
                    {"type": "long", "name": "quantity"},
                    {"type": "double", "name": "revenue"},
                    {"type": "long", "name": "is_purchase"},
                ]},
                "granularitySpec": {"segmentGranularity": "day", "queryGranularity": "none",
                                    "rollup": False, "intervals": [interval]},
            },
            "ioConfig": {"type": "index_parallel", "inputSource": input_source,
                         "inputFormat": {"type": "parquet"},
                         "appendToExisting": False, "dropExisting": True},
            "tuningConfig": {"type": "index_parallel", "partitionsSpec": {"type": "dynamic"},
                             "maxNumConcurrentSubTasks": 1},
        },
    }


def stage_local(export_path, snapshot_id, vm4_ssh, vm4_dir):
    """Fallback: copy the Parquet files from HDFS to VM4's local disk."""
    target = f"{vm4_dir}/snapshot_id={snapshot_id}"
    with tempfile.TemporaryDirectory() as td:
        subprocess.run(["hdfs", "dfs", "-get", f"{export_path}/*.parquet", td], check=True)
        subprocess.run(["ssh", "-o", "BatchMode=yes", vm4_ssh, f"mkdir -p '{target}'"], check=True)
        subprocess.run(["rsync", "-a", f"{td}/", f"{vm4_ssh}:{target}/"], check=True)
    return {"type": "local", "baseDir": target, "filter": "*.parquet"}


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--router", required=True)
    ap.add_argument("--datasource", required=True)
    ap.add_argument("--hdfs-uri", required=True)
    ap.add_argument("--hdfs-root", required=True)
    ap.add_argument("--mode", choices=["hdfs", "local"], default="hdfs")
    ap.add_argument("--vm4-ssh", default="")
    ap.add_argument("--vm4-dir", default="/opt/clickpulse/druid-input")
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--timeout", type=int, default=900)
    args = ap.parse_args()

    export_json = hdfs_cat(f"{args.hdfs_root}/_control/druid_export_latest.json")
    if not export_json:
        raise SystemExit("No curated export found on HDFS - run the Spark job first")
    export = json.loads(export_json)
    druid = Druid(args.router, args.datasource)
    current = druid.stats()

    if args.verify_only:
        rows = compare(export, current)
        print_compare(rows, f"Druid '{args.datasource}' vs curated table snapshot {export['snapshot_id']}")
        print("  RESULT:", "Druid mirrors the curated table" if all_ok(rows) else "MISMATCH")
        return 0 if all_ok(rows) else 1

    marker_path = f"{args.hdfs_root}/_control/druid_synced.json"
    marker = json.loads(hdfs_cat(marker_path) or "{}")
    if (not args.force and marker.get("snapshot_id") == export["snapshot_id"]
            and current and all_ok(compare(export, current))):
        print(f"[druid] Druid already mirrors curated snapshot {export['snapshot_id']} "
              f"(task {marker.get('task_id')}) -> nothing to ingest (idempotent re-run)")
        return 0

    # interval = union of what Druid has now and what the export has, whole UTC days
    lo = export["min_event_time_ms"]
    hi = export["max_event_time_ms"]
    if current:
        lo, hi = min(lo, current["min_event_time_ms"]), max(hi, current["max_event_time_ms"])
    start, end = day_floor(lo), day_floor(hi) + timedelta(days=1)
    interval = f"{start:%Y-%m-%dT%H:%M:%SZ}/{end:%Y-%m-%dT%H:%M:%SZ}"

    if args.mode == "hdfs":
        path = export["path"] if export["path"].startswith("hdfs://") else f"{args.hdfs_uri}{export['path']}"
        input_source = {"type": "hdfs", "paths": f"{path}/*.parquet"}
    else:
        input_source = stage_local(export["path"], export["snapshot_id"], args.vm4_ssh, args.vm4_dir)

    spec = build_spec(args.datasource, input_source, interval)
    print(f"[druid] ingesting snapshot {export['snapshot_id']} ({export['rows']} rows) "
          f"into '{args.datasource}', REPLACE interval {interval}")
    print(f"[druid] inputSource = {json.dumps(input_source)}")
    task_id = druid.submit(spec)
    print(f"[druid] task submitted: {task_id}")

    deadline = time.time() + args.timeout
    while True:
        try:
            st = druid.task_status(task_id)
        except RuntimeError as e:          # task can be briefly unknown right after submit
            if time.time() > deadline:
                raise
            print(f"[druid]   status not available yet ({str(e)[:80]})")
            time.sleep(3)
            continue
        state = st.get("statusCode") or st.get("status")
        if state == "SUCCESS":
            print(f"[druid] task SUCCESS in {st.get('duration', 0) / 1000:.1f}s")
            break
        if state == "FAILED":
            raise SystemExit(f"[druid] task FAILED: {st.get('errorMsg')}\n"
                             f"        see {args.router}/unified-console.html#ingestion")
        if time.time() > deadline:
            raise SystemExit(f"[druid] task {task_id} still {state} after {args.timeout}s")
        print(f"[druid]   task {state} ...")
        time.sleep(5)

    print("[druid] waiting for segments to be queryable and totals to match the curated table ...")
    while True:
        current = druid.stats()
        rows = compare(export, current)
        if all_ok(rows):
            break
        if time.time() > deadline:
            print_compare(rows, "Druid vs curated (timed out)")
            raise SystemExit("[druid] Druid did not converge to the curated snapshot")
        time.sleep(3)
    print_compare(rows, f"Druid '{args.datasource}' now mirrors curated snapshot {export['snapshot_id']}")

    hdfs_put_text(marker_path, json.dumps({
        "snapshot_id": export["snapshot_id"], "task_id": task_id, "interval": interval,
        "rows": export["rows"], "revenue": export["revenue"],
        "synced_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
