#!/usr/bin/env python3
"""
ClickPulse Spark batch job  (Spark standalone on VM3; driver on VM3)

  HDFS raw file  ->  validate  ->  rejects (HDFS dir + Iceberg table)
                 ->  de-duplicate (latest updated_at wins)
                 ->  classify vs curated table (INSERT / UPDATE / DROP / ALREADY_APPLIED)
                 ->  Iceberg MERGE INTO cp.clickpulse.events  (partitioned by event_date)
                 ->  upsert log + batch audit
                 ->  export current table state to Parquet for Druid

Spark reads ONLY from HDFS / the curated table - never from Kafka.

Upsert rule (by event_id):
  not in table                          -> INSERT (incl. late arrivals for already-loaded dates)
  event_time strictly later than stored -> UPDATE all fields (event may move to another event_date)
  event_time equal or earlier           -> DROP (stored row unchanged)

Re-runs: every decision is logged in cp.clickpulse.upsert_log keyed by
(batch_id, event_id, record_hash). Re-processing a batch finds its records
there (ALREADY_APPLIED) -> no inserts, updates or drops, no new table snapshot,
no new reject rows, no new Druid export.
"""
import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType, TimestampType)

import clickpulse_rules as R

NS = "cp.clickpulse"
EVENTS = f"{NS}.events"
REJECTS = f"{NS}.rejects"
UPSERT_LOG = f"{NS}.upsert_log"
AUDIT = f"{NS}.batch_audit"

PARSED_SCHEMA = StructType([
    StructField("line_no", LongType(), False),
    StructField("raw_line", StringType(), True),
    StructField("valid", BooleanType(), False),
    StructField("reasons", ArrayType(StringType()), True),
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", LongType(), True),
    StructField("device", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("event_date", DateType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("is_purchase", IntegerType(), True),
    StructField("record_hash", StringType(), True),
])

EVENT_COLS = ["event_id", "user_id", "session_id", "event_type", "product_id", "category", "price",
              "quantity", "device", "event_time", "updated_at", "event_date", "revenue", "is_purchase",
              "record_hash", "source_batch", "source_line", "processed_at"]


# ----------------------------------------------------------------------------- helpers
def jvm_fs(spark, path):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


def fs_exists(spark, path):
    fs, p = jvm_fs(spark, path)
    return fs.exists(p)


def fs_read_text(spark, path):
    fs, p = jvm_fs(spark, path)
    stream = fs.open(p)
    try:
        return spark._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()


def fs_write_text(spark, path, text):
    fs, p = jvm_fs(spark, path)
    out = fs.create(p, True)
    try:
        out.write(bytearray(text.encode("utf-8")))
    finally:
        out.close()


def current_snapshot(spark, table):
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    return rows[0][0] if rows else None


def parse_partition(rows):
    for line_no, line in rows:
        res = R.evaluate_line(line)
        rec = res["record"]
        if rec is None:
            yield (line_no, line, False, res["reasons"], res["event_id"]) + (None,) * 14
        else:
            yield (line_no, line, True, [], rec["event_id"], rec["user_id"], rec["session_id"],
                   rec["event_type"], rec["product_id"], rec["category"], rec["price"], rec["quantity"],
                   rec["device"], rec["event_time"], rec["updated_at"], rec["event_date"], rec["revenue"],
                   rec["is_purchase"], rec["record_hash"])


def read_raw_lines(spark, path):
    """Read the raw file splitting on '\\n' only; order lines by byte offset -> 1-based line numbers."""
    rdd = spark.sparkContext.newAPIHadoopFile(
        path,
        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "org.apache.hadoop.io.Text",
        conf={"textinputformat.record.delimiter": "\n"},
    )
    return (rdd.sortByKey(numPartitions=max(1, rdd.getNumPartitions()))
               .map(lambda kv: kv[1])
               .zipWithIndex()
               .map(lambda t: (t[1] + 1, t[0])))


def ensure_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS cp.clickpulse")
    cow = ("'format-version'='2', 'write.merge.mode'='copy-on-write', "
           "'write.update.mode'='copy-on-write', 'write.delete.mode'='copy-on-write'")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {EVENTS} (
            event_id STRING, user_id STRING, session_id STRING, event_type STRING,
            product_id STRING, category STRING, price DOUBLE, quantity BIGINT, device STRING,
            event_time TIMESTAMP, updated_at TIMESTAMP, event_date DATE,
            revenue DOUBLE, is_purchase INT,
            record_hash STRING, source_batch STRING, source_line BIGINT, processed_at TIMESTAMP)
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ({cow})""")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REJECTS} (
            batch_id STRING, line_no BIGINT, primary_reason STRING, reasons ARRAY<STRING>,
            event_id STRING, raw_line STRING, run_id STRING, rejected_at TIMESTAMP)
        USING iceberg
        PARTITIONED BY (batch_id)
        TBLPROPERTIES ({cow})""")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {UPSERT_LOG} (
            batch_id STRING, event_id STRING, record_hash STRING, action STRING, detail STRING,
            event_time TIMESTAMP, event_date DATE, prev_event_time TIMESTAMP, prev_event_date DATE,
            source_line BIGINT, run_id STRING, logged_at TIMESTAMP)
        USING iceberg
        PARTITIONED BY (batch_id)
        TBLPROPERTIES ({cow})""")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT} (
            run_id STRING, batch_id STRING, file_sha256 STRING, started_at TIMESTAMP, finished_at TIMESTAMP,
            raw_lines BIGINT, valid_lines BIGINT, rejected_lines BIGINT,
            duplicate_lines_collapsed BIGINT, identical_duplicate_lines BIGINT, superseded_versions BIGINT,
            unique_events BIGINT, already_applied BIGINT,
            inserts BIGINT, late_arrival_inserts BIGINT, updates BIGINT, date_moving_updates BIGINT, drops BIGINT,
            reject_reasons STRING, events_snapshot_before BIGINT, events_snapshot_after BIGINT,
            table_rows_after BIGINT, druid_export STRING, status STRING)
        USING iceberg
        TBLPROPERTIES ('format-version'='2')""")


# ----------------------------------------------------------------------------- export for Druid
def export_for_druid(spark, root, snapshot_id):
    path = f"{root}/export/druid/snapshot_id={snapshot_id}"
    df = spark.table(EVENTS).select(
        F.unix_millis("event_time").alias("event_time_ms"),
        F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
        "event_id", "user_id", "session_id", "event_type", "product_id", "category", "device",
        "price", "quantity", "revenue", "is_purchase",
        F.date_format("event_date", "yyyy-MM-dd").alias("event_date"),
        F.date_format("updated_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("updated_at"),
        "source_batch",
    )
    df.coalesce(1).write.mode("overwrite").parquet(path)

    exported = spark.read.parquet(path)
    tot = exported.agg(
        F.count(F.lit(1)).alias("rows"), F.countDistinct("event_id").alias("distinct_ids"),
        F.round(F.sum("revenue"), 2).alias("revenue"), F.sum("is_purchase").alias("purchases"),
        F.min("event_time_ms").alias("min_ms"), F.max("event_time_ms").alias("max_ms"),
    ).collect()[0]
    by_date = {r["event_date"]: {"events": r["events"], "purchases": int(r["purchases"] or 0),
                                 "revenue": float(r["revenue"] or 0.0)}
               for r in exported.groupBy("event_date").agg(
                   F.count(F.lit(1)).alias("events"), F.sum("is_purchase").alias("purchases"),
                   F.round(F.sum("revenue"), 2).alias("revenue")).collect()}
    stats = {
        "snapshot_id": snapshot_id,
        "path": path,
        "rows": tot["rows"],
        "distinct_event_ids": tot["distinct_ids"],
        "revenue": float(tot["revenue"] or 0.0),
        "purchases": int(tot["purchases"] or 0),
        "min_event_time_ms": tot["min_ms"],
        "max_event_time_ms": tot["max_ms"],
        "by_date": dict(sorted(by_date.items())),
        "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    fs_write_text(spark, f"{root}/_control/druid_export_latest.json", json.dumps(stats, indent=2))
    return stats


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", required=True)
    ap.add_argument("--root", required=True, help="HDFS root, e.g. hdfs://cp-vm2:9000/clickpulse")
    ap.add_argument("--warehouse", required=True, help="Iceberg warehouse, e.g. hdfs://cp-vm2:9000/clickpulse/warehouse")
    args = ap.parse_args()

    run_id = f"{args.batch}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    started = datetime.now(timezone.utc)
    t0 = time.time()

    spark = (SparkSession.builder
             .appName(f"clickpulse-{args.batch}")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.cp", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.cp.type", "hadoop")
             .config("spark.sql.catalog.cp.warehouse", args.warehouse)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    # ---------------------------------------------------------------- 1. raw input (HDFS)
    raw_dir = f"{args.root}/raw/batch_id={args.batch}"
    manifest_path = f"{raw_dir}/_MANIFEST.json"
    if not fs_exists(spark, manifest_path):
        raise SystemExit(f"No raw data for batch '{args.batch}' at {raw_dir} - run the consumer first")
    manifest = json.loads(fs_read_text(spark, manifest_path))
    raw_path = f"{raw_dir}/{manifest['source_file']}"

    parsed = spark.createDataFrame(read_raw_lines(spark, raw_path).mapPartitions(parse_partition), PARSED_SCHEMA)
    parsed = parsed.localCheckpoint(eager=True)
    raw_lines = parsed.count()
    if raw_lines != int(manifest["lines"]):
        raise SystemExit(f"Line count mismatch: HDFS file has {raw_lines}, manifest says {manifest['lines']}")

    # ---------------------------------------------------------------- 2. rejects
    invalid = parsed.filter(~F.col("valid"))
    rejected_lines = invalid.count()
    reason_counts = {r["primary_reason"]: r["n"] for r in
                     invalid.select(F.col("reasons")[0].alias("primary_reason"))
                            .groupBy("primary_reason").agg(F.count(F.lit(1)).alias("n")).collect()}

    existing_rej = spark.table(REJECTS).filter(F.col("batch_id") == args.batch).select("line_no")
    new_rejects = (invalid.join(existing_rej, "line_no", "left_anti")
                   .select(F.lit(args.batch).alias("batch_id"), "line_no",
                           F.col("reasons")[0].alias("primary_reason"), "reasons", "event_id", "raw_line",
                           F.lit(run_id).alias("run_id"), F.lit(started).cast("timestamp").alias("rejected_at")))
    new_reject_rows = new_rejects.count()
    if new_reject_rows:
        new_rejects.writeTo(REJECTS).append()
        # separate HDFS location (one JSON file per batch, every invalid occurrence)
        (spark.table(REJECTS).filter(F.col("batch_id") == args.batch).orderBy("line_no")
         .select("batch_id", "line_no", "primary_reason", "reasons", "event_id", "raw_line")
         .coalesce(1).write.mode("overwrite").json(f"{args.root}/rejects/batch_id={args.batch}"))

    # ---------------------------------------------------------------- 3. de-duplicate valid records
    valid = parsed.filter(F.col("valid"))
    valid_lines = valid.count()
    distinct_valid_lines = valid.select("raw_line").distinct().count()
    w = Window.partitionBy("event_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("line_no").desc())
    winners = valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    unique_events = winners.count()

    # ---------------------------------------------------------------- 4. replay detection
    applied = (spark.table(UPSERT_LOG).filter(F.col("batch_id") == args.batch)
               .select("event_id", "record_hash").distinct())
    already_applied = winners.join(applied, ["event_id", "record_hash"], "left_semi").count()
    candidates = winners.join(applied, ["event_id", "record_hash"], "left_anti")

    # ---------------------------------------------------------------- 5. classify vs curated table
    snap_before = current_snapshot(spark, EVENTS)
    current = spark.table(EVENTS).select(
        "event_id", F.col("event_time").alias("prev_event_time"),
        F.col("event_date").alias("prev_event_date"), F.col("record_hash").alias("prev_hash"))
    loaded_dates = spark.table(EVENTS).select(F.col("event_date").alias("_d")).distinct()

    c = candidates.join(current, "event_id", "left")
    c = c.join(loaded_dates, c.event_date == loaded_dates._d, "left")
    action = (F.when(F.col("prev_event_time").isNull(), "INSERT")
               .when(F.col("event_time") > F.col("prev_event_time"), "UPDATE")
               .otherwise("DROP"))
    detail = (F.when((F.col("_action") == "INSERT") & F.col("_d").isNotNull(), "LATE_ARRIVAL_FOR_LOADED_DATE")
               .when(F.col("_action") == "INSERT", "NEW_EVENT")
               .when((F.col("_action") == "UPDATE") & (F.col("event_date") != F.col("prev_event_date")),
                     F.concat(F.lit("MOVED_DATE "), F.col("prev_event_date").cast("string"), F.lit(" -> "),
                              F.col("event_date").cast("string")))
               .when(F.col("_action") == "UPDATE", "SAME_DATE")
               .when(F.col("record_hash") == F.col("prev_hash"), "IDENTICAL_COPY")
               .when(F.col("event_time") == F.col("prev_event_time"), "EQUAL_EVENT_TIME")
               .otherwise("OLDER_EVENT_TIME"))
    classified = (c.withColumn("_action", action).withColumn("_detail", detail).drop("_d")
                  .localCheckpoint(eager=True))   # freeze decisions BEFORE the table changes

    counts = {r["_action"]: r["n"] for r in classified.groupBy("_action").agg(F.count(F.lit(1)).alias("n")).collect()}
    inserts, updates, drops = counts.get("INSERT", 0), counts.get("UPDATE", 0), counts.get("DROP", 0)
    late_inserts = classified.filter(F.col("_detail") == "LATE_ARRIVAL_FOR_LOADED_DATE").count()
    moving_updates = classified.filter(F.col("_detail").startswith("MOVED_DATE")).count()

    # ---------------------------------------------------------------- 6. MERGE (upsert) into Iceberg
    processed_at = datetime.now(timezone.utc)
    upserts = (classified.filter(F.col("_action").isin("INSERT", "UPDATE"))
               .withColumn("source_batch", F.lit(args.batch))
               .withColumn("source_line", F.col("line_no"))
               .withColumn("processed_at", F.lit(processed_at).cast("timestamp"))
               .select(*EVENT_COLS))
    if inserts + updates > 0:
        upserts.createOrReplaceTempView("cp_batch_upserts")
        spark.sql(f"""
            MERGE INTO {EVENTS} t
            USING cp_batch_upserts s
            ON t.event_id = s.event_id
            WHEN MATCHED AND s.event_time > t.event_time THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")

    snap_after = current_snapshot(spark, EVENTS)
    tbl = spark.table(EVENTS).agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("event_id").alias("ids")).collect()[0]
    if tbl["rows"] != tbl["ids"]:
        raise SystemExit(f"INTEGRITY ERROR: {tbl['rows']} rows but {tbl['ids']} distinct event_ids")

    # ---------------------------------------------------------------- 7. upsert log (decisions)
    if inserts + updates + drops > 0:
        (classified.select(
            F.lit(args.batch).alias("batch_id"), "event_id", "record_hash",
            F.col("_action").alias("action"), F.col("_detail").alias("detail"),
            "event_time", "event_date", "prev_event_time", "prev_event_date",
            F.col("line_no").alias("source_line"), F.lit(run_id).alias("run_id"),
            F.lit(processed_at).cast("timestamp").alias("logged_at"))
         .writeTo(UPSERT_LOG).append())

    # ---------------------------------------------------------------- 8. export for Druid (only if table changed)
    export_info = "SKIPPED (table unchanged)"
    export_stats = None
    latest_path = f"{args.root}/_control/druid_export_latest.json"
    prev_export = json.loads(fs_read_text(spark, latest_path)) if fs_exists(spark, latest_path) else None
    if snap_after is not None and (prev_export is None or prev_export.get("snapshot_id") != snap_after):
        export_stats = export_for_druid(spark, args.root, snap_after)
        export_info = export_stats["path"]
    elif prev_export:
        export_stats = prev_export

    # ---------------------------------------------------------------- 9. audit + summary
    finished = datetime.now(timezone.utc)
    summary = {
        "run_id": run_id, "batch_id": args.batch, "file_sha256": manifest.get("file_sha256"),
        "raw_lines": raw_lines, "valid_lines": valid_lines, "rejected_lines": rejected_lines,
        "new_reject_rows_written": new_reject_rows, "reject_reasons": dict(sorted(reason_counts.items())),
        "duplicate_lines_collapsed": valid_lines - unique_events,
        "identical_duplicate_lines": valid_lines - distinct_valid_lines,
        "superseded_versions": distinct_valid_lines - unique_events,
        "unique_events": unique_events, "already_applied": already_applied,
        "inserts": inserts, "late_arrival_inserts": late_inserts,
        "updates": updates, "date_moving_updates": moving_updates, "drops": drops,
        "events_snapshot_before": snap_before, "events_snapshot_after": snap_after,
        "table_changed": snap_before != snap_after, "table_rows_after": tbl["rows"],
        "druid_export": export_info, "duration_s": round(time.time() - t0, 1),
    }
    audit_row = spark.createDataFrame([(
        run_id, args.batch, summary["file_sha256"], started, finished,
        raw_lines, valid_lines, rejected_lines, summary["duplicate_lines_collapsed"],
        summary["identical_duplicate_lines"], summary["superseded_versions"], unique_events, already_applied,
        inserts, late_inserts, updates, moving_updates, drops,
        json.dumps(summary["reject_reasons"]), snap_before, snap_after, tbl["rows"], export_info, "SUCCESS",
    )], spark.table(AUDIT).schema)
    audit_row.writeTo(AUDIT).append()

    js = json.dumps(summary, indent=2, default=str)
    fs_write_text(spark, f"{args.root}/_control/runs/batch_id={args.batch}/latest.json", js)
    fs_write_text(spark, f"{args.root}/_control/runs/batch_id={args.batch}/{run_id}.json", js)

    print_report(summary, classified, export_stats)
    spark.stop()
    return 0


def print_report(s, classified, export_stats):
    line = "=" * 78
    print("\n" + line)
    print(f"  CLICKPULSE BATCH REPORT   batch={s['batch_id']}   run={s['run_id']}")
    print(line)
    print(f"  raw lines in HDFS ............ {s['raw_lines']:>6}")
    print(f"  rejected (invalid) ........... {s['rejected_lines']:>6}   new reject rows written: {s['new_reject_rows_written']}")
    for k, v in s["reject_reasons"].items():
        print(f"      - {k:<28} {v:>4}")
    print(f"  valid lines .................. {s['valid_lines']:>6}")
    print(f"  duplicates collapsed ......... {s['duplicate_lines_collapsed']:>6}   "
          f"(identical lines {s['identical_duplicate_lines']}, superseded versions {s['superseded_versions']})")
    print(f"  unique events in batch ....... {s['unique_events']:>6}")
    print(f"  already applied (re-run) ..... {s['already_applied']:>6}")
    print("  " + "-" * 40)
    print(f"  INSERTS ...................... {s['inserts']:>6}   (late arrivals for loaded dates: {s['late_arrival_inserts']})")
    print(f"  UPDATES ...................... {s['updates']:>6}   (moved to another event_date: {s['date_moving_updates']})")
    print(f"  DROPS ........................ {s['drops']:>6}")
    print("  " + "-" * 40)
    print(f"  curated rows after ........... {s['table_rows_after']:>6}")
    print(f"  Iceberg snapshot ............. {s['events_snapshot_before']} -> {s['events_snapshot_after']}"
          f"   ({'CHANGED' if s['table_changed'] else 'UNCHANGED'})")
    print(f"  Druid export ................. {s['druid_export']}")
    if export_stats:
        print(f"  table totals ................. revenue={export_stats['revenue']:.2f}  purchases={export_stats['purchases']}")
    print(line)
    notable = (classified.filter((F.col("_action") != "INSERT") | (F.col("_detail") != "NEW_EVENT"))
               .select("event_id", F.col("_action").alias("action"), F.col("_detail").alias("detail"),
                       F.date_format("prev_event_time", "yyyy-MM-dd HH:mm:ss").alias("stored_time"),
                       F.date_format("event_time", "yyyy-MM-dd HH:mm:ss").alias("incoming_time"))
               .orderBy("action", "event_id"))
    if notable.count():
        print("  Non-trivial decisions (updates, drops, late arrivals):")
        notable.show(200, truncate=False)
    sys.stdout.flush()


if __name__ == "__main__":
    sys.exit(main())
