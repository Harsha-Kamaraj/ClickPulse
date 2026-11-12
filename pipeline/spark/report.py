#!/usr/bin/env python3
"""
ClickPulse curated-table inspector (VM3 demo). Read-only.

  --section all | tables | partitions | snapshots | audit | rejects | decisions | edge | daily
  --event-id evt_xxx   show one event's history (current row + every decision + rejects)
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NS = "cp.clickpulse"
EVENTS, REJECTS, LOG, AUDIT = f"{NS}.events", f"{NS}.rejects", f"{NS}.upsert_log", f"{NS}.batch_audit"


def h(title):
    print("\n" + "=" * 78 + f"\n  {title}\n" + "=" * 78)


def sql(spark, q, n=100, truncate=False):
    print(f"spark-sql> {' '.join(q.split())}")
    spark.sql(q).show(n, truncate=truncate)


def tables(spark):
    h("Iceberg tables in catalog 'cp' (Hadoop catalog on HDFS)")
    sql(spark, "SHOW TABLES IN cp.clickpulse")
    for t in (EVENTS, REJECTS, LOG, AUDIT):
        print(f"  {t:<28} rows = {spark.table(t).count()}")
    sql(spark, f"SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_event_ids FROM {EVENTS}")


def partitions(spark):
    h("Curated table is PARTITIONED BY event_date (UTC date of event_time)")
    sql(spark, f"SELECT partition.event_date AS event_date, record_count, file_count "
               f"FROM {EVENTS}.partitions ORDER BY event_date")


def snapshots(spark):
    h("Iceberg snapshots of the curated table (one per MERGE - re-runs add none)")
    sql(spark, f"""SELECT committed_at, snapshot_id, operation,
                   summary['added-records'] AS added, summary['deleted-records'] AS deleted,
                   summary['total-records'] AS total
                   FROM {EVENTS}.snapshots ORDER BY committed_at""")


def audit(spark):
    h("Batch audit - every run, including re-runs (inserts / updates / drops)")
    sql(spark, f"""SELECT batch_id, started_at, raw_lines AS raw, rejected_lines AS rejected,
                   duplicate_lines_collapsed AS dups, unique_events AS uniq, already_applied AS replayed,
                   inserts, late_arrival_inserts AS late, updates, date_moving_updates AS moved, drops,
                   table_rows_after AS rows_after,
                   CASE WHEN events_snapshot_before <=> events_snapshot_after THEN 'unchanged' ELSE 'CHANGED' END AS table
                   FROM {AUDIT} ORDER BY started_at""")


def rejects(spark):
    h("Rejects - separate Iceberg table (every invalid occurrence counted)")
    sql(spark, f"SELECT batch_id, primary_reason, count(*) AS n FROM {REJECTS} "
               f"GROUP BY batch_id, primary_reason ORDER BY batch_id, primary_reason")
    sql(spark, f"SELECT batch_id, line_no, primary_reason, reasons, event_id, substr(raw_line, 1, 60) AS raw_line "
               f"FROM {REJECTS} ORDER BY batch_id, line_no", n=200)


def decisions(spark):
    h("Upsert decisions other than plain inserts (updates, drops, late arrivals)")
    sql(spark, f"""SELECT batch_id, action, detail, count(*) AS n FROM {LOG}
                   GROUP BY batch_id, action, detail ORDER BY batch_id, action, detail""")
    sql(spark, f"""SELECT batch_id, event_id, action, detail, prev_event_time AS stored_time,
                   event_time AS incoming_time FROM {LOG}
                   WHERE NOT (action = 'INSERT' AND detail = 'NEW_EVENT')
                   ORDER BY batch_id, action, event_id""", n=200)


def daily(spark):
    h("Daily totals from the curated table (what Druid must mirror)")
    sql(spark, f"""SELECT event_date, count(*) AS events, sum(is_purchase) AS purchases,
                   round(sum(revenue), 2) AS revenue FROM {EVENTS}
                   GROUP BY event_date ORDER BY event_date""")
    sql(spark, f"""SELECT count(*) AS events, sum(is_purchase) AS purchases, round(sum(revenue), 2) AS revenue
                   FROM {EVENTS}""")


def first_snapshot_after(spark, batch):
    r = spark.sql(f"SELECT events_snapshot_after FROM {AUDIT} WHERE batch_id = '{batch}' "
                  f"AND (inserts + updates) > 0 ORDER BY started_at LIMIT 1").collect()
    return r[0][0] if r else None


def edge(spark):
    h("EDGE CASES (proved from the tables, Day 1 snapshot via Iceberg time travel)")
    snap_d1 = first_snapshot_after(spark, "day1")
    cols = "event_id, event_type, event_time, event_date, price, quantity, revenue, source_batch"

    print("\n[1] UPDATE that changes event_time -> event moves to its new date and exists exactly once")
    r = spark.sql(f"SELECT event_id FROM {LOG} WHERE detail LIKE 'MOVED_DATE%' "
                  f"ORDER BY event_id LIMIT 1").collect()
    if r:
        eid = r[0][0]
        if snap_d1:
            print(f"  Day-1 version (snapshot {snap_d1}):")
            sql(spark, f"SELECT {cols} FROM {EVENTS} VERSION AS OF {snap_d1} WHERE event_id = '{eid}'")
        print("  Current version:")
        sql(spark, f"SELECT {cols} FROM {EVENTS} WHERE event_id = '{eid}'")
        sql(spark, f"SELECT count(*) AS copies_in_table FROM {EVENTS} WHERE event_id = '{eid}'")
    else:
        print("  (none yet - run day2)")

    print("\n[2] LATE ARRIVAL -> inserted into a date that was already loaded; that date's totals change")
    r = spark.sql(f"SELECT event_id, event_date FROM {LOG} WHERE detail = 'LATE_ARRIVAL_FOR_LOADED_DATE' "
                  f"ORDER BY event_date, event_id LIMIT 1").collect()
    if r:
        eid, d = r[0][0], r[0][1]
        sql(spark, f"SELECT {cols} FROM {EVENTS} WHERE event_id = '{eid}'")
        if snap_d1:
            print("  Per-date reconciliation: after_day1 + late_inserts + moved_in - moved_out = now")
            sql(spark, f"""
                WITH d1 AS (SELECT event_date, count(*) AS n FROM {EVENTS} VERSION AS OF {snap_d1} GROUP BY event_date),
                     now AS (SELECT event_date, count(*) AS n, round(sum(revenue),2) AS rev FROM {EVENTS} GROUP BY event_date),
                     late AS (SELECT event_date, count(*) AS n FROM {LOG}
                              WHERE detail = 'LATE_ARRIVAL_FOR_LOADED_DATE' GROUP BY event_date),
                     min AS (SELECT event_date, count(*) AS n FROM {LOG}
                             WHERE action = 'UPDATE' AND event_date <> prev_event_date GROUP BY event_date),
                     mout AS (SELECT prev_event_date AS event_date, count(*) AS n FROM {LOG}
                              WHERE action = 'UPDATE' AND event_date <> prev_event_date GROUP BY prev_event_date)
                SELECT d1.event_date, d1.n AS after_day1, coalesce(late.n,0) AS late_inserts,
                       coalesce(min.n,0) AS moved_in, coalesce(mout.n,0) AS moved_out, now.n AS now, now.rev AS revenue_now
                FROM d1 JOIN now USING (event_date) LEFT JOIN late USING (event_date)
                LEFT JOIN min USING (event_date) LEFT JOIN mout USING (event_date)
                ORDER BY event_date""")
    else:
        print("  (none yet - run day2)")

    print("\n[3] INVALID UPDATE (later event_time but negative value) -> rejected, stored valid row kept")
    r = spark.sql(f"""SELECT r.event_id, r.batch_id, r.primary_reason, r.raw_line FROM {REJECTS} r
                      JOIN {EVENTS} e ON r.event_id = e.event_id
                      WHERE r.batch_id <> e.source_batch AND r.primary_reason LIKE 'NEGATIVE%'
                      ORDER BY r.event_id LIMIT 1""").collect()
    if r:
        eid = r[0][0]
        print(f"  rejected in {r[0][1]} ({r[0][2]}): {r[0][3]}")
        sql(spark, f"SELECT {cols} FROM {EVENTS} WHERE event_id = '{eid}'")
    else:
        print("  (none yet - run day2)")

    print("\n[4] DROPS -> incoming event_time equal/earlier than stored, stored row unchanged")
    sql(spark, f"""SELECT l.batch_id, l.event_id, l.detail, l.event_time AS incoming_time,
                   e.event_time AS stored_time, e.source_batch
                   FROM {LOG} l JOIN {EVENTS} e USING (event_id)
                   WHERE l.action = 'DROP' ORDER BY l.batch_id, l.event_id""", n=50)

    print("\n[5] RE-RUNS -> no inserts, updates or drops, table snapshot unchanged")
    sql(spark, f"""SELECT batch_id, started_at, already_applied AS replayed, inserts, updates, drops,
                   CASE WHEN events_snapshot_before <=> events_snapshot_after THEN 'unchanged' ELSE 'CHANGED' END AS table
                   FROM {AUDIT} WHERE already_applied > 0 ORDER BY started_at""")

    print("\n[6] NO DUPLICATES in the curated table")
    sql(spark, f"SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_ids, "
               f"count(*) - count(DISTINCT event_id) AS duplicates FROM {EVENTS}")


def event_history(spark, eid):
    eid = eid.replace("'", "")
    h(f"History of {eid}")
    sql(spark, f"SELECT * FROM {EVENTS} WHERE event_id = '{eid}'")
    sql(spark, f"SELECT batch_id, action, detail, prev_event_time, event_time, logged_at FROM {LOG} "
               f"WHERE event_id = '{eid}' ORDER BY logged_at")
    sql(spark, f"SELECT batch_id, line_no, reasons, raw_line FROM {REJECTS} WHERE event_id = '{eid}'")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=False)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--section", default="all")
    ap.add_argument("--event-id")
    a = ap.parse_args()
    spark = (SparkSession.builder.appName("clickpulse-report")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.cp", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.cp.type", "hadoop")
             .config("spark.sql.catalog.cp.warehouse", a.warehouse)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    if a.event_id:
        event_history(spark, a.event_id)
    else:
        sections = {"tables": tables, "partitions": partitions, "snapshots": snapshots, "audit": audit,
                    "rejects": rejects, "decisions": decisions, "edge": edge, "daily": daily}
        order = list(sections) if a.section == "all" else a.section.split(",")
        for s in order:
            sections[s](spark)
    spark.stop()


if __name__ == "__main__":
    main()
