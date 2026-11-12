# 📊 ClickPulse: build your own analytics system

An end-to-end clickstream analytics platform for RetailMart. It ingests raw website events, rejects invalid records, collapses duplicates, upserts corrections and late arrivals into a curated Iceberg table, and serves revenue and purchase analytics from Druid on a dashboard. Airflow orchestrates it, and every step is safe to re-run.

**Team:** Harsha K (VM1) · Anjanaa (VM2) · Gagan R (VM3) · Abhishek (VM4). The VMs are connected over ZeroTier.
**👉 Who runs what, and the demo script: [EXECUTION_PLAN.md](EXECUTION_PLAN.md)**

## Architecture

```
            VM1 (Harsha)                 VM2 (Anjanaa)                  VM3 (Gagan)                    VM4 (Abhishek)
 ┌──────────────────────────┐   ┌──────────────────────────┐   ┌──────────────────────────┐   ┌──────────────────────────┐
 │ data/clicks_dayN.jsonl   │   │  Airflow  (orchestrates  │   │ Spark standalone         │   │ Apache Druid             │
 │      │                   │   │  every task, retries)    │   │  master + worker +       │   │  clickpulse_events       │
 │      ▼  producer         │   │                          │   │  driver (client mode)    │   │      │ Druid SQL         │
 │ Kafka broker 1 (+KRaft   │──▶│ consumer ──▶ HDFS        │──▶│  validate → rejects      │──▶│      ▼                   │
 │   controller) :9092/9093 │   │   NameNode + DataNode    │   │  dedupe → classify       │   │ Streamlit dashboard      │
 │ Kafka broker 2   :9094   │   │   /clickpulse/raw (exact)│◀──│  MERGE INTO Iceberg      │   │  KPIs, filters, charts   │
 │ topic RF=2, 3 partitions │   │   /clickpulse/warehouse  │   │  export Parquet snapshot │   │                          │
 └──────────────────────────┘   └──────────────────────────┘   └──────────────────────────┘   └──────────────────────────┘
        Producer ──▶ Kafka ──▶ HDFS ──▶ Spark ──▶ Iceberg ──▶ Druid ──▶ Dashboard            (all HDFS clients: VM1, VM3, VM4)
```

Airflow DAG `clickpulse_pipeline` (one run per batch, `{"batch": "day1"}`):

`preflight` → `produce_to_kafka` (ssh VM1) → `consume_to_hdfs` (VM2) → `spark_upsert_iceberg` (ssh VM3) → `druid_ingest` (HTTP to VM4) → `verify_druid_mirrors_table`

| Layer | Where | What is stored |
|---|---|---|
| Kafka `clickpulse.raw.clicks` | VM1 (2 brokers, RF 2) | every input line as one message, value byte-identical; headers carry batch, line no, file sha256. Retention is infinite |
| Kafka `clickpulse.control` (compacted) | VM1 | one `COMPLETE` marker per published batch (makes the producer idempotent) |
| HDFS `/clickpulse/raw/batch_id=X/` | VM2 | the input file rebuilt from Kafka, **byte-identical** to the source (sha256 checked), plus `_MANIFEST.json` |
| HDFS `/clickpulse/rejects/batch_id=X/` | VM2 | every invalid occurrence with its reasons (JSON) |
| Iceberg `cp.clickpulse.events` | HDFS warehouse | curated table, **partitioned by `event_date`** (UTC), one row per `event_id` |
| Iceberg `cp.clickpulse.rejects` | HDFS warehouse | separate rejects table (batch, line, reasons, raw line) |
| Iceberg `cp.clickpulse.upsert_log` | HDFS warehouse | every INSERT, UPDATE or DROP decision, with the stored and incoming times |
| Iceberg `cp.clickpulse.batch_audit` | HDFS warehouse | one row per run: rejects, duplicates, inserts, updates, drops, snapshot before and after |
| HDFS `/clickpulse/export/druid/snapshot_id=N/` | VM2 | Parquet export of the curated table's current state after each upsert |
| Druid `clickpulse_events` | VM4 | mirror of the curated table (rollup off, day segments) |

## How the requirements are met

| Requirement | Implementation |
|---|---|
| Kafka with two brokers in KRaft mode | [scripts/vm1/10_install_kafka.sh](scripts/vm1/10_install_kafka.sh): broker 1 is `broker,controller`, broker 2 is `broker`, no ZooKeeper |
| VM1, VM2, VM3 on the same HDFS, NameNode + DataNode on VM2 | one generated `core-site.xml`/`hdfs-site.xml` ([scripts/lib/hadoop.sh](scripts/lib/hadoop.sh)), `fs.defaultFS=hdfs://cp-vm2:9000` |
| Spark standalone; reads HDFS or the curated table, never Kafka | [pipeline/spark/process_batch.py](pipeline/spark/process_batch.py) reads `/clickpulse/raw` and Iceberg only |
| Iceberg curated table, upserts, event-date partitioning | `MERGE INTO cp.clickpulse.events … PARTITIONED BY (event_date)` |
| Raw data preserved | consumer rebuilds the file from Kafka and verifies sha256 against the producer's checksum before writing |
| Invalid records rejected, stored with reasons, separate location and table | [pipeline/spark/clickpulse_rules.py](pipeline/spark/clickpulse_rules.py) → `/clickpulse/rejects/` + `cp.clickpulse.rejects`, one row per occurrence |
| Duplicates collapse; latest `updated_at` wins | window `row_number()` per `event_id` ordered by `updated_at DESC, line_no DESC` |
| Upsert rule: insert, update if strictly later, otherwise drop | classified against the table, then `MERGE … WHEN MATCHED AND s.event_time > t.event_time THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`; drops counted and logged |
| Druid mirrors the curated table | Parquet snapshot export, then Druid native batch with `dropExisting=true` over the union interval; the verify task compares totals per date |
| Late arrivals, date-moving updates, invalid updates | proved in [pipeline/spark/report.py](pipeline/spark/report.py) `--section edge` (Iceberg time travel) |
| Reprocessing produces no new inserts, updates or drops | decision log keyed by `(batch, event_id, record_hash)`; producer, consumer, Spark and Druid each skip work that is already done. [demo/20_vm2_rerun_idempotency.sh](demo/20_vm2_rerun_idempotency.sh) fingerprints everything before and after |
| Dashboard: Druid SQL, date and device filters, revenue and purchase KPIs, revenue-by-category bars | [dashboard/app.py](dashboard/app.py) |
| Airflow orchestration, retries, safe reruns | [pipeline/airflow/dags/clickpulse_dag.py](pipeline/airflow/dags/clickpulse_dag.py) |

### Business rules
* **Valid event types:** `page_view`, `product_view`, `add_to_cart`, `remove_from_cart`, `purchase`.
* **Purchase:** `event_type = purchase`. **Revenue:** Σ `price × quantity` over purchase events only.
* **Event date:** the UTC date of `event_time`. A timestamp without an offset is taken as UTC.
* **Reject reasons:** `INVALID_JSON`, `NOT_A_JSON_OBJECT`, `MISSING_EVENT_ID`, `MISSING_USER_ID`, `MISSING_SESSION_ID`, `INVALID_EVENT_TYPE`, `INVALID_EVENT_TIME`, `NEGATIVE_PRICE`, `NEGATIVE_QUANTITY` (plus type errors such as `INVALID_PRICE`). Zero is valid.
* **Order of operations within a batch:** reject invalid lines, then collapse duplicates (latest `updated_at` wins), then compare with the table by `event_time`. An invalid update therefore never replaces a valid stored row.

## Repository layout

```
config/cluster.env            ZeroTier IPs, users, versions  (edit once, identical on all VMs)
data/                         clicks_day1.jsonl, clicks_day2.jsonl (from Siya-Moghe/project-datasets)
scripts/all/                  00_bootstrap_vm.sh                       - every VM
scripts/vm1/ (Harsha)         10_install_kafka  11_start_kafka  run_producer  stop_kafka
scripts/vm2/ (Anjanaa)        10_install_hdfs_airflow  11_start_hdfs  12_setup_ssh  13_start_airflow  run_consumer
scripts/vm3/ (Gagan)          10_install_spark  11_start_spark  run_spark_job  run_report  stop_spark
scripts/vm4/ (Abhishek)       10_install_druid_dashboard  11_start_druid  12_start_dashboard  stop_vm4
scripts/airflow/run_task.sh   what each Airflow task executes
scripts/admin/                reset_for_rehearsal.sh  (never after the final run)
demo/                         numbered demo scripts (see EXECUTION_PLAN.md)
pipeline/producer|consumer|spark|druid|airflow|reference|tools
dashboard/app.py              Streamlit + Druid SQL
tests/                        unit tests + Docker Spark/Iceberg integration test
```

## Testing (laptop, optional)

```bash
python3 -m unittest discover -s tests -v                 # validation rules
python3 pipeline/reference/reference_model.py data/clicks_day1.jsonl data/clicks_day2.jsonl   # expected numbers
tests/local_spark_test.sh                                # real Spark 3.5.3 + Iceberg in Docker: day1, day2, day1, day2 vs reference
```

The pipeline was tested on a laptop before the VMs were ready:
* The Spark job ran on Spark 3.5.3 + Iceberg 1.6.1, on both Ubuntu 22.04/Python 3.10 and Ubuntu 24.04/Python 3.12 with OpenJDK 11. Every batch report and the final totals match the reference model.
* The producer and consumer ran against a real Kafka 3.9.0 broker: the landed file was byte-identical to the source, reruns were skipped, and a reused batch ID was refused.
* The DAG imports in Airflow 2.10.5.
* The VM4 scripts ran unchanged on Ubuntu 24.04: install, start Druid 30.0.1, start the dashboard. Day 1 and then Day 2 were ingested through Druid's HDFS input source and Parquet reader. Druid matched the curated table on every per-date number (217 rows, 0 duplicates), and reruns were skipped. The Streamlit dashboard rendered against real Druid with no errors, including with filters applied.
