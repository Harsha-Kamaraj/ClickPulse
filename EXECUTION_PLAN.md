# ClickPulse: execution plan (who runs what)

| VM  | Owner        | Runs on this VM                                                    | Host alias |
|-----|--------------|--------------------------------------------------------------------|------------|
| VM1 | **Harsha K** | Kafka broker 1 (+ KRaft controller), Kafka broker 2, producer      | `cp-vm1`   |
| VM2 | **Anjanaa**  | HDFS NameNode + DataNode, Airflow (scheduler + UI), Kafka consumer | `cp-vm2`   |
| VM3 | **Gagan R**  | Spark master + worker + driver (standalone), Iceberg tables        | `cp-vm3`   |
| VM4 | **Abhishek** | Druid (all services) + Streamlit dashboard                         | `cp-vm4`   |

VM2 is the control room. Airflow on VM2 starts the producer on VM1 and `spark-submit` on VM3 over SSH, and it sends the Druid ingestion to VM4 over HTTP.

Every script is idempotent. If one fails or the VM reboots, run it again.
Run scripts as your normal user, not with `sudo`. The scripts call sudo themselves where needed.

---

## Phase 0: one-time preparation (everyone, about 15 min)

1. **ZeroTier.** Every VM joins the team network: `sudo zerotier-cli join <network-id>`. Then authorise all four VMs in ZeroTier Central.
   Each person runs `ip -4 addr | grep zt` and posts their ZeroTier IP and Linux username in the group chat.
2. **Harsha** puts the four IPs and usernames into [config/cluster.env](config/cluster.env), then commits and pushes.
3. **Everyone** clones the repo to the same path on their VM:
   ```bash
   sudo mkdir -p /opt/clickpulse && sudo chown $USER /opt/clickpulse
   git clone https://github.com/Harsha-Kamaraj/ClickPulse.git /opt/clickpulse/ClickPulse
   cd /opt/clickpulse/ClickPulse
   ```
   If you cloned before Harsha pushed the IPs, run `git pull`.
4. **Everyone** runs:
   ```bash
   bash scripts/all/00_bootstrap_vm.sh
   ```
   This installs Java 11 and Python, writes `cp-vm1..cp-vm4` into `/etc/hosts`, creates `/opt/clickpulse`, and pings the other three VMs. All four must show *reachable* before you continue.

## Phase 1: install (each on your own VM, all in parallel, 10–20 min)

| Who          | Command                                            | Installs                                               |
|--------------|----------------------------------------------------|--------------------------------------------------------|
| Harsha (VM1) | `bash scripts/vm1/10_install_kafka.sh`             | Kafka 3.9 (2 KRaft brokers), Hadoop client, producer venv |
| Anjanaa (VM2)| `bash scripts/vm2/10_install_hdfs_airflow.sh`      | Hadoop 3.3.6 (formats the NameNode once), Airflow 2.10.5, consumer venv |
| Gagan (VM3)  | `bash scripts/vm3/10_install_spark.sh`             | Spark 3.5.3, Iceberg 1.6.1 jar, Hadoop client          |
| Abhishek (VM4)| `bash scripts/vm4/10_install_druid_dashboard.sh`  | Druid 30.0.1 (+ hdfs/parquet extensions, Hadoop conf), Streamlit |

## Phase 2: start the cluster (in this order)

| # | Who            | Command                                   | Expected result |
|---|----------------|-------------------------------------------|-----------------|
| 1 | Anjanaa (VM2)  | `bash scripts/vm2/11_start_hdfs.sh`       | `Live DataNodes: 1`, `/clickpulse/*` dirs |
| 2 | Harsha (VM1)   | `bash scripts/vm1/11_start_kafka.sh`      | 2 brokers, topics with `ReplicationFactor: 2` |
| 3 | Gagan (VM3)    | `bash scripts/vm3/11_start_spark.sh`      | `master status=ALIVE workers=1`, HDFS listing works |
| 4 | Abhishek (VM4) | `bash scripts/vm4/11_start_druid.sh`      | all Druid services healthy |
| 5 | Abhishek (VM4) | `bash scripts/vm4/12_start_dashboard.sh`  | dashboard on `:8501` (says "no data yet") |
| 6 | Anjanaa (VM2)  | `bash scripts/vm2/12_setup_ssh.sh` (once) | asks for VM1/VM3/VM4 passwords once; the owners type them |
| 7 | Anjanaa (VM2)  | `bash scripts/vm2/13_start_airflow.sh`    | DAG `clickpulse_pipeline` listed, UI on `:8080` |
| 8 | Anjanaa (VM2)  | `bash demo/00_cluster_health.sh`          | every line **UP** |

After any VM reboot, run that VM's start scripts again. Data on disk is kept.

## Phase 3: rehearse, then do the final run

1. Do a complete practice run with Phase 4 below.
2. Reset only after a rehearsal: on VM2, run `bash scripts/admin/reset_for_rehearsal.sh` and type `RESET`.
3. **Final run rule (from the brief).** The final Day 1 run must finish before Day 2. After the final Day 2 run, never reset Kafka, HDFS, the curated table or Druid again. `reset_for_rehearsal.sh` must not be used after that point.

If the evaluators want the pipeline run live, the demo itself is the final run. Do a rehearsal and reset beforehand, then run Phase 4 for real.
If you already did the final run before the demo, skip the two `10_vm2_run_pipeline.sh` steps. Show the inspection scripts instead, then run step 11, the rerun proof, which is always safe.

---

## Phase 4: the demo (about 25 min, speaker per step)

Open these in browser tabs beforehand (from any laptop on ZeroTier, using the IPs):
Airflow `http://<VM2>:8080` (admin/admin), HDFS `http://<VM2>:9870`, Spark `http://<VM3>:8080`,
Druid `http://<VM4>:8888`, Dashboard `http://<VM4>:8501`.

Scripts pause between sections; press ENTER to continue. Set `DEMO_NO_PAUSE=1` to run them straight through.

| Step | Who | Run on | Command | What to say / show |
|------|-----|--------|---------|--------------------|
| 0 | Anjanaa | VM2 | `bash demo/00_cluster_health.sh` | 4 VMs on ZeroTier, every service up. Architecture: Producer → Kafka → HDFS → Spark → Iceberg → Druid → Dashboard, orchestrated by Airflow |
| 1 | Harsha | VM1 | `bash demo/01_vm1_show_kafka.sh` | 2 brokers in **KRaft** mode (no ZooKeeper). Broker 1 is also the controller. Topic has 3 partitions and RF 2, so every partition has a copy on both brokers. Retention is infinite |
| 2 | Harsha, then Gagan | VM1, VM3 | `bash demo/02_show_hdfs_client.sh` | VM1 and VM3 read and write the **same HDFS** whose NameNode and DataNode are on VM2 |
| 3 | Anjanaa | VM2 | `bash demo/10_vm2_run_pipeline.sh day1` | Airflow grid view: preflight → produce (VM1) → consume (VM2) → Spark (VM3) → Druid ingest → verify. Retries=2 on every task. **Gagan** shows the running app in the Spark UI; **Abhishek** shows the ingestion task in the Druid console |
| 4 | Anjanaa | VM2 | `bash demo/11_vm2_show_raw_and_rejects.sh day1` | Raw file in HDFS is **byte-identical** to the source (same sha256, 144 lines, including bad ones). 16 rejects with reasons, in a separate HDFS dir |
| 5 | Gagan | VM3 | `bash demo/12_vm3_show_curated.sh after-day1` | Iceberg table partitioned by `event_date`, 116 rows. Rejects table. Audit row (116 inserts) |
| 6 | Abhishek | VM4 | `bash demo/13_vm4_show_druid.sh`, then the dashboard | Druid equals the curated table and the reference model. Dashboard: **Revenue 4,320.98, Purchases 15**. Filters for date range and device. Bar chart of revenue by category |
| 7 | Anjanaa | VM2 | `bash demo/10_vm2_run_pipeline.sh day2` | Same DAG, Day 2 **upserts** into the existing table (no rebuild): 101 inserts (11 late), 12 updates, 12 drops |
| 8 | Gagan | VM3 | `bash demo/12_vm3_show_curated.sh after-day2` | Edge cases, shown with Iceberg time travel. Each matches a spec example (see below) |
| 9 | Abhishek | VM4 | `bash demo/13_vm4_show_druid.sh`, then refresh the dashboard | **Revenue 10,061.31, Purchases 33**. Druid mirrors the table. Event lookup `evt_f3afdd8733` gives 1 row, dated 2025-01-04 |
| 10 | Harsha | VM1 | `bash demo/01_vm1_show_kafka.sh` | 144 + 148 messages; each batch byte-identical to its file; control markers |
| 11 | Anjanaa | VM2 | `bash demo/20_vm2_rerun_idempotency.sh` | Reruns the **whole DAG** for Day 1 (after Day 2) and Day 2. Before/after fingerprint: Kafka offsets, HDFS raw and rejects, Iceberg tables, Druid segments are all **UNCHANGED** |
| 12 | Gagan, then Abhishek | VM3, VM4 | `bash demo/12_vm3_show_curated.sh after-rerun`, refresh dashboard | Audit rows for the reruns: 0 inserts, 0 updates, 0 drops (116 / 125 already applied). Same snapshot. Same KPIs |

Useful extras during Q&A:
- Gagan: `bash demo/12_vm3_show_curated.sh event evt_129bdeb398` shows the full history of one event: current row, every decision, and its rejects.
- Anyone on VM2: `bash scripts/airflow/run_task.sh verify day2` checks Druid against the curated table again.
- Anyone: `python3 pipeline/reference/reference_model.py data/clicks_day1.jsonl data/clicks_day2.jsonl` prints the expected numbers from plain Python.

### Edge cases from the brief: where each one shows up (step 8)

| Brief example | In our data | Result |
|---------------|-------------|--------|
| evt_A: Day 2 has a later `event_time` with a new value | `evt_74686fcb68` (quantity 1→2, 2025-01-02 → 2025-01-04), 12 such events in total | **UPDATE**. Counts under the new date and appears exactly once in the table and in Druid |
| evt_B: Day 2 has an earlier time, or an identical copy | `evt_cc120fac4a` (earlier time), `evt_9d801b43bf` (identical copy), 12 in total | **DROP**. Stored row unchanged. Reported in the audit and the upsert log |
| evt_C: new event dated an already-loaded day | `evt_00627c60de` (2025-01-02), 11 in total | **INSERT**. The day's totals change; the per-date reconciliation shows it |
| evt_D: stored event, later time, but `price = -5` | `evt_129bdeb398` (price −14.99), `evt_b28a29110d` (quantity −3) | **REJECT**. Stored valid row kept |
| Reprocess either file, including Day 1 after Day 2 | step 11 | 0 inserts, 0 updates, 0 drops. No new snapshot. Druid not re-ingested |

### Expected numbers (checked with the reference model and a real Spark + Iceberg run)

| | Day 1 | Day 2 | Rerun Day 1 | Rerun Day 2 |
|---|---|---|---|---|
| raw lines in HDFS | 144 | 148 | 144 | 148 |
| rejected (every occurrence) | 16 | 15 | 16 (0 new rows) | 15 (0 new rows) |
| duplicates collapsed | 12 | 8 | 12 | 8 |
| unique events | 116 | 125 | 116 | 125 |
| inserts / late arrivals | 116 / 0 | 101 / 11 | 0 | 0 |
| updates (date moved) | 0 | 12 (12) | 0 | 0 |
| drops | 0 | 12 | 0 | 0 |
| already applied | 0 | 0 | 116 | 125 |
| curated rows after | 116 | 217 | 217 | 217 |
| revenue / purchases | 4,320.98 / 15 | 10,061.31 / 33 | unchanged | unchanged |

Day 1 rejects: INVALID_EVENT_TIME 2, INVALID_EVENT_TYPE 2, INVALID_JSON 2, MISSING_EVENT_ID 2, MISSING_SESSION_ID 1, MISSING_USER_ID 1, NEGATIVE_PRICE 2, NEGATIVE_QUANTITY 4.
Day 2 rejects: INVALID_EVENT_TIME 2, INVALID_EVENT_TYPE 2, INVALID_JSON 3, MISSING_EVENT_ID 1, MISSING_SESSION_ID 1, MISSING_USER_ID 2, NEGATIVE_PRICE 2, NEGATIVE_QUANTITY 2.

After Day 2, per event date:

| event_date | events | purchases | revenue |
|---|---|---|---|
| 2025-01-01 | 57 | 7 | 3,289.67 |
| 2025-01-02 | 32 | 3 | 99.53 |
| 2025-01-03 | 26 | 1 | 260.84 |
| 2025-01-04 | 25 | 8 | 3,182.09 |
| 2025-01-05 | 77 | 14 | 3,229.18 |

Revenue by category: Electronics 7,158.29 · Sports 835.15 · Home & Kitchen 790.90 · Grocery 571.93 · Fashion 442.62 · Books 210.22 · Beauty 52.20.

---

## Likely viva questions and short answers

- **Why Iceberg?** It supports `MERGE INTO` (upserts), hidden or identity partitioning by `event_date`, snapshots and time travel (we use these to prove the edge cases), and it needs only HDFS (a Hadoop catalog, no Hive metastore).
- **Drop vs reject?** A reject is an *invalid* record: bad JSON, missing ID, unknown type, bad time, negative value. It is stored with its reasons in `/clickpulse/rejects/` and `cp.clickpulse.rejects`. A drop is a *valid* record whose `event_time` is not newer than the stored one, logged in `cp.clickpulse.upsert_log` and counted in `batch_audit.drops`.
- **Which version wins within a file?** Invalid lines are removed first. Among the valid versions of an `event_id`, the latest `updated_at` wins, wherever it is in the file; ties go to the later line. Identical lines collapse to one.
- **Why does a rerun give 0 drops and not N drops?** Every decision is logged per `(batch_id, event_id, record_hash)`. On a rerun, those records are recognised as *already applied*, so nothing is inserted, updated or dropped again. The curated table gets no new snapshot. Even without the log, the table would end up identical; the log is what keeps the counts honest.
- **How does Druid mirror the table?** After every MERGE, Spark exports the full current table to Parquet on HDFS, tagged with the snapshot id. Druid then does a native batch ingestion with `dropExisting=true` over an interval covering old and new data, so the datasource is replaced. An event that moved dates exists only on its new date. The verify task compares Druid SQL with the curated totals, per date.
- **How do you know the numbers are right?** `pipeline/reference/reference_model.py` re-implements the spec in plain Python. Druid is checked against it in `demo/13_vm4_show_druid.sh`.
- **Spark reads from Kafka?** No. Spark reads only the raw file in HDFS and the Iceberg tables. Kafka → HDFS is done by the consumer on VM2.
- **Where does the Spark driver run?** `spark-submit --deploy-mode client` is launched on VM3 (over SSH from Airflow), so the driver, master and worker are all on VM3.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| A script says `meant for VM<n>` | You are on the wrong VM, or the IP in `config/cluster.env` is wrong. `CP_FORCE=1` overrides the check |
| VMs can't ping each other | ZeroTier: is the member authorised in ZeroTier Central? Check `sudo zerotier-cli listnetworks` |
| Download is very slow or fails | Copy the tarball into `/opt/clickpulse/downloads/` (any VM can `scp` it to another) and re-run |
| HDFS `Connection refused` from VM1/VM3 | Run `scripts/vm2/11_start_hdfs.sh` on VM2. Check `getent hosts cp-vm2` shows the ZeroTier IP |
| HDFS write from VM1/VM3 hangs at the DataNode | The DataNode port 9866 must be reachable. Check `nc -zv cp-vm2 9866` and that the firewall is off (`sudo ufw disable`) |
| Airflow task `produce_to_kafka` or `spark_upsert_iceberg` fails with ssh errors | Re-run `scripts/vm2/12_setup_ssh.sh`. The repo must exist at `/opt/clickpulse/ClickPulse` on VM1/VM3 |
| `druid_ingest` fails reading HDFS | Check the task log in the Druid console (Ingestion tab). As a fallback, set `DRUID_INPUT_MODE="local"` in `config/cluster.env` on VM2; VM2 then copies the Parquet files to VM4 over SSH |
| Druid won't start (memory) | `DRUID_MEMORY` must be at least `4250m` (default 6g). Give VM4 at least 8 GB RAM, or set 5g |
| Spark job: `Python worker failed` on Python 3.12 | Install python3.11 and set `export PYSPARK_PYTHON=python3.11` in `/opt/clickpulse/spark/conf/spark-env.sh` |
| Logs | `/opt/clickpulse/logs/` on every VM; Airflow task logs in the UI |
