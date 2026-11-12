# ClickPulse: evaluation-day guide

What the evaluators check:

1. The pipeline was **already run for Day 1 and Day 2** before you arrive. The curated table and the rejects table hold the final data.
2. The **Druid datasource and the dashboard are already populated** with both days.
3. During the evaluation you **demonstrate the working infrastructure**, **verify the final data and dashboard against the expected results**, and do **one live idempotency test: rerun the same Day 2 input** through the existing pipeline.

So nothing is loaded for the first time during the evaluation. The only pipeline run you do live is the Day 2 rerun, and it must change nothing.

| VM | Person | Runs |
|----|--------|------|
| VM1 | Harsha | Kafka |
| VM2 | Anjanaa | HDFS, Airflow, drives the live rerun |
| VM3 | Gagan | Spark + Iceberg |
| VM4 | Abhishek | Druid + dashboard |

All commands run from the repo: `cd /opt/clickpulse/ClickPulse`. The demo scripts pause between sections; press ENTER to continue.

---

## Part A: before evaluation day (once)

### A1. Final run (Anjanaa drives it, everyone online)

If the rehearsal left the cluster in a clean state (Day 1, then Day 2, all numbers matching), you can keep it as the final run and go straight to A2. Extra audit rows from rehearsal reruns are fine, since they all show 0 changes. Otherwise start clean:

| # | Who | Command | Must show |
|---|-----|---------|-----------|
| 1 | Anjanaa | `bash scripts/admin/reset_for_rehearsal.sh` (type `RESET`) | everything emptied |
| 2 | Anjanaa | `bash demo/00_cluster_health.sh` | every line **UP** |
| 3 | Anjanaa | `bash demo/10_vm2_run_pipeline.sh day1` | all 6 Airflow tasks green; 144 raw, 16 rejected, 116 inserts |
| 4 | Anjanaa | `bash demo/10_vm2_run_pipeline.sh day2` | all 6 green; 101 inserts (11 late), 12 updates, 12 drops; 217 rows |
| 5 | Gagan | `bash demo/12_vm3_show_curated.sh after-day2` | numbers in the [expected results](#expected-results) table |
| 6 | Abhishek | `bash demo/13_vm4_show_druid.sh` + open the dashboard | Druid equals the curated table; **$10,061.31**, **33** |

Day 1 must finish before Day 2 starts. The script waits for the DAG run to end, so running them one after the other is enough.

### A2. Lock the final run

```bash
bash scripts/admin/mark_final_run.sh        # Anjanaa, on VM2
```

This writes `/clickpulse/_control/FINAL_RUN_DONE` on HDFS. From then on `reset_for_rehearsal.sh` refuses to run, so the final data cannot be wiped by accident. **The rules forbid resetting Kafka, HDFS, the curated table or Druid after the final Day 2 run.**

### A3. Shut down cleanly (data survives)

| Who | Command |
|-----|---------|
| Harsha | `bash scripts/vm1/stop_kafka.sh` |
| Anjanaa | `bash scripts/vm2/stop_all_vm2.sh` |
| Gagan | `bash scripts/vm3/stop_spark.sh` |
| Abhishek | `bash scripts/vm4/stop_vm4.sh` |

---

## Part B: evaluation day, before the evaluators arrive (about 10 min)

1. **Everyone:** start the Linux VM in UTM and check `sudo zerotier-cli listnetworks` shows `OK`.
2. Start the services, **in this order**:

| # | Who | Command |
|---|-----|---------|
| 1 | Anjanaa | `bash scripts/vm2/11_start_hdfs.sh` |
| 2 | Harsha | `bash scripts/vm1/11_start_kafka.sh` |
| 3 | Gagan | `bash scripts/vm3/11_start_spark.sh` |
| 4 | Abhishek | `bash scripts/vm4/11_start_druid.sh` then `bash scripts/vm4/12_start_dashboard.sh` |
| 5 | Anjanaa | `bash scripts/vm2/13_start_airflow.sh` |
| 6 | Anjanaa | `bash demo/00_cluster_health.sh` (every line **UP**) |

Only the **start** scripts. Never run an install script (`10_...`) or `reset_for_rehearsal.sh` on evaluation day.

3. Open these browser tabs (inside a VM use the `cp-vmN` names, from a Mac on ZeroTier use the IPs):

| UI | URL | Who shows it |
|----|-----|--------------|
| Airflow (admin / admin) | http://10.47.230.52:8080 | Anjanaa |
| HDFS NameNode | http://10.47.230.52:9870 | Anjanaa |
| Spark master | http://10.47.230.65:8080 | Gagan |
| Druid console | http://10.47.230.129:8888 | Abhishek |
| Dashboard | http://10.47.230.129:8501 | Abhishek |

4. Quick self-check that the final data is still there: Abhishek's dashboard shows **Rows in Druid: 217**, **Batches loaded: day1, day2**.

---

## Part C: during the evaluation (about 20 min)

### C1. Working infrastructure (5 min)

| Step | Who | Command / action | What to say |
|------|-----|------------------|-------------|
| 1 | Anjanaa | `bash demo/00_cluster_health.sh` | 4 VMs on one ZeroTier network; every service is up. Architecture: Producer → Kafka → HDFS → Spark → Iceberg → Druid → Dashboard, orchestrated by Airflow |
| 2 | Harsha | `bash demo/01_vm1_show_kafka.sh` | 2 brokers in **KRaft** mode, no ZooKeeper; broker 1 is also the controller. 3 partitions, replication factor 2, both replicas in sync. Retention forever. **144 + 148** messages, one per input line |
| 3 | Harsha, then Gagan | `bash demo/02_show_hdfs_client.sh` | VM1 and VM3 read and write the **same HDFS**; its NameNode and DataNode are on VM2 |
| 4 | Gagan | Spark UI tab | Spark standalone 3.5.3: master ALIVE, 1 worker (2 cores, 2 GB) on VM3. Completed applications are cleared on restart; the live rerun in step 11 shows up here as `clickpulse-day2` |
| 5 | Anjanaa | Airflow tab, DAG `clickpulse_pipeline` | the green Day 1 and Day 2 runs; 6 tasks, retries = 2 |

### C2. Verify the final data and dashboard (8 min)

| Step | Who | Command / action | Check against |
|------|-----|------------------|---------------|
| 6 | Anjanaa | `bash demo/11_vm2_show_raw_and_rejects.sh day1` then `... day2` | raw file in HDFS is **byte-identical** to the input (same sha256): 144 and 148 lines. Rejects with reasons in a separate HDFS folder: 16 and 15 |
| 7 | Gagan | `bash demo/12_vm3_show_curated.sh after-day2` | curated table partitioned by `event_date`, **217 rows, 0 duplicates**; **rejects table: 31 rows** (16 + 15); audit rows; edge cases (late arrivals, updates that move the date, invalid update rejected, drops); daily totals |
| 8 | Abhishek | `bash demo/13_vm4_show_druid.sh` | Druid totals and per-date numbers equal the curated table and the independent reference model |
| 9 | Abhishek | dashboard | **Revenue $10,061.31, Purchases 33**. Change the filters live: device **mobile**, dates **2025-01-04 to 2025-01-05** gives **$5,038.91** and **14**. Revenue-by-category bars add up to the total |
| 10 | anyone | `python3 pipeline/reference/reference_model.py data/clicks_day1.jsonl data/clicks_day2.jsonl` | the expected numbers computed independently in plain Python; they match everything above |

### C3. Live idempotency test: rerun Day 2 (5 min)

| Step | Who | Command / action | Must show |
|------|-----|------------------|-----------|
| 11 | Anjanaa | `bash demo/20_vm2_rerun_idempotency.sh day2` | fingerprints Kafka, HDFS raw and rejects, every Iceberg table and Druid, reruns the **whole Airflow DAG** for `day2`, fingerprints again. Every line **UNCHANGED**; the only change is `batch_audit` (+1 audit row, expected). The task logs say: producer *already published*, consumer *already landed with the same checksum*, Spark *125 already applied, 0 inserts / 0 updates / 0 drops*, Druid *already mirrors … nothing to ingest* |
| 12 | Gagan | `bash demo/12_vm3_show_curated.sh after-rerun` | a new `day2` audit row: **replayed 125, inserts 0, updates 0, drops 0, table `unchanged`**; still the same 2 snapshots; still 217 rows |
| 13 | Abhishek | `bash demo/13_vm4_show_druid.sh`, refresh the dashboard | same segments, **217 rows, $10,061.31, 33** |
| 14 | Harsha | `bash demo/01_vm1_show_kafka.sh` | still **144 + 148** messages: the rerun published nothing new |

While step 11 runs, Gagan can show the new Spark application in the Spark UI and Abhishek can show that no new ingestion task ran in the Druid console.

---

## Expected results

| What | Value |
|------|-------|
| Day 1 | 144 raw lines, 16 rejected, 12 duplicates collapsed, 116 inserts |
| Day 2 | 148 raw lines, 15 rejected, 8 duplicates collapsed, 125 unique events: 101 inserts (11 late arrivals), 12 updates (all move the date), 12 drops (6 identical copies, 6 older event time) |
| Curated table | 217 rows, 217 distinct `event_id`, 0 duplicates, 5 partitions |
| Rejects table | 31 rows (16 + 15), every occurrence counted |
| Totals | revenue **10,061.31**, purchases **33** |
| Druid | 217 rows, one segment per day (2025-01-01 … 2025-01-05) |
| Kafka | 144 + 148 messages; 2 control markers |
| Live Day 2 rerun | 0 inserts, 0 updates, 0 drops; nothing new in Kafka, HDFS or Druid |

| Event date | Events | Purchases | Revenue |
|------------|-------:|----------:|--------:|
| 2025-01-01 | 57 | 7 | 3,289.67 |
| 2025-01-02 | 32 | 3 | 99.53 |
| 2025-01-03 | 26 | 1 | 260.84 |
| 2025-01-04 | 25 | 8 | 3,182.09 |
| 2025-01-05 | 77 | 14 | 3,229.18 |

Event ids worth showing on request:

- `evt_f3afdd8733`: Day 1 on 2025-01-01, updated by Day 2 to 2025-01-04; exactly 1 row. Dashboard "Event lookup" or `bash demo/12_vm3_show_curated.sh event evt_f3afdd8733`.
- `evt_129bdeb398`: invalid Day 2 update (negative price) rejected; the valid Day 1 row (29.98) is kept.
- `evt_00627c60de`: late arrival inserted into 2025-01-02, a date Day 1 already loaded.

---

## Never on evaluation day

- `reset_for_rehearsal.sh` (it refuses anyway once the final run is locked).
- `demo/10_vm2_run_pipeline.sh day1` as a "fresh" load. The data is already final. The only live run is the `day2` rerun in step 11 (re-running is safe, but it is not what was asked).
- Install scripts (`10_install_...`), loading data through the Druid console, or deleting anything in HDFS.
- Running `hdfs` or `spark-submit` by hand with an old lab setup. Use the scripts; they always use `/opt/clickpulse`.

## If something goes wrong

| Symptom | Fix |
|---------|-----|
| A line is DOWN in the health check | that person reruns their start script from Part B. Firewall: `sudo ufw status`; allow `10.47.230.0/24` or `sudo ufw disable` |
| A VM can't reach the others | `sudo zerotier-cli listnetworks` must show `OK`; restart with `sudo systemctl restart zerotier-one` |
| An Airflow task is red during the rerun | open the task log in the Airflow UI; Airflow retries twice by itself. Re-running the same `day2` command is safe |
| Dashboard empty right after start | Druid is still loading segments; wait a minute and press **Refresh from Druid** |

Background, viva answers and the full troubleshooting table: [EXECUTION_PLAN.md](EXECUTION_PLAN.md).
