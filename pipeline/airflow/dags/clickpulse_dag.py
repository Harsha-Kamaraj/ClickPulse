"""
ClickPulse pipeline DAG  (Airflow on VM2)

    preflight -> produce (VM1) -> consume (VM2) -> spark (VM3) -> druid_sync (VM4 via HTTP) -> verify

Trigger (manually, one batch per run):
    airflow dags trigger clickpulse_pipeline --conf '{"batch": "day1"}'
or use demo/vm2_run_pipeline.sh day1

Every task is idempotent, so retries and full re-runs are safe:
  produce     skips a batch already published to Kafka (control-topic marker + checksum)
  consume     skips a batch already landed in HDFS (manifest + checksum)
  spark       records already applied for this batch are not re-applied -> 0 inserts/updates/drops,
              no new Iceberg snapshot, no new reject rows, no new export
  druid_sync  skips ingestion when Druid already mirrors the curated snapshot
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

REPO = os.environ.get("CP_REPO", "/opt/clickpulse/ClickPulse")
TASK = f"bash {REPO}/scripts/airflow/run_task.sh"

default_args = {
    "owner": "clickpulse",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="clickpulse_pipeline",
    description="Kafka -> HDFS -> Spark -> Iceberg -> Druid for one clickstream batch",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,          # batches are applied strictly one after another
    default_args=default_args,
    params={"batch": Param("day1", type="string", pattern="^[a-z0-9_]+$",
                           description="batch id; reads data/clicks_<batch>.jsonl on VM1")},
    tags=["clickpulse"],
    is_paused_upon_creation=False,
    doc_md=__doc__,
) as dag:
    batch = "{{ params.batch }}"

    preflight = BashOperator(task_id="preflight", bash_command=f"{TASK} preflight {batch}", retries=3)
    produce = BashOperator(task_id="produce_to_kafka", bash_command=f"{TASK} produce {batch}")
    consume = BashOperator(task_id="consume_to_hdfs", bash_command=f"{TASK} consume {batch}")
    spark = BashOperator(task_id="spark_upsert_iceberg", bash_command=f"{TASK} spark {batch}")
    druid = BashOperator(task_id="druid_ingest", bash_command=f"{TASK} druid_sync {batch}")
    verify = BashOperator(task_id="verify_druid_mirrors_table", bash_command=f"{TASK} verify {batch}", retries=1)

    preflight >> produce >> consume >> spark >> druid >> verify
