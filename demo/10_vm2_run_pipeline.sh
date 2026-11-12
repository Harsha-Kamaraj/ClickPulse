#!/usr/bin/env bash
# =============================================================================
#  DEMO - RUN ON: VM2  (Anjanaa)
#  Triggers the Airflow DAG for one batch and follows it live, then prints the
#  important task output (producer, consumer, Spark batch report, Druid sync, verify).
#
#     demo/10_vm2_run_pipeline.sh day1
#     demo/10_vm2_run_pipeline.sh day2
#  (Same as clicking "Trigger DAG w/ config" {"batch":"day1"} in the Airflow UI.)
# =============================================================================
source "$(dirname "$0")/../scripts/lib/common.sh"
source "$CP_ROOT/scripts/lib/airflow_env.sh"
expect_vm 2
BATCH="${1:?usage: 10_vm2_run_pipeline.sh <day1|day2>}"
validate_batch "$BATCH"
[[ -f "$(batch_file "$BATCH")" ]] || die "No data file for batch $BATCH"

API="http://${VM2_HOST}:${AIRFLOW_PORT}/api/v1"
AUTH=(-u "${AIRFLOW_ADMIN_USER}:${AIRFLOW_ADMIN_PASSWORD}")
DAG=clickpulse_pipeline
RUN_ID="${BATCH}__$(date -u +%Y%m%dT%H%M%SZ)"

api() { curl -sf "${AUTH[@]}" -H 'Content-Type: application/json' "$@"; }

banner "Airflow: trigger $DAG for batch '$BATCH'" "run_id = $RUN_ID"
api -X POST "$API/dags/$DAG/dagRuns" -d "{\"dag_run_id\":\"$RUN_ID\",\"conf\":{\"batch\":\"$BATCH\"}}" >/dev/null \
  || die "Could not trigger the DAG via REST ($API). Is scripts/vm2/13_start_airflow.sh running?"
ok "triggered - follow along in the UI: http://${VM2_HOST}:${AIRFLOW_PORT}/dags/$DAG/grid"

TASKS=(preflight produce_to_kafka consume_to_hdfs spark_upsert_iceberg druid_ingest verify_druid_mirrors_table)
last=""
while true; do
  state="$(api "$API/dags/$DAG/dagRuns/$RUN_ID" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])')"
  tis="$(api "$API/dags/$DAG/dagRuns/$RUN_ID/taskInstances")"
  line="$(python3 - "$tis" "${TASKS[@]}" <<'EOF'
import json, sys
d = {t["task_id"]: t for t in json.loads(sys.argv[1])["task_instances"]}
sym = {"success": "✔", "running": "▶", "failed": "✘", "up_for_retry": "↻", "queued": "…",
       "scheduled": "…", "upstream_failed": "✘", "skipped": "-"}
out, dot = [], "\u00b7"
for t in sys.argv[2:]:
    s = (d.get(t) or {}).get("state") or "none"
    tries = (d.get(t) or {}).get("try_number") or 0
    out.append(f"{sym.get(s, '·')} {t}{f' (try {tries})' if tries and tries > 1 else ''}")
print("  |  ".join(out))
EOF
)"
  if [[ "$line" != "$last" ]]; then echo "  [$(date +%H:%M:%S)] $line"; last="$line"; fi
  [[ "$state" == "success" || "$state" == "failed" ]] && break
  sleep 4
done

show_log() {  # task_id title filter
  local tid="$1" title="$2" try
  try="$(echo "$tis" | python3 -c "import json,sys; print(next((t['try_number'] for t in json.load(sys.stdin)['task_instances'] if t['task_id']=='$tid'), 1) or 1)")"
  step "$title   (task $tid, try $try)"
  { curl -sf "${AUTH[@]}" -H 'Accept: text/plain' "$API/dags/$DAG/dagRuns/$RUN_ID/taskInstances/$tid/logs/$try?full_content=true" \
    || curl -sf "${AUTH[@]}" -H 'Accept: text/plain' "$API/dags/$DAG/dagRuns/$RUN_ID/taskInstances/$tid/logs/1?full_content=true"; } \
    | grep -E "\{subprocess\.py:[0-9]+\} INFO - " | sed -E 's/^.*\{subprocess\.py:[0-9]+\} INFO - //' \
    | grep -v -E "^(Tmp dir root location|Running command|Output:|Command exited with return code)" | sed 's/^/  /'
}

tis="$(api "$API/dags/$DAG/dagRuns/$RUN_ID/taskInstances")"
show_log preflight              "Preflight checks"
show_log produce_to_kafka       "VM1 producer -> Kafka"
show_log consume_to_hdfs        "VM2 consumer -> HDFS raw"
show_log spark_upsert_iceberg   "VM3 Spark -> Iceberg MERGE (batch report)"
show_log druid_ingest           "Druid ingestion"
show_log verify_druid_mirrors_table "Verification: Druid vs curated table"

if [[ "$state" == "success" ]]; then
  banner "DAG run $RUN_ID: SUCCESS"
else
  banner "DAG run $RUN_ID: FAILED - open http://${VM2_HOST}:${AIRFLOW_PORT}/dags/$DAG/grid for the logs"
  exit 1
fi
