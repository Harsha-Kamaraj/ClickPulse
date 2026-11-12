#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM2  (Anjanaa)
#  Starts the Airflow scheduler + webserver in the background (safe to re-run).
#  UI: http://cp-vm2:8080   (user/password from config/cluster.env)
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
source "$CP_ROOT/scripts/lib/airflow_env.sh"
require_not_root
expect_vm 2

banner "Starting Airflow on VM2"
mkdir -p "$CP_LOGS/airflow"
airflow db migrate >/dev/null

start_bg() {  # name command...
  local name="$1"; shift
  local pidf="$CP_RUN/airflow-$name.pid"
  if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    info "airflow $name already running (pid $(cat "$pidf"))"; return 0
  fi
  nohup "$@" > "$CP_LOGS/airflow/$name.log" 2>&1 &
  echo $! > "$pidf"
  ok "airflow $name started (pid $!, log $CP_LOGS/airflow/$name.log)"
}

start_bg scheduler airflow scheduler
start_bg webserver airflow webserver --port "$AIRFLOW_PORT"
wait_for_port "$VM2_HOST" "$AIRFLOW_PORT" 120 "Airflow webserver"

step "Waiting for the clickpulse_pipeline DAG to be parsed"
for _ in $(seq 1 40); do
  airflow dags list 2>/dev/null | grep -q clickpulse_pipeline && break
  sleep 3
done
airflow dags list-import-errors 2>/dev/null | grep -v "No data found" || true
airflow dags unpause clickpulse_pipeline >/dev/null 2>&1 || true
airflow dags list 2>/dev/null | grep -E "dag_id|clickpulse" || warn "DAG not visible yet - check $CP_LOGS/airflow/scheduler.log"

banner "Airflow UI: http://${VM2_HOST}:${AIRFLOW_PORT}   (${AIRFLOW_ADMIN_USER} / ${AIRFLOW_ADMIN_PASSWORD})"
