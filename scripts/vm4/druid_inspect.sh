#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM4  (Abhishek)
#  Shows what Druid holds for the ClickPulse datasource: columns, segments,
#  ingestion tasks, row count.  Read-only.
#
#  --drop  deletes the datasource (all its segments). Only for removing data that
#          was NOT loaded by the pipeline. NEVER use it after the final Day 2 run.
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
require_not_root
expect_vm 4

DRUID="http://localhost:8888"
DS="$DRUID_DATASOURCE"
DQ() { echo "${C_DIM}druid-sql> $1${C_RESET}"; python3 "$CP_ROOT/pipeline/druid/druid_query.py" "$DRUID" "$1"; echo; }

port_open localhost 8888 || die "Druid router is not up on :8888 - run scripts/vm4/11_start_druid.sh"

banner "Druid datasource '$DS'"
step "All datasources Druid knows about"
DQ "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid' ORDER BY 1"

step "Columns of '$DS' (the pipeline creates event_id, event_type, device, category, revenue, source_batch, ...)"
DQ "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '$DS' ORDER BY ORDINAL_POSITION"

step "Segments"
DQ "SELECT \"start\", \"end\", \"size\" AS size_bytes, is_available, is_published
    FROM sys.segments WHERE datasource = '$DS' ORDER BY \"start\""

step "Ingestion tasks for '$DS'"
DQ "SELECT task_id, type, status, created_time FROM sys.tasks WHERE datasource = '$DS' ORDER BY created_time"

step "Row count"
DQ "SELECT COUNT(*) AS \"rows\", MIN(__time) AS first_time, MAX(__time) AS last_time FROM \"$DS\""

if [[ "${1:-}" == "--drop" ]]; then
  echo
  warn "This deletes every segment of Druid datasource '$DS'."
  warn "Do NOT do this after the final Day 2 run - Druid must keep mirroring the curated table."
  read -r -p "Type DROP to continue: " ans
  [[ "$ans" == "DROP" ]] || die "Cancelled."
  step "Marking all segments unused and submitting a kill task"
  curl -s -X DELETE "$DRUID/druid/coordinator/v1/datasources/$DS" >/dev/null || true
  sleep 3
  curl -s -X POST -H 'Content-Type: application/json' \
    -d "{\"type\":\"kill\",\"dataSource\":\"$DS\",\"interval\":\"1000-01-01/3000-01-01\"}" \
    "$DRUID/druid/indexer/v1/task"; echo
  ok "Datasource '$DS' dropped. It disappears from Druid SQL within about a minute."
  info "Re-run the druid_ingest task in Airflow (or the whole DAG) to load ClickPulse data again."
fi
