#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM3 (Gagan R). Read-only inspection of the Iceberg tables.
#  Usage:
#    scripts/vm3/run_report.sh                      # everything
#    scripts/vm3/run_report.sh audit,edge           # some sections
#        sections: tables partitions snapshots audit rejects decisions edge daily
#    scripts/vm3/run_report.sh --event evt_f3afdd8733   # one event's history
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
ARGS=(--section all)
if [[ "${1:-}" == "--event" ]]; then
  [[ "${2:-}" =~ ^[A-Za-z0-9_.-]+$ ]] || die "usage: run_report.sh --event <event_id>"
  ARGS=(--event-id "$2")
elif [[ -n "${1:-}" ]]; then
  ARGS=(--section "$1")
fi
"$SPARK_HOME/bin/spark-submit" \
  --master "$SPARK_MASTER_URL" --deploy-mode client --name clickpulse-report \
  --conf spark.ui.showConsoleProgress=false \
  "$CP_ROOT/pipeline/spark/report.py" \
  --warehouse "$ICEBERG_WAREHOUSE" "${ARGS[@]}" 2> >(grep -v -E '^\S+ \S+ (INFO|WARN) ' >&2)
