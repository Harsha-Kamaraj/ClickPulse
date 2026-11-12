#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM4  (Abhishek)
#  Starts the Streamlit dashboard (Druid SQL backed). Safe to re-run.
#  Open: http://cp-vm4:8501  (or http://<VM4 ZeroTier IP>:8501 from any laptop on the network)
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
require_not_root
expect_vm 4

banner "Starting ClickPulse dashboard"
mkdir -p "$CP_LOGS/dashboard"
pidf="$CP_RUN/dashboard.pid"
if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
  info "dashboard already running (pid $(cat "$pidf"))"
else
  CP_DRUID_URL="http://localhost:8888" CP_DRUID_DATASOURCE="$DRUID_DATASOURCE" \
  nohup "$CP_VENV/bin/streamlit" run "$CP_ROOT/dashboard/app.py" \
      --server.port "$DASHBOARD_PORT" --server.address 0.0.0.0 --server.headless true \
      --browser.gatherUsageStats false > "$CP_LOGS/dashboard/streamlit.log" 2>&1 &
  echo $! > "$pidf"
fi
wait_for_port localhost "$DASHBOARD_PORT" 60 "Dashboard"
banner "Dashboard: http://${VM4_HOST}:${DASHBOARD_PORT}   (http://${VM4_IP}:${DASHBOARD_PORT})"
