#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM4  (Abhishek)
#  Starts Druid (all services on this VM) in the background. Safe to re-run.
#  Console: http://cp-vm4:8888
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
require_not_root
expect_vm 4

banner "Starting Druid on VM4 (memory ${DRUID_MEMORY})"
mkdir -p "$CP_LOGS/druid"
if port_open localhost 8888; then
  info "Druid router already listening on :8888"
else
  cd "$DRUID_HOME" || die "Druid not installed"
  nohup bin/start-druid -m "$DRUID_MEMORY" > "$CP_LOGS/druid/start-druid.log" 2>&1 &
  echo $! > "$CP_RUN/druid.pid"
  info "start-druid pid $(cat "$CP_RUN/druid.pid"), log $CP_LOGS/druid/start-druid.log"
  for _ in $(seq 1 120); do
    port_open localhost 8888 && break
    if ! kill -0 "$(cat "$CP_RUN/druid.pid")" 2>/dev/null; then
      tail -20 "$CP_LOGS/druid/start-druid.log" >&2
      die "start-druid exited (see above). If it is about memory, raise DRUID_MEMORY in config/cluster.env (minimum 4250m)."
    fi
    sleep 2
  done
fi

wait_for_port localhost 8888 240 "Druid router"
step "Waiting for Druid services to report healthy"
for svc in "8081:coordinator/overlord" "8082:broker" "8083:historical" "8091:middleManager"; do
  port="${svc%%:*}"; name="${svc#*:}"
  for _ in $(seq 1 90); do
    [[ "$(curl -s "http://localhost:${port}/status/health")" == "true" ]] && break
    sleep 2
  done
  if [[ "$(curl -s "http://localhost:${port}/status/health")" == "true" ]]; then ok "$name healthy"; else warn "$name not healthy yet"; fi
done
curl -s "http://localhost:8888/status" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("  Druid version", d.get("version")); print("  extensions:", ", ".join(m.get("artifact","?") for m in d.get("modules",[]) if "hdfs" in m.get("artifact","") or "parquet" in m.get("artifact","")))' || true

banner "Druid console: http://${VM4_HOST}:8888"
