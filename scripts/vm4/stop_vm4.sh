#!/usr/bin/env bash
# RUN ON: VM4 (Abhishek) - stops the dashboard and Druid (segments are kept).
source "$(dirname "$0")/../lib/common.sh"
for f in dashboard druid; do
  pidf="$CP_RUN/$f.pid"
  if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; then kill "$(cat "$pidf")"; ok "stopped $f"; fi
  rm -f "$pidf"
done
pkill -f "streamlit run" 2>/dev/null || true
pkill -f "org.apache.druid.cli.Main" 2>/dev/null || true
pkill -f "org.apache.zookeeper.server.quorum.QuorumPeerMain" 2>/dev/null || true
ok "VM4 services stopped"
