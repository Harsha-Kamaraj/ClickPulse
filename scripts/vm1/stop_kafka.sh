#!/usr/bin/env bash
# RUN ON: VM1 (Harsha K) - stops both brokers. Only for shutting the VM down;
# the project requires both brokers to stay up during runs.
source "$(dirname "$0")/../lib/common.sh"
for b in 2 1; do
  pid="$(pgrep -f "kafka.Kafka .*broker$b.properties" || true)"
  if [[ -n "$pid" ]]; then info "stopping broker$b (pid $pid)"; kill $pid; else info "broker$b not running"; fi
  for _ in $(seq 1 30); do pgrep -f "kafka.Kafka .*broker$b.properties" >/dev/null || break; sleep 1; done
done
ok "Kafka stopped"
