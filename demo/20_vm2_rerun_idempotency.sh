#!/usr/bin/env bash
# =============================================================================
#  DEMO - RUN ON: VM2  (Anjanaa)   -- after day1 AND day2 have been processed
#  "Automate: orchestrate with Airflow and show that reruns change nothing."
#
#  1. fingerprints Kafka, HDFS raw + rejects, every Iceberg table and Druid
#  2. re-runs the full Airflow DAG for day1 (i.e. Day 1 AFTER Day 2) and day2
#  3. fingerprints again and compares
#
#     demo/20_vm2_rerun_idempotency.sh            # re-run day1 then day2
#     demo/20_vm2_rerun_idempotency.sh day1       # just one batch
# =============================================================================
source "$(dirname "$0")/../scripts/lib/common.sh"
expect_vm 2
if [[ $# -gt 0 ]]; then BATCHES=("$@"); else BATCHES=(day1 day2); fi
for b in "${BATCHES[@]}"; do validate_batch "$b"; done
export HDFS_ROOT KAFKA_BOOTSTRAP KAFKA_TOPIC KAFKA_CONTROL_TOPIC DRUID_DATASOURCE DRUID_ROUTER
FP="$CP_ROOT/pipeline/tools/fingerprint.py"
TMP="$(mktemp -d)"

banner "Idempotency proof: re-running ${BATCHES[*]} must change nothing"
step "Fingerprint BEFORE"
"$CP_VENV/bin/python" "$FP" snapshot "$TMP/before.json" || die "fingerprint failed"
python3 -m json.tool "$TMP/before.json" | sed 's/^/  /'
pause

for b in "${BATCHES[@]}"; do
  bash "$CP_ROOT/demo/10_vm2_run_pipeline.sh" "$b" || die "re-run of $b failed"
done

step "Fingerprint AFTER"
"$CP_VENV/bin/python" "$FP" snapshot "$TMP/after.json" || die "fingerprint failed"
banner "BEFORE vs AFTER"
"$CP_VENV/bin/python" "$FP" compare "$TMP/before.json" "$TMP/after.json"
rc=$?
echo "  (Gagan can now run demo/12_vm3_show_curated.sh after-rerun to show the audit rows: 0 inserts / 0 updates / 0 drops)"
rm -rf "$TMP"
exit $rc
