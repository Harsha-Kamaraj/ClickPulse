#!/usr/bin/env bash
# =============================================================================
#  OPTIONAL developer test (any laptop with Docker) - NOT part of the demo.
#  Runs the real Spark + Iceberg job (pipeline/spark/process_batch.py) in a
#  Spark 3.5.3 container against a local-filesystem "HDFS", for the sequence
#     day1 -> day2 -> day1 (rerun) -> day2 (rerun)
#  and checks every batch report + final totals against the reference model.
#
#  Usage:  tests/local_spark_test.sh
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="apache/spark:3.5.3-scala2.12-java11-python3-ubuntu"
ICEBERG_JAR="iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
CACHE="$ROOT/tests/.cache"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/clickpulse-test.XXXX")"
mkdir -p "$CACHE"
trap 'rm -rf "$WORK"' EXIT

[[ -s "$CACHE/$ICEBERG_JAR" ]] || curl -fsSL -o "$CACHE/$ICEBERG_JAR" \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/$ICEBERG_JAR"

# simulate what the Kafka consumer lands in HDFS
for b in day1 day2; do
  d="$WORK/clickpulse/raw/batch_id=$b"; mkdir -p "$d"
  cp "$ROOT/data/clicks_$b.jsonl" "$d/"
  lines=$(wc -l < "$ROOT/data/clicks_$b.jsonl" | tr -d ' ')
  sha=$(shasum -a 256 "$ROOT/data/clicks_$b.jsonl" | cut -d' ' -f1)
  printf '{"batch_id":"%s","source_file":"clicks_%s.jsonl","file_sha256":"%s","lines":%s}\n' "$b" "$b" "$sha" "$lines" > "$d/_MANIFEST.json"
done

spark() {
  docker run --rm --user root -v "$ROOT:/repo:ro" -v "$WORK:/work" -v "$CACHE:/jars:ro" -w /repo "$IMAGE" \
    /opt/spark/bin/spark-submit --master 'local[2]' --jars "/jars/$ICEBERG_JAR" \
      --conf spark.ui.enabled=false --conf spark.driver.extraJavaOptions=-Dlog4j.rootLogger=WARN \
      --py-files /repo/pipeline/spark/clickpulse_rules.py "$@" 2>&1 | grep -v -E '^\s*(WARN|INFO)|^[0-9]{2}/[0-9]{2}/[0-9]{2} '
}

for b in day1 day2 day1 day2; do
  echo ">>>>>>>>>>>>>>>> processing $b"
  spark /repo/pipeline/spark/process_batch.py --batch "$b" --root file:///work/clickpulse --warehouse file:///work/clickpulse/warehouse
  cp "$WORK/clickpulse/_control/runs/batch_id=$b/latest.json" "$WORK/summary_$(ls "$WORK" | grep -c summary_)_$b.json"
done

echo ">>>>>>>>>>>>>>>> report"
spark /repo/pipeline/spark/report.py --root file:///work/clickpulse --warehouse file:///work/clickpulse/warehouse --section all

echo ">>>>>>>>>>>>>>>> comparing with reference model"
python3 "$ROOT/pipeline/reference/reference_model.py" --json day1="$ROOT/data/clicks_day1.jsonl" day2="$ROOT/data/clicks_day2.jsonl" \
  day1="$ROOT/data/clicks_day1.jsonl" day2="$ROOT/data/clicks_day2.jsonl" > "$WORK/expected.json"
python3 - "$WORK" <<'EOF'
import json, sys, glob, os
work = sys.argv[1]
exp = json.load(open(os.path.join(work, "expected.json")))
runs = sorted(glob.glob(os.path.join(work, "summary_*.json")))
keys = ["raw_lines","rejected_lines","reject_reasons","valid_lines","duplicate_lines_collapsed","unique_events",
        "already_applied","inserts","late_arrival_inserts","updates","date_moving_updates","drops","table_rows_after"]
bad = 0
for e, path in zip(exp["batches"], runs):
    got = json.load(open(path))
    for k in keys:
        if got[k] != e[k]:
            bad += 1; print(f"MISMATCH {os.path.basename(path)} {k}: spark={got[k]} expected={e[k]}")
exportj = json.load(open(os.path.join(work, "clickpulse/_control/druid_export_latest.json")))
t = exp["totals"]
for k in ["rows","revenue","purchases","by_date"]:
    if exportj[k] != t[k]:
        bad += 1; print(f"MISMATCH totals {k}: spark={exportj[k]} expected={t[k]}")
last = json.load(open(runs[-1])); third = json.load(open(runs[-2]))
for r in (third, last):
    if r["table_changed"] or not r["druid_export"].startswith("SKIPPED"):
        bad += 1; print("MISMATCH rerun changed the table/export:", r["run_id"])
print("ALL CHECKS PASSED" if bad == 0 else f"{bad} CHECK(S) FAILED")
sys.exit(1 if bad else 0)
EOF
