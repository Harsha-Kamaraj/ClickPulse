#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM3. Normally called by Airflow (VM2 -> ssh -> VM3); can be run by hand.
#  Usage: scripts/vm3/run_spark_job.sh day1
#  spark-submit in CLIENT mode -> the driver runs here on VM3, executors on the
#  VM3 worker, against the standalone master spark://cp-vm3:7077.
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
BATCH="${1:?usage: run_spark_job.sh <batch e.g. day1>}"
validate_batch "$BATCH"

exec "$SPARK_HOME/bin/spark-submit" \
  --master "$SPARK_MASTER_URL" \
  --deploy-mode client \
  --name "clickpulse-$BATCH" \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --executor-memory "$SPARK_EXECUTOR_MEMORY" \
  --conf spark.ui.showConsoleProgress=false \
  --py-files "$CP_ROOT/pipeline/spark/clickpulse_rules.py" \
  "$CP_ROOT/pipeline/spark/process_batch.py" \
  --batch "$BATCH" \
  --root "${HDFS_URI}${HDFS_ROOT}" \
  --warehouse "$ICEBERG_WAREHOUSE" 2> >(grep -v -E '^\S+ \S+ (INFO|WARN) ' >&2)
