#!/usr/bin/env bash
# Airflow environment (sourced after common.sh on VM2)
export AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER="$CP_ROOT/pipeline/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
export AIRFLOW__CORE__DEFAULT_TIMEZONE=utc
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=15
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT="$AIRFLOW_PORT"
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False
export AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
# the DAG's bash tasks need to find the repo + config
export CP_REPO="$CP_ROOT"
export PATH="$AIRFLOW_VENV/bin:$PATH"
