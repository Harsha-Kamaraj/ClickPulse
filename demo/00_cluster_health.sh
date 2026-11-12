#!/usr/bin/env bash
# =============================================================================
#  DEMO STEP 0 - RUN ON: any VM (Anjanaa on VM2 drives it in the demo)
#  One-screen health check of every service on all 4 VMs over ZeroTier.
# =============================================================================
source "$(dirname "$0")/../scripts/lib/common.sh"

banner "ClickPulse cluster health  ($(date -u '+%Y-%m-%d %H:%M:%S UTC'), from $(hostname))"

check() {  # vm label host port
  if port_open "$3" "$4"; then
    printf "  %-5s %-42s %-22s ${C_GREEN}UP${C_RESET}\n" "$1" "$2" "$3:$4"
  else
    printf "  %-5s %-42s %-22s ${C_RED}DOWN${C_RESET}\n" "$1" "$2" "$3:$4"; DOWN=$((DOWN + 1))
  fi
}
DOWN=0
step "ZeroTier reachability"
for n in 1 2 3 4; do
  hv="VM${n}_HOST"; iv="VM${n}_IP"
  if ping -c1 -W2 "${!hv}" >/dev/null 2>&1; then
    echo "  VM${n}  ${!hv} (${!iv})  ${C_GREEN}reachable${C_RESET}"
  else
    echo "  VM${n}  ${!hv} (${!iv})  ${C_RED}unreachable${C_RESET}"; DOWN=$((DOWN + 1))
  fi
done

step "Services"
check VM1 "Kafka broker 1 (KRaft controller listener)" "$VM1_HOST" 9093
check VM1 "Kafka broker 1"                               "$VM1_HOST" 9092
check VM1 "Kafka broker 2"                               "$VM1_HOST" 9094
check VM2 "HDFS NameNode RPC"                            "$VM2_HOST" "$HDFS_PORT"
check VM2 "HDFS NameNode UI"                             "$VM2_HOST" 9870
check VM2 "HDFS DataNode"                                "$VM2_HOST" 9866
check VM2 "Airflow webserver"                            "$VM2_HOST" "$AIRFLOW_PORT"
check VM3 "Spark master"                                 "$VM3_HOST" 7077
check VM3 "Spark master UI"                              "$VM3_HOST" 8080
check VM4 "Druid router / console"                       "$VM4_HOST" 8888
check VM4 "Druid broker"                                 "$VM4_HOST" 8082
check VM4 "Dashboard (Streamlit)"                        "$VM4_HOST" "$DASHBOARD_PORT"

step "Details"
curl -s "http://${VM2_HOST}:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" 2>/dev/null \
  | python3 -c 'import json,sys; b=json.load(sys.stdin)["beans"][0]; print("  HDFS: live datanodes=%s, files+dirs=%s, state=%s" % (b["NumLiveDataNodes"], b["FilesTotal"], b["FSState"]))' 2>/dev/null \
  || echo "  HDFS: (NameNode JMX not reachable)"
curl -s "http://${VM3_HOST}:8080/json/" 2>/dev/null \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print("  Spark: master %s, workers=%d, cores=%s, mem=%sMB" % (d["status"], sum(1 for w in d["workers"] if w["state"]=="ALIVE"), d["cores"], d["memory"]))' 2>/dev/null \
  || echo "  Spark: (master UI not reachable)"
curl -s "http://${VM2_HOST}:${AIRFLOW_PORT}/health" 2>/dev/null \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print("  Airflow: metadatabase=%s, scheduler=%s" % (d["metadatabase"]["status"], d["scheduler"]["status"]))' 2>/dev/null \
  || echo "  Airflow: (health endpoint not reachable)"
echo "  Druid: health=$(curl -s "${DRUID_ROUTER}/status/health" 2>/dev/null || echo '?')"

echo
if [[ $DOWN -eq 0 ]]; then ok "All services are up"; else warn "$DOWN check(s) failed"; fi
echo "  UIs:  Airflow http://${VM2_HOST}:${AIRFLOW_PORT}   HDFS http://${VM2_HOST}:9870   Spark http://${VM3_HOST}:8080"
echo "        Druid   http://${VM4_HOST}:8888   Dashboard http://${VM4_HOST}:${DASHBOARD_PORT}"
