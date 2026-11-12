#!/usr/bin/env bash
# =============================================================================
#  Shared helpers for every ClickPulse script.   Usage:  source scripts/lib/common.sh
# =============================================================================
set -o pipefail

# ---- locate the repo & load config -----------------------------------------
CP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export CP_ROOT
# shellcheck disable=SC1091
source "$CP_ROOT/config/cluster.env"
# shellcheck disable=SC1091
[[ -f "$CP_ROOT/config/cluster.local.env" ]] && source "$CP_ROOT/config/cluster.local.env"

# ---- derived settings --------------------------------------------------------
export CP_BASE CP_REPO
export KAFKA_HOME="$CP_BASE/kafka"
export HADOOP_HOME="$CP_BASE/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="$CP_BASE/spark"
export DRUID_HOME="$CP_BASE/druid"
export CP_VENV="$CP_BASE/venv"
export AIRFLOW_VENV="$CP_BASE/airflow-venv"
export AIRFLOW_HOME="$CP_BASE/airflow"
export CP_LOGS="$CP_BASE/logs"
export CP_RUN="$CP_BASE/run"
export CP_DATA="$CP_BASE/data"
export CP_DOWNLOADS="$CP_BASE/downloads"

if [[ -d "$CP_BASE" && -w "$CP_BASE" ]]; then mkdir -p "$CP_LOGS" "$CP_RUN" 2>/dev/null; fi

export KAFKA_BOOTSTRAP="${VM1_HOST}:9092,${VM1_HOST}:9094"
export HDFS_URI="hdfs://${VM2_HOST}:${HDFS_PORT}"
export ICEBERG_WAREHOUSE="${HDFS_URI}${HDFS_ROOT}/warehouse"
export SPARK_MASTER_URL="spark://${VM3_HOST}:7077"
export DRUID_ROUTER="http://${VM4_HOST}:8888"

for _d in "$KAFKA_HOME/bin" "$HADOOP_HOME/bin" "$HADOOP_HOME/sbin" "$SPARK_HOME/bin" "$SPARK_HOME/sbin" "$DRUID_HOME/bin"; do
  [[ -d "$_d" ]] && PATH="$_d:$PATH"
done
export PATH

# ---- Java ----------------------------------------------------------------------
detect_java_home() {
  local j
  for j in /usr/lib/jvm/java-11-openjdk-* /usr/lib/jvm/java-17-openjdk-*; do
    [[ -x "$j/bin/java" ]] && { echo "$j"; return 0; }
  done
  if command -v java >/dev/null 2>&1; then
    dirname "$(dirname "$(readlink -f "$(command -v java)")")"
    return 0
  fi
  return 1
}
if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVA_HOME="$(detect_java_home 2>/dev/null || true)"
  export JAVA_HOME
fi

# ---- pretty output ---------------------------------------------------------------
if [[ -t 1 ]]; then
  C_RESET=$'\e[0m'; C_BOLD=$'\e[1m'; C_DIM=$'\e[2m'
  C_RED=$'\e[31m'; C_GREEN=$'\e[32m'; C_YELLOW=$'\e[33m'; C_BLUE=$'\e[34m'; C_CYAN=$'\e[36m'
else
  C_RESET=""; C_BOLD=""; C_DIM=""; C_RED=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_CYAN=""
fi

info()  { echo "${C_BLUE}[INFO]${C_RESET}  $*"; }
ok()    { echo "${C_GREEN}[ OK ]${C_RESET}  $*"; }
warn()  { echo "${C_YELLOW}[WARN]${C_RESET}  $*" >&2; }
err()   { echo "${C_RED}[FAIL]${C_RESET}  $*" >&2; }
die()   { err "$*"; exit 1; }
step()  { echo; echo "${C_BOLD}${C_CYAN}==> $*${C_RESET}"; }

banner() {
  local line="=============================================================================="
  echo
  echo "${C_BOLD}${C_CYAN}${line}${C_RESET}"
  printf "${C_BOLD}${C_CYAN}  %s${C_RESET}\n" "$@"
  echo "${C_BOLD}${C_CYAN}${line}${C_RESET}"
}

# Show a command before running it (nice for demos)
run() {
  echo "${C_DIM}\$ $*${C_RESET}"
  "$@"
}

pause() {
  # DEMO_NO_PAUSE=1 to run straight through
  [[ "${DEMO_NO_PAUSE:-0}" == "1" || ! -t 0 ]] && return 0
  read -r -p "${C_DIM}-- press ENTER to continue --${C_RESET}" _
}

# ---- guards ------------------------------------------------------------------------
require_cmd() { command -v "$1" >/dev/null 2>&1 || die "'$1' not found. $2"; }

require_not_root() {
  [[ "$(id -u)" -ne 0 ]] || die "Run this as your normal user (not root / not with sudo). It calls sudo itself where needed."
}

# Warn if a script meant for VM<n> is executed somewhere else.
expect_vm() {
  local n="$1" ip_var="VM${1}_IP"
  local ip="${!ip_var}"
  if ! ip -4 addr 2>/dev/null | grep -q "inet ${ip}/"; then
    warn "This script is meant for VM${n} (${ip}); this machine does not own that IP."
    warn "Check config/cluster.env or make sure you are on the right VM."
    if [[ "${CP_FORCE:-0}" != "1" ]]; then
      die "Refusing to continue. Re-run with CP_FORCE=1 to override."
    fi
  fi
}

validate_batch() {
  [[ "$1" =~ ^[a-z0-9_]+$ ]] || die "Invalid batch id '$1' (allowed: lowercase letters, digits, underscore, e.g. day1)"
}

batch_file() {  # day1 -> /opt/clickpulse/ClickPulse/data/clicks_day1.jsonl
  echo "$CP_ROOT/data/clicks_${1}.jsonl"
}

# ---- network helpers ------------------------------------------------------------------
port_open() { timeout 3 bash -c "</dev/tcp/$1/$2" >/dev/null 2>&1; }

wait_for_port() {  # host port timeout_s label
  local host="$1" port="$2" timeout_s="${3:-60}" label="${4:-$1:$2}" t=0
  until port_open "$host" "$port"; do
    (( t >= timeout_s )) && { err "$label not reachable on $host:$port after ${timeout_s}s"; return 1; }
    sleep 2; t=$((t + 2))
  done
  ok "$label is up ($host:$port)"
}

# ---- downloads ---------------------------------------------------------------------------
# Cached in $CP_DOWNLOADS (copy a tarball there from a teammate to skip the download).
# Sources, fastest first: Apache CDN (current releases only) -> Huawei Cloud Apache mirror
# (has old releases, fast) -> archive.apache.org (complete but slow).
# Every file is verified against the OFFICIAL sha512 published on archive.apache.org.
APACHE_MIRRORS=("https://dlcdn.apache.org" "https://repo.huaweicloud.com/apache" "https://archive.apache.org/dist")

official_sha512() {  # rel -> prints lowercase hex sha512 (empty if unavailable)
  curl -fsSL --connect-timeout 20 "https://archive.apache.org/dist/$1.sha512" 2>/dev/null | python3 -c '
import re, sys
t = sys.stdin.read().strip()
if "=" in t:
    t = t.rsplit("=", 1)[1]            # SHA512 (file) = hash
elif re.match(r"^\S+:\s", t):
    t = t.split(":", 1)[1]             # file: AAAA BBBB ... (gpg style)
else:
    t = t.split()[0] if t else ""      # hash  file
t = re.sub(r"\s+", "", t).lower()
print(t if re.fullmatch(r"[0-9a-f]{128}", t) else "")'
}

download() {  # path_under_apache_dist dest_file
  local rel="$1" dest="$2" want got base
  mkdir -p "$(dirname "$dest")"
  if [[ -s "$dest" ]]; then info "Using cached $(basename "$dest")"; return 0; fi
  want="$(official_sha512 "$rel")"
  [[ -n "$want" ]] || warn "Could not fetch the official sha512 for $rel - download will not be verified"
  for base in "${APACHE_MIRRORS[@]}"; do
    info "Downloading ${base}/${rel}"
    if curl -fL --retry 3 --connect-timeout 20 -o "$dest.part" "${base}/${rel}"; then
      if [[ -n "$want" ]]; then
        got="$(sha512sum "$dest.part" | cut -d' ' -f1)"
        if [[ "$got" != "$want" ]]; then warn "sha512 mismatch from $base - trying next source"; rm -f "$dest.part"; continue; fi
        ok "sha512 verified"
      fi
      mv "$dest.part" "$dest"; ok "Downloaded $(basename "$dest")"; return 0
    fi
  done
  rm -f "$dest.part"
  die "Could not download $rel. Copy the file manually to $dest and re-run."
}

download_url() {  # full_url dest_file
  local url="$1" dest="$2"
  mkdir -p "$(dirname "$dest")"
  if [[ -s "$dest" ]]; then info "Using cached $(basename "$dest")"; return 0; fi
  info "Downloading $url"
  curl -fL --retry 3 --connect-timeout 20 -o "$dest.part" "$url" || die "Download failed: $url"
  mv "$dest.part" "$dest"
}

# Extract tarball into $CP_BASE/<name> (symlink-free, idempotent)
install_tarball() {  # tarball target_dir
  local tgz="$1" target="$2"
  if [[ -d "$target" && -n "$(ls -A "$target" 2>/dev/null)" ]]; then
    info "$(basename "$target") already installed at $target"; return 0
  fi
  local tmp; tmp="$(mktemp -d "$CP_BASE/.extract.XXXX")"
  tar -xzf "$tgz" -C "$tmp"
  local top; top="$(find "$tmp" -mindepth 1 -maxdepth 1 -type d | head -1)"
  rm -rf "$target"; mv "$top" "$target"; rm -rf "$tmp"
  ok "Installed $(basename "$target") -> $target"
}

# ---- HDFS helpers ------------------------------------------------------------------------
hdfs_cmd() { "$HADOOP_HOME/bin/hdfs" "$@" 2>/dev/null; }

# ---- SSH from VM2 (Airflow) to the other VMs ----------------------------------------------
vm_ssh() {  # vm_number command...
  local n="$1"; shift
  local user_var="VM${n}_USER" host_var="VM${n}_HOST"
  ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
      "${!user_var}@${!host_var}" "$@"
}
