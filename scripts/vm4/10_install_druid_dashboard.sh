#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM4  (Abhishek)  - once, after scripts/all/00_bootstrap_vm.sh
#
#  Installs:
#   * Apache Druid ${DRUID_VERSION} (single machine, bin/start-druid)
#       + extensions druid-hdfs-storage, druid-parquet-extensions
#       + Hadoop core-site.xml / hdfs-site.xml in conf/druid/auto/_common/
#         so Druid can read the Parquet export straight from HDFS on VM2
#   * Python venv with Streamlit for the dashboard
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
source "$CP_ROOT/scripts/lib/hadoop.sh"
set -e  # stop at the first error
require_not_root
expect_vm 4

banner "VM4 install: Druid ${DRUID_VERSION} + dashboard"

step "Druid binaries"
TGZ="$CP_DOWNLOADS/apache-druid-${DRUID_VERSION}-bin.tar.gz"
download "druid/${DRUID_VERSION}/apache-druid-${DRUID_VERSION}-bin.tar.gz" "$TGZ"
install_tarball "$TGZ" "$DRUID_HOME"

step "Druid extensions + HDFS client config"
COMMON="$DRUID_HOME/conf/druid/auto/_common"
[[ -f "$COMMON/common.runtime.properties" ]] || die "Unexpected Druid layout: $COMMON missing"
python3 - "$COMMON/common.runtime.properties" <<'EOF'
import json, re, sys
path = sys.argv[1]
text = open(path).read()
m = re.search(r'^druid\.extensions\.loadList=(\[.*?\])\s*$', text, re.M)
current = json.loads(m.group(1)) if m else []
for ext in ["druid-hdfs-storage", "druid-parquet-extensions", "druid-multi-stage-query"]:
    if ext not in current:
        current.append(ext)
line = "druid.extensions.loadList=" + json.dumps(current)
text = text.replace(m.group(0), line) if m else text + "\n" + line + "\n"
open(path, "w").write(text)
print("  loadList =", current)
EOF
render_hadoop_conf "$COMMON" druid
ok "core-site.xml + hdfs-site.xml copied to $COMMON"
ls -1 "$DRUID_HOME/extensions" | grep -E "druid-(hdfs-storage|parquet-extensions)" \
  || die "Druid extensions missing in $DRUID_HOME/extensions"

step "Dashboard venv (Streamlit)"
python3 -m venv "$CP_VENV"
"$CP_VENV/bin/pip" install -q --upgrade pip
"$CP_VENV/bin/pip" install -q -r "$CP_ROOT/dashboard/requirements.txt"
ok "venv ready: $CP_VENV"

banner "VM4 install done" "Next: scripts/vm4/11_start_druid.sh, then 12_start_dashboard.sh"
