#!/usr/bin/env bash
# =============================================================================
#  RUN ON: ALL FOUR VMs (Harsha, Anjanaa, Gagan, Abhishek) - once, first.
#
#  * installs Java 11, Python venv, curl, jq, ssh server ...
#  * writes cp-vm1..cp-vm4 -> ZeroTier IPs into /etc/hosts
#  * creates /opt/clickpulse owned by you
#  * checks every other VM is reachable over ZeroTier
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
set -e  # stop at the first error
require_not_root

banner "ClickPulse bootstrap on $(hostname)"

step "Installing OS packages (sudo)"
sudo apt-get update -y
PKGS=(python3 python3-venv python3-pip curl wget jq rsync netcat-openbsd openssh-server openssh-client
      lsof procps perl iproute2 iputils-ping ca-certificates)
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y "${PKGS[@]}"
if ! sudo DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk-headless; then
  warn "openjdk-11 not available on this distro, installing openjdk-17 instead"
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-17-jdk-headless
fi
sudo systemctl enable --now ssh 2>/dev/null || sudo systemctl enable --now sshd 2>/dev/null || true
JAVA_HOME="$(detect_java_home)"; export JAVA_HOME
ok "Java: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)  ($JAVA_HOME)"

step "Writing cluster host names into /etc/hosts"
TMP_HOSTS="$(mktemp)"
# drop any previous ClickPulse block / cp-vm lines, then append a fresh block
grep -v -E "^# (>>>|<<<) ClickPulse|[[:space:]](${VM1_HOST}|${VM2_HOST}|${VM3_HOST}|${VM4_HOST})([[:space:]]|$)" /etc/hosts > "$TMP_HOSTS"
{
  echo "# >>> ClickPulse (ZeroTier) >>>"
  echo "${VM1_IP}  ${VM1_HOST}"
  echo "${VM2_IP}  ${VM2_HOST}"
  echo "${VM3_IP}  ${VM3_HOST}"
  echo "${VM4_IP}  ${VM4_HOST}"
  echo "# <<< ClickPulse <<<"
} >> "$TMP_HOSTS"
sudo cp /etc/hosts "/etc/hosts.clickpulse.bak.$(date +%s)"
sudo cp "$TMP_HOSTS" /etc/hosts
rm -f "$TMP_HOSTS"
grep -E "cp-vm[1-4]" /etc/hosts

step "Creating $CP_BASE (owned by $USER)"
sudo mkdir -p "$CP_BASE"
sudo chown "$USER":"$(id -gn)" "$CP_BASE"
mkdir -p "$CP_LOGS" "$CP_RUN" "$CP_DATA" "$CP_DOWNLOADS"
ok "$CP_BASE ready"

if [[ "$CP_ROOT" != "$CP_REPO" ]]; then
  warn "This repo is at $CP_ROOT but CP_REPO in config/cluster.env is $CP_REPO."
  warn "Airflow (VM2) runs remote scripts at \$CP_REPO, so clone the repo there on every VM:"
  warn "   git clone <repo-url> $CP_REPO"
fi

step "Checking ZeroTier connectivity to all VMs"
if command -v zerotier-cli >/dev/null 2>&1; then
  sudo zerotier-cli listnetworks 2>/dev/null || true
else
  warn "zerotier-cli not found - install it: curl -s https://install.zerotier.com | sudo bash"
fi
ALL_OK=1
for n in 1 2 3 4; do
  hv="VM${n}_HOST"; iv="VM${n}_IP"
  if ping -c 1 -W 2 "${!hv}" >/dev/null 2>&1; then
    ok "VM${n} ${!hv} (${!iv}) reachable"
  else
    warn "VM${n} ${!hv} (${!iv}) NOT reachable (is it on, joined & authorised in ZeroTier Central?)"
    ALL_OK=0
  fi
done

banner "Bootstrap finished on $(hostname)" \
       "Next: run the install script for YOUR VM (see EXECUTION_PLAN.md)"
[[ $ALL_OK == 1 ]] || warn "Some VMs were unreachable - fix ZeroTier before continuing."
