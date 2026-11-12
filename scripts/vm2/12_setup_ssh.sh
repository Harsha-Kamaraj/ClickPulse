#!/usr/bin/env bash
# =============================================================================
#  RUN ON: VM2  (Anjanaa)  - once
#  Airflow runs on VM2 and starts the producer on VM1 and spark-submit on VM3
#  over SSH, so VM2 needs password-less SSH to VM1, VM3 and VM4.
#  You will be asked for each VM's password ONE time (ask Harsha / Gagan /
#  Abhishek to type it, or have them run `passwd` beforehand).
# =============================================================================
source "$(dirname "$0")/../lib/common.sh"
require_not_root
expect_vm 2

banner "Password-less SSH from VM2 to VM1, VM3, VM4"
KEY="$HOME/.ssh/id_ed25519"
if [[ ! -f "$KEY" ]]; then
  mkdir -p "$HOME/.ssh"; chmod 700 "$HOME/.ssh"
  ssh-keygen -t ed25519 -N "" -f "$KEY" -C "clickpulse-airflow@vm2"
fi

for n in 1 3 4; do
  uv="VM${n}_USER"; hv="VM${n}_HOST"
  target="${!uv}@${!hv}"
  step "VM${n}: $target"
  if ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8 "$target" true 2>/dev/null; then
    ok "already password-less"
  else
    ssh-copy-id -o StrictHostKeyChecking=accept-new -i "$KEY.pub" "$target"
  fi
  vm_ssh "$n" "echo \"  -> \$(hostname) ok, repo: \$(ls -d ${CP_REPO} 2>/dev/null || echo MISSING)\""
done
banner "SSH ready - Airflow can now reach VM1, VM3 and VM4"
