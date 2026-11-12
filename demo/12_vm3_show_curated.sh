#!/usr/bin/env bash
# =============================================================================
#  DEMO - RUN ON: VM3  (Gagan R)
#  Shows the Iceberg curated table, rejects table, decisions and edge cases.
#     demo/12_vm3_show_curated.sh after-day1   # tables, partitions, audit, rejects, daily
#     demo/12_vm3_show_curated.sh after-day2   # + snapshots, rejects, decisions, edge cases
#     demo/12_vm3_show_curated.sh after-rerun  # audit + snapshots (nothing changed)
#     demo/12_vm3_show_curated.sh event evt_f3afdd8733
# =============================================================================
source "$(dirname "$0")/../scripts/lib/common.sh"
expect_vm 3
R="$CP_ROOT/scripts/vm3/run_report.sh"
case "${1:-after-day2}" in
  after-day1)  banner "Curated Iceberg table after Day 1";   "$R" tables,partitions,snapshots,audit,rejects,daily ;;
  after-day2)  banner "Curated Iceberg table after Day 2";   "$R" tables,partitions,snapshots,audit,rejects,decisions,edge,daily ;;
  after-rerun) banner "After the re-run (nothing may change)"; "$R" snapshots,audit,tables,daily ;;
  event)       "$R" --event "${2:?event id}" ;;
  all)         "$R" all ;;
  *) die "usage: $0 after-day1|after-day2|after-rerun|event <id>|all" ;;
esac
