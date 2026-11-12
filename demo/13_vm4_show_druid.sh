#!/usr/bin/env bash
# =============================================================================
#  DEMO - RUN ON: VM4  (Abhishek)
#  Druid SQL view of the data + check against the independent reference model.
#     demo/13_vm4_show_druid.sh           # after day1 or day2
# =============================================================================
source "$(dirname "$0")/../scripts/lib/common.sh"
expect_vm 4
DQ() { echo "${C_DIM}druid-sql> $1${C_RESET}"; python3 "$CP_ROOT/pipeline/druid/druid_query.py" "http://localhost:8888" "$1"; echo; }
DS="\"$DRUID_DATASOURCE\""

banner "Druid datasource $DRUID_DATASOURCE (served from VM4)"
step "Segments (one per event_date, only the latest version is active)"
DQ "SELECT \"start\", \"end\", version, \"size\" AS size_bytes FROM sys.segments
    WHERE datasource = '$DRUID_DATASOURCE' AND is_published = 1 AND is_overshadowed = 0 ORDER BY \"start\""
step "Last ingestion tasks"
DQ "SELECT task_id, status, created_time, duration FROM sys.tasks WHERE datasource = '$DRUID_DATASOURCE'
    ORDER BY created_time DESC LIMIT 5"
pause

step "KPIs (dashboard cards)"
DQ "SELECT ROUND(SUM(revenue), 2) AS revenue, SUM(is_purchase) AS purchases, COUNT(*) AS events,
    COUNT(DISTINCT event_id) AS distinct_event_ids, COUNT(*) - COUNT(DISTINCT event_id) AS duplicates FROM $DS"
step "Per event date"
DQ "SELECT event_date, COUNT(*) AS events, SUM(is_purchase) AS purchases, ROUND(SUM(revenue), 2) AS revenue
    FROM $DS GROUP BY 1 ORDER BY 1"
step "Revenue by category (dashboard bar chart)"
DQ "SELECT COALESCE(category, 'Unknown') AS category, ROUND(SUM(revenue), 2) AS revenue, COUNT(*) AS purchases
    FROM $DS WHERE event_type = 'purchase' GROUP BY 1 ORDER BY 2 DESC"
step "Filter example: mobile only, 2025-01-04 .. 2025-01-05"
DQ "SELECT ROUND(SUM(revenue), 2) AS revenue, SUM(is_purchase) AS purchases FROM $DS
    WHERE __time >= TIMESTAMP '2025-01-04 00:00:00' AND __time < TIMESTAMP '2025-01-06 00:00:00' AND device = 'mobile'"
pause

step "Edge-case events in Druid (each must appear exactly once)"
DQ "SELECT event_id, event_type, event_time, event_date, price, quantity, revenue, source_batch FROM $DS
    WHERE event_id IN ('evt_f3afdd8733', 'evt_cc120fac4a', 'evt_129bdeb398', 'evt_00627c60de') ORDER BY __time"
echo "  evt_f3afdd8733 : updated in day2, moved 2025-01-01 -> 2025-01-04"
echo "  evt_cc120fac4a : day2 copy had an EARLIER event_time -> dropped, day1 row kept"
echo "  evt_129bdeb398 : day2 update had price -14.99 -> rejected, valid day1 row kept"
echo "  evt_00627c60de : late arrival in day2 for already-loaded date 2025-01-02 -> inserted"
pause

step "Independent check: Druid vs plain-Python reference model of the spec"
LOADED="$(python3 "$CP_ROOT/pipeline/druid/druid_query.py" "http://localhost:8888" \
  "SELECT DISTINCT source_batch FROM $DS" --json | python3 -c 'import json,sys; print(" ".join(sorted(r["source_batch"] for r in json.load(sys.stdin))))')"
# day2 rows present -> day1 and day2 were processed (in that order); otherwise only day1
ARGS=("day1=$(batch_file day1)")
[[ " $LOADED " == *" day2 "* ]] && ARGS+=("day2=$(batch_file day2)")
echo "  batches loaded: ${ARGS[*]}"
python3 "$CP_ROOT/pipeline/reference/reference_model.py" --json "${ARGS[@]}" > /tmp/clickpulse_expected.json
python3 - /tmp/clickpulse_expected.json "$CP_ROOT/pipeline/druid" "$DRUID_DATASOURCE" <<'PY'
import json, sys
sys.path.insert(0, sys.argv[2])
from druid_query import query
ds = '"%s"' % sys.argv[3]
exp = json.load(open(sys.argv[1]))["totals"]
got = query("http://localhost:8888", "SELECT COUNT(*) AS n, ROUND(SUM(revenue),2) AS rev, SUM(is_purchase) AS p FROM " + ds)[0]
rows = [("rows", exp["rows"], got["n"]), ("revenue", exp["revenue"], got["rev"]), ("purchases", exp["purchases"], got["p"])]
for d, v in exp["by_date"].items():
    g = query("http://localhost:8888", "SELECT COUNT(*) AS n, ROUND(SUM(revenue),2) AS rev FROM %s WHERE event_date = '%s'" % (ds, d))[0]
    rows.append((d + " events", v["events"], g["n"]))
    rows.append((d + " revenue", v["revenue"], g["rev"]))
bad = 0
print("  %-24s%14s%14s" % ("metric", "reference", "druid"))
for k, a, b in rows:
    ok = abs(float(a) - float(b or 0)) < 0.005
    bad += not ok
    print("  %-24s%14s%14s   %s" % (k, a, b, "OK" if ok else "MISMATCH"))
print("\n  RESULT: " + ("Druid matches the reference model" if not bad else "%d mismatches" % bad))
PY
