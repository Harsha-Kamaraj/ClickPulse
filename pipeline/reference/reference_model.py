#!/usr/bin/env python3
"""
Independent reference model of the ClickPulse rules (plain Python, no Spark).

Replays a sequence of batch files through the spec and prints, per batch, the
expected rejects / duplicates / inserts / updates / drops, plus the expected
final curated-table totals. Used to cross-check what Spark + Druid produced.

  python3 pipeline/reference/reference_model.py data/clicks_day1.jsonl data/clicks_day2.jsonl
  python3 pipeline/reference/reference_model.py --json day1=data/clicks_day1.jsonl day2=data/clicks_day2.jsonl
"""
import json
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "spark"))
import clickpulse_rules as R  # noqa: E402


def run(batches):
    table, ledger, results = {}, set(), []
    for batch_id, path in batches:
        with open(path, "rb") as fh:
            data = fh.read()
        lines = data.split(b"\n")
        if data.endswith(b"\n"):
            lines = lines[:-1]
        reasons, valid = Counter(), []
        for n, raw in enumerate(lines, start=1):
            res = R.evaluate_line(raw.decode("utf-8", errors="replace"))
            if res["valid"]:
                valid.append((n, raw, res["record"]))
            else:
                reasons[res["reasons"][0]] += 1
        best = {}
        for n, raw, rec in valid:
            k = rec["event_id"]
            if k not in best or R.dedupe_key(rec, n) > R.dedupe_key(best[k][1], best[k][0]):
                best[k] = (n, rec)
        loaded_dates = {r["event_date"] for r in table.values()}
        st = Counter()
        for k, (n, rec) in sorted(best.items()):
            if (batch_id, k, rec["record_hash"]) in ledger:
                st["already_applied"] += 1
                continue
            ledger.add((batch_id, k, rec["record_hash"]))
            cur = table.get(k)
            if cur is None:
                st["inserts"] += 1
                if rec["event_date"] in loaded_dates:
                    st["late_arrival_inserts"] += 1
                table[k] = rec
            elif rec["event_time"] > cur["event_time"]:
                st["updates"] += 1
                if rec["event_date"] != cur["event_date"]:
                    st["date_moving_updates"] += 1
                table[k] = rec
            else:
                st["drops"] += 1
        results.append({
            "batch_id": batch_id, "raw_lines": len(lines), "rejected_lines": sum(reasons.values()),
            "reject_reasons": dict(sorted(reasons.items())), "valid_lines": len(valid),
            "duplicate_lines_collapsed": len(valid) - len(best), "unique_events": len(best),
            "already_applied": st["already_applied"], "inserts": st["inserts"],
            "late_arrival_inserts": st["late_arrival_inserts"], "updates": st["updates"],
            "date_moving_updates": st["date_moving_updates"], "drops": st["drops"],
            "table_rows_after": len(table),
        })
    by_date = defaultdict(lambda: {"events": 0, "purchases": 0, "revenue": 0.0})
    by_cat = defaultdict(float)
    for r in table.values():
        d = by_date[r["event_date"].isoformat()]
        d["events"] += 1
        d["purchases"] += r["is_purchase"]
        d["revenue"] = round(d["revenue"] + r["revenue"], 2)
        if r["is_purchase"]:
            by_cat[r["category"] or "Unknown"] = round(by_cat[r["category"] or "Unknown"] + r["revenue"], 2)
    totals = {
        "rows": len(table),
        "revenue": round(sum(r["revenue"] for r in table.values()), 2),
        "purchases": sum(r["is_purchase"] for r in table.values()),
        "by_date": dict(sorted(by_date.items())),
        "revenue_by_category": dict(sorted(by_cat.items(), key=lambda kv: -kv[1])),
    }
    return results, totals, table


def main(argv):
    as_json = "--json" in argv
    args = [a for a in argv if a != "--json"]
    batches = []
    for a in args:
        if "=" in a:
            b, p = a.split("=", 1)
        else:
            b = os.path.basename(a).replace("clicks_", "").replace(".jsonl", "")
            p = a
        batches.append((b, p))
    if not batches:
        print(__doc__)
        return 1
    results, totals, _ = run(batches)
    if as_json:
        print(json.dumps({"batches": results, "totals": totals}, indent=2))
        return 0
    for r in results:
        print(f"[{r['batch_id']}] raw={r['raw_lines']} rejected={r['rejected_lines']} {r['reject_reasons']}")
        print(f"        valid={r['valid_lines']} dup_collapsed={r['duplicate_lines_collapsed']} "
              f"unique={r['unique_events']} already_applied={r['already_applied']}")
        print(f"        inserts={r['inserts']} (late {r['late_arrival_inserts']}) updates={r['updates']} "
              f"(moved {r['date_moving_updates']}) drops={r['drops']} -> rows={r['table_rows_after']}")
    print(f"EXPECTED TABLE: rows={totals['rows']} revenue={totals['revenue']:.2f} purchases={totals['purchases']}")
    for d, v in totals["by_date"].items():
        print(f"   {d}: events={v['events']:>3} purchases={v['purchases']:>3} revenue={v['revenue']:.2f}")
    print("   revenue by category:", totals["revenue_by_category"])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
