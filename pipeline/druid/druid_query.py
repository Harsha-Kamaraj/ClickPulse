#!/usr/bin/env python3
"""Run Druid SQL and print a table.   druid_query.py <router-url> "<sql>" [--json]   (stdlib only)"""
import json
import sys
import urllib.error
import urllib.request


def query(router, sql):
    body = json.dumps({"query": sql, "resultFormat": "object",
                       "context": {"sqlTimeZone": "Etc/UTC", "useApproximateCountDistinct": False,
                                   "useCache": False, "populateCache": False}}).encode()
    req = urllib.request.Request(router.rstrip("/") + "/druid/v2/sql", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        sys.exit(f"Druid SQL error {e.code}: {e.read().decode()[:500]}")


def print_table(rows):
    if not rows:
        print("(no rows)")
        return
    cols = list(rows[0].keys())
    vals = [["" if r.get(c) is None else str(r.get(c)) for c in cols] for r in rows]
    w = [max(len(c), *(len(v[i]) for v in vals)) for i, c in enumerate(cols)]
    sep = "+" + "+".join("-" * (x + 2) for x in w) + "+"
    print(sep)
    print("| " + " | ".join(c.ljust(w[i]) for i, c in enumerate(cols)) + " |")
    print(sep)
    for v in vals:
        print("| " + " | ".join(v[i].ljust(w[i]) for i in range(len(cols))) + " |")
    print(sep)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    res = query(sys.argv[1], sys.argv[2])
    if "--json" in sys.argv:
        print(json.dumps(res))
    else:
        print_table(res)
