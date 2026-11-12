"""
ClickPulse dashboard (VM4) - every number on this page comes from Druid SQL.

  * filters: event-date range + device (applied to every query)
  * KPI cards: revenue, purchases (+ events, users, sessions, avg order value)
  * bar chart: revenue by category
  * daily revenue, shopping funnel, event lookup, and the exact SQL that ran
"""
import json
import os
import urllib.error
import urllib.request
from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st

DRUID = os.environ.get("CP_DRUID_URL", "http://localhost:8888").rstrip("/")
DS = os.environ.get("CP_DRUID_DATASOURCE", "clickpulse_events")

st.set_page_config(page_title="ClickPulse", page_icon="📊", layout="wide")

st.markdown("""
<style>
  div[data-testid="stMetric"] {background: rgba(127,127,127,0.08); border-radius: 10px; padding: 14px 18px;}
  div[data-testid="stMetricValue"] {font-size: clamp(1.15rem, 1.7vw, 1.9rem);}
  div[data-testid="stMetricValue"] * {overflow: visible !important; text-overflow: clip !important;}
  .block-container {padding-top: 1.6rem;}
</style>""", unsafe_allow_html=True)

EXECUTED_SQL = []
REQUIRED_COLUMNS = {"__time", "event_id", "event_type", "event_date", "device", "category",
                    "user_id", "session_id", "revenue", "is_purchase", "source_batch"}


def druid_sql(query, record=True):
    if record:
        EXECUTED_SQL.append(" ".join(query.split()))
    body = json.dumps({"query": query, "resultFormat": "object",
                       "context": {"sqlTimeZone": "Etc/UTC", "useApproximateCountDistinct": False}}).encode()
    req = urllib.request.Request(f"{DRUID}/druid/v2/sql", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return pd.DataFrame(json.loads(r.read().decode()))
    except urllib.error.HTTPError as e:
        st.error(f"Druid SQL error {e.code}: {e.read().decode()[:400]}")
        st.code(query, language="sql")
        st.stop()
    except urllib.error.URLError as e:
        st.error(f"Cannot reach Druid at {DRUID}: {e.reason}")
        st.stop()


def q(s):
    """Quote a string literal for Druid SQL."""
    return "'" + str(s).replace("'", "''") + "'"


@st.cache_data(ttl=15)
def bounds():
    exists = druid_sql(f"SELECT COUNT(*) AS n FROM INFORMATION_SCHEMA.TABLES "
                       f"WHERE TABLE_SCHEMA='druid' AND TABLE_NAME={q(DS)}", record=False)
    if exists.empty or int(exists["n"][0]) == 0:
        return None
    cols = druid_sql(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                     f"WHERE TABLE_SCHEMA='druid' AND TABLE_NAME={q(DS)}", record=False)
    have = set(cols["COLUMN_NAME"]) if not cols.empty else set()
    missing = sorted(REQUIRED_COLUMNS - have)
    if missing:
        return {"missing": missing, "have": sorted(have)}
    # (Druid cannot MIN/MAX a string column, so bounds come from __time)
    b = druid_sql(f"""SELECT TIME_FORMAT(MIN(__time), 'yyyy-MM-dd') AS lo,
                      TIME_FORMAT(MAX(__time), 'yyyy-MM-dd') AS hi, COUNT(*) AS n FROM "{DS}" """, record=False)
    batches = druid_sql(f"""SELECT DISTINCT source_batch FROM "{DS}" """, record=False)["source_batch"].tolist()
    devices = druid_sql(f"""SELECT DISTINCT COALESCE(device, 'unknown') AS device FROM "{DS}" ORDER BY 1""",
                        record=False)["device"].tolist()
    return {"lo": date.fromisoformat(b["lo"][0]), "hi": date.fromisoformat(b["hi"][0]),
            "rows": int(b["n"][0]), "batches": sorted(batches), "devices": devices}


# ----------------------------------------------------------------------------- header + filters
st.title("📊 ClickPulse — RetailMart shopping analytics")
info = bounds()
if info is None:
    st.warning(f"Druid datasource `{DS}` has no data yet. Run the pipeline for day1 first.")
    st.stop()
if "missing" in info:
    st.error(f"Druid datasource `{DS}` exists but was not loaded by the ClickPulse pipeline: "
             f"it is missing the columns {', '.join(info['missing'])}.")
    st.markdown(
        "Either Druid is still loading its segments (wait a minute and press Refresh), or data was "
        "loaded into this name some other way (for example the Druid console's *Load data*).\n\n"
        "On VM4 run `bash scripts/vm4/druid_inspect.sh` to see what is in it. If it is not ClickPulse "
        "data and the final run has not happened yet, delete it with "
        "`bash scripts/vm4/druid_inspect.sh --drop` and run the pipeline again.")
    st.caption("Columns found: " + (", ".join(info["have"]) or "(none)"))
    if st.button("↻ Refresh from Druid"):
        st.cache_data.clear()
        st.rerun()
    st.stop()

with st.sidebar:
    st.header("Filters")
    rng = st.date_input("Event date (UTC)", value=(info["lo"], info["hi"]),
                        min_value=info["lo"], max_value=info["hi"])
    if isinstance(rng, (list, tuple)):
        start, end = (rng[0], rng[1]) if len(rng) == 2 else (rng[0], rng[0])
    else:
        start = end = rng
    devices = st.multiselect("Device", info["devices"], default=info["devices"])
    st.divider()
    if st.button("↻ Refresh from Druid"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"Druid: `{DRUID}`  \nDatasource: `{DS}`  \nRows in Druid: **{info['rows']}**  \n"
               f"Batches loaded: **{', '.join(info['batches'])}**")

if not devices:
    st.info("Select at least one device.")
    st.stop()

dev_sql = ", ".join(q(d) for d in devices if d in info["devices"])
WHERE = (f"__time >= TIMESTAMP {q(start.isoformat() + ' 00:00:00')} "
         f"AND __time < TIMESTAMP {q((end + timedelta(days=1)).isoformat() + ' 00:00:00')} "
         f"AND COALESCE(device, 'unknown') IN ({dev_sql})")
st.caption(f"Showing **{start} → {end}** · devices: **{', '.join(devices)}** · revenue = Σ price × quantity over purchase events")

# ----------------------------------------------------------------------------- KPIs
k = druid_sql(f"""
    SELECT ROUND(SUM(CASE WHEN event_type = 'purchase' THEN revenue ELSE 0 END), 2) AS revenue,
           SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)                 AS purchases,
           COUNT(*)                                                                 AS events
    FROM "{DS}" WHERE {WHERE}""").iloc[0]
# exact distinct counts: one per query (each needs its own Druid merge buffers)
users = druid_sql(f"""SELECT COUNT(DISTINCT user_id) AS n FROM "{DS}" WHERE {WHERE}""")["n"][0]
sessions = druid_sql(f"""SELECT COUNT(DISTINCT session_id) AS n FROM "{DS}" WHERE {WHERE}""")["n"][0]
revenue = float(k["revenue"] or 0)
purchases = int(k["purchases"] or 0)
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Revenue", f"${revenue:,.2f}")
c2.metric("Purchases", f"{purchases:,}")
c3.metric("Avg order value", f"${revenue / purchases:,.2f}" if purchases else "—")
c4.metric("Events", f"{int(k['events'] or 0):,}")
c5.metric("Users / sessions", f"{int(users or 0)} / {int(sessions or 0)}")

# ----------------------------------------------------------------------------- charts
left, right = st.columns([3, 2])
with left:
    st.subheader("Revenue by category")
    cat = druid_sql(f"""
        SELECT COALESCE(category, 'Unknown') AS category,
               ROUND(SUM(revenue), 2) AS revenue, COUNT(*) AS purchases
        FROM "{DS}" WHERE {WHERE} AND event_type = 'purchase'
        GROUP BY 1 ORDER BY revenue DESC""")
    if cat.empty:
        st.info("No purchases for this filter.")
    else:
        bars = alt.Chart(cat).mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#4C78A8").encode(
            x=alt.X("revenue:Q", title="Revenue ($)"),
            y=alt.Y("category:N", sort="-x", title=None),
            tooltip=["category", alt.Tooltip("revenue:Q", format=",.2f"), "purchases"])
        labels = bars.mark_text(align="left", dx=4, color="gray").encode(text=alt.Text("revenue:Q", format=",.2f"))
        st.altair_chart((bars + labels).properties(height=max(220, 38 * len(cat))), use_container_width=True)

with right:
    st.subheader("Shopping funnel")
    fun = druid_sql(f"""SELECT event_type, COUNT(*) AS events FROM "{DS}" WHERE {WHERE} GROUP BY 1""")
    order = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "purchase"]
    st.altair_chart(alt.Chart(fun).mark_bar(color="#72B7B2").encode(
        x=alt.X("event_type:N", sort=order, title=None), y=alt.Y("events:Q", title="Events"),
        tooltip=["event_type", "events"]).properties(height=280), use_container_width=True)

st.subheader("Daily revenue and purchases (event date, UTC)")
daily = druid_sql(f"""
    SELECT event_date, ROUND(SUM(revenue), 2) AS revenue, SUM(is_purchase) AS purchases, COUNT(*) AS events
    FROM "{DS}" WHERE {WHERE} GROUP BY 1 ORDER BY 1""")
if not daily.empty:
    base = alt.Chart(daily).encode(x=alt.X("event_date:N", title=None))
    st.altair_chart(
        alt.layer(base.mark_bar(color="#4C78A8", opacity=0.85).encode(y=alt.Y("revenue:Q", title="Revenue ($)"),
                                                                       tooltip=["event_date", "revenue", "purchases", "events"]),
                  base.mark_line(point=True, color="#F58518").encode(y=alt.Y("purchases:Q", title="Purchases")))
        .resolve_scale(y="independent").properties(height=260), use_container_width=True)
    st.dataframe(daily, hide_index=True, use_container_width=True)

# ----------------------------------------------------------------------------- event lookup
with st.expander("🔎 Event lookup (prove an event appears exactly once, on its current date)"):
    eid = st.text_input("event_id", placeholder="evt_f3afdd8733")
    if eid:
        res = druid_sql(f"""SELECT event_id, event_type, event_time, event_date, device, category, price, quantity,
                            revenue, source_batch FROM "{DS}" WHERE event_id = {q(eid.strip())}""")
        st.write(f"**{len(res)}** row(s) in Druid")
        st.dataframe(res, hide_index=True, use_container_width=True)

with st.expander("🧾 Druid SQL executed for this view"):
    for s in EXECUTED_SQL:
        st.code(s, language="sql")
