"""
ClickPulse validation & normalisation rules  (pure Python, no dependencies).

Used by the Spark job (shipped to executors with --py-files) and by the
independent reference model in pipeline/reference/.

A record is INVALID (rejected) if it:
  * is not a readable JSON object                       -> INVALID_JSON / NOT_A_JSON_OBJECT
  * has a missing, null or empty event_id/user_id/session_id
                                                        -> MISSING_EVENT_ID / MISSING_USER_ID / MISSING_SESSION_ID
  * has an event_type outside the five valid values     -> INVALID_EVENT_TYPE
  * has an unparseable event_time                       -> INVALID_EVENT_TIME
  * has a negative price or quantity (zero is valid)    -> NEGATIVE_PRICE / NEGATIVE_QUANTITY
  (plus type errors that make a value unusable: INVALID_PRICE / INVALID_QUANTITY / INVALID_<ID>)
"""
import hashlib
import json
import math
import re
from datetime import datetime, timedelta, timezone

VALID_EVENT_TYPES = ("page_view", "product_view", "add_to_cart", "remove_from_cart", "purchase")
ID_FIELDS = ("event_id", "user_id", "session_id")

_TS_RE = re.compile(
    r"^\s*(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2})(?::(\d{2})(?:[.,](\d{1,9}))?)?"
    r"\s*([Zz]|[+-]\d{2}(?::?\d{2})?)?\s*$"
)


def parse_timestamp(value):
    """ISO-8601 string -> timezone-aware UTC datetime, or None if unparseable.
    A timestamp without an offset is taken as UTC."""
    if not isinstance(value, str):
        return None
    m = _TS_RE.match(value)
    if not m:
        return None
    y, mo, d, h, mi, s, frac, tz = m.groups()
    try:
        micros = int((frac or "0")[:6].ljust(6, "0"))
        dt = datetime(int(y), int(mo), int(d), int(h), int(mi), int(s or 0), micros, tzinfo=timezone.utc)
    except ValueError:
        return None
    if tz and tz not in ("Z", "z"):
        digits = tz[1:].replace(":", "")
        hh, mm = int(digits[:2]), int(digits[2:4] or 0)
        if hh > 23 or mm > 59:
            return None
        offset = timedelta(hours=hh, minutes=mm)
        dt = dt - offset if tz[0] == "+" else dt + offset
    return dt


def _is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _clean_id(v):
    """Return (value, problem) for an identifier field."""
    if v is None:
        return None, "MISSING"
    if isinstance(v, str):
        return (v, None) if v.strip() else (None, "MISSING")
    if _is_number(v):
        return str(v), None
    return None, "INVALID"


def _opt_str(v):
    if v is None:
        return None
    return v if isinstance(v, str) else json.dumps(v)


def iso(dt):
    return None if dt is None else dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ").replace(".000000Z", "Z")


def evaluate_line(line):
    """Validate one raw line.

    Returns a dict:
      valid      bool
      reasons    list[str]           (empty when valid)
      record     dict | None         normalised record when valid
      event_id   str | None          best-effort id (for reject reporting)
    """
    reasons = []
    try:
        obj = json.loads(line)
    except (ValueError, RecursionError, TypeError):
        return {"valid": False, "reasons": ["INVALID_JSON"], "record": None, "event_id": None}
    if not isinstance(obj, dict):
        return {"valid": False, "reasons": ["NOT_A_JSON_OBJECT"], "record": None, "event_id": None}

    ids = {}
    for f in ID_FIELDS:
        val, problem = _clean_id(obj.get(f))
        ids[f] = val
        if problem:
            reasons.append(f"{problem}_{f.upper()}")

    event_type = obj.get("event_type")
    if not (isinstance(event_type, str) and event_type in VALID_EVENT_TYPES):
        reasons.append("INVALID_EVENT_TYPE")

    event_time = parse_timestamp(obj.get("event_time"))
    if event_time is None:
        reasons.append("INVALID_EVENT_TIME")

    price = obj.get("price")
    if price is not None:
        if not _is_number(price) or not math.isfinite(price):
            reasons.append("INVALID_PRICE")
        elif price < 0:
            reasons.append("NEGATIVE_PRICE")

    quantity = obj.get("quantity")
    if quantity is not None:
        if not _is_number(quantity) or not math.isfinite(quantity) or float(quantity) != int(quantity):
            reasons.append("INVALID_QUANTITY")
        elif quantity < 0:
            reasons.append("NEGATIVE_QUANTITY")

    if reasons:
        return {"valid": False, "reasons": reasons, "record": None, "event_id": ids.get("event_id")}

    price = None if price is None else float(price)
    quantity = None if quantity is None else int(quantity)
    updated_at = parse_timestamp(obj.get("updated_at"))
    is_purchase = 1 if event_type == "purchase" else 0
    revenue = round(price * quantity, 2) if (is_purchase and price is not None and quantity is not None) else 0.0

    rec = {
        "event_id": ids["event_id"],
        "user_id": ids["user_id"],
        "session_id": ids["session_id"],
        "event_type": event_type,
        "product_id": _opt_str(obj.get("product_id")),
        "category": _opt_str(obj.get("category")),
        "price": price,
        "quantity": quantity,
        "device": _opt_str(obj.get("device")),
        "event_time": event_time,
        "updated_at": updated_at,
        "event_date": event_time.date(),          # UTC date (event_time is UTC-aware)
        "revenue": revenue,
        "is_purchase": is_purchase,
    }
    rec["record_hash"] = record_hash(rec)
    return {"valid": True, "reasons": [], "record": rec, "event_id": rec["event_id"]}


def record_hash(rec):
    """Content fingerprint of the business fields (identical events -> identical hash)."""
    payload = [
        rec["event_id"], rec["user_id"], rec["session_id"], rec["event_type"], rec["product_id"],
        rec["category"], rec["price"], rec["quantity"], rec["device"],
        iso(rec["event_time"]), iso(rec["updated_at"]),
    ]
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def dedupe_key(rec, line_no):
    """Sort key: the version with the latest updated_at wins; ties -> the later line in the file.
    A missing/unparseable updated_at loses to any real one."""
    ua = rec["updated_at"]
    return (ua is not None, ua or datetime.min.replace(tzinfo=timezone.utc), line_no)
