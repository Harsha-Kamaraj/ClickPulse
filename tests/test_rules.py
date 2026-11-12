"""Unit tests for the validation rules:  python3 -m unittest discover -s tests -v"""
import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pipeline", "spark"))
import clickpulse_rules as R  # noqa: E402

BASE = ('{"event_id":"evt_1","user_id":"U1","session_id":"S1","event_type":"purchase","product_id":"P1",'
        '"category":"Books","price":10.5,"quantity":2,"device":"mobile",'
        '"event_time":"2025-01-02T10:00:00Z","updated_at":"2025-01-02T10:00:00Z"}')


def line(**over):
    import json
    d = json.loads(BASE)
    d.update(over)
    for k, v in list(over.items()):
        if v is ...:
            del d[k]
    return json.dumps(d)


class Rules(unittest.TestCase):
    def test_valid_purchase(self):
        r = R.evaluate_line(BASE)
        self.assertTrue(r["valid"])
        self.assertEqual(r["record"]["revenue"], 21.0)
        self.assertEqual(r["record"]["event_date"], date(2025, 1, 2))

    def test_unreadable(self):
        self.assertEqual(R.evaluate_line('{"event_id":"x",')["reasons"], ["INVALID_JSON"])
        self.assertEqual(R.evaluate_line("")["reasons"], ["INVALID_JSON"])
        self.assertEqual(R.evaluate_line("[1,2]")["reasons"], ["NOT_A_JSON_OBJECT"])

    def test_missing_ids(self):
        self.assertIn("MISSING_EVENT_ID", R.evaluate_line(line(event_id=...))["reasons"])
        self.assertIn("MISSING_USER_ID", R.evaluate_line(line(user_id=None))["reasons"])
        self.assertIn("MISSING_SESSION_ID", R.evaluate_line(line(session_id="  "))["reasons"])

    def test_event_type(self):
        self.assertIn("INVALID_EVENT_TYPE", R.evaluate_line(line(event_type="clickk"))["reasons"])
        for t in R.VALID_EVENT_TYPES:
            self.assertTrue(R.evaluate_line(line(event_type=t))["valid"], t)

    def test_event_time(self):
        self.assertIn("INVALID_EVENT_TIME", R.evaluate_line(line(event_time="yesterday"))["reasons"])
        self.assertIn("INVALID_EVENT_TIME", R.evaluate_line(line(event_time="2025-02-30T10:00:00Z"))["reasons"])
        r = R.evaluate_line(line(event_time="2025-01-02T01:30:00+05:30"))   # -> 2025-01-01T20:00Z
        self.assertEqual(r["record"]["event_date"], date(2025, 1, 1))

    def test_negative_and_zero(self):
        self.assertIn("NEGATIVE_PRICE", R.evaluate_line(line(price=-5))["reasons"])
        self.assertIn("NEGATIVE_QUANTITY", R.evaluate_line(line(quantity=-1))["reasons"])
        self.assertTrue(R.evaluate_line(line(price=0, quantity=0))["valid"])

    def test_revenue_only_for_purchases(self):
        self.assertEqual(R.evaluate_line(line(event_type="add_to_cart"))["record"]["revenue"], 0.0)

    def test_identical_records_same_hash(self):
        self.assertEqual(R.evaluate_line(BASE)["record"]["record_hash"], R.evaluate_line(BASE)["record"]["record_hash"])
        self.assertNotEqual(R.evaluate_line(BASE)["record"]["record_hash"],
                            R.evaluate_line(line(category="Toys"))["record"]["record_hash"])

    def test_dedupe_latest_updated_at_wins(self):
        a = R.evaluate_line(line(updated_at="2025-01-02T10:05:00Z"))["record"]
        b = R.evaluate_line(line(updated_at="2025-01-02T10:00:00Z", category="X"))["record"]
        self.assertGreater(R.dedupe_key(a, 1), R.dedupe_key(b, 2))   # earlier line, later updated_at wins


if __name__ == "__main__":
    unittest.main()
