"""Validate Pipeline 1's deterministic raw fixtures without external services."""

import csv
import re
from collections import Counter
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def rows(relative_path: str):
    with (ROOT / relative_path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


payments = rows("data/raw/payments/payments.csv")
accounts = {row["account_id"]: row for row in rows("data/raw/reference/accounts.csv")}
clients = {row["client_id"]: row for row in rows("data/raw/reference/clients.csv")}
payment_counts = Counter(row["payment_id"] for row in payments)

assert len(payments) == 6005, f"expected 6005 payment rows, found {len(payments)}"
assert len(payment_counts) == 6000, f"expected 6000 payment IDs, found {len(payment_counts)}"
assert sum(count - 1 for count in payment_counts.values()) == 5
assert sum(not row["account_id"] for row in payments) == 5
valid_currencies = {"AED", "AUD", "BRL", "CAD", "CHF", "EUR", "GBP", "INR", "JPY", "MXN", "SGD", "USD"}
assert sum(row["currency"] not in valid_currencies for row in payments) == 5
assert sum(float(row["amount"]) < 0 for row in payments) == 5

invalid_dates = 0
for row in payments:
    try:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", row["payment_date"]):
            raise ValueError("date must use YYYY-MM-DD")
        date.fromisoformat(row["payment_date"])
    except ValueError:
        invalid_dates += 1
assert invalid_dates == 5

for account in accounts.values():
    assert account["client_id"] in clients

print("Pipeline 1 fixture contract passed: 6005 rows and 5 of each controlled defect.")
