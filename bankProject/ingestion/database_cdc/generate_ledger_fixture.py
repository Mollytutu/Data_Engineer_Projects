"""Build 100 ledger rows from valid project account/client references."""

import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_ROOT = PROJECT_ROOT / "data/raw/reference"
OUTPUT = PROJECT_ROOT / "data/raw/core_banking/account_ledger_transactions.csv"
TRANSACTION_TYPES = (
    "SALARY_DEPOSIT", "WIRE_IN", "WIRE_OUT", "ACH_CREDIT", "ACH_DEBIT",
    "ATM_WITHDRAWAL", "BANK_FEE", "INTERNAL_TRANSFER", "INTEREST",
)
CREDIT_TYPES = {"SALARY_DEPOSIT", "WIRE_IN", "ACH_CREDIT", "INTEREST"}
FIELDS = (
    "transaction_id", "account_id", "client_id", "transaction_timestamp",
    "transaction_type", "debit_credit", "amount", "currency",
    "balance_after_transaction", "available_balance", "ledger_balance",
    "status", "updated_at",
)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    clients = {
        row["client_id"]: row
        for row in read_rows(REFERENCE_ROOT / "clients.csv")
        if row["status"] == "ACTIVE"
    }
    accounts = [
        row
        for row in read_rows(REFERENCE_ROOT / "accounts.csv")
        if row["status"] == "ACTIVE" and row["client_id"] in clients
    ][:100]
    if len(accounts) != 100:
        raise ValueError("Reference data must contain at least 100 active account/client pairs")

    start = datetime(2026, 8, 10, 7, 0, tzinfo=timezone.utc)
    output_rows = []
    for index, account in enumerate(accounts, start=1):
        transaction_type = TRANSACTION_TYPES[(index - 1) % len(TRANSACTION_TYPES)]
        debit_credit = "C" if transaction_type in CREDIT_TYPES else "D"
        amount = Decimal("25.00") + Decimal(index) * Decimal("7.35")
        starting_balance = Decimal(account["available_balance"])
        signed_amount = amount if debit_credit == "C" else -amount
        balance = starting_balance + signed_amount
        transaction_time = start + timedelta(minutes=index)
        output_rows.append(
            {
                "transaction_id": f"L{index:03d}",
                "account_id": account["account_id"],
                "client_id": account["client_id"],
                "transaction_timestamp": transaction_time.isoformat(),
                "transaction_type": transaction_type,
                "debit_credit": debit_credit,
                "amount": f"{amount:.2f}",
                "currency": account["currency"],
                "balance_after_transaction": f"{balance:.2f}",
                "available_balance": f"{balance:.2f}",
                "ledger_balance": f"{balance:.2f}",
                "status": "PENDING" if index % 10 == 2 else "POSTED",
                "updated_at": (transaction_time + timedelta(seconds=1)).isoformat(),
            }
        )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(output_rows)
    print(f"Created {len(output_rows)} reference-linked rows at {OUTPUT}")


if __name__ == "__main__":
    main()
