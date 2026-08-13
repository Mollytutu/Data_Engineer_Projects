"""Produce synthetic payment lifecycle events that honor the raw data contract."""

import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


TOPIC = "payment_events"
BOOTSTRAP_SERVERS = "localhost:9092"
BAD_EVENT_RATE = 0.02
MAX_RUNTIME_SECONDS = 10 * 60

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ACCOUNTS_PATH = PROJECT_ROOT / "data/raw/reference/accounts.csv"
CLIENTS_PATH = PROJECT_ROOT / "data/raw/reference/clients.csv"

PAYMENT_WEIGHTS = {
    "ACH": 0.20,
    "WIRE": 0.15,
    "SWIFT": 0.10,
    "SEPA": 0.10,
    "CARD": 0.15,
    "BOOK_TRANSFER": 0.10,
    "FX_TRANSFER": 0.10,
    "REAL_TIME_PAYMENT": 0.10,
}

AMOUNT_RANGES = {
    "ACH": (20, 5_000),
    "WIRE": (1_000, 100_000),
    "SWIFT": (5_000, 250_000),
    "SEPA": (20, 50_000),
    "CARD": (1, 10_000),
    "BOOK_TRANSFER": (20, 100_000),
    "FX_TRANSFER": (100, 250_000),
    "REAL_TIME_PAYMENT": (1, 25_000),
}

REJECTION_REASONS = [
    "INSUFFICIENT_FUNDS",
    "ACCOUNT_RESTRICTED",
    "INVALID_BENEFICIARY",
    "SANCTIONS_REVIEW_FAILED",
]

RETURN_REASONS = [
    "ACCOUNT_CLOSED",
    "INVALID_ACCOUNT",
    "CUSTOMER_REQUESTED_RETURN",
]


def load_reference_data():
    """Return eligible accounts and country codes from the raw reference data."""
    accounts = pd.read_csv(ACCOUNTS_PATH, dtype=str)
    clients = pd.read_csv(CLIENTS_PATH, dtype=str)

    required_account_columns = {
        "account_id",
        "client_id",
        "currency",
        "status",
    }
    required_client_columns = {
        "client_id",
        "country_code",
        "status",
    }

    missing_account_columns = required_account_columns - set(accounts.columns)
    missing_client_columns = required_client_columns - set(clients.columns)
    if missing_account_columns:
        raise ValueError(
            f"accounts.csv is missing columns: {sorted(missing_account_columns)}"
        )
    if missing_client_columns:
        raise ValueError(
            f"clients.csv is missing columns: {sorted(missing_client_columns)}"
        )

    accounts["status"] = accounts["status"].str.strip().str.upper()
    clients["status"] = clients["status"].str.strip().str.upper()
    clients["country_code"] = clients["country_code"].str.strip().str.upper()

    active_clients = clients.loc[
        clients["status"] == "ACTIVE",
        ["client_id", "country_code"],
    ]
    eligible_accounts = accounts.loc[
        accounts["status"] == "ACTIVE",
        ["account_id", "client_id", "currency"],
    ].merge(
        active_clients,
        on="client_id",
        how="inner",
        validate="many_to_one",
    )

    if eligible_accounts.empty:
        raise ValueError("No active accounts owned by active clients were found")
    if eligible_accounts["account_id"].duplicated().any():
        raise ValueError("Eligible account IDs must be unique")
    if eligible_accounts[["account_id", "client_id", "currency"]].isna().any().any():
        raise ValueError("Eligible accounts contain a missing required value")

    account_pool = eligible_accounts[
        ["account_id", "client_id", "currency"]
    ].to_dict("records")
    country_pool = sorted(active_clients["country_code"].dropna().unique().tolist())

    return account_pool, country_pool


def build_kafka_producer():
    # Import lazily so reference-data validation can run without a Kafka client.
    from kafka import KafkaProducer
    from kafka.serializer import DefaultSerializer, SerializeWrapper

    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Broker retries cannot create duplicates for one send operation.
        enable_idempotence=True,
        acks="all",
        max_in_flight_requests_per_connection=5,
        key_serializer=DefaultSerializer(encoding="utf-8"),
        value_serializer=SerializeWrapper(
            lambda value: json.dumps(value).encode("utf-8")
        ),
    )


def create_payment(account_pool, country_pool):
    """Build a valid payment using one complete account reference record."""
    account = random.choice(account_pool)
    payment_type = random.choices(
        list(PAYMENT_WEIGHTS),
        weights=list(PAYMENT_WEIGHTS.values()),
        k=1,
    )[0]
    minimum_amount, maximum_amount = AMOUNT_RANGES[payment_type]

    return {
        "payment_id": f"PAY-{uuid.uuid4().hex[:12].upper()}",
        "client_id": account["client_id"],
        "account_id": account["account_id"],
        "payment_type": payment_type,
        "currency": account["currency"].strip().upper(),
        "amount": round(random.uniform(minimum_amount, maximum_amount), 2),
        "direction": random.choice(["INBOUND", "OUTBOUND"]),
        "counterparty_country": random.choice(country_pool),
    }


def create_lifecycle(payment_type):
    """Create a valid status transition sequence for a payment rail."""
    if random.random() < 0.05:
        return [
            ("RECEIVED", None),
            ("VALIDATED", None),
            ("REJECTED", random.choice(REJECTION_REASONS)),
        ]

    rail_statuses = {
        "ACH": ["SUBMITTED_TO_ACH"],
        "WIRE": ["APPROVED", "SENT_TO_NETWORK"],
        "SWIFT": ["SANCTIONS_SCREENING", "APPROVED", "SENT_TO_SWIFT"],
        "SEPA": ["SUBMITTED_TO_SEPA"],
        "CARD": ["AUTHORIZED", "CAPTURED"],
        "BOOK_TRANSFER": ["POSTED_TO_LEDGER"],
        "FX_TRANSFER": ["FX_RATE_APPLIED", "SENT_TO_NETWORK"],
        "REAL_TIME_PAYMENT": ["SENT_TO_NETWORK"],
    }
    statuses = ["RECEIVED", "VALIDATED", *rail_statuses[payment_type], "SETTLED"]
    lifecycle = [(status, None) for status in statuses]

    if random.random() < 0.03:
        lifecycle.append(("RETURNED", random.choice(RETURN_REASONS)))

    return lifecycle


def inject_bad_data(event):
    """Mutate at most one field and return whether the event should be duplicated."""
    if random.random() >= BAD_EVENT_RATE:
        return event, False

    problem = random.choice(
        [
            "DUPLICATE_EVENT",
            "MISSING_ACCOUNT_ID",
            "INVALID_CURRENCY",
            "NEGATIVE_AMOUNT",
            "MALFORMED_OR_LATE_EVENT_TIME",
        ]
    )
    should_duplicate = problem == "DUPLICATE_EVENT"

    if problem == "MISSING_ACCOUNT_ID":
        event["account_id"] = None
    elif problem == "INVALID_CURRENCY":
        event["currency"] = "INVALID"
    elif problem == "NEGATIVE_AMOUNT":
        event["amount"] = -abs(event["amount"])
    elif problem == "MALFORMED_OR_LATE_EVENT_TIME":
        if random.choice([True, False]):
            event["event_time"] = "BAD_TIMESTAMP"
        else:
            event["event_time"] = (
                datetime.now(timezone.utc) - timedelta(days=2)
            ).isoformat()

    # Debug-only metadata lets tests verify the injected fixture category.
    event["mock_problem_type"] = problem
    return event, should_duplicate


def create_event(payment, previous_status, new_status, reason):
    now = datetime.now(timezone.utc).isoformat()
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "PAYMENT_STATUS_CHANGED",
        "event_time": now,
        "ingested_at": now,
        "payment_id": payment["payment_id"],
        "client_id": payment["client_id"],
        "account_id": payment["account_id"],
        "payment_type": payment["payment_type"],
        "direction": payment["direction"],
        "currency": payment["currency"],
        "amount": payment["amount"],
        "counterparty_country": payment["counterparty_country"],
        "previous_status": previous_status,
        "new_status": new_status,
        "reason_code": reason,
    }
    return inject_bad_data(event)


def send_event(producer, event, should_duplicate=False):
    """Send an event and, for a duplicate fixture, resend the identical payload."""
    kafka_key = event["payment_id"]
    producer.send(TOPIC, key=kafka_key, value=event)
    if should_duplicate:
        producer.send(TOPIC, key=kafka_key, value=event)
    print(json.dumps(event, indent=2))


def run():
    account_pool, country_pool = load_reference_data()
    producer = build_kafka_producer()
    deadline = time.monotonic() + MAX_RUNTIME_SECONDS
    print(
        f"Producing to {TOPIC} with {len(account_pool)} eligible accounts "
        f"and {len(country_pool)} reference countries for at most "
        f"{MAX_RUNTIME_SECONDS // 60} minutes"
    )

    try:
        while time.monotonic() < deadline:
            payment = create_payment(account_pool, country_pool)
            lifecycle = create_lifecycle(payment["payment_type"])
            previous_status = None

            for new_status, reason in lifecycle:
                if time.monotonic() >= deadline:
                    break
                event, should_duplicate = create_event(
                    payment,
                    previous_status,
                    new_status,
                    reason,
                )
                send_event(producer, event, should_duplicate)
                previous_status = new_status
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(min(random.uniform(0.5, 2.0), remaining))

            producer.flush()
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(random.uniform(1.0, 3.0), remaining))
    except KeyboardInterrupt:
        print("Stopping payment event producer")
    finally:
        producer.flush()
        producer.close()
        print("Payment event producer stopped")


if __name__ == "__main__":
    run()
