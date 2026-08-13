# Data guide and mock-data contract

This directory contains the synthetic data used by the Citi-style banking pipeline. It is the shared contract between mock-data producers, batch ingestion, Kafka streaming, Spark/Glue validation, Snowflake, dbt, fraud monitoring, reconciliation, and reporting.

The data is fictional and must not contain real customer information. “Citi” describes the architecture being studied; this repository is not an official Citibank system or data model.

## Pipeline context

```text
Mock source systems
├── Customer/KYC source ──────────────┐
├── Core account source ──────────────┼── batch files ──> raw ──> curated
└── Payment processing source ────────┘
                                      └── Kafka events ─> streaming validation

raw/curated ─> Snowflake ─> dbt core and marts ─> fraud, reconciliation, reporting
```

- [`raw/`](raw/README.md) represents immutable, source-aligned landing data, similar to an S3 raw zone.
- [`curated/`](curated/README.md) is reserved for validated, standardized, deduplicated data.
- Batch extracts may be CSV or JSON at the source boundary.
- Streaming payments are JSON events sent to the `payment_events` Kafka topic.
- The Spark consumer supports local Parquet output for development and direct `s3a://` output for the production-style path; it never copies Kafka broker files.
- Large production-style outputs should use Parquet in the curated zone because it is typed, compressed, and columnar.

## Business domains and mock sources

| Domain | Mock source system | Current dataset | Natural arrival pattern |
| --- | --- | --- | --- |
| Client relationship | CRM/KYC platform | `raw/reference/clients.csv` | Daily snapshot or change-data capture |
| Individual identity | KYC/customer master | `raw/reference/individual_clients.csv` | Daily snapshot or change-data capture |
| Business identity | Legal-entity master | `raw/reference/business_clients.csv` | Daily snapshot or change-data capture |
| Accounts | Core banking system | `raw/reference/accounts.csv` | Daily snapshot plus account changes |
| Payments | Payment hub/rails | `raw/payments/payments.csv` | Batch settlement extract |
| Payment lifecycle | Payment hub | Kafka `payment_events` JSON | Continuous event stream |

When adding another source, first identify its owning business system and grain. Do not combine unrelated grains in one table. For example, one client, one account, one payment, and one payment status event are four different grains.

## Entity model

```text
clients
├── individual_clients (exactly one subtype when client_type = INDIVIDUAL)
├── business_clients   (exactly one subtype when client_type = CORPORATE)
└── accounts
    └── payments
        └── payment status events
```

| Relationship | Rule |
| --- | --- |
| Client to subtype | Each client appears in exactly one type-specific table |
| Client to account | One client may own many accounts; each account has one client in this simplified model |
| Account to payment | One account may have many payments; each payment uses one account |
| Payment to event | One payment produces an ordered lifecycle of status events |

Real banking systems also require joint ownership, authorized signers, beneficiaries, addresses, beneficial owners, and account-party roles. Those should be modeled as relationship tables instead of adding repeated columns such as `client_id_2`.

## Current batch schemas

### Clients

`raw/reference/clients.csv` has one row per bank customer.

| Column | Type | Required | Rule |
| --- | --- | --- | --- |
| `client_id` | string | Yes | Primary key; `CLIENT-` plus a zero-padded number |
| `client_type` | string | Yes | `INDIVIDUAL` or `CORPORATE` |
| `country_code` | string | Yes | Uppercase ISO 3166-1 alpha-2 code |
| `risk_rating` | string | Yes | `LOW`, `MEDIUM`, or `HIGH` |
| `onboarding_date` | date | Yes | ISO `YYYY-MM-DD` |
| `status` | string | Yes | Controlled client-status value such as `ACTIVE` or `SUSPENDED` |

### Individual clients

`raw/reference/individual_clients.csv` has one row for every individual client.

| Column | Type | Required | Rule |
| --- | --- | --- | --- |
| `client_id` | string | Yes | Primary key and foreign key to `clients.client_id` |
| `given_name` | string | Yes | Person's given name |
| `family_name` | string | Yes | Person's family name |

Names are attributes, not identifiers. Duplicate names are valid; `client_id` provides uniqueness. International production data may require middle names, preferred names, prefixes, suffixes, native-script values, and name history.

### Business clients

`raw/reference/business_clients.csv` has one row for every corporate client.

| Column | Type | Required | Rule |
| --- | --- | --- | --- |
| `client_id` | string | Yes | Primary key and foreign key to `clients.client_id` |
| `legal_name` | string | Yes | Registered organization name |

Future business attributes should include registration number, jurisdiction, legal-entity type, incorporation date, trade name, and beneficial-owner relationships.

### Accounts

`raw/reference/accounts.csv` has one row per account.

| Column | Type | Required | Rule |
| --- | --- | --- | --- |
| `account_id` | string | Yes | Primary key; `ACC-` plus a zero-padded number |
| `client_id` | string | Yes | Foreign key to `clients.client_id` |
| `account_type` | string | Yes | Controlled account-product value |
| `currency` | string | Yes | Uppercase ISO 4217 code |
| `country_code` | string | Yes | Booking-country ISO alpha-2 code |
| `opened_date` | date | Yes | ISO `YYYY-MM-DD` |
| `status` | string | Yes | Controlled account-status value |
| `available_balance` | decimal(18,2) | Yes | Available balance in the account currency |

An account's currency does not have to equal the client's country currency. Multi-currency banking is intentional.

### Payments

`raw/payments/payments.csv` has one row per batch payment.

| Column | Type | Required | Rule |
| --- | --- | --- | --- |
| `payment_id` | string | Yes | Primary business key |
| `client_id` | string | Yes | Foreign key to `clients.client_id` |
| `account_id` | string | Yes | Foreign key to `accounts.account_id` |
| `payment_date` | date | Yes | ISO `YYYY-MM-DD` |
| `direction` | string | Yes | `INBOUND` or `OUTBOUND` from the bank client's perspective |
| `payment_type` | string | Yes | `ACH`, `WIRE`, `SWIFT`, `SEPA`, `CARD`, `BOOK_TRANSFER`, `FX_TRANSFER`, or `REAL_TIME_PAYMENT` |
| `currency` | string | Yes | Uppercase ISO 4217 code |
| `amount` | decimal(18,2) | Yes | Positive transaction amount; direction is stored separately |
| `counterparty_country` | string | Yes | Country of the other party, not the client's country |
| `status` | string | Yes | Final/current batch status: `PENDING`, `SETTLED`, `REJECTED`, or `RETURNED` |

In this simplified model, the payment currency normally comes from the selected account. A future foreign-exchange model should add instructed currency, settlement currency, exchange rate, and converted amount explicitly.

## Streaming payment event contract

A stream record represents a payment lifecycle change, not another copy of the batch payment row. The Kafka message key should be `payment_id` so events for the same payment remain in the same partition.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `event_id` | UUID string | Yes | Unique event identifier used for event-level deduplication |
| `event_type` | string | Yes | Currently `PAYMENT_STATUS_CHANGED` |
| `event_time` | timestamp | Yes | UTC ISO 8601 business-event time |
| `ingested_at` | timestamp | Recommended | UTC time the platform received the event; required to measure late arrival |
| `payment_id` | string | Yes | Stable across every lifecycle event for one payment |
| `client_id` | string | Yes | Must match the owner of `account_id` |
| `account_id` | string | Yes | Must exist and be eligible for payment generation |
| `payment_type` | string | Yes | Uses the same controlled values as batch payments |
| `direction` | string | Yes | `INBOUND` or `OUTBOUND` |
| `currency` | string | Yes | Uppercase ISO 4217 code |
| `amount` | decimal | Yes | Positive and stable across the payment lifecycle |
| `counterparty_country` | string | Yes | Uppercase ISO alpha-2 code |
| `previous_status` | string/null | Yes | Null only for the first lifecycle event |
| `new_status` | string | Yes | New lifecycle state |
| `reason_code` | string/null | No | Controlled rejection or return reason |

Intermediate event statuses such as `RECEIVED`, `VALIDATED`, `APPROVED`, and `SENT_TO_NETWORK` are valid in the stream even though the batch file exposes only final/current statuses.

For valid traffic, select an active account owned by an active client. Copy `account_id`, `client_id`, and account `currency` together from the same reference row. Never select these fields independently.

## Column naming standard

- Use lowercase `snake_case`: `counterparty_country`, not `CounterpartyCountry`.
- Use singular entity names in columns: `client_id`, not `clients_id`.
- End stable identifiers and foreign keys with `_id`.
- End calendar dates with `_date`; store them as `YYYY-MM-DD`.
- End instants with `_at` or `_time`; store UTC ISO 8601 timestamps with an offset or `Z`.
- End currency values with `_currency` only when qualification is needed, such as `settlement_currency`; the existing single value is `currency`.
- Store monetary values as decimal, never binary floating point in curated or warehouse models.
- Keep amount sign semantics consistent. Here, `amount` is positive and `direction` carries inbound/outbound meaning.
- Use uppercase controlled codes for statuses, countries, currencies, and payment types.
- Use `is_...` or `has_...` for booleans.
- Do not encode business meaning in row order, filenames, or display names.
- Do not use ambiguous fields such as `name`, `type`, `date`, or `status` when multiple entities appear in the same model. Qualify them when needed.
- Do not place ingestion metadata in business fields. Use names such as `source_system`, `source_file`, `ingested_at`, `batch_id`, `kafka_topic`, `kafka_partition`, and `kafka_offset`.

## How to generate matching mock data

Generate parent data before child data so every foreign key can be selected from an existing pool:

1. Generate `clients` with stable IDs, country, risk, onboarding date, and status.
2. Generate exactly one matching subtype record per client.
3. Generate one or more accounts by selecting an existing `client_id`.
4. Generate batch payments by selecting one complete account record and copying its `account_id`, `client_id`, and currency.
5. Generate lifecycle events from one payment object. Reuse its payment, client, account, currency, amount, and direction for every event.
6. Inject controlled quality problems only after a valid record has been constructed.

Use a fixed random seed for reproducible test datasets. Keep ID generation deterministic for batch fixtures and collision-resistant for streaming events. Test uniqueness rather than assuming a random identifier cannot collide.

### Cross-field rules

- `accounts.client_id` must exist in `clients`.
- `payments.client_id` must exist in `clients`.
- `payments.account_id` must exist in `accounts`.
- `payments.client_id` must equal the owner recorded on the selected account.
- An individual client must exist only in `individual_clients`; a corporate client must exist only in `business_clients`.
- `opened_date` must not precede the modeled system's allowed history or occur after the processing date.
- A payment event's `previous_status` must equal the preceding event's `new_status`.
- Terminal lifecycle states must follow an allowed transition path.

## Controlled bad data

Bad data should be deliberate, labeled in documentation, isolated to known records, and reproducible. Build a valid record first and then mutate one field so a test normally has one expected failure reason.

The current payment batch contains exactly five fixtures in each category:

- duplicate payment rows;
- missing `account_id` values;
- invalid currency codes;
- negative amounts; and
- malformed or missing payment dates.

Exact fixture IDs are listed in [`raw/README.md`](raw/README.md). A streaming producer should mock equivalent categories using duplicate events, missing accounts, invalid currencies, negative amounts, and malformed or late event times. Missing and nonexistent IDs are different tests and should have different rejection codes.

To test true late arrival, keep a valid old `event_time` and a later `ingested_at`. A malformed timestamp tests parsing; it does not test watermark behavior.

## Big-data layout conventions

Do not create one ever-growing production file. Preserve raw payloads and partition larger datasets by low-cardinality time fields:

```text
raw/payments/source_system=payment_hub/ingestion_date=2026-08-08/part-*.json
curated/payments/payment_date=2026-08-08/part-*.parquet
curated/payment_events/event_date=2026-08-08/hour=14/part-*.parquet
quarantine/payment_events/error_date=2026-08-08/error_code=INVALID_CURRENCY/part-*.parquet
```

- Avoid partitioning by high-cardinality IDs such as `payment_id` or `client_id`.
- Treat raw objects as immutable and retain source metadata.
- Use schema versioning for events, preferably with `schema_version` or a schema registry.
- Use Kafka offsets plus `event_id` for replay-safe processing.
- Deduplicate within the watermark appropriate to the business SLA.
- Route invalid records to quarantine with the original payload, error code, error detail, and ingestion metadata.
- Compact small files in curated storage before analytical reads.
- Do not log names, account details, or full payloads in a real regulated environment.

## Required validation before publishing data

At minimum, automated tests should verify:

- required columns and expected data types;
- primary-key uniqueness outside named duplicate fixtures;
- subtype completeness and exclusivity;
- foreign-key and account-owner integrity;
- allowed enum, country, and currency values;
- positive amounts and decimal precision;
- valid dates and UTC timestamps;
- valid lifecycle transitions;
- expected counts for controlled bad-data fixtures; and
- no unexpected schema drift.

Any new mock source should update this contract, the raw-zone documentation, validation tests, and the corresponding producer or ingestion configuration in the same change.
