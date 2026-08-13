# Raw banking data

This directory represents immutable source extracts in the pipeline's raw zone. Transformations should normally write to the curated zone rather than modify files here. The CSV files currently included are synthetic seed extracts for the project.

## Directory layout

```text
raw/
├── core_banking/
│   └── account_ledger_transactions.csv
├── payments/
│   └── payments.csv
└── reference/
    ├── clients.csv
    ├── individual_clients.csv
    ├── business_clients.csv
    └── accounts.csv
```

`core_banking/account_ledger_transactions.csv` contains the 100-row historical
source fixture for Pipeline 3. Its `account_id`, `client_id`, and `currency`
come from matching active rows in the reference files.

## Client model

A client is the customer relationship held by the bank. A client can be either an individual person or a business. The common identity and relationship attributes belong in `clients.csv`; attributes that apply only to one kind of client belong in a subtype file.

### `reference/clients.csv`

One row per bank customer. `client_id` is the primary key.

| Column | Meaning |
| --- | --- |
| `client_id` | Stable customer identifier |
| `client_type` | Discriminator: `INDIVIDUAL` or `CORPORATE` |
| `country_code` | Client's ISO-style country code |
| `risk_rating` | Current customer risk classification |
| `onboarding_date` | Date the banking relationship began |
| `status` | Current relationship status |

Names are not stored in this common table because personal names and business legal names have different structure and validation rules.

### `reference/individual_clients.csv`

One row for each `clients.csv` row whose `client_type` is `INDIVIDUAL`.

| Column | Meaning |
| --- | --- |
| `client_id` | Primary key and foreign key to `clients.client_id` |
| `given_name` | Person's given/first name |
| `family_name` | Person's family/last name |

Separating the name components supports customer communication, identity matching, KYC, and sanctions screening. A production international name model may also need middle names, prefixes, suffixes, preferred names, native-script names, and effective dates.

### `reference/business_clients.csv`

One row for each `clients.csv` row whose `client_type` is `CORPORATE`.

| Column | Meaning |
| --- | --- |
| `client_id` | Primary key and foreign key to `clients.client_id` |
| `legal_name` | Registered legal name of the organization |

A production model would commonly add registration number, jurisdiction, entity type, trade name, incorporation date, and beneficial-ownership relationships.

## Accounts and payments

### `reference/accounts.csv`

One row per bank account. `account_id` is the primary key and `client_id` is a foreign key to `clients.client_id`. A client may own multiple accounts. Both individual and corporate clients use this table because an account has the same core relationship to its owner regardless of client type.

### `payments/payments.csv`

One row per payment. `payment_id` is the primary key. `client_id` identifies the bank customer and `account_id` identifies that customer's account. The remaining fields describe the date, direction, payment rail/type, currency, amount, counterparty country, and processing status.

The `counterparty_country` describes the other side of a payment; it does not identify another client record.

### Intentional payment quality problems

`payments.csv` deliberately contains the following defects for data-quality, quarantine, and reconciliation exercises. These are test fixtures rather than accidental source corruption.

| Problem | Expected count | Fixture records |
| --- | ---: | --- |
| Duplicate payments | 5 extra rows | A second copy of `PAY-00000001` through `PAY-00000005` |
| Missing `account_id` | 5 rows | `PAY-00005976` through `PAY-00005980` |
| Invalid currency code | 5 rows | `PAY-00005981` through `PAY-00005985` |
| Negative amount | 5 rows | `PAY-00005986` through `PAY-00005990` |
| Malformed or missing `payment_date` | 5 rows | `PAY-00005991` through `PAY-00005995` |

The file therefore has 6,005 data rows but only 6,000 distinct payment IDs. Each non-duplicate fixture belongs to exactly one problem category so validation counts remain deterministic. The date fixtures represent the requested malformed-or-late-record category by using malformed or missing source dates; true arrival lateness would require a separate ingestion timestamp.

## Relationships and integrity rules

```text
clients (1) ── (0..1) individual_clients
        (1) ── (0..1) business_clients
        (1) ── (many) accounts
        (1) ── (many) payments
accounts (1) ── (many) payments
```

- Every client must appear in exactly one subtype file, as selected by `client_type`.
- Every account must reference an existing client.
- Outside the intentional quality fixtures above, every payment must reference an existing client and account.
- Outside the intentional quality fixtures above, a payment's account must belong to the same client recorded on the payment.
- IDs, rather than names, are used for joins and uniqueness.
