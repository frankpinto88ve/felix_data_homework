# Félix Data Engineering Manager take-home

This repository implements a small Iceberg lakehouse for remittances. It uses:

- **Apache Iceberg** for partitioned lakehouse tables.
- **DuckDB** as the local ingestion and dbt query engine.
- **Apache Iceberg REST Catalog** in Docker for local table metadata.
- **Python** only to load source CSVs into the required `raw` schema.
- **dbt** for modeled, tested, and documented transformations.

## Project layout

```text
data/raw/             Supplied CSV inputs (not committed)
ingestion/            CSV-to-Iceberg raw-table loader
lake/                 Generated Iceberg metadata and Parquet data (not committed)
warehouse/            Local DuckDB control database (not committed)
dbt_project/          dbt models, tests, macros, and documentation
```

## Prerequisites

Python 3.10+ is recommended.

Docker Desktop is required for the local Iceberg REST catalog.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run locally

1. Put `payments.csv`, `receipts.csv`, and `disbursements.csv` in `data/raw/`.
   The supplied archive can be unpacked without its macOS metadata files using:

   ```bash
   unzip -j Arquivo.zip payments.csv receipts.csv disbursements.csv -d data/raw
   ```

2. Start the local Iceberg catalog:

   ```bash
   docker compose up -d iceberg-rest
   docker compose build pipeline
   ```

3. Load whichever source files are available into Iceberg's `raw` schema:

   ```bash
   docker compose run --rm pipeline python ingestion/load_raw_data.py
   ```

4. Build the empty staging and intermediate tables from the historical raw data:

   ```bash
   docker compose run --rm pipeline sh -lc 'cd dbt_project && dbt build --profiles-dir . --vars "{\"initial_load\": true}"'
   ```

## Initial and incremental transformation runs

Use `initial_load: true` only once: when staging and intermediate are empty and you need the historical backfill. It uses append semantics and skips the expensive source sort because the provided historical snapshot has unique source IDs.

```bash
docker compose run --rm pipeline sh -lc 'cd dbt_project && dbt build --profiles-dir . --vars "{initial_load: true}"'
```

After that, there is one normal transformation run: execute it daily with a rolling 48-hour raw-ingestion window.

```bash
docker compose run --rm pipeline sh -lc "cd dbt_project && dbt build --profiles-dir . --vars '{window_start: \"2026-10-23 15:00:00+00:00\", window_end: \"2026-10-25 15:00:00+00:00\"}'"
```

`window_start` and `window_end` are runtime variables. Adjust them ad hoc when a specific repair or backfill needs a different UTC interval; keep the end exclusive and avoid overlapping concurrent runs.

The window is start-inclusive and end-exclusive. It rereads raw records ingested during the prior 48 hours, merges their newest versions into staging, and refreshes the affected intermediate remittances. Overlap with the previous daily run is intentional and does not create duplicates. Raw remains append-only; staging retains one current row per source ID in its original event-date partition.

The loader treats each source file independently. It appends newly seen files, records a run UUID and minute-truncated UTC ingestion timestamp on every raw row, and skips an exact file redelivery based on its SHA-256 content hash.

## Data model and event assumptions

### Relation design

| Layer | Relation | Grain / primary key | Partitioning | Purpose |
| --- | --- | --- | --- | --- |
| Raw | `lake.raw.payments` | Source payment record (`id`) | `_ingested_at` day | Append-only payment CDC records plus ingestion lineage. |
| Raw | `lake.raw.receipts` | Source receipt record (`id`) | `_ingested_at` day | Append-only receipt CDC records plus ingestion lineage. |
| Raw | `lake.raw.disbursements` | Source disbursement record (`id`) | `_ingested_at` day | Append-only disbursement CDC records plus ingestion lineage. |
| Staging | `lake.staging.stg_payments` | One current row per `payment_id` | `payment_created_at` day | Typed, canonical payment state. |
| Staging | `lake.staging.stg_receipts` | One current row per `receipt_id` | `receipt_created_at` day | Typed receipt and sender/beneficiary links. |
| Staging | `lake.staging.stg_disbursements` | One current row per `disbursement_id` | `disbursement_created_at` day | Typed provider payout state. |
| Intermediate | `lake.intermediate.int_receipts_enriched` | One row per `receipt_id` | `receipt_created_at` day | Receipt-driven remittance record, enriched with payment and disbursement state. |
| Marts | `felix.marts.*` | Metric-specific | None; query-time views | Current-month assignment metrics. |

### Core schemas

The raw tables retain all supplied CSV columns and add the same ingestion lineage fields:

| Raw lineage column | Meaning |
| --- | --- |
| `_ingestion_run_id` | UUID shared by every file processed in one loader invocation. |
| `_ingested_at` | UTC loader timestamp, truncated to the minute. |
| `_source_file_name` | Delivered filename, retained for observability only. |
| `_source_file_hash` | SHA-256 hash of file content, used for redelivery detection. |

The modeled business columns are:

| Relation | Important columns |
| --- | --- |
| `stg_payments` | `payment_id`, `sender_user_id`, `payment_amount`, `payment_currency`, payment statuses, `has_dispute_chargeback`, `payment_created_at`, `payment_updated_at` |
| `stg_receipts` | `receipt_id`, `payment_id`, `disbursement_id`, `sender_user_id`, `beneficiary_id`, `amount_charged`, `fee_amount`, destination country/currency, `receipt_created_at` |
| `stg_disbursements` | `disbursement_id`, `provider_name`, payout statuses, `disbursement_amount`, destination country/currency, `disbursement_created_at`, `disbursement_updated_at` |
| `int_receipts_enriched` | Receipt keys and amounts plus linked payment/disbursement fields, `has_payment`, `has_disbursement`, `is_complete_remittance`, and `remittance_updated_at` |

### Delivery and time assumptions

- Payments, receipts, and disbursements are independent CDC deliveries. A run does not require all three files, and joins never rely on the loader batch UUID.
- A file is skipped only when its content hash has already completed ingestion. A renamed file with identical content is therefore skipped; a new file containing repeated business records is retained in raw and resolved downstream.
- Raw is append-only. Staging selects the newest CDC version for each source ID and merges it into the canonical current row.
- Intermediate is receipt-driven and uses left joins to payment and disbursement. It can therefore retain an incomplete remittance while an independently delivered counterpart is still absent.
- Assignment marts use only `is_complete_remittance = TRUE`, so incomplete joins do not affect reported metrics.
- Raw is partitioned by ingestion day for efficient incremental reads. Staging and intermediate are partitioned by source event day so late records remain in the partition matching when the business event occurred.
- The standard 48-hour run is the accepted finality window. It repairs normal late arrivals; records corrected after that window require an explicit ad hoc run with wider `window_start` and `window_end` values.

## Assignment metric views

The local DuckDB `felix.marts` schema contains query-time views over the Iceberg intermediate table. Iceberg supports the data tables, while DuckDB stores the view definitions because its Iceberg catalog integration cannot create views. Each view is restricted to the active UTC calendar month, so it automatically recalculates as new intermediate data arrives:

- `monthly_remittance_metrics`: sender-side volume (`amount_charged`) and payment chargeback rate by month, destination corridor, and payment currency.
- `monthly_user_metrics`: monthly sender volume, recurring-customer classification (two or more remittances), and the users comprising the top 50% of volume within each payment currency.
- `monthly_provider_disbursement_metrics`: provider disbursement count, strict-`FAILED` count and rate, and provider classification.
- `beneficiary_sender_clusters`: beneficiaries shared by two or more senders, with an array of those sender IDs.

The views deliberately do not persist aggregates: querying them always reflects the current intermediate state without a separate mart refresh job.
