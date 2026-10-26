# dbt model layers

- `staging/`: source-level cleanup, type casting, renamed fields, and deduplication.
- `intermediate/`: business-context joins, especially the payment → receipt → disbursement remittance lifecycle. `int_receipts_enriched` upserts only receipt-event rows inside the configured incremental window.
- `marts/`: consumer-facing facts, dimensions, and metric models.
