#!/bin/sh
set -eu

for table_path in \
    /warehouse/raw/payments \
    /warehouse/raw/receipts \
    /warehouse/raw/disbursements \
    /warehouse/staging/stg_payments \
    /warehouse/staging/stg_receipts \
    /warehouse/staging/stg_disbursements \
    /warehouse/intermediate/int_receipts_enriched
do
    mkdir -p "${table_path}/metadata" "${table_path}/data"
done

exec "$@"
