{{
    config(
        materialized='incremental',
        incremental_strategy='append' if var('initial_load', false) else 'merge',
        unique_key='receipt_id',
        database='lake',
        schema='intermediate'
    )
}}

WITH receipts_in_window AS (
    SELECT *
    FROM {{ ref('stg_receipts') }}
    {% if is_incremental() %}
    WHERE receipt_created_at >= TRY_CAST('{{ var("window_start", "1900-01-01 00:00:00+00:00") }}' AS TIMESTAMPTZ)
      AND receipt_created_at < TRY_CAST('{{ var("window_end", "9999-12-31 23:59:59+00:00") }}' AS TIMESTAMPTZ)
    {% endif %}
),

changed_payments AS (
    SELECT payment_id
    FROM {{ ref('stg_payments') }}
    {% if is_incremental() %}
    WHERE ingested_at >= TRY_CAST('{{ var("window_start", "1900-01-01 00:00:00+00:00") }}' AS TIMESTAMPTZ)
      AND ingested_at < TRY_CAST('{{ var("window_end", "9999-12-31 23:59:59+00:00") }}' AS TIMESTAMPTZ)
    {% endif %}
),

changed_disbursements AS (
    SELECT disbursement_id
    FROM {{ ref('stg_disbursements') }}
    {% if is_incremental() %}
    WHERE ingested_at >= TRY_CAST('{{ var("window_start", "1900-01-01 00:00:00+00:00") }}' AS TIMESTAMPTZ)
      AND ingested_at < TRY_CAST('{{ var("window_end", "9999-12-31 23:59:59+00:00") }}' AS TIMESTAMPTZ)
    {% endif %}
),

affected_receipt_ids AS (
    SELECT receipt_id
    FROM receipts_in_window

    {% if is_incremental() and not var('initial_load', false) %}
    UNION

    SELECT receipts.receipt_id
    FROM {{ ref('stg_receipts') }} AS receipts
    INNER JOIN changed_payments
        ON receipts.payment_id = changed_payments.payment_id
    WHERE receipts.receipt_created_at >= TRY_CAST('{{ var("window_start", "1900-01-01 00:00:00+00:00") }}' AS TIMESTAMPTZ)
      AND receipts.receipt_created_at < TRY_CAST('{{ var("window_end", "9999-12-31 23:59:59+00:00") }}' AS TIMESTAMPTZ)

    UNION

    SELECT receipts.receipt_id
    FROM {{ ref('stg_receipts') }} AS receipts
    INNER JOIN changed_disbursements
        ON receipts.disbursement_id = changed_disbursements.disbursement_id
    WHERE receipts.receipt_created_at >= TRY_CAST('{{ var("window_start", "1900-01-01 00:00:00+00:00") }}' AS TIMESTAMPTZ)
      AND receipts.receipt_created_at < TRY_CAST('{{ var("window_end", "9999-12-31 23:59:59+00:00") }}' AS TIMESTAMPTZ)
    {% endif %}
),

remittances AS (
    SELECT
        receipts.receipt_id,
        receipts.payment_id,
        receipts.disbursement_id,
        receipts.sender_user_id,
        receipts.beneficiary_id,
        receipts.destination_country_code,
        receipts.destination_currency,
        receipts.amount_charged,
        receipts.fee_amount,
        receipts.exchange_rate,
        receipts.promotion_amount,
        receipts.destination_amount,
        receipts.receipt_created_at,
        payments.payment_amount,
        payments.payment_currency,
        payments.payment_status,
        payments.payment_detailed_status,
        payments.payment_type,
        payments.has_dispute_chargeback,
        payments.payment_created_at,
        payments.payment_updated_at,
        disbursements.disbursement_user_id,
        disbursements.disbursement_type,
        disbursements.disbursement_status,
        disbursements.disbursement_raw_status,
        disbursements.provider_status,
        disbursements.disbursement_amount,
        disbursements.provider_name,
        disbursements.beneficiary_name,
        disbursements.destination_country,
        disbursements.disbursement_created_at,
        disbursements.disbursement_updated_at,
        payments.payment_id IS NOT NULL AS has_payment,
        disbursements.disbursement_id IS NOT NULL AS has_disbursement,
        payments.payment_id IS NOT NULL
            AND disbursements.disbursement_id IS NOT NULL AS is_complete_remittance,
        GREATEST(
            receipts.ingested_at,
            COALESCE(payments.ingested_at, receipts.ingested_at),
            COALESCE(disbursements.ingested_at, receipts.ingested_at)
        ) AS remittance_updated_at
    FROM {{ ref('stg_receipts') }} AS receipts
    INNER JOIN affected_receipt_ids
        ON receipts.receipt_id = affected_receipt_ids.receipt_id
    LEFT JOIN {{ ref('stg_payments') }} AS payments
        ON receipts.payment_id = payments.payment_id
    LEFT JOIN {{ ref('stg_disbursements') }} AS disbursements
        ON receipts.disbursement_id = disbursements.disbursement_id
)

SELECT *
FROM remittances
