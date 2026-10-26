{{
    config(
        materialized='incremental',
        incremental_strategy='append' if var('initial_load', false) else 'merge',
        unique_key='receipt_id',
        database='lake',
        schema='staging'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'receipts') }}
    {% if is_incremental() %}
    WHERE _ingested_at >= TRY_CAST('{{ var("window_start", "1900-01-01 00:00:00+00:00") }}' AS TIMESTAMPTZ)
      AND _ingested_at < TRY_CAST('{{ var("window_end", "9999-12-31 23:59:59+00:00") }}' AS TIMESTAMPTZ)
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        {% if var('initial_load', false) %}
        1 AS _deduplication_rank
        {% else %}
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY
                _ingested_at DESC,
                _source_file_hash DESC
        ) AS _deduplication_rank
        {% endif %}
    FROM source
){% if is_incremental() and not var('initial_load', false) %},

existing AS (
    SELECT
        receipt_id,
        ingested_at
    FROM {{ this }}
){% endif %}

SELECT
    id AS receipt_id,
    destination_country_code,
    destination_currency,
    TRY_CAST(amount_charged AS DECIMAL(18, 2)) AS amount_charged,
    TRY_CAST(fee AS DECIMAL(18, 2)) AS fee_amount,
    TRY_CAST(exchange_rate AS DECIMAL(18, 6)) AS exchange_rate,
    TRY_CAST(promotion_amount AS DECIMAL(18, 2)) AS promotion_amount,
    TRY_CAST(destination_amount AS DECIMAL(18, 2)) AS destination_amount,
    user_id AS sender_user_id,
    payment_id,
    disbursement_id,
    beneficiary_id,
    TRY_CAST(created_at AS TIMESTAMP WITH TIME ZONE) AS receipt_created_at,
    _ingestion_run_id AS ingestion_run_id,
    _ingested_at AS ingested_at,
    _source_file_name AS source_file_name,
    _source_file_hash AS source_file_hash
FROM deduplicated AS source_data
{% if is_incremental() and not var('initial_load', false) %}
LEFT JOIN existing
    ON source_data.id = existing.receipt_id
{% endif %}
WHERE source_data._deduplication_rank = 1
{% if is_incremental() and not var('initial_load', false) %}
  AND (
      existing.receipt_id IS NULL
      OR source_data._ingested_at >= existing.ingested_at
  )
{% endif %}
