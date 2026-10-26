{{
    config(
        materialized='incremental',
        incremental_strategy='append' if var('initial_load', false) else 'merge',
        unique_key='payment_id',
        database='lake',
        schema='staging'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'payments') }}
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
                TRY_CAST(updated_at AS TIMESTAMP WITH TIME ZONE) DESC NULLS LAST,
                _ingested_at DESC,
                _source_file_hash DESC
        ) AS _deduplication_rank
        {% endif %}
    FROM source
){% if is_incremental() and not var('initial_load', false) %},

existing AS (
    SELECT
        payment_id,
        payment_updated_at
    FROM {{ this }}
){% endif %}

SELECT
    id AS payment_id,
    TRY_CAST(amount AS DECIMAL(18, 2)) AS payment_amount,
    currency AS payment_currency,
    user_id AS sender_user_id,
    status AS payment_status,
    detailed_status AS payment_detailed_status,
    NULLIF(error_code, '') AS payment_error_code,
    NULLIF(reason, '') AS payment_reason,
    type AS payment_type,
    TRY_CAST(total_intents AS INTEGER) AS total_payment_intents,
    NULLIF(location_of_request_state_code, '') AS request_state_code,
    TRY_CAST(has_dispute_chargeback AS BOOLEAN) AS has_dispute_chargeback,
    TRY_CAST(created_at AS TIMESTAMP WITH TIME ZONE) AS payment_created_at,
    TRY_CAST(updated_at AS TIMESTAMP WITH TIME ZONE) AS payment_updated_at,
    _ingestion_run_id AS ingestion_run_id,
    _ingested_at AS ingested_at,
    _source_file_name AS source_file_name,
    _source_file_hash AS source_file_hash
FROM deduplicated AS source_data
{% if is_incremental() and not var('initial_load', false) %}
LEFT JOIN existing
    ON source_data.id = existing.payment_id
{% endif %}
WHERE source_data._deduplication_rank = 1
{% if is_incremental() and not var('initial_load', false) %}
  AND (
      existing.payment_id IS NULL
      OR TRY_CAST(source_data.updated_at AS TIMESTAMP WITH TIME ZONE) >= existing.payment_updated_at
  )
{% endif %}
