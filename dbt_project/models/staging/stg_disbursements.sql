{{
    config(
        materialized='incremental',
        incremental_strategy='append' if var('initial_load', false) else 'merge',
        unique_key='disbursement_id',
        database='lake',
        schema='staging'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'disbursements') }}
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
        disbursement_id,
        disbursement_updated_at
    FROM {{ this }}
){% endif %}

SELECT
    id AS disbursement_id,
    user_id AS disbursement_user_id,
    type AS disbursement_type,
    status AS disbursement_status,
    raw_status AS disbursement_raw_status,
    NULLIF(provider_status, '') AS provider_status,
    TRY_CAST(amount AS DECIMAL(18, 2)) AS disbursement_amount,
    disbursement_provider AS provider_name,
    payer_destination_name AS beneficiary_name,
    destination_country,
    destination_currency,
    TRY_CAST(created_at AS TIMESTAMP WITH TIME ZONE) AS disbursement_created_at,
    TRY_CAST(updated_at AS TIMESTAMP WITH TIME ZONE) AS disbursement_updated_at,
    _ingestion_run_id AS ingestion_run_id,
    _ingested_at AS ingested_at,
    _source_file_name AS source_file_name,
    _source_file_hash AS source_file_hash
FROM deduplicated AS source_data
{% if is_incremental() and not var('initial_load', false) %}
LEFT JOIN existing
    ON source_data.id = existing.disbursement_id
{% endif %}
WHERE source_data._deduplication_rank = 1
{% if is_incremental() and not var('initial_load', false) %}
  AND (
      existing.disbursement_id IS NULL
      OR TRY_CAST(source_data.updated_at AS TIMESTAMP WITH TIME ZONE) >= existing.disbursement_updated_at
  )
{% endif %}
