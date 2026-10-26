{{
    config(
        materialized='view',
        database='felix',
        schema='marts'
    )
}}

WITH scoped_remittances AS (
    {{ current_month_complete_remittances() }}
),

provider_metrics AS (
    SELECT
        DATE_TRUNC('MONTH', receipt_created_at) AS reporting_month,
        provider_name,
        COUNT(DISTINCT disbursement_id) AS total_disbursement_count,
        COUNT(DISTINCT CASE
            WHEN UPPER(TRIM(COALESCE(disbursement_status, ''))) = 'FAILED'
                THEN disbursement_id
        END) AS failed_disbursement_count
    FROM scoped_remittances
    WHERE provider_name IS NOT NULL
    GROUP BY
        reporting_month,
        provider_name
)

SELECT
    reporting_month,
    provider_name,
    total_disbursement_count,
    failed_disbursement_count,
    failed_disbursement_count::DOUBLE / NULLIF(total_disbursement_count, 0)
        AS failed_disbursement_rate,
    CASE
        WHEN failed_disbursement_count = 0 THEN 'no_failures'
        WHEN failed_disbursement_count = total_disbursement_count THEN 'all_failed'
        ELSE 'has_failures'
    END AS provider_classification
FROM provider_metrics
