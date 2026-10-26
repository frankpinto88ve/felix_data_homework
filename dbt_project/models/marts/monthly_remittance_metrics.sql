{{
    config(
        materialized='view',
        database='felix',
        schema='marts'
    )
}}

WITH scoped_remittances AS (
    {{ current_month_complete_remittances() }}
)

SELECT
    DATE_TRUNC('MONTH', receipt_created_at) AS reporting_month,
    destination_country_code,
    destination_currency,
    payment_currency,
    COUNT(DISTINCT receipt_id) AS remittance_count,
    COALESCE(SUM(amount_charged), 0) AS total_amount_charged,
    COUNT(DISTINCT payment_id) AS payment_count,
    COUNT(DISTINCT CASE
        WHEN COALESCE(has_dispute_chargeback, FALSE) THEN payment_id
    END) AS chargeback_payment_count,
    COUNT(DISTINCT CASE
        WHEN COALESCE(has_dispute_chargeback, FALSE) THEN payment_id
    END)::DOUBLE / NULLIF(COUNT(DISTINCT payment_id), 0) AS chargeback_rate
FROM scoped_remittances
GROUP BY
    reporting_month,
    destination_country_code,
    destination_currency,
    payment_currency
