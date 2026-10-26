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
    beneficiary_id,
    LIST(DISTINCT sender_user_id ORDER BY sender_user_id) AS sending_user_ids,
    COUNT(DISTINCT sender_user_id) AS sending_user_count
FROM scoped_remittances
WHERE beneficiary_id IS NOT NULL
GROUP BY
    reporting_month,
    beneficiary_id
HAVING COUNT(DISTINCT sender_user_id) >= 2
