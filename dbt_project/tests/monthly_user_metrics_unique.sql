SELECT
    reporting_month,
    sender_user_id,
    payment_currency
FROM {{ ref('monthly_user_metrics') }}
GROUP BY
    reporting_month,
    sender_user_id,
    payment_currency
HAVING COUNT(*) > 1
