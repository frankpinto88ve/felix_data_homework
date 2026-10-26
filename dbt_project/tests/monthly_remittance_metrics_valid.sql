SELECT *
FROM {{ ref('monthly_remittance_metrics') }}
WHERE total_amount_charged < 0
   OR chargeback_rate < 0
   OR chargeback_rate > 1
