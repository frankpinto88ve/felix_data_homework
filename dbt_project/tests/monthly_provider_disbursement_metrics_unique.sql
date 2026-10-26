SELECT
    reporting_month,
    provider_name
FROM {{ ref('monthly_provider_disbursement_metrics') }}
GROUP BY
    reporting_month,
    provider_name
HAVING COUNT(*) > 1
