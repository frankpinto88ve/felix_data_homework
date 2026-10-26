SELECT
    reporting_month,
    destination_country_code,
    destination_currency,
    payment_currency
FROM {{ ref('monthly_remittance_metrics') }}
GROUP BY
    reporting_month,
    destination_country_code,
    destination_currency,
    payment_currency
HAVING COUNT(*) > 1
