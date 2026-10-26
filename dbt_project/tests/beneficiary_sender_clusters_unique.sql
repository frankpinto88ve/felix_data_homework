SELECT
    reporting_month,
    beneficiary_id
FROM {{ ref('beneficiary_sender_clusters') }}
GROUP BY
    reporting_month,
    beneficiary_id
HAVING COUNT(*) > 1
