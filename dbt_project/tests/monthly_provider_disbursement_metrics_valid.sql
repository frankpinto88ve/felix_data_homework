SELECT *
FROM {{ ref('monthly_provider_disbursement_metrics') }}
WHERE failed_disbursement_count > total_disbursement_count
   OR failed_disbursement_rate < 0
   OR failed_disbursement_rate > 1
