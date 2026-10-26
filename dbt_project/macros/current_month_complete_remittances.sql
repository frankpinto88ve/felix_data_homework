{% macro current_month_complete_remittances() %}
SELECT *
FROM {{ ref('int_receipts_enriched') }}
WHERE is_complete_remittance
  AND receipt_created_at >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)
  AND receipt_created_at < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP) + INTERVAL 1 MONTH
{% endmacro %}
