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

user_currency_metrics AS (
    SELECT
        DATE_TRUNC('MONTH', receipt_created_at) AS reporting_month,
        sender_user_id,
        payment_currency,
        COUNT(DISTINCT receipt_id) AS monthly_remittance_count,
        COALESCE(SUM(amount_charged), 0) AS monthly_amount_transacted
    FROM scoped_remittances
    GROUP BY
        reporting_month,
        sender_user_id,
        payment_currency
),

sender_month_metrics AS (
    SELECT
        reporting_month,
        sender_user_id,
        SUM(monthly_remittance_count) AS sender_month_remittance_count
    FROM user_currency_metrics
    GROUP BY
        reporting_month,
        sender_user_id
),

ranked_users AS (
    SELECT
        user_currency_metrics.*,
        sender_month_metrics.sender_month_remittance_count,
        SUM(monthly_amount_transacted) OVER (
            PARTITION BY reporting_month, payment_currency
        ) AS monthly_total_amount,
        SUM(monthly_amount_transacted) OVER (
            PARTITION BY reporting_month, payment_currency
            ORDER BY monthly_amount_transacted DESC, sender_user_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_amount
    FROM user_currency_metrics
    INNER JOIN sender_month_metrics
        USING (reporting_month, sender_user_id)
)

SELECT
    reporting_month,
    sender_user_id,
    payment_currency,
    monthly_remittance_count,
    monthly_amount_transacted,
    sender_month_remittance_count,
    sender_month_remittance_count >= 2 AS is_recurring_customer,
    monthly_total_amount,
    cumulative_amount,
    cumulative_amount - monthly_amount_transacted < monthly_total_amount * 0.50
        AS is_high_spender,
    CASE
        WHEN cumulative_amount - monthly_amount_transacted < monthly_total_amount * 0.50
            THEN 'high_spender'
        ELSE 'not_high_spender'
    END AS high_spender_classification
FROM ranked_users
