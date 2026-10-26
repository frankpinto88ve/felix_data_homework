SELECT *
FROM {{ ref('monthly_user_metrics') }}
WHERE (is_high_spender
        AND cumulative_amount - monthly_amount_transacted >= monthly_total_amount * 0.50)
   OR (NOT is_high_spender
        AND cumulative_amount - monthly_amount_transacted < monthly_total_amount * 0.50)
   OR (is_recurring_customer AND sender_month_remittance_count < 2)
