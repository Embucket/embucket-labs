{{ simple_cte([
    ('dim_date','dim_date'),
    ('prep_charge', 'prep_charge')
]) }}

, renewal_fiscal_years AS (

    SELECT DISTINCT dim_date.fiscal_year
    FROM prep_charge
    INNER JOIN dim_date
      ON prep_charge.effective_start_month <= dim_date.date_actual
      AND (prep_charge.effective_end_month > dim_date.date_actual
        OR prep_charge.effective_end_month IS NULL)
      AND dim_date.day_of_month = 1
    WHERE subscription_status IN ('Active', 'Cancelled')
      AND charge_type = 'Recurring'
      AND mrr != 0

)

SELECT *
FROM renewal_fiscal_years
