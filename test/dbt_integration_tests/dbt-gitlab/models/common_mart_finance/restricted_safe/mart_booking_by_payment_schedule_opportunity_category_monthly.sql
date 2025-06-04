{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('fct_crm_opportunity', 'fct_crm_opportunity'),
    ('dim_crm_opportunity', 'dim_crm_opportunity'),
    ('dim_date', 'dim_date')
]) }},

standard_booking AS (

/* Prepaid sales_assisted non-ramp booking */

  SELECT
    DATE(DATE_TRUNC('month', fct_crm_opportunity.close_date)) AS period,
    COUNT(fct_crm_opportunity.amount)                         AS standard_booking_count
  FROM fct_crm_opportunity
  LEFT JOIN dim_crm_opportunity 
    ON fct_crm_opportunity.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
  WHERE dim_crm_opportunity.opportunity_category = 'Standard'
    AND (dim_crm_opportunity.payment_schedule = 'Prepaid' OR dim_crm_opportunity.payment_schedule = 'AWS Prepay' OR dim_crm_opportunity.payment_schedule = 'GCP Prepay')
    AND dim_crm_opportunity.is_won = TRUE
    AND dim_crm_opportunity.is_web_portal_purchase = FALSE
  GROUP BY period
  ORDER BY period

),


all_bookings AS (

/* Select all sales-assisted bookings */


  SELECT
    DATE(DATE_TRUNC('month', fct_crm_opportunity.close_date)) AS period,
    COUNT(fct_crm_opportunity.amount)                         AS all_sales_assisted_booking_count
  FROM fct_crm_opportunity
  LEFT JOIN dim_crm_opportunity 
    ON fct_crm_opportunity.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
  WHERE dim_crm_opportunity.is_won = TRUE
    AND dim_crm_opportunity.is_web_portal_purchase = FALSE
  GROUP BY period
  ORDER BY period

),

final AS (

/* Compare standard vs. all bookings to determine percentage of non-standard booking */

  SELECT
    --Primary key
    all_bookings.period,

    --Dates
    dim_date.fiscal_year                                                                                        AS fiscal_year,
    dim_date.fiscal_quarter_name_fy                                                                             AS fiscal_quarter,

    --Aggregates
    standard_booking.standard_booking_count,
    all_bookings.all_sales_assisted_booking_count,
    ROUND(((standard_booking.standard_booking_count / all_bookings.all_sales_assisted_booking_count) * 100), 2) AS percentage_standard,
    100 - percentage_standard                                                                                   AS percentage_non_standard
  FROM standard_booking
  LEFT JOIN all_bookings 
    ON standard_booking.period = all_bookings.period
  LEFT JOIN dim_date 
    ON standard_booking.period = dim_date.date_actual
  ORDER BY all_bookings.period

)


{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-08",
updated_date="2024-05-08"
) }}
