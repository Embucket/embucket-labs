{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('fct_crm_opportunity', 'fct_crm_opportunity'),
    ('dim_crm_opportunity', 'dim_crm_opportunity'),
    ('dim_date', 'dim_date')
]) }},

basis AS (

/* Booking amount and count by subscription type */

  SELECT
    DATE(DATE_TRUNC('month', fct_crm_opportunity.close_date)) AS period,
    dim_crm_opportunity.subscription_type                     AS subscription_type,
    SUM(fct_crm_opportunity.amount)                           AS opportunity_amount,
    COUNT(fct_crm_opportunity.amount)                         AS opportunity_count
  FROM fct_crm_opportunity
  LEFT JOIN dim_crm_opportunity 
    ON fct_crm_opportunity.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
  WHERE dim_crm_opportunity.is_won = TRUE
  {{ dbt_utils.group_by(n=2)}}
  ORDER BY period, dim_crm_opportunity.subscription_type

),

final AS (

/* Adding fiscal year and quarter */

  SELECT
    --Primary key
    basis.period,

    --Dates
    dim_date.fiscal_year            AS fiscal_year,
    dim_date.fiscal_quarter_name_fy AS fiscal_quarter,

    --Additive fields
    basis.subscription_type         AS subscription_type,

    --Amounts
    basis.opportunity_amount        AS opportunity_amount,
    basis.opportunity_count         AS opportunity_count

  FROM basis
  LEFT JOIN dim_date ON basis.period = dim_date.date_actual

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-08",
updated_date="2024-05-08"
) }}
