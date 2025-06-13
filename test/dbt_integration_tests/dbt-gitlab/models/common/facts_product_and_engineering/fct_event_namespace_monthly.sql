{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_date', 'dim_date')
    ])
}},

fact_with_month AS (

  SELECT
    fct_event_daily.*,
    dim_date.first_day_of_month AS event_calendar_month
  FROM {{ ref('fct_event_daily') }} AS fct_event_daily
  LEFT JOIN dim_date
    ON fct_event_daily.dim_event_date_id = dim_date.date_id

),

latest_event_timestamp AS (
  SELECT
    dim_ultimate_parent_namespace_id,
    event_calendar_month,
    MAX(latest_event_date_timestamp) AS latest_event_date_timestamp_month
  FROM fact_with_month
  GROUP BY dim_ultimate_parent_namespace_id, event_calendar_month
),

plan_by_month AS (
  SELECT
  -- join keys
    fact_with_month.dim_ultimate_parent_namespace_id,
    fact_with_month.event_calendar_month,

    -- plan info
    fact_with_month.plan_id_at_event_date,
    fact_with_month.plan_name_at_event_date,
    fact_with_month.plan_was_paid_at_event_date

  FROM fact_with_month
  INNER JOIN latest_event_timestamp
    ON fact_with_month.dim_ultimate_parent_namespace_id = latest_event_timestamp.dim_ultimate_parent_namespace_id
      AND fact_with_month.latest_event_date_timestamp = latest_event_timestamp.latest_event_date_timestamp_month

  -- for safety only
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY fact_with_month.dim_ultimate_parent_namespace_id, fact_with_month.event_calendar_month
    ORDER BY fact_with_month.plan_id_at_event_date DESC
  ) = 1
),

/*
Aggregate namespace event data by month
Exclude the current month because the data is incomplete (and the plan could change)
*/

fct_event_namespace_monthly AS (

  SELECT
    --Primary Key
    {{
      dbt_utils.generate_surrogate_key([
        'fact_with_month.event_calendar_month',
        'fact_with_month.event_name',
        'fact_with_month.dim_ultimate_parent_namespace_id'
      ])
    }}
      AS event_namespace_monthly_pk,

      --Foreign Keys
    fact_with_month.dim_ultimate_parent_namespace_id,
    fact_with_month.dim_latest_product_tier_id,
    fact_with_month.dim_latest_subscription_id,
    fact_with_month.dim_crm_account_id,
    fact_with_month.dim_billing_account_id,

    --Degenerate Dimensions (No stand-alone, promoted dimension table)
    plan_by_month.plan_id_at_event_date AS plan_id_at_event_month,
    plan_by_month.plan_name_at_event_date AS plan_name_at_event_month,
    plan_by_month.plan_was_paid_at_event_date AS plan_was_paid_at_event_month,
    fact_with_month.event_calendar_month,
    fact_with_month.event_name,
    fact_with_month.section_name,
    fact_with_month.stage_name,
    fact_with_month.group_name,
    fact_with_month.is_smau,
    fact_with_month.is_gmau,
    fact_with_month.is_umau,
    fact_with_month.data_source,

    --Facts
    SUM(fact_with_month.event_count) AS event_count,
    COUNT(DISTINCT fact_with_month.dim_user_id) AS user_count,
    COUNT(DISTINCT fact_with_month.event_date) AS event_date_count

  FROM fact_with_month
  LEFT JOIN plan_by_month
    ON fact_with_month.dim_ultimate_parent_namespace_id = plan_by_month.dim_ultimate_parent_namespace_id
      AND fact_with_month.event_calendar_month = plan_by_month.event_calendar_month
  WHERE fact_with_month.dim_ultimate_parent_namespace_id IS NOT NULL
  {{ dbt_utils.group_by(n=18) }}

)

{{ dbt_audit(
    cte_ref="fct_event_namespace_monthly",
    created_by="@cbraza",
    updated_by="@michellecooper",
    created_date="2023-02-14",
    updated_date="2023-05-12"
) }}
