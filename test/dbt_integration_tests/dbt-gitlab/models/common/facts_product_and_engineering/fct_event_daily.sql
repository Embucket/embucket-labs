{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_event_valid', 'fct_event_valid'),
    ('dim_plan','dim_plan')
    ])
}},

/*
Aggregate events by date, user, ultimate parent namespace, and event
Limit to 24 months of history for performance reasons
*/

fct_event_daily_grouped AS (

  SELECT
    --Primary Key
    {{
      dbt_utils.generate_surrogate_key([
        'event_date',
        'dim_user_id',
        'dim_ultimate_parent_namespace_id',
        'event_name'
      ])
    }} AS event_user_daily_pk,

    --Foreign Keys
    dim_latest_product_tier_id,
    dim_latest_subscription_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_user_sk,
    --dim_user_id is the current foreign key, and is a natural_key, and will be updated to user_id in a future MR.
    dim_user_id,
    dim_ultimate_parent_namespace_id,
    dim_event_date_id,

    --Degenerate Dimensions (No stand-alone, promoted dimension table)
    event_date,
    event_name,
    days_since_user_creation_at_event_date,
    days_since_namespace_creation_at_event_date,
    stage_name,
    section_name,
    group_name,
    is_smau,
    is_gmau,
    is_umau,
    data_source,
    is_null_user,

    --Facts
    /* We are seeing multiple plans per day at the user level,
    so we will take the MAX plan_id for the day
    (issue: https://gitlab.com/gitlab-data/analytics/-/issues/21808) */
    MAX(plan_id_at_event_date) AS plan_id_at_event_date,
    COUNT(*) AS event_count,
    MAX(event_created_at) AS latest_event_date_timestamp

  FROM fct_event_valid
  WHERE
    event_date >= DATEADD('month', -24, DATE_TRUNC('month', CURRENT_DATE))
  {{ dbt_utils.group_by(n=21) }}

),

fct_event_daily AS (

  SELECT
    fct_event_daily_grouped.*,
    dim_plan.plan_name    AS plan_name_at_event_date,
    dim_plan.is_plan_paid AS plan_was_paid_at_event_date
  FROM fct_event_daily_grouped
  LEFT JOIN dim_plan
    ON fct_event_daily_grouped.plan_id_at_event_date = dim_plan.dim_plan_id

)

SELECT *
FROM fct_event_daily
