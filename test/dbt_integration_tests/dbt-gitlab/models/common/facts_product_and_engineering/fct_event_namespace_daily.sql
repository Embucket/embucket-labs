{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('prep_gitlab_dotcom_plan', 'prep_gitlab_dotcom_plan')
    ])
}},

/*
Aggregate events by date, event, and ultimate parent namespace
Limit to 24 months of history for performance reasons
*/

fct_event_namespace_daily_grouped AS (
  SELECT
    --Primary Key
    {{ dbt_utils.generate_surrogate_key(['event_date', 'event_name', 'dim_ultimate_parent_namespace_id']) }}
      AS event_namespace_daily_pk,

      --Foreign Keys
    dim_latest_product_tier_id,
    dim_latest_subscription_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_ultimate_parent_namespace_id,
    dim_event_date_id,

    --Degenerate Dimensions (No stand-alone, promoted dimension table)
    days_since_namespace_creation_at_event_date,
    event_date,
    event_name,
    stage_name,
    section_name,
    group_name,
    data_source,
    is_smau,
    is_gmau,
    is_umau,

    --Facts

    /* We are seeing multiple plans per day at the user level,
    so we will take the MAX plan_id for the day (issue:
    https://gitlab.com/gitlab-data/analytics/-/issues/21808) */
    MAX(plan_id_at_event_date) AS plan_id_at_event_date,
    SUM(event_count) AS event_count,
    COUNT(dim_user_id) AS user_count
  FROM {{ ref( 'fct_event_daily') }}
  WHERE dim_ultimate_parent_namespace_id IS NOT NULL
  {{ dbt_utils.group_by(n=17) }}

),

fct_event_namespace_daily AS (
  SELECT
    fct_event_namespace_daily_grouped.*,
    prep_gitlab_dotcom_plan.plan_name    AS plan_name_at_event_date,
    prep_gitlab_dotcom_plan.plan_is_paid AS plan_was_paid_at_event_date
  FROM fct_event_namespace_daily_grouped
  LEFT JOIN prep_gitlab_dotcom_plan
    ON fct_event_namespace_daily_grouped.plan_id_at_event_date = prep_gitlab_dotcom_plan.dim_plan_id

)

{{ dbt_audit(
    cte_ref="fct_event_namespace_daily",
    created_by="@iweeks",
    updated_by="@michellecooper",
    created_date="2022-04-09",
    updated_date="2024-10-18"
) }}
