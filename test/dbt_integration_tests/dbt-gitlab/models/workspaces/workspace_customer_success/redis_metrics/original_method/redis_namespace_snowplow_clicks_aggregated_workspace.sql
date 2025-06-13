{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

WITH clicks_filtered AS (
  SELECT
    *
  FROM {{ ref('fct_behavior_structured_event_redis_hll_counters') }}
  WHERE event_action IN (
    'action_active_users_project_repo',
    'p_terraform_state_api_unique_users',
    'i_search_paid'
  )
),

final AS (
  SELECT
    DATE_TRUNC('month', behavior_at) AS date_month,
    ultimate_parent_namespace_id,
    event_action,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS distinct_users
  FROM clicks_filtered
  {{ dbt_utils.group_by(n = 3) }}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@utkarsh060",
    created_date="2022-05-24",
    updated_date="2024-04-04"
) }}