{{
  config(
    materialized='table',
    tags=["product", "mnpi_exception"]
  )
}}

WITH events AS (
  SELECT
    *
  FROM {{ ref('mart_behavior_structured_event_service_ping_metrics') }}
  -- only include redis_hll metrics with 28d time frame, which limits to user-based metrics
  -- more details here: https://gitlab.com/gitlab-org/gitlab/-/issues/411607#note_1392956155
  WHERE data_source IN (
    'redis_hll',
    'internal_events'
  )
    AND time_frame = '28d'
),

dates AS (
  SELECT
    *
  FROM {{ ref('dim_date') }}
),

final AS (
  SELECT
    DATE_TRUNC('month', events.behavior_at) AS date_month,
    events.ultimate_parent_namespace_id,
    events.metrics_path,
    COUNT(DISTINCT events.gsc_pseudonymized_user_id) AS distinct_users_whole_month,
    COUNT(DISTINCT IFF(dates.days_until_last_day_of_month <= 27, events.gsc_pseudonymized_user_id, NULL)) AS distinct_users_last_28d_month
  FROM events
  LEFT JOIN dates ON dates.date_actual = DATE_TRUNC('day', events.behavior_at)
  {{ dbt_utils.group_by(n = 3) }}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@utkarsh060",
    created_date="2022-12-21",
    updated_date="2024-03-13"
) }}
