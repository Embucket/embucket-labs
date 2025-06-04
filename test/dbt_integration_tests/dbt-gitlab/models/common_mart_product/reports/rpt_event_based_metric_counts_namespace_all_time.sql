{{
  config(
    materialized='table',
    tags=["product", "mnpi_exception"]
  )
}}

WITH events_old AS (
  SELECT *
  FROM {{ ref('fct_behavior_structured_event_service_ping') }}
  /*
  Only include events where `key_path` is not null. When `key_path` is not null,
  this indicates that there is a one-to-one relationship between a metric and a Redis
  event. In these cases, the Service Ping schema is not designed to send the Redis event name,
  it only sends the metric path. Because of this, we use this file to aggregate those events
  correctly. Additionally, because of the aggregation logic in this file, this model should only be
  used to aggregate event-based all-time metrics.
  */
  WHERE key_path IS NOT NULL
),

events_new AS (
  SELECT *
  FROM {{ ref('mart_behavior_structured_event_service_ping_metrics') }}
  /*
  Only include redis metrics with all-time time frame, which limits to event-based metrics.
  More details here: https://gitlab.com/gitlab-org/gitlab/-/issues/411607#note_1392956155
  */
  WHERE data_source = 'redis'
    AND time_frame = 'all'
),

unioned AS (
  SELECT
    DATE_TRUNC('month', events_old.behavior_at) AS month,
    events_old.ultimate_parent_namespace_id,
    events_old.key_path                             AS metrics_path
  FROM events_old

  UNION ALL

  SELECT
    DATE_TRUNC('month', events_new.behavior_at) AS month,
    events_new.ultimate_parent_namespace_id,
    events_new.metrics_path
  FROM events_new
),

agg AS (
  SELECT
    unioned.month,
    unioned.ultimate_parent_namespace_id,
    unioned.metrics_path,
    COUNT(*)                                AS monthly_value
  FROM unioned
  {{ dbt_utils.group_by(n = 3) }}
),

final AS (
  SELECT
    agg.month,
    agg.ultimate_parent_namespace_id,
    agg.metrics_path,
    agg.monthly_value,
    SUM(agg.monthly_value) OVER(
      PARTITION BY
        agg.ultimate_parent_namespace_id,
        agg.metrics_path
      ORDER BY
        agg.month ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_metric_value
  FROM agg
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@utkarsh060",
    created_date="2023-01-30",
    updated_date="2024-03-13"
) }}
