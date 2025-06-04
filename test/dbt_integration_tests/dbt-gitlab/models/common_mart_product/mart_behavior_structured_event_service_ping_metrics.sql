{{
  config(
    materialized='incremental',
    unique_key = "snowplow_events_service_ping_metrics_sk",
    on_schema_change = "sync_all_columns",
    tags=["product", "mnpi_exception"]
  )
}}

WITH service_ping_events AS (
  SELECT
    *
  FROM {{ ref('fct_behavior_structured_event_service_ping') }}

   {% if is_incremental() %}

    WHERE behavior_at > (SELECT MAX(behavior_at) FROM {{this}})

  {% endif %}
),

metric_bridge AS (
  SELECT DISTINCT
    metrics_path,
    metrics_status,
    time_frame,
    data_source,
    redis_event
  FROM {{ ref('bdg_metrics_redis_events') }}
),

joined AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['behavior_structured_event_pk', 'metrics_path']) }} AS snowplow_events_service_ping_metrics_sk,
    service_ping_events.behavior_structured_event_pk,
    service_ping_events.behavior_at,
    service_ping_events.gsc_pseudonymized_user_id,
    service_ping_events.dim_namespace_id,
    service_ping_events.dim_project_id,
    service_ping_events.gsc_plan,
    service_ping_events.ultimate_parent_namespace_id,
    service_ping_events.redis_event_name,
    metric_bridge.metrics_path,
    metric_bridge.metrics_status,
    metric_bridge.time_frame,
    metric_bridge.data_source
  FROM service_ping_events
  /*
    Inner join on redis event name only allows events to come through that have
    a many-to-many relationship with Service Ping metrics. For events that have
    a one-to-one relationship with Service Ping metric, we can simply look at the
    key_path on the Snowplow event.
  */
  INNER JOIN metric_bridge ON service_ping_events.redis_event_name = metric_bridge.redis_event
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-12-21",
    updated_date="2024-09-18"
) }}
