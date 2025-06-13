{{ config(
    materialized='incremental',
    post_hook=["{{ rolling_window_delete('behavior_at','day', 8) }}"],
    on_schema_change = "sync_all_columns",
    tags=["product", "mnpi_exception"],
    full_refresh = only_force_full_refresh()
) }}

WITH base AS (

  SELECT ultimate_parent_namespace_id,
         gsc_pseudonymized_user_id,
         behavior_at,
         metrics_path,
         redis_event_name,
  FROM {{ ref('mart_behavior_structured_event_service_ping_metrics') }}
  WHERE time_frame = '7d'
  AND metrics_status = 'active'
  AND data_source IN ('internal_events', 'redis', 'redis_hll')
  AND behavior_at >= DATEADD(DAY, -8, current_date)

  {% if is_incremental() %}

    AND behavior_at > (SELECT MAX(behavior_at) FROM {{this}})

  {% endif %}

)

{{ dbt_audit(
    cte_ref="base",
    created_by="@rbacovic",
    updated_by="@rbacovic",
    created_date="2024-10-14",
    updated_date="2024-10-14"
) }}
