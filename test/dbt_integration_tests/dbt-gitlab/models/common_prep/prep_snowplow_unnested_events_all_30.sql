{{config(

    materialized='incremental',
    unique_key='event_id',
    tags=['product'],
    full_refresh= only_force_full_refresh(),
    on_schema_change='sync_all_columns',
    post_hook=["{{ rolling_window_delete('behavior_at','day',30) }}"],
    cluster_by=['behavior_at::DATE']
  )

}}

-- depends_on: {{ ref('snowplow_unnested_events') }}

WITH unioned_view AS (

{{ schema_union_limit('snowplow_', 'snowplow_unnested_events', 'derived_tstamp', 30, database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}

),

final AS (

  {{ macro_prep_snowplow_unnested_events_all(unioned_view) }}
  {% if is_incremental() %}
    WHERE behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})
  {% endif %}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@utkarsh060",
    updated_by="@utkarsh060",
    created_date="2024-06-11",
    updated_date="2024-06-11"
) }}
