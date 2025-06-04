{{ config(
        materialized = "incremental",
        unique_key = "dim_behavior_operating_system_sk",
        tags=['product'],
        snowflake_warehouse=generate_warehouse_name('XL')
    )
}}

WITH device_information AS (

  SELECT
    dim_behavior_operating_system_sk,
    os,
    os_name,
    os_manufacturer,
    os_timezone,
    device_type,
    is_device_mobile,
    MAX(behavior_at)                                         AS max_timestamp
  FROM {{ ref('prep_snowplow_unnested_events_all') }}
  WHERE true

  {% if is_incremental() %}

  AND behavior_at > (SELECT MAX(max_timestamp) FROM {{this}})

  {% endif %}

  {{ dbt_utils.group_by(n=7) }}

)

{{ dbt_audit(
    cte_ref="device_information",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2022-09-20",
    updated_date="2022-12-01"
) }}
