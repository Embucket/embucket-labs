{{ config(
    materialized = "incremental",
    unique_key = "dim_behavior_browser_sk",
    tags=['product']
    )
}}
WITH browser_information AS (

  SELECT DISTINCT
    -- surrogate key
    dim_behavior_browser_sk,

    -- natural key
    browser_name,
    browser_major_version,
    browser_minor_version,

    -- attributes
    browser_engine,
    browser_language,
    MAX(behavior_at)            AS max_timestamp
  FROM {{ ref('prep_snowplow_unnested_events_all') }}
  WHERE true

  {% if is_incremental() %}

  AND behavior_at > (SELECT MAX(max_timestamp) FROM {{this}})

  {% endif %}

  {{ dbt_utils.group_by(n=6) }}


)

{{ dbt_audit(
    cte_ref="browser_information",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2022-09-20",
    updated_date="2022-11-30"
) }}
