{{ config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    on_schema_change='sync_all_columns',
    tags=['product'],
    post_hook=["{{ rolling_window_delete('behavior_at','day',190) }}"]
) }}

WITH source_190 AS (

  SELECT
    {{
      dbt_utils.star(from=ref('fct_behavior_structured_event_without_assignment'),
      except=[
        'CREATED_BY',
        'UPDATED_BY',
        'MODEL_CREATED_DATE',
        'MODEL_UPDATED_DATE',
        'DBT_CREATED_AT',
        'DBT_UPDATED_AT'
        ])
    }}
  FROM {{ ref('fct_behavior_structured_event_without_assignment') }}
  WHERE DATE_TRUNC(MONTH, behavior_at) >= DATEADD(DAY, -190, DATE_TRUNC(DAY, CURRENT_DATE))
    {% if is_incremental() %}
      AND behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})
    {% endif %}

)

{{ dbt_audit(
    cte_ref="source_190",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-11-01",
    updated_date="2022-11-01"
) }}
