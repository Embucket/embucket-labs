{{ config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    on_schema_change='sync_all_columns',
    tags=['product'],
    full_refresh= only_force_full_refresh(),
    post_hook=["{{ rolling_window_delete('behavior_at','day',400) }}"]
) }}

WITH source_400 AS (

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
  WHERE DATE_TRUNC(MONTH, behavior_at) >= DATEADD(DAY, -400, DATE_TRUNC(DAY, CURRENT_DATE))

    {% if is_incremental() %}
       AND behavior_at > (SELECT MAX({{ var('incremental_backfill_date', 'behavior_at') }}) FROM {{ this }})
       AND behavior_at <= (SELECT DATEADD(MONTH, 1, MAX({{ var('incremental_backfill_date', 'behavior_at') }})) FROM {{ this }})

    {% else %}
      -- This will cover the first creation of the table or a full refresh and requires that the table be backfilled
       AND behavior_at > DATEADD('day', -30 ,CURRENT_DATE())

    {% endif %}

)

{{ dbt_audit(
    cte_ref="source_400",
    created_by="@chrissharp",
    updated_by="@michellecooper",
    created_date="2022-11-01",
    updated_date="2024-02-16"
) }}
