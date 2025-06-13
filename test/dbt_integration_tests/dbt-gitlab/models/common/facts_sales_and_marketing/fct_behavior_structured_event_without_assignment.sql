{{ config(
        materialized = "incremental",
        unique_key = "behavior_structured_event_pk",
        on_schema_change='sync_all_columns',
        tags=['product'],
        full_refresh= only_force_full_refresh(),
        cluster_by=['behavior_at::DATE'],
        snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{
    simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event'),
    ('dim_behavior_event', 'dim_behavior_event')
    ])
}}

, final AS (

    SELECT

      {{
      dbt_utils.star(from=ref('fct_behavior_structured_event'),
      relation_alias='fct_behavior_structured_event',
      except=[
        'CREATED_BY',
        'UPDATED_BY',
        'MODEL_CREATED_DATE',
        'MODEL_UPDATED_DATE',
        'DBT_CREATED_AT',
        'DBT_UPDATED_AT'
        ])
      }}

    FROM fct_behavior_structured_event
    INNER JOIN dim_behavior_event
      ON fct_behavior_structured_event.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk
    WHERE dim_behavior_event.event_action != 'assignment'

    {% if is_incremental() %}

      AND behavior_at > (SELECT MAX({{ var('incremental_backfill_date', 'behavior_at') }}) FROM {{ this }})
      AND behavior_at <= (SELECT DATEADD(MONTH, 1, MAX({{ var('incremental_backfill_date', 'behavior_at') }})) FROM {{ this }})

    {% else %}
    -- This will cover the first creation of the table or a full refresh and requires that the table be backfilled
      AND behavior_at > DATEADD('day', -30 ,CURRENT_DATE())

    {% endif %}


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-01",
    updated_date="2024-02-16"
) }}
