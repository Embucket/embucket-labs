{{ config(
        materialized = "incremental",
        unique_key = "behavior_structured_event_pk",
        on_schema_change='sync_all_columns',
        cluster_by=['experiment_name'],
        tags=['product'],
        snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{
    simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event')

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
    WHERE fct_behavior_structured_event.has_gitlab_experiment_context = TRUE

    {% if is_incremental() %}

    AND behavior_at > (SELECT MAX(behavior_at) FROM {{this}})

    {% endif %}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-01",
    updated_date="2024-02-07"
) }}
