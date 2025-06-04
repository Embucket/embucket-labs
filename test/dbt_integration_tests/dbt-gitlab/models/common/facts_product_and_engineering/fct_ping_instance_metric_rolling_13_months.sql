{{ config(
    tags=["product", "mnpi_exception"],
    materialized='incremental',
    unique_key='ping_instance_metric_id',
    on_schema_change='sync_all_columns',
    pre_hook = ["{{'USE WAREHOUSE ' ~ generate_warehouse_name('4XL') ~ ';' if not is_incremental() }}"],
    post_hook=["{{ rolling_window_delete('ping_created_date','month',13) }}", "{{'USE WAREHOUSE ' ~ generate_warehouse_name('XL') ~ ';' if not is_incremental() }}"]
) }}

{{ simple_cte([
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}},

fct_ping_instance_metric AS (

  SELECT
    {{
      dbt_utils.star(from=ref('fct_ping_instance_metric'),
      except=[
        'CREATED_BY',
        'UPDATED_BY',
        'MODEL_CREATED_DATE',
        'MODEL_UPDATED_DATE',
        'DBT_CREATED_AT',
        'DBT_UPDATED_AT'
        ])
    }}
  FROM {{ ref('fct_ping_instance_metric') }}
  WHERE DATE_TRUNC(MONTH, ping_created_date) >= DATEADD(MONTH, -13, DATE_TRUNC(MONTH, CURRENT_DATE))
    {% if is_incremental() %}
      AND uploaded_at >= (SELECT MAX(uploaded_at) FROM {{ this }})
    {% endif %}

),

final AS (

  SELECT
    fct_ping_instance_metric.*,
    dim_ping_metric.time_frame
  FROM fct_ping_instance_metric
  LEFT JOIN dim_ping_metric
    ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@rbacovic",
    created_date="2022-07-20",
    updated_date="2023-06-05"
) }}
