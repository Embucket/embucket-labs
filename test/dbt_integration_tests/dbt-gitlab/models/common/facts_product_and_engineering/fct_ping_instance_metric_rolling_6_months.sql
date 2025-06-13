{{ config(
    tags=["product", "mnpi_exception"],
    materialized='incremental',
    unique_key='ping_instance_metric_id',
  on_schema_change='sync_all_columns',
    post_hook=["{{ rolling_window_delete('ping_created_date','month',6) }}", "{{'USE WAREHOUSE ' ~ generate_warehouse_name('XL') ~ ';' if not is_incremental() }}"]
) }}

WITH fct_ping_instance_metric_rolling_6_months AS (

    SELECT
      {{ dbt_utils.star(from=ref('fct_ping_instance_metric_rolling_13_months'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
    FROM {{ ref('fct_ping_instance_metric_rolling_13_months') }}
    WHERE DATE_TRUNC(MONTH, fct_ping_instance_metric_rolling_13_months.ping_created_date) >= DATEADD(MONTH, -6, DATE_TRUNC(MONTH,CURRENT_DATE))
    {% if is_incremental() %}
      AND uploaded_at >= (SELECT MAX(uploaded_at) FROM {{ this }})
    {% endif %}

)

SELECT *
FROM fct_ping_instance_metric_rolling_6_months