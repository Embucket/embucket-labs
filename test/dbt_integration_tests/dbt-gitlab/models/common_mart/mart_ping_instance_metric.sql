{{ config(
    tags=["product", "mnpi_exception"]
) }}

WITH final AS (

    SELECT
      {{ dbt_utils.star(from=ref('mart_ping_instance_metric_7_day'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE',
        'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
    FROM {{ ref('mart_ping_instance_metric_7_day') }}

    UNION ALL

    SELECT
      {{ dbt_utils.star(from=ref('mart_ping_instance_metric_28_day'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE',
        'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
    FROM {{ ref('mart_ping_instance_metric_28_day') }}

    UNION ALL

    SELECT
      {{ dbt_utils.star(from=ref('mart_ping_instance_metric_all_time'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE',
        'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
    FROM {{ ref('mart_ping_instance_metric_all_time') }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-05-03",
    updated_date="2022-07-20"
) }}

