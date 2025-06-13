{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

WITH source AS (

  SELECT
    {{ dbt_utils.star(from=ref('prep_performance_indicators_yaml'), except=['PERFORMANCE_INDICATOR_PK',
'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
  FROM {{ ref('prep_performance_indicators_yaml') }}

)

SELECT *
FROM source
