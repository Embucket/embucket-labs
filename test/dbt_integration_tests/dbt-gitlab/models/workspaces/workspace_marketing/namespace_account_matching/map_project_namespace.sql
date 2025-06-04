{{ config(
    materialized='table'
) }}

{{ simple_cte([
    ('projects','prep_map_project_namespace')
]) }},

project_namespace_map AS (

  SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['project_id']) }} AS dim_project_sk,
    {{ dbt_utils.generate_surrogate_key(['project_namespace_id']) }} AS dim_namespace_sk,
    project_id,
    project_namespace_id AS namespace_id
  FROM projects

)

SELECT *
FROM project_namespace_map
