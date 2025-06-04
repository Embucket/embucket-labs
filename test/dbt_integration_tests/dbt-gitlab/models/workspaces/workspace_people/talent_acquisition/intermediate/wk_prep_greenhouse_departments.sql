      
{{ config(
    materialized='ephemeral'
) }}

WITH source AS (
  SELECT
    *
  FROM {{ ref('greenhouse_departments_source') }} 
),
  greenhouse_departments (department_name, department_id, hierarchy_id, hierarchy_name) AS (
    SELECT
      department_name,
      department_id,
      TO_ARRAY(department_id)   AS hierarchy_id,
      TO_ARRAY(department_name) AS hierarchy_name
    FROM source
    WHERE parent_id IS NULL
    UNION ALL
    SELECT
      iteration.department_name,
      iteration.department_id,
      ARRAY_APPEND(anchor.hierarchy_id, iteration.department_id)     AS hierarchy_id,
      ARRAY_APPEND(anchor.hierarchy_name, iteration.department_name) AS hierarchy_name
    FROM source iteration
    INNER JOIN greenhouse_departments anchor
      ON iteration.parent_id = anchor.department_id
  )
SELECT
  department_name,
  department_id,
  ARRAY_SIZE(hierarchy_id)   AS hierarchy_level,
  hierarchy_id,
  hierarchy_name,
  hierarchy_name[0]::VARCHAR AS level_1,
  hierarchy_name[1]::VARCHAR AS level_2,
  hierarchy_name[2]::VARCHAR AS level_3
FROM greenhouse_departments