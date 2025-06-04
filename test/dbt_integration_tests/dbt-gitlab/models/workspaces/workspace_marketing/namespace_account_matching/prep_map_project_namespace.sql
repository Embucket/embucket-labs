{{ config(
    materialized='ephemeral'
) }}

{{ simple_cte([
    ('project_history','gitlab_dotcom_projects_snapshots_base'),
    ('project_current','gitlab_dotcom_projects_source')
]) }},

-- In the event a project is deleted we still want to know the historic map to namespace
list AS (
  SELECT DISTINCT
    project_id,
    project_namespace_id
  FROM project_history
  WHERE project_namespace_id IS NOT NULL

  UNION 

  SELECT DISTINCT
    project_id,
    project_namespace_id
  FROM project_current
  WHERE project_namespace_id IS NOT NULL

)

SELECT *
FROM list