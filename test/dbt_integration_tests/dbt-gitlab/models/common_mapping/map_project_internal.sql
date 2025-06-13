{{ config({
        "materialized": "view",
    })
}}

WITH
internal_namespace AS (
  SELECT *
  FROM {{ ref('map_namespace_internal') }}
),

lineage AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_lineage_scd') }}
  WHERE is_current = TRUE
),

projects AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_projects_source') }}
),

final AS (

  SELECT DISTINCT
    internal_namespace.ultimate_parent_namespace_id,
    projects.namespace_id AS parent_namespace_id,
    projects.project_namespace_id,
    projects.project_id
  FROM internal_namespace
  INNER JOIN lineage
    ON internal_namespace.ultimate_parent_namespace_id = lineage.ultimate_parent_id
  LEFT JOIN projects
    ON lineage.namespace_id = projects.project_namespace_id
  WHERE projects.project_id IS NOT NULL

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@pempey",
    updated_by="@pempey",
    created_date="2023-02-01",
    updated_date="2023-02-01"
) }}
