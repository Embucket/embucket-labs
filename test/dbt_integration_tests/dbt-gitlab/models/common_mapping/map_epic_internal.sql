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

epics AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_epics_source') }}
),

final AS (

  SELECT DISTINCT
    internal_namespace.ultimate_parent_namespace_id,
    epics.group_id AS namespace_id,
    epics.epic_id
  FROM internal_namespace
  INNER JOIN lineage
    ON internal_namespace.ultimate_parent_namespace_id = lineage.ultimate_parent_id
  LEFT JOIN epics
    ON lineage.namespace_id = epics.group_id
  WHERE epics.group_id IS NOT NULL

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@pempey",
    updated_by="@lisvinueza",
    created_date="2023-02-15",
    updated_date="2023-05-05"
) }}