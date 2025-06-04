WITH workday_hcm_organization_source AS (

  SELECT *
  FROM {{ ref('workday_hcm_organization_source') }}
),

workday_hcm_organization_hierarchy_detail_source AS (

  SELECT *
  FROM {{ ref('workday_hcm_organization_hierarchy_detail_source') }}
),

lvl1 AS (
  SELECT *
  FROM workday_hcm_organization_source
  WHERE type = 'Cost_Center_Hierarchy'
    AND sub_type = 'Top Level'
),

lvl2 AS (
  SELECT *
  FROM workday_hcm_organization_source
  WHERE type = 'Cost_Center_Hierarchy'
    AND sub_type = 'Cost_Center'
),

lvl3 AS (
  SELECT *
  FROM workday_hcm_organization_source
  WHERE type = 'Cost_Center_Hierarchy'
    AND sub_type = 'Division'
),

lvl4_base AS (
  SELECT *
  FROM workday_hcm_organization_source
  WHERE type = 'Cost_Center'
    AND sub_type = 'Department'
),

lvl4_link AS (
  SELECT *
  FROM workday_hcm_organization_hierarchy_detail_source
),

lvl4 AS (
  SELECT
    lvl4_base.*,
    lvl4_link.linked_organization_id
  FROM lvl4_base
  LEFT JOIN lvl4_link ON lvl4_base.id = lvl4_link.organization_id
),

final AS (
  SELECT
    lvl1.id                AS top_level_reference_id,
    lvl1.organization_code AS top_level_code,
    lvl1.name              AS top_level,
    lvl2.id                AS cost_center_reference_id,
    lvl2.organization_code AS cost_center_code,
    lvl2.name              AS cost_center,
    lvl3.id                AS division_reference_id,
    lvl3.organization_code AS division_code,
    lvl3.name              AS division,
    lvl4.id                AS department_reference_id,
    lvl4.organization_code AS department_code,
    lvl4.name              AS department_name,
    lvl4.is_active         AS is_department_active,
    lvl4.inactive_date     AS department_inactive_date
  FROM lvl1
  LEFT JOIN lvl2 ON lvl1.id = lvl2.superior_organization_id
  LEFT JOIN lvl3 ON lvl2.id = lvl3.superior_organization_id
  LEFT JOIN lvl4 ON lvl3.id = lvl4.linked_organization_id
)

SELECT *
FROM final
