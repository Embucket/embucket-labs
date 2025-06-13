{{ config(
    materialized='table'
) }}

{{ simple_cte([
    ('group_links','gitlab_dotcom_group_group_links_source'),
    ('project_links','gitlab_dotcom_project_group_links_source'),
    ('projects_map','prep_map_project_namespace'),
    ('access_levels','gitlab_dotcom_access_levels_source')
]) }},

map_to_namespace AS (

  SELECT 
    project_links.project_group_link_id AS share_id,
    projects_map.project_namespace_id AS shared_namespace_id,
    project_links.group_id AS received_namespace_id,
    project_links.group_access,
    project_links.created_at,
    project_links.updated_at,
    project_links.expires_at,
    project_links.valid_from,
    project_links.valid_to,
    project_links.is_currently_valid,
    'Project' AS share_source
  FROM project_links
  LEFT JOIN projects_map
    ON project_links.project_id = projects_map.project_id

),

combine AS (
  SELECT 
    group_links.group_group_link_id AS share_id,
    group_links.shared_group_id AS shared_namespace_id,
    group_links.shared_with_group_id AS received_namespace_id,
    group_links.group_access,
    group_links.created_at,
    group_links.updated_at,
    group_links.expires_at,
    group_links.valid_from,
    group_links.valid_to,
    group_links.is_currently_valid,
    'Group' AS share_source
  FROM group_links

  UNION ALL

  SELECT *
  FROM map_to_namespace
),

fct AS (

  SELECT 
    -- Primary Key
    {{ dbt_utils.generate_surrogate_key(['combine.share_id','combine.share_source','valid_from']) }} AS group_share_pk,

    -- Foreign Keys
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['combine.shared_namespace_id'])) }} AS dim_shared_namespace_sk,
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['combine.received_namespace_id'])) }} AS dim_received_namespace_sk,

    -- Legacy Keys
    combine.share_id,
    combine.shared_namespace_id,
    combine.received_namespace_id,

    -- Degenerate Dimensions
    combine.share_source,
    combine.group_access AS access_level,
    access_levels.access_level_name,
    combine.created_at,
    combine.updated_at,
    combine.expires_at,
    combine.valid_from,
    combine.valid_to,
    combine.is_currently_valid

    -- Facts

  FROM combine
  LEFT JOIN access_levels
    ON combine.group_access = access_levels.access_level_id

)

SELECT *
FROM fct
