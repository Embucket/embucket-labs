{{ config(
    materialized='table'
) }}

{{ simple_cte([
    ('members','gitlab_dotcom_members_source'),
    ('projects_map','prep_map_project_namespace'),
    ('access_levels','gitlab_dotcom_access_levels_source'),
    ('namespace','gitlab_dotcom_namespaces_snapshots_base')
]) }},

remap_projects AS (

  SELECT
    members.member_id,
    members.user_id,
    members.access_level,
    members.member_type,
    members.created_at,
    members.invite_accepted_at,
    members.requested_at,
    members.expires_at,
    members.invite_token,
    members.valid_from,
    members.valid_to,
    members.is_currently_valid,
    -- A project could be deleted
    COALESCE(
      projects_map.project_namespace_id,
      members.source_id, -1) AS member_namespace_id
  FROM members
  LEFT JOIN projects_map
    ON members.source_id = projects_map.project_id
      AND members.member_source_type = 'Project'

),

user_namespaces AS (
  -- Owner membership of user namespace is only implied and not explicit in the members table.
  SELECT
    (owner_id || namespace_id)::INT AS member_id,
    owner_id AS user_id,
    50 AS access_level,
    'UserMember' AS member_type,
    namespace_created_at AS created_at,
    NULL AS invite_accepted_at,
    NULL AS requested_at,
    NULL AS expires_at,
    NULL AS invite_token,
    valid_from,
    valid_to,
    IFF(valid_to IS NULL, TRUE, FALSE) AS is_currently_valid,
    namespace_id
  FROM namespace
  WHERE namespace_type = 'User'

),

combine AS (
  SELECT *
  FROM remap_projects

  UNION ALL

  SELECT *
  FROM user_namespaces
),

fct AS (

  SELECT
    -- Primary Key
    {{ dbt_utils.generate_surrogate_key(['combine.member_id','combine.valid_from']) }} AS direct_member_pk,

    -- Foreign Keys
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['combine.user_id'])) }} AS dim_user_sk,
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['combine.member_namespace_id'])) }} AS dim_namespace_sk,

    -- Legacy Keys
    combine.member_id,
    combine.user_id,
    combine.member_namespace_id AS namespace_id,

    -- Degenerate Dimensions
    combine.access_level,
    access_levels.access_level_name,
    combine.member_type,
    combine.created_at AS membership_created_at,
    combine.invite_accepted_at,
    combine.requested_at,
    combine.expires_at,
    IFF(combine.invite_token IS NOT NULL, TRUE, FALSE) AS is_invite,
    IFF(combine.invite_accepted_at IS NOT NULL, TRUE, FALSE) AS is_past_invite,
    IFF(combine.requested_at IS NOT NULL, TRUE, FALSE) AS is_request,
    combine.valid_from,
    combine.valid_to,
    combine.is_currently_valid

    -- Facts

  FROM combine
  LEFT JOIN access_levels
    ON combine.access_level = access_levels.access_level_id

)

SELECT *
FROM fct
