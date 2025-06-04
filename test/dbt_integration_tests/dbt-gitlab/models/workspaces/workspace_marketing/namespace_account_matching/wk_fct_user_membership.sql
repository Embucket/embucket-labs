{{ config(
    materialized='table'
) }}


WITH direct_membership AS (

  SELECT *
  FROM {{ ref('wk_fct_direct_membership') }}
  WHERE user_id IS NOT NULL
    AND is_currently_valid = TRUE
    AND is_request = FALSE
    AND is_invite = FALSE

),

group_share AS (

  SELECT *
  FROM {{ ref('wk_fct_group_share') }}
  WHERE is_currently_valid = TRUE

),

lineage AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_lineage_scd') }}
  WHERE is_current = TRUE

),

access_levels AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_access_levels_source') }}

),

-- Group and Project Share act as direct membership,
-- unioning them here includes them in inheritance checks on downstream memberships
direct_share AS (
  SELECT
    direct_membership.direct_member_pk AS unique_id,
    direct_membership.member_id,
    direct_membership.user_id,
    direct_membership.namespace_id,
    direct_membership.access_level,
    direct_membership.member_type,
    direct_membership.membership_created_at
  FROM direct_membership

  UNION ALL

  SELECT
    group_share.group_share_pk,
    group_share.share_id,
    group_share.received_namespace_id, -- stand in for user_id
    group_share.shared_namespace_id,
    group_share.access_level,
    group_share.share_source,
    group_share.created_at
  FROM group_share
),

-- Backdoor into inheritance by looking for direct membership for any up stream namespaces
direct_lineage AS (
  SELECT
    direct_share.unique_id,
    direct_share.member_id,
    direct_share.user_id,
    direct_share.namespace_id,
    direct_share.access_level,
    direct_share.member_type,
    direct_share.membership_created_at,
    lineage.lineage_valid_from,
    lineage.upstream_lineage
  FROM direct_share
  INNER JOIN lineage
    ON direct_share.namespace_id = lineage.namespace_id
),

direct_lineage_flatten AS (
  SELECT DISTINCT
    direct_lineage.namespace_id,
    direct_lineage.lineage_valid_from,
    lineage.value::INTEGER AS lineage_namespace_id
  FROM direct_lineage
  INNER JOIN LATERAL FLATTEN(INPUT =>upstream_lineage) AS lineage
  WHERE lineage_namespace_id != direct_lineage.namespace_id
),

full_direct_membership AS (
  SELECT
    direct_share.unique_id,
    direct_share.user_id,
    direct_lineage_flatten.namespace_id,
    direct_share.access_level,
    GREATEST(
      direct_lineage_flatten.lineage_valid_from,
      direct_share.membership_created_at
    ) AS access_at,
    direct_share.member_id AS source_member_id,
    direct_share.namespace_id AS source_namespace_id,
    direct_share.member_type AS source_member_type,
    'Inherited' AS membership_type
  FROM direct_lineage_flatten
  INNER JOIN direct_share
    ON direct_lineage_flatten.lineage_namespace_id = direct_share.namespace_id

  UNION

  SELECT
    unique_id,
    user_id,
    namespace_id,
    access_level,
    membership_created_at,
    member_id,
    namespace_id,
    member_type,
    IFF(member_type IN ('Group', 'Project'), member_type || ' Share', 'Direct') AS membership_type
  FROM direct_share
),

combine AS (
  SELECT
    unique_id,
    user_id,
    namespace_id,
    access_level,
    access_at,
    source_member_id AS source_share_id,
    source_member_id,
    source_namespace_id,
    membership_type
  FROM full_direct_membership
  WHERE source_member_type NOT IN ('Group', 'Project')

  UNION ALL

  SELECT
    shared.unique_id,
    full_direct_membership.user_id,
    shared.namespace_id,
    LEAST(shared.access_level, full_direct_membership.access_level) AS access_level,
    GREATEST(shared.access_at, full_direct_membership.access_at) AS access_at,
    shared.source_member_id AS source_share_id,
    full_direct_membership.source_member_id,
    shared.user_id AS source_namespace_id,
    shared.membership_type AS membership_type
  FROM full_direct_membership
  INNER JOIN full_direct_membership AS shared
    ON shared.user_id = full_direct_membership.namespace_id
      AND shared.source_member_type IN ('Group', 'Project')
  WHERE full_direct_membership.source_member_type NOT IN ('Group', 'Project')

),

fct AS (

  SELECT
    -- Primary Key
    {{
      dbt_utils.generate_surrogate_key([
        'combine.user_id',
        'combine.namespace_id',
        'combine.source_member_id',
        'combine.source_share_id'
      ])
    }} AS user_membership_pk,

    -- Foreign Keys
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['combine.user_id'])) }} AS dim_user_sk,
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['combine.namespace_id'])) }} AS dim_namespace_sk,

    -- Legacy Keys

    combine.user_id,
    combine.namespace_id,
    combine.source_member_id,
    combine.source_share_id,

    -- Degenerate Dimensions
    combine.access_level,
    access_levels.access_level_name,
    combine.membership_type,
    combine.access_at

    -- Facts

  FROM combine
  LEFT JOIN access_levels
    ON combine.access_level = access_levels.access_level_id

)

SELECT *
FROM fct
