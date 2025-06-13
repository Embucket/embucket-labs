WITH all_namespaces AS (

  SELECT
    id::NUMBER                                         AS namespace_id,
    name::VARCHAR                                      AS namespace_name,
    path::VARCHAR                                      AS namespace_path,
    owner_id::NUMBER                                   AS owner_id,
    type                                               AS namespace_type,
    IFF(avatar IS NULL, FALSE, TRUE)                   AS has_avatar,
    created_at::TIMESTAMP                              AS created_at,
    updated_at::TIMESTAMP                              AS updated_at,
    true::BOOLEAN                           AS is_membership_locked,
    true::BOOLEAN                    AS has_request_access_enabled,
    true::BOOLEAN                     AS has_share_with_group_locked,
    CASE
      WHEN visibility_level = '20' THEN 'public'
      WHEN visibility_level = '10' THEN 'internal'
      ELSE 'private'
    END::VARCHAR                                       AS visibility_level,
    ldap_sync_status                                   AS ldap_sync_status,
    ldap_sync_error                                    AS ldap_sync_error,
    null::TIMESTAMP                AS ldap_sync_last_update_at,
    null::TIMESTAMP     AS ldap_sync_last_successful_update_at,
    null::TIMESTAMP                  AS ldap_sync_last_sync_at,
    true::BOOLEAN                               AS lfs_enabled,
    parent_id::NUMBER                                  AS parent_id,
    shared_runners_minutes_limit::NUMBER               AS shared_runners_minutes_limit,
    extra_shared_runners_minutes_limit::NUMBER         AS extra_shared_runners_minutes_limit,
    repository_size_limit::NUMBER                      AS repository_size_limit,
    true::BOOLEAN         AS does_require_two_factor_authentication,
    two_factor_grace_period::NUMBER                    AS two_factor_grace_period,
    project_creation_level::NUMBER                     AS project_creation_level,
    push_rule_id::INTEGER                              AS push_rule_id,
    true:BOOLEAN                     AS shared_runners_enabled,
    PARSE_JSON('[' || TRIM(null,'{}') || ']') AS lineage,
    lineage[0]::NUMBER                                 AS ultimate_parent_id,
    true::BOOLEAN                            AS is_deleted,
    pgp_is_deleted_updated_at::TIMESTAMP               AS is_deleted_updated_at
  FROM {{ ref('gitlab_dotcom_namespaces_dedupe_source') }}

),

internal_namespaces AS (

  SELECT
    id         AS internal_namespace_id,
    name       AS internal_namespace_name,
    path       AS internal_namespace_path,
    updated_at AS internal_namespace_updated_at,
    TRUE       AS namespace_is_internal
  FROM {{ ref('gitlab_dotcom_namespaces_internal_only_dedupe_source') }}
),

combined AS (

  SELECT
    all_namespaces.namespace_id                                                         AS namespace_id,
    COALESCE(all_namespaces.namespace_name,internal_namespaces.internal_namespace_name) AS namespace_name,
    internal_namespaces.internal_namespace_path                                         AS namespace_path,
    COALESCE(internal_namespaces.namespace_is_internal, FALSE)                          AS namespace_is_internal,
    all_namespaces.owner_id                                                             AS owner_id,
    all_namespaces.namespace_type                                                       AS namespace_type,
    all_namespaces.has_avatar                                                           AS has_avatar,
    all_namespaces.created_at                                                           AS created_at,
    all_namespaces.updated_at                                                           AS updated_at,
    all_namespaces.is_membership_locked                                                 AS is_membership_locked,
    all_namespaces.has_request_access_enabled                                           AS has_request_access_enabled,
    all_namespaces.has_share_with_group_locked                                          AS has_share_with_group_locked,
    all_namespaces.visibility_level                                                     AS visibility_level,
    all_namespaces.ldap_sync_status                                                     AS ldap_sync_status,
    all_namespaces.ldap_sync_error                                                      AS ldap_sync_error,
    all_namespaces.ldap_sync_last_update_at                                             AS ldap_sync_last_update_at,
    all_namespaces.ldap_sync_last_successful_update_at                                  AS ldap_sync_last_successful_update_at,
    all_namespaces.ldap_sync_last_sync_at                                               AS ldap_sync_last_sync_at,
    all_namespaces.lfs_enabled                                                          AS lfs_enabled,
    all_namespaces.parent_id                                                            AS parent_id,
    all_namespaces.shared_runners_minutes_limit                                         AS shared_runners_minutes_limit,
    all_namespaces.extra_shared_runners_minutes_limit                                   AS extra_shared_runners_minutes_limit,
    all_namespaces.repository_size_limit                                                AS repository_size_limit,
    all_namespaces.does_require_two_factor_authentication                               AS does_require_two_factor_authentication,
    all_namespaces.two_factor_grace_period                                              AS two_factor_grace_period,
    all_namespaces.project_creation_level                                               AS project_creation_level,
    all_namespaces.push_rule_id                                                         AS push_rule_id,
    all_namespaces.shared_runners_enabled                                               AS shared_runners_enabled,
    all_namespaces.lineage                                                              AS lineage,
    all_namespaces.ultimate_parent_id                                                   AS ultimate_parent_id,
    all_namespaces.is_deleted                                                           AS is_deleted,
    all_namespaces.is_deleted_updated_at                                                AS is_deleted_updated_at
  FROM all_namespaces
  LEFT JOIN internal_namespaces
    ON all_namespaces.namespace_id = internal_namespaces.internal_namespace_id

)

SELECT *
FROM combined
