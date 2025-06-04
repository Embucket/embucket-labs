{{ config({
    "database": env_var('SNOWFLAKE_PROD_DATABASE') ,
    "schema": "legacy",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}
WITH namespace_groups AS (

  SELECT
    namespace_id                                        AS group_id,
    namespace_name                                      AS group_name,
    namespace_path                                      AS group_path,
    owner_id,
    has_avatar,
    created_at,
    updated_at,
    is_membership_locked,
    has_request_access_enabled,
    has_share_with_group_locked,
    visibility_level,
    ldap_sync_status,
    ldap_sync_error,
    ldap_sync_last_update_at,
    ldap_sync_last_successful_update_at,
    ldap_sync_last_sync_at,
    lfs_enabled,
    parent_id                                           AS parent_group_id,
    shared_runners_minutes_limit,
    repository_size_limit,
    does_require_two_factor_authentication,
    two_factor_grace_period,
    plan_id,
    project_creation_level,
    IFF(namespaces.parent_id IS NOT NULL, TRUE, FALSE)  AS is_parent_group

  FROM {{ ref('gitlab_dotcom_namespaces') }} AS namespaces
  WHERE namespace_type = 'Group'

)

SELECT *
FROM namespace_groups
