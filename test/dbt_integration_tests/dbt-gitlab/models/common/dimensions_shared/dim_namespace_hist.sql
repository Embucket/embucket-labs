WITH hist as (

    SELECT
      namespace_snapshot_id,

      -- Foreign Keys & IDs
      dim_namespace_id,
      parent_id,
      owner_id,
      push_rule_id,

      -- Date dimensions
      namespace_created_at,
      namespace_updated_at,
      ldap_sync_last_update_at,
      ldap_sync_last_successful_update_at,
      ldap_sync_last_sync_at,

      -- Namespace metadata
      namespace_name,
      namespace_path,
      namespace_type,
      has_avatar,
      is_membership_locked,
      has_request_access_enabled,
      has_share_with_group_locked,
      visibility_level,
      ldap_sync_status,
      ldap_sync_error,
      lfs_enabled,
      shared_runners_enabled,
      shared_runners_minutes_limit,
      extra_shared_runners_minutes_limit,
      repository_size_limit,
      does_require_two_factor_authentication,
      two_factor_grace_period,
      project_creation_level,

      -- hist dimensions
      valid_from,
      valid_to
    FROM {{ ref('prep_namespace_hist') }}
    
)

{{ dbt_audit(
    cte_ref="hist",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-06-16",
    updated_date="2021-06-16"
) }}