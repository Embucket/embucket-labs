{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_namespace', 'prep_namespace')
]) }},

final AS (

  SELECT
    -- Surrogate Key
    dim_namespace_sk,

    -- Natural Key
    namespace_id,

    -- Legacy Key
    dim_namespace_id,

    -- Foreign Keys
    owner_id,
    creator_id,
    ultimate_parent_namespace_id,
    parent_id,
    dim_product_tier_id,

    -- Attributes
    namespace_is_internal,
    namespace_is_ultimate_parent,
    namespace_name,
    namespace_name_unmasked,
    namespace_path,
    namespace_type,
    has_avatar,
    created_at,
    updated_at,
    is_membership_locked,
    has_request_access_enabled,
    has_share_with_group_locked,
    is_setup_for_company,
    visibility_level,
    ldap_sync_status,
    ldap_sync_error,
    ldap_sync_last_update_at,
    ldap_sync_last_successful_update_at,
    ldap_sync_last_sync_at,
    lfs_enabled,
    shared_runners_enabled,
    shared_runners_minutes_limit,
    extra_shared_runners_minutes_limit,
    repository_size_limit,
    does_require_two_factor_authentication,
    two_factor_grace_period,
    project_creation_level,
    push_rule_id,
    namespace_creator_is_blocked,
    gitlab_plan_id,
    gitlab_plan_title,
    gitlab_plan_is_paid,
    namespace_member_count  AS current_member_count,
    namespace_project_count AS current_project_count,
    has_code_suggestions_enabled,
    is_deleted,
    is_deleted_updated_at
  FROM prep_namespace
  WHERE is_currently_valid = TRUE

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@utkarsh060",
    created_date="2020-12-29",
    updated_date="2024-07-09"
) }}
