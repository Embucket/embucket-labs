WITH prep_project AS (

    SELECT 

      -- surrogate key
      dim_project_sk,

      -- natural key
      project_id,

      --legacy natural key
      dim_project_id,

      --foreign keys
      dim_namespace_id,
      ultimate_parent_namespace_id,
      dim_user_id_creator,
      dim_date_id,

      -- plan/product tier metadata at creation
      dim_product_tier_id_at_creation,
      -- projects metadata
      created_at,
      updated_at,
      last_activity_at,
      visibility_level,
      is_archived,
      has_avatar,
      project_star_count,
      merge_requests_rebase_enabled,
      import_type,
      is_imported,
      approvals_before_merge,
      reset_approvals_on_push,
      merge_requests_ff_only_enabled,
      mirror,
      mirror_user_id,
      shared_runners_enabled,
      build_allow_git_fetch,
      build_timeout,
      mirror_trigger_builds,
      pending_delete,
      public_builds,
      last_repository_check_failed,
      last_repository_check_at,
      container_registry_enabled,
      only_allow_merge_if_pipeline_succeeds,
      has_external_issue_tracker,
      repository_storage,
      repository_read_only,
      request_access_enabled,
      has_external_wiki,
      ci_config_path,
      lfs_enabled,
      only_allow_merge_if_all_discussions_are_resolved,
      repository_size_limit,
      printing_merge_request_link_enabled,
      has_auto_canceling_pending_pipelines,
      service_desk_enabled,
      delete_error,
      last_repository_updated_at,
      storage_version,
      resolve_outdated_diff_discussions,
      disable_overriding_approvers_per_merge_request,
      remote_mirror_available_overridden,
      only_mirror_protected_branches,
      pull_mirror_available_overridden,
      mirror_overwrites_diverged_branches,
      has_merge_trains_enabled,
      namespace_is_internal,
      project_description, 
      project_import_source,
      project_issues_template,
      project_name,
      project_path,
      project_import_url,
      project_merge_requests_template,
      active_service_types_array,
      is_learn_gitlab,
      member_count
    FROM {{ ref('prep_project') }}

)

{{ dbt_audit(
    cte_ref="prep_project",
    created_by="@mpeychet_",
    updated_by="@michellecooper",
    created_date="2021-05-19",
    updated_date="2024-04-10"
) }}
