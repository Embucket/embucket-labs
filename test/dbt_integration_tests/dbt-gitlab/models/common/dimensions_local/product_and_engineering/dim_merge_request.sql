{{ config({
    "tags": ["product"],
    "post-hook": "{{ missing_member_column(primary_key = 'dim_merge_request_sk', not_null_test_cols = []) }}",
    "snowflake_warehouse": generate_warehouse_name('XL')
}) }}

WITH prep_merge_request AS (

    SELECT

      -- SURROGATE KEY
      dim_merge_request_sk,

      -- NATURAL KEY
      merge_request_id,

      -- LEGACY NATURAL KEY
      dim_merge_request_id,

      -- FOREIGN KEYS
      dim_project_sk,
      dim_project_sk_target,
      dim_namespace_sk,
      ultimate_parent_namespace_id,
      created_date_id,
      dim_plan_sk_at_creation,
      dim_milestone_sk,
      dim_user_author_sk,
      dim_user_assignee_sk,
      dim_user_merge_user_sk,
      dim_user_updated_by_sk,
      dim_user_last_edited_by_sk,
      dim_user_merged_by_sk,
      dim_user_latest_closed_by_sk,
      dim_ci_pipeline_sk_head,
      latest_merge_request_diff_id,

      merge_request_internal_id,
      merge_request_title,
      merge_request_description,
      is_merge_to_master,
      merge_error,
      approvals_before_merge,
      lock_version,
      time_estimate,
      merge_request_state_id,
      merge_request_state,
      merge_request_status,
      does_merge_when_pipeline_succeeds,
      does_squash,
      is_discussion_locked,
      does_allow_maintainer_to_push,
      created_at,
      updated_at,
      labels,
      masked_label_title,
      merge_request_last_edited_at,
      is_community_contributor_related,
      is_included_in_engineering_metrics,
      is_part_of_product,
      namespace_is_internal,

      merged_at,
      first_comment_at,
      first_commit_at,
      latest_closed_at,
      first_approved_at,
      latest_build_started_at,
      removed_lines,
      added_lines,
      modified_paths_size,
      diff_size,
      commits_count,
      hours_to_merged_status,
      is_deleted,
      is_deleted_updated_at

    FROM {{ ref('prep_merge_request') }}

)

{{ dbt_audit(
    cte_ref="prep_merge_request",
    created_by="@mpeychet_",
    updated_by="@michellecooper",
    created_date="2021-06-17",
    updated_date="2024-11-14"
) }}
