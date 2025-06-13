{{ config({
     "tags":["product"],
     "post-hook": "{{ missing_member_column(primary_key = 'dim_issue_sk', not_null_test_cols = []) }}",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH prep_issue AS (

    SELECT

      -- SURROGATE KEY
      dim_issue_sk,

      -- NATURAL KEY
      issue_id,

      -- LEGACY NATURAL KEY
      dim_issue_id,

      -- FOREIGN KEYS
      dim_project_sk,
      dim_namespace_sk,
      ultimate_parent_namespace_id,
      dim_epic_sk,
      created_date_id,
      dim_plan_sk_at_creation,
      dim_milestone_sk,
      sprint_id,
      dim_user_author_sk,
      dim_user_updated_by_sk,
      dim_user_last_edited_by_sk,
      dim_user_closed_by_sk,

      issue_internal_id,
      moved_to_id,
      duplicated_to_id,
      promoted_to_epic_id,
      created_at,
      updated_at,
      issue_last_edited_at,
      issue_closed_at,
      is_confidential,
      issue_title,
      issue_description,

      weight,
      due_date,
      lock_version,
      time_estimate,
      has_discussion_locked,
      relative_position,
      service_desk_reply_to,
      issue_state_id,
      issue_state,
      issue_type,
      severity,
      issue_url,
      milestone_title,
      milestone_due_date,
      labels,
      masked_label_title,
      upvote_count,
      first_mentioned_in_commit_at,
      first_associated_with_milestone_at,
      first_added_to_board_at,
      first_weight_set_at,
      priority,
      is_internal_issue,
      is_security_issue,
      is_included_in_engineering_metrics,
      is_part_of_product,
      is_community_contributor_related,
      is_deleted,
      is_deleted_updated_at
    FROM {{ ref('prep_issue') }}

)

{{ dbt_audit(
    cte_ref="prep_issue",
    created_by="@mpeychet_",
    updated_by="@utkarsh060",
    created_date="2021-06-17",
    updated_date="2024-07-09"
) }}
