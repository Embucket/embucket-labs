{{ config({
     "tags":["product"],
     "post-hook": "{{ missing_member_column(primary_key = 'dim_epic_sk', not_null_test_cols = []) }}",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}


WITH prep_epic AS (

    SELECT
      -- PRIMARY KEY
      dim_epic_sk,

      -- NATURAL KEY
      epic_id,

      -- LEGACY NATURAL_KEY TO BE DEPRECATED DURING CHANGE MANAGEMENT PLAN
      dim_epic_id,

      -- FOREIGN KEY
      dim_namespace_sk,
      ultimate_parent_namespace_id,
      dim_created_date_id,
      dim_plan_sk_at_creation,
      dim_user_assignee_sk,
      dim_user_author_sk,
      dim_user_updated_by_sk,
      dim_user_last_edited_by_sk,

      --METADATA
      epic_internal_id,
      lock_version,
      epic_start_date,
      epic_end_date,
      epic_last_edited_at,
      created_at,
      updated_at,
      epic_title,
      -- epic_description, PII masked
      closed_at,
      state_id,
      parent_id,
      relative_position,
      start_date_sourcing_epic_id,
      is_confidential,
      is_internal_epic,
      epic_state,
      epic_title_length,
      epic_description_length,
      epic_url,
      labels,
      upvote_count
    FROM {{ ref('prep_epic') }}

)

{{ dbt_audit(
    cte_ref="prep_epic",
    created_by="@mpeychet_",
    updated_by="@annapiaseczna",
    created_date="2021-06-22",
    updated_date="2023-12-11"
) }}
