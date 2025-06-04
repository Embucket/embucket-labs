{{ config({
     "tags":["product"],
     "post-hook": "{{ missing_member_column(primary_key = 'dim_milestone_sk', not_null_test_cols = []) }}",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH prep_milestone AS (

  SELECT
    dim_milestone_sk,
    milestone_id,
    dim_milestone_id,
    created_at,
    updated_at,
    created_date_id,
    dim_project_id,
    ultimate_parent_namespace_id,
    dim_plan_id,
    milestone_title,
    milestone_description,
    milestone_start_date,
    milestone_due_date,
    milestone_status
  FROM {{ ref('prep_milestone') }}

)

{{ dbt_audit(
    cte_ref="prep_milestone",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-02-28",
    updated_date="2024-02-28"
) }}
