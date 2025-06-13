{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_requirement_sk",
    "on_schema_change": "sync_all_columns",
    "post-hook": "{{ missing_member_column(primary_key = 'dim_requirement_sk', not_null_test_cols = []) }}"
    })
}}

WITH source AS (

  SELECT
    -- primary key
    dim_requirement_sk,

    -- natural key
    requirement_id,

    -- foreign keys
    dim_project_id,
    ultimate_parent_namespace_id,
    dim_plan_id,
    author_id,
    created_date_id,

    -- metadata
    requirement_internal_id,
    requirement_state,
    created_at,
    updated_at
  FROM {{ ref('prep_requirement') }}
  {% if is_incremental() %}

  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

  {% endif %}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-08-10",
    updated_date="2023-08-10"
) }}
