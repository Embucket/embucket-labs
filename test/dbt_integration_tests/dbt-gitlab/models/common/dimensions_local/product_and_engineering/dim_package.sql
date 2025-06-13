{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_package_sk",
    "on_schema_change": "sync_all_columns",
    "post-hook": "{{ missing_member_column(primary_key = 'dim_package_sk', not_null_test_cols = []) }}"
    })
}}

WITH final AS (

    SELECT
      -- Surrogate Key
      dim_package_sk,

      -- Natural Key
      package_id,

      -- Foreign Keys
      dim_project_id,
      dim_namespace_id,
      ultimate_parent_namespace_id,
      created_date_id,
      dim_plan_id,
      creator_id,

      -- Dimensional contexts
      package_version,
      package_type,
      created_at,
      updated_at
    FROM {{ ref('prep_package')}}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-07-28",
    updated_date="2023-08-10"
) }}
