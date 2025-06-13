{{ config(
    tags=["product"]
) }}

WITh prep AS (

  SELECT *
  FROM {{ ref('prep_ci_pipeline') }}

),

final AS (

  SELECT

    -- SURROGATE KEY
    dim_ci_pipeline_sk,

    --LEGACY NATURAL KEY
    dim_ci_pipeline_id,

    --NATURAL KEY
    ci_pipeline_id,

    -- FOREIGN KEYS
    dim_project_id,
    dim_namespace_id,
    ultimate_parent_namespace_id,
    dim_user_id,
    created_date_id,
    dim_plan_id,
    merge_request_id,

    created_at,
    started_at,
    updated_at,
    committed_at,
    finished_at,
    ci_pipeline_duration_in_s,

    pipeline_name_internal_only,
    status,
    ref,
    has_tag,
    yaml_errors,
    lock_version,
    auto_canceled_by_id,
    pipeline_schedule_id,
    ci_pipeline_source,
    ci_pipeline_source_id,
    config_source,
    is_protected,
    failure_reason_id,
    failure_reason,
    ci_pipeline_internal_id,
    is_deleted,
    is_deleted_updated_at
  FROM prep

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mpeychet_",
    updated_by="@mdrussell",
    created_date="2021-06-10",
    updated_date="2025-01-14"
) }}
