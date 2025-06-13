{{ config(
    tags=["product"]
) }}

WITH prep_ci_runner AS (

  SELECT

    -- SURROGATE KEY
    dim_ci_runner_sk,

    --NATURAL KEY
    ci_runner_id,

    --LEGACY NATURAL KEY
    dim_ci_runner_id,

    -- FOREIGN KEYS
    created_date_id,
    created_at,
    updated_at,
    ci_runner_description,

    --- CI Runner Manager Mapping
    ci_runner_manager,

    --- CI Runner Machine Type Mapping
    ci_runner_machine_type,
    cost_factor,
    contacted_at,
    is_active,
    ci_runner_version,
    revision,
    platform,
    is_untagged,
    is_locked,
    access_level,
    maximum_timeout,
    ci_runner_type,
    ci_runner_type_summary,
    public_projects_minutes_cost_factor,
    private_projects_minutes_cost_factor

  FROM {{ ref('prep_ci_runner') }}

)

{{ dbt_audit(
    cte_ref="prep_ci_runner",
    created_by="@snalamaru",
    updated_by="@michellecooper",
    created_date="2021-06-23",
    updated_date="2025-01-10"
) }}
